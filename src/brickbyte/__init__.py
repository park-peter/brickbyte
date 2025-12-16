"""
BrickByte - Bridge Airbyte's 600+ connectors directly into Databricks.
"""
import logging
import os
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import virtualenv

from brickbyte.types import Source

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
# Configure logging - suppress noisy third-party DEBUG/INFO logs
logging.getLogger().setLevel(logging.WARNING)

_noisy_loggers = [
    "py4j",
    "pyspark",
    "pyspark.sql.connect",
    "pyspark.sql.connect.client",
    "databricks",
    "databricks.sdk",
    "urllib3",
    "grpc",
    "airbyte",
]
for _logger_name in _noisy_loggers:
    logging.getLogger(_logger_name).setLevel(logging.WARNING)

logger = logging.getLogger("brickbyte")
logger.setLevel(logging.INFO)


@dataclass
class SyncResult:
    """Result of a sync operation."""

    records_written: int
    streams_synced: List[str]
    enriched_tables: List[str] = field(default_factory=list)


class VirtualEnvManager:
    """Manages isolated Python virtual environments for source connectors."""

    def __init__(self, env_dir: str):
        self.env_dir = env_dir

    def create_virtualenv(self):
        virtualenv.cli_run([self.env_dir])

    def install_airbyte_source(
        self, source: str, override_install: Optional[str] = None
    ):
        library = override_install or f"airbyte-{source}"
        subprocess.check_call(
            [os.path.join(self.env_dir, "bin", "pip"), "install", library],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    def delete_virtualenv(self):
        if os.path.exists(self.env_dir):
            shutil.rmtree(self.env_dir)

    @property
    def bin_path(self):
        return os.path.join(self.env_dir, "bin")


class BrickByte:
    """
    BrickByte - Sync data from any Airbyte source to Databricks.
    
    Uses a streaming architecture to bypass local disk storage and
    write directly to Unity Catalog Volumes.
    """

    def __init__(
        self, 
        base_venv_directory: Optional[str] = None,
    ):
        """
        Initialize BrickByte.

        Args:
            base_venv_directory: Directory to store virtual environments.
                                Defaults to user's home directory.
        """
        self._base_venv_directory = base_venv_directory or str(Path.home())
        self._source_env_managers: Dict[str, VirtualEnvManager] = {}

    def _setup_source(self, source: str, source_install: Optional[str] = None):
        """Install source connector in isolated venv."""
        if source in self._source_env_managers:
            return

        path = os.path.join(self._base_venv_directory, f"brickbyte-{source}")
        manager = VirtualEnvManager(path)
        manager.create_virtualenv()
        manager.install_airbyte_source(source, source_install)
        self._source_env_managers[source] = manager

    def _get_source_exec_path(self, source: str) -> str:
        """Get path to source connector executable."""
        return os.path.join(self._source_env_managers[source].bin_path, source)

    def _validate_sync_params(
        self,
        mode: str,
        staging_volume: str,
    ):
        """Validate sync parameters."""
        # Validate mode
        # Currently we only support what StreamingWriter supports (effectively append/create)
        # But we keep parameter for API compatibility/future expansion
        valid_modes = ("append", "overwrite")
        if mode not in valid_modes:
            if mode == "merge":
                raise NotImplementedError("Merge mode is not yet supported in streaming architecture.")
            raise ValueError(
                f"Invalid mode '{mode}'. Must be one of: {', '.join(valid_modes)}"
            )
            
        if not staging_volume:
            # staging_volume is conditionally required by the factory (if no active Spark session)
            # We defer detailed validation to the factory
            pass

    def preview(
        self,
        source: str,
        source_config: dict,
        catalog: str,
        schema: str,
        streams: Optional[List[str]] = None,
        source_install: Optional[str] = None,
        sample_size: int = 5,
    ):
        """
        Preview a sync operation.

        Args:
            source: Airbyte source connector name
            source_config: Configuration dictionary for the source
            catalog: Unity Catalog name
            schema: Target schema name
            streams: List of streams to preview (None = all streams)
            primary_key: For merge preview, dict mapping stream names to keys
            source_install: Override source installation
            sample_size: Number of sample records per stream

        Returns:
            PreviewResult with detailed comparison
        """
        import airbyte as ab

        from brickbyte.preview import PreviewEngine

        try:
            # Setup source
            logger.info(f"Setting up {source}...")
            self._setup_source(source, source_install)

            # Configure source
            ab_source = ab.get_source(
                source,
                config=source_config,
                local_executable=self._get_source_exec_path(source),
            )
            ab_source.check()

            if streams:
                ab_source.select_streams(streams)
            else:
                ab_source.select_all_streams()

            selected = list(ab_source.get_selected_streams())

            # Generate preview
            logger.info("Generating preview (streaming)...")
            engine = PreviewEngine(catalog=catalog, schema=schema)
            result = engine.preview(
                ab_source=ab_source,
                streams=selected,
                sample_size=sample_size,
            )

            return result

        finally:
            self.cleanup()

    def sync(
        self,
        source: str,
        source_config: dict,
        catalog: str,
        schema: str,
        staging_volume: Optional[str] = None,
        streams: Optional[List[str]] = None,
        mode: str = "overwrite",
        enrich_metadata: bool = False,
        warehouse_id: Optional[str] = None,
        source_install: Optional[str] = None,
        cleanup: bool = True,
        buffer_size_records: int = 50000,
        buffer_size_mb: int = 100,
    ) -> SyncResult:
        """
        Sync data from an Airbyte source to Databricks (Streaming).
        
        Args:
            source: Airbyte source connector name (e.g., "source-github")
            source_config: Configuration dictionary for the source connector
            catalog: Unity Catalog name (e.g., "main")
            schema: Target schema name (e.g., "bronze")
            staging_volume: Unity Catalog Volume path (REQUIRED for remote)
            streams: List of streams to sync. None = all streams (default)
            mode: Write mode (currently supports "overwrite" and "append")
            enrich_metadata: If True, use AI to generate column descriptions
            warehouse_id: SQL warehouse ID (optional, auto-discovered)
            source_install: Override source installation (e.g., custom git URL)
            cleanup: Whether to cleanup venvs after sync (default: True)
            buffer_size_records: Records per micro-batch (default: 50k)
            buffer_size_mb: Max batch size in MB (default: 100MB)

        Returns:
            SyncResult with records_written, streams_synced, and enriched_tables
        """
        import airbyte as ab

        from brickbyte.writers import create_streaming_writer

        # Validate parameters
        self._validate_sync_params(mode, staging_volume)

        try:
            # Setup source connector
            logger.info(f"Setting up {source}...")
            self._setup_source(source, source_install)

            # Configure source
            logger.info(f"Configuring {source}...")
            ab_source = ab.get_source(
                source,
                config=source_config,
                local_executable=self._get_source_exec_path(source),
            )

            logger.info("Validating source connection...")
            ab_source.check()

            if streams:
                ab_source.select_streams(streams)
            else:
                ab_source.select_all_streams()

            selected = list(ab_source.get_selected_streams())

            # STREAMING EXECUTION
            via_msg = f" via {staging_volume}" if staging_volume else " (Native Spark)"
            logger.info(f"Streaming {len(selected)} streams to {catalog}.{schema}{via_msg}...")
            
            writer = create_streaming_writer(
                catalog=catalog,
                schema=schema,
                staging_volume=staging_volume,
                warehouse_id=warehouse_id,
                buffer_size_records=buffer_size_records,
                buffer_size_mb=buffer_size_mb,
            )
            
            total_records = 0
            # Iterate through selected streams
            for stream_name in selected:
                logger.info(f"  Streaming: {stream_name}")
                
                # Handle Overwrite Mode
                if mode == "overwrite":
                    writer.drop_table(stream_name)

                try:
                    records_generator = ab_source.get_records(stream_name)
                    count = 0
                    for record in records_generator:
                        writer.write_record(stream_name, record)
                        count += 1
                        if count % 10000 == 0:
                            logger.info(f"    ...streamed {count} records")
                    
                    # Flush remaining
                    writer.flush_stream(stream_name)
                    logger.info(f"    ✓ {count} records streamed")
                    total_records += count
                except Exception as e:
                    logger.warning(f"  Warning: Streaming failed for {stream_name}: {e}")
            
            writer.close()
            
            # Enrich metadata with AI if requested
            enriched_tables = []
            if enrich_metadata:
                logger.info("Enriching metadata with AI...")
                from brickbyte.enrichment import enrich_table

                for stream_name in selected:
                    try:
                        enrich_table(
                            catalog=catalog,
                            schema=schema,
                            table=stream_name,
                            apply_to_catalog=True,
                        )
                        enriched_tables.append(stream_name)
                    except Exception as e:
                        logger.warning(f"  Warning: Could not enrich {stream_name}: {e}")

            return SyncResult(
                records_written=total_records,
                streams_synced=selected,
                enriched_tables=enriched_tables,
            )

        finally:
            if cleanup:
                self.cleanup()

    def cleanup(self):
        """Remove virtual environments."""
        for manager in self._source_env_managers.values():
            manager.delete_virtualenv()
        self._source_env_managers.clear()


__all__ = ["BrickByte", "SyncResult", "Source"]
