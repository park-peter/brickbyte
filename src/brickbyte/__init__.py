"""
Brickbyte - Sync data from 600+ sources directly into Databricks.
"""
import logging
import os
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

from brickbyte.types import Source

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
# Suppress noisy third-party DEBUG/INFO logs
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
    failed_streams: List[str] = field(default_factory=list)
    enriched_tables: List[str] = field(default_factory=list)


class VirtualEnvManager:
    """Manages isolated Python virtual environments for source connectors."""

    def __init__(self, env_dir: str):
        self.env_dir = env_dir

    def create_virtualenv(self):
        import virtualenv
        virtualenv.cli_run([self.env_dir])

    def install_source(
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


class Brickbyte:
    """
    Brickbyte - Sync data from any source connector to Databricks.
    
    Uses a streaming architecture to bypass local disk storage and
    write directly to Unity Catalog.
    
    Supports automatic credential discovery from Databricks Secrets:
        - Default scope: "brickbyte"
        - Key convention: "{source-name}/{field}" (e.g., "source-s3/aws_access_key_id")
        - Optional YAML profiles for credential reuse across sources
    """

    def __init__(
        self, 
        base_venv_directory: Optional[str] = None,
        secrets_scope: str = "brickbyte",
        profiles: Optional[str] = None,
    ):
        """
        Initialize Brickbyte.

        Args:
            base_venv_directory: Directory to store virtual environments.
                                Defaults to user's home directory.
            secrets_scope: Databricks Secrets scope for credential discovery
                          (default: "brickbyte")
            profiles: Optional path to YAML profiles file for advanced
                     credential configuration (e.g., credential reuse)
        """
        self._base_venv_directory = base_venv_directory or str(Path.home())
        self._source_env_managers: Dict[str, VirtualEnvManager] = {}
        
        # Initialize credential resolver
        from brickbyte.credentials import CredentialResolver
        self._credential_resolver = CredentialResolver(
            secrets_scope=secrets_scope,
            profiles_path=profiles,
        )

    def _setup_source(self, source: str, source_install: Optional[str] = None):
        """Install source connector in isolated venv."""
        if source in self._source_env_managers:
            return

        path = os.path.join(self._base_venv_directory, f"brickbyte-{source}")
        manager = VirtualEnvManager(path)
        manager.create_virtualenv()
        manager.install_source(source, source_install)
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
        valid_modes = ("append", "overwrite")
        if mode not in valid_modes:
            if mode == "merge":
                raise NotImplementedError("Merge mode is not yet supported.")
            raise ValueError(
                f"Invalid mode '{mode}'. Must be one of: {', '.join(valid_modes)}"
            )

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
            source: Source connector name
            source_config: Configuration dictionary for the source
            catalog: Unity Catalog name
            schema: Target schema name
            streams: List of streams to preview (None = all streams)
            source_install: Override source installation
            sample_size: Number of sample records per stream

        Returns:
            PreviewResult with detailed comparison
        """
        import airbyte as ab

        from brickbyte.preview import PreviewEngine

        merged_config = self._credential_resolver.merge_credentials(source, source_config)

        try:
            logger.info(f"Setting up {source}...")
            self._setup_source(source, source_install)

            ab_source = ab.get_source(
                source,
                config=merged_config,
                local_executable=self._get_source_exec_path(source),
            )
            ab_source.check()

            if streams:
                ab_source.select_streams(streams)
            else:
                ab_source.select_all_streams()

            selected = list(ab_source.get_selected_streams())

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
        flatten: bool = False,
        enrich_metadata: bool = False,
        enrich_model: Optional[str] = None,
        warehouse_id: Optional[str] = None,
        source_install: Optional[str] = None,
        cleanup: bool = True,
        buffer_size_records: int = 50000,
        buffer_size_mb: int = 100,
        continue_on_error: bool = False,
    ) -> SyncResult:
        """
        Sync data from a source connector to Databricks (Streaming).
        
        Args:
            source: Source connector name (e.g., "source-github")
            source_config: Configuration dictionary for the source connector
            catalog: Unity Catalog name (e.g., "main")
            schema: Target schema name (e.g., "bronze")
            staging_volume: Unity Catalog Volume path (REQUIRED for remote)
            streams: List of streams to sync. None = all streams (default)
            mode: Write mode ("overwrite" or "append")
            flatten: If True, flatten record fields into columns. 
                    If False (default), store as JSON in 'data' column.
            enrich_metadata: If True, use AI to generate column descriptions
            enrich_model: Foundation Model endpoint for enrichment
            warehouse_id: SQL warehouse ID (optional, auto-discovered)
            source_install: Override source installation (e.g., custom git URL)
            cleanup: Whether to cleanup venvs after sync (default: True)
            buffer_size_records: Records per micro-batch (default: 50k)
            buffer_size_mb: Max batch size in MB (default: 100MB)
            continue_on_error: If True, continue syncing other streams if one fails

        Returns:
            SyncResult with records_written, streams_synced, failed_streams, enriched_tables
        """
        import airbyte as ab

        from brickbyte.writers import create_streaming_writer

        self._validate_sync_params(mode, staging_volume)
        merged_config = self._credential_resolver.merge_credentials(source, source_config)

        try:
            logger.info(f"Setting up {source}...")
            self._setup_source(source, source_install)

            logger.info(f"Configuring {source}...")
            ab_source = ab.get_source(
                source,
                config=merged_config,
                local_executable=self._get_source_exec_path(source),
            )

            logger.info("Validating source connection...")
            ab_source.check()

            if streams:
                ab_source.select_streams(streams)
            else:
                ab_source.select_all_streams()

            selected = list(ab_source.get_selected_streams())

            via_msg = f" via {staging_volume}" if staging_volume else " (Native Spark)"
            logger.info(f"Streaming {len(selected)} streams to {catalog}.{schema}{via_msg}...")
            
            writer = create_streaming_writer(
                catalog=catalog,
                schema=schema,
                staging_volume=staging_volume,
                warehouse_id=warehouse_id,
                buffer_size_records=buffer_size_records,
                buffer_size_mb=buffer_size_mb,
                flatten=flatten,
            )
            
            total_records = 0
            failed_streams: List[str] = []
            
            for stream_name in selected:
                logger.info(f"  Streaming: {stream_name}")
                
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
                    
                    writer.flush_stream(stream_name)
                    logger.info(f"    ✓ {count} records streamed")
                    total_records += count
                except Exception as e:
                    error_name = type(e).__name__
                    logger.error(f"  ✗ Failed to stream {stream_name}: {e}")
                    failed_streams.append(stream_name)
                    
                    is_fatal = "ConnectorFailed" in error_name
                    if is_fatal and not continue_on_error:
                        raise
                    if not continue_on_error:
                        raise
            
            if failed_streams:
                if continue_on_error:
                    logger.warning(
                        f"Completed with {len(failed_streams)} failed streams: {failed_streams}"
                    )
                else:
                    raise RuntimeError(f"Sync failed. Failed streams: {failed_streams}")
            
            writer.close()
            
            successful_streams = [s for s in selected if s not in failed_streams]
            
            enriched_tables = []
            if enrich_metadata and successful_streams:
                logger.info("Enriching metadata with AI...")
                from brickbyte.enrichment import enrich_table

                model = enrich_model or "databricks-meta-llama-3-3-70b-instruct"
                for stream_name in successful_streams:
                    try:
                        enrich_table(
                            catalog=catalog,
                            schema=schema,
                            table=stream_name,
                            apply_to_catalog=True,
                            model_name=model,
                        )
                        enriched_tables.append(stream_name)
                    except Exception as e:
                        logger.warning(f"  Warning: Could not enrich {stream_name}: {e}")

            return SyncResult(
                records_written=total_records,
                streams_synced=successful_streams,
                failed_streams=failed_streams,
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

    def list_configured_sources(self) -> List[str]:
        """List all sources that have credentials configured."""
        return self._credential_resolver.list_available_sources()

    def validate_credentials(self, source: str) -> bool:
        """Check if credentials are configured for a source."""
        return self._credential_resolver.validate(source)


__all__ = ["Brickbyte", "SyncResult", "Source"]
