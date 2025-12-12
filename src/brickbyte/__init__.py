"""
BrickByte - Bridge Airbyte's 600+ connectors directly into Databricks.
"""
import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import virtualenv

from brickbyte.types import Source


@dataclass
class SyncResult:
    """Result of a sync operation."""

    records_written: int
    streams_synced: List[str]


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

    Example (simplest):
        bb = BrickByte()
        bb.sync(
            source="source-faker",
            source_config={"count": 100},
            catalog="main",
            schema="bronze",
        )

    Example (with stream selection):
        bb = BrickByte()
        bb.sync(
            source="source-github",
            source_config={
                "credentials": {"personal_access_token": "..."},
                "repositories": ["owner/repo"],
            },
            catalog="main",
            schema="bronze",
            streams=["commits", "issues"],
        )
    """

    def __init__(self, base_venv_directory: Optional[str] = None):
        """
        Initialize BrickByte.

        Args:
            base_venv_directory: Directory to store virtual environments.
                                Defaults to user's home directory.
        """
        self._base_venv_directory = base_venv_directory or str(Path.home())
        self._source_env_managers: Dict[str, VirtualEnvManager] = {}
        self._cache = None

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

    def _get_cache(self):
        """Get or create local cache for state management."""
        if self._cache:
            return self._cache

        import airbyte as ab

        path = os.path.join(self._base_venv_directory, "brickbyte", "cache")
        self._cache = ab.new_local_cache(
            cache_name="brickbyte", cache_dir=path, cleanup=True
        )
        return self._cache

    def sync(
        self,
        source: str,
        source_config: dict,
        catalog: str,
        schema: str,
        streams: Optional[List[str]] = None,
        warehouse_id: Optional[str] = None,
        mode: str = "full_refresh",
        source_install: Optional[str] = None,
        cleanup: bool = True,
    ) -> SyncResult:
        """
        Sync data from an Airbyte source to Databricks.

        This is the main method - it handles everything:
        - Installs source connector in isolated venv
        - Configures and validates source connection
        - Reads data to local cache
        - Auto-discovers warehouse and authenticates to Databricks
        - Writes data directly to Unity Catalog
        - Cleans up virtual environments

        Args:
            source: Airbyte source connector name (e.g., "source-github")
            source_config: Configuration dictionary for the source connector
            catalog: Unity Catalog name (e.g., "main")
            schema: Target schema name (e.g., "bronze")
            streams: List of streams to sync. None = all streams (default)
            warehouse_id: SQL warehouse ID. Auto-discovered if not provided.
            mode: "full_refresh" (default) or "incremental"
            source_install: Override source installation (e.g., custom git URL)
            cleanup: Whether to cleanup venvs after sync (default: True)

        Returns:
            SyncResult with records_written and streams_synced

        Example:
            bb = BrickByte()
            result = bb.sync(
                source="source-faker",
                source_config={"count": 100},
                catalog="main",
                schema="bronze",
            )
            print(f"Synced {result.records_written} records")
        """
        import airbyte as ab

        from brickbyte.writer import create_writer

        try:
            # Setup source connector
            print(f"Setting up {source}...")
            self._setup_source(source, source_install)

            # Configure source
            print(f"Configuring {source}...")
            ab_source = ab.get_source(
                source,
                config=source_config,
                local_executable=self._get_source_exec_path(source),
            )

            print("Validating source connection...")
            ab_source.check()

            if streams:
                ab_source.select_streams(streams)
            else:
                ab_source.select_all_streams()

            selected = list(ab_source.get_selected_streams())

            # Read data to cache
            print(f"Reading {len(selected)} streams from source...")
            cache = self._get_cache()
            ab_source.read(cache=cache)

            # Write to Databricks
            print(f"Writing to {catalog}.{schema}...")
            writer = create_writer(
                catalog=catalog,
                schema=schema,
                warehouse_id=warehouse_id,
            )

            full_refresh = mode == "full_refresh"
            records = writer.write_from_cache(
                cache=cache,
                streams=selected,
                full_refresh=full_refresh,
            )
            writer.close()

            print(f"✓ Synced {records} records from {len(selected)} streams")

            return SyncResult(
                records_written=records,
                streams_synced=selected,
            )

        finally:
            if cleanup:
                self.cleanup()

    def cleanup(self):
        """Remove virtual environments created by BrickByte."""
        for manager in self._source_env_managers.values():
            manager.delete_virtualenv()
        self._source_env_managers.clear()


__all__ = ["BrickByte", "SyncResult", "Source"]
