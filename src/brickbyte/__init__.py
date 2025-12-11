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

# Hardcoded destination install URL - always Databricks
DESTINATION_INSTALL_URL = (
    "git+https://github.com/park-peter/brickbyte.git"
    "#subdirectory=integrations/destination-databricks-py"
)


@dataclass
class SyncResult:
    """Result of a sync operation."""

    records_written: int
    streams_synced: List[str]


class VirtualEnvManager:
    """Manages isolated Python virtual environments for connectors."""

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

    def install_from_url(self, url: str):
        subprocess.check_call(
            [os.path.join(self.env_dir, "bin", "pip"), "install", url],
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
        self._destination_env_manager: Optional[VirtualEnvManager] = None
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

    def _setup_destination(self):
        """Install Databricks destination connector in isolated venv."""
        if self._destination_env_manager:
            return

        path = os.path.join(
            self._base_venv_directory, "brickbyte-destination-databricks"
        )
        manager = VirtualEnvManager(path)
        manager.create_virtualenv()
        manager.install_from_url(DESTINATION_INSTALL_URL)
        self._destination_env_manager = manager

    def _get_source_exec_path(self, source: str) -> str:
        """Get path to source connector executable."""
        return os.path.join(self._source_env_managers[source].bin_path, source)

    def _get_destination_exec_path(self) -> str:
        """Get path to destination connector executable."""
        return os.path.join(
            self._destination_env_manager.bin_path, "destination-databricks"
        )

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

    def _get_destination(
        self,
        catalog: str,
        schema: str,
        warehouse_id: Optional[str] = None,
    ):
        """Get configured Databricks destination."""
        import airbyte as ab
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        server_hostname = w.config.host.replace("https://", "").rstrip("/")
        token = w.config.token

        # Auto-discover warehouse if not provided
        if not warehouse_id:
            warehouses = list(w.warehouses.list())
            running = [
                wh
                for wh in warehouses
                if wh.state and wh.state.value == "RUNNING"
            ]
            if running:
                warehouse_id = running[0].id
            else:
                raise ValueError(
                    "No running SQL warehouse found. "
                    "Specify warehouse_id or start a warehouse."
                )

        http_path = f"/sql/1.0/warehouses/{warehouse_id}"

        return ab.get_destination(
            "destination-databricks",
            config={
                "server_hostname": server_hostname,
                "http_path": http_path,
                "token": token,
                "catalog": catalog,
                "schema": schema,
            },
            local_executable=self._get_destination_exec_path(),
        )

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
        - Installs Databricks destination connector
        - Configures and validates source connection
        - Auto-discovers warehouse and authenticates to Databricks
        - Syncs data
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

        try:
            # Setup connectors
            print(f"Setting up {source}...")
            self._setup_source(source, source_install)
            print("Setting up Databricks destination...")
            self._setup_destination()

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

            # Configure destination
            print("Configuring Databricks destination...")
            destination = self._get_destination(catalog, schema, warehouse_id)

            # Get cache
            cache = self._get_cache()

            # Sync
            force_full_refresh = mode == "full_refresh"
            print(f"Syncing {len(selected)} streams to {catalog}.{schema}…")
            result = destination.write(
                ab_source, cache=cache, force_full_refresh=force_full_refresh
            )

            records = getattr(result, "processed_records", 0)
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

        if self._destination_env_manager:
            self._destination_env_manager.delete_virtualenv()
            self._destination_env_manager = None


__all__ = ["BrickByte", "SyncResult", "Source"]
