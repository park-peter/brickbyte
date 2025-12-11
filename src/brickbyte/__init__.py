import os
import subprocess
import shutil
from pathlib import Path

import virtualenv
from typing import Optional, List, Dict, Union

from typing import Optional

from brickbyte.types import Source, Destination
from brickbyte.destination import _get_databricks_destination


class VirtualEnvManager:
    def __init__(self, env_dir, libraries: List[str] = None):
        self.env_dir = env_dir
        self.libraries = libraries

    def create_virtualenv(self):
        virtualenv.cli_run([self.env_dir])
        print(f"Virtual environment created at {self.env_dir}")

    def install_library(self, library=None):
        library_to_install = [library] or self.libraries
        if not library_to_install:
            raise ValueError("No library specified for installation")
        subprocess.check_call([os.path.join(self.env_dir, 'bin', 'pip'), 'install', *library_to_install])
        print(f"Libraries {library_to_install} installed in virtual environment")

    def install_airbyte_source(self, source: str,
                               override_install: Optional[str] = None):
        if override_install:
            library_to_install = override_install
        else:
            library_to_install = f"airbyte-{source}"
        if not library_to_install:
            raise ValueError("No library specified for installation")
        subprocess.check_call([os.path.join(self.env_dir, 'bin', 'pip'), 'install', library_to_install])
        print(f"Library {library_to_install} installed in virtual environment")
        if not override_install:
            return os.path.join(self.bin_path, source)

    def install_airbyte_from_file(self, path: str):
        subprocess.check_call([os.path.join(self.env_dir, 'bin', 'pip'), 'install', path])
        print(f"Library {path} installed in virtual environment")

    def delete_virtualenv(self):
        shutil.rmtree(self.env_dir)
        print(f"Virtual environment at {self.env_dir} deleted")

    @property
    def bin_path(self):
        return os.path.join(self.env_dir, 'bin')

    def __enter__(self):
        self.create_virtualenv()
        if self.library:
            self.install_library()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.delete_virtualenv()


class BrickByte:

    def __init__(self, sources: Union[List[Source], str],
                 destination: Destination,
                 sources_install: Optional[Dict[str, str]] = None,
                 destination_install: Optional[str] = None,
                 base_venv_directory: Optional[str] = None):
        if isinstance(sources, str):
            self._sources = [sources]
        else:
            self._sources = sources
        self._destination = destination
        self._sources_install = sources_install or {}
        self._destination_install = destination_install or {}
        self._source_env_managers: Dict[str, VirtualEnvManager] = {}
        self._destination_env_manager: Optional[VirtualEnvManager] = None

        self._base_venv_directory = base_venv_directory or str(Path.home())
        assert len(self._sources) > 0, "At least one source should be provided"


    def setup(self):
        for source in self._sources:
            path = os.path.join(self._base_venv_directory, f'brickbyte-{source}')
            source_manager = VirtualEnvManager(path)
            source_manager.create_virtualenv()
            source_manager.install_airbyte_source(source, self._sources_install.get(source))
            self._source_env_managers[source] = source_manager

        path = os.path.join(self._base_venv_directory, f'brickbyte-destination-databricks')
        destination_env_manager = VirtualEnvManager(path)
        destination_env_manager.create_virtualenv()
        destination_env_manager.install_airbyte_from_file(self._destination_install)
        self._destination_env_manager = destination_env_manager

    def get_or_create_cache(self):
        try:
            import airbyte as ab
        except ImportError:
            print("please install airbyte it is not installed")
        path = os.path.join(self._base_venv_directory, f'brickbyte/cache/')
        return ab.new_local_cache(cache_name="brickbyte", cache_dir=path, cleanup=True)

    def get_source_exec_path(self, source: Source):
        bin_path = self._source_env_managers[source].bin_path
        return os.path.join(bin_path, source)

    def get_destination_exec_path(self):
        return os.path.join(self._destination_env_manager.bin_path, "destination-databricks")

    def cleanup(self):
        for source_manager in self._source_env_managers.values():
            source_manager.delete_virtualenv()
        self._destination_env_manager.delete_virtualenv()

    def get_databricks_destination(
        self,
        catalog: str,
        schema: str,
        warehouse_id: Optional[str] = None,
        local_workspace: bool = True,
        server_hostname: Optional[str] = None,
        token: Optional[str] = None,
    ):
        """
        Get a Databricks destination connector.
        
        Args:
            catalog: Unity Catalog name
            schema: Target schema name
            warehouse_id: SQL warehouse ID. If not provided and local_workspace=True,
                         will auto-discover a running warehouse.
            local_workspace: If True, use current workspace credentials via Databricks SDK.
                            If False, server_hostname and token must be provided.
            server_hostname: Databricks workspace hostname (required if local_workspace=False)
            token: Databricks personal access token (required if local_workspace=False)
        
        Returns:
            Configured Airbyte destination
        
        Example:
            # Use current workspace with auto-discovered warehouse
            destination = bb.get_databricks_destination(
                catalog="main", schema="bronze"
            )
            
            # Use current workspace with specific warehouse
            destination = bb.get_databricks_destination(
                catalog="main", schema="bronze", warehouse_id="abc123def456"
            )
            
            # Connect to different workspace
            destination = bb.get_databricks_destination(
                catalog="main", schema="bronze", warehouse_id="abc123def456",
                local_workspace=False, server_hostname="adb-xxx.azuredatabricks.net", token="dapi..."
            )
        """
        return _get_databricks_destination(
            local_executable=self.get_destination_exec_path(),
            catalog=catalog,
            schema=schema,
            warehouse_id=warehouse_id,
            local_workspace=local_workspace,
            server_hostname=server_hostname,
            token=token,
        )
