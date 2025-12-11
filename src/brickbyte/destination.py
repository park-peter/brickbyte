"""
Databricks destination helper for BrickByte.
"""
import os
from typing import Optional

import airbyte as ab


def _get_databricks_destination(
    local_executable: str,
    catalog: str,
    schema: str,
    http_path: str,
    local_workspace: bool = True,
    server_hostname: Optional[str] = None,
    token: Optional[str] = None,
):
    """Internal helper to create Databricks destination."""
    if local_workspace:
        try:
            from databricks.sdk import WorkspaceClient
            w = WorkspaceClient()
            server_hostname = w.config.host.replace("https://", "").rstrip("/")
            token = w.config.token
        except Exception as e:
            # Fallback to environment variables
            server_hostname = os.environ.get("DATABRICKS_HOST", "").replace("https://", "").rstrip("/")
            token = os.environ.get("DATABRICKS_TOKEN", "")
            if not server_hostname or not token:
                raise ValueError(
                    f"Could not get workspace credentials. Either run in a Databricks notebook "
                    f"or set DATABRICKS_HOST and DATABRICKS_TOKEN environment variables. Error: {e}"
                )
    else:
        if not server_hostname or not token:
            raise ValueError(
                "When local_workspace=False, server_hostname and token must be provided."
            )
    
    return ab.get_destination(
        "destination-databricks",
        config={
            "server_hostname": server_hostname,
            "http_path": http_path,
            "token": token,
            "catalog": catalog,
            "schema": schema,
        },
        local_executable=local_executable
    )

