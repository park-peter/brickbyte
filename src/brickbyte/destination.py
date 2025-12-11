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
    warehouse_id: Optional[str] = None,
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
            
            # Auto-discover warehouse if not provided
            if not warehouse_id:
                warehouses = list(w.warehouses.list())
                running = [wh for wh in warehouses if wh.state and wh.state.value == "RUNNING"]
                if running:
                    warehouse_id = running[0].id
                else:
                    raise ValueError(
                        "No running SQL warehouse found. Please specify warehouse_id or start a warehouse."
                    )
        except ImportError as e:
            raise ValueError(f"databricks-sdk is required for local_workspace=True: {e}")
        except Exception as e:
            # Fallback to environment variables for host/token
            server_hostname = os.environ.get("DATABRICKS_HOST", "").replace("https://", "").rstrip("/")
            token = os.environ.get("DATABRICKS_TOKEN", "")
            if not server_hostname or not token:
                raise ValueError(
                    f"Could not get workspace credentials. Either run in a Databricks notebook "
                    f"or set DATABRICKS_HOST and DATABRICKS_TOKEN environment variables. Error: {e}"
                )
            if not warehouse_id:
                raise ValueError("warehouse_id is required when credentials come from environment variables.")
    else:
        if not server_hostname or not token:
            raise ValueError(
                "When local_workspace=False, server_hostname and token must be provided."
            )
        if not warehouse_id:
            raise ValueError("warehouse_id is required when local_workspace=False.")
    
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
        local_executable=local_executable
    )

