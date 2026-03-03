"""
brickbyte - Sync data from 600+ sources directly into Databricks.
"""
import logging
from dataclasses import dataclass, field
from typing import List

from brickbyte.types import Source

logging.getLogger("brickbyte").addHandler(logging.NullHandler())


@dataclass
class SyncResult:
    """Result of a sync operation."""

    records_written: int
    streams_synced: List[str]
    failed_streams: List[str] = field(default_factory=list)
    enriched_tables: List[str] = field(default_factory=list)


def client(
    base_venv_directory: str | None = None,
    secrets_scope: str = "brickbyte",
    profiles: str | None = None,
):
    """
    Create a brickbyte Client.

    Args:
        base_venv_directory: Directory to store virtual environments.
                            Defaults to user's home directory.
        secrets_scope: Databricks Secrets scope for credential discovery
                      (default: "brickbyte")
        profiles: Optional path to YAML profiles file for advanced
                 credential configuration (e.g., credential reuse)

    Returns:
        Client instance
    """
    from brickbyte._client import Client

    return Client(
        base_venv_directory=base_venv_directory,
        secrets_scope=secrets_scope,
        profiles=profiles,
    )


__all__ = ["client", "SyncResult", "Source"]
