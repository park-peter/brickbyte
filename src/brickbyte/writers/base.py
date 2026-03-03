"""
Abstract base writer for brickbyte.
Defines the interface all writers must implement.
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Optional


class BaseWriter(ABC):
    """
    Abstract base class for all brickbyte writers.

    Writers handle writing data from PyAirbyte cache to Databricks.
    """

    def __init__(
        self,
        catalog: str,
        schema: str,
        run_id: str = "",
        dedup_keys: Optional[Dict[str, List[str]]] = None,
    ):
        self.catalog = catalog
        self.schema = schema
        self.run_id = run_id
        self.dedup_keys = dedup_keys

    def get_table_name(self, stream_name: str) -> str:
        """Get fully qualified, backtick-quoted table name for a stream."""
        from brickbyte._sanitize import quoted_table_name, sanitize_stream_name

        sanitized = sanitize_stream_name(stream_name)
        return quoted_table_name(self.catalog, self.schema, sanitized)

    def get_staging_table_name(self, stream_name: str, run_id: str) -> str:
        """Get staging table name for safe overwrite."""
        from brickbyte._sanitize import quoted_table_name, sanitize_stream_name

        sanitized = sanitize_stream_name(stream_name)
        run_id_short = run_id[:8]
        staging_name = f"{sanitized}__stg__{run_id_short}"
        return quoted_table_name(self.catalog, self.schema, staging_name)

    def _get_dedup_keys_for_stream(self, stream_name: str) -> Optional[List[str]]:
        """Get dedup keys for a specific stream."""
        if self.dedup_keys is None:
            return None
        # Check for __all__ (list was expanded to all streams)
        if "__all__" in self.dedup_keys:
            return self.dedup_keys["__all__"]
        return self.dedup_keys.get(stream_name)

    @abstractmethod
    def table_exists(self, stream_name: str) -> bool:
        """Check if a table exists."""
        pass

    @abstractmethod
    def get_table_schema(self, stream_name: str) -> Optional[Dict[str, str]]:
        """Get schema of an existing table."""
        pass

    @abstractmethod
    def drop_table(self, stream_name: str):
        """Drop a table if it exists."""
        pass

    @abstractmethod
    def write_record(self, stream_name: str, record: dict):
        """Buffer a single record for writing."""
        pass

    @abstractmethod
    def flush_stream(self, stream_name: str):
        """Flush buffered records for a specific stream."""
        pass

    @abstractmethod
    def close(self):
        """Flush all buffers and clean up resources."""
        pass

    @abstractmethod
    def safe_overwrite_begin(self, stream_name: str, run_id: str):
        """Begin safe overwrite — redirect writes to staging table."""
        pass

    @abstractmethod
    def safe_overwrite_finish(self, stream_name: str, run_id: str):
        """Finish safe overwrite — atomic swap from staging to target."""
        pass
