"""
Abstract base writer for BrickByte.
Defines the interface all writers must implement.
"""
from abc import ABC, abstractmethod
from typing import Dict, Optional


class BaseWriter(ABC):
    """
    Abstract base class for all BrickByte writers.
    
    Writers handle writing data from PyAirbyte cache to Databricks.
    """
    
    def __init__(self, catalog: str, schema: str):
        """
        Initialize the writer.
        
        Args:
            catalog: Unity Catalog name
            schema: Target schema name
        """
        self.catalog = catalog
        self.schema = schema
    
    def get_table_name(self, stream_name: str) -> str:
        """Get fully qualified table name for a stream."""
        return f"{self.catalog}.{self.schema}.{stream_name}"
    
    @abstractmethod
    def table_exists(self, stream_name: str) -> bool:
        """Check if a table exists."""
        pass
    
    @abstractmethod
    def get_table_schema(self, stream_name: str) -> Optional[Dict[str, str]]:
        """
        Get schema of an existing table.
        
        Returns:
            Dict mapping column names to types, or None if table doesn't exist
        """
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

