"""
Abstract base writer for BrickByte.
Defines the interface all writers must implement.
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


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
    def write(
        self,
        cache: Any,
        streams: List[str],
        mode: str = "overwrite",
        primary_key: Optional[Dict[str, List[str]]] = None,
    ) -> int:
        """
        Write data from cache to Databricks.
        
        Args:
            cache: PyAirbyte cache object containing the data
            streams: List of stream names to write
            mode: Write mode - "append", "overwrite", or "merge"
            primary_key: For merge mode, dict mapping stream names to primary key columns
        
        Returns:
            Total number of records written
        """
        pass
    
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
    def close(self):
        """Clean up resources."""
        pass

