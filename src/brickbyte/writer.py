"""
Direct Databricks writer for BrickByte.
Writes data from PyAirbyte cache directly to Databricks Unity Catalog.
"""
import json
from collections import defaultdict
from datetime import datetime
from typing import List, Optional
from uuid import uuid4


class DatabricksWriter:
    """
    Writes data directly to Databricks Unity Catalog tables.
    
    Data is written in Airbyte raw format:
    - _airbyte_ab_id: unique record identifier
    - _airbyte_emitted_at: timestamp when record was extracted
    - _airbyte_data: JSON payload of the record
    """
    
    FLUSH_INTERVAL = 1000
    
    def __init__(
        self,
        server_hostname: str,
        http_path: str,
        access_token: str,
        catalog: str,
        schema: str,
    ):
        self.server_hostname = server_hostname
        self.http_path = http_path
        self.access_token = access_token
        self.catalog = catalog
        self.schema = schema
        self._connection = None
        self._buffer = defaultdict(list)
        self._buffer_size = 0
    
    def _get_connection(self):
        """Get or create database connection."""
        if self._connection is None:
            from databricks import sql
            self._connection = sql.connect(
                server_hostname=self.server_hostname,
                http_path=self.http_path,
                access_token=self.access_token,
                catalog=self.catalog,
                schema=self.schema,
            )
        return self._connection
    
    def _execute(self, query: str):
        """Execute a SQL query."""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(query)
        cursor.close()
    
    def create_raw_table(self, stream_name: str):
        """Create the raw table for a stream if it doesn't exist."""
        query = f"""
        CREATE TABLE IF NOT EXISTS _airbyte_raw_{stream_name} (
            _airbyte_ab_id STRING,
            _airbyte_emitted_at TIMESTAMP,
            _airbyte_data STRING
        )
        """
        self._execute(query)
    
    def drop_table(self, stream_name: str):
        """Drop a raw table (used for full refresh)."""
        self._execute(f"DROP TABLE IF EXISTS _airbyte_raw_{stream_name}")
    
    @staticmethod
    def _quote_value(value):
        """Escape and quote a value for SQL insertion."""
        if isinstance(value, str):
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        elif isinstance(value, dict):
            escaped = json.dumps(value).replace("'", "''")
            return f"'{escaped}'"
        elif value is None:
            return "NULL"
        elif isinstance(value, datetime):
            return f"'{str(value)}'"
        else:
            return str(value)
    
    def _flush_buffer(self):
        """Flush buffered records to Databricks."""
        if not self._buffer:
            return
        
        conn = self._get_connection()
        cursor = conn.cursor()
        
        for table, records in self._buffer.items():
            if not records:
                continue
            
            values = ",".join([
                f"({self._quote_value(rid)}, "
                f"{self._quote_value(ts)}, "
                f"{self._quote_value(data)})"
                for rid, ts, data in records
            ])
            
            query = (
                f"INSERT INTO _airbyte_raw_{table} "
                f"(_airbyte_ab_id, _airbyte_emitted_at, _airbyte_data) "
                f"VALUES {values}"
            )
            cursor.execute(query)
        
        cursor.close()
        self._buffer.clear()
        self._buffer_size = 0
    
    def write_record(self, stream_name: str, record: dict):
        """
        Buffer a record for writing.
        
        Args:
            stream_name: Name of the stream
            record: Record data as dictionary
        """
        record_id = str(uuid4())
        timestamp = datetime.now()
        data_json = json.dumps(record)
        
        self._buffer[stream_name].append((record_id, timestamp, data_json))
        self._buffer_size += 1
        
        if self._buffer_size >= self.FLUSH_INTERVAL:
            self._flush_buffer()
    
    def write_from_cache(
        self,
        cache,
        streams: List[str],
        full_refresh: bool = True,
    ) -> int:
        """
        Write data from PyAirbyte cache to Databricks.
        
        Args:
            cache: PyAirbyte cache object containing the data
            streams: List of stream names to write
            full_refresh: If True, drop and recreate tables
        
        Returns:
            Total number of records written
        """
        total_records = 0
        
        for stream_name in streams:
            # Handle table creation/refresh
            if full_refresh:
                self.drop_table(stream_name)
            self.create_raw_table(stream_name)
            
            # Get data from cache
            try:
                dataset = cache[stream_name]
                df = dataset.to_pandas()
            except Exception as e:
                print(f"  Warning: Could not read stream '{stream_name}': {e}")
                continue
            
            # Write each record
            record_count = 0
            for _, row in df.iterrows():
                record = row.to_dict()
                self.write_record(stream_name, record)
                record_count += 1
            
            total_records += record_count
        
        # Flush remaining records
        self._flush_buffer()
        
        return total_records
    
    def close(self):
        """Close the database connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
    
    def check_connection(self) -> bool:
        """
        Test the database connection.
        
        Returns:
            True if connection successful
        
        Raises:
            Exception if connection fails
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        return True


def create_writer(
    catalog: str,
    schema: str,
    warehouse_id: Optional[str] = None,
) -> DatabricksWriter:
    """
    Create a DatabricksWriter with auto-discovered credentials.
    
    Args:
        catalog: Unity Catalog name
        schema: Target schema name
        warehouse_id: SQL warehouse ID (auto-discovered if not provided)
    
    Returns:
        Configured DatabricksWriter
    """
    from databricks.sdk import WorkspaceClient
    
    w = WorkspaceClient()
    server_hostname = w.config.host.replace("https://", "").rstrip("/")
    access_token = w.config.token
    
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
    
    return DatabricksWriter(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token,
        catalog=catalog,
        schema=schema,
    )

