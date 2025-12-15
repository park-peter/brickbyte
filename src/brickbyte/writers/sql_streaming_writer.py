"""
SQL Streaming writer for BrickByte using PyArrow buffering and COPY INTO.
"""
import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from brickbyte.writers.base import BaseWriter

logger = logging.getLogger(__name__)

class SQLStreamingWriter(BaseWriter):
    """
    Writes data to Databricks using streaming micro-batches via SQL Connector.
    
    Buffers records in memory using PyArrow, writes micro-batches to
    Volumes as Parquet files, and loads them using COPY INTO.
    """

    def __init__(
        self,
        catalog: str,
        schema: str,
        staging_volume: str,
        server_hostname: str,
        http_path: str,
        access_token: str,
        buffer_size_records: int = 10000,
        buffer_size_mb: int = 50,
    ):
        """
        Initialize the SQL Streaming writer.

        Args:
            catalog: Unity Catalog name
            schema: Target schema name
            staging_volume: Unity Catalog Volume path (format: catalog.schema.volume_name)
            server_hostname: Databricks workspace hostname
            http_path: SQL warehouse HTTP path
            access_token: Access token
            buffer_size_records: Max records before flush
            buffer_size_mb: Max buffer size in MB before flush
        """
        super().__init__(catalog, schema)
        self.staging_volume = staging_volume
        self.server_hostname = server_hostname
        self.http_path = http_path
        self.access_token = access_token
        self.buffer_size_records = buffer_size_records
        self.buffer_size_mb = buffer_size_mb * 1024 * 1024  # Convert to bytes

        self._connection = None
        # buffer structure: {stream_name: [list of records]}
        self._buffers: Dict[str, List[dict]] = {}
        self._buffer_counts: Dict[str, int] = {}
        
        # Verify staging volume format
        parts = self.staging_volume.split(".")
        if len(parts) != 3:
            raise ValueError(
                f"staging_volume must be in format 'catalog.schema.volume_name', "
                f"got: {self.staging_volume}"
            )
        self._vol_subpath = os.path.join(parts[0], parts[1], parts[2])

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
        try:
            cursor.execute(query)
        finally:
            cursor.close()

    def _get_staging_dir(self, stream_name: str) -> str:
        """Get staging directory path in Volume."""
        base_path = f"/Volumes/{self._vol_subpath}"
        stream_dir = os.path.join(base_path, "brickbyte_streaming", stream_name)
        os.makedirs(stream_dir, exist_ok=True)
        return stream_dir

    def table_exists(self, stream_name: str) -> bool:
        """Check if a table exists."""
        table_name = self.get_table_name(stream_name)
        try:
            self._execute(f"DESCRIBE TABLE {table_name}")
            return True
        except Exception:
            return False

    def get_table_schema(self, stream_name: str) -> Optional[Dict[str, str]]:
        """Get schema of an existing table."""
        if not self.table_exists(stream_name):
            return None
        
        table_name = self.get_table_name(stream_name)
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(f"DESCRIBE TABLE {table_name}")
        results = cursor.fetchall()
        cursor.close()
        return {row[0]: row[1] for row in results}

    def drop_table(self, stream_name: str):
        """Drop a table if it exists."""
        table_name = self.get_table_name(stream_name)
        self._execute(f"DROP TABLE IF EXISTS {table_name}")

    def _transform_record(self, record: dict) -> dict:
        """Add Airbyte metadata fields."""
        return {
            "_airbyte_raw_id": str(uuid4()),
            "_airbyte_extracted_at": datetime.now(),
            "_airbyte_data": json.dumps(record)
        }

    def write_record(self, stream_name: str, record: dict):
        """
        Buffer a single record.
        
        Args:
            stream_name: Name of the stream
            record: Dictionary of record data
        """
        if stream_name not in self._buffers:
            self._buffers[stream_name] = []
            self._buffer_counts[stream_name] = 0
            
        # Append transformed record
        # Note: Transformation happens here to be ready for PyArrow
        transformed = self._transform_record(record)
        self._buffers[stream_name].append(transformed)
        self._buffer_counts[stream_name] += 1
        
        # Check thresholds
        if self._buffer_counts[stream_name] >= self.buffer_size_records:
            self.flush_stream(stream_name)

    def flush_stream(self, stream_name: str):
        """Flush buffer for a specific stream."""
        if stream_name not in self._buffers or not self._buffers[stream_name]:
            return

        records = self._buffers[stream_name]
        
        try:
            # 1. Convert to PyArrow Table
            table = pa.Table.from_pylist(records)
            
            # 2. Write to Parquet in Volume
            staging_dir = self._get_staging_dir(stream_name)
            filename = f"batch_{datetime.now().strftime('%Y%m%d%H%M%S%f')}.parquet"
            file_path = os.path.join(staging_dir, filename)
            
            pq.write_table(table, file_path)
            
            # 3. Execute COPY INTO
            table_name = self.get_table_name(stream_name)
            
            # Ensure table exists (create if not exists)
            create_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                _airbyte_raw_id STRING,
                _airbyte_extracted_at TIMESTAMP,
                _airbyte_data STRING
            )
            """
            self._execute(create_query)
            
            copy_query = f"""
            COPY INTO {table_name}
            FROM '{file_path}'
            FILEFORMAT = PARQUET
            FORMAT_OPTIONS ('mergeSchema' = 'true')
            COPY_OPTIONS ('force' = 'true')
            """
            self._execute(copy_query)
            
            # 4. Cleanup
            os.remove(file_path)
            
        except Exception as e:
            logger.error(f"Error flushing stream {stream_name}: {e}")
            raise e
        
        # Reset buffer
        self._buffers[stream_name] = []
        self._buffer_counts[stream_name] = 0

    def write(
        self,
        cache: any,
        streams: List[str],
        mode: str = "overwrite",
        primary_key: Optional[Dict[str, List[str]]] = None,
    ) -> int:
        """
        Write data from cache to Databricks (Compatibility Wrapper).
        """
        total = 0
        self._get_connection() # Ensure connection
        for stream in streams:
            if mode == "overwrite":
                 self.drop_table(stream)
            
            # Read from cache into streaming buffer
            dataset = cache[stream]
            for record in dataset.to_pandas().to_dict(orient="records"):
                self.write_record(stream, record)
                total += 1
            
            self.flush_stream(stream)
            
        return total

    def close(self):
        """Flush all remaining buffers and close connection."""
        for stream_name in list(self._buffers.keys()):
            self.flush_stream(stream_name)
            
        if self._connection:
            self._connection.close()
            self._connection = None
