"""
SQL Streaming writer for BrickByte using PyArrow buffering and COPY INTO.

Uses micro-batch streaming for:
- Bounded memory usage (flushes at configurable thresholds)
- Fault tolerance (each flush = implicit checkpoint)
- Databricks auto-optimize handles small file compaction
"""
import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from brickbyte.writers.base import BaseWriter

logger = logging.getLogger(__name__)


class SQLStreamingWriter(BaseWriter):
    """
    Writes data to Databricks using micro-batch streaming via SQL Connector.
    
    Each flush writes to Delta immediately via COPY INTO, providing:
    - Implicit checkpointing (resume from last successful batch on failure)
    - Bounded memory (configurable batch size)
    - Databricks auto-optimize handles small file compaction
    """

    def __init__(
        self,
        catalog: str,
        schema: str,
        staging_volume: str,
        server_hostname: str,
        http_path: str,
        access_token: str,
        buffer_size_records: int = 50000,
        buffer_size_mb: int = 100,
    ):
        """
        Initialize SQL Streaming Writer.
        
        Args:
            catalog: Unity Catalog name
            schema: Target schema name
            staging_volume: Unity Catalog Volume path for staging parquet files
            server_hostname: Databricks server hostname
            http_path: SQL Warehouse HTTP path
            access_token: Databricks access token
            buffer_size_records: Records per micro-batch (default: 50k)
            buffer_size_mb: Max batch size in MB (default: 100MB)
        """
        super().__init__(catalog, schema)
        self.staging_volume = staging_volume
        self.server_hostname = server_hostname
        self.http_path = http_path
        self._access_token = access_token
        
        self.buffer_size_records = buffer_size_records
        self.buffer_size_bytes = buffer_size_mb * 1024 * 1024

        self._connection = None
        self._buffers: Dict[str, List[dict]] = {}
        self._buffer_counts: Dict[str, int] = {}
        self._buffer_sizes: Dict[str, int] = {}  # Track approx byte size
        
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
                access_token=self._access_token,
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
            "_airbyte_data": json.dumps(record, default=str)
        }

    def write_record(self, stream_name: str, record: dict):
        """Buffer a single record."""
        if stream_name not in self._buffers:
            self._buffers[stream_name] = []
            self._buffer_counts[stream_name] = 0
            self._buffer_sizes[stream_name] = 0
            
        transformed = self._transform_record(record)
        self._buffers[stream_name].append(transformed)
        self._buffer_counts[stream_name] += 1
        self._buffer_sizes[stream_name] += sys.getsizeof(transformed.get("_airbyte_data", ""))
        
        # Check both thresholds
        if (self._buffer_counts[stream_name] >= self.buffer_size_records or
                self._buffer_sizes[stream_name] >= self.buffer_size_bytes):
            self.flush_stream(stream_name)

    def flush_stream(self, stream_name: str):
        """Flush buffer for a specific stream."""
        if stream_name not in self._buffers or not self._buffers[stream_name]:
            return

        records = self._buffers[stream_name]
        
        try:
            table = pa.Table.from_pylist(records)
            
            staging_dir = self._get_staging_dir(stream_name)
            filename = f"batch_{datetime.now().strftime('%Y%m%d%H%M%S%f')}.parquet"
            file_path = os.path.join(staging_dir, filename)
            
            pq.write_table(table, file_path, compression='zstd')
            
            table_name = self.get_table_name(stream_name)
            
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
            
            os.remove(file_path)
            
        except Exception as e:
            logger.error(f"Error flushing stream {stream_name}: {e}")
            raise
        
        # Reset buffer
        self._buffers[stream_name] = []
        self._buffer_counts[stream_name] = 0
        self._buffer_sizes[stream_name] = 0



    def close(self):
        """Flush all remaining buffers and close connection."""
        for stream_name in list(self._buffers.keys()):
            self.flush_stream(stream_name)

            try:
                # Cleanup staging directory if empty
                staging_dir = self._get_staging_dir(stream_name)
                if os.path.exists(staging_dir):
                     # os.rmdir only works if empty
                     os.rmdir(staging_dir)
            except Exception:
                pass
            
        if self._connection:
            self._connection.close()
            self._connection = None
