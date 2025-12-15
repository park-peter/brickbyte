"""
Spark Streaming writer for BrickByte using native Databricks/Spark execution.
"""
import json
import logging
import os
import shutil
from datetime import datetime
from typing import Dict, List, Optional
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from brickbyte.writers.base import BaseWriter

logger = logging.getLogger(__name__)

class SparkStreamingWriter(BaseWriter):
    """
    Writes data to Databricks using native Spark streaming micro-batches.
    
    Buffers records in memory using PyArrow, writes micro-batches to
    local temp storage, and loads them using Spark.
    """

    def __init__(
        self,
        catalog: str,
        schema: str,
        buffer_size_records: int = 10000,
        buffer_size_mb: int = 50,
    ):
        """
        Initialize the Spark Streaming writer.

        Args:
            catalog: Unity Catalog name
            schema: Target schema name
            buffer_size_records: Max records before flush
            buffer_size_mb: Max buffer size in MB before flush
        """
        super().__init__(catalog, schema)
        self.buffer_size_records = buffer_size_records
        self.buffer_size_mb = buffer_size_mb * 1024 * 1024  # Convert to bytes

        self._spark = None
        # buffer structure: {stream_name: [list of records]}
        self._buffers: Dict[str, List[dict]] = {}
        self._buffer_counts: Dict[str, int] = {}
        
        # Local temp directory for buffering
        self._temp_dir = "/local_disk0/tmp/brickbyte_spark_streaming"
        os.makedirs(self._temp_dir, exist_ok=True)

    @property
    def spark(self):
        """Get or create Spark session."""
        if self._spark is None:
            from pyspark.sql import SparkSession
            self._spark = SparkSession.builder.getOrCreate()
        return self._spark

    def _get_staging_dir(self, stream_name: str) -> str:
        """Get local staging directory."""
        stream_dir = os.path.join(self._temp_dir, stream_name)
        os.makedirs(stream_dir, exist_ok=True)
        return stream_dir

    def table_exists(self, stream_name: str) -> bool:
        """Check if a table exists."""
        table_name = self.get_table_name(stream_name)
        return self.spark.catalog.tableExists(table_name)

    def get_table_schema(self, stream_name: str) -> Optional[Dict[str, str]]:
        """Get schema of an existing table."""
        if not self.table_exists(stream_name):
            return None
        
        table_name = self.get_table_name(stream_name)
        df = self.spark.table(table_name)
        return {f.name: str(f.dataType) for f in df.schema.fields}

    def drop_table(self, stream_name: str):
        """Drop a table if it exists."""
        table_name = self.get_table_name(stream_name)
        self.spark.sql(f"DROP TABLE IF EXISTS {table_name}")

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
        """
        if stream_name not in self._buffers:
            self._buffers[stream_name] = []
            self._buffer_counts[stream_name] = 0
            
        transformed = self._transform_record(record)
        self._buffers[stream_name].append(transformed)
        self._buffer_counts[stream_name] += 1
        
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
            
            # 2. Write to local Parquet
            staging_dir = self._get_staging_dir(stream_name)
            filename = f"batch_{datetime.now().strftime('%Y%m%d%H%M%S%f')}.parquet"
            file_path = os.path.join(staging_dir, filename)
            
            pq.write_table(table, file_path)
            
            # 3. Load via Spark
            table_name = self.get_table_name(stream_name)
            
            # Read local parquet
            df = self.spark.read.parquet(file_path)
            
            # Write to Delta
            (df.write
               .format("delta")
               .mode("append")
               .option("mergeSchema", "true")
               .saveAsTable(table_name))
            
            # 4. Cleanup
            os.remove(file_path)
            
        except Exception as e:
            logger.error(f"Error flushing stream {stream_name}: {e}")
            raise e
        
        # Reset buffer
        self._buffers[stream_name] = []
        self._buffer_counts[stream_name] = 0

    def write(self, cache: any, streams: List[str], mode: str = "overwrite", **kwargs) -> int:
        """Compatibility wrapper."""
        total = 0
        for stream in streams:
            if mode == "overwrite":
                self.drop_table(stream)
            
            dataset = cache[stream]
            for record in dataset.to_pandas().to_dict(orient="records"):
                self.write_record(stream, record)
                total += 1
            self.flush_stream(stream)
        return total

    def close(self):
        """Flush all remaining buffers."""
        for stream_name in list(self._buffers.keys()):
            self.flush_stream(stream_name)
        
        # Try to cleanup temp dir
        try:
            if os.path.exists(self._temp_dir):
                shutil.rmtree(self._temp_dir)
        except Exception:
            pass
