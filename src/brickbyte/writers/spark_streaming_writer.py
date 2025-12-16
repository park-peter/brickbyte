"""
Spark Streaming writer for BrickByte using native Databricks/Spark execution.

Uses micro-batch streaming for:
- Bounded memory usage (flushes at configurable thresholds)
- Fault tolerance (each flush = implicit checkpoint)
- Databricks auto-optimize handles small file compaction
"""
import json
import logging
import sys
from datetime import datetime
from typing import Dict, List, Optional
from uuid import uuid4

from brickbyte.writers.base import BaseWriter

logger = logging.getLogger(__name__)


class SparkStreamingWriter(BaseWriter):
    """
    Writes data to Databricks using micro-batch streaming.
    
    Each flush writes to Delta immediately, providing:
    - Implicit checkpointing (resume from last successful batch on failure)
    - Bounded memory (configurable batch size)
    - Databricks auto-optimize handles small file compaction
    """

    def __init__(
        self,
        catalog: str,
        schema: str,
        buffer_size_records: int = 50000,
        buffer_size_mb: int = 100,
    ):
        """
        Initialize Spark Streaming Writer.
        
        Args:
            catalog: Unity Catalog name
            schema: Target schema name
            buffer_size_records: Records per micro-batch (default: 50k)
            buffer_size_mb: Max batch size in MB (default: 100MB)
        """
        super().__init__(catalog, schema)
        self.buffer_size_records = buffer_size_records
        self.buffer_size_bytes = buffer_size_mb * 1024 * 1024

        self._spark = None
        self._buffers: Dict[str, List[dict]] = {}
        self._buffer_counts: Dict[str, int] = {}
        self._buffer_sizes: Dict[str, int] = {}

    @property
    def spark(self):
        """Get or create Spark session."""
        if self._spark is None:
            from pyspark.sql import SparkSession
            self._spark = SparkSession.builder.getOrCreate()
        return self._spark

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
        self._buffer_sizes[stream_name] += sys.getsizeof(
            transformed.get("_airbyte_data", "")
        )
        
        # Flush micro-batch when thresholds hit
        if (self._buffer_counts[stream_name] >= self.buffer_size_records or
                self._buffer_sizes[stream_name] >= self.buffer_size_bytes):
            self._write_micro_batch(stream_name)

    def _write_micro_batch(self, stream_name: str):
        """Write a micro-batch to Delta (each call = implicit checkpoint)."""
        if stream_name not in self._buffers or not self._buffers[stream_name]:
            return

        records = self._buffers[stream_name]
        batch_count = len(records)
        table_name = self.get_table_name(stream_name)
        
        try:
            df = self.spark.createDataFrame(records)
            (df.write
               .format("delta")
               .mode("append")
               .option("mergeSchema", "true")
               .saveAsTable(table_name))
            
            logger.debug("Wrote %d records to %s", batch_count, table_name)
            
        except Exception as e:
            logger.error("Error writing batch for %s: %s", stream_name, e)
            raise
        
        # Reset buffer
        self._buffers[stream_name] = []
        self._buffer_counts[stream_name] = 0
        self._buffer_sizes[stream_name] = 0

    def flush_stream(self, stream_name: str):
        """Flush any remaining buffered records to Delta."""
        self._write_micro_batch(stream_name)

    def close(self):
        """Flush all remaining buffers."""
        for stream_name in self._buffers:
            self.flush_stream(stream_name)
