"""
Spark Streaming writer for brickbyte using native Databricks/Spark execution.

Uses micro-batch streaming for:
- Bounded memory usage (flushes at configurable thresholds)
- Fault tolerance (each flush = implicit checkpoint)
- Databricks auto-optimize handles small file compaction
"""
import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional
from uuid import uuid4

from brickbyte._schema import DK_MISSING
from brickbyte.writers.base import BaseWriter

logger = logging.getLogger(__name__)


class SparkStreamingWriter(BaseWriter):
    """
    Writes data to Databricks using micro-batch streaming.
    """

    def __init__(
        self,
        catalog: str,
        schema: str,
        buffer_size_records: int = 50000,
        buffer_size_mb: int = 100,
        flatten: bool = False,
        run_id: str = "",
        dedup_keys: Optional[Dict[str, List[str]]] = None,
    ):
        super().__init__(catalog, schema, run_id=run_id, dedup_keys=dedup_keys)
        self.buffer_size_records = buffer_size_records
        self.buffer_size_bytes = buffer_size_mb * 1024 * 1024
        self.flatten = flatten

        self._spark = None
        self._buffers: Dict[str, List[dict]] = {}
        self._buffer_counts: Dict[str, int] = {}
        self._buffer_sizes: Dict[str, int] = {}
        self._overwrite_streams: Dict[str, str] = {}  # stream_name -> staging_table_name

    @property
    def spark(self):
        """Get or create Spark session."""
        if self._spark is None:
            from pyspark.sql import SparkSession

            self._spark = SparkSession.builder.getOrCreate()
        return self._spark

    def table_exists(self, stream_name: str) -> bool:
        table_name = self.get_table_name(stream_name)
        return self.spark.catalog.tableExists(table_name)

    def get_table_schema(self, stream_name: str) -> Optional[Dict[str, str]]:
        if not self.table_exists(stream_name):
            return None

        table_name = self.get_table_name(stream_name)
        df = self.spark.table(table_name)
        return {f.name: str(f.dataType) for f in df.schema.fields}

    def drop_table(self, stream_name: str):
        table_name = self.get_table_name(stream_name)
        self.spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    def _transform_record(self, stream_name: str, record: dict) -> dict:
        """Transform record based on flatten mode."""
        if self.flatten:
            transformed = dict(record)
            transformed["_record_id"] = str(uuid4())
            transformed["_extracted_at"] = datetime.now(timezone.utc)
            transformed["_run_id"] = self.run_id

            # Add dedup key columns if configured
            dk_keys = self._get_dedup_keys_for_stream(stream_name)
            if dk_keys is not None:
                dk_missing = False
                for i, key in enumerate(dk_keys):
                    if key in record:
                        transformed[f"_dk_{i}"] = record[key]
                    else:
                        transformed[f"_dk_{i}"] = None
                        dk_missing = True
                transformed[DK_MISSING] = dk_missing

            return transformed
        else:
            transformed = {
                "record_id": str(uuid4()),
                "extracted_at": datetime.now(timezone.utc),
                "data": json.dumps(record, default=str),
                "run_id": self.run_id,
            }

            # Add dedup key columns if configured
            dk_keys = self._get_dedup_keys_for_stream(stream_name)
            if dk_keys is not None:
                dk_missing = False
                for i, key in enumerate(dk_keys):
                    if key in record:
                        transformed[f"_dk_{i}"] = record[key]
                    else:
                        transformed[f"_dk_{i}"] = None
                        dk_missing = True
                transformed[DK_MISSING] = dk_missing

            return transformed

    def write_record(self, stream_name: str, record: dict):
        """Buffer a single record."""
        if stream_name not in self._buffers:
            self._buffers[stream_name] = []
            self._buffer_counts[stream_name] = 0
            self._buffer_sizes[stream_name] = 0

        transformed = self._transform_record(stream_name, record)
        self._buffers[stream_name].append(transformed)
        self._buffer_counts[stream_name] += 1

        # Estimate size
        if self.flatten:
            self._buffer_sizes[stream_name] += sum(
                len(str(v).encode("utf-8")) for v in transformed.values()
            )
        else:
            self._buffer_sizes[stream_name] += len(
                transformed["data"].encode("utf-8")
            )

        # Flush micro-batch when thresholds hit
        if (
            self._buffer_counts[stream_name] >= self.buffer_size_records
            or self._buffer_sizes[stream_name] >= self.buffer_size_bytes
        ):
            self._write_micro_batch(stream_name)

    def _get_write_table_name(self, stream_name: str) -> str:
        """Get the table name to write to (staging during overwrite, target otherwise)."""
        if stream_name in self._overwrite_streams:
            return self._overwrite_streams[stream_name]
        return self.get_table_name(stream_name)

    def _write_micro_batch(self, stream_name: str):
        """Write a micro-batch to Delta."""
        if stream_name not in self._buffers or not self._buffers[stream_name]:
            return

        records = self._buffers[stream_name]
        batch_count = len(records)
        table_name = self._get_write_table_name(stream_name)

        try:
            df = self.spark.createDataFrame(records)
            (
                df.write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(table_name)
            )

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
        for stream_name in list(self._buffers.keys()):
            self.flush_stream(stream_name)

    def safe_overwrite_begin(self, stream_name: str, run_id: str):
        """Begin safe overwrite — redirect writes to staging table."""
        staging_name = self.get_staging_table_name(stream_name, run_id)
        self._overwrite_streams[stream_name] = staging_name
        # Drop any leftover staging table
        self.spark.sql(f"DROP TABLE IF EXISTS {staging_name}")

    def safe_overwrite_finish(self, stream_name: str, run_id: str):
        """Finish safe overwrite — atomic swap from staging to target."""
        staging_name = self.get_staging_table_name(stream_name, run_id)
        target_name = self.get_table_name(stream_name)

        try:
            target_exists = self.spark.catalog.tableExists(target_name)

            if target_exists:
                self._atomic_overwrite(target_name, staging_name)
                self.spark.sql(f"DROP TABLE IF EXISTS {staging_name}")
            else:
                self.spark.sql(
                    f"ALTER TABLE {staging_name} RENAME TO {target_name}"
                )
        except Exception:
            # On failure, drop staging table, target untouched
            self.spark.sql(f"DROP TABLE IF EXISTS {staging_name}")
            raise
        finally:
            self._overwrite_streams.pop(stream_name, None)

    def _atomic_overwrite(self, target_name: str, staging_name: str):
        """Perform atomic INSERT OVERWRITE with schema alignment."""
        target_df = self.spark.table(target_name)
        staging_df = self.spark.table(staging_name)

        target_schema = {f.name: str(f.dataType) for f in target_df.schema.fields}
        staging_schema = {f.name: str(f.dataType) for f in staging_df.schema.fields}

        target_cols = set(target_schema.keys())
        staging_cols = set(staging_schema.keys())

        # Check for incompatible type changes
        _SAFE_WIDENINGS = {
            ("IntegerType", "LongType"),
            ("IntegerType", "DoubleType"),
            ("LongType", "DoubleType"),
            ("FloatType", "DoubleType"),
            ("ShortType", "IntegerType"),
            ("ShortType", "LongType"),
            ("ByteType", "ShortType"),
            ("ByteType", "IntegerType"),
            ("ByteType", "LongType"),
        }

        for col in target_cols & staging_cols:
            t_type = target_schema[col]
            s_type = staging_schema[col]
            if t_type != s_type:
                if (s_type, t_type) not in _SAFE_WIDENINGS and (
                    t_type,
                    s_type,
                ) not in _SAFE_WIDENINGS:
                    # Check if one can be cast to the other
                    if t_type != "StringType" and s_type != "StringType":
                        raise ValueError(
                            f"Incompatible type change for column '{col}': "
                            f"{t_type} -> {s_type}. "
                            f"Drop the table manually to reset schema."
                        )

        # Add new columns from staging to target
        new_cols = staging_cols - target_cols
        for col in new_cols:
            col_type = staging_schema[col]
            self.spark.sql(
                f"ALTER TABLE {target_name} ADD COLUMNS (`{col}` {col_type})"
            )

        # Build SELECT for INSERT OVERWRITE with all columns
        all_cols = target_cols | staging_cols
        select_parts = []
        for col in sorted(all_cols):
            if col in staging_cols and col in target_cols:
                s_type = staging_schema[col]
                t_type = target_schema[col]
                if s_type != t_type and (s_type, t_type) in _SAFE_WIDENINGS:
                    select_parts.append(f"CAST(`{col}` AS {t_type}) AS `{col}`")
                elif s_type != t_type and (t_type, s_type) in _SAFE_WIDENINGS:
                    select_parts.append(f"CAST(`{col}` AS {s_type}) AS `{col}`")
                else:
                    select_parts.append(f"`{col}`")
            elif col in staging_cols:
                select_parts.append(f"`{col}`")
            else:
                select_parts.append(f"NULL AS `{col}`")

        col_list = ", ".join(f"`{c}`" for c in sorted(all_cols))
        select_expr = ", ".join(select_parts)

        self.spark.sql(
            f"INSERT OVERWRITE {target_name} ({col_list}) "
            f"SELECT {select_expr} FROM {staging_name}"
        )
