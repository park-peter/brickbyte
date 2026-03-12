"""
SQL Streaming writer for brickbyte using PyArrow buffering and COPY INTO.

Uses micro-batch streaming for:
- Bounded memory usage (flushes at configurable thresholds)
- Fault tolerance (each flush = implicit checkpoint)
- Databricks auto-optimize handles small file compaction
"""
import json
import logging
import os
import shutil
import tempfile
from datetime import datetime, timezone
from typing import Dict, List, Optional
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from brickbyte._schema import DK_MISSING, RAW_TABLE_DDL
from brickbyte.writers.base import BaseWriter

logger = logging.getLogger(__name__)


class SQLStreamingWriter(BaseWriter):
    """
    Writes data to Databricks using micro-batch streaming via SQL Connector.
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
        flatten: bool = False,
        run_id: str = "",
        dedup_keys: Optional[Dict[str, List[str]]] = None,
    ):
        super().__init__(catalog, schema, run_id=run_id, dedup_keys=dedup_keys)
        self.staging_volume = staging_volume
        self.server_hostname = server_hostname
        self.http_path = http_path
        self._access_token = access_token
        self.flatten = flatten

        self.buffer_size_records = buffer_size_records
        self.buffer_size_bytes = buffer_size_mb * 1024 * 1024

        self._connection = None
        self._buffers: Dict[str, List[dict]] = {}
        self._buffer_counts: Dict[str, int] = {}
        self._buffer_sizes: Dict[str, int] = {}
        self._batch_index: int = 0
        self._overwrite_streams: Dict[str, str] = {}
        self._local_staging_root = tempfile.mkdtemp(prefix="brickbyte-sql-")

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
                staging_allowed_local_path=self._local_staging_root,
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
        """Get local staging directory path for parquet generation."""
        from brickbyte._sanitize import sanitize_stream_name

        sanitized = sanitize_stream_name(stream_name)
        stream_dir = os.path.join(self._local_staging_root, sanitized)
        os.makedirs(stream_dir, exist_ok=True)
        return stream_dir

    def _get_volume_dir(self, stream_name: str) -> str:
        """Get destination directory path inside the Unity Catalog Volume."""
        from brickbyte._sanitize import sanitize_stream_name

        sanitized = sanitize_stream_name(stream_name)
        return f"/Volumes/{self._vol_subpath}/brickbyte_streaming/{self.run_id}/{sanitized}"

    def table_exists(self, stream_name: str) -> bool:
        table_name = self.get_table_name(stream_name)
        try:
            self._execute(f"DESCRIBE TABLE {table_name}")
            return True
        except Exception:
            return False

    def get_table_schema(self, stream_name: str) -> Optional[Dict[str, str]]:
        if not self.table_exists(stream_name):
            return None

        table_name = self.get_table_name(stream_name)
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(f"DESCRIBE TABLE {table_name}")
            results = cursor.fetchall()
            return {row[0]: row[1] for row in results}
        finally:
            cursor.close()

    def drop_table(self, stream_name: str):
        table_name = self.get_table_name(stream_name)
        self._execute(f"DROP TABLE IF EXISTS {table_name}")

    def _transform_record(self, stream_name: str, record: dict) -> dict:
        """Transform record based on flatten mode."""
        if self.flatten:
            transformed = dict(record)
            transformed["_record_id"] = str(uuid4())
            transformed["_extracted_at"] = datetime.now(timezone.utc)
            transformed["_run_id"] = self.run_id

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

        # Check both thresholds
        if (
            self._buffer_counts[stream_name] >= self.buffer_size_records
            or self._buffer_sizes[stream_name] >= self.buffer_size_bytes
        ):
            self.flush_stream(stream_name)

    def _get_write_table_name(self, stream_name: str) -> str:
        """Get the table name to write to."""
        if stream_name in self._overwrite_streams:
            return self._overwrite_streams[stream_name]
        return self.get_table_name(stream_name)

    def flush_stream(self, stream_name: str):
        """Flush buffer for a specific stream."""
        if stream_name not in self._buffers or not self._buffers[stream_name]:
            return

        records = self._buffers[stream_name]
        table_name = self._get_write_table_name(stream_name)

        local_staging_dir = self._get_staging_dir(stream_name)
        volume_staging_dir = self._get_volume_dir(stream_name)
        filename = f"{self.run_id}_{self._batch_index:06d}.parquet"
        file_path = os.path.join(local_staging_dir, filename)
        volume_file_path = f"{volume_staging_dir}/{filename}"
        self._batch_index += 1

        try:
            table = pa.Table.from_pylist(records)
            pq.write_table(table, file_path, compression="zstd")

            if not self.flatten:
                create_query = RAW_TABLE_DDL.format(table_name=table_name)
                self._execute(create_query)
            else:
                # Flatten first-write: infer DDL from PyArrow schema
                if not self._table_exists_by_name(table_name):
                    ddl = self._infer_ddl_from_arrow(table.schema, table_name)
                    self._execute(ddl)

            self._execute(f"PUT '{file_path}' INTO '{volume_file_path}' OVERWRITE")
            copy_query = f"""
            COPY INTO {table_name}
            FROM '{volume_file_path}'
            FILEFORMAT = PARQUET
            FORMAT_OPTIONS ('mergeSchema' = 'true')
            """
            self._execute(copy_query)

        except Exception as e:
            logger.error(f"Error flushing stream {stream_name}: {e}")
            raise
        finally:
            try:
                self._execute(f"REMOVE '{volume_file_path}'")
            except Exception:
                pass
            # Always clean up the parquet file
            if os.path.exists(file_path):
                os.remove(file_path)

        # Reset buffer
        self._buffers[stream_name] = []
        self._buffer_counts[stream_name] = 0
        self._buffer_sizes[stream_name] = 0

    def _table_exists_by_name(self, table_name: str) -> bool:
        """Check if a table exists by its full quoted name."""
        try:
            self._execute(f"DESCRIBE TABLE {table_name}")
            return True
        except Exception:
            return False

    def _infer_ddl_from_arrow(self, arrow_schema: pa.Schema, table_name: str) -> str:
        """Generate CREATE TABLE DDL from a PyArrow schema."""
        _TYPE_MAP = {
            pa.string(): "STRING",
            pa.int64(): "BIGINT",
            pa.int32(): "INT",
            pa.float64(): "DOUBLE",
            pa.float32(): "FLOAT",
            pa.bool_(): "BOOLEAN",
            pa.date32(): "DATE",
        }

        columns = []
        for field in arrow_schema:
            sql_type = _TYPE_MAP.get(field.type, "STRING")
            if pa.types.is_timestamp(field.type):
                sql_type = "TIMESTAMP"
            columns.append(f"  `{field.name}` {sql_type}")

        cols_ddl = ",\n".join(columns)
        return f"CREATE TABLE IF NOT EXISTS {table_name} (\n{cols_ddl}\n)"

    def close(self):
        """Flush all remaining buffers and close connection."""
        for stream_name in list(self._buffers.keys()):
            self.flush_stream(stream_name)

            try:
                staging_dir = self._get_staging_dir(stream_name)
                if os.path.exists(staging_dir):
                    shutil.rmtree(staging_dir)
            except Exception:
                pass

        if self._connection:
            self._connection.close()
            self._connection = None

        if os.path.exists(self._local_staging_root):
            shutil.rmtree(self._local_staging_root, ignore_errors=True)

    def safe_overwrite_begin(self, stream_name: str, run_id: str):
        """Begin safe overwrite — redirect writes to staging table."""
        staging_name = self.get_staging_table_name(stream_name, run_id)
        self._overwrite_streams[stream_name] = staging_name
        try:
            self._execute(f"DROP TABLE IF EXISTS {staging_name}")
        except Exception:
            pass

    def safe_overwrite_finish(self, stream_name: str, run_id: str):
        """Finish safe overwrite — atomic swap from staging to target."""
        staging_name = self.get_staging_table_name(stream_name, run_id)
        target_name = self.get_table_name(stream_name)

        try:
            target_exists = self._table_exists_by_name(target_name)

            if target_exists:
                self._atomic_overwrite_sql(target_name, staging_name)
                self._execute(f"DROP TABLE IF EXISTS {staging_name}")
            else:
                self._execute(
                    f"ALTER TABLE {staging_name} RENAME TO {target_name}"
                )
        except Exception:
            self._execute(f"DROP TABLE IF EXISTS {staging_name}")
            raise
        finally:
            self._overwrite_streams.pop(stream_name, None)

    # Safe widening pairs: (narrower, wider) — SQL type names (lowercase)
    _SAFE_WIDENINGS_SQL = {
        ("int", "bigint"),
        ("int", "double"),
        ("bigint", "double"),
        ("float", "double"),
        ("smallint", "int"),
        ("smallint", "bigint"),
        ("tinyint", "smallint"),
        ("tinyint", "int"),
        ("tinyint", "bigint"),
    }

    def _atomic_overwrite_sql(self, target_name: str, staging_name: str):
        """Perform atomic INSERT OVERWRITE via SQL with schema alignment and type checks."""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(f"DESCRIBE TABLE {target_name}")
            target_schema = {row[0]: row[1] for row in cursor.fetchall()}

            cursor.execute(f"DESCRIBE TABLE {staging_name}")
            staging_schema = {row[0]: row[1] for row in cursor.fetchall()}
        finally:
            cursor.close()

        target_cols = set(target_schema.keys())
        staging_cols = set(staging_schema.keys())

        # Check for incompatible type changes
        for col in target_cols & staging_cols:
            t_type = target_schema[col].lower()
            s_type = staging_schema[col].lower()
            if t_type != s_type:
                pair = (s_type, t_type)
                reverse = (t_type, s_type)
                is_safe = pair in self._SAFE_WIDENINGS_SQL
                is_reverse_safe = reverse in self._SAFE_WIDENINGS_SQL
                if not is_safe and not is_reverse_safe:
                    if t_type != "string" and s_type != "string":
                        raise ValueError(
                            f"Incompatible type change for column '{col}': "
                            f"{target_schema[col]} -> {staging_schema[col]}. "
                            f"Drop the table manually to reset schema."
                        )

        # Add new columns from staging to target
        new_cols = staging_cols - target_cols
        for col in new_cols:
            col_type = staging_schema[col]
            self._execute(
                f"ALTER TABLE {target_name} ADD COLUMNS (`{col}` {col_type})"
            )

        all_cols = target_cols | staging_cols
        select_parts = []
        for col in sorted(all_cols):
            if col in staging_cols and col in target_cols:
                s_type = staging_schema[col].lower()
                t_type = target_schema[col].lower()
                if s_type != t_type:
                    # Always widen to the wider type
                    if (s_type, t_type) in self._SAFE_WIDENINGS_SQL:
                        select_parts.append(
                            f"CAST(`{col}` AS {target_schema[col]}) AS `{col}`"
                        )
                    elif (t_type, s_type) in self._SAFE_WIDENINGS_SQL:
                        # Staging is wider — widen target to match
                        self._execute(
                            f"ALTER TABLE {target_name} "
                            f"ALTER COLUMN `{col}` TYPE {staging_schema[col]}"
                        )
                        select_parts.append(f"`{col}`")
                    else:
                        select_parts.append(f"`{col}`")
                else:
                    select_parts.append(f"`{col}`")
            elif col in staging_cols:
                select_parts.append(f"`{col}`")
            else:
                select_parts.append(f"NULL AS `{col}`")

        col_list = ", ".join(f"`{c}`" for c in sorted(all_cols))
        select_expr = ", ".join(select_parts)

        self._execute(
            f"INSERT OVERWRITE {target_name} ({col_list}) "
            f"SELECT {select_expr} FROM {staging_name}"
        )
