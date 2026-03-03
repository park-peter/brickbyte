"""
Incremental sync state management for brickbyte.

Manages the `__brickbyte_state` table to track sync state per (source, stream) pair.
"""
import json
import logging
from typing import Optional

logger = logging.getLogger("brickbyte")

STATE_TABLE_SUFFIX = "__brickbyte_state"

STATE_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {table_name} (
    source STRING,
    stream_name STRING,
    state STRING,
    run_id STRING,
    updated_at TIMESTAMP
)
"""

UPSERT_STATE_SPARK_SQL = """
MERGE INTO {table_name} t
USING (SELECT :source AS source, :stream_name AS stream_name,
              :state AS state, :run_id AS run_id,
              current_timestamp() AS updated_at) s
ON t.source = s.source AND t.stream_name = s.stream_name
WHEN MATCHED THEN UPDATE SET
    t.state = s.state, t.run_id = s.run_id, t.updated_at = s.updated_at
WHEN NOT MATCHED THEN INSERT (source, stream_name, state, run_id, updated_at)
    VALUES (s.source, s.stream_name, s.state, s.run_id, s.updated_at)
"""

# SQL connector uses named params with :name syntax
UPSERT_STATE_SQL = """
MERGE INTO {table_name} t
USING (SELECT :source AS source, :stream_name AS stream_name,
              :state AS state, :run_id AS run_id,
              current_timestamp() AS updated_at) s
ON t.source = s.source AND t.stream_name = s.stream_name
WHEN MATCHED THEN UPDATE SET
    t.state = s.state, t.run_id = s.run_id, t.updated_at = s.updated_at
WHEN NOT MATCHED THEN INSERT (source, stream_name, state, run_id, updated_at)
    VALUES (s.source, s.stream_name, s.state, s.run_id, s.updated_at)
"""


class StateManager:
    """Manages incremental sync state in a Delta table.

    Works with both Spark (when active) and the SQL connector (when
    staging_volume / warehouse_id are provided, i.e. remote mode).
    """

    def __init__(
        self,
        catalog: str,
        schema: str,
        staging_volume: Optional[str] = None,
        warehouse_id: Optional[str] = None,
    ):
        self.catalog = catalog
        self.schema = schema
        self._state_table = f"`{catalog}`.`{schema}`.`{STATE_TABLE_SUFFIX}`"
        self._spark = None
        self._connection = None
        self._initialized = False
        self._staging_volume = staging_volume
        self._warehouse_id = warehouse_id

    def _ensure_table(self):
        """Create the state table if it doesn't exist."""
        if self._initialized:
            return

        ddl = STATE_TABLE_DDL.format(table_name=self._state_table)
        spark = self._get_spark()
        if spark:
            spark.sql(ddl)
        else:
            self._sql_execute(ddl)
        self._initialized = True

    def _get_spark(self):
        """Get Spark session if available."""
        if self._spark is None:
            try:
                from pyspark.sql import SparkSession

                self._spark = SparkSession.getActiveSession()
            except ImportError:
                pass
        return self._spark

    def _get_connection(self):
        """Get or create a SQL connector connection for remote mode."""
        if self._connection is not None:
            return self._connection

        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        server_hostname = w.config.host.replace("https://", "").rstrip("/")
        access_token = w.config.token

        wh_id = self._warehouse_id
        if not wh_id:
            warehouses = list(w.warehouses.list())
            running = [
                wh for wh in warehouses if wh.state and wh.state.value == "RUNNING"
            ]
            if running:
                wh_id = running[0].id
            else:
                raise RuntimeError(
                    "No running SQL warehouse found for state management. "
                    "Provide warehouse_id or start a warehouse."
                )

        from databricks import sql

        self._connection = sql.connect(
            server_hostname=server_hostname,
            http_path=f"/sql/1.0/warehouses/{wh_id}",
            access_token=access_token,
            catalog=self.catalog,
            schema=self.schema,
        )
        return self._connection

    def _sql_execute(self, query: str, params: Optional[dict] = None):
        """Execute a query via SQL connector."""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.fetchall() if cursor.description else []
        finally:
            cursor.close()

    def save_state(self, source: str, stream_name: str, state: dict, run_id: str):
        """Save state for a (source, stream) pair via MERGE upsert."""
        self._ensure_table()
        state_json = json.dumps(state, default=str)

        params = {
            "source": source,
            "stream_name": stream_name,
            "state": state_json,
            "run_id": run_id,
        }

        spark = self._get_spark()
        if spark:
            spark.sql(
                UPSERT_STATE_SPARK_SQL.format(table_name=self._state_table),
                args=params,
            )
        else:
            self._sql_execute(
                UPSERT_STATE_SQL.format(table_name=self._state_table),
                params,
            )

    def get_state(self, source: str, stream_name: str) -> Optional[dict]:
        """Load state for a (source, stream) pair. Returns None if no state exists."""
        self._ensure_table()

        spark = self._get_spark()
        if spark:
            from pyspark.sql.functions import col

            df = (
                spark.table(self._state_table)
                .filter(
                    (col("source") == source) & (col("stream_name") == stream_name)
                )
                .select("state")
                .limit(1)
            )
            rows = df.collect()
            if rows:
                return json.loads(rows[0]["state"])
            return None

        rows = self._sql_execute(
            f"SELECT state FROM {self._state_table} "
            f"WHERE source = :source AND stream_name = :stream_name LIMIT 1",
            {"source": source, "stream_name": stream_name},
        )
        if rows:
            return json.loads(rows[0][0])
        return None

    def clear_state(self, source: str, stream_name: str):
        """Delete state for a (source, stream) pair."""
        self._ensure_table()

        spark = self._get_spark()
        if spark:
            spark.sql(
                f"DELETE FROM {self._state_table} "
                f"WHERE source = '{source}' AND stream_name = '{stream_name}'"
            )
        else:
            self._sql_execute(
                f"DELETE FROM {self._state_table} "
                f"WHERE source = :source AND stream_name = :stream_name",
                {"source": source, "stream_name": stream_name},
            )
