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
    """Manages incremental sync state in a Delta table."""

    def __init__(self, catalog: str, schema: str):
        self.catalog = catalog
        self.schema = schema
        self._state_table = f"`{catalog}`.`{schema}`.`{STATE_TABLE_SUFFIX}`"
        self._spark = None
        self._connection = None
        self._initialized = False

    def _ensure_table(self):
        """Create the state table if it doesn't exist."""
        if self._initialized:
            return

        spark = self._get_spark()
        if spark:
            spark.sql(STATE_TABLE_DDL.format(table_name=self._state_table))
        else:
            raise RuntimeError(
                "StateManager requires either an active SparkSession or "
                "a SQL connection to manage state."
            )
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

    def save_state(self, source: str, stream_name: str, state: dict, run_id: str):
        """Save state for a (source, stream) pair via MERGE upsert."""
        self._ensure_table()
        state_json = json.dumps(state, default=str)

        spark = self._get_spark()
        if spark:
            spark.sql(
                UPSERT_STATE_SQL.format(table_name=self._state_table),
                args={
                    "source": source,
                    "stream_name": stream_name,
                    "state": state_json,
                    "run_id": run_id,
                },
            )
        else:
            raise RuntimeError("StateManager requires Spark for state management.")

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

        raise RuntimeError("StateManager requires Spark for state management.")

    def clear_state(self, source: str, stream_name: str):
        """Delete state for a (source, stream) pair."""
        self._ensure_table()

        spark = self._get_spark()
        if spark:
            spark.sql(
                f"DELETE FROM {self._state_table} "
                f"WHERE source = '{source}' AND stream_name = '{stream_name}'"
            )
