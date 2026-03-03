"""
Deduplication logic for brickbyte.

Uses MERGE to remove duplicate records based on user-specified keys.
"""
import logging
from typing import List

logger = logging.getLogger("brickbyte")


def deduplicate_stream(
    executor,
    table_name: str,
    key_columns: List[str],
    run_id_col: str,
    extracted_at_col: str,
    record_id_col: str,
    flatten: bool = True,
    dk_missing_col: str = "_dk_missing",
):
    """
    Deduplicate a stream's table using MERGE.

    Keeps the row with the latest extracted_at per unique key combo.
    On timestamp ties, breaks by record_id (lexicographic max).
    Records with _dk_missing=true are excluded from dedup.

    Args:
        executor: Writer instance with _execute (SQL) or spark (Spark) attribute
        table_name: Fully qualified table name
        key_columns: Columns to use as dedup keys
        run_id_col: Name of the run_id column
        extracted_at_col: Name of the extracted_at column
        record_id_col: Name of the record_id column
        flatten: Whether the table is in flatten mode
        dk_missing_col: Name of the dk_missing indicator column
    """
    if not key_columns:
        return

    key_match = " AND ".join(
        f"t.`{col}` <=> s.`{col}`" for col in key_columns
    )

    # Build the dedup MERGE statement
    # This keeps only the latest record per key combo
    merge_sql = f"""
    MERGE INTO {table_name} t
    USING (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY {', '.join(f'`{c}`' for c in key_columns)}
            ORDER BY `{extracted_at_col}` DESC, `{record_id_col}` DESC
        ) AS _rn
        FROM {table_name}
        WHERE `{dk_missing_col}` = false
    ) s
    ON {key_match}
       AND t.`{record_id_col}` = s.`{record_id_col}`
    WHEN MATCHED AND s._rn > 1 THEN DELETE
    """

    _execute_sql(executor, merge_sql)


def _execute_sql(executor, sql: str):
    """Execute SQL via whatever executor is available."""
    if executor is None:
        # Try Spark
        try:
            from pyspark.sql import SparkSession

            spark = SparkSession.getActiveSession()
            if spark:
                spark.sql(sql)
                return
        except ImportError:
            pass
        raise RuntimeError("No executor available for dedup SQL")

    if hasattr(executor, "spark"):
        executor.spark.sql(sql)
    elif hasattr(executor, "_execute"):
        executor._execute(sql)
    else:
        raise RuntimeError(f"Unknown executor type: {type(executor)}")
