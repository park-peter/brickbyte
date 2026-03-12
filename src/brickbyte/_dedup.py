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

    from brickbyte._sanitize import validate_identifier

    validated_keys = [validate_identifier(col) for col in key_columns]
    validate_identifier(run_id_col)
    validated_extracted_at_col = validate_identifier(extracted_at_col)
    validated_record_id_col = validate_identifier(record_id_col)
    validated_dk_missing_col = validate_identifier(dk_missing_col)

    key_match = " AND ".join(f"t.`{col}` <=> s.`{col}`" for col in validated_keys)

    # Build the dedup MERGE statement
    # This keeps only the latest record per key combo
    merge_sql = f"""
    MERGE INTO {table_name} t
    USING (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY {', '.join(f'`{c}`' for c in validated_keys)}
            ORDER BY `{validated_extracted_at_col}` DESC, `{validated_record_id_col}` DESC
        ) AS _rn
        FROM {table_name}
        WHERE `{validated_dk_missing_col}` = false
    ) s
    ON {key_match}
       AND t.`{validated_record_id_col}` = s.`{validated_record_id_col}`
    WHEN MATCHED AND s._rn > 1 THEN DELETE
    """

    _execute_sql(executor, merge_sql)


def _execute_sql(executor, sql: str):
    """Execute SQL via the provided executor.

    The executor should be a writer instance that has either a ``spark``
    attribute (SparkStreamingWriter) or an ``_execute`` method
    (SQLStreamingWriter).  Passing ``None`` is a programming error —
    callers must always supply the writer that owns the table.
    """
    if executor is None:
        raise RuntimeError(
            "No executor provided for dedup SQL. "
            "This is a bug — the writer that wrote the table must be passed."
        )

    if hasattr(executor, "spark"):
        executor.spark.sql(sql)
    elif hasattr(executor, "_execute"):
        executor._execute(sql)
    else:
        raise RuntimeError(f"Unknown executor type: {type(executor)}")
