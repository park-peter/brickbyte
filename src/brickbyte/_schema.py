"""
Canonical schema constants and DDL for brickbyte tables.
"""

# Raw mode columns (no underscore prefix - all columns are brickbyte-owned)
RAW_RECORD_ID = "record_id"
RAW_EXTRACTED_AT = "extracted_at"
RAW_DATA = "data"
RAW_RUN_ID = "run_id"

RAW_COLUMNS = [RAW_RECORD_ID, RAW_EXTRACTED_AT, RAW_DATA, RAW_RUN_ID]

# Flatten mode metadata columns (underscore prefix to avoid collision with source fields)
FLATTEN_RECORD_ID = "_record_id"
FLATTEN_EXTRACTED_AT = "_extracted_at"
FLATTEN_RUN_ID = "_run_id"

FLATTEN_META_COLUMNS = [FLATTEN_RECORD_ID, FLATTEN_EXTRACTED_AT, FLATTEN_RUN_ID]

# SQL DDL for raw table creation
RAW_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {table_name} (
    record_id STRING,
    extracted_at TIMESTAMP,
    data STRING,
    run_id STRING
)
"""

# Dedup key missing indicator column
DK_MISSING = "_dk_missing"
