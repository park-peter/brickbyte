# BrickByte 🧱

**Sync data from Airbyte's 600+ connectors to Databricks with Streaming performance.**

BrickByte wraps [PyAirbyte](https://github.com/airbytehq/airbyte) to extract data from any source and streams it directly to Databricks Unity Catalog using Volumes and `COPY INTO`.

## Features

- **600+ Sources** - All Airbyte connectors work out of the box
- **Streaming Architecture** - Bypasses local disk, no OOM issues
- **High Performance** - Uses Unity Catalog Volumes and `COPY INTO`
- **AI Enrichment** - Auto-generate table descriptions and detect PII via Foundation Models
- **Preview** - See what schema changes will occur before syncing
- **Simple API** - One-line sync

## Quick Start

```python
%pip install airbyte databricks-sdk databricks-sql-connector virtualenv
%pip install git+https://github.com/park-peter/brickbyte.git
dbutils.library.restartPython()
```

```python
from brickbyte import BrickByte

bb = BrickByte()
bb.sync(
    source="source-faker",
    source_config={"count": 100},
    catalog="main",
    schema="bronze",
    # staging_volume="main.staging.vol", # Optional if using Native Spark (in Notebook)
)
```

## Examples

### Simple Sync (Overwrite)

```python
bb.sync(
    source="source-github",
    source_config={
        "credentials": {"personal_access_token": "ghp_..."},
        "repositories": ["owner/repo"],
    },
    catalog="main",
    schema="bronze",
    staging_volume="main.staging.brickbyte_volume",
)
```

### With AI Metadata Enrichment

```python
result = bb.sync(
    source="source-salesforce",
    source_config={...},
    catalog="main",
    schema="bronze",
    staging_volume="main.staging.brickbyte_volume",
    enrich_metadata=True,  # AI-powered metadata enrichment
)
# Tables get:
#   - AI-generated table description (COMMENT ON TABLE)
#   - Field descriptions stored in TBLPROPERTIES
#   - PII detection stored as table TAGS
```

### Preview Before Sync

```python
preview = bb.preview(
    source="source-github",
    source_config={...},
    catalog="main",
    schema="bronze",
)
print(preview)
```

## Architecture

### Hybrid Mode
BrickByte automatically selects the best write strategy:

1. **Native Spark** (Default in Databricks Notebooks/Jobs)
   - Uses `createDataFrame` + micro-batch writes to Delta
   - **Fastest performance**. No Volume required.

2. **SQL Streaming** (Remote / Local)
   - Writes to Volume → `COPY INTO` via SQL Warehouse
   - Robust remote execution. Requires `staging_volume`.

```
[In Notebook] ──▶ Spark createDataFrame ──▶ Delta Table (No Volume)

[Remote]      ──▶ SQL Streaming ──▶ Volume ──▶ COPY INTO ──▶ Delta Table
```

## Requirements

- Python 3.10+
- Databricks workspace with Unity Catalog
- SQL Warehouse
- Unity Catalog Volume for staging (Required only for Remote/SQL mode)

## Dependencies

```toml
[project]
dependencies = [
  "virtualenv",
  "databricks-sdk==0.74.0",
  "databricks-sql-connector==4.2.2",
  "airbyte==0.34.0",
  "pyarrow>=14.0.0",
]

[project.optional-dependencies]
local-spark = ["delta-spark>=3.0.0", "pyspark>=3.5.0"]
```

For local Spark + Delta development:
```bash
pip install brickbyte[local-spark]
```

## License

MIT License
