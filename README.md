# BrickByte 🧱

**Sync data from Airbyte's 600+ connectors to Databricks with Streaming performance.**

BrickByte wraps [PyAirbyte](https://github.com/airbytehq/airbyte) to extract data from any source and streams it directly to Databricks Unity Catalog using Volumes and `COPY INTO`.

## Features

- **600+ Sources** - All Airbyte connectors work out of the box
- **Streaming Architecture** - Bypasses local disk, no OOM issues
- **High Performance** - Uses Unity Catalog Volumes and `COPY INTO`
- **AI Enrichment** - Auto-generate column descriptions via Foundation Models
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
    enrich_metadata=True,  # Auto-generate column descriptions
    # buffer_size_records=50000, # Optional: Adjust for larger batches
)
# Tables now have AI-generated column descriptions in Unity Catalog
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

1. **Native Spark Streaming** (Default in Databricks Notebooks/Jobs)
   - Writes directly to local temp storage -> Spark loads to Delta.
   - **Fastest performance**. No Volume required.

2. **SQL Streaming** (Remote / Local Laptop)
   - Writes to Volume -> `COPY INTO` via SQL Warehouse.
   - Robust remote execution. Requires `staging_volume`.

```
[In Notebook] ──▶ Spark Streaming ──▶ Local Temp ──▶ Delta Table
                                         (No Volume)

[Remote Client] ──▶ SQL Streaming ──▶ Volume ──▶ COPY INTO ──▶ Delta Table
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
  "delta-spark==3.2.0",
  "pyarrow==19.0.0",
]
```

## License

MIT License
