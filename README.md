# brickbyte

Sync data from 600+ source connectors to Databricks Unity Catalog with streaming performance.

brickbyte wraps [PyAirbyte](https://github.com/airbytehq/PyAirbyte) to extract data from 600+ sources into Databricks. For sources that Lakeflow Connect already covers, use Lakeflow Connect. brickbyte fills the gap for everything else. Schedule with Lakeflow Jobs, transform downstream with Declarative Pipelines.

## Features

- **600+ Sources** - Any Airbyte connector works out of the box
- **Streaming Architecture** - Bounded memory, no local disk needed
- **Raw + Flattened Output** - JSON blob or spread columns
- **Safe Overwrite** - Atomic staged replace preserves metadata
- **Incremental Sync** - State-managed delta processing for connectors that support state APIs
- **Deduplication** - MERGE-based dedup with validated user-defined keys
- **Concurrent Streams** - Parallel writes with isolated per-thread writers
- **Progress Reporting** - Callback events every 5000 records plus per-stream completion
- **Timeout Control** - Cooperative timeout for long-running syncs
- **AI Enrichment** - Column descriptions, PII detection via Foundation Models
- **Preview** - Schema comparison before committing

## Quick Start

```python
import brickbyte

bb = brickbyte.client()

result = bb.sync(
    source="source-faker",
    source_config={"count": 1000},
    catalog="main",
    schema="bronze",
)

print(f"Synced {result.records_written} records")
```

## Output Schema

### Raw Mode (default)

| Column | Type | Description |
|--------|------|-------------|
| `record_id` | STRING | Unique UUID per record |
| `extracted_at` | TIMESTAMP | UTC extraction time |
| `data` | STRING | JSON blob of source record |
| `run_id` | STRING | UUID identifying the sync run |

### Flatten Mode (`flatten=True`)

Source fields become top-level columns, plus metadata:

| Column | Type | Description |
|--------|------|-------------|
| `_record_id` | STRING | Unique UUID per record |
| `_extracted_at` | TIMESTAMP | UTC extraction time |
| `_run_id` | STRING | UUID identifying the sync run |
| *(source columns)* | *(inferred)* | Source data fields |

## Architecture

```
                    ┌──────────────────┐
                    │   PyAirbyte      │
                    │  600+ Connectors │
                    └────────┬─────────┘
                             │
                    ┌────────▼─────────┐
                    │    brickbyte     │
                    │  Streaming Core  │
                    └───┬─────────┬────┘
                        │         │
           ┌────────────▼──┐  ┌──▼────────────┐
           │ Spark Writer  │  │  SQL Writer    │
           │ (in-notebook) │  │ (COPY INTO)    │
           │ No Volume     │  │ Volume needed  │
           └───────┬───────┘  └───────┬────────┘
                   │                  │
                   └─────────┬────────┘
                    ┌────────▼─────────┐
                    │  Delta Lake      │
                    │  Unity Catalog   │
                    └──────────────────┘
```

## Examples

### Flattened Output

```python
result = bb.sync(
    source="source-faker",
    source_config={"count": 100},
    catalog="main",
    schema="bronze",
    flatten=True,
)
```

### Incremental Sync

```python
result = bb.sync(
    source="source-github",
    source_config={"repository": "owner/repo"},
    catalog="main",
    schema="bronze",
    incremental=True,
)
```

Incremental mode requires connector state APIs (`set_stream_state`, `set_state_for_stream`, or `set_state`) to apply saved state before reading. If saved state exists but the connector does not support state injection, the sync fails fast.

### Deduplication

```python
result = bb.sync(
    source="source-faker",
    source_config={"count": 100},
    catalog="main",
    schema="bronze",
    deduplicate=True,
    dedup_keys=["email"],  # or per-stream: {"users": ["email"], "orders": ["order_id"]}
)
```

`dedup_keys` is applied only when `deduplicate=True`; otherwise it is ignored. Dedup key names must be valid identifier strings (unsafe characters like backticks/semicolons are rejected).

### Concurrent Streams

```python
result = bb.sync(
    source="source-faker",
    source_config={"count": 100},
    catalog="main",
    schema="bronze",
    max_parallel_streams=4,
)
```

### Progress Reporting

```python
def on_progress(event):
    print(f"{event.stream_name}: {event.records_processed} records")

result = bb.sync(
    source="source-faker",
    source_config={"count": 100},
    catalog="main",
    schema="bronze",
    progress_callback=on_progress,
)
```

Progress callbacks fire every 5000 processed records per stream and once when each stream completes.

### Timeout

```python
result = bb.sync(
    source="source-github",
    source_config={"repository": "owner/repo"},
    catalog="main",
    schema="bronze",
    timeout_seconds=300,
)
```

### AI Metadata Enrichment

```python
result = bb.sync(
    source="source-faker",
    source_config={"count": 100},
    catalog="main",
    schema="bronze",
    enrich_metadata=True,
)
```

### Preview

```python
preview = bb.preview(
    source="source-faker",
    source_config={"count": 100},
    catalog="main",
    schema="bronze",
)
print(preview)
```

## Credential Management

brickbyte auto-discovers credentials from Databricks Secrets:

```bash
# Store credentials (one-time setup)
databricks secrets put-secret brickbyte source-s3/aws_access_key_id
databricks secrets put-secret brickbyte source-s3/aws_secret_access_key
```

```python
# Credentials auto-discovered - just provide non-sensitive config
result = bb.sync(
    source="source-s3",
    source_config={"bucket": "my-bucket"},
    catalog="main",
    schema="bronze",
)
```

Supports dotted keys for nested config (`source-x/credentials.client_id` maps to `{"credentials": {"client_id": "..."}}`), custom scopes, and YAML profiles for credential reuse.

## Requirements

- Python 3.10 - 3.12
- Databricks Unity Catalog
- For SQL mode: SQL Warehouse + Unity Catalog Volume

## Development

```bash
uv pip install -e ".[dev]"
uv run pytest tests/ -v -m "not integration"
uv run ruff check src/
```

## License

Apache 2.0
