# BrickByte 🧱

**Sync data from Airbyte's 600+ connectors to Databricks in one line.**

BrickByte wraps [PyAirbyte](https://github.com/airbytehq/airbyte) to extract data from any source and writes directly to Databricks Unity Catalog.

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
)
```

That's it. BrickByte handles everything:
- ✅ Installs source connector in isolated venv
- ✅ Reads data via PyAirbyte
- ✅ Auto-discovers running SQL warehouse
- ✅ Auto-authenticates via Databricks SDK
- ✅ Writes directly to Unity Catalog
- ✅ Cleans up after itself

## Examples

### GitHub

```python
bb.sync(
    source="source-github",
    source_config={
        "credentials": {
            "option_title": "PAT Credentials",
            "personal_access_token": "ghp_...",
        },
        "repositories": ["owner/repo"],
    },
    catalog="main",
    schema="raw_github",
    streams=["commits", "issues", "pull_requests"],
)
```

### Confluence

```python
bb.sync(
    source="source-confluence",
    source_config={
        "domain_name": "your-company.atlassian.net",
        "email": "you@company.com",
        "api_token": "...",
    },
    catalog="main",
    schema="raw_confluence",
)
```

### DataDog

```python
bb.sync(
    source="source-datadog",
    source_config={
        "api_key": "...",
        "application_key": "...",
        "site": "datadoghq.com",
        "start_date": "2024-01-01T00:00:00Z",
        "end_date": "2024-12-31T23:59:59Z",
    },
    catalog="main",
    schema="raw_datadog",
)
```

## API Reference

### `BrickByte()`

```python
bb = BrickByte(base_venv_directory="/tmp/brickbyte")  # Optional
```

### `bb.sync()`

```python
result = bb.sync(
    source="source-github",           # Required: Airbyte source name
    source_config={...},              # Required: Source configuration
    catalog="main",                   # Required: Unity Catalog name
    schema="bronze",                  # Required: Target schema
    streams=["commits", "issues"],    # Optional: Streams (None = all)
    warehouse_id="abc123",            # Optional: SQL warehouse (auto-discovered)
    mode="full_refresh",              # Optional: "full_refresh" or "incremental"
    cleanup=True,                     # Optional: Cleanup venvs (default: True)
)

print(f"Synced {result.records_written} records")
print(f"Streams: {result.streams_synced}")
```

## Supported Sources

All [600+ Airbyte connectors](https://docs.airbyte.com/integrations):

| Category | Sources |
|----------|---------|
| **CRM** | Salesforce, HubSpot, Pipedrive, Close.com |
| **Marketing** | Facebook Marketing, Google Ads, LinkedIn Ads |
| **Analytics** | Google Analytics, Mixpanel, Amplitude, DataDog |
| **Payments** | Stripe, Braintree, PayPal, Chargebee |
| **Databases** | PostgreSQL, MySQL, MongoDB, MSSQL |
| **Files** | S3, GCS, Azure Blob Storage, SFTP |
| **Productivity** | Slack, Notion, Jira, Asana, Confluence |
| **Dev Tools** | GitHub, GitLab, Sentry |

## How It Works

```
bb.sync("source-github", {...}, "main", "bronze")
         │
         ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Airbyte Source │────▶│  Local Cache    │────▶│   Databricks    │
│  (isolated venv)│     │  (DuckDB)       │     │  (direct write) │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                        │
                                                        ▼
                                               ┌─────────────────┐
                                               │  Unity Catalog  │
                                               │  _airbyte_raw_* │
                                               └─────────────────┘
```

## Data Format

Data lands in raw tables:

```sql
CREATE TABLE _airbyte_raw_<stream> (
    _airbyte_ab_id STRING,         -- Unique record ID
    _airbyte_emitted_at TIMESTAMP, -- Extraction time
    _airbyte_data STRING           -- JSON payload
)
```

Query example:
```sql
SELECT 
    _airbyte_data:sha AS commit_sha,
    _airbyte_data:commit.message AS message
FROM main.bronze._airbyte_raw_commits
LIMIT 10;
```

## Architecture

```
src/brickbyte/
├── __init__.py   # BrickByte class
├── writer.py     # Direct Databricks writer
└── types.py      # Type definitions
```

## Requirements

- Python 3.10+
- Databricks workspace with Unity Catalog
- Running SQL Warehouse

## License

MIT License

---

<p align="center">Built with ❤️ for the Databricks community</p>
