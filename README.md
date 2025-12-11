# BrickByte 🧱

**Sync data from Airbyte's 600+ connectors to Databricks in one line.**

BrickByte wraps [PyAirbyte](https://github.com/airbytehq/airbyte) to make it dead simple to extract data from any source and land it directly into Databricks Unity Catalog.

## Quick Start

```python
%pip install airbyte
%pip install git+https://github.com/park-peter/brickbyte.git --force-reinstall --no-deps
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
- ✅ Installs Databricks destination connector
- ✅ Auto-discovers a running SQL warehouse
- ✅ Auto-authenticates via Databricks SDK
- ✅ Syncs data to Unity Catalog
- ✅ Cleans up after itself

## Real-World Examples

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
bb = BrickByte(base_venv_directory="/tmp/brickbyte")  # Optional: custom venv location
```

### `bb.sync()`

```python
result = bb.sync(
    source="source-github",           # Required: Airbyte source connector name
    source_config={...},              # Required: Source configuration dict
    catalog="main",                   # Required: Unity Catalog name
    schema="bronze",                  # Required: Target schema name
    streams=["commits", "issues"],    # Optional: List of streams (None = all)
    warehouse_id="abc123",            # Optional: SQL warehouse ID (auto-discovered)
    mode="full_refresh",              # Optional: "full_refresh" or "incremental"
    cleanup=True,                     # Optional: Cleanup venvs after sync (default: True)
)

print(f"Synced {result.records_written} records")
print(f"Streams: {result.streams_synced}")
```

## Supported Sources

BrickByte supports all [600+ Airbyte connectors](https://docs.airbyte.com/integrations):

| Category | Sources |
|----------|---------|
| **CRM** | Salesforce, HubSpot, Pipedrive, Close.com |
| **Marketing** | Facebook Marketing, Google Ads, LinkedIn Ads, TikTok Marketing |
| **Analytics** | Google Analytics, Mixpanel, Amplitude, PostHog, DataDog |
| **Payments** | Stripe, Braintree, PayPal, Chargebee |
| **Support** | Zendesk Support, Intercom, Freshdesk |
| **Databases** | PostgreSQL, MySQL, MongoDB, MSSQL |
| **Files** | S3, GCS, Azure Blob Storage, SFTP |
| **Productivity** | Slack, Notion, Jira, Asana, Airtable, Confluence |
| **E-commerce** | Shopify, Amazon Seller Partner |
| **Dev Tools** | GitHub, GitLab, Sentry |

## How It Works

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Databricks Notebook                         │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  bb.sync("source-github", {...}, "main", "bronze")                  │
│       │                                                             │
│       ▼                                                             │
│  ┌───────────┐     ┌───────────┐     ┌──────────────────────────┐  │
│  │  Airbyte  │────▶│ PyAirbyte │────▶│  Databricks Destination  │  │
│  │  Source   │     │   Cache   │     │     (auto-configured)    │  │
│  └───────────┘     └───────────┘     └────────────┬─────────────┘  │
│                                                   │                 │
│                                                   ▼                 │
│                                       ┌──────────────────────────┐  │
│                                       │   Unity Catalog Tables   │  │
│                                       │   _airbyte_raw_<stream>  │  │
│                                       └──────────────────────────┘  │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## Data Format

Data lands in raw tables with this schema:

```sql
CREATE TABLE _airbyte_raw_<stream_name> (
    _airbyte_ab_id STRING,         -- Unique record identifier
    _airbyte_emitted_at TIMESTAMP, -- When the record was extracted
    _airbyte_data STRING           -- JSON payload
)
```

Use Databricks SQL or dbt to transform into your preferred schema.

## Sync Modes

- **Full Refresh** (default) — Replaces all data in destination
- **Incremental** — Only syncs new/updated records using state

```python
bb.sync(..., mode="incremental")
```

## Requirements

- Python 3.10+
- Databricks workspace with Unity Catalog
- Running SQL Warehouse (auto-discovered)

## Contributing

Contributions welcome! Please submit a Pull Request.

## License

MIT License - see [LICENSE](LICENSE) for details.

---

<p align="center">
  Built with ❤️ for the Databricks community
</p>
