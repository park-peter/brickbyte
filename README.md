# BrickByte 🧱

**Bridge Airbyte's 600+ connectors directly into Databricks**

BrickByte enables you to leverage [Airbyte's](https://github.com/airbytehq/airbyte) extensive library of pre-built data connectors to extract data from virtually any source and land it directly into Databricks—all without leaving your Databricks environment.

## Why BrickByte?

Airbyte has done the hard work of building and maintaining [600+ connectors](https://docs.airbyte.com/integrations) for APIs, databases, files, and more. Instead of reinventing the wheel, BrickByte lets you:

- **Use battle-tested connectors** — Tap into Airbyte's mature connector ecosystem
- **Run entirely in Databricks** — No separate infrastructure needed
- **Support incremental syncs** — Only pull new data with state management
- **Land data in Unity Catalog** — Write directly to your Databricks tables
- **Auto-authenticate** — Uses Databricks SDK to connect to current workspace automatically

## How It Works

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Databricks Notebook                         │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌───────────┐     ┌───────────┐     ┌──────────────────────────┐  │
│  │  Airbyte  │────▶│ PyAirbyte │────▶│  BrickByte Destination   │  │
│  │  Source   │     │   Cache   │     │     (Databricks SQL)     │  │
│  │ Connector │     │           │     │                          │  │
│  └───────────┘     └───────────┘     └────────────┬─────────────┘  │
│       │                                           │                 │
│       │                                           ▼                 │
│       │                               ┌──────────────────────────┐  │
│  - Salesforce                         │   Unity Catalog Tables   │  │
│  - Stripe                             │   _airbyte_raw_<stream>  │  │
│  - HubSpot                            └──────────────────────────┘  │
│  - 600+ more...                                                     │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

BrickByte manages isolated Python virtual environments for each connector to avoid dependency conflicts, then orchestrates data extraction via [PyAirbyte](https://github.com/airbytehq/airbyte) and loading into Databricks using a custom destination connector.

## Quick Start

### 1. Install in Databricks

Run the setup notebook or install manually:

```python
%pip install airbyte
%pip install git+https://github.com/park-peter/brickbyte.git --force-reinstall --no-deps
dbutils.library.restartPython()
```

### 2. Initialize BrickByte

```python
from brickbyte import BrickByte

bb = BrickByte(
    sources=["source-faker"],
    destination="destination-databricks",
    destination_install="git+https://github.com/park-peter/brickbyte.git#subdirectory=integrations/destination-databricks-py"
)
bb.setup()
```

### 3. Configure Source

```python
import airbyte as ab

cache = bb.get_or_create_cache()

source = ab.get_source(
    "source-faker",
    config={"count": 100},
    local_executable=bb.get_source_exec_path("source-faker")
)
source.check()
source.select_all_streams()
```

### 4. Run Sync

```python
# Uses current workspace credentials and auto-discovers a running warehouse
destination = bb.get_databricks_destination(
    catalog="your_catalog",
    schema="your_schema",
)

write_result = destination.write(source, cache=cache, force_full_refresh=True)
```

### 5. Cleanup

```python
bb.cleanup()
```

## Destination Configuration

The `get_databricks_destination` method automatically uses your current workspace credentials and discovers a running SQL warehouse when running in a Databricks notebook.

```python
# Simplest: auto-detect everything (warehouse, host, token)
destination = bb.get_databricks_destination(
    catalog="main",
    schema="bronze",
)

# Specify a particular warehouse
destination = bb.get_databricks_destination(
    catalog="main",
    schema="bronze",
    warehouse_id="abc123def456",
)

# Connect to a different workspace
destination = bb.get_databricks_destination(
    catalog="main",
    schema="bronze",
    warehouse_id="abc123def456",
    local_workspace=False,
    server_hostname="adb-xxx.azuredatabricks.net",
    token="dapi..."
)
```

| Parameter | Description | Required |
|-----------|-------------|----------|
| `catalog` | Unity Catalog name | ✅ |
| `schema` | Target schema name | ✅ |
| `warehouse_id` | SQL warehouse ID (auto-discovered if not provided) | |
| `local_workspace` | Use current workspace credentials (default: `True`) | |
| `server_hostname` | Databricks hostname (required if `local_workspace=False`) | |
| `token` | Databricks PAT (required if `local_workspace=False`) | |

## Supported Sources

BrickByte supports all [PyAirbyte-compatible sources](https://docs.airbyte.com/using-airbyte/pyairbyte/getting-started). Here are some popular ones:

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

[View all 600+ connectors →](https://docs.airbyte.com/integrations)

## Example Notebooks

Example notebooks are available in the `notebooks/` directory:

- `_setup.py` — Shared setup (install dependencies)
- `brickbyte-example.py` — Basic example with source-faker
- `brickbyte-confluence.py` — Sync from Atlassian Confluence
- `brickbyte-github.py` — Sync from GitHub
- `brickbyte-datadog.py` — Sync from DataDog

These are Python files that Databricks reads as notebooks.

## Sync Modes

BrickByte supports:

- **Full Refresh (Overwrite)** — Replaces all data in the destination table
- **Full Refresh (Append)** — Appends all source data to the destination
- **Incremental (Append)** — Only syncs new/updated records using state

## Data Format

Data is landed in raw tables with the following schema:

```sql
CREATE TABLE _airbyte_raw_<stream_name> (
    _airbyte_ab_id STRING,        -- Unique record identifier
    _airbyte_emitted_at TIMESTAMP, -- When the record was extracted
    _airbyte_data STRING           -- JSON payload of the record
)
```

You can then use Databricks SQL or dbt to transform raw data into your preferred schema.

## Architecture

```
brickbyte/
├── src/brickbyte/
│   ├── __init__.py      # BrickByte class & VirtualEnvManager
│   ├── destination.py   # Databricks destination helper
│   └── types.py         # Type definitions for sources & destinations
├── integrations/
│   └── destination-databricks-py/
│       └── destination_databricks/
│           ├── destination.py  # Airbyte destination implementation
│           ├── writer.py       # SQL-based data writer
│           └── client.py       # Databricks SQL client
└── notebooks/
    ├── _setup.py              # Shared setup notebook
    ├── brickbyte-example.py   # Basic example
    ├── brickbyte-confluence.py
    ├── brickbyte-github.py
    └── brickbyte-datadog.py
```

## Development

```bash
# Clone the repository
git clone https://github.com/park-peter/brickbyte.git
cd brickbyte

# Install dependencies for the destination connector
make install

# Run tests
pytest
```

## Requirements

- Python 3.10+
- Databricks workspace with Unity Catalog
- SQL Warehouse
- Databricks SDK (for auto-authentication)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License - see [LICENSE](LICENSE) for details.

## Acknowledgments

- [Airbyte](https://airbyte.com) for building an incredible open-source data integration platform
- [PyAirbyte](https://docs.airbyte.com/using-airbyte/pyairbyte/getting-started) for enabling Airbyte connectors to run anywhere Python runs

---

<p align="center">
  Built with ❤️ for the Databricks community
</p>
