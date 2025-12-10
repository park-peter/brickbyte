# BrickByte 🧱

**Bridge Airbyte's 600+ connectors directly into Databricks**

BrickByte enables you to leverage [Airbyte's](https://github.com/airbytehq/airbyte) extensive library of pre-built data connectors to extract data from virtually any source and land it directly into Databricks—all without leaving your Databricks environment.

## Why BrickByte?

Airbyte has done the hard work of building and maintaining [600+ connectors](https://docs.airbyte.com/integrations) for APIs, databases, files, and more. Instead of reinventing the wheel, BrickByte lets you:

- **Use battle-tested connectors** — Tap into Airbyte's mature connector ecosystem
- **Run entirely in Databricks** — No separate infrastructure needed
- **Support incremental syncs** — Only pull new data with state management
- **Land data in Unity Catalog** — Write directly to your Databricks tables

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

```python
%pip install airbyte
dbutils.library.restartPython()
```

```python
%pip install git+https://github.com/stikkireddy/brickbyte.git --force-reinstall --no-deps
dbutils.library.restartPython()
```

### 2. Initialize BrickByte

```python
from brickbyte import BrickByte

bb = BrickByte(
    sources=["source-faker"],  # Airbyte source connector name
    destination="destination-databricks",
    destination_install="git+https://github.com/stikkireddy/brickbyte.git#subdirectory=integrations/destination-databricks-py"
)

bb.setup()  # Creates virtual environments and installs connectors
```

### 3. Configure and Run Sync

```python
import airbyte as ab

# Create a local cache for state management
cache = bb.get_or_create_cache()

# Configure your source
source = ab.get_source(
    "source-faker",
    config={"count": 100},
    local_executable=bb.get_source_exec_path("source-faker")
)

source.check()  # Verify connection
source.select_all_streams()  # Or select specific streams

# Configure the Databricks destination
destination = ab.get_destination(
    "destination-databricks",
    config={
        "server_hostname": "<your-databricks-hostname>",
        "http_path": "/sql/1.0/warehouses/<warehouse-id>",
        "token": "<your-databricks-token>",
        "catalog": "your_catalog",
        "schema": "your_schema",
    },
    local_executable=bb.get_destination_exec_path()
)

# Run the sync
write_result = destination.write(source, cache=cache, force_full_refresh=True)
```

### 4. Cleanup

```python
bb.cleanup()  # Removes virtual environments
```

## Destination Configuration

| Parameter | Description | Required |
|-----------|-------------|----------|
| `server_hostname` | Databricks workspace hostname (e.g., `adb-xxx.azuredatabricks.net`) | ✅ |
| `http_path` | SQL warehouse HTTP path (e.g., `/sql/1.0/warehouses/abc123`) | ✅ |
| `token` | Databricks personal access token | ✅ |
| `catalog` | Unity Catalog catalog name | ✅ |
| `schema` | Target schema name | ✅ |

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
│   └── types.py         # Type definitions for sources & destinations
└── integrations/
    └── destination-databricks-py/
        └── destination_databricks/
            ├── destination.py  # Airbyte destination implementation
            ├── writer.py       # SQL-based data writer
            └── client.py       # Databricks SQL client
```

## Development

```bash
# Clone the repository
git clone https://github.com/stikkireddy/brickbyte.git
cd brickbyte

# Install dependencies for the destination connector
make install

# Run tests
pytest
```

## Requirements

- Python 3.10+
- Databricks workspace with Unity Catalog
- SQL Warehouse or cluster with SQL capabilities
- Databricks personal access token

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

