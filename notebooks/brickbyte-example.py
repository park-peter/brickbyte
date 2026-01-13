# Databricks notebook source
# MAGIC %run ./_setup

# COMMAND ----------

# MAGIC %md
# MAGIC # BrickByte Quick Start
# MAGIC 
# MAGIC BrickByte bridges Airbyte's 600+ connectors directly into Databricks.
# MAGIC 
# MAGIC ## Credential Management
# MAGIC 
# MAGIC BrickByte automatically discovers credentials from **Databricks Secrets**:
# MAGIC 
# MAGIC | Scope | Key Pattern | Example |
# MAGIC |-------|-------------|---------|
# MAGIC | `brickbyte` | `{source-name}/{field}` | `source-s3/aws_access_key_id` |
# MAGIC 
# MAGIC **Setup your secrets once:**
# MAGIC ```
# MAGIC databricks secrets put-secret brickbyte source-s3/aws_access_key_id
# MAGIC databricks secrets put-secret brickbyte source-s3/aws_secret_access_key
# MAGIC ```
# MAGIC 
# MAGIC Then just sync - credentials are discovered automatically!

# COMMAND ----------

from brickbyte import BrickByte

bb = BrickByte()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Sync (No Credentials Needed for Faker)

# COMMAND ----------

# Simple sync with source-faker (no credentials required)
result = bb.sync(
    source="source-faker",
    source_config={"count": 100},
    catalog="",      # TODO: Set your Unity Catalog name
    schema="",       # TODO: Set your target schema
)

print(f"Synced {result.records_written} records from {len(result.streams_synced)} streams")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sync with Auto-Discovered Credentials
# MAGIC 
# MAGIC If you've set up secrets in scope `brickbyte`, credentials are merged automatically:

# COMMAND ----------

# Credentials auto-discovered from Databricks Secrets
# Just provide non-sensitive config - credentials come from secrets
result = bb.sync(
    source="source-s3",
    source_config={
        "bucket": "my-bucket",
        "region_name": "us-east-1",
        "streams": [{"name": "data", "globs": ["**/*.parquet"], "format": {"filetype": "parquet"}}],
    },
    # aws_access_key_id and aws_secret_access_key auto-discovered from:
    #   scope: brickbyte
    #   keys: source-s3/aws_access_key_id, source-s3/aws_secret_access_key
    catalog="",  # TODO: Set your catalog
    schema="",   # TODO: Set your schema
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## List Configured Sources

# COMMAND ----------

# See which sources have credentials configured
sources = bb.list_configured_sources()
print(f"Sources with credentials: {sources}")

# Validate specific source
if bb.validate_credentials("source-s3"):
    print("✓ S3 credentials found")
else:
    print("✗ S3 credentials not configured")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Custom Secrets Scope

# COMMAND ----------

# Use a different secrets scope
bb_custom = BrickByte(secrets_scope="my-team-secrets")

# COMMAND ----------

# MAGIC %md
# MAGIC ## YAML Profiles (Advanced)
# MAGIC 
# MAGIC For credential reuse across multiple sources, use a YAML profiles file:
# MAGIC 
# MAGIC ```yaml
# MAGIC # /Workspace/Shared/brickbyte/profiles.yml
# MAGIC profiles:
# MAGIC   azure-shared:
# MAGIC     tenant_id: "{{ secret('azure/tenant_id') }}"
# MAGIC     client_id: "{{ secret('azure/client_id') }}"
# MAGIC     client_secret: "{{ secret('azure/client_secret') }}"
# MAGIC 
# MAGIC mappings:
# MAGIC   source-microsoft-teams: azure-shared
# MAGIC   source-azure-blob-storage: azure-shared
# MAGIC ```

# COMMAND ----------

# Load with YAML profiles
# bb_profiles = BrickByte(profiles="/Workspace/Shared/brickbyte/profiles.yml")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Before Sync

# COMMAND ----------

# Preview before sync (optional)
# preview = bb.preview(
#     source="source-faker",
#     source_config={"count": 100},
#     catalog="main",
#     schema="bronze",
# )
# print(preview)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sync with AI Metadata Enrichment

# COMMAND ----------

# Sync with AI metadata enrichment
# result = bb.sync(
#     source="source-faker",
#     source_config={"count": 100},
#     catalog="main",
#     schema="bronze",
#     enrich_metadata=True,
#     enrich_model="databricks-meta-llama-3-1-70b-instruct",
# )
# print(f"Enriched tables: {result.enriched_tables}")
