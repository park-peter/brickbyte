# Databricks notebook source
# MAGIC %md
# MAGIC # BrickByte - Confluence Example
# MAGIC 
# MAGIC Sync data from Atlassian Confluence to Databricks in one line.
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC - Confluence Cloud account
# MAGIC - API Token (https://id.atlassian.com/manage-profile/security/api-tokens)

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

from brickbyte import BrickByte

bb = BrickByte()

result = bb.sync(
    source="source-confluence",
    source_config={
        "domain_name": "",  # e.g., "your-company.atlassian.net"
        "email": "",        # Your Atlassian account email
        "api_token": "",    # Generate at https://id.atlassian.com/manage-profile/security/api-tokens
    },
    catalog="",  # TODO: Set your Unity Catalog name
    schema="",   # TODO: Set your target schema
    # streams=["pages", "spaces"],  # Optional: select specific streams
)

print(f"Synced {result.records_written} records from {len(result.streams_synced)} streams")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Your Data
# MAGIC 
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC     _airbyte_data:id AS page_id,
# MAGIC     _airbyte_data:title AS title,
# MAGIC     _airbyte_data:status AS status
# MAGIC FROM your_catalog.your_schema._airbyte_raw_pages
# MAGIC LIMIT 10;
# MAGIC ```
