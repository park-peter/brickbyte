# Databricks notebook source
# MAGIC %md
# MAGIC # BrickByte - DataDog Example
# MAGIC 
# MAGIC Sync monitoring data from DataDog to Databricks in one line.
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC - DataDog API Key (https://app.datadoghq.com/organization-settings/api-keys)
# MAGIC - DataDog Application Key (https://app.datadoghq.com/organization-settings/application-keys)

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

from brickbyte import BrickByte

bb = BrickByte()

result = bb.sync(
    source="source-datadog",
    source_config={
        "api_key": "",            # DataDog API Key
        "application_key": "",    # DataDog Application Key
        "site": "datadoghq.com",  # Options: datadoghq.com, us3.datadoghq.com, datadoghq.eu
        "start_date": "2024-01-01T00:00:00Z",
        "end_date": "2024-12-31T23:59:59Z",
    },
    catalog="",  # TODO: Set your Unity Catalog name
    schema="",   # TODO: Set your target schema
    # streams=["monitors", "dashboards"],  # Optional: select specific streams
)

print(f"Synced {result.records_written} records from {len(result.streams_synced)} streams")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Your Data
# MAGIC 
# MAGIC ```sql
# MAGIC -- View monitors
# MAGIC SELECT 
# MAGIC     _airbyte_data:id AS monitor_id,
# MAGIC     _airbyte_data:name AS name,
# MAGIC     _airbyte_data:overall_state AS status
# MAGIC FROM your_catalog.your_schema._airbyte_raw_monitors
# MAGIC LIMIT 20;
# MAGIC 
# MAGIC -- View dashboards
# MAGIC SELECT 
# MAGIC     _airbyte_data:id AS dashboard_id,
# MAGIC     _airbyte_data:title AS title,
# MAGIC     _airbyte_data:author_handle AS author
# MAGIC FROM your_catalog.your_schema._airbyte_raw_dashboards
# MAGIC LIMIT 20;
# MAGIC ```
