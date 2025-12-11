# Databricks notebook source
# MAGIC %md
# MAGIC # BrickByte - DataDog Example
# MAGIC 
# MAGIC This notebook demonstrates how to sync monitoring and analytics data from DataDog to Databricks using BrickByte.
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC - DataDog account
# MAGIC - API Key (found at https://app.datadoghq.com/organization-settings/api-keys)
# MAGIC - Application Key (found at https://app.datadoghq.com/organization-settings/application-keys)
# MAGIC - Databricks workspace with Unity Catalog
# MAGIC 
# MAGIC ## Note
# MAGIC Ensure your API and Application keys have the appropriate permissions for the data you want to sync.

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

from brickbyte import BrickByte

bb = BrickByte(
    sources=["source-datadog"],
    destination="destination-databricks",
    destination_install="git+https://github.com/park-peter/brickbyte.git#subdirectory=integrations/destination-databricks-py"
)
bb.setup()

# COMMAND ----------

import airbyte as ab

FORCE_FULL_REFRESH = True
cache = bb.get_or_create_cache()

# Configure the DataDog source
# Documentation: https://docs.airbyte.com/integrations/sources/datadog
source = ab.get_source(
    "source-datadog",
    config={
        "api_key": "",            # DataDog API Key
        "application_key": "",    # DataDog Application Key
        "site": "datadoghq.com",  # Options: datadoghq.com, us3.datadoghq.com, datadoghq.eu, etc.
        "start_date": "2024-01-01T00:00:00Z",
        "end_date": "2024-12-31T23:59:59Z",
    },
    local_executable=bb.get_source_exec_path("source-datadog")
)
source.check()
source.select_all_streams()
print("Available streams:", source.get_available_streams())

# COMMAND ----------

# Configure the Databricks destination using current workspace
destination = bb.get_databricks_destination(
    catalog="",      # Unity Catalog name
    schema="",       # Target schema
)

write_result = destination.write(source, cache=cache, force_full_refresh=FORCE_FULL_REFRESH)
print("Sync completed!")

# COMMAND ----------

bb.cleanup()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Your Data
# MAGIC 
# MAGIC After the sync completes, you can query your DataDog data:
# MAGIC 
# MAGIC ```sql
# MAGIC -- View monitors and their status
# MAGIC SELECT 
# MAGIC     _airbyte_emitted_at,
# MAGIC     _airbyte_data:id AS monitor_id,
# MAGIC     _airbyte_data:name AS name,
# MAGIC     _airbyte_data:type AS type,
# MAGIC     _airbyte_data:overall_state AS status
# MAGIC FROM your_catalog.your_schema._airbyte_raw_monitors
# MAGIC ORDER BY _airbyte_data:name
# MAGIC LIMIT 20;
# MAGIC 
# MAGIC -- View recent incidents
# MAGIC SELECT 
# MAGIC     _airbyte_data:id AS incident_id,
# MAGIC     _airbyte_data:attributes.title AS title,
# MAGIC     _airbyte_data:attributes.severity AS severity,
# MAGIC     _airbyte_data:attributes.state AS state,
# MAGIC     _airbyte_data:attributes.created AS created_at
# MAGIC FROM your_catalog.your_schema._airbyte_raw_incidents
# MAGIC ORDER BY _airbyte_data:attributes.created DESC
# MAGIC LIMIT 20;
# MAGIC 
# MAGIC -- View dashboards
# MAGIC SELECT 
# MAGIC     _airbyte_data:id AS dashboard_id,
# MAGIC     _airbyte_data:title AS title,
# MAGIC     _airbyte_data:author_handle AS author,
# MAGIC     _airbyte_data:created_at AS created_at
# MAGIC FROM your_catalog.your_schema._airbyte_raw_dashboards
# MAGIC LIMIT 20;
# MAGIC ```

