# Databricks notebook source
# MAGIC %md
# MAGIC # BrickByte - Confluence Example
# MAGIC 
# MAGIC This notebook demonstrates how to sync data from Atlassian Confluence to Databricks using BrickByte.
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC - Confluence Cloud account
# MAGIC - API Token (generate at https://id.atlassian.com/manage-profile/security/api-tokens)
# MAGIC - Databricks workspace with Unity Catalog

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

from brickbyte import BrickByte

bb = BrickByte(
    sources=["source-confluence"],
    destination="destination-databricks",
    destination_install="git+https://github.com/park-peter/brickbyte.git#subdirectory=integrations/destination-databricks-py"
)
bb.setup()

# COMMAND ----------

import airbyte as ab

FORCE_FULL_REFRESH = True
cache = bb.get_or_create_cache()

# Configure the Confluence source
# Documentation: https://docs.airbyte.com/integrations/sources/confluence
source = ab.get_source(
    "source-confluence",
    config={
        "domain_name": "",  # e.g., "your-company.atlassian.net"
        "email": "",        # Your Atlassian account email
        "api_token": "",    # Generate at https://id.atlassian.com/manage-profile/security/api-tokens
    },
    local_executable=bb.get_source_exec_path("source-confluence")
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
# MAGIC After the sync completes, you can query your Confluence data:
# MAGIC 
# MAGIC ```sql
# MAGIC -- View synced pages
# MAGIC SELECT 
# MAGIC     _airbyte_ab_id,
# MAGIC     _airbyte_emitted_at,
# MAGIC     _airbyte_data:id AS page_id,
# MAGIC     _airbyte_data:title AS title,
# MAGIC     _airbyte_data:status AS status
# MAGIC FROM your_catalog.your_schema._airbyte_raw_pages
# MAGIC LIMIT 10;
# MAGIC ```

