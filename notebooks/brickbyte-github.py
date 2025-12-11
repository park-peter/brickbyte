# Databricks notebook source
# MAGIC %md
# MAGIC # BrickByte - GitHub Example
# MAGIC 
# MAGIC This notebook demonstrates how to sync data from GitHub to Databricks using BrickByte.
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC - GitHub account
# MAGIC - Personal Access Token (generate at https://github.com/settings/tokens)
# MAGIC - Databricks workspace with Unity Catalog
# MAGIC 
# MAGIC ## Note
# MAGIC For public repositories, you can use a token with minimal scopes. For private repositories, ensure your token has appropriate repository access.

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

from brickbyte import BrickByte

bb = BrickByte(
    sources=["source-github"],
    destination="destination-databricks",
    destination_install="git+https://github.com/park-peter/brickbyte.git#subdirectory=integrations/destination-databricks-py"
)
bb.setup()

# COMMAND ----------

import airbyte as ab

FORCE_FULL_REFRESH = True
cache = bb.get_or_create_cache()

# Configure the GitHub source
# Documentation: https://docs.airbyte.com/integrations/sources/github
source = ab.get_source(
    "source-github",
    config={
        "credentials": {
            "option_title": "PAT Credentials",
            "personal_access_token": "",  # Your GitHub PAT
        },
        "repositories": ["airbytehq/airbyte"],  # format: "owner/repo"
        # "start_date": "2024-01-01T00:00:00Z",  # Optional
    },
    local_executable=bb.get_source_exec_path("source-github")
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
# MAGIC After the sync completes, you can query your GitHub data:
# MAGIC 
# MAGIC ```sql
# MAGIC -- View recent commits
# MAGIC SELECT 
# MAGIC     _airbyte_emitted_at,
# MAGIC     _airbyte_data:sha AS commit_sha,
# MAGIC     _airbyte_data:commit.message AS message,
# MAGIC     _airbyte_data:commit.author.name AS author,
# MAGIC     _airbyte_data:commit.author.date AS commit_date
# MAGIC FROM your_catalog.your_schema._airbyte_raw_commits
# MAGIC ORDER BY _airbyte_data:commit.author.date DESC
# MAGIC LIMIT 20;
# MAGIC 
# MAGIC -- View open issues
# MAGIC SELECT 
# MAGIC     _airbyte_data:number AS issue_number,
# MAGIC     _airbyte_data:title AS title,
# MAGIC     _airbyte_data:state AS state,
# MAGIC     _airbyte_data:user.login AS author,
# MAGIC     _airbyte_data:created_at AS created_at
# MAGIC FROM your_catalog.your_schema._airbyte_raw_issues
# MAGIC WHERE _airbyte_data:state = 'open'
# MAGIC ORDER BY _airbyte_data:created_at DESC
# MAGIC LIMIT 20;
# MAGIC ```

