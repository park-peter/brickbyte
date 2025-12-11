# Databricks notebook source
# MAGIC %md
# MAGIC # BrickByte - GitHub Example
# MAGIC 
# MAGIC Sync data from GitHub to Databricks in one line.
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC - GitHub Personal Access Token (https://github.com/settings/tokens)

# COMMAND ----------

# MAGIC %run ./_setup

# COMMAND ----------

from brickbyte import BrickByte

bb = BrickByte()

result = bb.sync(
    source="source-github",
    source_config={
        "credentials": {
            "option_title": "PAT Credentials",
            "personal_access_token": "",  # Your GitHub PAT
        },
        "repositories": ["airbytehq/airbyte"],  # format: "owner/repo"
        # "start_date": "2024-01-01T00:00:00Z",  # Optional
    },
    catalog="",  # TODO: Set your Unity Catalog name
    schema="",   # TODO: Set your target schema
    # streams=["commits", "issues", "pull_requests"],  # Optional: select specific streams
)

print(f"Synced {result.records_written} records from {len(result.streams_synced)} streams")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Your Data
# MAGIC 
# MAGIC ```sql
# MAGIC -- View recent commits
# MAGIC SELECT 
# MAGIC     _airbyte_data:sha AS commit_sha,
# MAGIC     _airbyte_data:commit.message AS message,
# MAGIC     _airbyte_data:commit.author.name AS author
# MAGIC FROM your_catalog.your_schema._airbyte_raw_commits
# MAGIC ORDER BY _airbyte_data:commit.author.date DESC
# MAGIC LIMIT 20;
# MAGIC 
# MAGIC -- View open issues
# MAGIC SELECT 
# MAGIC     _airbyte_data:number AS issue_number,
# MAGIC     _airbyte_data:title AS title,
# MAGIC     _airbyte_data:state AS state
# MAGIC FROM your_catalog.your_schema._airbyte_raw_issues
# MAGIC WHERE _airbyte_data:state = 'open'
# MAGIC LIMIT 20;
# MAGIC ```
