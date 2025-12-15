# Databricks notebook source
# MAGIC %run ./_setup

# COMMAND ----------

from brickbyte import BrickByte

bb = BrickByte()

# COMMAND ----------

# Simple sync (overwrite mode - default)
result = bb.sync(
    source="source-faker",
    source_config={"count": 100},
    catalog="",      # TODO: Set your Unity Catalog name
    schema="",       # TODO: Set your target schema
    # staging_volume="", # Optional: Set if running outside of Databricks Notebooks
)

print(f"Synced {result.records_written} records from {len(result.streams_synced)} streams")

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

# Sync with AI metadata enrichment
# result = bb.sync(
#     source="source-faker",
#     source_config={"count": 100},
#     catalog="main",
#     schema="bronze",
#     staging_volume="main.staging.vol",
#     enrich_metadata=True,  # Auto-generate column descriptions
# )
# print(f"Enriched tables: {result.enriched_tables}")
