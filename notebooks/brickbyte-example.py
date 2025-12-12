# Databricks notebook source
# MAGIC %run ./_setup

# COMMAND ----------

from brickbyte import BrickByte

bb = BrickByte()

result = bb.sync(
    source="source-faker",
    source_config={"count": 100},
    catalog="",      # TODO: Set your Unity Catalog name
    schema="",       # TODO: Set your target schema
)

print(f"Synced {result.records_written} records from {len(result.streams_synced)} streams")
