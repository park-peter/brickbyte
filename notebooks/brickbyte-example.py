# Databricks notebook source
# MAGIC %run ./_setup

# COMMAND ----------

from brickbyte import BrickByte

bb = BrickByte(
    sources=["source-faker"],
    destination="destination-databricks",
    destination_install="git+https://github.com/park-peter/brickbyte.git#subdirectory=integrations/destination-databricks-py"
)
bb.setup()

# COMMAND ----------

import airbyte as ab

FORCE_FULL_REFRESH = True
cache = bb.get_or_create_cache()

# Configure the Faker source (generates fake data for testing)
source = ab.get_source(
    "source-faker",
    config={"count": 100},
    local_executable=bb.get_source_exec_path("source-faker")
)
source.check()
source.select_all_streams()

# COMMAND ----------

# Configure the Databricks destination using current workspace
# warehouse_id is optional - if not provided, will auto-discover a running warehouse
destination = bb.get_databricks_destination(
    catalog="",      # Unity Catalog name
    schema="",       # Target schema
    # warehouse_id="",  # Optional: specify warehouse ID, or leave empty to auto-discover
)

write_result = destination.write(source, cache=cache, force_full_refresh=FORCE_FULL_REFRESH)

# COMMAND ----------

bb.cleanup()

