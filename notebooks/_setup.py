# Databricks notebook source
# MAGIC %pip install airbyte==0.38.0 databricks-sdk==0.95.0 databricks-sql-connector==4.2.5 virtualenv==20.29.3 pyarrow==21.0.0 pyyaml==6.0.3
# MAGIC %pip install git+https://github.com/park-peter/brickbyte.git --force-reinstall --no-deps

# COMMAND ----------

dbutils.library.restartPython()
