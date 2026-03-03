# Databricks notebook source
# MAGIC %pip install airbyte databricks-sdk databricks-sql-connector virtualenv pyarrow
# MAGIC %pip install git+https://github.com/park-peter/brickbyte.git --force-reinstall --no-deps

# COMMAND ----------

dbutils.library.restartPython()
