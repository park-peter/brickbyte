# Databricks notebook source
# MAGIC %run ./_setup

# COMMAND ----------

# MAGIC %md
# MAGIC # Azure Blob Storage to Databricks with BrickByte
# MAGIC 
# MAGIC This notebook syncs files from Azure Blob Storage to Delta Lake tables in Unity Catalog.
# MAGIC 
# MAGIC **Supported file formats:** CSV, JSON, Parquet, Avro, JSONL
# MAGIC 
# MAGIC **Authentication options:**
# MAGIC - Storage Account Key
# MAGIC - Service Principal (Client Credentials)
# MAGIC - OAuth 2.0
# MAGIC 
# MAGIC **Prerequisites:**
# MAGIC - Azure Storage Account name
# MAGIC - Container (bucket) name
# MAGIC - Authentication credentials

# COMMAND ----------

from brickbyte import Brickbyte

bb = Brickbyte()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 1: Authenticate with Storage Account Key

# COMMAND ----------

# Store credentials in Databricks Secrets for production:
#   account_key = dbutils.secrets.get(scope="azure", key="storage_account_key")

result = bb.sync(
    source="source-azure-blob-storage",
    source_config={
        "azure_blob_storage_account_name": "your_storage_account",
        "azure_blob_storage_container_name": "your_container",
        "credentials": {
            "auth_type": "storage_account_key",
            "azure_blob_storage_account_key": "YOUR_STORAGE_ACCOUNT_KEY",
        },
        "streams": [
            {
                "name": "csv_files",
                "globs": ["**/*.csv"],
                "format": {"filetype": "csv"},
            }
        ],
    },
    catalog="",  # TODO: Set your Unity Catalog name
    schema="",   # TODO: Set your target schema
)

print(f"Synced {result.records_written} records from {len(result.streams_synced)} streams")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 2: Authenticate with Service Principal (Client Credentials)
# MAGIC 
# MAGIC Requires IAM role `Storage Blob Data Reader` assigned to the Service Principal.

# COMMAND ----------

result = bb.sync(
    source="source-azure-blob-storage",
    source_config={
        "azure_blob_storage_account_name": "your_storage_account",
        "azure_blob_storage_container_name": "your_container",
        "credentials": {
            "auth_type": "client_credentials",
            "tenant_id": "YOUR_TENANT_ID",
            "client_id": "YOUR_CLIENT_ID",
            "client_secret": "YOUR_CLIENT_SECRET",
        },
        "streams": [
            {
                "name": "parquet_files",
                "globs": ["**/*.parquet"],
                "format": {"filetype": "parquet"},
            }
        ],
    },
    catalog="",  # TODO: Set your Unity Catalog name
    schema="",   # TODO: Set your target schema
)

print(f"Synced {result.records_written} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sync JSON Lines Files

# COMMAND ----------

result = bb.sync(
    source="source-azure-blob-storage",
    source_config={
        "azure_blob_storage_account_name": "your_storage_account",
        "azure_blob_storage_container_name": "your_container",
        "credentials": {
            "auth_type": "storage_account_key",
            "azure_blob_storage_account_key": "YOUR_KEY",
        },
        "streams": [
            {
                "name": "json_data",
                "globs": ["data/**/*.jsonl", "logs/**/*.json"],
                "format": {"filetype": "jsonl"},
            }
        ],
    },
    catalog="",  # TODO: Set your Unity Catalog name
    schema="",   # TODO: Set your target schema
)

print(f"Synced {result.records_written} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sync Avro Files

# COMMAND ----------

result = bb.sync(
    source="source-azure-blob-storage",
    source_config={
        "azure_blob_storage_account_name": "your_storage_account",
        "azure_blob_storage_container_name": "your_container",
        "credentials": {
            "auth_type": "storage_account_key",
            "azure_blob_storage_account_key": "YOUR_KEY",
        },
        "streams": [
            {
                "name": "avro_data",
                "globs": ["**/*.avro"],
                "format": {"filetype": "avro"},
            }
        ],
    },
    catalog="",  # TODO: Set your Unity Catalog name
    schema="",   # TODO: Set your target schema
)

print(f"Synced {result.records_written} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Multiple Streams (Different File Types)

# COMMAND ----------

result = bb.sync(
    source="source-azure-blob-storage",
    source_config={
        "azure_blob_storage_account_name": "your_storage_account",
        "azure_blob_storage_container_name": "your_container",
        "credentials": {
            "auth_type": "storage_account_key",
            "azure_blob_storage_account_key": "YOUR_KEY",
        },
        "streams": [
            {
                "name": "sales_csv",
                "globs": ["sales/**/*.csv"],
                "format": {"filetype": "csv"},
            },
            {
                "name": "events_json",
                "globs": ["events/**/*.jsonl"],
                "format": {"filetype": "jsonl"},
            },
            {
                "name": "analytics_parquet",
                "globs": ["analytics/**/*.parquet"],
                "format": {"filetype": "parquet"},
            },
        ],
    },
    catalog="",  # TODO: Set your Unity Catalog name
    schema="",   # TODO: Set your target schema
)

print(f"Synced {result.records_written} records from streams: {result.streams_synced}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## With AI Metadata Enrichment

# COMMAND ----------

# result = bb.sync(
#     source="source-azure-blob-storage",
#     source_config={
#         "azure_blob_storage_account_name": "your_storage_account",
#         "azure_blob_storage_container_name": "your_container",
#         "credentials": {
#             "auth_type": "storage_account_key",
#             "azure_blob_storage_account_key": "YOUR_KEY",
#         },
#         "streams": [
#             {
#                 "name": "customer_data",
#                 "globs": ["customers/**/*.csv"],
#                 "format": {"filetype": "csv"},
#             }
#         ],
#     },
#     catalog="",  # TODO: Set your Unity Catalog name
#     schema="",   # TODO: Set your target schema
#     enrich_metadata=True,
#     enrich_model="databricks-meta-llama-3-3-70b-instruct",
# )

# print(f"Enriched tables: {result.enriched_tables}")
