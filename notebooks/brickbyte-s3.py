# Databricks notebook source
# MAGIC %run ./_setup

# COMMAND ----------

# MAGIC %md
# MAGIC # Amazon S3 to Databricks with BrickByte
# MAGIC 
# MAGIC This notebook syncs files from Amazon S3 to Delta Lake tables in Unity Catalog.
# MAGIC 
# MAGIC **Supported file formats:** CSV, JSON, Parquet, Avro, JSONL
# MAGIC 
# MAGIC **Authentication options:**
# MAGIC - Auto-discovered from Databricks Secrets (recommended)
# MAGIC - IAM Access Key + Secret (inline)
# MAGIC - IAM Role (for Databricks on AWS)
# MAGIC 
# MAGIC **Prerequisites:**
# MAGIC - S3 bucket name
# MAGIC - AWS credentials with `s3:GetObject` and `s3:ListBucket` permissions

# COMMAND ----------

from brickbyte import BrickByte

bb = BrickByte()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 1: Auto-Discovered Credentials (Recommended)
# MAGIC 
# MAGIC Set up secrets once with the CLI:
# MAGIC ```bash
# MAGIC databricks secrets create-scope brickbyte
# MAGIC databricks secrets put-secret brickbyte source-s3/aws_access_key_id
# MAGIC databricks secrets put-secret brickbyte source-s3/aws_secret_access_key
# MAGIC databricks secrets put-secret brickbyte source-s3/region_name
# MAGIC ```
# MAGIC 
# MAGIC Then just sync - credentials are discovered automatically!

# COMMAND ----------

# Credentials auto-discovered from Databricks Secrets
result = bb.sync(
    source="source-s3",
    source_config={
        "bucket": "your-s3-bucket",
        # aws_access_key_id, aws_secret_access_key, region_name auto-discovered!
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
# MAGIC ## Option 2: Inline Credentials (Quick Testing)

# COMMAND ----------

result = bb.sync(
    source="source-s3",
    source_config={
        "bucket": "your-s3-bucket",
        "aws_access_key_id": "YOUR_ACCESS_KEY_ID",
        "aws_secret_access_key": "YOUR_SECRET_ACCESS_KEY",
        "region_name": "us-east-1",
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
# MAGIC ## Option 2: Authenticate with IAM Role ARN
# MAGIC 
# MAGIC Use this when running on AWS infrastructure with IAM roles.

# COMMAND ----------

result = bb.sync(
    source="source-s3",
    source_config={
        "bucket": "your-s3-bucket",
        "role_arn": "arn:aws:iam::123456789012:role/YourS3AccessRole",
        "region_name": "us-east-1",
        "streams": [
            {
                "name": "data_files",
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
# MAGIC ## Sync from Specific Path Prefix

# COMMAND ----------

result = bb.sync(
    source="source-s3",
    source_config={
        "bucket": "your-s3-bucket",
        "path_prefix": "data/2024/",  # Only sync from this prefix
        "aws_access_key_id": "YOUR_ACCESS_KEY_ID",
        "aws_secret_access_key": "YOUR_SECRET_ACCESS_KEY",
        "region_name": "us-east-1",
        "streams": [
            {
                "name": "2024_data",
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
    source="source-s3",
    source_config={
        "bucket": "your-s3-bucket",
        "aws_access_key_id": "YOUR_ACCESS_KEY_ID",
        "aws_secret_access_key": "YOUR_SECRET_ACCESS_KEY",
        "region_name": "us-east-1",
        "streams": [
            {
                "name": "event_logs",
                "globs": ["logs/**/*.jsonl", "events/**/*.json"],
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
    source="source-s3",
    source_config={
        "bucket": "your-s3-bucket",
        "aws_access_key_id": "YOUR_ACCESS_KEY_ID",
        "aws_secret_access_key": "YOUR_SECRET_ACCESS_KEY",
        "region_name": "us-east-1",
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
# MAGIC ## CSV with Custom Delimiter and Header

# COMMAND ----------

result = bb.sync(
    source="source-s3",
    source_config={
        "bucket": "your-s3-bucket",
        "aws_access_key_id": "YOUR_ACCESS_KEY_ID",
        "aws_secret_access_key": "YOUR_SECRET_ACCESS_KEY",
        "region_name": "us-east-1",
        "streams": [
            {
                "name": "tsv_files",
                "globs": ["**/*.tsv"],
                "format": {
                    "filetype": "csv",
                    "delimiter": "\t",
                    "quote_char": '"',
                    "escape_char": "\\",
                    "header_definition": {"header_definition_type": "Autogenerated"},
                },
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
    source="source-s3",
    source_config={
        "bucket": "your-s3-bucket",
        "aws_access_key_id": "YOUR_ACCESS_KEY_ID",
        "aws_secret_access_key": "YOUR_SECRET_ACCESS_KEY",
        "region_name": "us-east-1",
        "streams": [
            {
                "name": "raw_csv",
                "globs": ["raw/**/*.csv"],
                "format": {"filetype": "csv"},
            },
            {
                "name": "processed_parquet",
                "globs": ["processed/**/*.parquet"],
                "format": {"filetype": "parquet"},
            },
            {
                "name": "logs_jsonl",
                "globs": ["logs/**/*.jsonl"],
                "format": {"filetype": "jsonl"},
            },
        ],
    },
    catalog="",  # TODO: Set your Unity Catalog name
    schema="",   # TODO: Set your target schema
)

print(f"Synced {result.records_written} records from: {result.streams_synced}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## With Start Date Filter (Incremental)

# COMMAND ----------

result = bb.sync(
    source="source-s3",
    source_config={
        "bucket": "your-s3-bucket",
        "aws_access_key_id": "YOUR_ACCESS_KEY_ID",
        "aws_secret_access_key": "YOUR_SECRET_ACCESS_KEY",
        "region_name": "us-east-1",
        "start_date": "2024-01-01T00:00:00Z",  # Only files modified after this date
        "streams": [
            {
                "name": "recent_data",
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
# MAGIC ## S3-Compatible Storage (MinIO, DigitalOcean Spaces, etc.)

# COMMAND ----------

result = bb.sync(
    source="source-s3",
    source_config={
        "bucket": "your-bucket",
        "aws_access_key_id": "YOUR_ACCESS_KEY",
        "aws_secret_access_key": "YOUR_SECRET_KEY",
        "endpoint": "https://nyc3.digitaloceanspaces.com",  # Custom endpoint
        "streams": [
            {
                "name": "data",
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
# MAGIC ## With AI Metadata Enrichment

# COMMAND ----------

# result = bb.sync(
#     source="source-s3",
#     source_config={
#         "bucket": "your-s3-bucket",
#         "aws_access_key_id": "YOUR_ACCESS_KEY_ID",
#         "aws_secret_access_key": "YOUR_SECRET_ACCESS_KEY",
#         "region_name": "us-east-1",
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
