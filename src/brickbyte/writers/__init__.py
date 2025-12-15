"""
BrickByte Writers Module.
"""
import logging
from typing import Optional, Union

from brickbyte.writers.base import BaseWriter
from brickbyte.writers.spark_streaming_writer import SparkStreamingWriter
from brickbyte.writers.sql_streaming_writer import SQLStreamingWriter

logger = logging.getLogger(__name__)

def create_streaming_writer(
    catalog: str,
    schema: str,
    staging_volume: Optional[str] = None,
    warehouse_id: Optional[str] = None,
    force_sql: bool = False,
    buffer_size_records: int = 50000,
    buffer_size_mb: int = 100,
) -> Union[SparkStreamingWriter, SQLStreamingWriter]:
    """
    Create a streaming writer based on environment.
    
    Logic:
    1. If Spark is active (and not force_sql=True) -> SparkStreamingWriter (No Volume needed).
    2. Else -> SQLStreamingWriter (Volume REQUIRED).
    """
    
    # 1. Attempt to detect Spark
    spark_active = False
    if not force_sql:
        try:
            from pyspark.sql import SparkSession
            if SparkSession.getActiveSession():
                spark_active = True
        except ImportError:
            pass
            
    if spark_active:
        logger.info("Spark detected. Using Native Spark Streaming Writer.")
        return SparkStreamingWriter(
            catalog=catalog,
            schema=schema,
            buffer_size_records=buffer_size_records,
            buffer_size_mb=buffer_size_mb,
        )
    
    # 2. Fallback to SQL Writer
    logger.info("Spark not active (or forced off). Using SQL Streaming Writer.")
    
    if not staging_volume:
        raise ValueError(
            "staging_volume is REQUIRED when running outside of Databricks (or when forcing SQL mode). "
            "Because we cannot access local disk from the warehouse, we must stage files in a Volume."
        )

    from databricks.sdk import WorkspaceClient
    
    w = WorkspaceClient()
    server_hostname = w.config.host.replace("https://", "").rstrip("/")
    access_token = w.config.token
    
    # Auto-discover warehouse if not provided
    if not warehouse_id:
        warehouses = list(w.warehouses.list())
        running = [
            wh for wh in warehouses
            if wh.state and wh.state.value == "RUNNING"
        ]
        if running:
            warehouse_id = running[0].id
        else:
            raise ValueError(
                "No running SQL warehouse found. "
                "Specify warehouse_id or start a warehouse."
            )
            
    http_path = f"/sql/1.0/warehouses/{warehouse_id}"
    
    return SQLStreamingWriter(
        catalog=catalog,
        schema=schema,
        staging_volume=staging_volume,
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token,
        buffer_size_records=buffer_size_records,
        buffer_size_mb=buffer_size_mb,
    )


__all__ = [
    "BaseWriter",
    "SparkStreamingWriter",
    "SQLStreamingWriter",
    "create_streaming_writer",
]
