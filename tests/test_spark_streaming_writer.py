"""
Unit tests for SparkStreamingWriter.
"""
import os
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from brickbyte.writers.spark_streaming_writer import SparkStreamingWriter


class TestSparkStreamingWriter:

    @pytest.fixture
    def writer(self, tmp_path):
        """Create a SparkStreamingWriter with mocked Spark."""
        with patch.dict(os.environ, {"SPARK_LOCAL_DIRS": str(tmp_path)}):
            writer = SparkStreamingWriter(
                catalog="main",
                schema="test",
                buffer_size_records=3,
                buffer_size_mb=1,
            )
            # Mock Spark session
            writer._spark = MagicMock()
            return writer

    def test_init_defaults(self, tmp_path):
        """Test default initialization values."""
        with patch.dict(os.environ, {"SPARK_LOCAL_DIRS": str(tmp_path)}):
            writer = SparkStreamingWriter(catalog="main", schema="bronze")
            
            assert writer.catalog == "main"
            assert writer.schema == "bronze"
            assert writer.buffer_size_records == 50000
            assert writer.buffer_size_bytes == 100 * 1024 * 1024  # 100MB

    def test_get_table_name(self, writer):
        """Test fully qualified table name generation."""
        assert writer.get_table_name("users") == "main.test.users"
        assert writer.get_table_name("orders") == "main.test.orders"

    def test_transform_record(self, writer):
        """Test that transform_record adds Airbyte metadata."""
        record = {"id": 1, "name": "test"}
        transformed = writer._transform_record(record)
        
        assert "_airbyte_raw_id" in transformed
        assert "_airbyte_extracted_at" in transformed
        assert "_airbyte_data" in transformed
        
        # Verify UUID format
        assert len(transformed["_airbyte_raw_id"]) == 36
        
        # Verify datetime
        assert isinstance(transformed["_airbyte_extracted_at"], datetime)
        
        # Verify JSON serialization
        assert '"id": 1' in transformed["_airbyte_data"]
        assert '"name": "test"' in transformed["_airbyte_data"]

    def test_write_record_buffers(self, writer):
        """Test that write_record buffers records correctly."""
        writer.write_record("stream1", {"id": 1})
        writer.write_record("stream1", {"id": 2})
        
        assert len(writer._buffers["stream1"]) == 2
        assert writer._buffer_counts["stream1"] == 2

    def test_write_record_flushes_at_threshold(self, writer):
        """Test that write_record flushes when record threshold is hit."""
        writer._write_micro_batch = MagicMock()
        
        # Write up to threshold (3 records)
        writer.write_record("stream1", {"id": 1})
        writer.write_record("stream1", {"id": 2})
        assert writer._write_micro_batch.call_count == 0
        
        # Third record should trigger flush
        writer.write_record("stream1", {"id": 3})
        assert writer._write_micro_batch.call_count == 1

    @patch("pyarrow.parquet.write_table")
    @patch("os.remove")
    def test_write_micro_batch(self, mock_remove, mock_pq_write, writer):
        """Test micro-batch write to Delta."""
        # Setup buffer
        writer._buffers["stream1"] = [
            {"_airbyte_raw_id": "1", "_airbyte_extracted_at": datetime.now(), "_airbyte_data": "{}"}
        ]
        writer._buffer_counts["stream1"] = 1
        writer._buffer_sizes["stream1"] = 100
        
        # Mock Spark operations
        mock_df = MagicMock()
        writer._spark.read.parquet.return_value = mock_df
        
        writer._write_micro_batch("stream1")
        
        # Verify parquet was written
        mock_pq_write.assert_called_once()
        
        # Verify Spark read/write
        writer._spark.read.parquet.assert_called_once()
        mock_df.write.format.assert_called_with("delta")
        
        # Verify buffer was reset
        assert writer._buffers["stream1"] == []
        assert writer._buffer_counts["stream1"] == 0
        assert writer._buffer_sizes["stream1"] == 0

    def test_flush_stream_calls_write_micro_batch(self, writer):
        """Test that flush_stream delegates to _write_micro_batch."""
        writer._write_micro_batch = MagicMock()
        writer._buffers["stream1"] = [{"id": 1}]
        
        writer.flush_stream("stream1")
        
        writer._write_micro_batch.assert_called_once_with("stream1")

    def test_close_flushes_all_streams(self, writer):
        """Test that close flushes all buffered streams."""
        writer.flush_stream = MagicMock()
        writer._buffers["stream1"] = [{"id": 1}]
        writer._buffers["stream2"] = [{"id": 2}]
        
        writer.close()
        
        assert writer.flush_stream.call_count == 2

    def test_drop_table(self, writer):
        """Test drop_table executes correct SQL."""
        writer.drop_table("users")
        
        writer._spark.sql.assert_called_with("DROP TABLE IF EXISTS main.test.users")

    def test_table_exists(self, writer):
        """Test table_exists check."""
        writer._spark.catalog.tableExists.return_value = True
        assert writer.table_exists("users") is True
        
        writer._spark.catalog.tableExists.return_value = False
        assert writer.table_exists("orders") is False

    def test_get_table_schema(self, writer):
        """Test getting table schema."""
        # Mock schema
        mock_field1 = MagicMock()
        mock_field1.name = "id"
        mock_field1.dataType = "LongType"
        
        mock_field2 = MagicMock()
        mock_field2.name = "name"
        mock_field2.dataType = "StringType"
        
        mock_df = MagicMock()
        mock_df.schema.fields = [mock_field1, mock_field2]
        writer._spark.table.return_value = mock_df
        writer._spark.catalog.tableExists.return_value = True
        
        schema = writer.get_table_schema("users")
        
        assert schema == {"id": "LongType", "name": "StringType"}

    def test_staging_dir_creation(self, writer):
        """Test that staging directories are created."""
        staging_dir = writer._get_staging_dir("test_stream")
        
        assert os.path.exists(staging_dir)
        assert "test_stream" in staging_dir

