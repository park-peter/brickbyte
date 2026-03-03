"""
Unit tests for SparkStreamingWriter.
"""
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from brickbyte.writers.spark_streaming_writer import SparkStreamingWriter


class TestSparkStreamingWriter:

    @pytest.fixture
    def writer(self):
        """Create a SparkStreamingWriter with mocked Spark."""
        writer = SparkStreamingWriter(
            catalog="main",
            schema="test",
            buffer_size_records=3,
            buffer_size_mb=1,
        )
        # Mock Spark session
        writer._spark = MagicMock()
        return writer

    def test_init_defaults(self):
        """Test default initialization values."""
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

    def test_write_micro_batch(self, writer):
        """Test micro-batch write to Delta via createDataFrame."""
        # Setup buffer
        writer._buffers["stream1"] = [
            {"_airbyte_raw_id": "1", "_airbyte_extracted_at": datetime.now(), "_airbyte_data": "{}"}
        ]
        writer._buffer_counts["stream1"] = 1
        writer._buffer_sizes["stream1"] = 100
        
        # Mock Spark createDataFrame chain
        mock_df = MagicMock()
        mock_write = MagicMock()
        mock_df.write = mock_write
        mock_write.format.return_value = mock_write
        mock_write.mode.return_value = mock_write
        mock_write.option.return_value = mock_write
        writer._spark.createDataFrame.return_value = mock_df
        
        writer._write_micro_batch("stream1")
        
        # Verify createDataFrame was called
        writer._spark.createDataFrame.assert_called_once()
        
        # Verify write chain
        mock_write.format.assert_called_with("delta")
        mock_write.mode.assert_called_with("append")
        mock_write.saveAsTable.assert_called_with("main.test.stream1")
        
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

    def test_transform_record_handles_datetime(self, writer):
        """Test that datetime objects in records are serialized."""
        record = {"id": 1, "created_at": datetime(2024, 1, 1, 12, 0, 0)}
        transformed = writer._transform_record(record)
        
        # Should not raise, datetime converted to string via default=str
        assert "2024-01-01" in transformed["_airbyte_data"]
