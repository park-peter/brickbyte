"""
Tests for buffer size thresholds (records AND bytes).
"""
import os
from unittest.mock import MagicMock, patch

import pytest

from brickbyte.writers.spark_streaming_writer import SparkStreamingWriter
from brickbyte.writers.sql_streaming_writer import SQLStreamingWriter


class TestBufferSizeBytes:
    """Test byte-based buffer thresholds."""

    @pytest.fixture
    def spark_writer(self, tmp_path):
        """SparkStreamingWriter with low byte threshold."""
        with patch.dict(os.environ, {"SPARK_LOCAL_DIRS": str(tmp_path)}):
            writer = SparkStreamingWriter(
                catalog="main",
                schema="test",
                buffer_size_records=1000,  # High record limit
                buffer_size_mb=1,  # 1MB byte limit (will hit first)
            )
            writer._spark = MagicMock()
            writer._write_micro_batch = MagicMock()
            return writer

    @pytest.fixture
    def sql_writer(self):
        """SQLStreamingWriter with low byte threshold."""
        with patch("databricks.sql.connect"):
            writer = SQLStreamingWriter(
                catalog="main",
                schema="test",
                staging_volume="main.staging.vol",
                server_hostname="host",
                http_path="/sql",
                access_token="token",
                buffer_size_records=1000,  # High record limit
                buffer_size_mb=1,  # 1MB byte limit
            )
            writer.flush_stream = MagicMock()
            return writer

    def test_spark_flushes_on_byte_threshold(self, spark_writer):
        """Test SparkStreamingWriter flushes when byte threshold is hit."""
        # sys.getsizeof returns ~50 bytes overhead per string
        # So we need many medium-sized records to exceed 1MB
        # 1MB = 1,048,576 bytes / ~100 bytes per record = ~10k records
        # Use 20k char string to make each record ~20KB
        large_data = "x" * 20_000
        
        # Write records until we exceed threshold
        for i in range(40):  # 40 * ~25KB = ~1MB
            spark_writer.write_record("stream1", {"data": large_data, "i": i})
        
        assert spark_writer._write_micro_batch.call_count == 0
        
        # One more should trigger flush
        for i in range(20):
            spark_writer.write_record("stream1", {"data": large_data, "i": i})
        
        assert spark_writer._write_micro_batch.call_count >= 1

    def test_sql_flushes_on_byte_threshold(self, sql_writer):
        """Test SQLStreamingWriter flushes when byte threshold is hit."""
        large_data = "x" * 20_000
        
        for i in range(40):
            sql_writer.write_record("stream1", {"data": large_data, "i": i})
        
        assert sql_writer.flush_stream.call_count == 0
        
        for i in range(20):
            sql_writer.write_record("stream1", {"data": large_data, "i": i})
        
        assert sql_writer.flush_stream.call_count >= 1

    def test_record_threshold_still_works(self, tmp_path):
        """Test that record count threshold works independently."""
        with patch.dict(os.environ, {"SPARK_LOCAL_DIRS": str(tmp_path)}):
            writer = SparkStreamingWriter(
                catalog="main",
                schema="test",
                buffer_size_records=2,  # Low record limit
                buffer_size_mb=1000,  # High byte limit (won't hit)
            )
            writer._spark = MagicMock()
            writer._write_micro_batch = MagicMock()
            
            # Small records - byte threshold won't be hit
            writer.write_record("stream1", {"id": 1})
            assert writer._write_micro_batch.call_count == 0
            
            writer.write_record("stream1", {"id": 2})
            assert writer._write_micro_batch.call_count == 1

    def test_buffer_size_tracking(self, spark_writer):
        """Test that buffer sizes are tracked correctly."""
        spark_writer._write_micro_batch = MagicMock()  # Prevent actual flush
        
        spark_writer.write_record("stream1", {"data": "small"})
        
        assert spark_writer._buffer_sizes["stream1"] > 0
        initial_size = spark_writer._buffer_sizes["stream1"]
        
        spark_writer.write_record("stream1", {"data": "another"})
        
        assert spark_writer._buffer_sizes["stream1"] > initial_size

    def test_buffer_reset_after_flush(self, tmp_path):
        """Test that all buffer tracking is reset after flush."""
        with patch.dict(os.environ, {"SPARK_LOCAL_DIRS": str(tmp_path)}):
            writer = SparkStreamingWriter(
                catalog="main",
                schema="test",
                buffer_size_records=2,
                buffer_size_mb=100,
            )
            writer._spark = MagicMock()
            
            # Mock the write operations
            with patch("pyarrow.parquet.write_table"), patch("os.remove"):
                mock_df = MagicMock()
                writer._spark.read.parquet.return_value = mock_df
                
                writer.write_record("stream1", {"id": 1})
                writer.write_record("stream1", {"id": 2})  # Triggers flush
                
                # All tracking should be reset
                assert writer._buffers["stream1"] == []
                assert writer._buffer_counts["stream1"] == 0
                assert writer._buffer_sizes["stream1"] == 0

