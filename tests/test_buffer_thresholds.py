"""
Tests for buffer size thresholds (records AND bytes).
"""
from unittest.mock import MagicMock, patch

import pytest

from brickbyte.writers.spark_streaming_writer import SparkStreamingWriter
from brickbyte.writers.sql_streaming_writer import SQLStreamingWriter


class TestBufferSizeBytes:
    """Test byte-based buffer thresholds."""

    @pytest.fixture
    def spark_writer(self):
        writer = SparkStreamingWriter(
            catalog="main",
            schema="test",
            buffer_size_records=1000,
            buffer_size_mb=1,
            run_id="test-run",
        )
        writer._spark = MagicMock()
        writer._write_micro_batch = MagicMock()
        return writer

    @pytest.fixture
    def sql_writer(self):
        with patch("os.path.exists", return_value=True):
            writer = SQLStreamingWriter(
                catalog="main",
                schema="test",
                staging_volume="main.staging.vol",
                server_hostname="host",
                http_path="/sql",
                access_token="token",
                buffer_size_records=1000,
                buffer_size_mb=1,
                run_id="test-run",
            )
            writer.flush_stream = MagicMock()
            return writer

    def test_spark_flushes_on_byte_threshold(self, spark_writer):
        large_data = "x" * 20_000

        for i in range(40):
            spark_writer.write_record("stream1", {"data": large_data, "i": i})

        assert spark_writer._write_micro_batch.call_count == 0

        for i in range(20):
            spark_writer.write_record("stream1", {"data": large_data, "i": i})

        assert spark_writer._write_micro_batch.call_count >= 1

    def test_sql_flushes_on_byte_threshold(self, sql_writer):
        large_data = "x" * 20_000

        for i in range(40):
            sql_writer.write_record("stream1", {"data": large_data, "i": i})

        assert sql_writer.flush_stream.call_count == 0

        for i in range(20):
            sql_writer.write_record("stream1", {"data": large_data, "i": i})

        assert sql_writer.flush_stream.call_count >= 1

    def test_record_threshold_still_works(self):
        writer = SparkStreamingWriter(
            catalog="main",
            schema="test",
            buffer_size_records=2,
            buffer_size_mb=1000,
            run_id="test-run",
        )
        writer._spark = MagicMock()
        writer._write_micro_batch = MagicMock()

        writer.write_record("stream1", {"id": 1})
        assert writer._write_micro_batch.call_count == 0

        writer.write_record("stream1", {"id": 2})
        assert writer._write_micro_batch.call_count == 1

    def test_buffer_size_tracking(self, spark_writer):
        spark_writer._write_micro_batch = MagicMock()

        spark_writer.write_record("stream1", {"data": "small"})

        assert spark_writer._buffer_sizes["stream1"] > 0
        initial_size = spark_writer._buffer_sizes["stream1"]

        spark_writer.write_record("stream1", {"data": "another"})

        assert spark_writer._buffer_sizes["stream1"] > initial_size

    def test_buffer_reset_after_flush(self):
        writer = SparkStreamingWriter(
            catalog="main",
            schema="test",
            buffer_size_records=2,
            buffer_size_mb=100,
            run_id="test-run",
        )
        writer._spark = MagicMock()

        # Mock the Spark write chain
        mock_df = MagicMock()
        mock_write = MagicMock()
        mock_df.write = mock_write
        mock_write.format.return_value = mock_write
        mock_write.mode.return_value = mock_write
        mock_write.option.return_value = mock_write
        writer._spark.createDataFrame.return_value = mock_df

        writer.write_record("stream1", {"id": 1})
        writer.write_record("stream1", {"id": 2})

        assert writer._buffers["stream1"] == []
        assert writer._buffer_counts["stream1"] == 0
        assert writer._buffer_sizes["stream1"] == 0
