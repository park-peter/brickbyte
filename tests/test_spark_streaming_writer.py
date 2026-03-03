"""
Unit tests for SparkStreamingWriter.
"""
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from brickbyte.writers.spark_streaming_writer import SparkStreamingWriter


class TestSparkStreamingWriter:
    @pytest.fixture
    def writer(self):
        writer = SparkStreamingWriter(
            catalog="main",
            schema="test",
            buffer_size_records=3,
            buffer_size_mb=1,
            run_id="test-run-id",
        )
        writer._spark = MagicMock()
        return writer

    def test_init_defaults(self):
        writer = SparkStreamingWriter(catalog="main", schema="bronze")
        assert writer.catalog == "main"
        assert writer.schema == "bronze"
        assert writer.buffer_size_records == 50000
        assert writer.buffer_size_bytes == 100 * 1024 * 1024

    def test_get_table_name(self, writer):
        assert writer.get_table_name("users") == "`main`.`test`.`users`"
        assert writer.get_table_name("orders") == "`main`.`test`.`orders`"

    def test_transform_record_raw(self, writer):
        record = {"id": 1, "name": "test"}
        transformed = writer._transform_record("stream1", record)

        assert "record_id" in transformed
        assert "extracted_at" in transformed
        assert "data" in transformed
        assert "run_id" in transformed
        assert transformed["run_id"] == "test-run-id"

        assert len(transformed["record_id"]) == 36
        assert isinstance(transformed["extracted_at"], datetime)
        assert transformed["extracted_at"].tzinfo is not None
        assert '"id": 1' in transformed["data"]
        assert '"name": "test"' in transformed["data"]

    def test_transform_record_flatten(self):
        writer = SparkStreamingWriter(
            catalog="main", schema="test", flatten=True, run_id="test-run"
        )
        writer._spark = MagicMock()
        record = {"id": 1, "name": "test"}
        transformed = writer._transform_record("stream1", record)

        assert "_record_id" in transformed
        assert "_extracted_at" in transformed
        assert "_run_id" in transformed
        assert transformed["id"] == 1
        assert transformed["name"] == "test"
        assert "data" not in transformed

    def test_write_record_buffers(self, writer):
        writer.write_record("stream1", {"id": 1})
        writer.write_record("stream1", {"id": 2})
        assert len(writer._buffers["stream1"]) == 2
        assert writer._buffer_counts["stream1"] == 2

    def test_write_record_flushes_at_threshold(self, writer):
        writer._write_micro_batch = MagicMock()
        writer.write_record("stream1", {"id": 1})
        writer.write_record("stream1", {"id": 2})
        assert writer._write_micro_batch.call_count == 0
        writer.write_record("stream1", {"id": 3})
        assert writer._write_micro_batch.call_count == 1

    def test_write_micro_batch(self, writer):
        writer._buffers["stream1"] = [
            {
                "record_id": "1",
                "extracted_at": datetime.now(timezone.utc),
                "data": "{}",
                "run_id": "test",
            }
        ]
        writer._buffer_counts["stream1"] = 1
        writer._buffer_sizes["stream1"] = 100

        mock_df = MagicMock()
        mock_write = MagicMock()
        mock_df.write = mock_write
        mock_write.format.return_value = mock_write
        mock_write.mode.return_value = mock_write
        mock_write.option.return_value = mock_write
        writer._spark.createDataFrame.return_value = mock_df

        writer._write_micro_batch("stream1")

        writer._spark.createDataFrame.assert_called_once()
        mock_write.format.assert_called_with("delta")
        mock_write.mode.assert_called_with("append")
        mock_write.saveAsTable.assert_called_with("`main`.`test`.`stream1`")

        assert writer._buffers["stream1"] == []
        assert writer._buffer_counts["stream1"] == 0
        assert writer._buffer_sizes["stream1"] == 0

    def test_flush_stream_calls_write_micro_batch(self, writer):
        writer._write_micro_batch = MagicMock()
        writer._buffers["stream1"] = [{"id": 1}]
        writer.flush_stream("stream1")
        writer._write_micro_batch.assert_called_once_with("stream1")

    def test_close_flushes_all_streams(self, writer):
        writer.flush_stream = MagicMock()
        writer._buffers["stream1"] = [{"id": 1}]
        writer._buffers["stream2"] = [{"id": 2}]
        writer.close()
        assert writer.flush_stream.call_count == 2

    def test_drop_table(self, writer):
        writer.drop_table("users")
        writer._spark.sql.assert_called_with(
            "DROP TABLE IF EXISTS `main`.`test`.`users`"
        )

    def test_table_exists(self, writer):
        writer._spark.catalog.tableExists.return_value = True
        assert writer.table_exists("users") is True
        writer._spark.catalog.tableExists.return_value = False
        assert writer.table_exists("orders") is False

    def test_get_table_schema(self, writer):
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
        record = {"id": 1, "created_at": datetime(2024, 1, 1, 12, 0, 0)}
        transformed = writer._transform_record("stream1", record)
        assert "2024-01-01" in transformed["data"]
