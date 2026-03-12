"""
Tests for BaseWriter abstract class and common functionality.
"""
import json
from unittest.mock import MagicMock, patch

import pytest

from brickbyte.writers.base import BaseWriter
from brickbyte.writers.spark_streaming_writer import SparkStreamingWriter
from brickbyte.writers.sql_streaming_writer import SQLStreamingWriter


class TestBaseWriter:
    """Test BaseWriter abstract class."""

    def test_cannot_instantiate_directly(self):
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            BaseWriter(catalog="main", schema="test")


class TestTransformRecord:
    """Test _transform_record across implementations."""

    @pytest.fixture
    def spark_writer(self):
        writer = SparkStreamingWriter(
            catalog="main", schema="test", run_id="test-run-id"
        )
        writer._spark = MagicMock()
        return writer

    @pytest.fixture
    def sql_writer(self):
        with patch("os.path.exists", return_value=True):
            return SQLStreamingWriter(
                catalog="main",
                schema="test",
                staging_volume="a.b.c",
                server_hostname="h",
                http_path="p",
                access_token="t",
                run_id="test-run-id",
            )

    def test_sql_raw_transform_adds_metadata(self, sql_writer):
        record = {"id": 1, "email": "test@example.com"}
        transformed = sql_writer._transform_record("stream1", record)

        assert "record_id" in transformed
        assert "extracted_at" in transformed
        assert "data" in transformed
        assert "run_id" in transformed

    def test_data_is_json_string(self, spark_writer):
        record = {"id": 1, "nested": {"key": "value"}, "list": [1, 2, 3]}
        transformed = spark_writer._transform_record("stream1", record)

        data_str = transformed["data"]
        assert isinstance(data_str, str)

        parsed = json.loads(data_str)
        assert parsed["id"] == 1
        assert parsed["nested"]["key"] == "value"
        assert parsed["list"] == [1, 2, 3]

    def test_unique_record_ids(self, spark_writer):
        record = {"id": 1}

        ids = set()
        for _ in range(100):
            transformed = spark_writer._transform_record("stream1", record)
            ids.add(transformed["record_id"])

        assert len(ids) == 100

    def test_transform_handles_special_characters(self, spark_writer):
        record = {
            "text": 'Hello "world"',
            "unicode": "日本語",
            "newlines": "line1\nline2",
        }
        transformed = spark_writer._transform_record("stream1", record)

        parsed = json.loads(transformed["data"])
        assert parsed["text"] == 'Hello "world"'
        assert parsed["unicode"] == "日本語"
        assert parsed["newlines"] == "line1\nline2"

    def test_transform_handles_none_values(self, spark_writer):
        record = {"id": 1, "optional": None}
        transformed = spark_writer._transform_record("stream1", record)

        parsed = json.loads(transformed["data"])
        assert parsed["optional"] is None

    def test_transform_handles_empty_record(self, spark_writer):
        record = {}
        transformed = spark_writer._transform_record("stream1", record)

        assert transformed["data"] == "{}"


class TestWriterConsistency:
    """Test that both writers behave consistently."""

    @pytest.fixture
    def spark_writer(self):
        writer = SparkStreamingWriter(
            catalog="main",
            schema="test",
            buffer_size_records=100,
            run_id="test-run",
        )
        writer._spark = MagicMock()
        return writer

    @pytest.fixture
    def sql_writer(self):
        with patch("os.path.exists", return_value=True):
            return SQLStreamingWriter(
                catalog="main",
                schema="test",
                staging_volume="a.b.c",
                server_hostname="h",
                http_path="p",
                access_token="t",
                buffer_size_records=100,
                run_id="test-run",
            )

    def test_same_table_name_format(self, spark_writer, sql_writer):
        assert spark_writer.get_table_name("users") == sql_writer.get_table_name("users")

    def test_same_raw_transform_schema(self, spark_writer, sql_writer):
        record = {"id": 1, "name": "test"}

        spark_result = spark_writer._transform_record("stream1", record)
        sql_result = sql_writer._transform_record("stream1", record)

        assert set(spark_result.keys()) == set(sql_result.keys())
        assert type(spark_result["record_id"]) == type(sql_result["record_id"])
        assert type(spark_result["extracted_at"]) == type(sql_result["extracted_at"])
        assert type(spark_result["data"]) == type(sql_result["data"])
        assert type(spark_result["run_id"]) == type(sql_result["run_id"])
