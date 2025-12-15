"""
Tests for BaseWriter abstract class and common functionality.
"""
import json
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from brickbyte.writers.base import BaseWriter
from brickbyte.writers.spark_streaming_writer import SparkStreamingWriter
from brickbyte.writers.sql_streaming_writer import SQLStreamingWriter


class TestBaseWriter:
    """Test BaseWriter abstract class."""

    def test_cannot_instantiate_directly(self):
        """Test that BaseWriter cannot be instantiated directly."""
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            BaseWriter(catalog="main", schema="test")

    def test_get_table_name(self):
        """Test get_table_name via concrete implementation."""
        with patch("databricks.sql.connect"):
            writer = SQLStreamingWriter(
                catalog="main",
                schema="bronze",
                staging_volume="a.b.c",
                server_hostname="h",
                http_path="p",
                access_token="t",
            )
            
            assert writer.get_table_name("users") == "main.bronze.users"
            assert writer.get_table_name("my_table") == "main.bronze.my_table"


class TestTransformRecord:
    """Test _transform_record across implementations."""

    @pytest.fixture
    def spark_writer(self, tmp_path):
        import os
        with patch.dict(os.environ, {"SPARK_LOCAL_DIRS": str(tmp_path)}):
            writer = SparkStreamingWriter(catalog="main", schema="test")
            writer._spark = MagicMock()
            return writer

    @pytest.fixture
    def sql_writer(self):
        with patch("databricks.sql.connect"):
            return SQLStreamingWriter(
                catalog="main",
                schema="test",
                staging_volume="a.b.c",
                server_hostname="h",
                http_path="p",
                access_token="t",
            )

    def test_spark_transform_adds_metadata(self, spark_writer):
        """Test SparkStreamingWriter adds Airbyte metadata."""
        record = {"id": 1, "email": "test@example.com"}
        transformed = spark_writer._transform_record(record)
        
        assert "_airbyte_raw_id" in transformed
        assert "_airbyte_extracted_at" in transformed
        assert "_airbyte_data" in transformed

    def test_sql_transform_adds_metadata(self, sql_writer):
        """Test SQLStreamingWriter adds Airbyte metadata."""
        record = {"id": 1, "email": "test@example.com"}
        transformed = sql_writer._transform_record(record)
        
        assert "_airbyte_raw_id" in transformed
        assert "_airbyte_extracted_at" in transformed
        assert "_airbyte_data" in transformed

    def test_raw_id_is_uuid(self, spark_writer):
        """Test that _airbyte_raw_id is a valid UUID."""
        record = {"id": 1}
        transformed = spark_writer._transform_record(record)
        
        raw_id = transformed["_airbyte_raw_id"]
        # UUID format: 8-4-4-4-12 hex digits
        assert len(raw_id) == 36
        assert raw_id.count("-") == 4

    def test_extracted_at_is_datetime(self, spark_writer):
        """Test that _airbyte_extracted_at is a datetime."""
        record = {"id": 1}
        transformed = spark_writer._transform_record(record)
        
        assert isinstance(transformed["_airbyte_extracted_at"], datetime)

    def test_data_is_json_string(self, spark_writer):
        """Test that _airbyte_data is a valid JSON string."""
        record = {"id": 1, "nested": {"key": "value"}, "list": [1, 2, 3]}
        transformed = spark_writer._transform_record(record)
        
        data_str = transformed["_airbyte_data"]
        assert isinstance(data_str, str)
        
        # Should be valid JSON
        parsed = json.loads(data_str)
        assert parsed["id"] == 1
        assert parsed["nested"]["key"] == "value"
        assert parsed["list"] == [1, 2, 3]

    def test_unique_raw_ids(self, spark_writer):
        """Test that each transform generates unique raw_id."""
        record = {"id": 1}
        
        ids = set()
        for _ in range(100):
            transformed = spark_writer._transform_record(record)
            ids.add(transformed["_airbyte_raw_id"])
        
        assert len(ids) == 100  # All unique

    def test_transform_handles_special_characters(self, spark_writer):
        """Test that transform handles special characters in data."""
        record = {
            "text": 'Hello "world"',
            "unicode": "日本語",
            "newlines": "line1\nline2",
        }
        transformed = spark_writer._transform_record(record)
        
        parsed = json.loads(transformed["_airbyte_data"])
        assert parsed["text"] == 'Hello "world"'
        assert parsed["unicode"] == "日本語"
        assert parsed["newlines"] == "line1\nline2"

    def test_transform_handles_none_values(self, spark_writer):
        """Test that transform handles None values."""
        record = {"id": 1, "optional": None}
        transformed = spark_writer._transform_record(record)
        
        parsed = json.loads(transformed["_airbyte_data"])
        assert parsed["optional"] is None

    def test_transform_handles_empty_record(self, spark_writer):
        """Test that transform handles empty records."""
        record = {}
        transformed = spark_writer._transform_record(record)
        
        assert transformed["_airbyte_data"] == "{}"


class TestWriterConsistency:
    """Test that both writers behave consistently."""

    @pytest.fixture
    def spark_writer(self, tmp_path):
        import os
        with patch.dict(os.environ, {"SPARK_LOCAL_DIRS": str(tmp_path)}):
            writer = SparkStreamingWriter(
                catalog="main",
                schema="test",
                buffer_size_records=100,
            )
            writer._spark = MagicMock()
            return writer

    @pytest.fixture
    def sql_writer(self):
        with patch("databricks.sql.connect"):
            return SQLStreamingWriter(
                catalog="main",
                schema="test",
                staging_volume="a.b.c",
                server_hostname="h",
                http_path="p",
                access_token="t",
                buffer_size_records=100,
            )

    def test_same_table_name_format(self, spark_writer, sql_writer):
        """Test both writers generate same table names."""
        assert spark_writer.get_table_name("users") == sql_writer.get_table_name("users")
        assert spark_writer.get_table_name("test") == sql_writer.get_table_name("test")

    def test_same_transform_schema(self, spark_writer, sql_writer):
        """Test both writers produce same transformed schema."""
        record = {"id": 1, "name": "test"}
        
        spark_result = spark_writer._transform_record(record)
        sql_result = sql_writer._transform_record(record)
        
        # Same keys
        assert set(spark_result.keys()) == set(sql_result.keys())
        
        # Same types
        assert type(spark_result["_airbyte_raw_id"]) == type(sql_result["_airbyte_raw_id"])
        assert type(spark_result["_airbyte_extracted_at"]) == type(sql_result["_airbyte_extracted_at"])
        assert type(spark_result["_airbyte_data"]) == type(sql_result["_airbyte_data"])

