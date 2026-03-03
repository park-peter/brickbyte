"""
Tests for Preview Engine functionality.
"""
from unittest.mock import MagicMock, patch

import pytest

from brickbyte.preview import (
    PreviewEngine,
    PreviewResult,
    SchemaChange,
    StreamPreview,
)


class TestSchemaChange:
    def test_added_column_str(self):
        change = SchemaChange(column="new_col", change_type="added", source_type="str")
        result = str(change)
        assert "new_col" in result
        assert "NEW" in result
        assert "str" in result

    def test_removed_column_str(self):
        change = SchemaChange(
            column="old_col", change_type="removed", target_type="StringType"
        )
        result = str(change)
        assert "old_col" in result
        assert "REMOVED" in result

    def test_type_changed_str(self):
        change = SchemaChange(
            column="col",
            change_type="type_changed",
            source_type="int",
            target_type="StringType",
        )
        result = str(change)
        assert "col" in result
        assert "StringType" in result
        assert "int" in result


class TestStreamPreview:
    def test_str_with_counts(self):
        preview = StreamPreview(
            stream_name="users",
            source_count=100,
            target_count=50,
            new_records=30,
            modified_records=10,
            deleted_records=5,
        )
        result = str(preview)
        assert "users" in result
        assert "+30 new" in result
        assert "~10 modified" in result
        assert "-5 deleted" in result

    def test_str_streaming_unknown_count(self):
        preview = StreamPreview(stream_name="events", source_count=-1, target_count=0)
        result = str(preview)
        assert "events" in result
        assert "Unknown" in result or "Streaming" in result

    def test_str_with_schema_changes(self):
        preview = StreamPreview(
            stream_name="orders",
            source_count=50,
            target_count=40,
            schema_changes=[
                SchemaChange(column="new_field", change_type="added", source_type="str"),
            ],
        )
        result = str(preview)
        assert "Schema changes" in result
        assert "new_field" in result


class TestPreviewResult:
    def test_str_output(self):
        result = PreviewResult(
            streams=[
                StreamPreview("users", 100, 50, new_records=30),
                StreamPreview("orders", 200, 150, new_records=20),
            ],
            total_source_records=300,
            total_new_records=50,
            has_schema_changes=True,
        )
        output = str(result)
        assert "Sync Preview" in output
        assert "users" in output
        assert "orders" in output
        assert "Schema changes detected" in output

    def test_str_no_schema_changes(self):
        result = PreviewResult(
            streams=[StreamPreview("data", 10, 10)], has_schema_changes=False
        )
        output = str(result)
        assert "Schema changes detected" not in output


class TestPreviewEngine:
    @pytest.fixture
    def engine(self):
        engine = PreviewEngine(catalog="main", schema="test")
        engine._spark = MagicMock()
        return engine

    def test_get_table_name(self, engine):
        assert engine.get_table_name("users") == "`main`.`test`.`users`"

    def test_table_exists_true(self, engine):
        engine._spark.catalog.tableExists.return_value = True
        assert engine.table_exists("users") is True

    def test_table_exists_false(self, engine):
        engine._spark.catalog.tableExists.return_value = False
        assert engine.table_exists("users") is False

    def test_table_exists_no_spark(self):
        engine = PreviewEngine(catalog="main", schema="test")
        engine._spark = None
        with patch.object(PreviewEngine, "spark", property(lambda self: None)):
            assert engine.table_exists("users") is False

    def test_get_target_count(self, engine):
        engine._spark.catalog.tableExists.return_value = True
        mock_df = MagicMock()
        mock_df.count.return_value = 42
        engine._spark.table.return_value = mock_df
        assert engine.get_target_count("users") == 42

    def test_get_target_count_no_table(self, engine):
        engine._spark.catalog.tableExists.return_value = False
        assert engine.get_target_count("users") == 0

    def test_get_source_schema(self, engine):
        samples = [{"id": 1, "name": "test", "active": True, "score": 3.14}]
        schema = engine.get_source_schema(samples)
        assert schema["id"] == "int"
        assert schema["name"] == "str"
        assert schema["active"] == "bool"
        assert schema["score"] == "float"

    def test_get_source_schema_empty(self, engine):
        assert engine.get_source_schema([]) == {}

    def test_compare_schemas_added_columns(self, engine):
        source = {"id": "int", "name": "str", "new_col": "str"}
        target = {"id": "LongType", "name": "StringType"}
        changes = engine.compare_schemas(source, target)
        added = [c for c in changes if c.change_type == "added"]
        assert len(added) == 1
        assert added[0].column == "new_col"

    def test_compare_schemas_removed_columns(self, engine):
        source = {"id": "int"}
        target = {"id": "LongType", "old_col": "StringType"}
        changes = engine.compare_schemas(source, target)
        removed = [c for c in changes if c.change_type == "removed"]
        assert len(removed) == 1
        assert removed[0].column == "old_col"

    def test_compare_schemas_type_changes_detected(self, engine):
        source = {"id": "int", "name": "str", "score": "int"}
        target = {"id": "LongType", "name": "StringType", "score": "StringType"}
        changes = engine.compare_schemas(source, target)
        type_changed = [c for c in changes if c.change_type == "type_changed"]
        assert len(type_changed) == 1
        assert type_changed[0].column == "score"

    def test_compare_schemas_no_changes(self, engine):
        source = {"id": "int", "name": "str"}
        target = {"id": "LongType", "name": "StringType"}
        changes = engine.compare_schemas(source, target)
        assert len(changes) == 0

    def test_preview_stream(self, engine):
        engine._spark.catalog.tableExists.return_value = True
        mock_df = MagicMock()
        mock_df.count.return_value = 100
        engine._spark.table.return_value = mock_df
        mock_source = MagicMock()
        mock_source.get_records.return_value = iter(
            [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
        )
        preview = engine.preview_stream(mock_source, "users", sample_size=5)
        assert preview.stream_name == "users"
        assert preview.target_count == 100
        assert len(preview.sample_records) == 2

    def test_preview_all_streams(self, engine):
        engine._spark.catalog.tableExists.return_value = False
        mock_source = MagicMock()
        mock_source.get_records.return_value = iter([{"id": 1}])
        result = engine.preview(mock_source, ["stream1", "stream2"])
        assert len(result.streams) == 2
        assert result.streams[0].stream_name == "stream1"
        assert result.streams[1].stream_name == "stream2"
