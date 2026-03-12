"""
Tests for safe overwrite (staged replace) behavior.
"""
from unittest.mock import MagicMock

import pytest

from brickbyte.writers.spark_streaming_writer import SparkStreamingWriter


class TestSafeOverwrite:
    @pytest.fixture
    def writer(self):
        writer = SparkStreamingWriter(
            catalog="main",
            schema="test",
            buffer_size_records=100,
            run_id="abcdef12-3456-7890-abcd-ef1234567890",
        )
        writer._spark = MagicMock()
        return writer

    def test_staging_table_name_format(self, writer):
        name = writer.get_staging_table_name("users", "abcdef12-3456-7890-abcd-ef1234567890")
        assert "__stg__abcdef12" in name
        assert "`main`.`test`." in name

    def test_safe_overwrite_begin_sets_redirect(self, writer):
        writer.safe_overwrite_begin("users", "abcdef12")
        assert "users" in writer._overwrite_streams

    def test_safe_overwrite_begin_drops_staging(self, writer):
        writer.safe_overwrite_begin("users", "abcdef12")
        writer._spark.sql.assert_called()
        drop_calls = [
            c for c in writer._spark.sql.call_args_list if "DROP TABLE" in str(c)
        ]
        assert len(drop_calls) >= 1

    def test_safe_overwrite_finish_rename_when_no_target(self, writer):
        writer._overwrite_streams["users"] = "`main`.`test`.`users__stg__abcdef12`"
        writer._spark.catalog.tableExists.return_value = False

        writer.safe_overwrite_finish("users", "abcdef12")

        rename_calls = [
            c for c in writer._spark.sql.call_args_list if "ALTER TABLE" in str(c) and "RENAME" in str(c)
        ]
        assert len(rename_calls) == 1

    def test_safe_overwrite_finish_atomic_overwrite_when_target_exists(self, writer):
        writer._overwrite_streams["users"] = "`main`.`test`.`users__stg__abcdef12`"
        writer._spark.catalog.tableExists.return_value = True

        # Mock schemas
        mock_target_df = MagicMock()
        mock_target_field = MagicMock()
        mock_target_field.name = "id"
        mock_target_field.dataType = "StringType"
        mock_target_df.schema.fields = [mock_target_field]

        mock_staging_df = MagicMock()
        mock_staging_field = MagicMock()
        mock_staging_field.name = "id"
        mock_staging_field.dataType = "StringType"
        mock_staging_df.schema.fields = [mock_staging_field]

        writer._spark.table.side_effect = lambda name: {
            "`main`.`test`.`users`": mock_target_df,
            "`main`.`test`.`users__stg__abcdef12`": mock_staging_df,
        }[name]

        writer.safe_overwrite_finish("users", "abcdef12")

        # Should have INSERT OVERWRITE and DROP staging
        sql_calls = [str(c) for c in writer._spark.sql.call_args_list]
        insert_calls = [c for c in sql_calls if "INSERT OVERWRITE" in c]
        drop_calls = [c for c in sql_calls if "DROP TABLE" in c]
        assert len(insert_calls) == 1
        assert len(drop_calls) >= 1

    def test_safe_overwrite_failure_drops_staging(self, writer):
        writer._overwrite_streams["users"] = "`main`.`test`.`users__stg__abcdef12`"
        writer._spark.catalog.tableExists.side_effect = RuntimeError("test error")

        with pytest.raises(RuntimeError):
            writer.safe_overwrite_finish("users", "abcdef12")

        # Staging should be dropped on failure
        drop_calls = [
            c for c in writer._spark.sql.call_args_list if "DROP TABLE" in str(c)
        ]
        assert len(drop_calls) >= 1

    def test_writes_go_to_staging_during_overwrite(self, writer):
        writer.safe_overwrite_begin("stream1", "run123")

        # Write should go to staging table
        write_table = writer._get_write_table_name("stream1")
        assert "__stg__" in write_table

    def test_writes_go_to_target_normally(self, writer):
        write_table = writer._get_write_table_name("stream1")
        assert "__stg__" not in write_table

    def test_schema_alignment_new_columns(self, writer):
        """Staging has new columns -> target gets them via ALTER TABLE ADD."""
        writer._overwrite_streams["users"] = "`main`.`test`.`users__stg__abcdef12`"
        writer._spark.catalog.tableExists.return_value = True

        target_field = MagicMock()
        target_field.name = "id"
        target_field.dataType = "StringType"
        mock_target_df = MagicMock()
        mock_target_df.schema.fields = [target_field]

        staging_field_id = MagicMock()
        staging_field_id.name = "id"
        staging_field_id.dataType = "StringType"
        staging_field_new = MagicMock()
        staging_field_new.name = "email"
        staging_field_new.dataType = "StringType"
        mock_staging_df = MagicMock()
        mock_staging_df.schema.fields = [staging_field_id, staging_field_new]

        writer._spark.table.side_effect = lambda name: (
            mock_target_df
            if "stg" not in name
            else mock_staging_df
        )

        writer.safe_overwrite_finish("users", "abcdef12")

        alter_calls = [
            str(c) for c in writer._spark.sql.call_args_list if "ADD COLUMNS" in str(c)
        ]
        assert len(alter_calls) == 1
        assert "email" in alter_calls[0]

    def test_incompatible_type_change_raises(self, writer):
        """Incompatible type changes should raise ValueError."""
        writer._overwrite_streams["users"] = "`main`.`test`.`users__stg__abcdef12`"
        writer._spark.catalog.tableExists.return_value = True

        target_field = MagicMock()
        target_field.name = "data"
        target_field.dataType = "StructType"
        mock_target_df = MagicMock()
        mock_target_df.schema.fields = [target_field]

        staging_field = MagicMock()
        staging_field.name = "data"
        staging_field.dataType = "ArrayType"
        mock_staging_df = MagicMock()
        mock_staging_df.schema.fields = [staging_field]

        writer._spark.table.side_effect = lambda name: (
            mock_target_df
            if "stg" not in name
            else mock_staging_df
        )

        with pytest.raises(ValueError, match="Incompatible type change"):
            writer.safe_overwrite_finish("users", "abcdef12")

    def test_safe_widening_handles_parenthesized_type_strings(self, writer):
        writer._overwrite_streams["users"] = "`main`.`test`.`users__stg__abcdef12`"
        writer._spark.catalog.tableExists.return_value = True

        target_field = MagicMock()
        target_field.name = "id"
        target_field.dataType = "LongType()"
        mock_target_df = MagicMock()
        mock_target_df.schema.fields = [target_field]

        staging_field = MagicMock()
        staging_field.name = "id"
        staging_field.dataType = "IntegerType()"
        mock_staging_df = MagicMock()
        mock_staging_df.schema.fields = [staging_field]

        writer._spark.table.side_effect = lambda name: (
            mock_target_df if "stg" not in name else mock_staging_df
        )

        writer.safe_overwrite_finish("users", "abcdef12")

        sql_calls = [str(c) for c in writer._spark.sql.call_args_list]
        insert_calls = [c for c in sql_calls if "INSERT OVERWRITE" in c]
        assert len(insert_calls) == 1
        assert "CAST(`id` AS BIGINT)" in insert_calls[0]

    def test_reverse_safe_widening_alters_target_to_sql_type(self, writer):
        writer._overwrite_streams["users"] = "`main`.`test`.`users__stg__abcdef12`"
        writer._spark.catalog.tableExists.return_value = True

        target_field = MagicMock()
        target_field.name = "id"
        target_field.dataType = "IntegerType()"
        mock_target_df = MagicMock()
        mock_target_df.schema.fields = [target_field]

        staging_field = MagicMock()
        staging_field.name = "id"
        staging_field.dataType = "LongType()"
        mock_staging_df = MagicMock()
        mock_staging_df.schema.fields = [staging_field]

        writer._spark.table.side_effect = lambda name: (
            mock_target_df if "stg" not in name else mock_staging_df
        )

        writer.safe_overwrite_finish("users", "abcdef12")

        sql_calls = [str(c) for c in writer._spark.sql.call_args_list]
        alter_calls = [c for c in sql_calls if "ALTER COLUMN `id` TYPE" in c]
        assert len(alter_calls) == 1
        assert "BIGINT" in alter_calls[0]
