"""
Tests for deduplication logic.
"""
from unittest.mock import MagicMock, patch

import pytest

import brickbyte
from brickbyte._dedup import deduplicate_stream
from brickbyte._schema import DK_MISSING
from brickbyte.writers.spark_streaming_writer import SparkStreamingWriter


class TestDedupKeyNormalization:
    @pytest.fixture
    def bb(self, tmp_path):
        return brickbyte.client(base_venv_directory=str(tmp_path))

    def test_deduplicate_true_without_keys_raises(self, bb):
        with pytest.raises(ValueError, match="dedup_keys is required"):
            bb._normalize_dedup_keys(None, None)

    def test_deduplicate_with_empty_list_raises(self, bb):
        with pytest.raises(ValueError, match="non-empty"):
            bb._normalize_dedup_keys([], None)

    def test_deduplicate_with_per_stream_empty_raises(self, bb):
        with pytest.raises(ValueError, match="non-empty"):
            bb._normalize_dedup_keys({"stream": []}, None)

    def test_list_keys_normalized_to_dict(self, bb):
        result = bb._normalize_dedup_keys(["email"], None)
        assert result == {"__all__": ["email"]}

    def test_dict_keys_pass_through(self, bb):
        result = bb._normalize_dedup_keys({"users": ["email"]}, None)
        assert result == {"users": ["email"]}

    def test_invalid_identifier_in_list_raises(self, bb):
        with pytest.raises(ValueError, match="invalid key"):
            bb._normalize_dedup_keys(["bad`key"], None)

    def test_invalid_identifier_in_dict_raises(self, bb):
        with pytest.raises(ValueError, match="invalid key"):
            bb._normalize_dedup_keys({"users": ["bad;key"]}, None)


class TestDedupTransformRecord:
    def test_flatten_mode_dedup_keys_added(self):
        writer = SparkStreamingWriter(
            catalog="main",
            schema="test",
            flatten=True,
            run_id="test-run",
            dedup_keys={"stream1": ["email"]},
        )
        writer._spark = MagicMock()

        record = {"id": 1, "email": "test@example.com"}
        transformed = writer._transform_record("stream1", record)

        assert "_dk_0" in transformed
        assert transformed["_dk_0"] == "test@example.com"
        assert transformed[DK_MISSING] is False

    def test_raw_mode_dedup_keys_extracted_from_source(self):
        writer = SparkStreamingWriter(
            catalog="main",
            schema="test",
            flatten=False,
            run_id="test-run",
            dedup_keys={"stream1": ["user-id"]},
        )
        writer._spark = MagicMock()

        record = {"user-id": "abc123", "name": "test"}
        transformed = writer._transform_record("stream1", record)

        assert "_dk_0" in transformed
        assert transformed["_dk_0"] == "abc123"
        assert transformed[DK_MISSING] is False

    def test_missing_key_sets_dk_missing(self):
        writer = SparkStreamingWriter(
            catalog="main",
            schema="test",
            flatten=True,
            run_id="test-run",
            dedup_keys={"stream1": ["email"]},
        )
        writer._spark = MagicMock()

        record = {"id": 1}  # No email field
        transformed = writer._transform_record("stream1", record)

        assert transformed["_dk_0"] is None
        assert transformed[DK_MISSING] is True

    def test_null_key_value_dk_missing_stays_false(self):
        writer = SparkStreamingWriter(
            catalog="main",
            schema="test",
            flatten=True,
            run_id="test-run",
            dedup_keys={"stream1": ["email"]},
        )
        writer._spark = MagicMock()

        record = {"id": 1, "email": None}
        transformed = writer._transform_record("stream1", record)

        assert transformed["_dk_0"] is None
        assert transformed[DK_MISSING] is False

    def test_no_dedup_keys_no_dk_columns(self):
        writer = SparkStreamingWriter(
            catalog="main",
            schema="test",
            flatten=True,
            run_id="test-run",
        )
        writer._spark = MagicMock()

        record = {"id": 1, "email": "test@example.com"}
        transformed = writer._transform_record("stream1", record)

        assert "_dk_0" not in transformed
        assert DK_MISSING not in transformed

    def test_stream_not_in_dedup_dict_no_dk_columns(self):
        writer = SparkStreamingWriter(
            catalog="main",
            schema="test",
            flatten=True,
            run_id="test-run",
            dedup_keys={"other_stream": ["email"]},
        )
        writer._spark = MagicMock()

        record = {"id": 1, "email": "test@example.com"}
        transformed = writer._transform_record("stream1", record)

        assert "_dk_0" not in transformed
        assert DK_MISSING not in transformed

    def test_multiple_dedup_keys(self):
        writer = SparkStreamingWriter(
            catalog="main",
            schema="test",
            flatten=True,
            run_id="test-run",
            dedup_keys={"stream1": ["email", "phone"]},
        )
        writer._spark = MagicMock()

        record = {"id": 1, "email": "test@example.com", "phone": "555-1234"}
        transformed = writer._transform_record("stream1", record)

        assert transformed["_dk_0"] == "test@example.com"
        assert transformed["_dk_1"] == "555-1234"
        assert transformed[DK_MISSING] is False


class TestDedupKeyValidation:
    @pytest.fixture
    def bb(self, tmp_path):
        return brickbyte.client(base_venv_directory=str(tmp_path))

    def test_dict_key_using_sanitized_name_raises(self, bb, mock_airbyte):
        mock_source = MagicMock()
        mock_airbyte.get_source.return_value = mock_source
        mock_source.get_selected_streams.return_value = ["my-stream"]
        mock_source.get_records.return_value = []

        with patch("brickbyte.writers.create_streaming_writer") as mock_factory:
            mock_writer = MagicMock()
            mock_factory.return_value = mock_writer

            with pytest.raises(ValueError, match="sanitized name"):
                bb.sync(
                    source="source-faker",
                    source_config={},
                    catalog="main",
                    schema="test",
                    staging_volume="main.staging.vol",
                    mode="append",
                    deduplicate=True,
                    dedup_keys={"my_stream": ["email"]},
                )

    def test_unmatched_dict_key_raises(self, bb, mock_airbyte):
        mock_source = MagicMock()
        mock_airbyte.get_source.return_value = mock_source
        mock_source.get_selected_streams.return_value = ["users"]
        mock_source.get_records.return_value = []

        with patch("brickbyte.writers.create_streaming_writer") as mock_factory:
            mock_writer = MagicMock()
            mock_factory.return_value = mock_writer

            with pytest.raises(ValueError, match="does not match"):
                bb.sync(
                    source="source-faker",
                    source_config={},
                    catalog="main",
                    schema="test",
                    staging_volume="main.staging.vol",
                    mode="append",
                    deduplicate=True,
                    dedup_keys={"nonexistent": ["email"]},
                )

    def test_dedup_keys_ignored_when_deduplicate_false(self, bb, mock_airbyte):
        mock_source = MagicMock()
        mock_airbyte.get_source.return_value = mock_source
        mock_source.get_selected_streams.return_value = ["users"]
        mock_source.get_records.return_value = [{"id": 1}]

        with patch("brickbyte.writers.create_streaming_writer") as mock_factory:
            mock_writer = MagicMock()
            mock_factory.return_value = mock_writer

            result = bb.sync(
                source="source-faker",
                source_config={},
                catalog="main",
                schema="test",
                staging_volume="main.staging.vol",
                mode="append",
                deduplicate=False,
                dedup_keys=["email"],
            )

            assert result.records_written == 1


class TestDedupListKeysExpansion:
    """Test that List[str] dedup_keys are expanded to per-stream dict."""

    @pytest.fixture
    def bb(self, tmp_path):
        return brickbyte.client(base_venv_directory=str(tmp_path))

    def test_list_keys_applied_to_all_streams(self, bb, mock_airbyte):
        """List[str] dedup_keys should apply to every selected stream."""
        mock_source = MagicMock()
        mock_airbyte.get_source.return_value = mock_source
        mock_source.get_selected_streams.return_value = ["users", "orders"]
        mock_source.get_records.side_effect = [
            [{"id": 1, "email": "a@b.com"}],
            [{"id": 2, "email": "c@d.com"}],
        ]

        with patch("brickbyte.writers.create_streaming_writer") as mock_factory:
            mock_writer = MagicMock()
            mock_factory.return_value = mock_writer

            result = bb.sync(
                source="source-faker",
                source_config={},
                catalog="main",
                schema="test",
                staging_volume="main.staging.vol",
                mode="append",
                deduplicate=True,
                dedup_keys=["email"],
            )

            assert result.records_written == 2
            # The writer's dedup_keys kwarg should be a per-stream dict,
            # NOT {"__all__": ["email"]}
            call_kwargs = mock_factory.call_args[1]
            dk = call_kwargs["dedup_keys"]
            assert "users" in dk
            assert "orders" in dk
            assert "__all__" not in dk
            assert dk["users"] == ["email"]
            assert dk["orders"] == ["email"]

    def test_dedup_runs_with_executor_in_parallel_mode(self, bb, mock_airbyte):
        """In parallel mode, dedup must not fail with executor=None."""
        mock_source = MagicMock()
        mock_airbyte.get_source.return_value = mock_source
        mock_source.get_selected_streams.return_value = ["users"]
        mock_source.get_records.return_value = [{"id": 1, "email": "a@b.com"}]

        writers_created = []

        def mock_create_writer(**kwargs):
            w = MagicMock()
            w.spark = MagicMock()  # Has spark attr so _execute_sql works
            writers_created.append(w)
            return w

        with patch(
            "brickbyte.writers.create_streaming_writer",
            side_effect=mock_create_writer,
        ):
            result = bb.sync(
                source="source-faker",
                source_config={},
                catalog="main",
                schema="test",
                staging_volume="main.staging.vol",
                mode="append",
                deduplicate=True,
                dedup_keys=["email"],
                max_parallel_streams=2,
            )

            assert result.records_written == 1
            # Dedup MERGE should have been called on the writer
            assert any(
                w.spark.sql.called for w in writers_created
            ), "dedup MERGE should have been invoked on a writer"


class TestRunDedupRouting:
    @pytest.fixture
    def bb(self, tmp_path):
        return brickbyte.client(base_venv_directory=str(tmp_path))

    def test_flatten_mode_uses_internal_dk_columns(self, bb):
        with patch("brickbyte._dedup.deduplicate_stream") as mock_dedup:
            bb._run_dedup_for_stream(
                stream_name="users",
                deduplicate=True,
                normalized_dedup_keys={"users": ["email", "phone"]},
                flatten=True,
                catalog="main",
                schema="test",
                executor_writer=MagicMock(),
            )

        kwargs = mock_dedup.call_args.kwargs
        assert kwargs["key_columns"] == ["_dk_0", "_dk_1"]
        assert kwargs["run_id_col"] == "_run_id"
        assert kwargs["extracted_at_col"] == "_extracted_at"
        assert kwargs["record_id_col"] == "_record_id"


class TestDeduplicateStream:
    def test_deduplicate_executes_merge(self):
        mock_executor = MagicMock()
        mock_executor.spark = MagicMock()

        deduplicate_stream(
            executor=mock_executor,
            table_name="`main`.`test`.`users`",
            key_columns=["_dk_0"],
            run_id_col="run_id",
            extracted_at_col="extracted_at",
            record_id_col="record_id",
            flatten=False,
        )

        mock_executor.spark.sql.assert_called_once()
        call_args = str(mock_executor.spark.sql.call_args)
        assert "MERGE INTO" in call_args
        assert "_dk_0" in call_args

    def test_deduplicate_empty_keys_noop(self):
        mock_executor = MagicMock()
        deduplicate_stream(
            executor=mock_executor,
            table_name="`main`.`test`.`users`",
            key_columns=[],
            run_id_col="run_id",
            extracted_at_col="extracted_at",
            record_id_col="record_id",
        )
        mock_executor.spark.sql.assert_not_called()

    def test_deduplicate_invalid_key_identifier_raises(self):
        mock_executor = MagicMock()
        mock_executor.spark = MagicMock()

        with pytest.raises(ValueError, match="unsafe character"):
            deduplicate_stream(
                executor=mock_executor,
                table_name="`main`.`test`.`users`",
                key_columns=["bad`col"],
                run_id_col="run_id",
                extracted_at_col="extracted_at",
                record_id_col="record_id",
            )
