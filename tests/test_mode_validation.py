"""
Tests for sync mode validation and overwrite behavior.
"""
from unittest.mock import MagicMock, patch

import pytest

import brickbyte


class TestModeValidation:
    @pytest.fixture
    def bb(self, tmp_path):
        return brickbyte.client(base_venv_directory=str(tmp_path))

    def test_append_mode_valid(self, bb):
        bb._validate_sync_params(mode="append")

    def test_overwrite_mode_valid(self, bb):
        bb._validate_sync_params(mode="overwrite")

    def test_merge_mode_not_implemented(self, bb):
        with pytest.raises(NotImplementedError, match="Merge mode is not yet supported"):
            bb._validate_sync_params(mode="merge")

    def test_invalid_mode_raises_error(self, bb):
        with pytest.raises(ValueError, match="Invalid mode 'invalid'"):
            bb._validate_sync_params(mode="invalid")

    def test_unknown_mode_raises_error(self, bb):
        with pytest.raises(ValueError, match="Invalid mode"):
            bb._validate_sync_params(mode="upsert")


class TestOverwriteMode:
    @pytest.fixture
    def bb(self, tmp_path):
        return brickbyte.client(base_venv_directory=str(tmp_path))

    def test_overwrite_uses_safe_overwrite(self, bb, mock_airbyte):
        mock_source = MagicMock()
        mock_airbyte.get_source.return_value = mock_source
        mock_source.get_selected_streams.return_value = ["users", "orders"]
        mock_source.get_records.return_value = [{"id": 1}]

        with patch("brickbyte.writers.create_streaming_writer") as mock_factory:
            mock_writer = MagicMock()
            mock_factory.return_value = mock_writer

            bb.sync(
                source="source-faker",
                source_config={},
                catalog="main",
                schema="test",
                staging_volume="main.staging.vol",
                mode="overwrite",
            )

            # Should use safe_overwrite_begin/finish instead of drop_table
            assert mock_writer.safe_overwrite_begin.call_count == 2
            assert mock_writer.safe_overwrite_finish.call_count == 2
            mock_writer.drop_table.assert_not_called()

    def test_append_does_not_drop_table(self, bb, mock_airbyte):
        mock_source = MagicMock()
        mock_airbyte.get_source.return_value = mock_source
        mock_source.get_selected_streams.return_value = ["users"]
        mock_source.get_records.return_value = [{"id": 1}]

        with patch("brickbyte.writers.create_streaming_writer") as mock_factory:
            mock_writer = MagicMock()
            mock_factory.return_value = mock_writer

            bb.sync(
                source="source-faker",
                source_config={},
                catalog="main",
                schema="test",
                staging_volume="main.staging.vol",
                mode="append",
            )

            mock_writer.drop_table.assert_not_called()
            mock_writer.safe_overwrite_begin.assert_not_called()


class TestSyncModeIntegration:
    @pytest.fixture
    def bb(self, tmp_path):
        return brickbyte.client(base_venv_directory=str(tmp_path))

    def test_default_mode_is_overwrite(self, bb, mock_airbyte):
        mock_source = MagicMock()
        mock_airbyte.get_source.return_value = mock_source
        mock_source.get_selected_streams.return_value = ["stream1"]
        mock_source.get_records.return_value = []

        with patch("brickbyte.writers.create_streaming_writer") as mock_factory:
            mock_writer = MagicMock()
            mock_factory.return_value = mock_writer

            bb.sync(
                source="source-faker",
                source_config={},
                catalog="main",
                schema="test",
                staging_volume="main.staging.vol",
            )

            # Default is overwrite, so safe_overwrite should be called
            mock_writer.safe_overwrite_begin.assert_called_once_with("stream1", mock_factory.call_args[1]["run_id"])
