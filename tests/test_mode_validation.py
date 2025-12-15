"""
Tests for sync mode validation and overwrite behavior.
"""
from unittest.mock import MagicMock, patch

import pytest

from brickbyte import BrickByte


class TestModeValidation:
    """Test _validate_sync_params mode validation."""

    @pytest.fixture
    def brickbyte(self, tmp_path):
        return BrickByte(base_venv_directory=str(tmp_path))

    def test_append_mode_valid(self, brickbyte):
        """Test that append mode is valid."""
        # Should not raise
        brickbyte._validate_sync_params(mode="append", staging_volume="a.b.c")

    def test_overwrite_mode_valid(self, brickbyte):
        """Test that overwrite mode is valid."""
        # Should not raise
        brickbyte._validate_sync_params(mode="overwrite", staging_volume="a.b.c")

    def test_merge_mode_not_implemented(self, brickbyte):
        """Test that merge mode raises NotImplementedError."""
        with pytest.raises(NotImplementedError, match="Merge mode is not yet supported"):
            brickbyte._validate_sync_params(mode="merge", staging_volume="a.b.c")

    def test_invalid_mode_raises_error(self, brickbyte):
        """Test that invalid mode raises ValueError."""
        with pytest.raises(ValueError, match="Invalid mode 'invalid'"):
            brickbyte._validate_sync_params(mode="invalid", staging_volume="a.b.c")

    def test_unknown_mode_raises_error(self, brickbyte):
        """Test that unknown modes raise ValueError."""
        with pytest.raises(ValueError, match="Invalid mode"):
            brickbyte._validate_sync_params(mode="upsert", staging_volume="a.b.c")


class TestOverwriteMode:
    """Test overwrite mode behavior (drop_table before sync)."""

    @pytest.fixture
    def mock_airbyte(self):
        import sys
        mock_ab = MagicMock()
        with patch.dict(sys.modules, {"airbyte": mock_ab}):
            yield mock_ab

    @pytest.fixture
    def brickbyte(self, tmp_path):
        return BrickByte(base_venv_directory=str(tmp_path))

    def test_overwrite_drops_table_before_streaming(self, brickbyte, mock_airbyte):
        """Test that overwrite mode calls drop_table before streaming."""
        mock_source = MagicMock()
        mock_airbyte.get_source.return_value = mock_source
        mock_source.get_selected_streams.return_value = ["users", "orders"]
        mock_source.get_records.return_value = [{"id": 1}]

        with patch("brickbyte.writers.create_streaming_writer") as mock_factory:
            mock_writer = MagicMock()
            mock_factory.return_value = mock_writer

            brickbyte.sync(
                source="source-faker",
                source_config={},
                catalog="main",
                schema="test",
                staging_volume="main.staging.vol",
                mode="overwrite",
            )

            # Verify drop_table was called for each stream
            assert mock_writer.drop_table.call_count == 2
            mock_writer.drop_table.assert_any_call("users")
            mock_writer.drop_table.assert_any_call("orders")

    def test_append_does_not_drop_table(self, brickbyte, mock_airbyte):
        """Test that append mode does NOT call drop_table."""
        mock_source = MagicMock()
        mock_airbyte.get_source.return_value = mock_source
        mock_source.get_selected_streams.return_value = ["users"]
        mock_source.get_records.return_value = [{"id": 1}]

        with patch("brickbyte.writers.create_streaming_writer") as mock_factory:
            mock_writer = MagicMock()
            mock_factory.return_value = mock_writer

            brickbyte.sync(
                source="source-faker",
                source_config={},
                catalog="main",
                schema="test",
                staging_volume="main.staging.vol",
                mode="append",
            )

            # Verify drop_table was NOT called
            mock_writer.drop_table.assert_not_called()


class TestSyncModeIntegration:
    """Integration tests for sync with different modes."""

    @pytest.fixture
    def mock_airbyte(self):
        import sys
        mock_ab = MagicMock()
        with patch.dict(sys.modules, {"airbyte": mock_ab}):
            yield mock_ab

    @pytest.fixture
    def brickbyte(self, tmp_path):
        return BrickByte(base_venv_directory=str(tmp_path))

    def test_default_mode_is_overwrite(self, brickbyte, mock_airbyte):
        """Test that default mode is overwrite."""
        mock_source = MagicMock()
        mock_airbyte.get_source.return_value = mock_source
        mock_source.get_selected_streams.return_value = ["stream1"]
        mock_source.get_records.return_value = []

        with patch("brickbyte.writers.create_streaming_writer") as mock_factory:
            mock_writer = MagicMock()
            mock_factory.return_value = mock_writer

            # Call without specifying mode
            brickbyte.sync(
                source="source-faker",
                source_config={},
                catalog="main",
                schema="test",
                staging_volume="main.staging.vol",
            )

            # Default is overwrite, so drop_table should be called
            mock_writer.drop_table.assert_called_once_with("stream1")

