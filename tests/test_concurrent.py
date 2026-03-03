"""
Tests for concurrent stream processing.
"""
from unittest.mock import MagicMock, patch

import pytest

import brickbyte


class TestConcurrentStreams:
    @pytest.fixture
    def bb(self, tmp_path):
        return brickbyte.client(base_venv_directory=str(tmp_path))

    def test_parallel_streams_each_get_own_writer(self, bb, mock_airbyte):
        mock_source = MagicMock()
        mock_airbyte.get_source.return_value = mock_source
        mock_source.get_selected_streams.return_value = [
            "stream1",
            "stream2",
            "stream3",
        ]
        mock_source.get_records.side_effect = [
            [{"id": 1}],
            [{"id": 2}],
            [{"id": 3}],
        ]

        writers_created = []

        def mock_create_writer(**kwargs):
            w = MagicMock()
            writers_created.append(w)
            return w

        with patch(
            "brickbyte.writers.create_streaming_writer", side_effect=mock_create_writer
        ):
            result = bb.sync(
                source="source-faker",
                source_config={},
                catalog="main",
                schema="test",
                staging_volume="main.staging.vol",
                mode="append",
                max_parallel_streams=3,
            )

        assert result.records_written == 3
        # Each stream gets its own writer (in thread pool) + no sequential writer
        assert len(writers_created) == 3

    def test_error_propagation_with_continue_on_error_false(self, bb, mock_airbyte):
        mock_source = MagicMock()
        mock_airbyte.get_source.return_value = mock_source
        mock_source.get_selected_streams.return_value = ["stream1", "stream2"]
        mock_source.get_records.side_effect = [
            RuntimeError("connection failed"),
            [{"id": 1}],
        ]

        with patch("brickbyte.writers.create_streaming_writer") as mock_factory:
            mock_writer = MagicMock()
            mock_factory.return_value = mock_writer

            with pytest.raises(RuntimeError, match="connection failed"):
                bb.sync(
                    source="source-faker",
                    source_config={},
                    catalog="main",
                    schema="test",
                    staging_volume="main.staging.vol",
                    mode="append",
                    max_parallel_streams=2,
                    continue_on_error=False,
                )

    def test_sequential_mode_uses_single_writer(self, bb, mock_airbyte):
        mock_source = MagicMock()
        mock_airbyte.get_source.return_value = mock_source
        mock_source.get_selected_streams.return_value = ["stream1", "stream2"]
        mock_source.get_records.side_effect = [[{"id": 1}], [{"id": 2}]]

        with patch(
            "brickbyte.writers.create_streaming_writer"
        ) as mock_factory:
            mock_writer = MagicMock()
            mock_factory.return_value = mock_writer

            result = bb.sync(
                source="source-faker",
                source_config={},
                catalog="main",
                schema="test",
                staging_volume="main.staging.vol",
                mode="append",
                max_parallel_streams=1,
            )

        assert result.records_written == 2
        # Single writer for sequential mode
        mock_factory.assert_called_once()

    def test_parallel_incremental_saves_state_per_stream(self, bb, mock_airbyte):
        mock_source = MagicMock()
        mock_airbyte.get_source.return_value = mock_source
        mock_source.get_selected_streams.return_value = ["users", "orders"]
        mock_source.get_records.side_effect = [
            [{"id": 1}],
            [{"id": 2}],
        ]
        mock_source.get_state.return_value = None

        with patch("brickbyte._state.StateManager") as mock_state_manager_cls:
            mock_state_manager = MagicMock()
            mock_state_manager.get_state.return_value = None
            mock_state_manager_cls.return_value = mock_state_manager

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
                    max_parallel_streams=2,
                    incremental=True,
                )

        assert result.records_written == 2
        assert mock_state_manager.save_state.call_count == 2
