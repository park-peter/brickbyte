"""
Tests for concurrent stream processing.
"""
import threading
from unittest.mock import MagicMock, patch

import pytest

import brickbyte


class TestConcurrentStreams:
    @pytest.fixture
    def bb(self, tmp_path):
        return brickbyte.client(base_venv_directory=str(tmp_path))

    def _set_up_mock_sources(self, mock_airbyte, stream_records, stream_states=None):
        stream_names = list(stream_records)

        def make_source():
            mock_source = MagicMock()
            selected = {"streams": list(stream_names)}

            def select_all_streams():
                selected["streams"] = list(stream_names)

            def select_streams(streams):
                selected["streams"] = list(streams)

            def get_selected_streams():
                return list(selected["streams"])

            def get_records(stream_name):
                behavior = stream_records[stream_name]
                if isinstance(behavior, Exception):
                    raise behavior
                return iter(behavior)

            mock_source.select_all_streams.side_effect = select_all_streams
            mock_source.select_streams.side_effect = select_streams
            mock_source.get_selected_streams.side_effect = get_selected_streams
            mock_source.get_records.side_effect = get_records
            mock_source.check.return_value = None

            if stream_states is not None:
                mock_source.get_stream_state.side_effect = (
                    lambda stream_name: stream_states[stream_name]
                )

            return mock_source

        mock_airbyte.get_source.side_effect = lambda *args, **kwargs: make_source()

    def test_parallel_streams_each_get_own_writer(self, bb, mock_airbyte):
        self._set_up_mock_sources(
            mock_airbyte,
            {
                "stream1": [{"id": 1}],
                "stream2": [{"id": 2}],
                "stream3": [{"id": 3}],
            },
        )

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

    def test_parallel_mode_completes_when_streams_exceed_workers(self, bb, mock_airbyte):
        self._set_up_mock_sources(
            mock_airbyte,
            {
                "stream1": [{"id": 1}],
                "stream2": [{"id": 2}],
                "stream3": [{"id": 3}],
            },
        )

        result_holder = {}
        error_holder = {}

        with patch("brickbyte.writers.create_streaming_writer") as mock_factory:
            mock_factory.return_value = MagicMock()

            def run_sync():
                try:
                    result_holder["result"] = bb.sync(
                        source="source-faker",
                        source_config={},
                        catalog="main",
                        schema="test",
                        staging_volume="main.staging.vol",
                        mode="append",
                        max_parallel_streams=2,
                    )
                except Exception as e:  # pragma: no cover - assertion below
                    error_holder["error"] = e

            thread = threading.Thread(target=run_sync, daemon=True)
            thread.start()
            thread.join(1)

        assert thread.is_alive() is False
        assert "error" not in error_holder
        assert result_holder["result"].records_written == 3

    def test_error_propagation_with_continue_on_error_false(self, bb, mock_airbyte):
        self._set_up_mock_sources(
            mock_airbyte,
            {
                "stream1": RuntimeError("connection failed"),
                "stream2": [{"id": 1}],
            },
        )

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
        self._set_up_mock_sources(
            mock_airbyte,
            {
                "stream1": [{"id": 1}],
                "stream2": [{"id": 2}],
            },
        )

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
        self._set_up_mock_sources(
            mock_airbyte,
            {
                "users": [{"id": 1}],
                "orders": [{"id": 2}],
            },
            stream_states={
                "users": {"cursor": "users"},
                "orders": {"cursor": "orders"},
            },
        )

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
