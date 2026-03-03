"""
Verification tests for brickbyte functionalities (Streaming Only).
"""
from unittest.mock import MagicMock, patch

import pytest

import brickbyte


class TestBrickbyteFunctional:
    @pytest.fixture
    def bb(self, tmp_path):
        return brickbyte.client(base_venv_directory=str(tmp_path))

    def test_sync_streaming_default(self, bb, mock_airbyte):
        mock_source = MagicMock()
        mock_airbyte.get_source.return_value = mock_source
        mock_source.get_selected_streams.return_value = ["test_stream"]
        mock_source.get_records.return_value = [
            {"id": 1, "val": "a"},
            {"id": 2, "val": "b"},
        ]

        with patch("brickbyte.writers.create_streaming_writer") as mock_create_writer:
            mock_writer = MagicMock()
            mock_create_writer.return_value = mock_writer

            result = bb.sync(
                source="source-faker",
                source_config={},
                catalog="main",
                schema="test",
                staging_volume="main.staging.vol",
                mode="append",
            )

            assert result.records_written == 2
            mock_create_writer.assert_called_once()
            assert mock_writer.write_record.call_count == 2
            mock_writer.flush_stream.assert_called_with("test_stream")

    def test_incremental_applies_saved_state(self, bb, mock_airbyte):
        mock_source = MagicMock()
        mock_airbyte.get_source.return_value = mock_source
        mock_source.get_selected_streams.return_value = ["users"]
        mock_source.get_records.return_value = [{"id": 1}]
        mock_source.set_stream_state = MagicMock()
        mock_source.get_stream_state.return_value = {"cursor": "2024-01-02"}

        with patch("brickbyte._state.StateManager") as mock_state_manager_cls:
            mock_state_manager = MagicMock()
            mock_state_manager.get_state.return_value = {"cursor": "2024-01-01"}
            mock_state_manager_cls.return_value = mock_state_manager

            with patch("brickbyte.writers.create_streaming_writer") as mock_create_writer:
                mock_writer = MagicMock()
                mock_create_writer.return_value = mock_writer

                result = bb.sync(
                    source="source-faker",
                    source_config={},
                    catalog="main",
                    schema="test",
                    staging_volume="main.staging.vol",
                    mode="append",
                    incremental=True,
                )

        assert result.records_written == 1
        mock_source.set_stream_state.assert_called_once_with(
            "users",
            {"cursor": "2024-01-01"},
        )
        mock_state_manager.save_state.assert_called_once()

    def test_incremental_with_saved_state_without_state_api_raises(self, bb, mock_airbyte):
        mock_source = MagicMock(
            spec=[
                "check",
                "select_all_streams",
                "get_selected_streams",
                "get_records",
            ]
        )
        mock_airbyte.get_source.return_value = mock_source
        mock_source.get_selected_streams.return_value = ["users"]
        mock_source.get_records.return_value = []

        with patch("brickbyte._state.StateManager") as mock_state_manager_cls:
            mock_state_manager = MagicMock()
            mock_state_manager.get_state.return_value = {"cursor": "2024-01-01"}
            mock_state_manager_cls.return_value = mock_state_manager

            with pytest.raises(NotImplementedError, match="state injection support"):
                bb.sync(
                    source="source-faker",
                    source_config={},
                    catalog="main",
                    schema="test",
                    staging_volume="main.staging.vol",
                    mode="append",
                    incremental=True,
                )

    def test_progress_reporter_closed_on_error(self, bb, mock_airbyte):
        mock_source = MagicMock()
        mock_airbyte.get_source.return_value = mock_source
        mock_source.get_selected_streams.return_value = ["users"]
        mock_source.get_records.side_effect = RuntimeError("boom")

        with patch("brickbyte.writers.create_streaming_writer") as mock_create_writer:
            mock_writer = MagicMock()
            mock_create_writer.return_value = mock_writer

            with patch("brickbyte._progress.ProgressReporter") as mock_reporter_cls:
                reporter = MagicMock()
                mock_reporter_cls.return_value = reporter

                with pytest.raises(RuntimeError, match="boom"):
                    bb.sync(
                        source="source-faker",
                        source_config={},
                        catalog="main",
                        schema="test",
                        staging_volume="main.staging.vol",
                        mode="append",
                        progress_callback=lambda _evt: None,
                    )

        reporter.close.assert_called_once()

    def test_client_factory_returns_client(self):
        bb = brickbyte.client()
        assert type(bb).__name__ == "Client"

    def test_sync_result_dataclass(self):
        result = brickbyte.SyncResult(
            records_written=100,
            streams_synced=["a", "b"],
            failed_streams=["c"],
            enriched_tables=["a"],
        )
        assert result.records_written == 100
        assert len(result.streams_synced) == 2
