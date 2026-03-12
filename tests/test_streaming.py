"""
Unit tests for SQLStreamingWriter.
"""
from unittest.mock import MagicMock, patch

import pytest

from brickbyte.writers.sql_streaming_writer import SQLStreamingWriter


class TestStreamingWriter:
    @pytest.fixture
    def writer(self):
        with patch("os.path.exists", return_value=True):
            writer = SQLStreamingWriter(
                catalog="main",
                schema="test",
                staging_volume="main.staging.vol",
                server_hostname="test-host",
                http_path="/sql",
                access_token="token",
                buffer_size_records=2,
                run_id="test-run-id",
            )
            writer._connection = MagicMock()
            return writer

    def test_init_validation(self):
        with pytest.raises(ValueError):
            SQLStreamingWriter(
                catalog="main",
                schema="test",
                staging_volume="invalid_format",
                server_hostname="h",
                http_path="p",
                access_token="t",
            )

    @patch("pyarrow.parquet.write_table")
    @patch("os.path.exists", return_value=True)
    @patch("os.remove")
    @patch("os.makedirs")
    def test_flush_logic(self, mock_makedirs, mock_remove, mock_exists, mock_pq_write, writer):
        writer._execute = MagicMock()

        writer.write_record("stream1", {"id": 1})
        assert len(writer._buffers["stream1"]) == 1
        assert writer._execute.call_count == 0

        writer.write_record("stream1", {"id": 2})
        assert len(writer._buffers["stream1"]) == 0
        assert writer._execute.call_count == 4  # CREATE + PUT + COPY INTO + REMOVE
        mock_pq_write.assert_called_once()
        mock_remove.assert_called_once()

        put_call = writer._execute.call_args_list[1]
        assert "PUT" in put_call[0][0]

        copy_call = writer._execute.call_args_list[2]
        query = copy_call[0][0]
        assert "COPY INTO" in query
        assert "force" not in query.lower()

        remove_call = writer._execute.call_args_list[3]
        assert "REMOVE" in remove_call[0][0]

    @patch("pyarrow.parquet.write_table")
    @patch("os.remove")
    @patch("os.makedirs")
    def test_deterministic_filenames(self, mock_makedirs, mock_remove, mock_pq_write, writer):
        writer._execute = MagicMock()

        writer.write_record("stream1", {"id": 1})
        writer.write_record("stream1", {"id": 2})

        # Check filename contains run_id and batch index
        pq_call_args = mock_pq_write.call_args
        file_path = pq_call_args[0][1]
        assert "test-run-id" in file_path
        assert "000000" in file_path

    def test_close_flushes_remaining(self, writer):
        writer.flush_stream = MagicMock()
        writer._buffers["s1"] = [{"id": 1}]
        writer.close()
        writer.flush_stream.assert_called_with("s1")
