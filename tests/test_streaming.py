"""
Unit tests for StreamingWriter.
"""
from unittest.mock import MagicMock, patch

import pytest

from brickbyte.writers import SQLStreamingWriter


class TestStreamingWriter:

    @pytest.fixture
    def writer(self):
        with patch("databricks.sql.connect") as mock_connect:
            writer = SQLStreamingWriter(
                catalog="main",
                schema="test",
                staging_volume="main.staging.vol",
                server_hostname="test-host",
                http_path="/sql",
                access_token="token",
                buffer_size_records=2
            )
            writer._connection = mock_connect.return_value
            return writer

    def test_init_validation(self):
        """Test validation of staging volume format."""
        with pytest.raises(ValueError):
            SQLStreamingWriter(
                catalog="main",
                schema="test",
                staging_volume="invalid_format", 
                server_hostname="h", 
                http_path="p", 
                access_token="t"
            )

    @patch("pyarrow.parquet.write_table")
    @patch("os.remove")
    @patch("os.makedirs")
    def test_flush_logic(self, mock_makedirs, mock_remove, mock_pq_write, writer):
        """Test that data is flushed correctly when threshold is met."""
        # Mock execution
        writer._execute = MagicMock()
        
        # Write records
        writer.write_record("stream1", {"id": 1})
        assert len(writer._buffers["stream1"]) == 1
        assert writer._execute.call_count == 0  # No flush yet
        
        # Write second record (hits threshold=2)
        writer.write_record("stream1", {"id": 2})
        
        # Should have flushed
        assert len(writer._buffers["stream1"]) == 0
        assert writer._execute.call_count == 2 # CREATE + COPY INTO
        mock_pq_write.assert_called_once()
        mock_remove.assert_called_once()
        
        # Verify COPY INTO query
        copy_call = writer._execute.call_args_list[1]
        query = copy_call[0][0]
        assert "COPY INTO" in query
        assert "main.test.stream1" in query

    def test_close_flushes_remaining(self, writer):
        """Test that close flushes remaining records."""
        writer.flush_stream = MagicMock()
        writer._buffers["s1"] = [{"id": 1}]
        
        writer.close()
        
        writer.flush_stream.assert_called_with("s1")
