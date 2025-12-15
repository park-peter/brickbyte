"""
Verification tests for BrickByte functionalities (Streaming Only).
"""
from unittest.mock import MagicMock, patch

import pytest

from brickbyte import BrickByte


class TestBrickByteFunctional:
    
    @pytest.fixture
    def mock_airbyte(self):
        import sys
        mock_ab = MagicMock()
        with patch.dict(sys.modules, {"airbyte": mock_ab}):
            yield mock_ab

    @pytest.fixture
    def brickbyte(self, tmp_path):
        return BrickByte(base_venv_directory=str(tmp_path))

    def test_sync_streaming_default(self, brickbyte, mock_airbyte):
        """Test the sync method with default streaming behavior."""
        # Mock dependencies
        mock_source = MagicMock()
        mock_airbyte.get_source.return_value = mock_source
        mock_source.get_selected_streams.return_value = ["test_stream"]
        
        # Mock records generator
        mock_source.get_records.return_value = [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]
        
        # Mock writer factory
        with patch("brickbyte.writers.create_streaming_writer") as mock_create_writer:
            mock_writer = MagicMock()
            mock_create_writer.return_value = mock_writer
            
            result = brickbyte.sync(
                source="source-faker",
                source_config={},
                catalog="main",
                schema="test",
                staging_volume="main.staging.vol"
            )
            
            # Verifications
            assert result.records_written == 2
            mock_create_writer.assert_called_once()
            assert mock_writer.write_record.call_count == 2
            mock_writer.flush_stream.assert_called_with("test_stream")
            mock_writer.close.assert_called_once()
