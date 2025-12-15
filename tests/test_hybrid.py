"""
Tests for Hybrid Streaming Architecture (Spark vs SQL).
"""
import sys
from unittest.mock import MagicMock, patch

import pytest

from brickbyte.writers import SparkStreamingWriter, SQLStreamingWriter, create_streaming_writer


class TestHybridFactory:

    @patch("os.makedirs")
    def test_factory_detects_spark(self, mock_makedirs):
        """Test that SparkStreamingWriter is created when Spark is active."""
        
        # Mock pyspark.sql.SparkSession.getActiveSession
        with patch.dict(sys.modules, {"pyspark.sql": MagicMock()}):
            mock_session = MagicMock()
            sys.modules["pyspark.sql"].SparkSession.getActiveSession.return_value = mock_session
            
            writer = create_streaming_writer(
                catalog="main",
                schema="test"
            )
            
            assert isinstance(writer, SparkStreamingWriter)
            assert writer.catalog == "main"

    def test_factory_fallback_to_sql(self):
        """Test that SQLStreamingWriter is created when Spark is missing."""
        
        # Simulate import error for pyspark
        with patch.dict(sys.modules, {"pyspark.sql": None}):
            # We also need to mock databricks.sdk
            with patch("databricks.sdk.WorkspaceClient") as mock_ws_client:
                mock_w = MagicMock()
                mock_ws_client.return_value = mock_w
                mock_w.config.host = "https://test-host"
                mock_w.config.token = "token"
                
                # Mock warehouse listing
                mock_wh = MagicMock()
                mock_wh.state.value = "RUNNING"
                mock_wh.id = "wh-123"
                mock_w.warehouses.list.return_value = [mock_wh]
                
                writer = create_streaming_writer(
                    catalog="main",
                    schema="test",
                    staging_volume="main.test.vol"
                )
                
                assert isinstance(writer, SQLStreamingWriter)
                assert writer.staging_volume == "main.test.vol"

    def test_factory_raises_error_no_volume_no_spark(self):
        """Test that ValueError is raised if no Spark and no Volume."""
        
        with patch.dict(sys.modules, {"pyspark.sql": None}):
             with pytest.raises(ValueError, match="staging_volume is REQUIRED"):
                create_streaming_writer(
                    catalog="main",
                    schema="test",
                    staging_volume=None
                )
