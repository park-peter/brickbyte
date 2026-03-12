"""
Tests for Hybrid Streaming Architecture (Spark vs SQL).
"""
import sys
from unittest.mock import MagicMock, patch

import pytest

from brickbyte.writers import create_streaming_writer
from brickbyte.writers.spark_streaming_writer import SparkStreamingWriter
from brickbyte.writers.sql_streaming_writer import SQLStreamingWriter


class TestHybridFactory:
    @patch("os.makedirs")
    def test_factory_detects_spark(self, mock_makedirs):
        with patch.dict(sys.modules, {"pyspark.sql": MagicMock()}):
            mock_session = MagicMock()
            sys.modules["pyspark.sql"].SparkSession.getActiveSession.return_value = (
                mock_session
            )

            writer = create_streaming_writer(
                catalog="main", schema="test", run_id="test-run"
            )

            assert isinstance(writer, SparkStreamingWriter)
            assert writer.catalog == "main"

    def test_factory_fallback_to_sql(self):
        with patch.dict(sys.modules, {"pyspark.sql": None}):
            with patch("databricks.sdk.WorkspaceClient") as mock_ws_client:
                mock_w = MagicMock()
                mock_ws_client.return_value = mock_w
                mock_w.config.host = "https://test-host"
                mock_w.config.token = "token"

                mock_wh = MagicMock()
                mock_wh.state.value = "RUNNING"
                mock_wh.id = "wh-123"
                mock_w.warehouses.list.return_value = [mock_wh]

                with patch("os.path.exists", return_value=True):
                    writer = create_streaming_writer(
                        catalog="main",
                        schema="test",
                        staging_volume="main.test.vol",
                        run_id="test-run",
                    )

                    assert isinstance(writer, SQLStreamingWriter)
                    assert writer.staging_volume == "main.test.vol"

    def test_factory_raises_error_no_volume_no_spark(self):
        with patch.dict(sys.modules, {"pyspark.sql": None}):
            with pytest.raises(ValueError, match="staging_volume is REQUIRED"):
                create_streaming_writer(
                    catalog="main",
                    schema="test",
                    staging_volume=None,
                    run_id="test-run",
                )
