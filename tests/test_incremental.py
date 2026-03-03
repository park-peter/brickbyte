"""
Tests for incremental sync state management.
"""
from unittest.mock import MagicMock, patch

import pytest

from brickbyte._state import StateManager


class TestStateManager:
    @pytest.fixture
    def state_mgr(self):
        mgr = StateManager(catalog="main", schema="test")
        mgr._spark = MagicMock()
        mgr._initialized = True
        return mgr

    def test_state_table_name(self, state_mgr):
        assert "__brickbyte_state" in state_mgr._state_table

    def test_save_state_calls_merge(self, state_mgr):
        state_mgr.save_state(
            source="source-faker",
            stream_name="users",
            state={"cursor": "2024-01-01"},
            run_id="test-run",
        )
        state_mgr._spark.sql.assert_called_once()
        call_args = str(state_mgr._spark.sql.call_args)
        assert "MERGE INTO" in call_args

    @patch("brickbyte._state.col", create=True)
    def test_get_state_returns_parsed_json(self, mock_col, state_mgr):
        mock_df = MagicMock()
        mock_row = MagicMock()
        mock_row.__getitem__ = lambda self, key: '{"cursor": "2024-01-01"}'
        mock_df.collect.return_value = [mock_row]

        state_mgr._spark.table.return_value.filter.return_value.select.return_value.limit.return_value = mock_df

        with patch.dict("sys.modules", {"pyspark.sql.functions": MagicMock()}):
            result = state_mgr.get_state("source-faker", "users")
        assert result == {"cursor": "2024-01-01"}

    def test_get_state_returns_none_when_empty(self, state_mgr):
        mock_df = MagicMock()
        mock_df.collect.return_value = []
        state_mgr._spark.table.return_value.filter.return_value.select.return_value.limit.return_value = mock_df

        with patch.dict("sys.modules", {"pyspark.sql.functions": MagicMock()}):
            result = state_mgr.get_state("source-faker", "users")
        assert result is None

    def test_ensure_table_creates_ddl(self):
        mgr = StateManager(catalog="main", schema="test")
        mgr._spark = MagicMock()
        mgr._initialized = False
        mgr._ensure_table()

        mgr._spark.sql.assert_called_once()
        call_args = str(mgr._spark.sql.call_args)
        assert "CREATE TABLE IF NOT EXISTS" in call_args
        assert mgr._initialized is True

    def test_clear_state(self, state_mgr):
        state_mgr.clear_state("source-faker", "users")
        state_mgr._spark.sql.assert_called_once()
        call_args = str(state_mgr._spark.sql.call_args)
        assert "DELETE FROM" in call_args
