"""
Tests for SQL-mode enrichment.
"""
from unittest.mock import MagicMock, patch

import pytest

from brickbyte.enrichment.semantic import (
    ColumnEnrichment,
    SQLSemanticEnricher,
    TableEnrichment,
)


class TestSQLSemanticEnricher:
    @pytest.fixture
    def enricher(self):
        with patch("databricks.sql.connect") as mock_connect:
            e = SQLSemanticEnricher(
                server_hostname="host",
                http_path="/sql",
                access_token="token",
                catalog="main",
                schema="test",
            )
            e._connection = mock_connect.return_value
            return e

    def test_get_column_samples_raw_mode(self, enricher):
        cursor = MagicMock()
        enricher._connection.cursor.return_value = cursor

        # DESCRIBE returns data column
        cursor.fetchall.side_effect = [
            [("record_id",), ("extracted_at",), ("data",), ("run_id",)],
            [('{"name": "Alice", "email": "alice@example.com"}',)],
        ]

        samples = enricher._get_column_samples("`main`.`test`.`users`")

        assert "name" in samples
        assert "email" in samples
        assert samples["name"] == ["Alice"]

    def test_get_column_samples_flatten_mode(self, enricher):
        cursor = MagicMock()
        enricher._connection.cursor.return_value = cursor

        # DESCRIBE returns no data column (flatten mode)
        cursor.fetchall.side_effect = [
            [("name",), ("email",), ("_record_id",), ("_extracted_at",)],
            [("Alice", "alice@example.com"), ("Bob", "bob@example.com")],
        ]

        samples = enricher._get_column_samples("`main`.`test`.`users`")

        assert "name" in samples
        assert "email" in samples
        assert "_record_id" not in samples  # underscore-prefixed excluded

    def test_apply_to_catalog_column_comments_in_flatten(self, enricher):
        cursor = MagicMock()
        enricher._connection.cursor.return_value = cursor

        # DESCRIBE returns flatten columns (no data column)
        cursor.fetchall.return_value = [
            ("name",),
            ("email",),
            ("_record_id",),
        ]

        enrichment = TableEnrichment(
            table_name="`main`.`test`.`users`",
            columns=[
                ColumnEnrichment(
                    column_name="name",
                    description="User full name",
                ),
                ColumnEnrichment(
                    column_name="email",
                    description="User email address",
                    is_pii=True,
                    pii_type="email",
                ),
            ],
        )

        enricher.apply_to_catalog(enrichment)

        # Should have COMMENT ON COLUMN calls
        execute_calls = [str(c) for c in cursor.execute.call_args_list]
        comment_calls = [c for c in execute_calls if "COMMENT ON COLUMN" in c]
        assert len(comment_calls) >= 1

    def test_close_connection(self):
        with patch("databricks.sql.connect") as mock_connect:
            mock_conn = MagicMock()
            mock_connect.return_value = mock_conn

            e = SQLSemanticEnricher(
                server_hostname="host",
                http_path="/sql",
                access_token="token",
                catalog="main",
                schema="test",
            )
            # Force connection creation
            e._connection = mock_conn
            e.close()
            mock_conn.close.assert_called_once()
