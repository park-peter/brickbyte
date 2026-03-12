"""
Tests for stream name sanitization and SQL identifier validation.
"""
import pytest

from brickbyte._sanitize import quoted_table_name, sanitize_stream_name, validate_identifier


class TestSanitizeStreamName:
    def test_hyphen_to_underscore(self):
        assert sanitize_stream_name("my-stream") == "my_stream"

    def test_dot_to_underscore(self):
        assert sanitize_stream_name("my.stream") == "my_stream"

    def test_space_to_underscore(self):
        assert sanitize_stream_name("my stream") == "my_stream"

    def test_mixed_separators(self):
        assert sanitize_stream_name("my-stream.v2") == "my_stream_v2"

    def test_leading_digit_prefix(self):
        assert sanitize_stream_name("123stream") == "_123stream"

    def test_valid_name_unchanged(self):
        assert sanitize_stream_name("users") == "users"

    def test_uppercase_lowered(self):
        assert sanitize_stream_name("MyStream") == "mystream"

    def test_collision_detection_in_client(self):
        """Two streams that collide after sanitization should be caught."""
        # This would be detected in _client.py during sync
        name1 = sanitize_stream_name("a-b")
        name2 = sanitize_stream_name("a.b")
        assert name1 == name2 == "a_b"

    def test_dangerous_chars_removed(self):
        assert "`" not in sanitize_stream_name("stream`name")
        assert ";" not in sanitize_stream_name("stream;name")
        assert "\x00" not in sanitize_stream_name("stream\x00name")


class TestValidateIdentifier:
    def test_valid_identifier(self):
        assert validate_identifier("my_table") == "my_table"

    def test_empty_identifier_rejected(self):
        with pytest.raises(ValueError, match="cannot be empty"):
            validate_identifier("")

    def test_null_byte_rejected(self):
        with pytest.raises(ValueError, match="unsafe character"):
            validate_identifier("table\x00name")

    def test_backtick_rejected(self):
        with pytest.raises(ValueError, match="unsafe character"):
            validate_identifier("table`name")

    def test_semicolon_rejected(self):
        with pytest.raises(ValueError, match="unsafe character"):
            validate_identifier("table;name")

    def test_hyphen_allowed(self):
        assert validate_identifier("my-table") == "my-table"

    def test_dot_allowed(self):
        assert validate_identifier("my.table") == "my.table"

    def test_unicode_allowed(self):
        assert validate_identifier("日本語テーブル") == "日本語テーブル"


class TestQuotedTableName:
    def test_basic(self):
        assert quoted_table_name("main", "bronze", "users") == "`main`.`bronze`.`users`"

    def test_with_hyphens(self):
        assert (
            quoted_table_name("my-catalog", "my-schema", "my-table")
            == "`my-catalog`.`my-schema`.`my-table`"
        )

    def test_rejects_dangerous_catalog(self):
        with pytest.raises(ValueError):
            quoted_table_name("main`", "bronze", "users")
