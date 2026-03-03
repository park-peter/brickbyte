"""Tests for credential resolution."""
from unittest.mock import MagicMock

from brickbyte.credentials import CredentialResolver


class TestCredentialResolver:
    def test_init_default_scope(self):
        resolver = CredentialResolver()
        assert resolver.secrets_scope == "brickbyte"

    def test_init_custom_scope(self):
        resolver = CredentialResolver(secrets_scope="custom-scope")
        assert resolver.secrets_scope == "custom-scope"

    def test_merge_credentials_no_discovered(self):
        resolver = CredentialResolver()
        source_config = {"bucket": "my-bucket", "region": "us-east-1"}
        result = resolver.merge_credentials("source-s3", source_config)
        assert result == source_config

    def test_merge_credentials_with_discovered(self):
        resolver = CredentialResolver()
        resolver._cache["source-s3"] = {
            "aws_access_key_id": "discovered_key",
            "aws_secret_access_key": "discovered_secret",
        }
        source_config = {"bucket": "my-bucket"}
        result = resolver.merge_credentials("source-s3", source_config)
        assert result["bucket"] == "my-bucket"
        assert result["aws_access_key_id"] == "discovered_key"
        assert result["aws_secret_access_key"] == "discovered_secret"

    def test_merge_credentials_explicit_override(self):
        resolver = CredentialResolver()
        resolver._cache["source-s3"] = {
            "aws_access_key_id": "discovered_key",
            "aws_secret_access_key": "discovered_secret",
            "region_name": "us-west-2",
        }
        source_config = {
            "bucket": "my-bucket",
            "aws_access_key_id": "explicit_key",
        }
        result = resolver.merge_credentials("source-s3", source_config)
        assert result["aws_access_key_id"] == "explicit_key"
        assert result["aws_secret_access_key"] == "discovered_secret"
        assert result["region_name"] == "us-west-2"
        assert result["bucket"] == "my-bucket"

    def test_deep_merge_nested_dicts(self):
        resolver = CredentialResolver()
        base = {
            "credentials": {"client_id": "base_id", "client_secret": "base_secret"},
            "other": "value",
        }
        override = {"credentials": {"client_id": "override_id"}, "bucket": "my-bucket"}
        result = resolver._deep_merge(base, override)
        assert result["credentials"]["client_id"] == "override_id"
        assert result["credentials"]["client_secret"] == "base_secret"
        assert result["other"] == "value"
        assert result["bucket"] == "my-bucket"

    def test_validate_with_credentials(self):
        resolver = CredentialResolver()
        resolver._cache["source-s3"] = {"aws_access_key_id": "key"}
        assert resolver.validate("source-s3") is True

    def test_validate_without_credentials(self):
        resolver = CredentialResolver()
        assert resolver.validate("source-nonexistent") is False

    def test_clear_cache(self):
        resolver = CredentialResolver()
        resolver._cache["source-s3"] = {"key": "value"}
        resolver._available_keys = ["source-s3/key"]
        resolver.clear_cache()
        assert resolver._cache == {}
        assert resolver._available_keys is None


class TestCredentialResolverWithMockedDbutils:
    def test_list_secrets_for_source(self):
        resolver = CredentialResolver()
        mock_dbutils = MagicMock()
        mock_secret1 = MagicMock()
        mock_secret1.key = "source-s3/aws_access_key_id"
        mock_secret2 = MagicMock()
        mock_secret2.key = "source-s3/aws_secret_access_key"
        mock_secret3 = MagicMock()
        mock_secret3.key = "source-gcs/service_account"
        mock_dbutils.secrets.list.return_value = [mock_secret1, mock_secret2, mock_secret3]
        resolver._dbutils = mock_dbutils
        keys = resolver._list_secrets_for_source("source-s3")
        assert "aws_access_key_id" in keys
        assert "aws_secret_access_key" in keys
        assert "service_account" not in keys

    def test_get_secret(self):
        resolver = CredentialResolver()
        mock_dbutils = MagicMock()
        mock_dbutils.secrets.get.return_value = "secret_value"
        resolver._dbutils = mock_dbutils
        value = resolver._get_secret("source-s3/aws_access_key_id")
        assert value == "secret_value"
        mock_dbutils.secrets.get.assert_called_once_with(
            scope="brickbyte", key="source-s3/aws_access_key_id"
        )

    def test_get_secret_with_scope(self):
        resolver = CredentialResolver()
        mock_dbutils = MagicMock()
        mock_dbutils.secrets.get.return_value = "scoped_value"
        resolver._dbutils = mock_dbutils
        value = resolver._get_secret_with_scope("custom-scope", "my_key")
        assert value == "scoped_value"
        mock_dbutils.secrets.get.assert_called_once_with(
            scope="custom-scope", key="my_key"
        )

    def test_list_available_sources(self):
        resolver = CredentialResolver()
        mock_dbutils = MagicMock()
        mock_secrets = []
        for key in [
            "source-s3/key1",
            "source-s3/key2",
            "source-gcs/key1",
            "source-teams/key1",
        ]:
            mock_secret = MagicMock()
            mock_secret.key = key
            mock_secrets.append(mock_secret)
        mock_dbutils.secrets.list.return_value = mock_secrets
        resolver._dbutils = mock_dbutils
        sources = resolver.list_available_sources()
        assert "source-s3" in sources
        assert "source-gcs" in sources
        assert "source-teams" in sources

    def test_get_credentials_convention_based(self):
        resolver = CredentialResolver()
        mock_dbutils = MagicMock()
        mock_secret1 = MagicMock()
        mock_secret1.key = "source-s3/aws_access_key_id"
        mock_secret2 = MagicMock()
        mock_secret2.key = "source-s3/aws_secret_access_key"
        mock_dbutils.secrets.list.return_value = [mock_secret1, mock_secret2]
        mock_dbutils.secrets.get.side_effect = lambda scope, key: {
            "source-s3/aws_access_key_id": "key123",
            "source-s3/aws_secret_access_key": "secret456",
        }.get(key)
        resolver._dbutils = mock_dbutils
        creds = resolver.get_credentials("source-s3")
        assert creds["aws_access_key_id"] == "key123"
        assert creds["aws_secret_access_key"] == "secret456"

    def test_dotted_key_nested_mapping(self):
        resolver = CredentialResolver()
        mock_dbutils = MagicMock()
        mock_secret = MagicMock()
        mock_secret.key = "source-x/credentials.client_id"
        mock_dbutils.secrets.list.return_value = [mock_secret]
        mock_dbutils.secrets.get.side_effect = lambda scope, key: {
            "source-x/credentials.client_id": "my_client_id",
        }.get(key)
        resolver._dbutils = mock_dbutils
        creds = resolver.get_credentials("source-x")
        assert creds["credentials"]["client_id"] == "my_client_id"


class TestYamlProfiles:
    def test_resolve_profile_simple(self):
        resolver = CredentialResolver()
        resolver._profiles = {
            "test-profile": {"region": "us-east-1", "bucket": "my-bucket"}
        }
        result = resolver._resolve_profile("test-profile")
        assert result["region"] == "us-east-1"
        assert result["bucket"] == "my-bucket"

    def test_resolve_profile_nonexistent(self):
        resolver = CredentialResolver()
        resolver._profiles = {}
        result = resolver._resolve_profile("nonexistent")
        assert result == {}

    def test_mappings_take_precedence(self):
        resolver = CredentialResolver()
        resolver._profiles = {
            "azure-shared": {"tenant_id": "tenant123", "client_id": "client456"}
        }
        resolver._mappings = {"source-microsoft-teams": "azure-shared"}
        creds = resolver.get_credentials("source-microsoft-teams")
        assert creds["tenant_id"] == "tenant123"
        assert creds["client_id"] == "client456"

    def test_resolve_profile_with_explicit_scope_secret(self):
        resolver = CredentialResolver()
        mock_dbutils = MagicMock()
        mock_dbutils.secrets.get.return_value = "resolved_secret"
        resolver._dbutils = mock_dbutils

        resolver._profiles = {
            "test-profile": {"api_key": "{{ secret('custom-scope/my_key') }}"}
        }
        result = resolver._resolve_profile("test-profile")
        assert result["api_key"] == "resolved_secret"
        mock_dbutils.secrets.get.assert_called_with(scope="custom-scope", key="my_key")

    def test_unresolved_secret_logs_warning(self, caplog):
        import logging

        resolver = CredentialResolver()
        resolver._dbutils = MagicMock()
        resolver._dbutils.secrets.get.return_value = None

        resolver._profiles = {
            "test-profile": {"api_key": "{{ secret('missing_key') }}"}
        }

        with caplog.at_level(logging.WARNING, logger="brickbyte.credentials"):
            result = resolver._resolve_profile("test-profile")

        assert "api_key" not in result
        assert "Could not resolve secret" in caplog.text


class TestSetNested:
    def test_simple_key(self):
        resolver = CredentialResolver()
        d = {}
        resolver._set_nested(d, "key", "value")
        assert d == {"key": "value"}

    def test_dotted_key(self):
        resolver = CredentialResolver()
        d = {}
        resolver._set_nested(d, "credentials.client_id", "my_id")
        assert d == {"credentials": {"client_id": "my_id"}}

    def test_deep_dotted_key(self):
        resolver = CredentialResolver()
        d = {}
        resolver._set_nested(d, "a.b.c", "deep")
        assert d == {"a": {"b": {"c": "deep"}}}


class TestClientCredentialIntegration:
    def test_client_init_with_default_scope(self):
        import brickbyte

        bb = brickbyte.client()
        assert bb._credential_resolver.secrets_scope == "brickbyte"

    def test_client_init_with_custom_scope(self):
        import brickbyte

        bb = brickbyte.client(secrets_scope="my-custom-scope")
        assert bb._credential_resolver.secrets_scope == "my-custom-scope"

    def test_list_configured_sources(self):
        import brickbyte

        bb = brickbyte.client()
        bb._credential_resolver._cache = {"source-s3": {"key": "value"}}
        bb._credential_resolver._available_keys = ["source-s3/key", "source-gcs/key"]

        mock_dbutils = MagicMock()
        mock_s3 = MagicMock()
        mock_s3.key = "source-s3/key"
        mock_gcs = MagicMock()
        mock_gcs.key = "source-gcs/key"
        mock_dbutils.secrets.list.return_value = [mock_s3, mock_gcs]
        bb._credential_resolver._dbutils = mock_dbutils

        sources = bb.list_configured_sources()
        assert "source-s3" in sources
        assert "source-gcs" in sources

    def test_validate_credentials(self):
        import brickbyte

        bb = brickbyte.client()
        bb._credential_resolver._cache["source-s3"] = {"key": "value"}
        assert bb.validate_credentials("source-s3") is True
        assert bb.validate_credentials("source-nonexistent") is False
