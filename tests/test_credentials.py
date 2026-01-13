"""Tests for credential resolution."""
import pytest
from unittest.mock import MagicMock, patch

# Import credentials module directly to avoid virtualenv import
from brickbyte.credentials import CredentialResolver


class TestCredentialResolver:
    """Tests for CredentialResolver class."""

    def test_init_default_scope(self):
        """Test default secrets scope is 'brickbyte'."""
        resolver = CredentialResolver()
        assert resolver.secrets_scope == "brickbyte"

    def test_init_custom_scope(self):
        """Test custom secrets scope."""
        resolver = CredentialResolver(secrets_scope="custom-scope")
        assert resolver.secrets_scope == "custom-scope"

    def test_merge_credentials_no_discovered(self):
        """Test merge when no credentials discovered."""
        resolver = CredentialResolver()
        
        source_config = {"bucket": "my-bucket", "region": "us-east-1"}
        result = resolver.merge_credentials("source-s3", source_config)
        
        # Should return original config unchanged
        assert result == source_config

    def test_merge_credentials_with_discovered(self):
        """Test merge with discovered credentials."""
        resolver = CredentialResolver()
        # Manually inject cached credentials
        resolver._cache["source-s3"] = {
            "aws_access_key_id": "discovered_key",
            "aws_secret_access_key": "discovered_secret",
        }
        
        source_config = {"bucket": "my-bucket"}
        result = resolver.merge_credentials("source-s3", source_config)
        
        # Should merge discovered credentials
        assert result["bucket"] == "my-bucket"
        assert result["aws_access_key_id"] == "discovered_key"
        assert result["aws_secret_access_key"] == "discovered_secret"

    def test_merge_credentials_explicit_override(self):
        """Test that explicit config overrides discovered credentials."""
        resolver = CredentialResolver()
        # Manually inject cached credentials
        resolver._cache["source-s3"] = {
            "aws_access_key_id": "discovered_key",
            "aws_secret_access_key": "discovered_secret",
            "region_name": "us-west-2",
        }
        
        source_config = {
            "bucket": "my-bucket",
            "aws_access_key_id": "explicit_key",  # Override
        }
        result = resolver.merge_credentials("source-s3", source_config)
        
        # Explicit value should override discovered
        assert result["aws_access_key_id"] == "explicit_key"
        # Non-overridden discovered value should remain
        assert result["aws_secret_access_key"] == "discovered_secret"
        assert result["region_name"] == "us-west-2"
        assert result["bucket"] == "my-bucket"

    def test_deep_merge_nested_dicts(self):
        """Test deep merge with nested dictionaries."""
        resolver = CredentialResolver()
        
        base = {
            "credentials": {
                "client_id": "base_id",
                "client_secret": "base_secret",
            },
            "other": "value",
        }
        override = {
            "credentials": {
                "client_id": "override_id",
            },
            "bucket": "my-bucket",
        }
        
        result = resolver._deep_merge(base, override)
        
        assert result["credentials"]["client_id"] == "override_id"
        assert result["credentials"]["client_secret"] == "base_secret"
        assert result["other"] == "value"
        assert result["bucket"] == "my-bucket"

    def test_validate_with_credentials(self):
        """Test validate returns True when credentials exist."""
        resolver = CredentialResolver()
        resolver._cache["source-s3"] = {"aws_access_key_id": "key"}
        
        assert resolver.validate("source-s3") is True

    def test_validate_without_credentials(self):
        """Test validate returns False when no credentials exist."""
        resolver = CredentialResolver()
        
        assert resolver.validate("source-nonexistent") is False

    def test_clear_cache(self):
        """Test cache clearing."""
        resolver = CredentialResolver()
        resolver._cache["source-s3"] = {"key": "value"}
        resolver._available_keys = ["source-s3/key"]
        
        resolver.clear_cache()
        
        assert resolver._cache == {}
        assert resolver._available_keys is None


class TestCredentialResolverWithMockedDbutils:
    """Tests with mocked dbutils."""

    def test_list_secrets_for_source(self):
        """Test listing secrets for a specific source."""
        resolver = CredentialResolver()
        
        # Mock dbutils
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
        """Test getting a single secret."""
        resolver = CredentialResolver()
        
        mock_dbutils = MagicMock()
        mock_dbutils.secrets.get.return_value = "secret_value"
        resolver._dbutils = mock_dbutils
        
        value = resolver._get_secret("source-s3/aws_access_key_id")
        
        assert value == "secret_value"
        mock_dbutils.secrets.get.assert_called_once_with(
            scope="brickbyte",
            key="source-s3/aws_access_key_id"
        )

    def test_list_available_sources(self):
        """Test listing all available sources."""
        resolver = CredentialResolver()
        
        mock_dbutils = MagicMock()
        mock_secrets = []
        for key in ["source-s3/key1", "source-s3/key2", "source-gcs/key1", "source-teams/key1"]:
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
        """Test getting credentials via convention-based discovery."""
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


class TestYamlProfiles:
    """Tests for YAML profile loading."""

    def test_resolve_profile_simple(self):
        """Test resolving a simple profile without secret references."""
        resolver = CredentialResolver()
        resolver._profiles = {
            "test-profile": {
                "region": "us-east-1",
                "bucket": "my-bucket",
            }
        }
        
        result = resolver._resolve_profile("test-profile")
        
        assert result["region"] == "us-east-1"
        assert result["bucket"] == "my-bucket"

    def test_resolve_profile_nonexistent(self):
        """Test resolving a nonexistent profile returns empty dict."""
        resolver = CredentialResolver()
        resolver._profiles = {}
        
        result = resolver._resolve_profile("nonexistent")
        
        assert result == {}

    def test_mappings_take_precedence(self):
        """Test that profile mappings are used when available."""
        resolver = CredentialResolver()
        resolver._profiles = {
            "azure-shared": {
                "tenant_id": "tenant123",
                "client_id": "client456",
            }
        }
        resolver._mappings = {
            "source-microsoft-teams": "azure-shared",
        }
        
        creds = resolver.get_credentials("source-microsoft-teams")
        
        assert creds["tenant_id"] == "tenant123"
        assert creds["client_id"] == "client456"


def _has_virtualenv():
    """Check if virtualenv is available."""
    try:
        import virtualenv
        return True
    except ImportError:
        return False


@pytest.mark.skipif(not _has_virtualenv(), reason="virtualenv not installed")
class TestBrickbyteCredentialIntegration:
    """Tests for Brickbyte credential integration."""

    def test_brickbyte_init_with_default_scope(self):
        """Test Brickbyte initializes credential resolver with default scope."""
        from brickbyte import Brickbyte
        
        bb = Brickbyte()
        
        assert bb._credential_resolver.secrets_scope == "brickbyte"

    def test_brickbyte_init_with_custom_scope(self):
        """Test Brickbyte with custom secrets scope."""
        from brickbyte import Brickbyte
        
        bb = Brickbyte(secrets_scope="my-custom-scope")
        
        assert bb._credential_resolver.secrets_scope == "my-custom-scope"

    def test_list_configured_sources(self):
        """Test listing configured sources."""
        from brickbyte import Brickbyte
        
        bb = Brickbyte()
        bb._credential_resolver._cache = {
            "source-s3": {"key": "value"},
        }
        bb._credential_resolver._available_keys = ["source-s3/key", "source-gcs/key"]
        
        # Mock dbutils to return the cached keys
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
        """Test credential validation."""
        from brickbyte import Brickbyte
        
        bb = Brickbyte()
        bb._credential_resolver._cache["source-s3"] = {"key": "value"}
        
        assert bb.validate_credentials("source-s3") is True
        assert bb.validate_credentials("source-nonexistent") is False
