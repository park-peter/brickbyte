"""
Credential management for brickbyte.

Provides automatic credential resolution from Databricks Secrets
with optional YAML profiles for advanced use cases.
"""

import logging
import re
from typing import Any, Dict, List, Optional

logger = logging.getLogger("brickbyte.credentials")


class CredentialResolver:
    """
    Resolves credentials from Databricks Secrets with convention-based discovery.

    Default convention:
        Scope: "brickbyte" (configurable)
        Keys: "{source-name}/{field}" (e.g., "source-s3/aws_access_key_id")
    """

    def __init__(
        self,
        secrets_scope: str = "brickbyte",
        profiles_path: Optional[str] = None,
    ):
        self.secrets_scope = secrets_scope
        self.profiles_path = profiles_path
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._profiles: Optional[Dict[str, Any]] = None
        self._mappings: Dict[str, str] = {}
        self._dbutils = None
        self._available_keys: Optional[List[str]] = None

        # Load profiles if provided
        if profiles_path:
            self._load_profiles(profiles_path)

    def _get_dbutils(self):
        """Get dbutils instance (lazy loading)."""
        if self._dbutils is None:
            try:
                from pyspark.sql import SparkSession

                spark = SparkSession.getActiveSession()
                if spark:
                    from pyspark.dbutils import DBUtils

                    self._dbutils = DBUtils(spark)
            except Exception:
                pass

            if self._dbutils is None:
                try:
                    import IPython

                    self._dbutils = IPython.get_ipython().user_ns.get("dbutils")
                except Exception:
                    pass

        return self._dbutils

    def _list_secrets_for_source(self, source: str) -> List[str]:
        """List all secret keys available for a source."""
        dbutils = self._get_dbutils()
        if not dbutils:
            return []

        try:
            # Cache the full list of keys
            if self._available_keys is None:
                secrets = dbutils.secrets.list(self.secrets_scope)
                self._available_keys = [s.key for s in secrets]

            # Filter keys that start with the source name
            prefix = f"{source}/"
            return [key[len(prefix) :] for key in self._available_keys if key.startswith(prefix)]
        except Exception as e:
            logger.debug(f"Could not list secrets: {e}")
            return []

    def _get_secret(self, key: str) -> Optional[str]:
        """Get a single secret value from the default scope."""
        dbutils = self._get_dbutils()
        if not dbutils:
            return None

        try:
            return dbutils.secrets.get(scope=self.secrets_scope, key=key)
        except Exception as e:
            logger.debug(f"Could not get secret {key}: {e}")
            return None

    def _get_secret_with_scope(self, scope: str, key: str) -> Optional[str]:
        """Get a single secret value with explicit scope."""
        dbutils = self._get_dbutils()
        if not dbutils:
            return None

        try:
            return dbutils.secrets.get(scope=scope, key=key)
        except Exception as e:
            logger.debug(f"Could not get secret {scope}/{key}: {e}")
            return None

    def _set_nested(self, d: dict, dotted_key: str, value: Any):
        """Set a value in a nested dict using dotted key notation.

        Example: _set_nested(d, "credentials.client_id", "val")
        produces d = {"credentials": {"client_id": "val"}}
        """
        keys = dotted_key.split(".")
        current = d
        for k in keys[:-1]:
            if k not in current or not isinstance(current[k], dict):
                current[k] = {}
            current = current[k]
        current[keys[-1]] = value

    def _load_profiles(self, path: str):
        """Load YAML profiles from file."""
        try:
            import yaml

            # Handle workspace paths
            if path.startswith("/Workspace"):
                dbutils = self._get_dbutils()
                if dbutils:
                    content = dbutils.fs.head(f"file:{path}", 65536)
                else:
                    with open(path, "r") as f:
                        content = f.read()
            else:
                with open(path, "r") as f:
                    content = f.read()

            data = yaml.safe_load(content)
            self._profiles = data.get("profiles", {})
            self._mappings = data.get("mappings", {})
            logger.info(f"Loaded {len(self._profiles)} profiles from {path}")
        except Exception as e:
            logger.warning(f"Could not load profiles from {path}: {e}")
            self._profiles = {}
            self._mappings = {}

    def _resolve_profile(self, profile_name: str) -> Dict[str, Any]:
        """Resolve a named profile to credentials."""
        if not self._profiles or profile_name not in self._profiles:
            return {}

        profile = self._profiles[profile_name]
        resolved = {}

        for key, value in profile.items():
            if isinstance(value, str):
                # Check for secret reference: {{ secret('scope/key') }} or {{ secret('key') }}
                match = re.match(r"\{\{\s*secret\(['\"]([^'\"]+)['\"]\)\s*\}\}", value)
                if match:
                    secret_ref = match.group(1)
                    if "/" in secret_ref:
                        # Explicit scope/key
                        scope, skey = secret_ref.split("/", 1)
                        secret_value = self._get_secret_with_scope(scope, skey)
                    else:
                        # No scope specified, use default scope with source prefix
                        secret_value = self._get_secret(secret_ref)

                    if secret_value:
                        resolved[key] = secret_value
                    else:
                        logger.warning(
                            f"Could not resolve secret reference '{secret_ref}' "
                            f"in profile '{profile_name}'"
                        )
                else:
                    resolved[key] = value
            else:
                resolved[key] = value

        return resolved

    def get_credentials(self, source: str) -> Dict[str, Any]:
        """
        Get credentials for a source.

        Resolution order:
        1. Check if source is mapped to a profile (from YAML)
        2. Fall back to convention-based discovery from secrets
        """
        # Return cached if available
        if source in self._cache:
            return self._cache[source]

        credentials = {}

        # Check for profile mapping first
        if source in self._mappings:
            profile_name = self._mappings[source]
            credentials = self._resolve_profile(profile_name)
            if credentials:
                logger.debug(f"Resolved credentials for {source} from profile '{profile_name}'")

        # Fall back to convention-based discovery
        if not credentials:
            keys = self._list_secrets_for_source(source)
            for key in keys:
                full_key = f"{source}/{key}"
                value = self._get_secret(full_key)
                if value:
                    # Support dotted-key nested mapping
                    self._set_nested(credentials, key, value)

            if credentials:
                logger.debug(f"Discovered {len(credentials)} credentials for {source} from secrets")

        # Cache the result
        self._cache[source] = credentials
        return credentials

    def merge_credentials(
        self,
        source: str,
        source_config: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Merge discovered credentials into source_config.

        Explicit values in source_config take precedence over discovered credentials.
        """
        discovered = self.get_credentials(source)
        if not discovered:
            return source_config

        # Deep merge - discovered credentials as base, source_config overrides
        merged = self._deep_merge(discovered, source_config)
        return merged

    def _deep_merge(self, base: Dict, override: Dict) -> Dict:
        """Deep merge two dictionaries, with override taking precedence."""
        result = base.copy()
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        return result

    def validate(self, source: str) -> bool:
        """Validate that credentials exist for a source."""
        creds = self.get_credentials(source)
        return len(creds) > 0

    def list_available_sources(self) -> List[str]:
        """List all sources that have credentials configured."""
        dbutils = self._get_dbutils()
        if not dbutils:
            return list(self._mappings.keys())

        try:
            if self._available_keys is None:
                secrets = dbutils.secrets.list(self.secrets_scope)
                self._available_keys = [s.key for s in secrets]

            # Extract unique source names from keys
            sources = set()
            for key in self._available_keys:
                if "/" in key:
                    source = key.split("/")[0]
                    sources.add(source)

            # Add mapped sources
            sources.update(self._mappings.keys())

            return sorted(sources)
        except Exception:
            return list(self._mappings.keys())

    def clear_cache(self):
        """Clear the credential cache."""
        self._cache.clear()
        self._available_keys = None


def create_credential_resolver(
    secrets_scope: str = "brickbyte",
    profiles_path: Optional[str] = None,
) -> CredentialResolver:
    """Create a credential resolver."""
    return CredentialResolver(
        secrets_scope=secrets_scope,
        profiles_path=profiles_path,
    )
