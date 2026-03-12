"""
Shared pytest fixtures for fast and isolated test runs.
"""
import sys
from unittest.mock import MagicMock, patch

import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="Run tests marked as integration.",
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--run-integration"):
        return

    skip_integration = pytest.mark.skip(
        reason="integration test: pass --run-integration to run"
    )
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_integration)


@pytest.fixture(autouse=True)
def isolate_connector_setup(request, monkeypatch):
    """
    Keep unit tests hermetic by skipping connector venv creation/install work.
    """
    if request.node.get_closest_marker("integration"):
        return

    from brickbyte._client import Client

    def _noop_setup(self, source, source_install=None):
        return None

    def _fake_exec_path(self, source):
        return f"/tmp/brickbyte-{source}"

    monkeypatch.setattr(Client, "_setup_source", _noop_setup)
    monkeypatch.setattr(Client, "_get_source_exec_path", _fake_exec_path)


@pytest.fixture
def mock_airbyte():
    mock_ab = MagicMock()
    with patch.dict(sys.modules, {"airbyte": mock_ab}):
        yield mock_ab
