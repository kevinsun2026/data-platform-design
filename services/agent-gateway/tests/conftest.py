"""Pytest configuration shared by every agent-gateway test.

The platform's :class:`aidp_common.config.Settings` requires
``AIDP_DB_URL``, ``AIDP_REDIS_URL`` and ``AIDP_SERVICE_NAME`` to be
set. The agent-gateway service also runs ``create_app()`` at module
import time, which immediately calls
:func:`aidp_common.config.get_settings` and therefore reads those env
vars.

To avoid that read racing the autouse fixture, we set sane defaults
in :data:`os.environ` at *conftest import time* (before any test
module imports ``aidp_agent.main``). The autouse fixture then
``monkeypatch.setenv`` the same keys so per-test overrides still
work.
"""

from __future__ import annotations

import os

# Set defaults BEFORE anything imports the service so the module-level
# ``create_app()`` call in :mod:`aidp_agent.main` finds a valid
# settings cache. Real tests may still override these via monkeypatch.
os.environ.setdefault("AIDP_DB_URL", "sqlite:///:memory:")
os.environ.setdefault("AIDP_REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("AIDP_SERVICE_NAME", "aidp-agent-test")
os.environ.setdefault("AIDP_KAFKA_BROKERS", "localhost:9092")
# 64-byte secret; PyJWT requires >=32 bytes for HS256.
os.environ.setdefault(
    "AIDP_JWT_SECRET",
    "aidp-agent-test-secret-do-not-use-in-prod-padded-to-64-bytes-min!!",
)

from collections.abc import Generator

import pytest

_TEST_ENV: dict[str, str] = {
    "AIDP_DB_URL": "sqlite:///:memory:",
    "AIDP_REDIS_URL": "redis://localhost:6379/0",
    "AIDP_SERVICE_NAME": "aidp-agent-test",
    "AIDP_KAFKA_BROKERS": "localhost:9092",
    "AIDP_JWT_SECRET": "aidp-agent-test-secret-do-not-use-in-prod-padded-to-64-bytes-min!!",
}


@pytest.fixture(autouse=True)
def _aidp_agent_env(monkeypatch: pytest.MonkeyPatch) -> Generator[None, None, None]:
    """Re-apply the AIDP_* env vars per-test and reset the settings cache.

    :func:`monkeypatch.setenv` re-sets the values for each test (so
    an earlier ``monkeypatch.delenv`` does not leak). The
    :func:`aidp_common.config.reset_settings_cache` call forces the
    next :func:`get_settings` to re-read the env.
    """
    for key, value in _TEST_ENV.items():
        monkeypatch.setenv(key, value)

    from aidp_common import config as cfg

    cfg.reset_settings_cache()
    # Also reset the module-level agent-gateway AppState so each test
    # gets a fresh registry / router / metering dispatcher.
    from aidp_agent import main as agent_main
    from aidp_agent.mcp import server as mcp_server

    agent_main.set_state(None)
    mcp_server.set_datasource_client(None)
    mcp_server.set_mcp_server(None)
    try:
        yield
    finally:
        cfg.reset_settings_cache()
        agent_main.set_state(None)
        mcp_server.set_datasource_client(None)
        mcp_server.set_mcp_server(None)
