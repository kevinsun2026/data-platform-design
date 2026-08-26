"""Pytest configuration shared by every ``aidp_events`` test.

The platform's :class:`aidp_common.config.Settings` requires ``AIDP_DB_URL``,
``AIDP_REDIS_URL`` and ``AIDP_SERVICE_NAME`` to be set. Tests do not actually
talk to Postgres or Redis (Kafka tests use testcontainers; everything else
runs against the in-memory transport), but :func:`aidp_events.envelope.new_envelope`
pulls the service name from those settings, so we have to populate the cache.

The env is set with :func:`monkeypatch.setenv` so it does **not** leak into
other test packages running in the same ``uv run pytest libs/`` invocation.
"""

from __future__ import annotations

from collections.abc import Generator

import pytest

# Minimum env required by ``aidp_common.config.Settings``. Tests do not
# actually connect to these — they are here to make Settings valid.
_TEST_ENV: dict[str, str] = {
    "AIDP_DB_URL": "postgresql://localhost:5432/aidp_test",
    "AIDP_REDIS_URL": "redis://localhost:6379/0",
    "AIDP_SERVICE_NAME": "aidp-events-test",
    "AIDP_KAFKA_BROKERS": "localhost:9092",
}


@pytest.fixture(autouse=True)
def _aidp_events_env(monkeypatch: pytest.MonkeyPatch) -> Generator[None, None, None]:
    """Set the AIDP_* env vars for the duration of each test, then reset."""
    for key, value in _TEST_ENV.items():
        monkeypatch.setenv(key, value)

    from aidp_common import config as cfg

    cfg.reset_settings_cache()
    try:
        yield
    finally:
        cfg.reset_settings_cache()
