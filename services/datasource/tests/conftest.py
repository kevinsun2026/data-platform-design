"""Pytest configuration shared by every Datasource test.

The platform's :class:`aidp_common.config.Settings` requires
``AIDP_DB_URL``, ``AIDP_REDIS_URL`` and ``AIDP_SERVICE_NAME``
to be set. The Datasource service also runs ``create_app()`` at
module import time, which immediately calls
:func:`aidp_common.config.get_settings` and therefore reads
those env vars. To avoid that read racing the autouse fixture,
we set sane defaults in :data:`os.environ` at *conftest import
time* (before any test module imports
``aidp_datasource.main``).

The Datasource service also requires
``AIDP_DATASOURCE_CREDENTIAL_KEY`` (a base64-encoded 32-byte
AES key) for credential encryption. The fixture sets a
deterministic test key so the encryption layer has something to
load.
"""

from __future__ import annotations

import base64
import os

# Set defaults BEFORE anything imports the service so the
# module-level ``create_app()`` call in :mod:`aidp_datasource.main`
# finds a valid settings cache. Real tests may still override
# these via monkeypatch.
os.environ.setdefault("AIDP_DB_URL", "postgresql://localhost:5432/aidp_test")
os.environ.setdefault("AIDP_REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("AIDP_SERVICE_NAME", "aidp-datasource-test")
os.environ.setdefault("AIDP_KAFKA_BROKERS", "localhost:9092")
# 64-byte secret; PyJWT requires >=32 bytes for HS256.
os.environ.setdefault(
    "AIDP_JWT_SECRET",
    "aidp-datasource-test-secret-do-not-use-in-prod-padded-to-64-bytes-min!!",
)
# 32-byte (AES-256) credential key, base64-encoded. Deterministic
# so tests can decrypt what the service encrypted.
os.environ.setdefault(
    "AIDP_DATASOURCE_CREDENTIAL_KEY",
    base64.urlsafe_b64encode(b"\x01" * 32).decode("ascii"),
)

from collections.abc import Generator

import pytest

_TEST_ENV: dict[str, str] = {
    "AIDP_DB_URL": "postgresql://localhost:5432/aidp_test",
    "AIDP_REDIS_URL": "redis://localhost:6379/0",
    "AIDP_SERVICE_NAME": "aidp-datasource-test",
    "AIDP_KAFKA_BROKERS": "localhost:9092",
    "AIDP_JWT_SECRET": "aidp-datasource-test-secret-do-not-use-in-prod-padded-to-64-bytes-min!!",
    "AIDP_DATASOURCE_CREDENTIAL_KEY": base64.urlsafe_b64encode(b"\x01" * 32).decode("ascii"),
}


@pytest.fixture(autouse=True)
def _aidp_datasource_env(monkeypatch: pytest.MonkeyPatch) -> Generator[None, None, None]:
    """Re-apply the AIDP_* env vars per-test and reset the settings cache.

    :func:`monkeypatch.setenv` re-sets the values for each test
    (so an earlier ``monkeypatch.delenv`` does not leak). The
    :func:`aidp_common.config.reset_settings_cache` call forces
    the next :func:`get_settings` to re-read the env. The
    :func:`aidp_db.session.reset_engine_cache` call clears the
    cached engine so each test gets a fresh in-memory engine.
    The :func:`aidp_datasource.services.credential_service.reset_credential_key_cache`
    call clears the cached AES key so each test reads a fresh
    ``AIDP_DATASOURCE_CREDENTIAL_KEY``.
    """
    for key, value in _TEST_ENV.items():
        monkeypatch.setenv(key, value)

    from aidp_common import config as cfg
    from aidp_datasource.services import credential_service
    from aidp_db import session as db_session

    cfg.reset_settings_cache()
    db_session.reset_engine_cache()
    credential_service.reset_credential_key_cache()
    try:
        yield
    finally:
        cfg.reset_settings_cache()
        db_session.reset_engine_cache()
        credential_service.reset_credential_key_cache()
