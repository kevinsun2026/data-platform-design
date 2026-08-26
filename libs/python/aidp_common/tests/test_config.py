"""Tests for ``aidp_common.config``."""

from __future__ import annotations

import pytest
from aidp_common.config import Settings, get_settings, reset_settings_cache


@pytest.fixture(autouse=True)
def _reset_cache() -> None:
    """Ensure every test starts with a fresh ``get_settings`` cache."""
    reset_settings_cache()


def test_settings_loads_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AIDP_DB_URL", "postgresql://test")
    monkeypatch.setenv("AIDP_REDIS_URL", "redis://test")
    monkeypatch.setenv("AIDP_SERVICE_NAME", "test-svc")
    settings = get_settings()
    assert settings.db_url == "postgresql://test"
    assert settings.service_name == "test-svc"


def test_settings_required_fields_missing() -> None:
    """Without required env vars ``Settings()`` raises a ValidationError."""
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        Settings()  # type: ignore[call-arg]


def test_settings_defaults() -> None:
    """Only the required env vars are needed; the rest have defaults."""
    import os

    os.environ["AIDP_DB_URL"] = "postgresql://x"
    os.environ["AIDP_REDIS_URL"] = "redis://x"
    os.environ["AIDP_SERVICE_NAME"] = "svc"
    settings = get_settings()
    assert settings.kafka_brokers == "localhost:9092"
    assert settings.log_level == "INFO"
    assert settings.env == "dev"
    assert settings.otlp_endpoint is None


def test_settings_overrides_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AIDP_DB_URL", "postgresql://x")
    monkeypatch.setenv("AIDP_REDIS_URL", "redis://x")
    monkeypatch.setenv("AIDP_SERVICE_NAME", "svc")
    monkeypatch.setenv("AIDP_LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("AIDP_ENV", "prod")
    monkeypatch.setenv("AIDP_OTLP_ENDPOINT", "http://otel:4317")
    settings = get_settings()
    assert settings.log_level == "DEBUG"
    assert settings.env == "prod"
    assert settings.otlp_endpoint == "http://otel:4317"


def test_get_settings_is_cached(monkeypatch: pytest.MonkeyPatch) -> None:
    """Two calls without an intervening ``reset`` return the same instance."""
    monkeypatch.setenv("AIDP_DB_URL", "postgresql://a")
    monkeypatch.setenv("AIDP_REDIS_URL", "redis://a")
    monkeypatch.setenv("AIDP_SERVICE_NAME", "svc-a")
    first = get_settings()
    second = get_settings()
    assert first is second


def test_reset_settings_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AIDP_DB_URL", "postgresql://a")
    monkeypatch.setenv("AIDP_REDIS_URL", "redis://a")
    monkeypatch.setenv("AIDP_SERVICE_NAME", "svc-a")
    first = get_settings()
    reset_settings_cache()
    monkeypatch.setenv("AIDP_SERVICE_NAME", "svc-b")
    second = get_settings()
    assert first is not second
    assert second.service_name == "svc-b"
