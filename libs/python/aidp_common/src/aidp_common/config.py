"""Application configuration for AIDP Python services.

Settings are loaded from environment variables prefixed with ``AIDP_`` (see
``Settings.model_config``). ``get_settings`` returns a process-wide cached
instance; use ``reset_settings_cache`` from tests to force a re-read.
"""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Process-wide configuration shared by every AIDP Python service.

    Attributes:
        db_url: SQLAlchemy database URL (PostgreSQL in production).
        redis_url: Redis connection URL used for cache + rate-limit.
        kafka_brokers: Comma-separated Kafka broker list.
        service_name: Logical service name (used for logs, traces, metrics).
        log_level: Root logger level (``DEBUG`` / ``INFO`` / ``WARNING`` / ...).
        env: Deployment environment label (``dev`` / ``staging`` / ``prod``).
        otlp_endpoint: OTLP gRPC endpoint for OpenTelemetry export. ``None``
            disables exporter setup (tracing stays in-process).
        jwt_secret: HMAC secret used by :mod:`aidp_auth.jwt` to sign and
            verify HS256 access / refresh tokens. In production this is
            injected by the deployment platform (KMS-backed); tests should
            set it via :func:`monkeypatch.setenv` (``AIDP_JWT_SECRET=...``).
        jwt_algorithm: JWT signing algorithm. Defaults to ``"HS256"`` per
            plan global constraint; rotated only during a controlled
            migration.
        jwt_access_token_expires_minutes: Lifetime of the access token in
            minutes (default 720 = 12h, per plan global).
        jwt_refresh_token_expires_days: Lifetime of the refresh token in
            days (default 30, per plan global).
    """

    db_url: str
    redis_url: str
    kafka_brokers: str = "localhost:9092"
    service_name: str
    log_level: str = "INFO"
    env: str = "dev"
    otlp_endpoint: str | None = None
    jwt_secret: str = "aidp-dev-only-jwt-secret-do-not-use-in-prod"
    jwt_algorithm: str = "HS256"
    jwt_access_token_expires_minutes: int = 720  # 12 hours
    jwt_refresh_token_expires_days: int = 30

    model_config = SettingsConfigDict(
        env_prefix="AIDP_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


_settings: Settings | None = None


def get_settings() -> Settings:
    """Return the process-wide cached :class:`Settings` instance.

    The first call reads environment variables; subsequent calls return the
    same object. Tests should call :func:`reset_settings_cache` between cases
    to pick up new env values.
    """
    global _settings
    if _settings is None:
        _settings = _build_settings()
    return _settings


def _build_settings() -> Settings:
    """Construct a :class:`Settings` from the current environment.

    Reads the standard ``AIDP_*`` env vars; honors the ``env_file`` declared
    on :class:`Settings.model_config` for local development.
    """
    return Settings()  # type: ignore[call-arg]


def reset_settings_cache() -> None:
    """Clear the cached :class:`Settings` instance (intended for tests)."""
    global _settings
    _settings = None


__all__ = ["Settings", "get_settings", "reset_settings_cache"]
