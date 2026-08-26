"""Tests for ``aidp_db.migration``.

The migration module is a thin wrapper around Alembic; the unit-testable
surface is the *configuration* layer (URL resolution, ini section
override, helper detection). Running ``run_migrations`` end-to-end
requires a real Alembic script directory, which lives in each service
(``services/<name>/alembic/``) and is not present in the library itself.

So the tests here focus on:

- :func:`is_alembic_installed` reflects the real import state.
- :func:`make_alembic_config` produces a config with the correct script
  location, ini section, and URL.
- :func:`alembic_version` reads the current revision from a real engine.
- :func:`run_migrations` raises :class:`UpstreamError` when the script
  directory is missing.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from aidp_db.migration import (
    alembic_version,
    is_alembic_installed,
    make_alembic_config,
    run_migrations,
)
from sqlalchemy import create_engine, text

# ---------------------------------------------------------------------------
# Smoke tests
# ---------------------------------------------------------------------------


def test_is_alembic_installed_returns_true() -> None:
    """alembic is a hard dependency of aidp_db, so this is always true."""
    assert is_alembic_installed() is True


# ---------------------------------------------------------------------------
# make_alembic_config
# ---------------------------------------------------------------------------


def test_make_alembic_config_resolves_script_location(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Script location is resolved to an absolute path."""
    from aidp_common.config import reset_settings_cache

    reset_settings_cache()
    monkeypatch.setenv("AIDP_DB_URL", "sqlite:///:memory:")
    monkeypatch.setenv("AIDP_SERVICE_NAME", "test-migration")
    monkeypatch.setenv("AIDP_REDIS_URL", "redis://x")
    try:
        script_dir = tmp_path / "alembic"
        script_dir.mkdir()
        cfg = make_alembic_config(script_dir)
        # The resolved path is absolute and equals ``script_dir``.
        script_loc = cfg.get_main_option("script_location")
        assert script_loc is not None
        assert Path(script_loc) == script_dir.resolve()
        # The DB URL comes from the env (``AIDP_DB_URL``).
        assert cfg.get_main_option("sqlalchemy.url")
    finally:
        reset_settings_cache()


def test_make_alembic_config_applies_overrides(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``main_option`` overrides land on the resulting config."""
    from aidp_common.config import reset_settings_cache

    reset_settings_cache()
    monkeypatch.setenv("AIDP_DB_URL", "sqlite:///:memory:")
    monkeypatch.setenv("AIDP_SERVICE_NAME", "test-migration")
    monkeypatch.setenv("AIDP_REDIS_URL", "redis://x")
    try:
        script_dir = tmp_path / "alembic"
        script_dir.mkdir()
        cfg = make_alembic_config(
            script_dir,
            version_locations=str(tmp_path / "versions"),
            **{"sqlalchemy.url": "postgresql://override"},
        )
        assert cfg.get_main_option("version_locations") == str(tmp_path / "versions")
        assert cfg.get_main_option("sqlalchemy.url") == "postgresql://override"
    finally:
        reset_settings_cache()


def test_make_alembic_config_uses_default_ini_section(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When the caller does not override the section, ``alembic`` is used."""
    from aidp_common.config import reset_settings_cache

    reset_settings_cache()
    monkeypatch.setenv("AIDP_DB_URL", "sqlite:///:memory:")
    monkeypatch.setenv("AIDP_SERVICE_NAME", "test-migration")
    monkeypatch.setenv("AIDP_REDIS_URL", "redis://x")
    try:
        script_dir = tmp_path / "alembic"
        script_dir.mkdir()
        cfg = make_alembic_config(script_dir)
        assert cfg.config_ini_section == "alembic"
    finally:
        reset_settings_cache()


# ---------------------------------------------------------------------------
# run_migrations — failure paths
# ---------------------------------------------------------------------------


def test_run_migrations_raises_when_script_dir_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A missing script directory surfaces as UpstreamError."""
    from aidp_common.config import reset_settings_cache
    from aidp_common.errors import UpstreamError

    reset_settings_cache()
    monkeypatch.setenv("AIDP_DB_URL", "sqlite:///:memory:")
    monkeypatch.setenv("AIDP_SERVICE_NAME", "test-migration")
    monkeypatch.setenv("AIDP_REDIS_URL", "redis://x")

    missing = tmp_path / "does-not-exist"
    with pytest.raises(UpstreamError):
        run_migrations(missing)
    reset_settings_cache()


def test_run_migrations_raises_when_alembic_command_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An alembic runtime error is wrapped in UpstreamError."""
    from unittest.mock import patch

    from aidp_common.config import reset_settings_cache
    from aidp_common.errors import UpstreamError

    reset_settings_cache()
    monkeypatch.setenv("AIDP_DB_URL", "sqlite:///:memory:")
    monkeypatch.setenv("AIDP_SERVICE_NAME", "test-migration")
    monkeypatch.setenv("AIDP_REDIS_URL", "redis://x")

    script_dir = tmp_path / "alembic"
    script_dir.mkdir()

    def _explode(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("simulated alembic failure")

    with (
        patch("aidp_db.migration.alembic_command.upgrade", _explode),
        pytest.raises(UpstreamError),
    ):
        run_migrations(script_dir)
    reset_settings_cache()


# ---------------------------------------------------------------------------
# alembic_version — end-to-end on SQLite, no Alembic scripts needed
# ---------------------------------------------------------------------------


def test_alembic_version_returns_none_on_fresh_db(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A fresh SQLite database has no Alembic revision applied yet."""
    from aidp_common.config import reset_settings_cache

    reset_settings_cache()
    db_file = tmp_path / "fresh.db"
    monkeypatch.setenv("AIDP_DB_URL", f"sqlite:///{db_file}")
    monkeypatch.setenv("AIDP_SERVICE_NAME", "test-migration")
    monkeypatch.setenv("AIDP_REDIS_URL", "redis://x")

    script_dir = tmp_path / "alembic"
    script_dir.mkdir()
    assert alembic_version(script_dir) is None
    reset_settings_cache()


def test_alembic_version_returns_current_revision(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A database with the alembic_version row reports that revision."""
    from aidp_common.config import reset_settings_cache

    reset_settings_cache()
    db_file = tmp_path / "with-version.db"
    monkeypatch.setenv("AIDP_DB_URL", f"sqlite:///{db_file}")
    monkeypatch.setenv("AIDP_SERVICE_NAME", "test-migration")
    monkeypatch.setenv("AIDP_REDIS_URL", "redis://x")

    eng = create_engine(f"sqlite:///{db_file}")
    with eng.begin() as conn:
        conn.execute(text("CREATE TABLE alembic_version (version_num VARCHAR(32) NOT NULL)"))
        conn.execute(text("INSERT INTO alembic_version VALUES ('abc123')"))
    eng.dispose()

    script_dir = tmp_path / "alembic"
    script_dir.mkdir()
    assert alembic_version(script_dir) == "abc123"
    reset_settings_cache()
