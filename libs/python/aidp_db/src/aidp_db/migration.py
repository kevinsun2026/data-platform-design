"""Alembic migration runner used by AIDP services at startup.

The plan calls for a per-service ``alembic/`` directory (see Task 7
``services/iam/alembic/``, Task 9 ``services/audit/alembic/``, ...). This
module gives those services a single importable entry point,
:func:`run_migrations`, that:

1. Reads the target database URL from the ``AIDP_DB_URL`` environment
   variable (via :func:`aidp_common.config.get_settings`) — keeping the
   surface consistent with the rest of the platform.
2. Configures Alembic's offline / online APIs programmatically so the
   caller's ``alembic.ini`` does not have to duplicate the URL.
3. Reports failures through :class:`aidp_common.errors.UpstreamError` so
   the service can surface a uniform error to the platform orchestrator
   (e.g. Kubernetes startup probe).

Usage in a service ``main.py``::

    from aidp_db.migration import run_migrations

    run_migrations("alembic")  # relative to the service's CWD

For Alembic's own commands (``alembic revision --autogenerate``) we still
ship a per-service ``alembic.ini`` and ``alembic/env.py``. Those
commands are run from the host; :func:`run_migrations` is the in-process
equivalent that the service uses during startup.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

from aidp_common.config import get_settings
from aidp_common.errors import UpstreamError
from alembic import command as alembic_command
from alembic.config import Config as AlembicConfig
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory
from sqlalchemy import engine_from_config, pool

_LOG = logging.getLogger(__name__)

# Section name within ``alembic.ini`` where Alembic looks for the engine
# options. The default is ``[alembic]``; we mirror it here so callers can
# override pool sizing via env if needed.
_DEFAULT_INI_SECTION = "alembic"


def _load_config(script_location: str | Path) -> AlembicConfig:
    """Build an :class:`AlembicConfig` for *script_location*.

    Args:
        script_location: Path to the directory containing ``env.py`` and
            ``versions/``. May be relative to the current working directory
            or absolute.

    Returns:
        A configured :class:`AlembicConfig` whose ``script_location`` and
        ``sqlalchemy.url`` are populated. ``script_location`` is resolved to
        an absolute path so Alembic does not depend on CWD.
    """
    script_path = Path(script_location).resolve()
    if not script_path.exists():  # pragma: no cover - defensive
        raise UpstreamError(
            f"alembic script_location does not exist: {script_path}",
            details={"path": str(script_path)},
        )

    ini_path = script_path / "alembic.ini"
    cfg = AlembicConfig(str(ini_path) if ini_path.exists() else None)
    cfg.set_main_option("script_location", str(script_path))
    cfg.set_main_option("sqlalchemy.url", get_settings().db_url)
    return cfg


def _current_revision(url: str) -> str | None:
    """Return the current Alembic revision applied to the database at *url*.

    Args:
        url: SQLAlchemy database URL.

    Returns:
        The revision id (or ``None`` if no revisions have been applied).
    """
    engine = engine_from_config(
        {"sqlalchemy.url": url}, prefix="sqlalchemy.", poolclass=pool.NullPool
    )
    try:
        with engine.connect() as conn:
            ctx = MigrationContext.configure(conn)
            return ctx.get_current_revision()
    finally:
        engine.dispose()


def _head_revision(cfg: AlembicConfig) -> str | None:
    """Return the head revision declared in the script directory of *cfg*.

    Args:
        cfg: An :class:`AlembicConfig` with ``script_location`` set.

    Returns:
        The head revision id, or ``None`` if the script directory has no
        revisions (empty ``versions/`` folder).
    """
    script_dir = ScriptDirectory.from_config(cfg)
    heads = script_dir.get_heads()
    return heads[0] if heads else None


def run_migrations(script_location: str | Path | None = None) -> str | None:
    """Run Alembic migrations up to the latest head.

    This is the entry point services call at startup. It is a thin wrapper
    over :func:`alembic.command.upgrade` that:

    - reads the URL from :func:`aidp_common.config.get_settings`;
    - resolves the script location from (in order) the
      ``AIDP_ALEMBIC_SCRIPT_LOCATION`` env var, the *script_location*
      argument, and finally a CWD-relative ``"alembic"`` default;
    - validates that the resolved script directory exists;
    - logs the before/after revision so the platform log shipper can
      correlate deployment records with DB state.

    Resolution order for the script location:

    1. ``AIDP_ALEMBIC_SCRIPT_LOCATION`` environment variable — lets ops
       override the path at deploy time without rebuilding the image.
       Useful for images where the Alembic scripts are mounted from a
       ConfigMap / shared volume.
    2. The *script_location* argument — services should pass the
       service-local absolute path so the same code path works under
       both the container image and the monorepo-root ``pytest`` run.
    3. The CWD-relative default ``"alembic"`` — kept for backwards
       compatibility with callers that pre-date the env var / absolute
       path support.

    Args:
        script_location: Path to the directory containing ``env.py`` and
            ``versions/``. May be relative to CWD (the service's working
            directory at startup) or absolute. When ``None``, the
            resolution order above applies.

    Returns:
        The new head revision applied (same as the script head). Returns
        ``None`` when the script directory has no revisions.

    Raises:
        aidp_common.errors.UpstreamError: When the script directory does
            not exist or Alembic reports a migration error. The underlying
            Alembic exception is wrapped and re-raised so callers see a
            consistent error type.
    """
    env_location = _script_location_env()
    if env_location and env_location != "alembic":
        # Env var explicitly set; ops override wins.
        resolved: str | Path = env_location
    elif script_location is not None:
        resolved = script_location
    else:
        resolved = "alembic"
    cfg = _load_config(resolved)
    url = get_settings().db_url
    before = _current_revision(url)
    try:
        _LOG.info(
            "running alembic migrations",
            extra={
                "script_location": cfg.get_main_option("script_location"),
                "from_revision": before,
            },
        )
        alembic_command.upgrade(cfg, "head")
    except Exception as exc:  # pragma: no cover - failure path
        _LOG.exception("alembic migration failed")
        raise UpstreamError(
            "alembic migration failed",
            details={
                "script_location": str(resolved),
                "from_revision": before,
                "error": str(exc),
            },
        ) from exc

    after = _head_revision(cfg)
    _LOG.info(
        "alembic migrations complete",
        extra={"from_revision": before, "to_revision": after},
    )
    return after


def downgrade_migrations(
    script_location: str | Path = "alembic",
    revision: str = "-1",
) -> None:
    """Roll back migrations to *revision* (or ``base``).

    Convenience wrapper used by tests / one-off operations. Not called from
    the normal service startup path — production downgrades are handled
    via a Helm hook job, not inside the API process.

    Args:
        script_location: Path to the script directory. See
            :func:`run_migrations`.
        revision: Target Alembic revision. Defaults to ``-1`` (one step
            back); pass ``"base"`` to roll back everything.
    """
    cfg = _load_config(script_location)
    try:
        alembic_command.downgrade(cfg, revision)
    except Exception as exc:  # pragma: no cover - failure path
        raise UpstreamError(
            "alembic downgrade failed",
            details={"script_location": str(script_location), "revision": revision},
        ) from exc


def make_alembic_config(
    script_location: str | Path,
    *,
    ini_section: str = _DEFAULT_INI_SECTION,
    **overrides: Any,
) -> AlembicConfig:
    """Construct an :class:`AlembicConfig` for advanced callers.

    Most services should use :func:`run_migrations` instead. This helper
    exists for tests and for callers that need to drive Alembic commands
    other than ``upgrade`` / ``downgrade`` (e.g. ``alembic history``).

    Args:
        script_location: See :func:`run_migrations`.
        ini_section: Section name within ``alembic.ini`` that hosts
            SQLAlchemy engine options. Defaults to ``"alembic"``.
        **overrides: Extra ``main_option`` values to set on the config
            after the defaults are applied.

    Returns:
        A configured :class:`AlembicConfig` instance ready to pass to any
        :mod:`alembic.command` function.
    """
    cfg = _load_config(script_location)
    for key, value in overrides.items():
        cfg.set_main_option(key, str(value))
    cfg.config_ini_section = ini_section
    return cfg


def alembic_version(script_location: str | Path = "alembic") -> str | None:
    """Return the current Alembic revision in the database, if any.

    Useful for health checks and for the deployment validator that
    refuses to roll out a service whose migrations are out of date.

    Args:
        script_location: See :func:`run_migrations`. Only used to locate
            ``alembic.ini`` / ``env.py``; no migrations are run.

    Returns:
        The current revision id (``None`` when the database is empty).
    """
    cfg = _load_config(script_location)
    _ = cfg  # script_location resolved + URL set; we only need the URL.
    return _current_revision(get_settings().db_url)


def is_alembic_installed() -> bool:
    """Return ``True`` if the alembic package is importable.

    Defensive helper used by health checks in environments where the
    runtime image is slimmed down for cost reasons.
    """
    try:
        import alembic  # noqa: F401
    except ImportError:  # pragma: no cover - image without alembic
        return False
    return True


def _script_location_env() -> str:
    """Read ``AIDP_ALEMBIC_SCRIPT_LOCATION`` if set, else ``"alembic"``."""
    return os.environ.get("AIDP_ALEMBIC_SCRIPT_LOCATION", "alembic")


__all__ = [
    "alembic_version",
    "downgrade_migrations",
    "is_alembic_installed",
    "make_alembic_config",
    "run_migrations",
]
