"""Alembic environment for the AIDP Notify service.

This module is the bridge between Alembic and the notify service's
SQLAlchemy metadata. It is loaded by Alembic's offline (``--sql``)
and online (default) migration runners; both branches pull the
database URL from :func:`aidp_common.config.get_settings` so a single
``AIDP_DB_URL`` env var drives the service in every environment
(dev, CI, prod).

Key behaviours:

- We import :mod:`aidp_notify.models` for its side effect of
  registering every table on :class:`aidp_notify.models.Base` —
  Alembic then walks ``target_metadata`` to detect schema drift on
  autogenerate runs.
- We do *not* rely on autogenerate for the initial migration. The
  hand-written ``0001_initial.py`` is the canonical schema; later
  revisions can be generated with ``alembic revision --autogenerate``.
- We set ``compare_type=True`` so type changes (e.g. ``String(64)`` →
  ``String(128)``) show up in autogenerate diffs.
- We set ``compare_server_default=True`` so default-value changes
  show up in diffs.

The notify service is the **only** service that writes to the
``notification_channels`` / ``notification_templates`` /
``notification_logs`` tables. The cross-service ``tenants`` table is
owned by the IAM service; the notify service declares a soft FK
(``ForeignKey("tenants.id")`` without autogenerate) and never
migrates the tenants schema.
"""

from __future__ import annotations

import logging
import sys
from logging.config import fileConfig
from pathlib import Path

from aidp_common.config import get_settings
from alembic import context
from sqlalchemy import engine_from_config, pool

# ---------------------------------------------------------------------------
# Make the notify package importable so ``aidp_notify.models`` resolves
# when Alembic is run from the service root
# (``cd services/notify && alembic ...``).
# ---------------------------------------------------------------------------

_SERVICE_ROOT = Path(__file__).resolve().parent.parent
_SRC = _SERVICE_ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

# Importing ``aidp_db.session`` here ensures the L1 tenant listener is
# installed even when running Alembic outside the FastAPI process.
# Importing ``aidp_notify.models`` registers every notify table on
# ``Base.metadata``.
import aidp_db.session  # noqa: E402, F401  # side-effect: L1 listener
from aidp_notify.models import Base  # noqa: E402

# Alembic Config object provides access to alembic.ini values.
config = context.config

# Configure Python logging from the ``[loggers]`` section of alembic.ini.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

_LOG = logging.getLogger("alembic.env.notify")

# Metadata that autogenerate compares against. Pointed at the notify
# service's local ``Base`` so the diff is scoped to the notify schema
# only (cross-service drift detection is the platform's job, not ours).
target_metadata = Base.metadata


def _resolve_url() -> str:
    """Return the SQLAlchemy URL to use for this migration run.

    Priority:

    1. ``AIDP_DB_URL`` env var (via :func:`aidp_common.config.get_settings`).
    2. ``alembic.ini`` ``sqlalchemy.url`` (kept blank by convention).
    """
    settings_url = get_settings().db_url
    if settings_url:
        return settings_url
    ini_url = config.get_main_option("sqlalchemy.url")
    if ini_url:
        return ini_url
    raise RuntimeError(
        "AIDP_DB_URL is not set; set the env var or configure sqlalchemy.url in alembic.ini"
    )


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    Offline mode emits SQL to stdout (or to a file via ``-f``) without
    opening a real connection. Useful for code review of the migration
    diff and for environments where a live DB is not available
    (e.g. a CI job that just needs to print the SQL for a reviewer).
    """
    url = _resolve_url()
    _LOG.info("running migrations offline", extra={"url_dialect": url.split(":", 1)[0]})
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        compare_server_default=True,
        include_schemas=False,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode (default).

    Builds a fresh SQLAlchemy engine from the resolved URL and runs
    the migrations inside a transaction. ``NullPool`` is used so a CI
    run that creates and immediately discards the engine does not
    leak connections.
    """
    cfg_section = config.get_section(config.config_ini_section, {})
    cfg_section["sqlalchemy.url"] = _resolve_url()
    connectable = engine_from_config(
        cfg_section,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            compare_server_default=True,
            include_schemas=False,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
