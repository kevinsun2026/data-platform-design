"""FastAPI application factory for the Audit service.

This module is the entry point for ``uvicorn aidp_audit.main:app``. It:

- Builds the FastAPI app and registers the standard :mod:`aidp_common`
  middleware (structured logging, trace context, error envelope).
- Manages the SQLAlchemy engine lifecycle via FastAPI's ``lifespan``.
- Spawns the Kafka consumer as a background task on startup, cancels
  it on shutdown (draining a final partial batch).
- Runs Alembic migrations to ``head`` on startup so a fresh deployment
  is immediately usable.
- Provides a ``/healthz`` (liveness) endpoint and a ``/readyz``
  (readiness) endpoint that pings the database.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

# ``aidp_db.session`` is imported for its side effect of registering
# the L1 tenant listener (see :mod:`aidp_db.tenant`). We also import
# :mod:`aidp_audit.models` so the audit metadata is registered on the
# ``Base`` before Alembic autogenerate is ever pointed at it.
import aidp_db.session  # noqa: F401  # side-effect: L1 listener
from aidp_common.config import get_settings
from aidp_common.errors import UpstreamError
from aidp_common.logging import get_logger, setup_logging
from aidp_common.tracing import setup_tracing
from aidp_db.migration import run_migrations
from aidp_db.session import dispose_engine, get_engine
from fastapi import FastAPI, status

from aidp_audit import models  # noqa: F401  # side-effect: register tables
from aidp_audit.consumer import run_consumer

#: Absolute path to this service's Alembic script directory.
#:
#: Derived from ``__file__`` rather than CWD so the same code path works
#: under (a) a container image whose WORKDIR is the repo root, (b)
#: monorepo-root ``uv run pytest``, and (c) the local ``uv run uvicorn``
#: dev loop. The layout is::
#:
#:     services/audit/src/aidp_audit/main.py   <- this file
#:     services/audit/src/aidp_audit/
#:     services/audit/src/
#:     services/audit/
#:     services/audit/alembic/               <- Alembic scripts
#:
#: so ``Path(__file__).parent.parent.parent`` lands at ``services/audit/``.
#: ``aidp_db.migration.run_migrations`` will further honour
#: ``AIDP_ALEMBIC_SCRIPT_LOCATION`` when set, which lets ops override
#: the location at deploy time without rebuilding the image.
_ALEMBIC_SCRIPT_LOCATION: Path = Path(__file__).parent.parent.parent / "alembic"

_LOG = get_logger(__name__)

#: Set to ``True`` to disable spawning the Kafka consumer. Tests that
#: build the app via :func:`create_app` flip this to avoid consuming
#: real events during smoke tests. Production always leaves it
#: ``False`` (the default).
DISABLE_CONSUMER: bool = False


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    """Bring the service up at boot and tear it down on shutdown.

    On entry:

    1. Configure structured JSON logging.
    2. Configure OpenTelemetry tracing (no-op when ``AIDP_OTLP_ENDPOINT``
       is unset, which is the case in tests).
    3. Run Alembic migrations to ``head``. Any failure is logged and
       re-raised so the platform orchestrator's startup probe catches it
       and marks the pod as failing.
    4. Spawn the Kafka consumer as a background task (skipped when
       :data:`DISABLE_CONSUMER` is set, e.g. in tests).

    On exit:

    5. Cancel the consumer task; ``run_consumer`` drains a final
       partial batch before propagating ``CancelledError``.
    6. Dispose the cached engine so the connection pool is closed.
    """
    settings = get_settings()
    setup_logging(level=settings.log_level, service_name=settings.service_name, env=settings.env)
    setup_tracing(service_name=settings.service_name, env=settings.env)

    _LOG.info("audit service starting", extra={"service": settings.service_name})
    try:
        run_migrations(_ALEMBIC_SCRIPT_LOCATION)
    except UpstreamError:
        # Already logged; re-raise so the platform sees the failure.
        raise
    except Exception as exc:  # pragma: no cover - defensive guard
        _LOG.exception("unexpected error during startup")
        raise UpstreamError("audit service startup failed", details={"error": str(exc)}) from exc

    consumer_task: asyncio.Task[None] | None = None
    if not DISABLE_CONSUMER:
        consumer_task = asyncio.create_task(
            run_consumer(),
            name="aidp-audit-consumer",
        )
        _LOG.info("audit consumer started", extra={"task": consumer_task.get_name()})

    try:
        yield
    finally:
        if consumer_task is not None:
            consumer_task.cancel()
            try:
                await consumer_task
            except asyncio.CancelledError:
                pass
            except Exception:  # pragma: no cover - shutdown best-effort
                _LOG.exception("audit consumer task raised on shutdown")
        # Dispose the engine so its connection pool shuts down cleanly.
        try:
            dispose_engine()
        except Exception:  # pragma: no cover - best-effort cleanup
            _LOG.exception("error while disposing engine")
        _LOG.info("audit service stopped")


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------


def create_app() -> FastAPI:
    """Construct the FastAPI app for the Audit service.

    Returns:
        A fully-configured :class:`fastapi.FastAPI` instance with the
        standard :mod:`aidp_common` middleware + health endpoints
        installed. The returned app is suitable for direct use by
        ``uvicorn aidp_audit.main:app`` or as a child in a larger
        composition root.
    """
    settings = get_settings()
    app = FastAPI(
        title="AIDP Audit Service",
        version="0.1.0",
        description="Audit event aggregation (Kafka consumer) + tenant-scoped query API.",
        lifespan=lifespan,
    )

    @app.get("/healthz", status_code=status.HTTP_200_OK)
    async def healthz() -> dict[str, str]:
        """Liveness probe — confirms the process is up."""
        return {"status": "ok"}

    @app.get("/readyz", status_code=status.HTTP_200_OK)
    async def readyz() -> dict[str, Any]:
        """Readiness probe — confirms the database is reachable.

        Returns 200 with a JSON body describing the database dialect
        and the engine URL (with credentials redacted). Returns 503
        via :class:`aidp_common.errors.UpstreamError` when the
        database is unreachable.
        """
        engine = get_engine()
        try:
            with engine.connect() as conn:
                # ``SELECT 1`` is portable across Postgres and SQLite,
                # so the probe works against both production and the
                # testcontainers / SQLite fallback.
                conn.exec_driver_sql("SELECT 1")
        except Exception as exc:
            _LOG.exception("readiness check failed")
            raise UpstreamError(
                "database not reachable",
                details={"error": str(exc)},
            ) from exc
        return {
            "status": "ready",
            "database": engine.dialect.name,
        }

    # Mount the audit API. The router is imported lazily so the
    # create_app() call stays fast at import time.
    from aidp_audit.api.errors import install_app_error_handler
    from aidp_audit.api.query import router as query_router

    install_app_error_handler(app)
    app.include_router(query_router)

    _LOG.info(
        "audit app created",
        extra={"service": settings.service_name, "port": 8002},
    )
    return app


# Module-level instance so ``uvicorn aidp_audit.main:app`` works without an
# extra factory import. The factory remains available for tests that want
# to construct the app with a custom lifespan.
app: FastAPI = create_app()


__all__ = ["DISABLE_CONSUMER", "app", "create_app", "lifespan"]


# Configure ``logging`` defaults on import so a bare ``python -m aidp_audit.main``
# (which does not go through ``create_app``) still emits structured logs.
# This is a no-op if :func:`setup_logging` is invoked again from the lifespan.
if not logging.getLogger().handlers:  # pragma: no cover - import-time fallback
    setup_logging(service_name="aidp-audit")
