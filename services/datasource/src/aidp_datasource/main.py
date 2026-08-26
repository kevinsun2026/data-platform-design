"""FastAPI application factory for the Datasource service.

This module is the entry point for ``uvicorn aidp_datasource.main:app``. It:

- Builds the FastAPI app and registers the standard :mod:`aidp_common`
  middleware (structured logging, trace context, error envelope).
- Manages the SQLAlchemy engine lifecycle via FastAPI's ``lifespan``.
- Runs Alembic migrations to ``head`` on startup so a fresh deployment
  is immediately usable.
- Brings up the internal gRPC server (``DataSourceService``) on
  port 50051 — the agent-gateway is the only consumer.
- Provides a ``/healthz`` (liveness) endpoint and a ``/readyz``
  (readiness) endpoint that pings the database.

The HTTP routes for the datasource CRUD + connection test live
in :mod:`aidp_datasource.api.datasources`.
"""

from __future__ import annotations

import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

# ``aidp_db.session`` is imported for its side effect of registering
# the L1 tenant listener (see :mod:`aidp_db.tenant`). We also import
# :mod:`aidp_datasource.models` so the datasource metadata is
# registered on the ``Base`` before Alembic autogenerate is ever
# pointed at it.
import aidp_db.session  # noqa: F401  # side-effect: L1 listener
from aidp_common.config import get_settings
from aidp_common.errors import UpstreamError
from aidp_common.logging import get_logger, setup_logging
from aidp_common.tracing import setup_tracing
from aidp_db.migration import run_migrations
from aidp_db.session import dispose_engine, get_engine
from fastapi import FastAPI, status

from aidp_datasource import models  # noqa: F401  # side-effect: register tables
from aidp_datasource.proto.server import (
    DEFAULT_GRPC_PORT,
    ENV_GRPC_PORT,
    DatasourceGrpcServer,
)
from aidp_datasource.services.credential_service import reset_credential_key_cache

#: Absolute path to this service's Alembic script directory.
#:
#: Derived from ``__file__`` rather than CWD so the same code
#: path works under (a) a container image whose WORKDIR is the
#: repo root, (b) monorepo-root ``uv run pytest``, and (c) the
#: local ``uv run uvicorn`` dev loop. The layout is::
#:
#:     services/datasource/src/aidp_datasource/main.py   <- this file
#:     services/datasource/src/aidp_datasource/
#:     services/datasource/src/
#:     services/datasource/
#:     services/datasource/alembic/                     <- Alembic scripts
#:
#: so ``Path(__file__).parent.parent.parent`` lands at
#: ``services/datasource/``. ``aidp_db.migration.run_migrations``
#: will further honour ``AIDP_ALEMBIC_SCRIPT_LOCATION`` when set,
#: which lets ops override the location at deploy time without
#: rebuilding the image.
_ALEMBIC_SCRIPT_LOCATION: Path = Path(__file__).parent.parent.parent / "alembic"

_LOG = get_logger(__name__)


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    """Bring the service up at boot and tear it down on shutdown.

    On entry:

    1. Configure structured JSON logging.
    2. Configure OpenTelemetry tracing (no-op when
       ``AIDP_OTLP_ENDPOINT`` is unset, which is the case in tests).
    3. Run Alembic migrations to ``head``. Any failure is logged
       and re-raised so the platform orchestrator's startup probe
       catches it and marks the pod as failing.
    4. Start the internal gRPC server
       (:class:`DatasourceGrpcServer`).

    On exit:

    5. Stop the gRPC server, draining in-flight calls for
       ``1s``.
    6. Dispose the cached engine so the connection pool is
       closed.
    """
    settings = get_settings()
    setup_logging(level=settings.log_level, service_name=settings.service_name, env=settings.env)
    setup_tracing(service_name=settings.service_name, env=settings.env)

    _LOG.info("datasource service starting", extra={"service": settings.service_name})
    try:
        run_migrations(_ALEMBIC_SCRIPT_LOCATION)
    except UpstreamError:
        # Already logged; re-raise so the platform sees the failure.
        raise
    except Exception as exc:  # pragma: no cover - defensive guard
        _LOG.exception("unexpected error during startup")
        raise UpstreamError(
            "datasource service startup failed",
            details={"error": str(exc)},
        ) from exc

    grpc_port = int(os.environ.get(ENV_GRPC_PORT, str(DEFAULT_GRPC_PORT)))
    grpc_server = DatasourceGrpcServer(port=grpc_port)
    try:
        await grpc_server.start()
    except Exception as exc:
        _LOG.exception("failed to start gRPC server")
        raise UpstreamError(
            "datasource gRPC server failed to start",
            details={"port": grpc_port, "error": str(exc)},
        ) from exc

    try:
        yield
    finally:
        try:
            await grpc_server.stop(grace=1.0)
        except Exception:  # pragma: no cover - best-effort cleanup
            _LOG.exception("error while stopping gRPC server")
        try:
            dispose_engine()
        except Exception:  # pragma: no cover - best-effort cleanup
            _LOG.exception("error while disposing engine")
        # Tests sometimes swap the credential key; reset the
        # cache so a subsequent test does not see the previous
        # key. Production code never hits this branch because
        # the env var does not change mid-process.
        reset_credential_key_cache()
        _LOG.info("datasource service stopped")


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------


def create_app() -> FastAPI:
    """Construct the FastAPI app for the Datasource service.

    Returns:
        A fully-configured :class:`fastapi.FastAPI` instance with
        the standard :mod:`aidp_common` middleware + health
        endpoints + the datasource REST API installed. The
        returned app is suitable for direct use by ``uvicorn
        aidp_datasource.main:app`` or as a child in a larger
        composition root.
    """
    settings = get_settings()
    app = FastAPI(
        title="AIDP Datasource Service",
        version="0.1.0",
        description=(
            "Datasource registry (PG / MySQL / Oracle / Hive) with "
            "AES-256-GCM-encrypted credentials, served via FastAPI "
            "on port 8005 + an internal gRPC server on port 50051."
        ),
        lifespan=lifespan,
    )

    @app.get("/healthz", status_code=status.HTTP_200_OK)
    async def healthz() -> dict[str, str]:
        """Liveness probe — confirms the process is up."""
        return {"status": "ok"}

    @app.get("/readyz", status_code=status.HTTP_200_OK)
    async def readyz() -> dict[str, Any]:
        """Readiness probe — confirms the database is reachable.

        Returns 200 with a JSON body describing the database
        dialect and the engine URL (with credentials redacted).
        Returns 503 via :class:`aidp_common.errors.UpstreamError`
        when the database is unreachable.
        """
        engine = get_engine()
        try:
            with engine.connect() as conn:
                # ``SELECT 1`` is portable across Postgres and
                # SQLite, so the probe works against both
                # production and the testcontainers / SQLite
                # fallback.
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

    # Mount the datasource API. The router is imported lazily so
    # the create_app() call stays fast at import time. We also
    # import the schema + policies endpoint modules — their
    # ``@router.post`` decorators mutate the shared
    # ``datasources_router`` in place, so the schema +
    # policies endpoints land on the same
    # ``/api/v1/datasources`` prefix as the CRUD + test
    # routes.
    from aidp_datasource.api import policies as _policy_endpoints  # noqa: F401
    from aidp_datasource.api import schemas as _schema_endpoints  # noqa: F401
    from aidp_datasource.api.datasources import router as datasources_router
    from aidp_datasource.api.errors import install_app_error_handler

    install_app_error_handler(app)
    app.include_router(datasources_router)

    _LOG.info(
        "datasource app created",
        extra={"service": settings.service_name, "port": 8005},
    )
    return app


# Module-level instance so ``uvicorn aidp_datasource.main:app``
# works without an extra factory import. The factory remains
# available for tests that want to construct the app with a
# custom lifespan.
app: FastAPI = create_app()


__all__ = ["app", "create_app", "lifespan"]


# Configure ``logging`` defaults on import so a bare ``python -m
# aidp_datasource.main`` (which does not go through
# ``create_app``) still emits structured logs. This is a no-op
# if :func:`setup_logging` is invoked again from the lifespan.
if not logging.getLogger().handlers:  # pragma: no cover - import-time fallback
    setup_logging(service_name="aidp-datasource")
