"""REST handlers for the ``/api/v1/datasources/{id}/schemas`` surface.

The module is the transport adapter for the schema cache
introduced in Task 15. Four endpoints are mounted on the
datasource router (the same router
:mod:`aidp_datasource.api.datasources` already exposes the
CRUD + connection-test routes):

- ``POST   /api/v1/datasources/{id}/sync-schema`` — kick off
  a background introspection. Returns ``202 Accepted`` with
  a job id immediately; the actual work runs in
  :class:`fastapi.BackgroundTasks`.
- ``GET    /api/v1/datasources/{id}/schemas`` — return the
  latest cached snapshot (``tables_json`` + ``fingerprint``).
- ``GET    /api/v1/datasources/{id}/tables/{table}/preview``
  — live ``SELECT * LIMIT N`` via the connector.
- ``GET    /api/v1/datasources/{id}/tables/{table}/ddl`` —
  render the cached snapshot back to ``CREATE TABLE`` SQL.

L1 isolation
------------

Every handler takes the authenticated caller's
:class:`CurrentUser` via the ``require_permission`` dependency
and forwards ``tenant_id`` to the service. The L1 listener
on the session does the row-level filter; the service
double-checks via the explicit ``WHERE tenant_id = ...``
clauses in the SELECTs.
"""

from __future__ import annotations

import logging
from typing import Any

from aidp_auth.dependencies import require_permission
from aidp_auth.jwt import CurrentUser
from aidp_common.errors import NotFoundError
from aidp_db.session import get_session
from fastapi import BackgroundTasks, Depends, Path, Query, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select

#: The schema endpoints ride on the same router as the
#: datasource CRUD — the ``prefix`` is the only difference
#: (``/api/v1/datasources`` here, the existing CRUD module
#: uses the same prefix). We import the existing router
#: instead of constructing a new one so the FastAPI
#: ``openapi.json`` collapses the two surfaces into a
#: single ``datasources`` tag.
from aidp_datasource.api.datasources import router
from aidp_datasource.jobs.sync_schema import (
    enqueue_sync_schema_job,
    run_sync_schema_job,
)
from aidp_datasource.models import Datasource
from aidp_datasource.services.schema_service import (
    SchemaService,
    default_schema_service,
)

_LOG = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Permission strings
# ---------------------------------------------------------------------------

_PERM_READ = "datasource.read"
_PERM_WRITE = "datasource.write"

# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class SchemaSyncAcceptedResponse(BaseModel):
    """Body of ``POST /sync-schema``.

    Returned with HTTP 202 Accepted. The caller polls the
    same job id via the underlying ``SchemaSyncJobRegistry``
    (a future task adds ``GET /sync-schema/jobs/{job_id}``);
    for Phase 1 the caller can re-issue ``GET /schemas`` and
    check the ``fingerprint`` field for change.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=False)

    job_id: str = Field(min_length=1, max_length=36)
    datasource_id: str = Field(min_length=1, max_length=36)
    status: str = Field(description="Initial job status. Always 'pending'.")


class SchemaListResponse(BaseModel):
    """Body of ``GET /schemas``.

    The :attr:`tables` list is the verbatim
    ``DatasourceSchema.tables_json`` payload. The
    :attr:`fingerprint` is the SHA-256 hex digest of the
    canonicalised schema; the agent-gateway can use it to
    detect drift between two syncs without re-fetching the
    full payload.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=False)

    datasource_id: str
    fingerprint: str
    table_count: int
    refreshed_at: str | None = None
    tables: list[dict[str, Any]] = Field(default_factory=list)


class TablePreviewResponse(BaseModel):
    """Body of ``GET /tables/{table}/preview``.

    The :attr:`columns` list preserves the driver's
    column order so a UI rendering the preview can show the
    row in the same order the user sees it in the source DB.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=False)

    datasource_id: str
    table: str
    columns: list[str] = Field(default_factory=list)
    rows: list[dict[str, Any]] = Field(default_factory=list)
    row_count: int = Field(ge=0)


class TableDdlResponse(BaseModel):
    """Body of ``GET /tables/{table}/ddl``.

    The :attr:`ddl` field is the rendered ``CREATE TABLE``
    text. ``schema_kind`` is echoed for the agent-gateway
    so the LLM prompt can match the dialect (PG vs MySQL vs
    Oracle vs Hive).
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=False)

    datasource_id: str
    table: str
    schema_kind: str
    ddl: str


# ---------------------------------------------------------------------------
# Service dependency
# ---------------------------------------------------------------------------


def _get_service_dep() -> SchemaService:
    """FastAPI dependency: return the process-wide :class:`SchemaService`."""
    return default_schema_service()


# ---------------------------------------------------------------------------
# POST /sync-schema
# ---------------------------------------------------------------------------


@router.post(
    "/{datasource_id}/sync-schema",
    response_model=SchemaSyncAcceptedResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Kick off an async schema sync.",
)
def sync_schema(
    background_tasks: BackgroundTasks,
    datasource_id: str = Path(..., min_length=1, max_length=36),
    database: str | None = Query(
        default=None,
        max_length=255,
        description="Optional database override. Defaults to the connection's database.",
    ),
    user: CurrentUser = Depends(require_permission(_PERM_WRITE)),
    service: SchemaService = Depends(_get_service_dep),
) -> SchemaSyncAcceptedResponse:
    """Enqueue a background schema sync and return the job id.

    The handler returns immediately (``202 Accepted``) with
    the job id; the actual introspection runs in the
    :class:`BackgroundTasks` worker. The fingerprint change
    is observable by re-issuing ``GET /schemas`` and
    comparing the ``fingerprint`` field.

    Raises:
        404: When the datasource is missing for the caller's
            tenant. The background task is *not* enqueued
            in that case.
    """
    # Verify the datasource exists *before* enqueuing so a
    # 404 lands in the response (and not in the background
    # task's "failed" status, which the caller would only
    # discover by polling). We do **not** require a snapshot
    # to exist yet — a freshly-registered datasource has no
    # schema, and the background task is what populates it.
    with get_session() as session:
        row = session.execute(
            select(Datasource).where(
                Datasource.id == datasource_id,
                Datasource.tenant_id == user.tenant_id,
            )
        ).scalar_one_or_none()
        if row is None:
            raise NotFoundError("datasource", datasource_id)

    job = enqueue_sync_schema_job(
        tenant_id=user.tenant_id,
        actor=user.user_id,
        datasource_id=datasource_id,
        database=database,
        service=service,
    )
    background_tasks.add_task(run_sync_schema_job, job.job_id)
    _LOG.info(
        "schema sync enqueued",
        extra={
            "tenant_id": user.tenant_id,
            "datasource_id": datasource_id,
            "job_id": job.job_id,
        },
    )
    return SchemaSyncAcceptedResponse(
        job_id=job.job_id,
        datasource_id=datasource_id,
        status=job.status,
    )


# ---------------------------------------------------------------------------
# GET /schemas
# ---------------------------------------------------------------------------


@router.get(
    "/{datasource_id}/schemas",
    response_model=SchemaListResponse,
    status_code=status.HTTP_200_OK,
    summary="Return the latest cached schema snapshot for a datasource.",
)
def list_schemas(
    datasource_id: str = Path(..., min_length=1, max_length=36),
    database: str | None = Query(
        default=None,
        max_length=255,
        description="Unused today. Reserved for per-database filter (Phase 2).",
    ),
    user: CurrentUser = Depends(require_permission(_PERM_READ)),
    service: SchemaService = Depends(_get_service_dep),
) -> SchemaListResponse:
    """Return the latest cached snapshot for *datasource_id*.

    Raises:
        404: When the datasource is missing or no snapshot
            has ever been taken for it.
    """
    _ = database  # accepted for forward compatibility
    snapshot = service.list_schemas(
        tenant_id=user.tenant_id, datasource_id=datasource_id
    )
    return SchemaListResponse(
        datasource_id=datasource_id,
        fingerprint=snapshot.fingerprint,
        table_count=snapshot.table_count,
        refreshed_at=(
            snapshot.refreshed_at.isoformat() if snapshot.refreshed_at else None
        ),
        tables=list(snapshot.tables_json),
    )


# ---------------------------------------------------------------------------
# GET /tables/{table}/preview
# ---------------------------------------------------------------------------


#: Hard upper bound on the ``limit`` query parameter. The
#: connector itself is unconstrained; the API layer caps the
#: request at 1000 rows to defend against runaway responses
#: on wide tables.
_PREVIEW_LIMIT_MAX = 1000


@router.get(
    "/{datasource_id}/tables/{table}/preview",
    response_model=TablePreviewResponse,
    status_code=status.HTTP_200_OK,
    summary="Preview up to N rows of a table via the live connector.",
)
def preview_table(
    datasource_id: str = Path(..., min_length=1, max_length=36),
    table: str = Path(..., min_length=1, max_length=255),
    limit: int = Query(
        default=100,
        ge=1,
        le=_PREVIEW_LIMIT_MAX,
        description=(
            "Row cap. Defaults to 100; the API caps at "
            f"{_PREVIEW_LIMIT_MAX}."
        ),
    ),
    user: CurrentUser = Depends(require_permission(_PERM_READ)),
    service: SchemaService = Depends(_get_service_dep),
) -> TablePreviewResponse:
    """Return up to *limit* rows from *table* via the connector.

    The :attr:`columns` list preserves the driver's column
    order. The :attr:`row_count` field is the number of
    rows in the response (which may be less than *limit*
    when the table is short).

    Raises:
        404: When the datasource is missing.
        502: When the live query fails (the connector's
            :class:`ConnectorError` is translated by the
            unified AppError handler).
    """
    rows = service.preview_table(
        tenant_id=user.tenant_id,
        datasource_id=datasource_id,
        table=table,
        limit=limit,
    )
    columns: list[str] = []
    for row in rows:
        for key in row:
            if key not in columns:
                columns.append(key)
    return TablePreviewResponse(
        datasource_id=datasource_id,
        table=table,
        columns=columns,
        rows=rows,
        row_count=len(rows),
    )


# ---------------------------------------------------------------------------
# GET /tables/{table}/ddl
# ---------------------------------------------------------------------------


@router.get(
    "/{datasource_id}/tables/{table}/ddl",
    response_model=TableDdlResponse,
    status_code=status.HTTP_200_OK,
    summary="Render the cached schema for a table back to CREATE TABLE SQL.",
)
def get_table_ddl(
    datasource_id: str = Path(..., min_length=1, max_length=36),
    table: str = Path(..., min_length=1, max_length=255),
    user: CurrentUser = Depends(require_permission(_PERM_READ)),
    service: SchemaService = Depends(_get_service_dep),
) -> TableDdlResponse:
    """Return the rendered DDL for *table* from the cached snapshot.

    The DDL is rendered from the cached snapshot — *not*
    from a live ``SHOW CREATE TABLE`` — so the endpoint is
    cheap and survives brief upstream outages.

    Raises:
        404: When the datasource is missing, the snapshot
            is missing, or the table is not in the snapshot.
    """
    from aidp_common.errors import NotFoundError
    from aidp_db.session import get_session
    from sqlalchemy import select

    from aidp_datasource.models import Datasource

    # Look up the kind for the ``schema_kind`` echo. The
    # service already does the snapshot lookup; we keep
    # the kind read here so the response carries it.
    with get_session() as session:
        ds = session.execute(
            select(Datasource).where(
                Datasource.id == datasource_id,
                Datasource.tenant_id == user.tenant_id,
            )
        ).scalar_one_or_none()
        if ds is None:
            raise NotFoundError("datasource", datasource_id)
        kind = ds.kind
    ddl = service.get_table_ddl(
        tenant_id=user.tenant_id,
        datasource_id=datasource_id,
        table=table,
    )
    return TableDdlResponse(
        datasource_id=datasource_id,
        table=table,
        schema_kind=kind,
        ddl=ddl,
    )


__all__ = ["router"]
