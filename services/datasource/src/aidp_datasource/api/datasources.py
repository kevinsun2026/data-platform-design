"""REST handlers for the ``/api/v1/datasources`` surface.

The module is the transport adapter for the datasource service:

- ``POST   /api/v1/datasources`` — create
- ``GET    /api/v1/datasources`` — list (env/kind/tag filters)
- ``GET    /api/v1/datasources/{id}`` — detail
- ``PUT    /api/v1/datasources/{id}`` — update
- ``DELETE /api/v1/datasources/{id}`` — soft delete
- ``POST   /api/v1/datasources/{id}/test`` — test connection
- ``GET    /api/v1/datasources/types`` — list supported types

L1 isolation
------------

Every handler takes the authenticated caller's
:class:`CurrentUser` via :data:`current_user` (or one of the
``require_permission`` derivatives). The dependency binds the
request-scoped tenant context via
:func:`aidp_db.tenant.set_tenant_context`, so every downstream
ORM select is auto-filtered by ``WHERE tenant_id = :tid``.
"""

from __future__ import annotations

import logging

from aidp_auth.dependencies import require_permission
from aidp_auth.jwt import CurrentUser
from fastapi import APIRouter, Depends, Path, Query, status

from aidp_datasource.models import Datasource
from aidp_datasource.schemas import (
    ConnectionTestRequest,
    ConnectionTestResponse,
    DatasourceCreateRequest,
    DatasourceListResponse,
    DatasourceResponse,
    DatasourceTypeInfo,
    DatasourceTypesResponse,
    DatasourceUpdateRequest,
)
from aidp_datasource.services.datasource_service import (
    DatasourceService,
    TestConnectionOutcome,
    default_datasource_service,
)

_LOG = logging.getLogger(__name__)


#: A single router mounts the entire datasource surface under
#: ``/api/v1/datasources``. The brief lists seven endpoints; we
#: group them on one router for the same reason the notify
#: service does — the surface is small enough that the import
#: graph in :mod:`aidp_datasource.main` stays tiny.
router = APIRouter(prefix="/api/v1/datasources", tags=["datasources"])


# ---------------------------------------------------------------------------
# Permission strings
# ---------------------------------------------------------------------------

_PERM_READ = "datasource.read"
_PERM_WRITE = "datasource.write"
_PERM_TEST = "datasource.test"


# ---------------------------------------------------------------------------
# Row projection
# ---------------------------------------------------------------------------


def _row_to_response(row: Datasource) -> DatasourceResponse:
    """Project a :class:`Datasource` ORM row onto the wire shape.

    The credential columns (``credentials_ciphertext`` /
    ``credentials_nonce`` / ``credentials_aad`` /
    ``credentials_key_version``) are **never** included in the
    response — only the non-secret ``connection`` block.
    """
    return DatasourceResponse.model_validate(
        {
            "id": row.id,
            "tenant_id": row.tenant_id,
            "name": row.name,
            "kind": row.kind,
            "env": row.env,
            "description": row.description,
            "connection": dict(row.connection_json),
            "tags": list(row.tags),
            "enabled": bool(row.enabled),
            "created_at": row.created_at,
            "updated_at": row.updated_at,
        }
    )


# ---------------------------------------------------------------------------
# Helper dependency
# ---------------------------------------------------------------------------


def _get_service_dep() -> DatasourceService:
    """FastAPI dependency: return the process-wide :class:`DatasourceService`.

    A function (not a class) so the FastAPI dependency-injection
    engine can introspect the return type.
    """
    return default_datasource_service()


# ---------------------------------------------------------------------------
# GET /api/v1/datasources/types
# ---------------------------------------------------------------------------


@router.get(
    "/types",
    response_model=DatasourceTypesResponse,
    status_code=status.HTTP_200_OK,
    summary="List the datasource kinds the platform supports.",
)
def list_types(
    user: CurrentUser = Depends(require_permission(_PERM_READ)),
    service: DatasourceService = Depends(_get_service_dep),
) -> DatasourceTypesResponse:
    """Return the supported-type metadata for the operator dashboard.

    Static — the data is not per-tenant. We still require
    authentication so the endpoint is not anonymous; the
    ``tenant_id`` is unused.
    """
    items = [
        DatasourceTypeInfo.model_validate(entry) for entry in service.supported_types()
    ]
    return DatasourceTypesResponse(items=items)


# ---------------------------------------------------------------------------
# POST /api/v1/datasources
# ---------------------------------------------------------------------------


@router.post(
    "",
    response_model=DatasourceResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Register a new datasource (encrypts credentials before persisting).",
)
def create_datasource(
    body: DatasourceCreateRequest,
    user: CurrentUser = Depends(require_permission(_PERM_WRITE)),
    service: DatasourceService = Depends(_get_service_dep),
) -> DatasourceResponse:
    """Register a new datasource for the caller's tenant.

    A 409 is returned when a row with the same ``(tenant_id,
    name)`` already exists. A 400 is returned when
    ``kind`` / ``env`` / ``tags`` fail Pydantic validation.
    """
    row = service.create_datasource(
        tenant_id=user.tenant_id,
        actor=user.user_id,
        body=body,
    )
    return _row_to_response(row)


# ---------------------------------------------------------------------------
# GET /api/v1/datasources
# ---------------------------------------------------------------------------


@router.get(
    "",
    response_model=DatasourceListResponse,
    status_code=status.HTTP_200_OK,
    summary="List datasources for the caller's tenant (with optional filters).",
)
def list_datasources(
    env: str | None = Query(
        default=None,
        min_length=1,
        max_length=16,
        description="Filter by deployment environment (dev/staging/prod/test).",
    ),
    kind: str | None = Query(
        default=None,
        min_length=1,
        max_length=16,
        description="Filter by datasource kind (postgresql/mysql/oracle/hive).",
    ),
    tag: str | None = Query(
        default=None,
        min_length=1,
        max_length=64,
        description="Filter by tag (case-insensitive exact match).",
    ),
    user: CurrentUser = Depends(require_permission(_PERM_READ)),
    service: DatasourceService = Depends(_get_service_dep),
) -> DatasourceListResponse:
    """Return datasources for the caller's tenant, optionally filtered."""
    rows = service.list_datasources(
        tenant_id=user.tenant_id,
        env=env,
        kind=kind,
        tag=tag,
    )
    return DatasourceListResponse(items=[_row_to_response(r) for r in rows])


# ---------------------------------------------------------------------------
# GET /api/v1/datasources/{id}
# ---------------------------------------------------------------------------


@router.get(
    "/{datasource_id}",
    response_model=DatasourceResponse,
    status_code=status.HTTP_200_OK,
    summary="Fetch one datasource by id.",
)
def get_datasource(
    datasource_id: str = Path(..., min_length=1, max_length=36),
    user: CurrentUser = Depends(require_permission(_PERM_READ)),
    service: DatasourceService = Depends(_get_service_dep),
) -> DatasourceResponse:
    """Return one datasource row. 404 on miss / cross-tenant probe."""
    row = service.get_datasource(
        tenant_id=user.tenant_id,
        datasource_id=datasource_id,
    )
    return _row_to_response(row)


# ---------------------------------------------------------------------------
# PUT /api/v1/datasources/{id}
# ---------------------------------------------------------------------------


@router.put(
    "/{datasource_id}",
    response_model=DatasourceResponse,
    status_code=status.HTTP_200_OK,
    summary="Apply a partial update to a registered datasource.",
)
def update_datasource(
    body: DatasourceUpdateRequest,
    datasource_id: str = Path(..., min_length=1, max_length=36),
    user: CurrentUser = Depends(require_permission(_PERM_WRITE)),
    service: DatasourceService = Depends(_get_service_dep),
) -> DatasourceResponse:
    """Apply a partial update; credentials and ``kind`` are immutable here."""
    row = service.update_datasource(
        tenant_id=user.tenant_id,
        actor=user.user_id,
        datasource_id=datasource_id,
        body=body,
    )
    return _row_to_response(row)


# ---------------------------------------------------------------------------
# DELETE /api/v1/datasources/{id}
# ---------------------------------------------------------------------------


@router.delete(
    "/{datasource_id}",
    response_model=DatasourceResponse,
    status_code=status.HTTP_200_OK,
    summary="Soft-delete a datasource.",
)
def delete_datasource(
    datasource_id: str = Path(..., min_length=1, max_length=36),
    user: CurrentUser = Depends(require_permission(_PERM_WRITE)),
    service: DatasourceService = Depends(_get_service_dep),
) -> DatasourceResponse:
    """Mark the row as soft-deleted; the underlying record stays for audit."""
    row = service.soft_delete_datasource(
        tenant_id=user.tenant_id,
        actor=user.user_id,
        datasource_id=datasource_id,
    )
    return _row_to_response(row)


# ---------------------------------------------------------------------------
# POST /api/v1/datasources/{id}/test
# ---------------------------------------------------------------------------


@router.post(
    "/{datasource_id}/test",
    response_model=ConnectionTestResponse,
    status_code=status.HTTP_200_OK,
    summary="Probe the registered connection and record the outcome.",
)
def test_datasource_connection(
    body: ConnectionTestRequest,
    datasource_id: str = Path(..., min_length=1, max_length=36),
    user: CurrentUser = Depends(require_permission(_PERM_TEST)),
    service: DatasourceService = Depends(_get_service_dep),
) -> ConnectionTestResponse:
    """Run a connection probe against the registered datasource.

    A 200 with ``status="failed"`` is returned when the probe
    failed (so the operator can render the failure directly).
    Only a 404 (missing) or a 500 (internal) crosses into the
    error envelope.
    """
    outcome: TestConnectionOutcome = service.test_connection(
        tenant_id=user.tenant_id,
        actor=user.user_id,
        datasource_id=datasource_id,
        timeout_seconds=body.timeout_seconds,
    )
    return ConnectionTestResponse(
        datasource_id=outcome.datasource_id,
        status=outcome.status,
        latency_ms=outcome.latency_ms,
        error=outcome.error,
    )


__all__ = ["router"]
