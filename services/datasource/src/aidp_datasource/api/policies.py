"""REST handlers for the per-datasource policy surface.

The module is the transport adapter for the policy + PII
suggestion endpoints introduced in Task 16. Three endpoints
ride on the datasource router (the same router
:mod:`aidp_datasource.api.datasources` already exposes the
CRUD + connection-test + schema routes):

- ``POST /api/v1/datasources/{id}/policies`` — replace (or
  create) the policy blob for the datasource. Returns 200
  with the new policy row.
- ``GET  /api/v1/datasources/{id}/policies`` — return the
  current policy blob. 404 when no policy has ever been
  written.
- ``POST /api/v1/datasources/{id}/suggest-pii`` — call the
  PII service to produce a suggestion list for the
  datasource. Returns 200 with the
  ``list[PIIColumnSuggestion]`` payload (one entry per PII
  column the model flagged).

L1 isolation
------------

Every handler takes the authenticated caller's
:class:`CurrentUser` via the ``require_permission`` dependency
and forwards ``tenant_id`` to the service. The L1 listener
on the session does the row-level filter; the service
double-checks via the explicit ``WHERE tenant_id = ...``
clauses in the SELECTs.

Policy shape
------------

The :class:`DatasourcePolicy` model stores the policy as a
JSON blob (``policies_json``). The shape is opaque to the
datasource service; consumers (``agent-gateway``, ``audit``)
interpret the keys they care about. Common shapes:

- ``{"pii": {"columns": [{"table": "users", "name": "email", "type": "email"}]}}``
- ``{"masking": {"email": "hash", "phone": "mask"}}``
- ``{"row_filter": "tenant_id = :tenant_id"}``

The HTTP request body mirrors the JSON blob — callers send
``{"policies": {...}}`` and the handler persists the dict
verbatim.
"""

from __future__ import annotations

import logging
from typing import Any

from aidp_auth.dependencies import require_permission
from aidp_auth.jwt import CurrentUser
from aidp_common.errors import NotFoundError
from aidp_db.session import get_session
from fastapi import Depends, Path, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select

from aidp_datasource.api.datasources import router
from aidp_datasource.models import Datasource, DatasourcePolicy
from aidp_datasource.services.pii_service import (
    PIIService,
    default_pii_service,
)

_LOG = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Permission strings
# ---------------------------------------------------------------------------

_PERM_READ = "datasource.read"
_PERM_WRITE = "datasource.write"

# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------


class PolicyUpsertRequest(BaseModel):
    """Body of ``POST /api/v1/datasources/{id}/policies``.

    The :attr:`policies` field is the JSON blob the platform
    governance layer interprets. The shape is intentionally
    open (``dict[str, Any]``) so a future governance knob
    (row-level masking, PII tagging, write-vs-read-only,
    allowed roles) lands without an API signature change.

    The handler replaces the entire blob on every call —
    there is no PATCH / partial-merge in Phase 1. Callers
    that want a partial update must read the current blob,
    merge in Python, and POST the merged result.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=False)

    policies: dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "The policy blob. The shape is opaque to the "
            "datasource service; consumers (agent-gateway, "
            "audit) interpret the keys they care about."
        ),
    )


class PolicyResponse(BaseModel):
    """Body of ``GET`` / ``POST /api/v1/datasources/{id}/policies``.

    The :attr:`policies` field echoes the stored JSON blob
    verbatim. The :attr:`updated_at` field carries the
    ``TimestampMixin`` value so the operator UI can render
    "last edited 5 minutes ago" without a second query.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=False)

    datasource_id: str
    policies: dict[str, Any]
    updated_at: str | None = None


class PIISuggestResponse(BaseModel):
    """Body of ``POST /api/v1/datasources/{id}/suggest-pii``.

    The :attr:`suggestions` list carries the
    :class:`PIIColumnSuggestion` items the model flagged.
    Each item is a ``{"name": str, "type": str, "reason": str}``
    dict. The list may be empty when the model found no PII
    columns — the handler still returns 200 (a 200-with-empty
    list is more useful than a 404 because the caller can
    render "no PII detected" without a special-case).
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=False)

    datasource_id: str
    suggestions: list[dict[str, str]]


class PIISuggestRequest(BaseModel):
    """Body of ``POST /api/v1/datasources/{id}/suggest-pii``.

    The body is optional: the request can be sent with no
    payload (``{}``) to ask the service to consider every
    table in the cached schema. When :attr:`tables` is
    non-empty, only the named tables are considered (the
    rest are skipped). When :attr:`sample_size` is set it
    overrides the service default; the value is capped at 20
    so an accidentally-large request does not flood a
    high-throughput table.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=False)

    tables: list[str] | None = Field(
        default=None,
        max_length=128,
        description=(
            "Optional whitelist of table names. ``null`` (the "
            "default) means 'every table in the cached schema'."
        ),
    )
    sample_size: int | None = Field(
        default=None,
        ge=0,
        le=20,
        description=(
            "Per-table sample row count. ``null`` (the default) "
            "uses the service default of 5; ``0`` skips the "
            "sample fetch and reasons over column names only; "
            "the value is capped at 20 to defend against an "
            "accidentally-large request."
        ),
    )


# ---------------------------------------------------------------------------
# Service dependency
# ---------------------------------------------------------------------------


def _get_service_dep() -> PIIService:
    """FastAPI dependency: return the process-wide :class:`PIIService`."""
    return default_pii_service()


# ---------------------------------------------------------------------------
# POST /policies
# ---------------------------------------------------------------------------


@router.post(
    "/{datasource_id}/policies",
    response_model=PolicyResponse,
    status_code=status.HTTP_200_OK,
    summary="Replace the policy blob for a registered datasource.",
)
def upsert_policy(
    body: PolicyUpsertRequest,
    datasource_id: str = Path(..., min_length=1, max_length=36),
    user: CurrentUser = Depends(require_permission(_PERM_WRITE)),
) -> PolicyResponse:
    """Upsert the :class:`DatasourcePolicy` row for *datasource_id*.

    The handler replaces the entire ``policies_json`` blob on
    every call (there is no PATCH / partial-merge in
    Phase 1). The ``(tenant_id, datasource_id)`` unique
    constraint means the row is created on the first call
    and updated on subsequent calls — the API does not
    require the caller to pre-create the row.

    Side-effects:
        - writes one :class:`DatasourceAudit` row with
          ``action="policy_updated"`` (or
          ``action="policy_created"`` on the first call).
        - publishes ``datasource.policy.updated.v1`` to
          Kafka.

    Raises:
        404: When the datasource is missing for the caller's
            tenant.
    """
    with get_session() as session:
        # Verify the datasource exists for the caller's
        # tenant. We do the lookup *before* the upsert so a
        # 404 lands in the response and not as a FK error.
        ds = session.execute(
            select(Datasource).where(
                Datasource.id == datasource_id,
                Datasource.tenant_id == user.tenant_id,
            )
        ).scalar_one_or_none()
        if ds is None:
            raise NotFoundError("datasource", datasource_id)
        # Look up an existing policy row (one per datasource).
        existing = session.execute(
            select(DatasourcePolicy).where(
                DatasourcePolicy.datasource_id == datasource_id,
                DatasourcePolicy.tenant_id == user.tenant_id,
            )
        ).scalar_one_or_none()
        is_create = existing is None
        if is_create:
            existing = DatasourcePolicy(
                tenant_id=user.tenant_id,
                datasource_id=datasource_id,
                policies_json=dict(body.policies),
            )
            session.add(existing)
            action = "policy_created"
        else:
            # ``existing`` is the row we just looked up;
            # ``is_create`` is the inverse so the else branch
            # is reachable only when ``existing is not None``.
            assert existing is not None
            existing.policies_json = dict(body.policies)
            action = "policy_updated"
        from datetime import UTC, datetime

        from aidp_datasource.models import DatasourceAudit

        session.add(
            DatasourceAudit(
                tenant_id=user.tenant_id,
                datasource_id=datasource_id,
                action=action,
                actor=user.user_id,
                diff_json={"policies": dict(body.policies)},
            )
        )
        session.flush()
        # Re-read so the ``updated_at`` is materialised.
        session.refresh(existing)
        updated_at = (
            existing.updated_at.isoformat()
            if existing.updated_at
            else datetime.now(UTC).isoformat()
        )
        response = PolicyResponse(
            datasource_id=datasource_id,
            policies=dict(existing.policies_json),
            updated_at=updated_at,
        )
    # Publish the Kafka event *after* the SQL commit so a
    # Kafka outage cannot roll back the policy write. The
    # audit row is the durable source of truth; the event
    # is the real-time contract.
    _publish_policy_event(
        tenant_id=user.tenant_id,
        datasource_id=datasource_id,
        actor=user.user_id,
        action=action,
    )
    return response


# ---------------------------------------------------------------------------
# GET /policies
# ---------------------------------------------------------------------------


@router.get(
    "/{datasource_id}/policies",
    response_model=PolicyResponse,
    status_code=status.HTTP_200_OK,
    summary="Return the current policy blob for a datasource.",
)
def get_policy(
    datasource_id: str = Path(..., min_length=1, max_length=36),
    user: CurrentUser = Depends(require_permission(_PERM_READ)),
) -> PolicyResponse:
    """Return the current :class:`DatasourcePolicy` row.

    Raises:
        404: When the datasource is missing for the caller's
            tenant **or** no policy has ever been written.
            The two cases are deliberately collapsed so the
            endpoint never leaks the existence of another
            tenant's data.
    """
    with get_session() as session:
        # Verify the datasource exists for the caller's
        # tenant (L1 isolation).
        ds = session.execute(
            select(Datasource).where(
                Datasource.id == datasource_id,
                Datasource.tenant_id == user.tenant_id,
            )
        ).scalar_one_or_none()
        if ds is None:
            raise NotFoundError("datasource", datasource_id)
        row = session.execute(
            select(DatasourcePolicy).where(
                DatasourcePolicy.datasource_id == datasource_id,
                DatasourcePolicy.tenant_id == user.tenant_id,
            )
        ).scalar_one_or_none()
        if row is None:
            raise NotFoundError("datasource_policy", datasource_id)
        updated_at = (
            row.updated_at.isoformat() if row.updated_at else None
        )
        return PolicyResponse(
            datasource_id=datasource_id,
            policies=dict(row.policies_json),
            updated_at=updated_at,
        )


# ---------------------------------------------------------------------------
# POST /suggest-pii
# ---------------------------------------------------------------------------


@router.post(
    "/{datasource_id}/suggest-pii",
    response_model=PIISuggestResponse,
    status_code=status.HTTP_200_OK,
    summary="Ask the agent-gateway to suggest PII columns for a datasource.",
)
async def suggest_pii(
    body: PIISuggestRequest,
    datasource_id: str = Path(..., min_length=1, max_length=36),
    user: CurrentUser = Depends(require_permission(_PERM_READ)),
    service: PIIService = Depends(_get_service_dep),
) -> PIISuggestResponse:
    """Return the agent-gateway's PII column suggestions.

    The endpoint is a thin wrapper around
    :meth:`PIIService.suggest_pii`; the heavy lifting (schema
    fetch, sample rows, LLM call, response parsing) lives in
    the service. The handler is ``async`` because the
    service is async (the LLM call + connector ``preview``
    are coroutines); the rest of the datasource surface
    remains sync because the rest of the connectors
    synchronously block on the driver thread.

    Returns:
        A :class:`PIISuggestResponse` with the list of
        :class:`PIIColumnSuggestion` items the model
        flagged. The list may be empty (200 with empty list
        is the success outcome for "no PII detected").

    Raises:
        404: When the datasource is missing for the
            caller's tenant.
        502: When the LLM call fails *and* the local stub
            also fails (the service degrades to the stub on
            LLM errors, so this is the rare "both failed"
            case).
    """
    # Verify the datasource exists *before* the service call
    # so a 404 lands in the response and not as a generic
    # 502. The check is a one-row SELECT (indexed on
    # ``(tenant_id, id)``) and is cheap relative to the LLM
    # round-trip.
    with get_session() as session:
        ds = session.execute(
            select(Datasource).where(
                Datasource.id == datasource_id,
                Datasource.tenant_id == user.tenant_id,
            )
        ).scalar_one_or_none()
        if ds is None:
            raise NotFoundError("datasource", datasource_id)
    # ``PIIService.suggest_pii`` is an async coroutine; we
    # call it via ``asyncio.run`` so the handler can stay
    # sync (matches the rest of the datasource surface).
    suggestions = await service.suggest_pii(
        tenant_id=user.tenant_id,
        datasource_id=datasource_id,
        tables=body.tables,
        sample_size=body.sample_size if body.sample_size is not None else 5,
    )
    return PIISuggestResponse(
        datasource_id=datasource_id,
        suggestions=[s.to_dict() for s in suggestions],
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _publish_policy_event(
    *,
    tenant_id: str,
    datasource_id: str,
    actor: str,
    action: str,
) -> None:
    """Publish a ``datasource.policy.{created,updated}.v1`` Kafka event.

    Failures are logged, never raised — the audit row is
    the durable source of truth; the Kafka event is the
    real-time contract. The call mirrors the
    :meth:`DatasourceService._publish_event` pattern (free
    function, no class state) so the policies module does
    not need to depend on the :class:`DatasourceService`
    instance.
    """
    import asyncio

    from aidp_events.producer import publish_event

    event_type = (
        "datasource.policy.created.v1"
        if action == "policy_created"
        else "datasource.policy.updated.v1"
    )
    try:
        asyncio.run(
            publish_event(
                topic="datasource.events.v1",
                event_type=event_type,
                payload={
                    "datasource_id": datasource_id,
                    "actor": actor,
                    "action": action,
                },
                tenant_id=tenant_id,
            )
        )
    except Exception as exc:  # pragma: no cover - defensive
        _LOG.warning(
            "failed to publish datasource policy event",
            extra={
                "event_type": event_type,
                "tenant_id": tenant_id,
                "error": str(exc),
            },
        )


__all__ = [
    "PIISuggestRequest",
    "PIISuggestResponse",
    "PolicyResponse",
    "PolicyUpsertRequest",
    "router",
]
