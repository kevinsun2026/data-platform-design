"""HTTP routes for the audit service.

The module is the read-only transport adapter for the audit data
plane. The actual decryption + projection work lives in
:func:`_row_to_event_detail` and the SQL queries below; the route
handlers are thin shells that bind the FastAPI ``Depends`` graph.

Routes
------

``GET /api/v1/audit/events``
    Paginated list with optional filters (``user_id`` / ``action`` /
    ``from`` / ``to``). The tenant is always taken from the
    caller's :class:`aidp_auth.jwt.CurrentUser` — the route never
    accepts a ``tenant_id`` query parameter, so cross-tenant access
    via the URL is impossible.

``GET /api/v1/audit/events/{id}``
    Detail view. Returns the row with the *decrypted* payload
    (``aidp_audit.crypto.decrypt_payload``). Same L1 enforcement
    via the row lookup; an attacker who guesses another tenant's
    id gets a 404, not a 200 with data.

``GET /api/v1/audit/security-events``
    Paginated list of high-sensitivity security events. Same
    L1 enforcement and same filter surface as the events list
    (minus the ``user_id``-style filters — the SOC dashboard
    typically filters by ``severity`` instead).

Error envelope
--------------

All errors flow through :class:`aidp_common.errors.AppError`; the
:class:`aidp_audit.api.errors.app_error_handler` renders the unified
``{"code", "message", "details", "trace_id"}`` envelope.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

from aidp_auth.dependencies import current_user
from aidp_auth.jwt import CurrentUser
from aidp_common.errors import NotFoundError
from aidp_db.session import get_session
from fastapi import APIRouter, Depends, Path, Query, status
from sqlalchemy import func, select

from aidp_audit.crypto import decrypt_payload
from aidp_audit.models import AidpAuditEvent, AuditPayload, SecurityEvent
from aidp_audit.schemas import (
    AuditEventDetail,
    AuditEventListResponse,
    AuditEventSummary,
    PageMeta,
    SecurityEventListResponse,
    SecurityEventSummary,
)

_LOG = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/audit", tags=["audit"])


# ---------------------------------------------------------------------------
# Row projection
# ---------------------------------------------------------------------------


def _row_to_event_summary(row: AidpAuditEvent) -> AuditEventSummary:
    """Project an :class:`AidpAuditEvent` ORM row onto the wire shape."""
    return AuditEventSummary.model_validate(
        {
            "id": row.id,
            "tenant_id": row.tenant_id,
            "event_id": row.event_id,
            "topic": row.topic,
            "producer": row.producer,
            "event_type": row.event_type,
            "event_version": row.event_version,
            "trace_id": row.trace_id,
            "occurred_at": row.occurred_at,
            "actor_user_id": row.actor_user_id,
            "actor_ip": row.actor_ip,
            "resource_type": row.resource_type,
            "resource_id": row.resource_id,
            "action": row.action,
            "outcome": row.outcome,
            "severity": row.severity,
            "created_at": row.created_at,
        }
    )


def _row_to_event_detail(row: AidpAuditEvent, payload: AuditPayload) -> AuditEventDetail:
    """Project an ``audit_events`` + ``audit_payloads`` pair onto the wire.

    Decrypts the payload (raises :class:`aidp_common.errors.UpstreamError`
    on GCM auth failure). The plaintext is rendered as a JSON dict
    via ``json.loads``; the producer's ``json.dumps`` is the round
    trip partner so the structure is preserved.
    """
    plaintext = decrypt_payload(
        ciphertext=payload.ciphertext,
        nonce=payload.nonce,
        tenant_id=row.tenant_id,
        event_id=row.event_id,
        event_type=row.event_type,
    )
    try:
        decoded: dict[str, Any] = json.loads(plaintext.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        # Defensive: a tampered row could have garbage. Surface
        # the bytes as a single field so the operator can
        # diagnose without leaking the original payload shape.
        decoded = {"_raw": plaintext.decode("utf-8", errors="replace")}
    return AuditEventDetail.model_validate(
        {
            "id": row.id,
            "tenant_id": row.tenant_id,
            "event_id": row.event_id,
            "topic": row.topic,
            "producer": row.producer,
            "event_type": row.event_type,
            "event_version": row.event_version,
            "trace_id": row.trace_id,
            "occurred_at": row.occurred_at,
            "actor_user_id": row.actor_user_id,
            "actor_ip": row.actor_ip,
            "resource_type": row.resource_type,
            "resource_id": row.resource_id,
            "action": row.action,
            "outcome": row.outcome,
            "severity": row.severity,
            "created_at": row.created_at,
            "payload": decoded,
            "headers": dict(row.headers_json),
        }
    )


def _row_to_security_summary(row: SecurityEvent) -> SecurityEventSummary:
    """Project a :class:`SecurityEvent` row onto the wire shape."""
    return SecurityEventSummary.model_validate(
        {
            "id": row.id,
            "tenant_id": row.tenant_id,
            "audit_event_id": row.audit_event_id,
            "event_type": row.event_type,
            "action": row.action,
            "outcome": row.outcome,
            "severity": row.severity,
            "actor_user_id": row.actor_user_id,
            "actor_ip": row.actor_ip,
            "resource_type": row.resource_type,
            "resource_id": row.resource_id,
            "reason": row.reason,
            "occurred_at": row.occurred_at,
            "details": dict(row.details_json),
            "created_at": row.created_at,
        }
    )


# ---------------------------------------------------------------------------
# List: events
# ---------------------------------------------------------------------------


@router.get(
    "/events",
    response_model=AuditEventListResponse,
    status_code=status.HTTP_200_OK,
    summary="List audit events for the caller's tenant (paginated).",
)
def list_audit_events(
    user_id: str | None = Query(
        default=None,
        min_length=1,
        max_length=64,
        description="Filter by ``actor_user_id`` (the subject of the action).",
    ),
    action: str | None = Query(
        default=None,
        min_length=1,
        max_length=64,
        description="Filter by ``action`` (e.g. ``login``, ``create``).",
    ),
    event_type: str | None = Query(
        default=None,
        min_length=1,
        max_length=256,
        description="Filter by ``event_type`` (e.g. ``iam.user.logged_in``).",
    ),
    outcome: str | None = Query(
        default=None,
        min_length=1,
        max_length=16,
        description="Filter by ``outcome`` (``success`` / ``failure`` / ``denied``).",
    ),
    from_: datetime | None = Query(
        default=None,
        alias="from",
        description="Lower bound on ``occurred_at`` (inclusive).",
    ),
    to: datetime | None = Query(
        default=None,
        description="Upper bound on ``occurred_at`` (inclusive).",
    ),
    page: int = Query(default=1, ge=1, le=10_000),
    page_size: int = Query(default=50, ge=1, le=200),
    user: CurrentUser = Depends(current_user),
) -> AuditEventListResponse:
    """Return audit events for the caller's tenant.

    The L1 isolation is enforced by the
    :func:`aidp_db.tenant.set_tenant_context` side effect of
    :data:`aidp_auth.dependencies.current_user` plus the
    ``WHERE tenant_id = :current_tenant`` injection done by
    :mod:`aidp_db.tenant`. Callers cannot pass a ``tenant_id`` —
    the value comes from the verified JWT claims, not the URL.
    """
    with get_session() as session:
        stmt = select(AidpAuditEvent)
        count_stmt = select(func.count()).select_from(AidpAuditEvent)
        if user_id is not None:
            stmt = stmt.where(AidpAuditEvent.actor_user_id == user_id)
            count_stmt = count_stmt.where(AidpAuditEvent.actor_user_id == user_id)
        if action is not None:
            stmt = stmt.where(AidpAuditEvent.action == action)
            count_stmt = count_stmt.where(AidpAuditEvent.action == action)
        if event_type is not None:
            stmt = stmt.where(AidpAuditEvent.event_type == event_type)
            count_stmt = count_stmt.where(AidpAuditEvent.event_type == event_type)
        if outcome is not None:
            stmt = stmt.where(AidpAuditEvent.outcome == outcome)
            count_stmt = count_stmt.where(AidpAuditEvent.outcome == outcome)
        if from_ is not None:
            stmt = stmt.where(AidpAuditEvent.occurred_at >= from_)
            count_stmt = count_stmt.where(AidpAuditEvent.occurred_at >= from_)
        if to is not None:
            stmt = stmt.where(AidpAuditEvent.occurred_at <= to)
            count_stmt = count_stmt.where(AidpAuditEvent.occurred_at <= to)
        # The L1 listener auto-rewrites these queries to add
        # ``WHERE tenant_id = :current_tenant``; the explicit
        # ``user.tenant_id`` filter is redundant but kept as
        # documentation of intent.
        stmt = stmt.where(AidpAuditEvent.tenant_id == user.tenant_id)
        count_stmt = count_stmt.where(AidpAuditEvent.tenant_id == user.tenant_id)
        total = int(session.execute(count_stmt).scalar() or 0)
        rows = (
            session.execute(
                stmt.order_by(AidpAuditEvent.occurred_at.desc())
                .offset((page - 1) * page_size)
                .limit(page_size)
            )
            .scalars()
            .all()
        )
    return AuditEventListResponse(
        items=[_row_to_event_summary(row) for row in rows],
        page=PageMeta(page=page, page_size=page_size, total=total),
    )


# ---------------------------------------------------------------------------
# Detail: event by id
# ---------------------------------------------------------------------------


@router.get(
    "/events/{event_id}",
    response_model=AuditEventDetail,
    status_code=status.HTTP_200_OK,
    summary="Return one audit event with its decrypted payload.",
)
def get_audit_event(
    event_id: str = Path(..., min_length=1, max_length=36),
    user: CurrentUser = Depends(current_user),
) -> AuditEventDetail:
    """Return a single audit event including its decrypted payload.

    A request for an id that does not exist (or belongs to a
    different tenant) returns ``404 NOT_FOUND``. The L1 listener
    + the explicit ``tenant_id == user.tenant_id`` filter ensure a
    cross-tenant probe is indistinguishable from a missing row.
    """
    with get_session() as session:
        row = session.execute(
            select(AidpAuditEvent).where(
                AidpAuditEvent.id == event_id,
                AidpAuditEvent.tenant_id == user.tenant_id,
            )
        ).scalar_one_or_none()
        if row is None:
            raise NotFoundError("audit event", event_id)
        payload = session.execute(
            select(AuditPayload).where(AuditPayload.event_id == row.id)
        ).scalar_one_or_none()
        if payload is None:
            # Defensive: every AuditEvent has a payload, but if a
            # row ever exists without one we want to surface that
            # clearly rather than crash.
            raise NotFoundError("audit event payload", event_id)
    return _row_to_event_detail(row, payload)


# ---------------------------------------------------------------------------
# List: security events
# ---------------------------------------------------------------------------


@router.get(
    "/security-events",
    response_model=SecurityEventListResponse,
    status_code=status.HTTP_200_OK,
    summary="List security events for the caller's tenant (paginated).",
)
def list_security_events(
    severity: str | None = Query(
        default=None,
        min_length=1,
        max_length=16,
        description="Filter by severity (``info`` / ``warning`` / ``error`` / ``critical``).",
    ),
    outcome: str | None = Query(
        default=None,
        min_length=1,
        max_length=16,
        description="Filter by outcome (``success`` / ``failure`` / ``denied``).",
    ),
    from_: datetime | None = Query(
        default=None,
        alias="from",
        description="Lower bound on ``occurred_at`` (inclusive).",
    ),
    to: datetime | None = Query(
        default=None,
        description="Upper bound on ``occurred_at`` (inclusive).",
    ),
    page: int = Query(default=1, ge=1, le=10_000),
    page_size: int = Query(default=50, ge=1, le=200),
    user: CurrentUser = Depends(current_user),
) -> SecurityEventListResponse:
    """Return security events for the caller's tenant.

    Same L1 enforcement as :func:`list_audit_events`. The
    filter surface is intentionally narrower (no ``user_id``
    filter, no ``action`` filter) because the SOC dashboard
    typically filters by severity first.
    """
    with get_session() as session:
        stmt = select(SecurityEvent)
        count_stmt = select(func.count()).select_from(SecurityEvent)
        if severity is not None:
            stmt = stmt.where(SecurityEvent.severity == severity)
            count_stmt = count_stmt.where(SecurityEvent.severity == severity)
        if outcome is not None:
            stmt = stmt.where(SecurityEvent.outcome == outcome)
            count_stmt = count_stmt.where(SecurityEvent.outcome == outcome)
        if from_ is not None:
            stmt = stmt.where(SecurityEvent.occurred_at >= from_)
            count_stmt = count_stmt.where(SecurityEvent.occurred_at >= from_)
        if to is not None:
            stmt = stmt.where(SecurityEvent.occurred_at <= to)
            count_stmt = count_stmt.where(SecurityEvent.occurred_at <= to)
        stmt = stmt.where(SecurityEvent.tenant_id == user.tenant_id)
        count_stmt = count_stmt.where(SecurityEvent.tenant_id == user.tenant_id)
        total = int(session.execute(count_stmt).scalar() or 0)
        rows = (
            session.execute(
                stmt.order_by(SecurityEvent.occurred_at.desc())
                .offset((page - 1) * page_size)
                .limit(page_size)
            )
            .scalars()
            .all()
        )
    return SecurityEventListResponse(
        items=[_row_to_security_summary(row) for row in rows],
        page=PageMeta(page=page, page_size=page_size, total=total),
    )


__all__ = ["router"]
