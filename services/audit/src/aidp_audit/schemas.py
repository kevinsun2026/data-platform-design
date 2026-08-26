"""Pydantic v2 request / response models for the audit query API.

The HTTP layer in :mod:`aidp_audit.api.query` projects ORM rows onto
these models. The split is the platform-standard
``api → service → model → schema`` layering: the ORM models are
mutable SQLAlchemy rows, while the Pydantic models are the immutable
wire shape that the client sees.

Notes on Pydantic v2
--------------------

- Every model has ``model_config = ConfigDict(extra="forbid", ...)``
  so a misnamed field from a downstream caller surfaces as a 400
  instead of being silently dropped.
- Timestamps are timezone-aware :class:`datetime`; the SQLAlchemy
  side produces them with ``DateTime(timezone=True)`` so the round
  trip is symmetric.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

# ---------------------------------------------------------------------------
# AuditEvent
# ---------------------------------------------------------------------------


class AuditEventSummary(BaseModel):
    """A single row in the list response (``GET /api/v1/audit/events``).

    Intentionally does *not* include the payload (encrypted or
    otherwise) — list views should be cheap and the payload is only
    fetched by the detail endpoint. The schema mirrors the
    :class:`aidp_audit.models.AidpAuditEvent` row but drops the
    internal bookkeeping columns (``created_by`` / ``updated_by`` /
    ``deleted_at``).
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=False)

    id: str = Field(min_length=1, max_length=36)
    tenant_id: str = Field(min_length=1, max_length=36)
    event_id: str = Field(min_length=1, max_length=64)
    topic: str = Field(min_length=1, max_length=128)
    producer: str = Field(min_length=1, max_length=128)
    event_type: str = Field(min_length=1, max_length=256)
    event_version: int = Field(ge=1)
    trace_id: str = Field(min_length=32, max_length=32)
    occurred_at: datetime
    actor_user_id: str | None = None
    actor_ip: str | None = None
    resource_type: str | None = None
    resource_id: str | None = None
    action: str = Field(min_length=1, max_length=64)
    outcome: str = Field(min_length=1, max_length=16)
    severity: str = Field(min_length=1, max_length=16)
    created_at: datetime


class AuditEventDetail(AuditEventSummary):
    """A single audit event with its *decrypted* payload.

    Returned by ``GET /api/v1/audit/events/{id}`` for callers who are
    authorised to see the row's tenant. The payload is a
    JSON-compatible dict (the producer's ``EventEnvelope.payload``
    rendered back into Python objects). The encryption at rest is
    transparent to the API consumer — they see plaintext, the database
    sees ciphertext.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=False)

    payload: dict[str, Any] = Field(default_factory=dict)
    headers: dict[str, str] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# SecurityEvent
# ---------------------------------------------------------------------------


class SecurityEventSummary(BaseModel):
    """A row in the security-events list response.

    Mirrors :class:`aidp_audit.models.SecurityEvent`. The
    ``details_json`` field is exposed verbatim so a security
    dashboard can render structured context (failed-login reason,
    API-key prefix, source IP, etc.).
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=False)

    id: str = Field(min_length=1, max_length=36)
    tenant_id: str = Field(min_length=1, max_length=36)
    audit_event_id: str = Field(min_length=1, max_length=36)
    event_type: str = Field(min_length=1, max_length=256)
    action: str = Field(min_length=1, max_length=64)
    outcome: str = Field(min_length=1, max_length=16)
    severity: str = Field(min_length=1, max_length=16)
    actor_user_id: str | None = None
    actor_ip: str | None = None
    resource_type: str | None = None
    resource_id: str | None = None
    reason: str | None = None
    occurred_at: datetime
    details: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime


# ---------------------------------------------------------------------------
# Paginated list response
# ---------------------------------------------------------------------------


class PageMeta(BaseModel):
    """Pagination metadata for list endpoints."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=False)

    page: int = Field(ge=1)
    page_size: int = Field(ge=1, le=200)
    total: int = Field(ge=0)


class AuditEventListResponse(BaseModel):
    """Body of ``GET /api/v1/audit/events``."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=False)

    items: list[AuditEventSummary]
    page: PageMeta


class SecurityEventListResponse(BaseModel):
    """Body of ``GET /api/v1/audit/security-events``."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=False)

    items: list[SecurityEventSummary]
    page: PageMeta


__all__ = [
    "AuditEventDetail",
    "AuditEventListResponse",
    "AuditEventSummary",
    "PageMeta",
    "SecurityEventListResponse",
    "SecurityEventSummary",
]
