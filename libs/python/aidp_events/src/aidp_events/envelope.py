"""Canonical ``EventEnvelope`` for the AIDP platform event bus.

Every event published via :mod:`aidp_events.producer` carries an
:class:`EventEnvelope` (JSON-serialized) as its Kafka record value. The
envelope is the contract every service can rely on for cross-service
tracing, idempotency, and tenant isolation at the bus layer.

Field reference
---------------

``event_id``
    UUID4 string. Globally unique per event; consumer dedup key.
``tenant_id``
    Tenant identifier (matches the L1 isolation boundary enforced by
    ``aidp_db.tenant``).
``occurred_at``
    UTC datetime the event was created (timezone-aware, serialized as
    ISO 8601 with offset on the wire).
``producer``
    Service name that produced the event (defaults to
    ``AIDP_SERVICE_NAME``).
``event_type``
    Reverse-DNS-style event name (``datasource.connection.created``).
``payload``
    Business payload, an arbitrary JSON-compatible dict.
``trace_id``
    32-character lowercase hex OpenTelemetry trace id. When no OTel
    span is active at publish time, a per-envelope fallback is generated
    (still 32-hex) so consumers can still join on it.
``event_version``
    Integer schema version of ``payload``. Defaults to ``1``.
``headers``
    Free-form string→string map forwarded as Kafka record headers.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from aidp_common.config import get_settings
from opentelemetry import trace
from pydantic import BaseModel, ConfigDict, Field, field_validator


def _utcnow() -> datetime:
    """Return a timezone-aware ``datetime`` in UTC."""
    return datetime.now(UTC)


def _new_event_id() -> str:
    """Return a fresh UUID4 string."""
    return str(uuid.uuid4())


def _trace_id_from_context() -> str:
    """Return the active OTel trace id as 32 lowercase hex, or a fresh one.

    When no recording span is active the function generates a per-envelope
    UUID-derived hex id. The contract is: ``trace_id`` is *always* a
    32-character lowercase hex string, regardless of whether OTel is set
    up at the call site.
    """
    span = trace.get_current_span()
    ctx = span.get_span_context() if span is not None else None
    if ctx is not None and ctx.trace_id:
        return format(ctx.trace_id, "032x")
    # No active span — generate a 128-bit random id and hex-encode it.
    return uuid.uuid4().hex


class EventEnvelope(BaseModel):
    """The wire format for every AIDP event.

    The model is immutable from the consumer's perspective (``frozen=True``)
    so a handler cannot accidentally mutate the envelope and leak state
    across retries. The factory function :func:`new_envelope` builds a new
    instance with the platform's required defaults applied.
    """

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=False,
    )

    event_id: str = Field(default_factory=_new_event_id, min_length=1, max_length=64)
    tenant_id: str = Field(min_length=1, max_length=128)
    occurred_at: datetime = Field(default_factory=_utcnow)
    producer: str = Field(min_length=1, max_length=128)
    event_type: str = Field(min_length=1, max_length=256)
    payload: dict[str, Any] = Field(default_factory=dict)
    trace_id: str = Field(min_length=32, max_length=32)
    event_version: int = Field(default=1, ge=1)
    headers: dict[str, str] = Field(default_factory=dict)

    @field_validator("trace_id")
    @classmethod
    def _validate_trace_id_hex(cls, value: str) -> str:
        """``trace_id`` is always 32 lowercase hex characters."""
        try:
            int(value, 16)
        except ValueError as exc:
            raise ValueError(
                f"trace_id must be 32 lowercase hex characters, got {value!r}"
            ) from exc
        if len(value) != 32 or any(c not in "0123456789abcdef" for c in value):
            raise ValueError(f"trace_id must be 32 lowercase hex characters, got {value!r}")
        return value

    @field_validator("tenant_id", "producer", "event_type")
    @classmethod
    def _strip_whitespace(cls, value: str) -> str:
        return value.strip()


def new_envelope(
    *,
    event_type: str,
    tenant_id: str,
    payload: dict[str, Any],
    producer: str | None = None,
    event_version: int = 1,
    headers: dict[str, str] | None = None,
    trace_id: str | None = None,
    event_id: str | None = None,
    occurred_at: datetime | None = None,
) -> EventEnvelope:
    """Build a fresh :class:`EventEnvelope` with platform defaults applied.

    Defaults pulled from the environment (see :class:`aidp_common.config.Settings`):

    - ``producer`` ← ``AIDP_SERVICE_NAME`` (fallback ``"aidp-unknown"``).
    - ``trace_id`` ← active OTel context, or a per-call random 32-hex.
    - ``event_id`` ← fresh ``uuid4()``.
    - ``occurred_at`` ← current UTC time.
    """
    return EventEnvelope(
        event_id=event_id or _new_event_id(),
        tenant_id=tenant_id,
        occurred_at=occurred_at or _utcnow(),
        producer=producer or get_settings().service_name,
        event_type=event_type,
        payload=payload,
        trace_id=trace_id or _trace_id_from_context(),
        event_version=event_version,
        headers=headers if headers is not None else {},
    )


__all__ = ["EventEnvelope", "new_envelope"]
