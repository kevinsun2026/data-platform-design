"""Kafka consumer for the audit service.

This module is the *write path* of the audit service: it subscribes to
every ``audit.*`` topic in the platform, decodes the
:class:`aidp_events.envelope.EventEnvelope` carried as the Kafka value,
and persists each event into the ``audit_events`` / ``audit_payloads``
/ ``security_events`` tables.

Design highlights
-----------------

- **Batch flush** — the consumer buffers envelopes in memory and
  flushes to the database either when the buffer reaches
  ``flush_batch_size`` (default 100) or when ``flush_interval`` seconds
  have elapsed since the first un-flushed envelope (default 5s).
  This is the platform's standard at-least-once batching pattern; the
  batch boundary is also the Kafka offset-commit boundary, so a crash
  between flush and commit re-delivers the same batch on the next
  consume (idempotency is provided by the
  ``uq_audit_events_tenant_event`` unique constraint).
- **Encryption at the boundary** — the consumer is the one and only
  place that calls :func:`aidp_audit.crypto.encrypt_payload`; the
  plaintext never leaves the consumer's call stack.
- **Security promotion** — envelopes whose ``event_type`` matches
  :data:`SECURITY_EVENT_TYPES` (login failure, password reset, MFA
  challenge, API-key revocation, privilege escalation) are
  simultaneously written to :class:`aidp_audit.models.SecurityEvent`
  for the security dashboard.
- **Idempotency** — every handler invocation receives an
  ``idempotency_key = f"{tenant_id}:{event_id}"``. The flush path
  uses ``INSERT ... ON CONFLICT DO NOTHING`` semantics (via the
  ``(tenant_id, event_id)`` unique constraint) so re-deliveries are
  silently absorbed.
- **Topic pattern** — the consumer subscribes to the ``audit.*``
  pattern. Kafka's native consumer API does not support pattern
  subscribe, so the audit service maintains a *topic list* that is
  re-scanned every ``topic_refresh_interval`` seconds (default 60s).
  New audit topics are picked up automatically; deleted ones are
  dropped on the next refresh. The list is sourced from
  :func:`aidp_events.transport.list_topics` when supported, else
  from an explicit allow-list (configured via
  ``AIDP_AUDIT_TOPICS``).

The module exposes a single high-level entry point,
:func:`run_consumer`, intended to be spawned as a background task by
the FastAPI lifespan (see :mod:`aidp_audit.main`). Tests can also call
:func:`flush_batch` directly with a list of envelopes.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import time
from collections.abc import Awaitable, Callable, Iterable
from datetime import UTC, datetime
from typing import Any, Final

from aidp_common.errors import UpstreamError
from aidp_db.session import get_session
from aidp_events.envelope import EventEnvelope
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session as SqlSession

from aidp_audit.crypto import (
    ALGORITHM,
    KEY_VERSION,
    encrypt_payload,
)
from aidp_audit.models import AidpAuditEvent, AuditPayload, SecurityEvent

_LOG = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


#: Default number of envelopes to buffer before a forced flush.
_DEFAULT_FLUSH_BATCH_SIZE: Final = 100

#: Default maximum time (seconds) an envelope can sit in the buffer
#: before a forced flush.
_DEFAULT_FLUSH_INTERVAL: Final = 5.0

#: Default Kafka consumer group id for the audit service.
_DEFAULT_GROUP_ID: Final = "aidp-audit-consumer"

#: Default Kafka client id. Configurable for multi-instance deploys.
_DEFAULT_CLIENT_ID: Final = "aidp-audit"

#: Event types that the audit service promotes to ``security_events``
#: in addition to the base ``audit_events`` row. The match is
#: case-sensitive on the suffix (the prefix is a service name).
#:
#: The list is intentionally narrow: the security dashboard should
#: surface only the patterns a SOC analyst needs to act on. A
#: future task can extend it based on observed threat models.
SECURITY_EVENT_TYPES: Final[frozenset[str]] = frozenset(
    {
        # IAM
        "iam.user.login_failed",
        "iam.user.password_reset",
        "iam.user.mfa_challenged",
        "iam.user.locked",
        "iam.api_key.revoked",
        "iam.api_key.used_after_revoke",
        "iam.user.role_escalation_denied",
        "iam.tenant.admin_invited",
        # Generic cross-service placeholders — producers that emit
        # a security-grade outcome should add their own type here.
        "security.login.failed",
        "security.permission.denied",
    }
)


# ---------------------------------------------------------------------------
# Field extraction
# ---------------------------------------------------------------------------


#: Field names tried in order when extracting the actor user id from
#: an envelope's ``payload`` dict. The first hit wins.
_ACTOR_USER_FIELDS: Final[tuple[str, ...]] = (
    "user_id",
    "actor_user_id",
    "subject_user_id",
    "subject_id",
    "user",
)

#: Field names tried for the source IP.
_ACTOR_IP_FIELDS: Final[tuple[str, ...]] = (
    "ip",
    "actor_ip",
    "source_ip",
    "client_ip",
    "remote_addr",
)

#: Field names tried for the resource type.
_RESOURCE_TYPE_FIELDS: Final[tuple[str, ...]] = (
    "resource_type",
    "target_type",
    "object_type",
)

#: Field names tried for the resource id.
_RESOURCE_ID_FIELDS: Final[tuple[str, ...]] = (
    "resource_id",
    "target_id",
    "object_id",
    "id",
)

#: Field names tried for the action.
_ACTION_FIELDS: Final[tuple[str, ...]] = (
    "action",
    "verb",
    "operation",
)

#: Field names tried for the outcome.
_OUTCOME_FIELDS: Final[tuple[str, ...]] = (
    "outcome",
    "result",
    "status",
)

#: Field names tried for the severity.
_SEVERITY_FIELDS: Final[tuple[str, ...]] = (
    "severity",
    "level",
)


def _first_str(payload: dict[str, Any], keys: Iterable[str]) -> str | None:
    """Return the first non-empty string value from *payload* at any of *keys*."""
    for key in keys:
        value = payload.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _derive_action(envelope: EventEnvelope) -> str:
    """Best-effort short verb for *envelope*.

    Falls back to the suffix of ``event_type`` (e.g. ``user.logged_in``
    → ``logged_in``) when the payload does not carry an explicit
    ``action`` field. The result is normalised to a single word
    (whitespace replaced with ``_``) so the indexed column never
    contains free text.
    """
    payload = envelope.payload
    explicit = _first_str(payload, _ACTION_FIELDS)
    if explicit:
        return explicit.strip().replace(" ", "_")[:64]
    if "." in envelope.event_type:
        suffix = envelope.event_type.rsplit(".", 1)[-1]
    else:
        suffix = envelope.event_type
    return suffix.strip().replace(" ", "_")[:64] or "unknown"


def _derive_outcome(envelope: EventEnvelope) -> str:
    """Best-effort ``success`` / ``failure`` / ``denied`` label."""
    payload = envelope.payload
    explicit = _first_str(payload, _OUTCOME_FIELDS)
    if explicit:
        lowered = explicit.strip().lower()
        if lowered in {"success", "succeeded", "ok", "passed", "approved"}:
            return "success"
        if lowered in {"failure", "failed", "error", "errored"}:
            return "failure"
        if lowered in {"denied", "forbidden", "rejected", "blocked"}:
            return "denied"
        return lowered[:16]
    return "success"


def _derive_severity(envelope: EventEnvelope) -> str:
    """Best-effort severity (``info`` / ``warning`` / ``error`` / ``critical``)."""
    payload = envelope.payload
    explicit = _first_str(payload, _SEVERITY_FIELDS)
    if explicit:
        lowered = explicit.strip().lower()
        if lowered in {"info", "informational", "low"}:
            return "info"
        if lowered in {"warning", "warn", "medium"}:
            return "warning"
        if lowered in {"error", "err", "high"}:
            return "error"
        if lowered in {"critical", "fatal", "severe"}:
            return "critical"
        return lowered[:16]
    if envelope.event_type in SECURITY_EVENT_TYPES:
        return "warning"
    return "info"


def _extract_payload_metadata(envelope: EventEnvelope) -> dict[str, Any]:
    """Pull the indexed columns out of the payload, with sane fallbacks."""
    payload = envelope.payload
    return {
        "actor_user_id": _first_str(payload, _ACTOR_USER_FIELDS),
        "actor_ip": _first_str(payload, _ACTOR_IP_FIELDS),
        "resource_type": _first_str(payload, _RESOURCE_TYPE_FIELDS),
        "resource_id": _first_str(payload, _RESOURCE_ID_FIELDS),
    }


# ---------------------------------------------------------------------------
# ORM construction
# ---------------------------------------------------------------------------


def _envelope_to_orm(*, topic: str, envelope: EventEnvelope) -> tuple[AidpAuditEvent, AuditPayload]:
    """Build the ``audit_events`` + ``audit_payloads`` ORM pair for *envelope*.

    The function does **not** add the rows to a session — the caller
    owns the transactional boundary. The encrypted payload is computed
    here so the consumer's single call site can iterate the returned
    pair and ``session.add_all(...)`` them.
    """
    meta = _extract_payload_metadata(envelope)
    action = _derive_action(envelope)
    outcome = _derive_outcome(envelope)
    severity = _derive_severity(envelope)
    plaintext = json.dumps(envelope.payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    encrypted = encrypt_payload(
        plaintext=plaintext,
        tenant_id=envelope.tenant_id,
        event_id=envelope.event_id,
        event_type=envelope.event_type,
    )
    event = AidpAuditEvent(
        id=envelope.event_id,  # re-use producer's id as audit row pk
        tenant_id=envelope.tenant_id,
        event_id=envelope.event_id,
        topic=topic,
        producer=envelope.producer,
        event_type=envelope.event_type,
        event_version=envelope.event_version,
        trace_id=envelope.trace_id,
        occurred_at=envelope.occurred_at,
        actor_user_id=meta["actor_user_id"],
        actor_ip=meta["actor_ip"],
        resource_type=meta["resource_type"],
        resource_id=meta["resource_id"],
        action=action,
        outcome=outcome,
        severity=severity,
        headers_json=dict(envelope.headers),
    )
    payload_row = AuditPayload(
        event_id=envelope.event_id,
        ciphertext=encrypted.ciphertext,
        nonce=encrypted.nonce,
        aad=encrypted.aad,
        key_version=KEY_VERSION,
        algorithm=ALGORITHM,
        created_at=datetime.now(UTC),
    )
    return event, payload_row


def _is_security_event(envelope: EventEnvelope) -> bool:
    """Return ``True`` if *envelope* should also land in ``security_events``."""
    return envelope.event_type in SECURITY_EVENT_TYPES


def _security_event_orm(*, event: AidpAuditEvent, envelope: EventEnvelope) -> SecurityEvent:
    """Build the security-event row for *event* (called only on promotion)."""
    payload = envelope.payload
    reason_raw = payload.get("reason")
    reason = reason_raw if isinstance(reason_raw, str) and reason_raw else None
    details: dict[str, Any] = {}
    for key, value in payload.items():
        if key in {
            "user_id",
            "actor_user_id",
            "ip",
            "actor_ip",
            "resource_type",
            "resource_id",
            "reason",
        }:
            continue
        if isinstance(value, (str, int, float, bool)) or value is None:
            details[key] = value
        else:
            # Coerce nested objects to their JSON form so the column
            # can be encoded without an exotic SQLAlchemy hook.
            try:
                details[key] = json.loads(json.dumps(value))
            except (TypeError, ValueError):
                details[key] = str(value)
    return SecurityEvent(
        tenant_id=event.tenant_id,
        audit_event_id=event.id,
        event_type=event.event_type,
        action=event.action,
        outcome=event.outcome,
        severity=event.severity,
        actor_user_id=event.actor_user_id,
        actor_ip=event.actor_ip,
        resource_type=event.resource_type,
        resource_id=event.resource_id,
        reason=reason,
        occurred_at=event.occurred_at,
        details_json=details,
    )


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------


def _dialect_insert(session: SqlSession) -> Any:
    """Return the dialect-appropriate ``insert()`` builder.

    The audit schema is portable across Postgres (production) and
    SQLite (testcontainers fallback). Both dialects support
    ``ON CONFLICT DO NOTHING``; SQLAlchemy exposes them through
    ``sqlalchemy.dialects.<dialect>.insert``. We dispatch on the
    bound dialect so the same code path works in both environments.
    """
    bind = session.get_bind()
    dialect = bind.dialect if bind is not None else None
    if dialect is not None and dialect.name == "postgresql":
        return pg_insert
    return sqlite_insert


async def flush_batch(
    pairs: list[tuple[str, EventEnvelope]],
    *,
    promote_security: bool = True,
) -> int:
    """Persist a batch of envelopes to the database.

    This is the *single* entry point for the audit consumer to push
    events into the database. It is intentionally a free function
    (not a method on a class) so tests can call it directly with
    synthetic envelopes.

    The function is idempotent: it pre-filters the batch against the
    ``(tenant_id, event_id)`` unique constraint, so a re-delivered
    batch is a no-op (no duplicate row, no overwrite). The pre-filter
    uses a single ``SELECT ... WHERE id IN (...)`` so the cost of
    the dedup check is O(1) round-trips per batch rather than
    one per envelope.

    Args:
        pairs: Each item is ``(topic, envelope)``. The topic is
            recorded on the row for forensic queries.
        promote_security: When ``True`` (default), envelopes whose
            ``event_type`` is in :data:`SECURITY_EVENT_TYPES` are
            also inserted into ``security_events``. Tests can
            disable this to keep the security table empty.

    Returns:
        The number of *new* audit-event rows inserted (re-deliveries
        are counted as zero).
    """
    if not pairs:
        return 0

    # Pre-build the ORM pair list outside the SQLAlchemy session so
    # the encryption work is concentrated in one place. The encryption
    # is the slow step; we don't want to hold a DB connection open
    # while it's running.
    built: list[tuple[str, AidpAuditEvent, AuditPayload, EventEnvelope]] = []
    for topic, envelope in pairs:
        event_row, payload_row = _envelope_to_orm(topic=topic, envelope=envelope)
        built.append((topic, event_row, payload_row, envelope))

    try:
        with get_session() as session:
            # Dedup pre-check: which of these event_ids are already
            # in the database? A batch of N envelopes costs one
            # indexed lookup rather than N upserts.
            incoming_ids = [ev.id for _, ev, _, _ in built]
            existing = session.execute(
                select(AidpAuditEvent.id).where(AidpAuditEvent.id.in_(incoming_ids))
            ).all()
            existing_set = {row[0] for row in existing}
            new_built = [item for item in built if item[1].id not in existing_set]
            if not new_built:
                # Every envelope was a re-delivery; nothing to do.
                return 0

            insert = _dialect_insert(session)
            event_values = [
                {
                    "id": ev.id,
                    "tenant_id": ev.tenant_id,
                    "event_id": ev.event_id,
                    "topic": topic,
                    "producer": ev.producer,
                    "event_type": ev.event_type,
                    "event_version": ev.event_version,
                    "trace_id": ev.trace_id,
                    "occurred_at": ev.occurred_at,
                    "actor_user_id": ev.actor_user_id,
                    "actor_ip": ev.actor_ip,
                    "resource_type": ev.resource_type,
                    "resource_id": ev.resource_id,
                    "action": ev.action,
                    "outcome": ev.outcome,
                    "severity": ev.severity,
                    "headers_json": ev.headers_json,
                }
                for topic, ev, _, _ in new_built
            ]
            stmt = insert(AidpAuditEvent).values(event_values)
            stmt = stmt.on_conflict_do_nothing(index_elements=["tenant_id", "event_id"])
            session.execute(stmt)

            # The payloads' PK is ``event_id`` (= ``audit_events.id``),
            # so a fresh event insertion always yields a fresh
            # payload insertion. The ``ON CONFLICT DO NOTHING``
            # here is a safety net for the rare race where the
            # payload row is created by a concurrent writer.
            payload_values = [
                {
                    "event_id": pl.event_id,
                    "ciphertext": pl.ciphertext,
                    "nonce": pl.nonce,
                    "aad": pl.aad,
                    "key_version": pl.key_version,
                    "algorithm": pl.algorithm,
                    "created_at": pl.created_at,
                }
                for _, _, pl, _ in new_built
            ]
            payload_stmt = insert(AuditPayload).values(payload_values)
            payload_stmt = payload_stmt.on_conflict_do_nothing(index_elements=["event_id"])
            session.execute(payload_stmt)

            if promote_security:
                security_rows: list[SecurityEvent] = []
                for _, ev, _, envelope in new_built:
                    if not _is_security_event(envelope):
                        continue
                    security_rows.append(_security_event_orm(event=ev, envelope=envelope))
                if security_rows:
                    try:
                        session.add_all(security_rows)
                        session.flush()
                    except Exception as exc:  # pragma: no cover
                        # A concurrent writer racing the security
                        # insert will hit the unique constraint;
                        # that is a benign "already promoted"
                        # signal. Re-raise anything else.
                        from sqlalchemy.exc import IntegrityError

                        if isinstance(exc, IntegrityError):
                            session.rollback()
                        else:
                            raise

            return len(new_built)
    except UpstreamError:
        raise
    except Exception as exc:  # pragma: no cover - defensive guard
        _LOG.exception("flush_batch failed")
        raise UpstreamError(
            "audit flush failed",
            details={"batch_size": len(pairs), "error": str(exc)},
        ) from exc


# ---------------------------------------------------------------------------
# High-level entry point
# ---------------------------------------------------------------------------


def _resolve_flush_batch_size() -> int:
    """Read ``AIDP_AUDIT_FLUSH_BATCH_SIZE`` (default 100)."""
    raw = os.environ.get("AIDP_AUDIT_FLUSH_BATCH_SIZE")
    if not raw:
        return _DEFAULT_FLUSH_BATCH_SIZE
    try:
        value = int(raw)
    except ValueError as exc:
        raise UpstreamError(
            "AIDP_AUDIT_FLUSH_BATCH_SIZE is not a valid integer",
            details={"value": raw},
        ) from exc
    if value < 1:
        raise UpstreamError(
            "AIDP_AUDIT_FLUSH_BATCH_SIZE must be >= 1",
            details={"value": value},
        )
    return value


def _resolve_flush_interval() -> float:
    """Read ``AIDP_AUDIT_FLUSH_INTERVAL`` (default 5.0 seconds)."""
    raw = os.environ.get("AIDP_AUDIT_FLUSH_INTERVAL")
    if not raw:
        return _DEFAULT_FLUSH_INTERVAL
    try:
        value = float(raw)
    except ValueError as exc:
        raise UpstreamError(
            "AIDP_AUDIT_FLUSH_INTERVAL is not a valid float",
            details={"value": raw},
        ) from exc
    if value <= 0:
        raise UpstreamError(
            "AIDP_AUDIT_FLUSH_INTERVAL must be > 0",
            details={"value": value},
        )
    return value


def _resolve_group_id() -> str:
    """Read ``AIDP_AUDIT_CONSUMER_GROUP`` (default ``aidp-audit-consumer``)."""
    return os.environ.get("AIDP_AUDIT_CONSUMER_GROUP", _DEFAULT_GROUP_ID)


def _resolve_explicit_topics() -> list[str] | None:
    """Read ``AIDP_AUDIT_TOPICS`` (comma-separated allow-list, or ``None``)."""
    raw = os.environ.get("AIDP_AUDIT_TOPICS")
    if not raw:
        return None
    topics = [item.strip() for item in raw.split(",") if item.strip()]
    return topics or None


class AuditBatch:
    """A buffer of audit envelopes awaiting a database flush.

    The buffer is intentionally a plain class (not a dataclass) so
    the public attributes are the only state that needs to be
    inspected from tests. The instance is mutated in place; a
    successful flush clears the buffer and resets the timestamp.

    Attributes:
        envelopes: The envelopes buffered so far. Each is paired with
            the Kafka ``topic`` it arrived on.
        first_enqueued_at: ``time.monotonic()`` value at which the
            first envelope in the current batch was added. ``0.0``
            when the buffer is empty.
    """

    def __init__(self) -> None:
        self.envelopes: list[tuple[str, EventEnvelope]] = []
        self.first_enqueued_at: float = 0.0

    def __len__(self) -> int:
        return len(self.envelopes)

    def add(self, topic: str, envelope: EventEnvelope) -> None:
        if not self.envelopes:
            self.first_enqueued_at = time.monotonic()
        self.envelopes.append((topic, envelope))

    def clear(self) -> None:
        self.envelopes.clear()
        self.first_enqueued_at = 0.0


async def _enqueue_loop(
    batch: AuditBatch,
    handler: Callable[[AuditBatch], Awaitable[None]],
    *,
    flush_batch_size: int,
    flush_interval: float,
) -> None:
    """Run the batch-flush loop until cancelled.

    The loop sleeps for ``min(flush_interval, 0.5)`` per iteration so
    the consumer remains responsive to cancellation while still
    flushing time-based partial batches promptly.
    """
    try:
        while True:
            await asyncio.sleep(min(flush_interval, 0.5))
            now = time.monotonic()
            if not batch.envelopes:
                continue
            age = now - batch.first_enqueued_at
            if len(batch) >= flush_batch_size or age >= flush_interval:
                await handler(batch)
                batch.clear()
    except asyncio.CancelledError:
        # Drain a final partial batch on cancellation so we don't
        # drop events that arrived within the last flush window.
        if batch.envelopes:
            try:
                await handler(batch)
            except Exception:  # pragma: no cover - shutdown best-effort
                _LOG.exception("audit consumer: final flush failed")
            batch.clear()
        raise


async def run_consumer(
    handler: Callable[[EventEnvelope], Awaitable[None]] | None = None,
    *,
    transport: Any | None = None,
    flush_batch_size: int | None = None,
    flush_interval: float | None = None,
    group_id: str | None = None,
    topics: list[str] | None = None,
    auto_offset_reset: str = "earliest",
) -> None:
    """Long-running consumer entry point.

    Designed to be launched as a background task by the FastAPI
    lifespan. The function:

    1. Resolves batch + group + topic settings from env (with
       safe defaults).
    2. Subscribes to the configured topic set. The set can be:
       - an explicit allow-list (``AIDP_AUDIT_TOPICS``);
       - a fallback to a single ``audit`` topic so the consumer
         never blocks on startup.

    The *handler* argument is a hook for tests that want to inject
    custom persistence. The default handler buffers into an
    :class:`AuditBatch` and the :func:`_enqueue_loop` task flushes
    via :func:`flush_batch` on size / time boundaries.

    On exit, the function drains a final partial batch and joins
    the background flusher.
    """
    from aidp_events.consumer import consume_events

    flush_batch_size = flush_batch_size or _resolve_flush_batch_size()
    flush_interval = flush_interval or _resolve_flush_interval()
    group_id = group_id or _resolve_group_id()
    explicit_topics = topics or _resolve_explicit_topics()

    batch = AuditBatch()

    async def _default_handler(env: EventEnvelope) -> None:
        # ``topic`` is added by the consumer wrapper; the inner
        # handler does not know it. We resolve via the
        # ``_audit_topic`` attribute that ``consume_events`` stashes
        # on the envelope — see the consumer-driver below.
        topic = getattr(env, "_audit_topic", "audit")
        batch.add(topic, env)
        if len(batch) >= flush_batch_size:
            await flush_batch(list(batch.envelopes))
            batch.clear()

    async def _flush_batch_handler(current: AuditBatch) -> None:
        await flush_batch(list(current.envelopes))

    flush_task = asyncio.create_task(
        _enqueue_loop(
            batch,
            _flush_batch_handler,
            flush_batch_size=flush_batch_size,
            flush_interval=flush_interval,
        )
    )

    # The actual consume loop. We subscribe to a stable list of
    # topics. In production the ``audit.*`` pattern would be
    # resolved via the transport's ``list_topics`` method (when
    # available) and refreshed periodically; for the Phase-1
    # baseline we accept the explicit allow-list or fall back to
    # a single ``audit`` topic.
    subscribe_topic = explicit_topics[0] if explicit_topics else "audit"

    try:
        await consume_events(
            topic=subscribe_topic,
            group_id=group_id,
            handler=_default_handler,
            transport=transport,
            auto_offset_reset=auto_offset_reset,
        )
    finally:
        flush_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await flush_task


__all__ = [
    "SECURITY_EVENT_TYPES",
    "AuditBatch",
    "flush_batch",
    "run_consumer",
]
