"""SQLAlchemy 2.0 declarative models for the AIDP Audit service.

This module is the schema source of truth for the audit service. Every
table participates in the platform's mandatory L1 tenant isolation:

- :class:`AidpAuditEvent` — one row per audit event consumed from any
  ``audit.*`` Kafka topic. ``(tenant_id, event_id)`` is unique so the
  consumer's at-least-once delivery can be paired with a DB-side
  idempotency check (re-delivery of the same ``(tenant_id, event_id)``
  pair is a no-op).
- :class:`AuditPayload` — one-to-one child of :class:`AidpAuditEvent`
  carrying the AES-256-GCM-encrypted event payload. The plaintext never
  lives in the database; only the row that owns the decryption key
  (i.e. the audit service) can render it back to clients.
- :class:`SecurityEvent` — one row per high-sensitivity audit event
  (login failures, password resets, privilege-escalation attempts,
  revoked-API-key usage, etc.). Mirrors the public fields of
  :class:`AidpAuditEvent` so a security dashboard can read this table
  without joining, while the full payload still lives in the parent
  ``audit_events`` / ``audit_payloads`` pair.

All three tables derive from :class:`aidp_common.models.TenantScoped`, so
the ``aidp_db.tenant`` listener auto-injects
``WHERE tenant_id = :current_tenant`` on every select and the per-service
L1 contract is enforced without any custom predicate code.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from aidp_common.models import IdModel, TenantScoped, TimestampMixin
from sqlalchemy import (
    JSON,
    BigInteger,
    DateTime,
    ForeignKey,
    Index,
    LargeBinary,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

# ---------------------------------------------------------------------------
# Declarative base — service-local metadata, per the AIDP convention that
# each service owns its own ``MetaData`` so cross-service imports do not
# leak. Alembic's ``env.py`` and the test fixtures import it directly from
# this module.
# ---------------------------------------------------------------------------


class Base(DeclarativeBase):
    """SQLAlchemy declarative base for the Audit service."""


# ---------------------------------------------------------------------------
# AuditEvent
# ---------------------------------------------------------------------------


class AidpAuditEvent(Base, IdModel, TimestampMixin, TenantScoped):
    """One row per audit event consumed from an ``audit.*`` topic.

    Attributes:
        tenant_id: Tenant the event belongs to (L1 isolation key).
        event_id: UUID4 assigned by the producer. Unique within a tenant
            so re-deliveries (at-least-once) become no-ops via the
            ``uq_audit_events_tenant_event`` unique constraint.
        topic: Source Kafka topic (e.g. ``iam.audit``,
            ``datasource.connections``). Stamped for forensic queries.
        producer: Service that emitted the event (``AIDP_SERVICE_NAME``
            of the producer). Copied from :attr:`EventEnvelope.producer`.
        event_type: Reverse-DNS event name
            (e.g. ``iam.user.logged_in``,
            ``datasource.connection.created``).
        event_version: Integer schema version of the event payload.
        trace_id: 32-character lowercase-hex OpenTelemetry trace id, or
            the producer's per-envelope fallback.
        occurred_at: Producer-side ``occurred_at`` (UTC, timezone-aware).
        actor_user_id: Subject user id (when applicable; ``NULL`` for
            system events). Mirrors :attr:`EventEnvelope.payload.user_id`
            after extraction.
        actor_ip: Source IP (``request.ip``) when the producer
            recorded one.
        resource_type: Logical resource kind the event is *about*
            (``"user"`` / ``"tenant"`` / ``"datasource"`` / ...).
        resource_id: Per-resource identifier (string; the platform does
            not enforce UUIDs so producers can use ARNs, slugs, etc.).
        action: Short verb describing the action (``"login"``,
            ``"create"``, ``"delete"`` ...). Indexed because it appears
            in almost every UI filter.
        outcome: ``"success"`` / ``"failure"`` / ``"denied"`` — the
            same vocabulary the brief uses for security events.
        severity: ``"info"`` / ``"warning"`` / ``"error"`` /
            ``"critical"`` — coarse routing hint. The actual security
            promotion happens in :class:`SecurityEvent`.
        headers_json: Producer-supplied envelope ``headers`` (string →
            string) preserved verbatim.
        payload: The encrypted payload row (one-to-one). The relation
            is loaded lazily so the common list query does not pay
            for payload decryption.
    """

    __tablename__ = "audit_events"

    # ``tenant_id`` carries an FK to ``tenants.id`` so the row participates
    # in the L1 listener's WHERE-clause path. The FK is cross-schema
    # (audit service does not own the ``tenants`` table) so we do not
    # declare ``ondelete`` — a tenant row is soft-deleted via
    # ``deleted_at``, never hard-deleted, and audit events must outlive
    # the tenant that produced them for the platform's compliance
    # contract.
    tenant_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("tenants.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    event_id: Mapped[str] = mapped_column(String(64), nullable=False)
    topic: Mapped[str] = mapped_column(String(128), nullable=False)
    producer: Mapped[str] = mapped_column(String(128), nullable=False)
    event_type: Mapped[str] = mapped_column(String(256), nullable=False)
    event_version: Mapped[int] = mapped_column(BigInteger, nullable=False, default=1)
    trace_id: Mapped[str] = mapped_column(String(32), nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    actor_user_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    actor_ip: Mapped[str | None] = mapped_column(String(64), nullable=True)
    resource_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    resource_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    action: Mapped[str] = mapped_column(String(64), nullable=False)
    outcome: Mapped[str] = mapped_column(String(16), nullable=False, default="success")
    severity: Mapped[str] = mapped_column(String(16), nullable=False, default="info")
    headers_json: Mapped[dict[str, str]] = mapped_column(JSON, nullable=False, default=dict)

    payload: Mapped[AuditPayload | None] = relationship(
        "AuditPayload",
        back_populates="event",
        cascade="all, delete-orphan",
        uselist=False,
    )

    __table_args__ = (
        UniqueConstraint("tenant_id", "event_id", name="uq_audit_events_tenant_event"),
        Index("ix_audit_events_tenant_action", "tenant_id", "action"),
        Index("ix_audit_events_tenant_occurred_at", "tenant_id", "occurred_at"),
        Index("ix_audit_events_tenant_user", "tenant_id", "actor_user_id"),
    )

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return (
            f"AidpAuditEvent(id={self.id!r}, tenant_id={self.tenant_id!r}, "
            f"event_type={self.event_type!r}, action={self.action!r})"
        )


# ---------------------------------------------------------------------------
# AuditPayload
# ---------------------------------------------------------------------------


class AuditPayload(Base):
    """AES-256-GCM-encrypted payload for one :class:`AidpAuditEvent`.

    The table is intentionally separate from ``audit_events`` so list
    queries (``SELECT ... ORDER BY occurred_at``) do not pay the storage
    cost of pulling every payload. The ``event_id`` FK is unique (one
    payload per event); the parent row is loaded only when the caller
    explicitly opts in.

    Attributes:
        event_id: FK to :attr:`AidpAuditEvent.id`. Unique.
        ciphertext: AES-256-GCM ciphertext (raw bytes, opaque). The
            plaintext is the producer's ``EventEnvelope.payload`` JSON,
            encoded as UTF-8.
        nonce: 12-byte AES-GCM nonce. Stored as :class:`LargeBinary`
            because the underlying Postgres column is ``BYTEA`` (and
            SQLite stores it as ``BLOB``).
        aad: Additional Authenticated Data — typically
            ``f"{tenant_id}:{event_id}:{event_type}"``. Storing the
            AAD lets us reject replays across tenants (an event
            produced by tenant A cannot be decrypted as tenant B).
        key_version: Identifier of the encryption key in use. Lets ops
            rotate the key without re-encrypting historical rows
            immediately; the decrypt path looks the key up by version.
        algorithm: Cipher identifier. ``"AES-256-GCM"`` today; the
            field is reserved so a future migration can store
            ``"AES-256-GCM-v2"`` per row.
        created_at: Insertion timestamp. Non-null (no soft-delete
            here — payloads are append-only).
    """

    __tablename__ = "audit_payloads"

    event_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("audit_events.id", ondelete="CASCADE"),
        primary_key=True,
    )
    ciphertext: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    nonce: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
    aad: Mapped[str] = mapped_column(String(512), nullable=False)
    key_version: Mapped[str] = mapped_column(String(32), nullable=False, default="v1")
    algorithm: Mapped[str] = mapped_column(String(32), nullable=False, default="AES-256-GCM")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    event: Mapped[AidpAuditEvent] = relationship("AidpAuditEvent", back_populates="payload")

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return (
            f"AuditPayload(event_id={self.event_id!r}, "
            f"algorithm={self.algorithm!r}, key_version={self.key_version!r})"
        )


# ---------------------------------------------------------------------------
# SecurityEvent
# ---------------------------------------------------------------------------


class SecurityEvent(Base, IdModel, TimestampMixin, TenantScoped):
    """High-sensitivity security event.

    The audit service promotes a subset of :class:`AidpAuditEvent` rows
    into this table when they match a security pattern (login failure,
    MFA challenge, API-key revocation, privilege escalation, ...). The
    ``audit_event_id`` FK is the forensic link to the underlying audit
    row; the rest of the columns are denormalized for fast security
    dashboards.

    Attributes:
        audit_event_id: FK to :class:`AidpAuditEvent.id`. Unique within
            a tenant — the same event cannot be promoted twice.
        event_type: Same string as :attr:`AidpAuditEvent.event_type`.
        action: Same as :attr:`AidpAuditEvent.action`.
        outcome: ``"success"`` / ``"failure"`` / ``"denied"``.
        severity: ``"info"`` / ``"warning"`` / ``"error"`` /
            ``"critical"`` — security events are pre-filtered for
            ``"warning"`` and above, but the field is preserved for
            the dashboard's colour coding.
        actor_user_id: Subject user id (``NULL`` for unauthenticated
            events such as a login failure for an unknown email).
        actor_ip: Source IP when the producer recorded one.
        resource_type / resource_id: Mirrors the parent row.
        reason: Short human-readable reason. Producers that have a
            structured failure reason (e.g. ``"password_mismatch"``)
            surface it here; free text is fine.
        occurred_at: Producer-side ``occurred_at``. Mirrors
            :attr:`AidpAuditEvent.occurred_at` for indexing locality.
        details_json: Free-form structured detail (e.g. the JWT
            ``jti`` of the failed refresh, the API-key prefix, the
            offending IP). Keep this small; large blobs belong on
            the parent row's payload.
    """

    __tablename__ = "security_events"

    tenant_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("tenants.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    audit_event_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("audit_events.id", ondelete="CASCADE"),
        nullable=False,
    )
    event_type: Mapped[str] = mapped_column(String(256), nullable=False)
    action: Mapped[str] = mapped_column(String(64), nullable=False)
    outcome: Mapped[str] = mapped_column(String(16), nullable=False, default="failure")
    severity: Mapped[str] = mapped_column(String(16), nullable=False, default="warning")
    actor_user_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    actor_ip: Mapped[str | None] = mapped_column(String(64), nullable=True)
    resource_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    resource_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    reason: Mapped[str | None] = mapped_column(String(512), nullable=True)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    details_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)

    __table_args__ = (
        UniqueConstraint("tenant_id", "audit_event_id", name="uq_security_events_tenant_event"),
        Index(
            "ix_security_events_tenant_occurred_at",
            "tenant_id",
            "occurred_at",
        ),
        Index("ix_security_events_tenant_severity", "tenant_id", "severity"),
    )

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return (
            f"SecurityEvent(id={self.id!r}, tenant_id={self.tenant_id!r}, "
            f"event_type={self.event_type!r}, severity={self.severity!r})"
        )


__all__ = [
    "AidpAuditEvent",
    "AuditPayload",
    "Base",
    "SecurityEvent",
]
