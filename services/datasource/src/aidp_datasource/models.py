"""SQLAlchemy 2.0 declarative models for the AIDP Datasource service.

This module is the schema source of truth for the datasource service.
Every table participates in the platform's mandatory L1 tenant
isolation:

- :class:`Datasource` — one row per registered external database /
  data-warehouse connection. The ``connection_json`` column holds the
  non-secret connection descriptor (``host`` / ``port`` / ``database``
  / ``options``). Credentials (passwords, private keys, Kerberos
  keytabs) are stored **encrypted** in ``credentials_ciphertext`` —
  the plaintext is never persisted.

- :class:`DatasourceSchema` — a cache of ``information_schema`` /
  catalog introspection results. We keep the latest snapshot per
  datasource so a tenant browsing the catalogue does not have to open
  a live connection on every page load. Refreshed by
  :func:`aidp_datasource.services.datasource_service.refresh_schema`.

- :class:`DatasourcePolicy` — per-datasource access / masking policy.
  For Phase 1 this only carries a JSON blob the platform-level
  governance layer interprets; the column is reserved so the policy
  shape can evolve without a migration per knob.

- :class:`ConnectionTest` — append-only history of test-connection
  attempts. Useful for the operator dashboard ("when did this last
  fail?") and as the audit log entry that backs the
  ``datasource.test.succeeded.v1`` / ``datasource.test.failed.v1``
  Kafka events.

- :class:`DatasourceAudit` — append-only CRUD audit log. Written by
  the service layer on create / update / soft-delete / test /
  enable / disable. Backed by the same ``Kafka producer`` events but
  with the durable copy here for forensic queries.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from aidp_common.models import IdModel, TenantScoped, TimestampMixin
from sqlalchemy import (
    JSON,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# ---------------------------------------------------------------------------
# Declarative base — service-local metadata, per the AIDP convention that
# each service owns its own ``MetaData`` so cross-service imports do not
# leak. Alembic's ``env.py`` and the test fixtures import it directly from
# this module.
# ---------------------------------------------------------------------------


class Base(DeclarativeBase):
    """SQLAlchemy declarative base for the Datasource service."""


# ---------------------------------------------------------------------------
# Datasource
# ---------------------------------------------------------------------------


class Datasource(Base, IdModel, TimestampMixin, TenantScoped):
    """A tenant-registered external connection descriptor.

    Each row binds a logical name to a concrete connection descriptor
    (``connection_json``) and an encrypted credentials blob
    (``credentials_ciphertext`` / ``credentials_nonce`` /
    ``credentials_aad``). The credential encryption is performed by
    :class:`aidp_datasource.services.credential_service.CredentialService`
    using AES-256-GCM; the format matches the audit service's payload
    encryption so the same KMS integration eventually serves both.

    Attributes:
        tenant_id: Tenant the datasource belongs to (L1 isolation key).
        name: Human-readable label. Unique per tenant so a tenant can
            have several datasources of the same kind side-by-side.
        kind: Driver kind. One of ``"postgresql"``, ``"mysql"``,
            ``"oracle"``, ``"hive"``. Stored as a short string for
            portability; the connector layer validates membership.
        env: Deployment environment label (``"dev"`` / ``"staging"`` /
            ``"prod"``). The brief exposes it as a list filter
            (``GET /api/v1/datasources?env=prod``).
        description: Optional free-form description. Rendered in the
            operator dashboard; not surfaced to the agent-gateway.
        connection_json: Non-secret connection descriptor
            (``host`` / ``port`` / ``database`` / ``options``).
        credentials_ciphertext: AES-GCM ciphertext + auth tag (the
            :class:`aidp_audit.crypto.EncryptedPayload.ciphertext`
            field). ``b""`` when the connector does not require
            credentials (e.g. a Kerberos ticket that lives elsewhere).
        credentials_nonce: 12-byte AES-GCM nonce.
        credentials_aad: Additional Authenticated Data — the string
            ``f"{tenant_id}:{datasource_id}:{kind}"``. Stored
            verbatim so the decrypt path can recompute the AAD and
            fail on tenant_id / id / kind tampering.
        credentials_key_version: Key version used to encrypt the
            current row. Reserved for the day a key rotation runs;
            the column is written for every new row so a future
            re-encryption sweep can pick the right key from the
            version.
        tags: Free-form ``list[str]`` for the ``?tag=`` list filter.
        enabled: Soft-disable flag — ``False`` causes ``test()`` /
            ``get_schema()`` to short-circuit with a clear error so
            the operator knows the datasource is administratively
            down (vs. a real connectivity failure).
    """

    __tablename__ = "datasources"

    tenant_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("tenants.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    kind: Mapped[str] = mapped_column(String(16), nullable=False)
    env: Mapped[str] = mapped_column(String(16), nullable=False, default="prod")
    description: Mapped[str] = mapped_column(String(512), nullable=False, default="")
    connection_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    # Encrypted credential payload. ``LargeBinary`` so we do not depend on
    # the codec on the column side; the credential service is responsible
    # for serialising / deserialising the bytes.
    credentials_ciphertext: Mapped[bytes] = mapped_column(LargeBinary, nullable=False, default=b"")
    credentials_nonce: Mapped[bytes] = mapped_column(LargeBinary, nullable=False, default=b"")
    credentials_aad: Mapped[str] = mapped_column(String(256), nullable=False, default="")
    credentials_key_version: Mapped[str] = mapped_column(String(8), nullable=False, default="v1")
    tags: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    enabled: Mapped[bool] = mapped_column(
        Integer, nullable=False, default=1
    )  # SQLite: 0/1; SQLAlchemy maps bool to int on sqlite.

    __table_args__ = (
        UniqueConstraint("tenant_id", "name", name="uq_datasources_tenant_name"),
        UniqueConstraint("tenant_id", "id", name="uq_datasources_tenant_id"),
        Index("ix_datasources_tenant_kind", "tenant_id", "kind"),
        Index("ix_datasources_tenant_env", "tenant_id", "env"),
    )

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return (
            f"Datasource(id={self.id!r}, tenant_id={self.tenant_id!r}, "
            f"name={self.name!r}, kind={self.kind!r}, env={self.env!r})"
        )


# ---------------------------------------------------------------------------
# DatasourceSchema
# ---------------------------------------------------------------------------


class DatasourceSchema(Base, IdModel, TimestampMixin, TenantScoped):
    """Cached schema snapshot for a registered datasource.

    The :class:`aidp_datasource.services.datasource_service.refresh_schema`
    routine replaces (id-by-id) the rows for a given datasource after a
    successful ``get_schema()`` call. The cache is best-effort: a stale
    snapshot is preferable to a 502 because the live database is briefly
    unreachable.

    Attributes:
        tenant_id: Tenant the snapshot belongs to (L1 isolation key).
            Mirrored from the parent :class:`Datasource` so the L1
            listener can filter on the cache directly.
        datasource_id: FK to :class:`Datasource.id`. Cascade-delete
            semantics: when a datasource is soft-deleted, the snapshot
            stays (so a forensic query of the cache can still see
            ``deleted_at``); we clean up on hard-delete (a separate
            admin script — not exposed via the API).
        table_count: Number of tables in the snapshot. Stored
            separately so a list view can show the size without
            inflating every row with the full ``tables_json`` blob.
        tables_json: The snapshot itself — ``list[TableInfo]``
            serialised to JSON. Each ``TableInfo`` carries
            ``{"name": str, "schema": str | None, "columns":
            list[{"name": str, "type": str, "nullable": bool}]}``.
        fingerprint: A SHA-256 hex digest of the canonicalised
            schema (tables, columns, PKs, indexes — **not** row
            counts). A change in ``fingerprint`` is the platform's
            signal that the upstream schema has drifted and any
            downstream catalogue / agent plan needs to be
            re-validated. ``""`` (empty string) for snapshots
            taken before the column was added by the
            ``0002`` migration; such rows are treated as "no
            baseline" by the schema service.
        refreshed_at: When the snapshot was taken. ``NULL`` when the
            row has never been refreshed (e.g. the registry row was
            created by an import rather than a live test).
    """

    __tablename__ = "datasource_schemas"

    tenant_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("tenants.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    datasource_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("datasources.id", ondelete="CASCADE"),
        nullable=False,
    )
    table_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    tables_json: Mapped[list[dict[str, Any]]] = mapped_column(JSON, nullable=False, default=list)
    fingerprint: Mapped[str] = mapped_column(
        String(64), nullable=False, default="", server_default=""
    )
    refreshed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    __table_args__ = (
        Index("ix_datasource_schemas_tenant_ds", "tenant_id", "datasource_id"),
    )

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return (
            f"DatasourceSchema(id={self.id!r}, tenant_id={self.tenant_id!r}, "
            f"datasource_id={self.datasource_id!r}, table_count={self.table_count!r}, "
            f"fingerprint={self.fingerprint[:8]!r})"
        )


# ---------------------------------------------------------------------------
# DatasourcePolicy
# ---------------------------------------------------------------------------


class DatasourcePolicy(Base, IdModel, TimestampMixin, TenantScoped):
    """Per-datasource governance policy.

    The column shape is intentionally open (``JSON``) so the platform
    governance layer can introduce new knobs (row-level masking,
    PII tagging, write-vs-read-only, allowed roles) without a
    migration per change. A ``policies_json`` row is the source of
    truth for what the agent-gateway and the governance engine read.

    Attributes:
        tenant_id: Tenant the policy belongs to (L1 isolation key).
        datasource_id: FK to :class:`Datasource.id``. One policy
            per datasource.
        policies_json: The policy blob. The shape is opaque to the
            datasource service; consumers (``agent-gateway``,
            ``audit``) interpret the keys they care about.
    """

    __tablename__ = "datasource_policies"

    tenant_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("tenants.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    datasource_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("datasources.id", ondelete="CASCADE"),
        nullable=False,
    )
    policies_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)

    __table_args__ = (
        UniqueConstraint(
            "tenant_id", "datasource_id", name="uq_datasource_policies_tenant_ds"
        ),
    )

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return (
            f"DatasourcePolicy(id={self.id!r}, tenant_id={self.tenant_id!r}, "
            f"datasource_id={self.datasource_id!r})"
        )


# ---------------------------------------------------------------------------
# ConnectionTest
# ---------------------------------------------------------------------------


class ConnectionTest(Base, IdModel, TenantScoped):
    """Append-only connection-test history.

    The :class:`aidp_datasource.services.datasource_service.test_connection`
    routine writes one row per test attempt. The row carries the latency
    in milliseconds and a truncated error string (capped so a giant
    driver-side traceback does not blow up the row). The
    ``status`` is one of ``"succeeded"`` / ``"failed"`` / ``"disabled"``.

    ``created_at`` is the timestamp the row was inserted; there is no
    ``updated_at`` because a connection-test row is immutable once
    written (a retried test is a *new* row, not an update).

    Attributes:
        tenant_id: Tenant the test belongs to (L1 isolation key).
        datasource_id: FK to :class:`Datasource.id`. ``NULL`` when
            the test ran against a synthetic descriptor that was
            never persisted (e.g. the ``POST /test`` ad-hoc path);
            we keep the row so an operator can audit "what did this
            user try to connect to".
        kind: The kind (``"postgresql"`` / ...) at test time.
            Denormalised so an operator querying the test history
            can filter by driver without joining.
        status: One of ``"succeeded"`` / ``"failed"`` / ``"disabled"``.
        latency_ms: Wall-clock latency of the probe. ``NULL`` when
            the test failed before the connection was attempted
            (e.g. an authentication error returned a sync error
            from the driver).
        error: Truncated error string (``str(exc)``). ``NULL`` for
            ``"succeeded"`` rows.
    """

    __tablename__ = "connection_tests"

    tenant_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("tenants.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    datasource_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey("datasources.id", ondelete="SET NULL"),
        nullable=True,
    )
    kind: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="failed")
    latency_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error: Mapped[str | None] = mapped_column(String(2048), nullable=True)

    __table_args__ = (
        Index("ix_connection_tests_tenant_ds", "tenant_id", "datasource_id"),
        Index("ix_connection_tests_tenant_status", "tenant_id", "status"),
    )

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return (
            f"ConnectionTest(id={self.id!r}, tenant_id={self.tenant_id!r}, "
            f"datasource_id={self.datasource_id!r}, status={self.status!r})"
        )


# ---------------------------------------------------------------------------
# DatasourceAudit
# ---------------------------------------------------------------------------


class DatasourceAudit(Base, IdModel, TenantScoped):
    """Append-only CRUD audit row.

    The service layer writes one row per administrative action:
    ``created`` / ``updated`` / ``deleted`` (soft) / ``enabled`` /
    ``disabled`` / ``tested``. The row carries the actor's user id
    (from the ``Authorization: Bearer`` JWT) and a JSON diff of the
    fields that changed (so an operator can replay what the
    change was without re-running the full edit history).

    The Kafka events (``datasource.registered.v1`` etc.) are the
    real-time contract; this table is the durable query surface for
    the operator dashboard.

    Attributes:
        tenant_id: Tenant the action belongs to (L1 isolation key).
        datasource_id: FK to :class:`Datasource.id`. ``NULL`` when
            the audit row is for an ad-hoc test (no datasource was
            persisted).
        action: One of ``"created"`` / ``"updated"`` /
            ``"deleted"`` / ``"enabled"`` / ``"disabled"`` /
            ``"tested"``.
        actor: User id of the caller that triggered the action.
        diff_json: JSON object describing the change. For
            ``"created"`` rows the object is the full new descriptor
            minus the encrypted credentials. For ``"updated"`` rows
            the object is ``{"changed": {field: {"old": ..., "new":
            ...}}}``. For ``"tested"`` rows the object is
            ``{"status": "...", "latency_ms": ..., "error": "..."}``.
    """

    __tablename__ = "datasource_audits"

    tenant_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("tenants.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    datasource_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey("datasources.id", ondelete="SET NULL"),
        nullable=True,
    )
    action: Mapped[str] = mapped_column(String(16), nullable=False)
    actor: Mapped[str] = mapped_column(String(64), nullable=False)
    diff_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)

    __table_args__ = (
        Index("ix_datasource_audits_tenant_ds", "tenant_id", "datasource_id"),
        Index("ix_datasource_audits_tenant_action", "tenant_id", "action"),
    )

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return (
            f"DatasourceAudit(id={self.id!r}, tenant_id={self.tenant_id!r}, "
            f"datasource_id={self.datasource_id!r}, action={self.action!r}, "
            f"actor={self.actor!r})"
        )


__all__ = [
    "Base",
    "ConnectionTest",
    "Datasource",
    "DatasourceAudit",
    "DatasourcePolicy",
    "DatasourceSchema",
]
