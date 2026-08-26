"""initial schema: audit_events, audit_payloads, security_events

Revision ID: 0001
Revises:
Create Date: 2026-08-26 03:00:00.000000

This is the canonical first migration for the Audit service. It mirrors
the SQLAlchemy models in :mod:`aidp_audit.models` byte-for-byte across
both PostgreSQL (production) and SQLite (the testcontainers-fallback
target).

Highlights
----------

- ``tenants`` is owned by the IAM service; we declare a soft
  ``ForeignKey("tenants.id", ondelete="RESTRICT")`` on each
  tenant-scoped table. The migration does **not** create the
  ``tenants`` table — running the audit migration against an
  empty database will fail until the IAM migration is applied
  first. The cross-service ordering is the platform's contract;
  ops applies IAM before audit in the Helm chart.
- Every tenant-scoped table carries a ``tenant_id`` FK to
  ``tenants.id`` (``RESTRICT``) so SQLAlchemy can resolve
  ``Tenant.<children>`` relationships without an explicit
  ``primaryjoin``. (We do not declare ``Tenant`` relationships in
  the audit service because the audit service should not couple to
  the IAM models — it just needs the FK to participate in the L1
  isolation filter.)
- All ``server_default`` values match the Python-level ``default`` on
  the model. The Python-side ``default`` is the source of truth at
  runtime; the ``server_default`` keeps raw-SQL inserts and Alembic
  data backfills consistent.
- The downgrade reverses every operation in dependency order: child
  tables first (``security_events`` → ``audit_payloads``), then
  ``audit_events``.

Note: the file name is fixed by the brief (``0001_initial.py``); the
``file_template`` in ``alembic.ini`` derives filenames from
``revision`` + ``slug``, so the file's identity is the revision id
``0001``.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# ---------------------------------------------------------------------------
# Upgrade
# ---------------------------------------------------------------------------


def upgrade() -> None:
    """Create the audit schema: audit_events + audit_payloads + security_events.

    The ``tenants`` table is **not** created here — it is owned by
    the IAM service. Apply the IAM migration first.
    """
    # --- audit_events ------------------------------------------------------
    # The principal event log. Every audit event consumed from an
    # ``audit.*`` topic lands here. The (tenant_id, event_id)
    # unique constraint is the dedup key for the consumer's
    # at-least-once delivery.
    op.create_table(
        "audit_events",
        sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_by", sa.String(length=64), nullable=True),
        sa.Column("updated_by", sa.String(length=64), nullable=True),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("tenant_id", sa.String(length=36), nullable=False),
        sa.Column("event_id", sa.String(length=64), nullable=False),
        sa.Column("topic", sa.String(length=128), nullable=False),
        sa.Column("producer", sa.String(length=128), nullable=False),
        sa.Column("event_type", sa.String(length=256), nullable=False),
        sa.Column(
            "event_version",
            sa.BigInteger(),
            nullable=False,
            server_default="1",
        ),
        sa.Column("trace_id", sa.String(length=32), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("actor_user_id", sa.String(length=64), nullable=True),
        sa.Column("actor_ip", sa.String(length=64), nullable=True),
        sa.Column("resource_type", sa.String(length=64), nullable=True),
        sa.Column("resource_id", sa.String(length=128), nullable=True),
        sa.Column("action", sa.String(length=64), nullable=False),
        sa.Column(
            "outcome",
            sa.String(length=16),
            nullable=False,
            server_default="success",
        ),
        sa.Column(
            "severity",
            sa.String(length=16),
            nullable=False,
            server_default="info",
        ),
        sa.Column("headers_json", sa.JSON(), nullable=False),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name="fk_audit_events_tenant_id_tenants",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint("tenant_id", "event_id", name="uq_audit_events_tenant_event"),
    )
    op.create_index(op.f("ix_audit_events_tenant_id"), "audit_events", ["tenant_id"])
    op.create_index(
        "ix_audit_events_tenant_action",
        "audit_events",
        ["tenant_id", "action"],
    )
    op.create_index(
        "ix_audit_events_tenant_occurred_at",
        "audit_events",
        ["tenant_id", "occurred_at"],
    )
    op.create_index(
        "ix_audit_events_tenant_user",
        "audit_events",
        ["tenant_id", "actor_user_id"],
    )

    # --- audit_payloads ---------------------------------------------------
    # One-to-one child of ``audit_events`` carrying the AES-256-GCM
    # ciphertext + nonce + AAD. The PK is the parent ``id`` so the
    # two tables are physically linked and an event without a
    # payload cannot exist in the database.
    op.create_table(
        "audit_payloads",
        sa.Column("event_id", sa.String(length=36), primary_key=True, nullable=False),
        sa.Column("ciphertext", sa.LargeBinary(), nullable=False),
        sa.Column("nonce", sa.LargeBinary(), nullable=False),
        sa.Column("aad", sa.String(length=512), nullable=False),
        sa.Column(
            "key_version",
            sa.String(length=32),
            nullable=False,
            server_default="v1",
        ),
        sa.Column(
            "algorithm",
            sa.String(length=32),
            nullable=False,
            server_default="AES-256-GCM",
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["event_id"],
            ["audit_events.id"],
            name="fk_audit_payloads_event_id_audit_events",
            ondelete="CASCADE",
        ),
    )

    # --- security_events --------------------------------------------------
    # High-sensitivity subset of ``audit_events``. The
    # ``(tenant_id, audit_event_id)`` unique constraint prevents the
    # same event from being promoted twice (consumer re-delivery
    # is absorbed by the parent table's unique constraint).
    op.create_table(
        "security_events",
        sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_by", sa.String(length=64), nullable=True),
        sa.Column("updated_by", sa.String(length=64), nullable=True),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("tenant_id", sa.String(length=36), nullable=False),
        sa.Column("audit_event_id", sa.String(length=36), nullable=False),
        sa.Column("event_type", sa.String(length=256), nullable=False),
        sa.Column("action", sa.String(length=64), nullable=False),
        sa.Column(
            "outcome",
            sa.String(length=16),
            nullable=False,
            server_default="failure",
        ),
        sa.Column(
            "severity",
            sa.String(length=16),
            nullable=False,
            server_default="warning",
        ),
        sa.Column("actor_user_id", sa.String(length=64), nullable=True),
        sa.Column("actor_ip", sa.String(length=64), nullable=True),
        sa.Column("resource_type", sa.String(length=64), nullable=True),
        sa.Column("resource_id", sa.String(length=128), nullable=True),
        sa.Column("reason", sa.String(length=512), nullable=True),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("details_json", sa.JSON(), nullable=False),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name="fk_security_events_tenant_id_tenants",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["audit_event_id"],
            ["audit_events.id"],
            name="fk_security_events_audit_event_id_audit_events",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "tenant_id",
            "audit_event_id",
            name="uq_security_events_tenant_event",
        ),
    )
    op.create_index(
        op.f("ix_security_events_tenant_id"),
        "security_events",
        ["tenant_id"],
    )
    op.create_index(
        "ix_security_events_tenant_occurred_at",
        "security_events",
        ["tenant_id", "occurred_at"],
    )
    op.create_index(
        "ix_security_events_tenant_severity",
        "security_events",
        ["tenant_id", "severity"],
    )


# ---------------------------------------------------------------------------
# Downgrade
# ---------------------------------------------------------------------------


def downgrade() -> None:
    """Drop every audit table in reverse dependency order.

    Order: child tables first (``security_events``, ``audit_payloads``),
    then the root ``audit_events``. The cross-service ``tenants``
    table is *not* touched.
    """
    op.drop_index("ix_security_events_tenant_severity", table_name="security_events")
    op.drop_index("ix_security_events_tenant_occurred_at", table_name="security_events")
    op.drop_index(op.f("ix_security_events_tenant_id"), table_name="security_events")
    op.drop_table("security_events")
    op.drop_table("audit_payloads")
    op.drop_index("ix_audit_events_tenant_user", table_name="audit_events")
    op.drop_index("ix_audit_events_tenant_occurred_at", table_name="audit_events")
    op.drop_index("ix_audit_events_tenant_action", table_name="audit_events")
    op.drop_index(op.f("ix_audit_events_tenant_id"), table_name="audit_events")
    op.drop_table("audit_events")
