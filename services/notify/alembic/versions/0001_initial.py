"""initial schema: notification_channels, notification_templates, notification_logs

Revision ID: 0001
Revises:
Create Date: 2026-08-26 03:24:00.000000

This is the canonical first migration for the Notify service. It
mirrors the SQLAlchemy models in :mod:`aidp_notify.models` byte-for-byte
across both PostgreSQL (production) and SQLite (the testcontainers-fallback
target).

Highlights
----------

- ``tenants`` is owned by the IAM service; we declare a soft
  ``ForeignKey("tenants.id", ondelete="RESTRICT")`` on each
  tenant-scoped table. The migration does **not** create the
  ``tenants`` table — running the notify migration against an
  empty database will fail until the IAM migration is applied
  first. The cross-service ordering is the platform's contract;
  ops applies IAM before notify in the Helm chart.
- Every tenant-scoped table carries a ``tenant_id`` FK to
  ``tenants.id`` (``RESTRICT``) so the L1 listener can resolve the
  per-tenant rows without an explicit ``primaryjoin``. (We do not
  declare ``Tenant`` relationships in the notify service because
  the notify service should not couple to the IAM models.)
- All ``server_default`` values match the Python-level ``default`` on
  the model. The Python-side ``default`` is the source of truth at
  runtime; the ``server_default`` keeps raw-SQL inserts and Alembic
  data backfills consistent.
- The ``enabled`` column on ``notification_channels`` is stored as
  ``Integer`` (not ``Boolean``) so the same schema works on both
  PostgreSQL and SQLite; the Python model coerces the value to
  ``bool`` at the boundary.
- The downgrade reverses every operation in dependency order: child
  tables first (``notification_logs``), then ``notification_templates``
  and ``notification_channels``.

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
    """Create the notify schema: channels + templates + logs.

    The ``tenants`` table is **not** created here — it is owned by
    the IAM service. Apply the IAM migration first.
    """
    # --- notification_channels -------------------------------------------
    # One row per (tenant, channel-type) endpoint the tenant has
    # registered. The transport details live in ``config_json`` so
    # an admin can rotate the SMTP password or the webhook secret
    # without touching templates.
    op.create_table(
        "notification_channels",
        sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_by", sa.String(length=64), nullable=True),
        sa.Column("updated_by", sa.String(length=64), nullable=True),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("tenant_id", sa.String(length=36), nullable=False),
        sa.Column("channel", sa.String(length=16), nullable=False),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column(
            "enabled",
            sa.Integer(),
            nullable=False,
            server_default="1",
        ),
        sa.Column("config_json", sa.JSON(), nullable=False),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name="fk_notification_channels_tenant_id_tenants",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint(
            "tenant_id",
            "channel",
            "name",
            name="uq_notification_channels_tenant_name",
        ),
    )
    op.create_index(
        op.f("ix_notification_channels_tenant_id"),
        "notification_channels",
        ["tenant_id"],
    )
    op.create_index(
        "ix_notification_channels_tenant_channel",
        "notification_channels",
        ["tenant_id", "channel"],
    )

    # --- notification_templates ------------------------------------------
    # One row per (tenant, code, locale). The same logical template
    # can have multiple locale variants; the dispatcher picks the
    # best one for the request.
    op.create_table(
        "notification_templates",
        sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_by", sa.String(length=64), nullable=True),
        sa.Column("updated_by", sa.String(length=64), nullable=True),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("tenant_id", sa.String(length=36), nullable=False),
        sa.Column("code", sa.String(length=128), nullable=False),
        sa.Column(
            "locale",
            sa.String(length=16),
            nullable=False,
            server_default="default",
        ),
        sa.Column(
            "subject",
            sa.String(length=512),
            nullable=False,
            server_default="",
        ),
        sa.Column("body", sa.Text(), nullable=False),
        sa.Column(
            "content_type",
            sa.String(length=32),
            nullable=False,
            server_default="text/plain",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name="fk_notification_templates_tenant_id_tenants",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint(
            "tenant_id",
            "code",
            "locale",
            name="uq_notification_templates_tenant_code_locale",
        ),
    )
    op.create_index(
        op.f("ix_notification_templates_tenant_id"),
        "notification_templates",
        ["tenant_id"],
    )
    op.create_index(
        "ix_notification_templates_tenant_code",
        "notification_templates",
        ["tenant_id", "code"],
    )

    # --- notification_logs -----------------------------------------------
    # Append-only per-send log. The ``channel_id`` FK uses
    # ``ondelete=SET NULL`` so a deleted channel row does not
    # cascade-delete the historical log entries.
    op.create_table(
        "notification_logs",
        sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_by", sa.String(length=64), nullable=True),
        sa.Column("updated_by", sa.String(length=64), nullable=True),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("tenant_id", sa.String(length=36), nullable=False),
        sa.Column("template_code", sa.String(length=128), nullable=False),
        sa.Column(
            "locale",
            sa.String(length=16),
            nullable=False,
            server_default="default",
        ),
        sa.Column("channel", sa.String(length=16), nullable=False),
        sa.Column("channel_id", sa.String(length=36), nullable=True),
        sa.Column("recipient", sa.String(length=512), nullable=False),
        sa.Column(
            "subject_rendered",
            sa.String(length=512),
            nullable=False,
            server_default="",
        ),
        sa.Column(
            "body_rendered",
            sa.Text(),
            nullable=False,
            server_default="",
        ),
        sa.Column(
            "status",
            sa.String(length=16),
            nullable=False,
            server_default="queued",
        ),
        sa.Column(
            "attempt",
            sa.Integer(),
            nullable=False,
            server_default="1",
        ),
        sa.Column("response_code", sa.Integer(), nullable=True),
        sa.Column("error", sa.String(length=2048), nullable=True),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name="fk_notification_logs_tenant_id_tenants",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["channel_id"],
            ["notification_channels.id"],
            name="fk_notification_logs_channel_id_notification_channels",
            ondelete="SET NULL",
        ),
    )
    op.create_index(
        op.f("ix_notification_logs_tenant_id"),
        "notification_logs",
        ["tenant_id"],
    )
    op.create_index(
        "ix_notification_logs_tenant_template",
        "notification_logs",
        ["tenant_id", "template_code"],
    )
    op.create_index(
        "ix_notification_logs_tenant_status",
        "notification_logs",
        ["tenant_id", "status"],
    )
    op.create_index(
        "ix_notification_logs_tenant_created_at",
        "notification_logs",
        ["tenant_id", "created_at"],
    )


# ---------------------------------------------------------------------------
# Downgrade
# ---------------------------------------------------------------------------


def downgrade() -> None:
    """Drop every notify table in reverse dependency order.

    Order: child tables first (``notification_logs`` references
    ``notification_channels.id``), then ``notification_templates``,
    then ``notification_channels``. The cross-service ``tenants``
    table is *not* touched.
    """
    op.drop_index("ix_notification_logs_tenant_created_at", table_name="notification_logs")
    op.drop_index("ix_notification_logs_tenant_status", table_name="notification_logs")
    op.drop_index("ix_notification_logs_tenant_template", table_name="notification_logs")
    op.drop_index(op.f("ix_notification_logs_tenant_id"), table_name="notification_logs")
    op.drop_table("notification_logs")
    op.drop_index("ix_notification_templates_tenant_code", table_name="notification_templates")
    op.drop_index(op.f("ix_notification_templates_tenant_id"), table_name="notification_templates")
    op.drop_table("notification_templates")
    op.drop_index(
        "ix_notification_channels_tenant_channel",
        table_name="notification_channels",
    )
    op.drop_index(op.f("ix_notification_channels_tenant_id"), table_name="notification_channels")
    op.drop_table("notification_channels")
