"""initial schema: datasources, datasource_schemas, datasource_policies, connection_tests, datasource_audits

Revision ID: 0001
Revises:
Create Date: 2026-08-26 06:00:00.000000

This is the canonical first migration for the Datasource service.
It mirrors the SQLAlchemy models in :mod:`aidp_datasource.models`
byte-for-byte across both PostgreSQL (production) and SQLite (the
testcontainers-fallback target).

Highlights
----------

- ``tenants`` is owned by the IAM service; we declare a soft
  ``ForeignKey("tenants.id", ondelete="RESTRICT")`` on every
  tenant-scoped table. The migration does **not** create the
  ``tenants`` table — running the datasource migration against an
  empty database will fail until the IAM migration is applied
  first. The cross-service ordering is the platform's contract;
  ops applies IAM before datasource in the Helm chart.
- Every tenant-scoped table carries a ``tenant_id`` FK to
  ``tenants.id`` (``RESTRICT``) so the L1 listener can resolve
  the per-tenant rows without an explicit ``primaryjoin``. (We do
  not declare ``Tenant`` relationships in the datasource service
  because the datasource service should not couple to the IAM
  models.)
- All ``server_default`` values match the Python-level ``default``
  on the model. The Python-side ``default`` is the source of
  truth at runtime; the ``server_default`` keeps raw-SQL inserts
  and Alembic data backfills consistent.
- The ``enabled`` column on ``datasources`` is stored as
  ``Integer`` (not ``Boolean``) so the same schema works on both
  PostgreSQL and SQLite; the Python model coerces the value to
  ``bool`` at the boundary.
- The credential blob is stored as ``LargeBinary`` (not ``Text``)
  because the AES-GCM ciphertext is binary; the Python model
  coerces to ``bytes`` at the boundary.
- The downgrade reverses every operation in dependency order:
  child tables first (``datasource_audits`` / ``connection_tests``
  reference ``datasources.id``), then ``datasource_policies`` /
  ``datasource_schemas``, then ``datasources``.

Note: the file name is fixed by the brief (``0001_initial.py``);
the ``file_template`` in ``alembic.ini`` derives filenames from
``revision`` + ``slug``, so the file's identity is the revision
id ``0001``.
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
    """Create the datasource schema: datasources + 4 supporting tables.

    The ``tenants`` table is **not** created here — it is owned by
    the IAM service. Apply the IAM migration first.
    """
    # --- datasources ----------------------------------------------------
    # One row per (tenant, name) external connection descriptor.
    # The credential blob is stored as ``LargeBinary`` because the
    # AES-GCM ciphertext is binary; the connection_json carries
    # the non-secret descriptor (host/port/database/options).
    op.create_table(
        "datasources",
        sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_by", sa.String(length=64), nullable=True),
        sa.Column("updated_by", sa.String(length=64), nullable=True),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("tenant_id", sa.String(length=36), nullable=False),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("kind", sa.String(length=16), nullable=False),
        sa.Column("env", sa.String(length=16), nullable=False, server_default="prod"),
        sa.Column("description", sa.String(length=512), nullable=False, server_default=""),
        sa.Column("connection_json", sa.JSON(), nullable=False),
        sa.Column("credentials_ciphertext", sa.LargeBinary(), nullable=False),
        sa.Column("credentials_nonce", sa.LargeBinary(), nullable=False),
        sa.Column("credentials_aad", sa.String(length=256), nullable=False, server_default=""),
        sa.Column(
            "credentials_key_version",
            sa.String(length=8),
            nullable=False,
            server_default="v1",
        ),
        sa.Column("tags", sa.JSON(), nullable=False),
        sa.Column("enabled", sa.Integer(), nullable=False, server_default="1"),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name="fk_datasources_tenant_id_tenants",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint("tenant_id", "name", name="uq_datasources_tenant_name"),
        sa.UniqueConstraint("tenant_id", "id", name="uq_datasources_tenant_id"),
    )
    op.create_index(op.f("ix_datasources_tenant_id"), "datasources", ["tenant_id"])
    op.create_index(
        "ix_datasources_tenant_kind", "datasources", ["tenant_id", "kind"]
    )
    op.create_index(
        "ix_datasources_tenant_env", "datasources", ["tenant_id", "env"]
    )

    # --- datasource_schemas --------------------------------------------
    # Cached ``information_schema`` snapshot. Cascade-delete with
    # the parent datasource so a hard-delete cleans up.
    op.create_table(
        "datasource_schemas",
        sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_by", sa.String(length=64), nullable=True),
        sa.Column("updated_by", sa.String(length=64), nullable=True),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("tenant_id", sa.String(length=36), nullable=False),
        sa.Column("datasource_id", sa.String(length=36), nullable=False),
        sa.Column("table_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("tables_json", sa.JSON(), nullable=False),
        sa.Column("refreshed_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name="fk_datasource_schemas_tenant_id_tenants",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["datasource_id"],
            ["datasources.id"],
            name="fk_datasource_schemas_datasource_id_datasources",
            ondelete="CASCADE",
        ),
    )
    op.create_index(
        op.f("ix_datasource_schemas_tenant_id"), "datasource_schemas", ["tenant_id"]
    )
    op.create_index(
        "ix_datasource_schemas_tenant_ds",
        "datasource_schemas",
        ["tenant_id", "datasource_id"],
    )

    # --- datasource_policies -------------------------------------------
    # Per-datasource governance policy. One row per datasource.
    op.create_table(
        "datasource_policies",
        sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_by", sa.String(length=64), nullable=True),
        sa.Column("updated_by", sa.String(length=64), nullable=True),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("tenant_id", sa.String(length=36), nullable=False),
        sa.Column("datasource_id", sa.String(length=36), nullable=False),
        sa.Column("policies_json", sa.JSON(), nullable=False),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name="fk_datasource_policies_tenant_id_tenants",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["datasource_id"],
            ["datasources.id"],
            name="fk_datasource_policies_datasource_id_datasources",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "tenant_id",
            "datasource_id",
            name="uq_datasource_policies_tenant_ds",
        ),
    )
    op.create_index(
        op.f("ix_datasource_policies_tenant_id"), "datasource_policies", ["tenant_id"]
    )

    # --- connection_tests ----------------------------------------------
    # Append-only connection-test history. ``datasource_id`` is
    # ``ON DELETE SET NULL`` so a deleted datasource does not
    # cascade-delete the historical test log.
    op.create_table(
        "connection_tests",
        sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        # ``updated_at`` / ``updated_by`` / ``created_by`` / ``deleted_at``
        # are NOT included: a connection-test row is immutable once
        # written. The ``TimestampMixin`` columns are still added
        # by SQLAlchemy via the mixin (because we do not pick a
        # different mixin here), so they get the same default
        # treatment in the upgrade.
        sa.Column("created_by", sa.String(length=64), nullable=True),
        sa.Column("updated_by", sa.String(length=64), nullable=True),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("tenant_id", sa.String(length=36), nullable=False),
        sa.Column(
            "datasource_id",
            sa.String(length=36),
            nullable=True,
        ),
        sa.Column("kind", sa.String(length=16), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="failed"),
        sa.Column("latency_ms", sa.Integer(), nullable=True),
        sa.Column("error", sa.String(length=2048), nullable=True),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name="fk_connection_tests_tenant_id_tenants",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["datasource_id"],
            ["datasources.id"],
            name="fk_connection_tests_datasource_id_datasources",
            ondelete="SET NULL",
        ),
    )
    op.create_index(
        op.f("ix_connection_tests_tenant_id"), "connection_tests", ["tenant_id"]
    )
    op.create_index(
        "ix_connection_tests_tenant_ds",
        "connection_tests",
        ["tenant_id", "datasource_id"],
    )
    op.create_index(
        "ix_connection_tests_tenant_status",
        "connection_tests",
        ["tenant_id", "status"],
    )

    # --- datasource_audits ---------------------------------------------
    # Append-only CRUD audit log. ``datasource_id`` is
    # ``ON DELETE SET NULL`` so a deleted datasource does not
    # cascade-delete the historical audit log.
    op.create_table(
        "datasource_audits",
        sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_by", sa.String(length=64), nullable=True),
        sa.Column("updated_by", sa.String(length=64), nullable=True),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("tenant_id", sa.String(length=36), nullable=False),
        sa.Column(
            "datasource_id",
            sa.String(length=36),
            nullable=True,
        ),
        sa.Column("action", sa.String(length=16), nullable=False),
        sa.Column("actor", sa.String(length=64), nullable=False),
        sa.Column("diff_json", sa.JSON(), nullable=False),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name="fk_datasource_audits_tenant_id_tenants",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["datasource_id"],
            ["datasources.id"],
            name="fk_datasource_audits_datasource_id_datasources",
            ondelete="SET NULL",
        ),
    )
    op.create_index(
        op.f("ix_datasource_audits_tenant_id"), "datasource_audits", ["tenant_id"]
    )
    op.create_index(
        "ix_datasource_audits_tenant_ds",
        "datasource_audits",
        ["tenant_id", "datasource_id"],
    )
    op.create_index(
        "ix_datasource_audits_tenant_action",
        "datasource_audits",
        ["tenant_id", "action"],
    )


# ---------------------------------------------------------------------------
# Downgrade
# ---------------------------------------------------------------------------


def downgrade() -> None:
    """Drop every datasource table in reverse dependency order.

    Order: child tables first (``datasource_audits`` /
    ``connection_tests`` reference ``datasources.id``), then
    ``datasource_policies`` / ``datasource_schemas``, then
    ``datasources``. The cross-service ``tenants`` table is
    *not* touched.
    """
    # datasource_audits
    op.drop_index(
        "ix_datasource_audits_tenant_action", table_name="datasource_audits"
    )
    op.drop_index(
        "ix_datasource_audits_tenant_ds", table_name="datasource_audits"
    )
    op.drop_index(
        op.f("ix_datasource_audits_tenant_id"), table_name="datasource_audits"
    )
    op.drop_table("datasource_audits")

    # connection_tests
    op.drop_index(
        "ix_connection_tests_tenant_status", table_name="connection_tests"
    )
    op.drop_index(
        "ix_connection_tests_tenant_ds", table_name="connection_tests"
    )
    op.drop_index(
        op.f("ix_connection_tests_tenant_id"), table_name="connection_tests"
    )
    op.drop_table("connection_tests")

    # datasource_policies
    op.drop_index(
        op.f("ix_datasource_policies_tenant_id"), table_name="datasource_policies"
    )
    op.drop_table("datasource_policies")

    # datasource_schemas
    op.drop_index(
        "ix_datasource_schemas_tenant_ds", table_name="datasource_schemas"
    )
    op.drop_index(
        op.f("ix_datasource_schemas_tenant_id"), table_name="datasource_schemas"
    )
    op.drop_table("datasource_schemas")

    # datasources
    op.drop_index("ix_datasources_tenant_env", table_name="datasources")
    op.drop_index("ix_datasources_tenant_kind", table_name="datasources")
    op.drop_index(op.f("ix_datasources_tenant_id"), table_name="datasources")
    op.drop_table("datasources")
