"""initial schema: tenants, users, groups, roles, api_keys, sessions

Revision ID: 0001
Revises:
Create Date: 2026-08-26 00:30:00.000000

This is the canonical first migration for the IAM service. It mirrors
the SQLAlchemy models in :mod:`aidp_iam.models` byte-for-byte across
both PostgreSQL (production) and SQLite (the testcontainers-fallback
target).

Highlights
----------

- Every tenant-scoped table carries a ``tenant_id`` FK to ``tenants.id``
  (``RESTRICT``) so SQLAlchemy can resolve ``Tenant.<children>``
  relationships without an explicit ``primaryjoin``.
- All ``server_default`` values match the Python-level ``default`` on
  the model. The Python-side ``default`` is the source of truth at
  runtime; the ``server_default`` keeps raw-SQL inserts and Alembic
  data backfills consistent.
- The downgrade reverses every operation in dependency order: child
  tables first, then their referenced parents, then ``tenants``.

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
    """Create the IAM schema: tenants, users, groups, roles, api_keys, sessions."""
    # --- tenants -----------------------------------------------------------
    # The root of the L1 isolation tree. NOT tenant-scoped — it IS the tenant.
    op.create_table(
        "tenants",
        sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_by", sa.String(length=64), nullable=True),
        sa.Column("updated_by", sa.String(length=64), nullable=True),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("code", sa.String(length=64), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("plan", sa.String(length=32), nullable=False, server_default="free"),
        sa.Column(
            "isolation_level",
            sa.String(length=16),
            nullable=False,
            server_default="l1",
        ),
        sa.Column(
            "region",
            sa.String(length=32),
            nullable=False,
            server_default="us-east-1",
        ),
        sa.Column(
            "status",
            sa.String(length=16),
            nullable=False,
            server_default="active",
        ),
        sa.Column("settings_json", sa.JSON(), nullable=False),
        sa.UniqueConstraint("code", name="uq_tenants_code"),
    )
    op.create_index(op.f("ix_tenants_code"), "tenants", ["code"], unique=True)

    # --- users -------------------------------------------------------------
    op.create_table(
        "users",
        sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_by", sa.String(length=64), nullable=True),
        sa.Column("updated_by", sa.String(length=64), nullable=True),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("tenant_id", sa.String(length=36), nullable=False),
        sa.Column("username", sa.String(length=64), nullable=False),
        sa.Column("email", sa.String(length=255), nullable=False),
        sa.Column("phone", sa.String(length=32), nullable=True),
        sa.Column("display_name", sa.String(length=255), nullable=True),
        sa.Column("avatar_url", sa.String(length=1024), nullable=True),
        sa.Column(
            "status",
            sa.String(length=16),
            nullable=False,
            server_default="active",
        ),
        sa.Column("last_login_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "mfa_enabled",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("password_hash", sa.String(length=512), nullable=False),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name="fk_users_tenant_id_tenants",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint("tenant_id", "username", name="uq_users_tenant_username"),
        sa.UniqueConstraint("tenant_id", "email", name="uq_users_tenant_email"),
    )
    op.create_index(op.f("ix_users_tenant_id"), "users", ["tenant_id"], unique=False)
    op.create_index("ix_users_tenant_status", "users", ["tenant_id", "status"], unique=False)

    # --- groups ------------------------------------------------------------
    op.create_table(
        "groups",
        sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_by", sa.String(length=64), nullable=True),
        sa.Column("updated_by", sa.String(length=64), nullable=True),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("tenant_id", sa.String(length=36), nullable=False),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("description", sa.String(length=1024), nullable=True),
        sa.Column("parent_id", sa.String(length=36), nullable=True),
        sa.Column(
            "source",
            sa.String(length=16),
            nullable=False,
            server_default="user",
        ),
        sa.ForeignKeyConstraint(
            ["parent_id"],
            ["groups.id"],
            name="fk_groups_parent_id_groups",
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name="fk_groups_tenant_id_tenants",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint("tenant_id", "name", name="uq_groups_tenant_name"),
    )
    op.create_index(op.f("ix_groups_tenant_id"), "groups", ["tenant_id"], unique=False)
    op.create_index("ix_groups_tenant_parent", "groups", ["tenant_id", "parent_id"], unique=False)

    # --- user_group_members ------------------------------------------------
    op.create_table(
        "user_group_members",
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_by", sa.String(length=64), nullable=True),
        sa.Column("updated_by", sa.String(length=64), nullable=True),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("user_id", sa.String(length=36), nullable=False),
        sa.Column("group_id", sa.String(length=36), nullable=False),
        sa.Column(
            "role_in_group",
            sa.String(length=32),
            nullable=False,
            server_default="member",
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            name="fk_user_group_members_user_id_users",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["group_id"],
            ["groups.id"],
            name="fk_user_group_members_group_id_groups",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint(
            "user_id",
            "group_id",
            name="pk_user_group_members",
        ),
    )

    # --- roles -------------------------------------------------------------
    op.create_table(
        "roles",
        sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_by", sa.String(length=64), nullable=True),
        sa.Column("updated_by", sa.String(length=64), nullable=True),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("tenant_id", sa.String(length=36), nullable=False),
        sa.Column("code", sa.String(length=64), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("description", sa.String(length=1024), nullable=True),
        sa.Column(
            "scope",
            sa.String(length=16),
            nullable=False,
            server_default="tenant",
        ),
        sa.Column("permissions", sa.JSON(), nullable=False),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name="fk_roles_tenant_id_tenants",
            ondelete="RESTRICT",
        ),
        sa.UniqueConstraint("tenant_id", "code", name="uq_roles_tenant_code"),
    )
    op.create_index(op.f("ix_roles_tenant_id"), "roles", ["tenant_id"], unique=False)

    # --- user_role_bindings ------------------------------------------------
    op.create_table(
        "user_role_bindings",
        sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_by", sa.String(length=64), nullable=True),
        sa.Column("updated_by", sa.String(length=64), nullable=True),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("tenant_id", sa.String(length=36), nullable=False),
        sa.Column("user_id", sa.String(length=36), nullable=False),
        sa.Column("role_id", sa.String(length=36), nullable=False),
        sa.Column(
            "scope_type",
            sa.String(length=32),
            nullable=False,
            server_default="tenant",
        ),
        sa.Column("scope_id", sa.String(length=64), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("granted_by", sa.String(length=64), nullable=True),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name="fk_user_role_bindings_tenant_id_tenants",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            name="fk_user_role_bindings_user_id_users",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["role_id"],
            ["roles.id"],
            name="fk_user_role_bindings_role_id_roles",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint(
            "tenant_id",
            "user_id",
            "role_id",
            "scope_type",
            "scope_id",
            name="uq_user_role_bindings_unique",
        ),
    )
    op.create_index(
        op.f("ix_user_role_bindings_tenant_id"),
        "user_role_bindings",
        ["tenant_id"],
        unique=False,
    )
    op.create_index(
        "ix_user_role_bindings_user",
        "user_role_bindings",
        ["tenant_id", "user_id"],
        unique=False,
    )
    op.create_index(
        "ix_user_role_bindings_expires",
        "user_role_bindings",
        ["expires_at"],
        unique=False,
    )

    # --- api_keys ----------------------------------------------------------
    op.create_table(
        "api_keys",
        sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_by", sa.String(length=64), nullable=True),
        sa.Column("updated_by", sa.String(length=64), nullable=True),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("tenant_id", sa.String(length=36), nullable=False),
        sa.Column("user_id", sa.String(length=36), nullable=False),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("key_hash", sa.String(length=512), nullable=False),
        sa.Column("key_prefix", sa.String(length=16), nullable=False),
        sa.Column("scopes", sa.JSON(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_used_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name="fk_api_keys_tenant_id_tenants",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            name="fk_api_keys_user_id_users",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint("key_hash", name="uq_api_keys_key_hash"),
        sa.UniqueConstraint("tenant_id", "key_prefix", name="uq_api_keys_tenant_prefix"),
    )
    op.create_index(op.f("ix_api_keys_tenant_id"), "api_keys", ["tenant_id"], unique=False)
    op.create_index("ix_api_keys_user", "api_keys", ["tenant_id", "user_id"], unique=False)

    # --- sessions ----------------------------------------------------------
    op.create_table(
        "sessions",
        sa.Column("id", sa.String(length=36), primary_key=True, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_by", sa.String(length=64), nullable=True),
        sa.Column("updated_by", sa.String(length=64), nullable=True),
        sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("tenant_id", sa.String(length=36), nullable=False),
        sa.Column("user_id", sa.String(length=36), nullable=False),
        sa.Column("refresh_token_hash", sa.String(length=512), nullable=False),
        sa.Column("user_agent", sa.String(length=512), nullable=True),
        sa.Column("ip", sa.String(length=64), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "mfa_passed",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name="fk_sessions_tenant_id_tenants",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            name="fk_sessions_user_id_users",
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint("refresh_token_hash", name="uq_sessions_refresh_token_hash"),
    )
    op.create_index(op.f("ix_sessions_tenant_id"), "sessions", ["tenant_id"], unique=False)
    op.create_index("ix_sessions_user", "sessions", ["tenant_id", "user_id"], unique=False)
    op.create_index("ix_sessions_expires", "sessions", ["expires_at"], unique=False)


# ---------------------------------------------------------------------------
# Downgrade
# ---------------------------------------------------------------------------


def downgrade() -> None:
    """Drop every IAM table in reverse dependency order.

    Order: child tables first (``sessions``, ``api_keys``,
    ``user_role_bindings``, ``user_group_members``), then parents
    (``users``, ``roles``, ``groups``), then the root ``tenants``.
    """
    op.drop_index("ix_sessions_expires", table_name="sessions")
    op.drop_index("ix_sessions_user", table_name="sessions")
    op.drop_index(op.f("ix_sessions_tenant_id"), table_name="sessions")
    op.drop_table("sessions")
    op.drop_index("ix_api_keys_user", table_name="api_keys")
    op.drop_index(op.f("ix_api_keys_tenant_id"), table_name="api_keys")
    op.drop_table("api_keys")
    op.drop_index("ix_user_role_bindings_expires", table_name="user_role_bindings")
    op.drop_index("ix_user_role_bindings_user", table_name="user_role_bindings")
    op.drop_index(op.f("ix_user_role_bindings_tenant_id"), table_name="user_role_bindings")
    op.drop_table("user_role_bindings")
    op.drop_table("user_group_members")
    op.drop_index(op.f("ix_roles_tenant_id"), table_name="roles")
    op.drop_table("roles")
    op.drop_index("ix_groups_tenant_parent", table_name="groups")
    op.drop_index(op.f("ix_groups_tenant_id"), table_name="groups")
    op.drop_table("groups")
    op.drop_index("ix_users_tenant_status", table_name="users")
    op.drop_index(op.f("ix_users_tenant_id"), table_name="users")
    op.drop_table("users")
    op.drop_index(op.f("ix_tenants_code"), table_name="tenants")
    op.drop_table("tenants")
