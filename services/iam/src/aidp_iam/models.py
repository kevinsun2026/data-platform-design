"""SQLAlchemy 2.0 declarative models for the AIDP IAM service.

This module is the schema source of truth for the IAM service. Every model:

- Combines :class:`aidp_common.models.IdModel` (UUID4 ``id`` PK) and
  :class:`aidp_common.models.TimestampMixin` (audit / soft-delete columns)
  so cross-service bookkeeping is uniform.
- Adds :class:`aidp_common.models.TenantScoped` for everything that lives
  inside a tenant boundary. The ``aidp_db.tenant`` listener auto-injects
  ``WHERE tenant_id = :current_tenant`` on every select against these
  tables.

Tables
------

- :class:`Tenant` — top-level organization. Not tenant-scoped (it *is* the
  tenant); carries plan, isolation, region, status, and free-form
  ``settings_json``.
- :class:`User` — platform user inside a tenant. Holds credentials
  (``password_hash``) and identity contact fields.
- :class:`Group` — many-to-many grouping of users with a self-referential
  parent for nested org structures.
- :class:`UserGroupMember` — junction table for users <-> groups, with
  a per-group ``role_in_group`` (e.g. "owner", "member").
- :class:`Role` — RBAC role definition. ``permissions`` is a JSON list
  of scope strings; ``scope`` is the role's reach ("global" / "tenant").
- :class:`UserRoleBinding` — user <-> role assignment, optionally scoped
  to a specific resource (``scope_type`` + ``scope_id``) with an expiry.
- :class:`ApiKey` — long-lived bearer credential. The raw key is never
  stored; only a salted hash and a public ``key_prefix`` for UI
  identification.
- :class:`Session` — server-side record of an active refresh-token session.
  ``mfa_passed`` lets handlers re-evaluate step-up auth requirements.

Notes on column types
---------------------

- ``permissions``, ``scopes``, and ``settings_json`` are stored as
  :class:`sqlalchemy.JSON`. On PostgreSQL this maps to ``JSONB``; on
  SQLite (test fallback) it maps to ``TEXT``. Both round-trip via
  :func:`json.dumps` / :func:`json.loads`.
- Time columns use ``DateTime(timezone=True)`` for portability with
  Postgres ``TIMESTAMP WITH TIME ZONE``; the application layer always
  produces timezone-aware UTC datetimes (see
  :func:`aidp_common.models.utcnow`).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from aidp_common.models import IdModel, TenantScoped, TimestampMixin, utcnow
from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

# ---------------------------------------------------------------------------
# Declarative base — service-local metadata, per the AIDP convention that each
# service owns its own ``MetaData`` so cross-service imports do not leak.
# ---------------------------------------------------------------------------


class Base(DeclarativeBase):
    """SQLAlchemy declarative base for the IAM service.

    Kept private to the IAM package; Alembic's ``env.py`` and the test
    fixtures import it directly from this module to avoid the cross-service
    metadata coupling that would happen if we re-used a base from
    ``aidp_common``.
    """


# ---------------------------------------------------------------------------
# Tenant
# ---------------------------------------------------------------------------


class Tenant(Base, IdModel, TimestampMixin):
    """Top-level tenant (organization) record.

    The tenant table is the *root* of the L1 isolation tree — it does not
    carry a ``tenant_id`` column because the row itself is the tenant.
    Writes against tenant rows must therefore happen out-of-band (admin
    scripts / platform operators); the L1 listener is a no-op for this
    table because :class:`TenantScoped` is not in its bases.

    Attributes:
        code: Business identifier (``acme``, ``globex`` ...). Unique.
        name: Human-readable display name.
        plan: Subscription tier (``"free"``, ``"team"``, ``"enterprise"``).
        isolation_level: Tenant's physical isolation preference
            (``"l1"`` schema-per-tenant / row-level, ``"l2"`` dedicated DB).
        region: Deployment region (e.g. ``"us-east-1"``).
        status: Lifecycle state (``"active"``, ``"suspended"``, ``"deleted"``).
        settings_json: Free-form JSON for tenant-level feature flags.
    """

    __tablename__ = "tenants"

    code: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    plan: Mapped[str] = mapped_column(String(32), nullable=False, default="free")
    isolation_level: Mapped[str] = mapped_column(String(16), nullable=False, default="l1")
    region: Mapped[str] = mapped_column(String(32), nullable=False, default="us-east-1")
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="active")
    settings_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)

    users: Mapped[list[User]] = relationship(
        "User",
        back_populates="tenant",
        cascade="all, delete-orphan",
    )

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return f"Tenant(id={self.id!r}, code={self.code!r}, plan={self.plan!r})"


# ---------------------------------------------------------------------------
# User
# ---------------------------------------------------------------------------


class User(Base, IdModel, TimestampMixin, TenantScoped):
    """Platform user within a tenant.

    Attributes:
        username: Login name. Unique within a tenant (composite
            ``(tenant_id, username)``).
        email: Contact email. Unique within a tenant.
        phone: Optional E.164 phone for SMS MFA / recovery.
        display_name: User-visible full name.
        avatar_url: Optional CDN URL for the avatar image.
        status: ``"active"`` / ``"locked"`` / ``"disabled"`` / ``"invited"``.
        last_login_at: Timestamp of the last successful login (``NULL``
            when the user has never logged in).
        mfa_enabled: Whether MFA is enrolled. Forces a re-prompt on
            step-up auth.
        password_hash: Argon2id-encoded password (the raw value is never
            persisted).
    """

    __tablename__ = "users"

    # ``tenant_id`` carries an FK to ``tenants.id`` so SQLAlchemy can
    # resolve the ``Tenant.users`` relationship without an explicit
    # ``primaryjoin``. The FK uses ``RESTRICT`` (the default) so a
    # tenant cannot be hard-deleted while users still reference it;
    # soft-delete via ``tenants.deleted_at`` is the intended path.
    # The L1 isolation filter is enforced at the application layer
    # (see :mod:`aidp_db.tenant`), not by this FK.
    tenant_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("tenants.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    username: Mapped[str] = mapped_column(String(64), nullable=False)
    email: Mapped[str] = mapped_column(String(255), nullable=False)
    phone: Mapped[str | None] = mapped_column(String(32), nullable=True)
    display_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    avatar_url: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="active")
    last_login_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    mfa_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    password_hash: Mapped[str] = mapped_column(String(512), nullable=False)

    tenant: Mapped[Tenant] = relationship("Tenant", back_populates="users")
    group_memberships: Mapped[list[UserGroupMember]] = relationship(
        "UserGroupMember",
        back_populates="user",
        cascade="all, delete-orphan",
    )
    role_bindings: Mapped[list[UserRoleBinding]] = relationship(
        "UserRoleBinding",
        back_populates="user",
        cascade="all, delete-orphan",
    )
    api_keys: Mapped[list[ApiKey]] = relationship(
        "ApiKey",
        back_populates="user",
        cascade="all, delete-orphan",
    )
    sessions: Mapped[list[Session]] = relationship(
        "Session",
        back_populates="user",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        UniqueConstraint("tenant_id", "username", name="uq_users_tenant_username"),
        UniqueConstraint("tenant_id", "email", name="uq_users_tenant_email"),
        Index("ix_users_tenant_status", "tenant_id", "status"),
    )

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return f"User(id={self.id!r}, username={self.username!r}, tenant_id={self.tenant_id!r})"


# ---------------------------------------------------------------------------
# Group + UserGroupMember
# ---------------------------------------------------------------------------


class Group(Base, IdModel, TimestampMixin, TenantScoped):
    """Named collection of users within a tenant.

    Groups are self-referential: ``parent_id`` points at the enclosing
    group, enabling nested org structures (Engineering > Data > Analytics).
    ``source`` distinguishes platform-managed groups (``"system"``) from
    user-created ones (``"user"``) and from groups synced via SCIM /
    LDAP (``"scim"`` / ``"ldap"``).
    """

    __tablename__ = "groups"

    # FK to tenants.id — see the analogous comment on User.tenant_id
    # for the rationale (relationship resolution + referential integrity).
    tenant_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("tenants.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    description: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    parent_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey("groups.id", ondelete="SET NULL"),
        nullable=True,
    )
    source: Mapped[str] = mapped_column(String(16), nullable=False, default="user")

    members: Mapped[list[UserGroupMember]] = relationship(
        "UserGroupMember",
        back_populates="group",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        UniqueConstraint("tenant_id", "name", name="uq_groups_tenant_name"),
        Index("ix_groups_tenant_parent", "tenant_id", "parent_id"),
    )

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return f"Group(id={self.id!r}, name={self.name!r}, tenant_id={self.tenant_id!r})"


class UserGroupMember(Base, TimestampMixin):
    """Junction table linking :class:`User` to :class:`Group`.

    A pure many-to-many relation: no ``id`` PK (composite
    ``(user_id, group_id)`` instead) and no separate ``tenant_id``
    column — tenancy is implied by the FK targets, both of which carry
    their own ``tenant_id``.

    Attributes:
        role_in_group: Per-group role (e.g. ``"owner"`` / ``"member"``).
            Distinct from the platform-wide :class:`Role` table; this
            describes the user's relationship *within* the group (group
            admins, members, guests).
    """

    __tablename__ = "user_group_members"

    user_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("users.id", ondelete="CASCADE"),
        primary_key=True,
    )
    group_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("groups.id", ondelete="CASCADE"),
        primary_key=True,
    )
    role_in_group: Mapped[str] = mapped_column(String(32), nullable=False, default="member")

    user: Mapped[User] = relationship("User", back_populates="group_memberships")
    group: Mapped[Group] = relationship("Group", back_populates="members")

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return (
            f"UserGroupMember(user_id={self.user_id!r}, "
            f"group_id={self.group_id!r}, role_in_group={self.role_in_group!r})"
        )


# ---------------------------------------------------------------------------
# Role + UserRoleBinding
# ---------------------------------------------------------------------------


class Role(Base, IdModel, TimestampMixin, TenantScoped):
    """RBAC role definition.

    Attributes:
        code: Stable business key (``"admin"``, ``"data_engineer"`` ...).
            Unique within a tenant.
        name: Human-readable label.
        description: Optional long-form description (shown in the admin
            console).
        scope: Role's reach — ``"global"`` (whole platform) or
            ``"tenant"`` (the role's own tenant only).
        permissions: JSON array of scope strings the role grants
            (``["datasource:read", "datasource:write"]``). The literal
            ``"*"`` denotes a wildcard grant and is honoured by
            :func:`aidp_auth.dependencies._user_has_permission`.
    """

    __tablename__ = "roles"

    tenant_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("tenants.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    code: Mapped[str] = mapped_column(String(64), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    scope: Mapped[str] = mapped_column(String(16), nullable=False, default="tenant")
    permissions: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)

    bindings: Mapped[list[UserRoleBinding]] = relationship(
        "UserRoleBinding",
        back_populates="role",
        cascade="all, delete-orphan",
    )

    __table_args__ = (UniqueConstraint("tenant_id", "code", name="uq_roles_tenant_code"),)

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return f"Role(id={self.id!r}, code={self.code!r}, tenant_id={self.tenant_id!r})"


class UserRoleBinding(Base, IdModel, TimestampMixin, TenantScoped):
    """Assignment of a :class:`Role` to a :class:`User`.

    The binding is itself tenant-scoped (admin tooling must be able to
    see all bindings inside a tenant). Optional resource scoping narrows
    the role's effect to a specific object (e.g. a single datasource).

    Attributes:
        user_id: FK to :class:`User`.
        role_id: FK to :class:`Role`.
        scope_type: Resource-scope kind. ``"global"`` (whole platform),
            ``"tenant"`` (the binding's own tenant), or a resource-type
            string (e.g. ``"datasource"``).
        scope_id: Optional FK to a specific resource of *scope_type*.
            ``NULL`` when ``scope_type`` is ``"global"`` or ``"tenant"``.
        expires_at: Optional expiry. ``NULL`` means the binding is
            permanent (until explicitly revoked).
        granted_by: User id of the granting principal (carried as a
            string to keep the table decoupled from the auth service's
            user-id format).
    """

    __tablename__ = "user_role_bindings"

    tenant_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("tenants.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    user_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )
    role_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("roles.id", ondelete="CASCADE"),
        nullable=False,
    )
    scope_type: Mapped[str] = mapped_column(String(32), nullable=False, default="tenant")
    scope_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    granted_by: Mapped[str | None] = mapped_column(String(64), nullable=True)

    user: Mapped[User] = relationship("User", back_populates="role_bindings")
    role: Mapped[Role] = relationship("Role", back_populates="bindings")

    __table_args__ = (
        # A user gets at most one binding per (role, scope_type, scope_id) tuple.
        # Multiple ``scope_id=NULL`` bindings are still allowed as long as
        # the (role, scope_type) pair differs.
        UniqueConstraint(
            "tenant_id",
            "user_id",
            "role_id",
            "scope_type",
            "scope_id",
            name="uq_user_role_bindings_unique",
        ),
        Index("ix_user_role_bindings_user", "tenant_id", "user_id"),
        Index("ix_user_role_bindings_expires", "expires_at"),
    )

    @property
    def is_expired(self) -> bool:
        """Return ``True`` if the binding has an expiry in the past."""
        if self.expires_at is None:
            return False
        # SQLAlchemy returns timezone-naive datetimes from SQLite even
        # when the column is declared ``DateTime(timezone=True)``.
        # Normalize to UTC-aware so the comparison with :func:`utcnow`
        # (which is always aware) is safe on both backends.
        expiry = self.expires_at
        if expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=UTC)
        return expiry <= utcnow()

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return (
            f"UserRoleBinding(id={self.id!r}, user_id={self.user_id!r}, "
            f"role_id={self.role_id!r}, scope_type={self.scope_type!r})"
        )


# ---------------------------------------------------------------------------
# ApiKey
# ---------------------------------------------------------------------------


class ApiKey(Base, IdModel, TimestampMixin, TenantScoped):
    """Long-lived API credential for programmatic clients.

    The raw key is **never** stored. We persist:

    - ``key_hash``: Argon2id hash of the full key. Constant-time
      comparison against the client-supplied value on every request.
    - ``key_prefix``: The first 8 characters of the raw key. Shown in
      the admin UI so operators can identify which key is which without
      ever seeing the secret.

    Attributes:
        user_id: Owning user. ``CASCADE`` on delete so removing the user
            also revokes their keys.
        name: Human label (``"ci-deploy"``, ``"notebook-prod"``).
        key_hash: Argon2id hash of the raw API key.
        key_prefix: Public prefix; collision-free by construction
            (unique within a tenant).
        scopes: JSON list of scope strings the key carries. Same shape
            as :attr:`Role.permissions`.
        expires_at: Optional expiry. ``NULL`` means the key never
            expires.
        last_used_at: Timestamp of the most recent successful auth.
        revoked_at: Soft-revocation timestamp. ``NULL`` when active.
            A non-null value short-circuits the auth check.
    """

    __tablename__ = "api_keys"

    tenant_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("tenants.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    user_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    key_hash: Mapped[str] = mapped_column(String(512), nullable=False, unique=True)
    key_prefix: Mapped[str] = mapped_column(String(16), nullable=False)
    scopes: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    user: Mapped[User] = relationship("User", back_populates="api_keys")

    __table_args__ = (
        UniqueConstraint("tenant_id", "key_prefix", name="uq_api_keys_tenant_prefix"),
        Index("ix_api_keys_user", "tenant_id", "user_id"),
    )

    @property
    def is_revoked(self) -> bool:
        """Return ``True`` once the key has been revoked."""
        return self.revoked_at is not None

    @property
    def is_expired(self) -> bool:
        """Return ``True`` if the key has an expiry in the past."""
        if self.expires_at is None:
            return False
        # SQLAlchemy returns timezone-naive datetimes from SQLite even
        # when the column is declared ``DateTime(timezone=True)``.
        # Normalize to UTC-aware so the comparison with :func:`utcnow`
        # (which is always aware) is safe on both backends.
        expiry = self.expires_at
        if expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=UTC)
        return expiry <= utcnow()

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return (
            f"ApiKey(id={self.id!r}, name={self.name!r}, "
            f"key_prefix={self.key_prefix!r}, tenant_id={self.tenant_id!r})"
        )


# ---------------------------------------------------------------------------
# Session
# ---------------------------------------------------------------------------


class Session(Base, IdModel, TimestampMixin, TenantScoped):
    """Server-side record of an active refresh-token session.

    The refresh token is sent to the client as a JWT, but the **hash**
    of that token is persisted here so the server can revoke / look up
    the session without re-decoding the JWT on every call.

    Attributes:
        user_id: The session owner.
        refresh_token_hash: Hash of the issued refresh token. Unique
            (one row per issued refresh token).
        user_agent: Optional UA string captured at login (for the
            "active sessions" admin view).
        ip: Optional source IP captured at login.
        expires_at: Server-side expiry. Mirrors the JWT ``exp`` claim
            so a row can be hard-deleted after expiry.
        revoked_at: Soft-revocation timestamp. ``NULL`` when active.
        mfa_passed: ``True`` if the user completed MFA during this
            session's login. Step-up auth handlers re-evaluate this to
            decide whether to demand a fresh MFA challenge.
    """

    __tablename__ = "sessions"

    tenant_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("tenants.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    user_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )
    refresh_token_hash: Mapped[str] = mapped_column(String(512), nullable=False, unique=True)
    user_agent: Mapped[str | None] = mapped_column(String(512), nullable=True)
    ip: Mapped[str | None] = mapped_column(String(64), nullable=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    mfa_passed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    user: Mapped[User] = relationship("User", back_populates="sessions")

    __table_args__ = (
        Index("ix_sessions_user", "tenant_id", "user_id"),
        Index("ix_sessions_expires", "expires_at"),
    )

    @property
    def is_revoked(self) -> bool:
        """Return ``True`` once the session has been revoked."""
        return self.revoked_at is not None

    @property
    def is_expired(self) -> bool:
        """Return ``True`` if the session has passed its server-side expiry."""
        # SQLAlchemy returns timezone-naive datetimes from SQLite even
        # when the column is declared ``DateTime(timezone=True)``.
        # Normalize to UTC-aware so the comparison with :func:`utcnow`
        # (which is always aware) is safe on both backends.
        expiry = self.expires_at
        if expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=UTC)
        return expiry <= utcnow()

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return (
            f"Session(id={self.id!r}, user_id={self.user_id!r}, "
            f"tenant_id={self.tenant_id!r}, mfa_passed={self.mfa_passed!r})"
        )


# ---------------------------------------------------------------------------
# Public re-exports
# ---------------------------------------------------------------------------

__all__ = [
    "ApiKey",
    "Base",
    "Group",
    "Role",
    "Session",
    "Tenant",
    "User",
    "UserGroupMember",
    "UserRoleBinding",
]
