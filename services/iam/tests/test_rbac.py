"""Tests for :mod:`aidp_iam.services.rbac`.

The module is the live re-evaluation of the JWT-snapshot permission
check used by :mod:`aidp_auth.dependencies`. It must:

- read the user's role bindings under L1 tenant isolation,
- union the bound :class:`Role` 's ``permissions`` lists,
- recognise the ``"*"`` wildcard,
- skip expired bindings (so an expired role does not continue to
  grant access),
- return :class:`PermissionDecision` with a ``source`` string
  suitable for audit / observability,
- raise :class:`aidp_common.errors.ForbiddenError` from
  :func:`require_permission_for_user` when the check fails.

The fixtures wire the test in-memory SQLite engine into
:mod:`aidp_db.session` 's engine cache so the service layer's
``get_session()`` returns the same engine the tests query.
"""

from __future__ import annotations

import contextlib
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from functools import partial

import pytest
from aidp_common.config import get_settings
from aidp_common.errors import ForbiddenError
from aidp_db.session import get_session
from aidp_db.tenant import get_tenant_id, reset_tenant_context, set_tenant_context
from aidp_iam.models import Base, Role, User, UserRoleBinding
from aidp_iam.services import rbac
from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.pool import StaticPool

_AT = chr(64)


def email_for(local, domain):
    return f"{local}{_AT}{domain}"


# ---------------------------------------------------------------------------
# DB fixtures (matches test_auth_service.py pattern)
# ---------------------------------------------------------------------------


def _make_engine() -> Engine:
    eng = create_engine(
        "sqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(eng, "connect")
    def _enable_fk(dbapi_conn, _conn_record):
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA foreign_keys=ON")
        cur.close()

    Base.metadata.create_all(eng)
    return eng


@pytest.fixture
def engine() -> Iterator[Engine]:
    eng = _make_engine()
    try:
        yield eng
    finally:
        Base.metadata.drop_all(eng)
        eng.dispose()


@pytest.fixture
def session_factory(engine: Engine):
    return partial(get_session, engine)


@pytest.fixture(autouse=True)
def _wire_test_engine_into_db_cache(engine: Engine):
    import aidp_db.session as db_session

    settings = get_settings()
    prev_url = settings.db_url
    object.__setattr__(settings, "db_url", str(engine.url))
    db_session._engine_cache[str(engine.url)] = engine
    try:
        yield engine
    finally:
        db_session._engine_cache.pop(str(engine.url), None)
        object.__setattr__(settings, "db_url", prev_url)
        with contextlib.suppress(Exception):
            reset_tenant_context(set_tenant_context("placeholder"))


@pytest.fixture(autouse=True)
def _no_tenant_leak():
    yield
    tid = get_tenant_id()
    assert tid is None or tid == "placeholder", f"tenant leaked: {tid!r}"


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------


def _seed_tenant(session_factory, code: str, email: str, password: str = "StrongP@ss123"):
    """Register a tenant + admin and return ``(tenant_id, admin_id)``."""
    from aidp_iam.services.auth_service import register_tenant

    out = register_tenant(
        tenant_code=code,
        tenant_name=code,
        admin_email=email,
        admin_password=password,
        admin_username=code,
        admin_display_name=code.title(),
    )
    return out["tenant_id"], out["user"]["id"]


def _create_non_admin_user(session_factory, *, tenant_id: str, username: str, email: str) -> str:
    """Insert a plain (non-admin) user in *tenant_id* and return its id.

    The bootstrap ``register_tenant`` path always attaches the ``admin``
    role with the ``"*"`` wildcard, so any tests that want to
    observe the "no roles" or "specific role" path need a *non*-admin
    user. This helper is the canonical way to create one in tests.
    """
    from aidp_iam.services.auth_service import hash_password

    with session_factory() as s:
        u = User(
            tenant_id=tenant_id,
            username=username,
            email=email,
            display_name=username,
            status="active",
            mfa_enabled=False,
            password_hash=hash_password("StrongP@ss123"),
        )
        s.add(u)
        s.flush()
        return u.id


def _create_role(session_factory, *, tenant_id: str, code: str, permissions: list[str]) -> str:
    """Create a role with the given permissions; return its id."""
    with session_factory() as s:
        role = Role(
            tenant_id=tenant_id,
            code=code,
            name=code,
            scope="tenant",
            permissions=permissions,
        )
        s.add(role)
        s.flush()
        return role.id


def _create_user(session_factory, *, tenant_id: str, username: str, email: str) -> str:
    """Create a non-admin user; return its id."""
    from aidp_iam.services.auth_service import hash_password

    with session_factory() as s:
        u = User(
            tenant_id=tenant_id,
            username=username,
            email=email,
            display_name=username,
            status="active",
            mfa_enabled=False,
            password_hash=hash_password("StrongP@ss123"),
        )
        s.add(u)
        s.flush()
        return u.id


def _bind(
    session_factory,
    *,
    user_id: str,
    role_id: str,
    tenant_id: str,
    expires_at: datetime | None = None,
) -> str:
    """Bind a user to a role; return the binding id."""
    with session_factory() as s:
        b = UserRoleBinding(
            tenant_id=tenant_id,
            user_id=user_id,
            role_id=role_id,
            scope_type="tenant",
            granted_by=None,
            expires_at=expires_at,
        )
        s.add(b)
        s.flush()
        return b.id


# ---------------------------------------------------------------------------
# collect_user_permissions
# ---------------------------------------------------------------------------


def test_collect_user_permissions_empty_for_user_with_no_bindings(
    session_factory,
) -> None:
    """A user with no role bindings has an empty permission set."""
    tenant_id, _admin_id = _seed_tenant(
        session_factory, "no-roles", email_for("admin", "no-roles.example")
    )
    # A non-admin user is needed because the bootstrap admin already
    # has the wildcard role from ``register_tenant``.
    user_id = _create_non_admin_user(
        session_factory,
        tenant_id=tenant_id,
        username="plain",
        email=email_for("plain", "no-roles.example"),
    )
    perms = rbac.collect_user_permissions(user_id=user_id, tenant_id=tenant_id)
    assert perms == frozenset()


def test_collect_user_permissions_unions_role_permissions(
    session_factory,
) -> None:
    """Multiple bindings union their role.permissions lists."""
    tenant_id, _admin_id = _seed_tenant(
        session_factory, "union", email_for("admin", "union.example")
    )
    user_id = _create_non_admin_user(
        session_factory,
        tenant_id=tenant_id,
        username="multi",
        email=email_for("multi", "union.example"),
    )
    r1 = _create_role(
        session_factory,
        tenant_id=tenant_id,
        code="r1",
        permissions=["datasource:read", "datasource:write"],
    )
    r2 = _create_role(
        session_factory,
        tenant_id=tenant_id,
        code="r2",
        permissions=["datasource:write", "audit:read"],
    )
    _bind(session_factory, user_id=user_id, role_id=r1, tenant_id=tenant_id)
    _bind(session_factory, user_id=user_id, role_id=r2, tenant_id=tenant_id)
    perms = rbac.collect_user_permissions(user_id=user_id, tenant_id=tenant_id)
    assert perms == frozenset({"datasource:read", "datasource:write", "audit:read"})


def test_collect_user_permissions_skips_expired_bindings(session_factory) -> None:
    """Bindings with ``expires_at`` in the past do not grant permissions."""
    tenant_id, _admin_id = _seed_tenant(session_factory, "exp", email_for("admin", "exp.example"))
    user_id = _create_non_admin_user(
        session_factory,
        tenant_id=tenant_id,
        username="expirable",
        email=email_for("expirable", "exp.example"),
    )
    role_id = _create_role(
        session_factory,
        tenant_id=tenant_id,
        code="expired-role",
        permissions=["datasource:read"],
    )
    past = datetime.now(UTC) - timedelta(days=1)
    _bind(
        session_factory,
        user_id=user_id,
        role_id=role_id,
        tenant_id=tenant_id,
        expires_at=past,
    )
    perms = rbac.collect_user_permissions(user_id=user_id, tenant_id=tenant_id)
    assert perms == frozenset()


def test_collect_user_permissions_keeps_active_bindings_alongside_expired(
    session_factory,
) -> None:
    """An active binding still grants when a sibling binding is expired."""
    tenant_id, _admin_id = _seed_tenant(
        session_factory, "mixed", email_for("admin", "mixed.example")
    )
    user_id = _create_non_admin_user(
        session_factory,
        tenant_id=tenant_id,
        username="mix",
        email=email_for("mix", "mixed.example"),
    )
    r_expired = _create_role(
        session_factory,
        tenant_id=tenant_id,
        code="r-expired",
        permissions=["datasource:read"],
    )
    r_active = _create_role(
        session_factory,
        tenant_id=tenant_id,
        code="r-active",
        permissions=["audit:read"],
    )
    past = datetime.now(UTC) - timedelta(days=1)
    future = datetime.now(UTC) + timedelta(days=1)
    _bind(
        session_factory,
        user_id=user_id,
        role_id=r_expired,
        tenant_id=tenant_id,
        expires_at=past,
    )
    _bind(
        session_factory,
        user_id=user_id,
        role_id=r_active,
        tenant_id=tenant_id,
        expires_at=future,
    )
    perms = rbac.collect_user_permissions(user_id=user_id, tenant_id=tenant_id)
    assert perms == frozenset({"audit:read"})


def test_collect_user_permissions_preserves_wildcard(session_factory) -> None:
    """The ``"*"`` wildcard is preserved in the permission set."""
    tenant_id, _admin_id = _seed_tenant(session_factory, "wild", email_for("admin", "wild.example"))
    user_id = _create_non_admin_user(
        session_factory,
        tenant_id=tenant_id,
        username="stargazer",
        email=email_for("stargazer", "wild.example"),
    )
    role_id = _create_role(
        session_factory,
        tenant_id=tenant_id,
        code="super",
        permissions=["*"],
    )
    _bind(session_factory, user_id=user_id, role_id=role_id, tenant_id=tenant_id)
    perms = rbac.collect_user_permissions(user_id=user_id, tenant_id=tenant_id)
    assert perms == frozenset({"*"})


def test_collect_user_permissions_unknown_user_returns_empty(
    session_factory,
) -> None:
    """An unknown user id returns an empty set (no rows match)."""
    # No tenant / user creation — just ask for an unknown id.
    perms = rbac.collect_user_permissions(user_id=str(uuid.uuid4()), tenant_id=str(uuid.uuid4()))
    assert perms == frozenset()


# ---------------------------------------------------------------------------
# has_permission
# ---------------------------------------------------------------------------


def test_has_permission_via_direct_role_grant(session_factory) -> None:
    """A role whose permissions contain the requested string grants it."""
    tenant_id, _admin_id = _seed_tenant(
        session_factory, "direct", email_for("admin", "direct.example")
    )
    user_id = _create_non_admin_user(
        session_factory,
        tenant_id=tenant_id,
        username="reader",
        email=email_for("reader", "direct.example"),
    )
    role_id = _create_role(
        session_factory,
        tenant_id=tenant_id,
        code="reader",
        permissions=["datasource:read"],
    )
    _bind(session_factory, user_id=user_id, role_id=role_id, tenant_id=tenant_id)
    decision = rbac.has_permission(
        user_id=user_id,
        tenant_id=tenant_id,
        permission="datasource:read",
    )
    assert decision.allowed is True
    assert decision.source == "role"
    assert "datasource:read" in decision.permissions


def test_has_permission_returns_false_when_not_granted(session_factory) -> None:
    """A permission not in the user's role set returns ``allowed=False``."""
    tenant_id, _admin_id = _seed_tenant(
        session_factory, "denied", email_for("admin", "denied.example")
    )
    user_id = _create_non_admin_user(
        session_factory,
        tenant_id=tenant_id,
        username="readonly",
        email=email_for("readonly", "denied.example"),
    )
    role_id = _create_role(
        session_factory,
        tenant_id=tenant_id,
        code="reader",
        permissions=["datasource:read"],
    )
    _bind(session_factory, user_id=user_id, role_id=role_id, tenant_id=tenant_id)
    decision = rbac.has_permission(
        user_id=user_id,
        tenant_id=tenant_id,
        permission="datasource:write",
    )
    assert decision.allowed is False
    assert decision.source == "none"
    assert "datasource:read" in decision.permissions


def test_has_permission_via_wildcard(session_factory) -> None:
    """A role with ``"*"`` grants every permission check.

    The test uses a *non-admin* user so the wildcard comes from
    the role under test, not from the bootstrap ``admin`` role.
    """
    tenant_id, _admin_id = _seed_tenant(session_factory, "star", email_for("admin", "star.example"))
    user_id = _create_non_admin_user(
        session_factory,
        tenant_id=tenant_id,
        username="staruser",
        email=email_for("staruser", "star.example"),
    )
    role_id = _create_role(
        session_factory,
        tenant_id=tenant_id,
        code="superuser",
        permissions=["*"],
    )
    _bind(session_factory, user_id=user_id, role_id=role_id, tenant_id=tenant_id)
    decision = rbac.has_permission(
        user_id=user_id,
        tenant_id=tenant_id,
        permission="anything:goes",
    )
    assert decision.allowed is True
    assert decision.source == "wildcard"
    assert "*" in decision.permissions


def test_has_permission_unknown_user_returns_false_with_source_none() -> None:
    """An unknown user id returns ``allowed=False`` and ``source='none'``."""
    decision = rbac.has_permission(
        user_id=str(uuid.uuid4()),
        tenant_id=str(uuid.uuid4()),
        permission="datasource:read",
    )
    assert decision.allowed is False
    assert decision.source == "none"
    assert decision.permissions == frozenset()


def test_has_permission_empty_user_id_returns_false() -> None:
    """Empty inputs return ``allowed=False`` rather than raising."""
    decision = rbac.has_permission(
        user_id="",
        tenant_id="t-1",
        permission="datasource:read",
    )
    assert decision.allowed is False
    assert decision.source == "none"


def test_has_permission_empty_tenant_id_returns_false() -> None:
    """Empty tenant returns ``allowed=False`` rather than raising."""
    decision = rbac.has_permission(
        user_id="u-1",
        tenant_id="",
        permission="datasource:read",
    )
    assert decision.allowed is False
    assert decision.source == "none"


def test_has_permission_rejects_empty_permission_string() -> None:
    """An empty ``permission`` is a misconfiguration and must raise."""
    with pytest.raises(ValueError, match="non-empty"):
        rbac.has_permission(user_id="u-1", tenant_id="t-1", permission="")


# ---------------------------------------------------------------------------
# require_permission_for_user
# ---------------------------------------------------------------------------


def test_require_permission_for_user_returns_decision_when_allowed(
    session_factory,
) -> None:
    """The helper returns the decision on a positive check."""
    tenant_id, _admin_id = _seed_tenant(session_factory, "ok", email_for("admin", "ok.example"))
    user_id = _create_non_admin_user(
        session_factory,
        tenant_id=tenant_id,
        username="helper-ok",
        email=email_for("helper-ok", "ok.example"),
    )
    role_id = _create_role(
        session_factory,
        tenant_id=tenant_id,
        code="ok",
        permissions=["datasource:read"],
    )
    _bind(session_factory, user_id=user_id, role_id=role_id, tenant_id=tenant_id)
    decision = rbac.require_permission_for_user(
        user_id=user_id,
        tenant_id=tenant_id,
        permission="datasource:read",
    )
    assert decision.allowed is True
    assert decision.source == "role"


def test_require_permission_for_user_raises_forbidden_when_denied(
    session_factory,
) -> None:
    """The helper raises :class:`ForbiddenError` on a negative check."""
    tenant_id, _admin_id = _seed_tenant(
        session_factory, "forbid", email_for("admin", "forbid.example")
    )
    user_id = _create_non_admin_user(
        session_factory,
        tenant_id=tenant_id,
        username="helper-forbid",
        email=email_for("helper-forbid", "forbid.example"),
    )
    with pytest.raises(ForbiddenError) as exc_info:
        rbac.require_permission_for_user(
            user_id=user_id,
            tenant_id=tenant_id,
            permission="datasource:write",
        )
    assert "datasource:write" in str(exc_info.value)
    assert exc_info.value.status == 403


# ---------------------------------------------------------------------------
# Tenant isolation (L1) — cross-tenant queries must not see foreign bindings
# ---------------------------------------------------------------------------


def test_collect_user_permissions_isolated_to_tenant(session_factory) -> None:
    """A user's bindings in another tenant must not contribute to their set.

    The L1 ``aidp_db.tenant`` listener enforces ``WHERE tenant_id = :tid``
    on every ORM select. The test seeds two tenants, gives the same
    user id (via copy) bindings in both, and asserts the collector
    returns only the *current* tenant's bindings.

    Note: user ids are UUID4 strings generated by the platform, so
    we cannot use the *same* id in two tenants; instead we assert the
    listener filters by seeding distinct ids and confirming a query
    in tenant A does not see tenant B's bindings.
    """
    # Tenant A
    tenant_a, user_a = _seed_tenant(session_factory, "tena", email_for("admin", "tena.example"))
    role_a = _create_role(
        session_factory,
        tenant_id=tenant_a,
        code="r-a",
        permissions=["datasource:read"],
    )
    _bind(
        session_factory,
        user_id=user_a,
        role_id=role_a,
        tenant_id=tenant_a,
    )
    # Tenant B with its own role + binding for its own admin
    tenant_b, user_b = _seed_tenant(session_factory, "tenb", email_for("admin", "tenb.example"))
    role_b = _create_role(
        session_factory,
        tenant_id=tenant_b,
        code="r-b",
        permissions=["audit:read"],
    )
    _bind(
        session_factory,
        user_id=user_b,
        role_id=role_b,
        tenant_id=tenant_b,
    )

    # User A's permission set should not include tenant B's role.
    perms_a = rbac.collect_user_permissions(user_id=user_a, tenant_id=tenant_a)
    assert "datasource:read" in perms_a
    assert "audit:read" not in perms_a

    # User B's permission set should not include tenant A's role.
    perms_b = rbac.collect_user_permissions(user_id=user_b, tenant_id=tenant_b)
    assert "audit:read" in perms_b
    assert "datasource:read" not in perms_b


# ---------------------------------------------------------------------------
# Sanity check on the seed helper itself
# ---------------------------------------------------------------------------


def test_seed_helper_creates_tenant_with_admin_role(session_factory) -> None:
    """The bootstrap path creates an ``admin`` role with the ``"*"`` wildcard.

    Sanity-checks the test fixture — the ``_seed_tenant`` helper goes
    through :func:`auth_service.register_tenant`, which is the
    production code path.
    """
    tenant_id, user_id = _seed_tenant(session_factory, "boot", email_for("admin", "boot.example"))
    perms = rbac.collect_user_permissions(user_id=user_id, tenant_id=tenant_id)
    assert "*" in perms
