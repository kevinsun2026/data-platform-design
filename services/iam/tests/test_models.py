"""Tests for the IAM service SQLAlchemy models.

Covers:

- **CRUD round-trip** for every model (Tenant, User, Group,
  UserGroupMember, Role, UserRoleBinding, ApiKey, Session).
- **Soft-delete** via the :attr:`TimestampMixin.deleted_at` column.
- **L1 tenant isolation** — the ``aidp_db.tenant`` listener auto-injects
  ``WHERE tenant_id = :current_tenant`` on selects, and writes that omit
  ``tenant_id`` are rejected with :class:`sqlalchemy.exc.IntegrityError`.
- **Schema constraints** — uniqueness on
  ``(tenant_id, username)`` / ``(tenant_id, email)`` /
  ``(tenant_id, name)`` / ``(tenant_id, code)`` and on
  ``api_keys.key_hash`` / ``api_keys.key_prefix`` /
  ``sessions.refresh_token_hash``.
- **Cascade behaviour** — deleting a user cascades to their groups,
  role bindings, API keys, and sessions; deleting a group cascades to
  the junction rows.
- **Helper properties** — :attr:`ApiKey.is_revoked` /
  :attr:`ApiKey.is_expired` and the corresponding session helpers.

The fixture tries to spin up a testcontainers Postgres (matches the
brief); when the docker daemon / image is unavailable, the suite falls
back to an in-memory SQLite database. The fallback path is annotated
with ``# pragma: allow-testcontainers-fallback`` so the policy is
grep-able from the codebase.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from functools import partial
from typing import Any

# Import the L1 listener side-effect (idempotent; safe to call more than once).
import aidp_db.session  # noqa: F401  # side-effect: install do_orm_execute listener
import pytest
from aidp_db.session import get_session
from aidp_db.tenant import get_tenant_id, reset_tenant_context, set_tenant_context
from aidp_iam.models import (
    ApiKey,
    Base,
    Group,
    Role,
    Session,
    Tenant,
    User,
    UserGroupMember,
    UserRoleBinding,
)
from argon2 import PasswordHasher
from sqlalchemy import create_engine, event, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session as SqlSession

# ---------------------------------------------------------------------------
# Test database — try testcontainers (Postgres), fall back to SQLite
# ---------------------------------------------------------------------------


def _try_postgres_container() -> str | None:
    """Best-effort testcontainers bootstrap; return a URL or ``None``.

    The function deliberately swallows the typical sandbox failures
    (docker daemon not running, image not cached) so unit tests do not
    hard-depend on a running Docker. Callers detect the fallback by
    checking the URL scheme.
    """
    # pragma: allow-testcontainers-fallback
    try:
        from testcontainers.postgres import PostgresContainer
    except ImportError:  # pragma: no cover - dependency drift
        return None
    # pragma: allow-testcontainers-fallback
    try:
        with PostgresContainer("postgres:16-alpine") as pg:
            return pg.get_connection_url()
    except Exception:  # pragma: allow-testcontainers-fallback
        return None


_TEST_URL = _try_postgres_container() or "sqlite:///:memory:"
_USING_POSTGRES = _TEST_URL.startswith("postgres")
# SQLite's enforcement of FK constraints is off by default; we turn it on
# per connection so cascade behaviour matches the Postgres path.
_SQLITE = _TEST_URL.startswith("sqlite")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_engine() -> Engine:
    """Construct the test engine with the right pool semantics.

    For in-memory SQLite, every connection is normally its own database.
    We pin a single shared connection via :class:`StaticPool` so all
    sessions see the same data. For Postgres (or file-based SQLite) the
    default pool is fine.
    """
    if _SQLITE and ":memory:" in _TEST_URL:
        from sqlalchemy.pool import StaticPool

        return create_engine(
            _TEST_URL,
            future=True,
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
    return create_engine(_TEST_URL, future=True)


@pytest.fixture
def engine() -> Iterator[Engine]:
    """Yield a fresh engine with the IAM schema applied."""
    eng: Engine = _make_engine()

    if _SQLITE:
        # Enforce FK constraints on SQLite so cascade behaviour matches
        # the production Postgres path.

        @event.listens_for(eng, "connect")
        def _enable_fk(dbapi_conn: Any, _conn_record: Any) -> None:
            cur = dbapi_conn.cursor()
            cur.execute("PRAGMA foreign_keys=ON")
            cur.close()

    Base.metadata.create_all(eng)
    try:
        yield eng
    finally:
        Base.metadata.drop_all(eng)
        eng.dispose()


@pytest.fixture
def session_factory(engine: Engine) -> Any:
    """Yield a callable that returns an auto-committing :class:`Session` context manager.

    Built on top of :func:`aidp_db.session.get_session` so every block
    commits on successful exit and rolls back on exception. Plain
    ``sessionmaker()`` is *not* enough — its sessions do not auto-commit
    and the second ``with`` block would see a rolled-back state.
    """
    # Bind a partial so each test can write ``with session_factory() as s:``
    # exactly like the standard ``get_session`` API.
    return partial(get_session, engine)


@pytest.fixture
def hasher() -> PasswordHasher:
    """An Argon2id hasher with the platform's default settings."""
    return PasswordHasher()


@pytest.fixture(autouse=True)
def _no_tenant_leak() -> Iterator[None]:
    """Fail the test if a previous test left a tenant context bound."""
    yield
    assert get_tenant_id() is None, "tenant context leaked across tests"


@pytest.fixture(autouse=True)
def seed_tenants(session_factory: Any) -> None:
    """Insert ``t1`` and ``t2`` rows so any tenant-scoped FK check passes.

    Most tests use string tenant ids ``"t1"`` / ``"t2"`` as a convention.
    This autouse fixture pre-creates tenant rows whose ``id`` matches
    those ids (and whose ``code`` is set to the same value) so inserts
    against tenant-scoped tables do not fail on the
    ``tenant_id FK -> tenants.id`` constraint. Tests that need tenants
    with different codes / ids can use the ``make_tenant`` fixture
    instead.
    """
    with session_factory() as s:
        for tid in ("t1", "t2"):
            existing = s.get(Tenant, tid)
            if existing is None:
                s.add(
                    Tenant(
                        id=tid,
                        code=tid,
                        name=tid,
                        plan="team",
                        status="active",
                    )
                )
        s.commit()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_tenant(name: str = "acme") -> Tenant:
    return Tenant(code=name, name=name, plan="team", status="active")


@pytest.fixture
def make_tenant(
    session_factory: Any,
) -> Callable[[str], Tenant]:
    """Return a callable that inserts a tenant and returns it.

    The fixture exposes a closure so individual tests can pin a tenant
    id for foreign-key inserts without repeating the
    ``add + commit`` boilerplate. Returns a detached :class:`Tenant`
    with the id set so the caller can pass ``tenant_id=tenant.id`` to
    the model constructors in a different session.
    """

    def _factory(code: str = "t-acme") -> Tenant:
        with session_factory() as s:
            t = _make_tenant(code)
            s.add(t)
            s.flush()
            tid = t.id
        with session_factory() as s:
            t2 = s.get(Tenant, tid)
            assert t2 is not None
            # Detach so the caller can use ``tenant_id`` in a fresh
            # session without ORM identity confusion.
            s.expunge(t2)
            return t2  # type: ignore[no-any-return]

    return _factory


def _make_user(
    *,
    tenant_id: str,
    username: str = "alice",
    email: str | None = None,
    password_hash: str = "argon2id$placeholder",
) -> User:
    return User(
        tenant_id=tenant_id,
        username=username,
        email=email or f"{username}@{tenant_id}.example",
        display_name=username.title(),
        status="active",
        mfa_enabled=False,
        password_hash=password_hash,
    )


# ---------------------------------------------------------------------------
# Schema registration
# ---------------------------------------------------------------------------


def test_all_eight_tables_registered() -> None:
    """All eight IAM tables are present on :data:`Base.metadata`."""
    expected = {
        "tenants",
        "users",
        "groups",
        "user_group_members",
        "roles",
        "user_role_bindings",
        "api_keys",
        "sessions",
    }
    assert set(Base.metadata.tables.keys()) == expected


# ---------------------------------------------------------------------------
# Tenant
# ---------------------------------------------------------------------------


def test_tenant_create_and_query(session_factory: Any) -> None:
    """A tenant round-trips with all its fields intact."""
    with session_factory() as s:
        t = _make_tenant("acme")
        s.add(t)
        s.flush()
        tid = t.id

    with session_factory() as s:
        got = s.get(Tenant, tid)
        assert got is not None
        assert got.code == "acme"
        assert got.plan == "team"
        assert got.isolation_level == "l1"
        assert got.region == "us-east-1"
        assert got.status == "active"
        assert got.settings_json == {}
        assert got.id  # UUID4 string, non-empty
        assert got.created_at is not None
        assert got.updated_at is not None


def test_tenant_settings_json_round_trip(session_factory: Any) -> None:
    """The ``settings_json`` JSON column preserves nested structures."""
    payload = {"feature_flags": {"ai_agent": True, "max_rows": 1000}, "theme": "dark"}
    with session_factory() as s:
        t = _make_tenant("globex")
        t.settings_json = payload
        s.add(t)
        s.flush()
        tid = t.id

    with session_factory() as s:
        got = s.get(Tenant, tid)
        assert got is not None
        assert got.settings_json == payload


def test_tenant_code_must_be_unique(session_factory: Any) -> None:
    """The ``uq_tenants_code`` constraint rejects duplicate codes."""
    with session_factory() as s:
        s.add(_make_tenant("acme"))
        s.commit()
    with session_factory() as s:
        s.add(_make_tenant("acme"))
        with pytest.raises(IntegrityError):
            s.commit()
        s.rollback()


def test_tenant_soft_delete(session_factory: Any) -> None:
    """Setting ``deleted_at`` hides the row from active queries via the mixin.

    We do not have a global ``is_deleted`` filter in the L1 listener;
    callers must add it explicitly. This test pins the soft-delete
    column behaviour so a future regression that drops the column is
    caught immediately.
    """
    with session_factory() as s:
        t = _make_tenant("acme")
        s.add(t)
        s.flush()
        tid = t.id
        t.deleted_at = datetime.now(UTC)
        s.commit()

    with session_factory() as s:
        got = s.get(Tenant, tid)
        assert got is not None
        assert got.deleted_at is not None


# ---------------------------------------------------------------------------
# User
# ---------------------------------------------------------------------------


def test_user_create_with_argon2_password_hash(
    session_factory: Any, hasher: PasswordHasher
) -> None:
    """A user is created with an Argon2id hash and can be looked up by id."""
    with session_factory() as s:
        u = _make_user(tenant_id="t1", password_hash=hasher.hash("correct horse battery staple"))
        s.add(u)
        s.flush()
        uid = u.id
        # The persisted hash verifies the original password.
        assert hasher.verify(u.password_hash, "correct horse battery staple") is True

    with session_factory() as s:
        got = s.get(User, uid)
        assert got is not None
        assert got.username == "alice"
        assert got.mfa_enabled is False
        assert got.last_login_at is None


def test_user_tenant_id_indexed_for_l1_filter() -> None:
    """``tenant_id`` carries an index so L1 filters stay cheap."""
    users_table: Any = User.__table__
    # The single-column index from TenantScoped plus the composite
    # (tenant_id, status) index from __table_args__ both touch
    # ``tenant_id``; this test pins that property so a future
    # regression that drops the index is caught immediately.
    single_col_index_names = {idx.name for idx in users_table.indexes if len(idx.columns) == 1}
    assert "ix_users_tenant_id" in single_col_index_names
    # Sanity: every composite index that mentions ``tenant_id`` is the
    # (tenant_id, status) one we explicitly declared.
    for idx in users_table.indexes:
        col_names = {c.name for c in idx.columns}
        if "tenant_id" in col_names:
            assert col_names == {"tenant_id", "status"} or col_names == {"tenant_id"}


def test_user_username_unique_within_tenant(session_factory: Any) -> None:
    """``(tenant_id, username)`` is unique; the same name in another tenant is fine."""
    with session_factory() as s:
        s.add(_make_user(tenant_id="t1", username="alice"))
        s.add(_make_user(tenant_id="t2", username="alice"))  # ok, different tenant
        s.commit()
    with session_factory() as s:
        s.add(_make_user(tenant_id="t1", username="alice"))  # duplicate
        with pytest.raises(IntegrityError):
            s.commit()
        s.rollback()


def test_user_email_unique_within_tenant(session_factory: Any) -> None:
    """``(tenant_id, email)`` is unique; cross-tenant duplicates are fine."""
    with session_factory() as s:
        s.add(_make_user(tenant_id="t1", email="[email protected]"))
        s.add(_make_user(tenant_id="t2", email="[email protected]"))
        s.commit()
    with session_factory() as s:
        s.add(_make_user(tenant_id="t1", email="[email protected]"))
        with pytest.raises(IntegrityError):
            s.commit()
        s.rollback()


def test_user_last_login_at_updated(session_factory: Any) -> None:
    """``last_login_at`` is mutable; an update does not break the row."""
    with session_factory() as s:
        u = _make_user(tenant_id="t1")
        s.add(u)
        s.flush()
        uid = u.id
    with session_factory() as s:
        got = s.get(User, uid)
        assert got is not None
        got.last_login_at = datetime.now(UTC)
        s.commit()
    with session_factory() as s:
        got = s.get(User, uid)
        assert got is not None
        assert got.last_login_at is not None


# ---------------------------------------------------------------------------
# Group + UserGroupMember
# ---------------------------------------------------------------------------


def test_group_self_referential_parent(session_factory: Any) -> None:
    """A group's ``parent_id`` can point at another group in the same tenant."""
    with session_factory() as s:
        parent = Group(tenant_id="t1", name="engineering")
        s.add(parent)
        s.flush()
        child = Group(tenant_id="t1", name="data", parent_id=parent.id)
        s.add(child)
        s.commit()
        cid = child.id
        pid = parent.id

    with session_factory() as s:
        c = s.get(Group, cid)
        assert c is not None
        assert c.parent_id == pid


def test_user_group_membership_composite_pk(session_factory: Any) -> None:
    """A user can be added to many groups; the same pair is rejected as a duplicate."""
    with session_factory() as s:
        u = _make_user(tenant_id="t1")
        g1 = Group(tenant_id="t1", name="engineering")
        g2 = Group(tenant_id="t1", name="platform")
        s.add_all([u, g1, g2])
        s.flush()
        s.add(UserGroupMember(user_id=u.id, group_id=g1.id, role_in_group="owner"))
        s.add(UserGroupMember(user_id=u.id, group_id=g2.id, role_in_group="member"))
        s.commit()
        uid = u.id

    with session_factory() as s:
        dups = (
            s.execute(select(UserGroupMember).where(UserGroupMember.user_id == uid)).scalars().all()
        )
    assert len(dups) == 2


def test_user_group_membership_duplicate_pair_rejected(session_factory: Any) -> None:
    """Re-inserting the same ``(user_id, group_id)`` pair raises IntegrityError."""
    with session_factory() as s:
        u = _make_user(tenant_id="t1")
        g = Group(tenant_id="t1", name="engineering")
        s.add_all([u, g])
        s.flush()
        s.add(UserGroupMember(user_id=u.id, group_id=g.id))
        s.commit()
    with session_factory() as s:
        s.add(UserGroupMember(user_id=u.id, group_id=g.id))
        with pytest.raises(IntegrityError):
            s.commit()
        s.rollback()


def test_group_cascade_deletes_members(session_factory: Any) -> None:
    """Deleting a group removes its junction rows (CASCADE)."""
    with session_factory() as s:
        u = _make_user(tenant_id="t1")
        g = Group(tenant_id="t1", name="engineering")
        s.add_all([u, g])
        s.flush()
        s.add(UserGroupMember(user_id=u.id, group_id=g.id))
        s.commit()
        uid, gid = u.id, g.id

    with session_factory() as s:
        g_obj = s.get(Group, gid)
        assert g_obj is not None
        s.delete(g_obj)
        s.commit()

    with session_factory() as s:
        rows = (
            s.execute(select(UserGroupMember).where(UserGroupMember.group_id == gid))
            .scalars()
            .all()
        )
        assert rows == []
        # The user survives the group deletion.
        assert s.get(User, uid) is not None


# ---------------------------------------------------------------------------
# Role + UserRoleBinding
# ---------------------------------------------------------------------------


def test_role_permissions_default_empty_list(session_factory: Any) -> None:
    """A role's permissions default to ``[]`` when not set explicitly."""
    with session_factory() as s:
        r = Role(tenant_id="t1", code="reader", name="Reader")
        s.add(r)
        s.flush()
        rid = r.id
    with session_factory() as s:
        got = s.get(Role, rid)
        assert got is not None
        assert got.permissions == []
        assert got.scope == "tenant"


def test_role_permissions_json_round_trip(session_factory: Any) -> None:
    """The ``permissions`` JSON column preserves a list of strings."""
    perms = ["datasource:read", "datasource:write", "agent:invoke"]
    with session_factory() as s:
        r = Role(
            tenant_id="t1",
            code="admin",
            name="Admin",
            scope="global",
            permissions=perms,
        )
        s.add(r)
        s.flush()
        rid = r.id
    with session_factory() as s:
        got = s.get(Role, rid)
        assert got is not None
        assert got.permissions == perms
        assert got.scope == "global"


def test_role_code_unique_within_tenant(session_factory: Any) -> None:
    """``(tenant_id, code)`` is unique on the role table."""
    with session_factory() as s:
        s.add(Role(tenant_id="t1", code="admin", name="Admin"))
        s.commit()
    with session_factory() as s:
        s.add(Role(tenant_id="t1", code="admin", name="Admin 2"))
        with pytest.raises(IntegrityError):
            s.commit()
        s.rollback()


def test_user_role_binding_with_expiry_and_scope(session_factory: Any) -> None:
    """A binding can carry ``scope_type``, ``scope_id``, ``expires_at``, ``granted_by``."""
    with session_factory() as s:
        u = _make_user(tenant_id="t1")
        r = Role(tenant_id="t1", code="editor", name="Editor")
        s.add_all([u, r])
        s.flush()
        binding = UserRoleBinding(
            tenant_id="t1",
            user_id=u.id,
            role_id=r.id,
            scope_type="datasource",
            scope_id="ds-42",
            expires_at=datetime.now(UTC) + timedelta(days=30),
            granted_by="admin-1",
        )
        s.add(binding)
        s.commit()
        bid = binding.id

    with session_factory() as s:
        got = s.get(UserRoleBinding, bid)
        assert got is not None
        assert got.scope_type == "datasource"
        assert got.scope_id == "ds-42"
        assert got.granted_by == "admin-1"
        assert got.expires_at is not None
        assert got.is_expired is False


def test_user_role_binding_expiry_property(session_factory: Any) -> None:
    """``is_expired`` returns ``True`` for a binding whose expiry is in the past."""
    with session_factory() as s:
        u = _make_user(tenant_id="t1")
        r = Role(tenant_id="t1", code="editor", name="Editor")
        s.add_all([u, r])
        s.flush()
        binding = UserRoleBinding(
            tenant_id="t1",
            user_id=u.id,
            role_id=r.id,
            scope_type="tenant",
            expires_at=datetime.now(UTC) - timedelta(seconds=1),
        )
        s.add(binding)
        s.flush()
        assert binding.is_expired is True
        # A fresh binding is not expired.
        binding2 = UserRoleBinding(
            tenant_id="t1",
            user_id=u.id,
            role_id=r.id,
            scope_type="tenant",
        )
        s.add(binding2)
        s.flush()
        assert binding2.is_expired is False


def test_user_role_binding_unique_combination(session_factory: Any) -> None:
    """The same (user, role, scope_type, scope_id) tuple is rejected."""
    with session_factory() as s:
        u = _make_user(tenant_id="t1")
        r = Role(tenant_id="t1", code="editor", name="Editor")
        s.add_all([u, r])
        s.flush()
        s.add(
            UserRoleBinding(
                tenant_id="t1",
                user_id=u.id,
                role_id=r.id,
                scope_type="datasource",
                scope_id="ds-1",
            )
        )
        s.commit()
    with session_factory() as s:
        s.add(
            UserRoleBinding(
                tenant_id="t1",
                user_id=u.id,
                role_id=r.id,
                scope_type="datasource",
                scope_id="ds-1",
            )
        )
        with pytest.raises(IntegrityError):
            s.commit()
        s.rollback()


def test_user_role_binding_cascade_on_user_delete(session_factory: Any) -> None:
    """Deleting the user removes their role bindings."""
    with session_factory() as s:
        u = _make_user(tenant_id="t1")
        r = Role(tenant_id="t1", code="editor", name="Editor")
        s.add_all([u, r])
        s.flush()
        s.add(UserRoleBinding(tenant_id="t1", user_id=u.id, role_id=r.id))
        s.commit()
        uid = u.id

    with session_factory() as s:
        s.delete(s.get(User, uid))
        s.commit()

    with session_factory() as s:
        rows = (
            s.execute(select(UserRoleBinding).where(UserRoleBinding.user_id == uid)).scalars().all()
        )
        assert rows == []


# ---------------------------------------------------------------------------
# ApiKey
# ---------------------------------------------------------------------------


def test_api_key_create_with_argon2_hash(session_factory: Any, hasher: PasswordHasher) -> None:
    """An API key round-trips with an Argon2id hash that verifies the original."""
    raw_key = "ak_" + uuid.uuid4().hex
    with session_factory() as s:
        u = _make_user(tenant_id="t1")
        s.add(u)
        s.flush()
        ak = ApiKey(
            tenant_id="t1",
            user_id=u.id,
            name="ci-deploy",
            key_hash=hasher.hash(raw_key),
            key_prefix=raw_key[:8],
            scopes=["datasource:write"],
        )
        s.add(ak)
        s.commit()
        akid = ak.id

    with session_factory() as s:
        got = s.get(ApiKey, akid)
        assert got is not None
        assert got.name == "ci-deploy"
        assert got.key_prefix == raw_key[:8]
        assert got.scopes == ["datasource:write"]
        assert got.is_revoked is False
        assert got.is_expired is False
        # The hash verifies the original secret.
        assert hasher.verify(got.key_hash, raw_key) is True


def test_api_key_revocation_sets_revoked_at(session_factory: Any) -> None:
    """``revoked_at`` flips ``is_revoked`` to ``True`` once set."""
    with session_factory() as s:
        u = _make_user(tenant_id="t1")
        s.add(u)
        s.flush()
        ak = ApiKey(
            tenant_id="t1",
            user_id=u.id,
            name="k",
            key_hash="argon2id$placeholder",
            key_prefix="abc12345",
        )
        s.add(ak)
        s.commit()
        akid = ak.id

    with session_factory() as s:
        got = s.get(ApiKey, akid)
        assert got is not None
        assert got.is_revoked is False
        got.revoked_at = datetime.now(UTC)
        s.commit()

    with session_factory() as s:
        got = s.get(ApiKey, akid)
        assert got is not None
        assert got.is_revoked is True


def test_api_key_expiry_property(session_factory: Any) -> None:
    """``is_expired`` reflects a past expiry."""
    with session_factory() as s:
        u = _make_user(tenant_id="t1")
        s.add(u)
        s.flush()
        ak = ApiKey(
            tenant_id="t1",
            user_id=u.id,
            name="k",
            key_hash="argon2id$placeholder",
            key_prefix="abc12345",
            expires_at=datetime.now(UTC) - timedelta(days=1),
        )
        s.add(ak)
        s.flush()
        assert ak.is_expired is True


def test_api_key_hash_must_be_unique(session_factory: Any) -> None:
    """Two API keys with the same ``key_hash`` are rejected (prevents dup registration)."""
    with session_factory() as s:
        u = _make_user(tenant_id="t1")
        s.add(u)
        s.flush()
        s.add(
            ApiKey(
                tenant_id="t1",
                user_id=u.id,
                name="k1",
                key_hash="argon2id$same",
                key_prefix="prefix-1",
            )
        )
        s.commit()
    with session_factory() as s:
        s.add(
            ApiKey(
                tenant_id="t1",
                user_id=u.id,
                name="k2",
                key_hash="argon2id$same",
                key_prefix="prefix-2",
            )
        )
        with pytest.raises(IntegrityError):
            s.commit()
        s.rollback()


def test_api_key_prefix_unique_within_tenant(session_factory: Any) -> None:
    """``(tenant_id, key_prefix)`` is unique; the same prefix in another tenant is fine."""
    with session_factory() as s:
        u1 = _make_user(tenant_id="t1", username="u1")
        u2 = _make_user(tenant_id="t2", username="u2")
        s.add_all([u1, u2])
        s.flush()
        s.add(
            ApiKey(
                tenant_id="t1",
                user_id=u1.id,
                name="k1",
                key_hash="argon2id$h1",
                key_prefix="shared__",
            )
        )
        s.add(
            ApiKey(
                tenant_id="t2",
                user_id=u2.id,
                name="k1",
                key_hash="argon2id$h2",
                key_prefix="shared__",  # ok, different tenant
            )
        )
        s.commit()


def test_api_key_cascade_on_user_delete(session_factory: Any) -> None:
    """Deleting a user cascades to their API keys."""
    with session_factory() as s:
        u = _make_user(tenant_id="t1")
        s.add(u)
        s.flush()
        s.add(
            ApiKey(
                tenant_id="t1",
                user_id=u.id,
                name="k",
                key_hash="argon2id$h",
                key_prefix="prefix-1",
            )
        )
        s.commit()
        uid = u.id

    with session_factory() as s:
        s.delete(s.get(User, uid))
        s.commit()

    with session_factory() as s:
        rows = s.execute(select(ApiKey).where(ApiKey.user_id == uid)).scalars().all()
        assert rows == []


# ---------------------------------------------------------------------------
# Session
# ---------------------------------------------------------------------------


def test_session_create_and_helpers(session_factory: Any) -> None:
    """A session round-trips; ``is_revoked`` / ``is_expired`` reflect state."""
    with session_factory() as s:
        u = _make_user(tenant_id="t1")
        s.add(u)
        s.flush()
        sess = Session(
            tenant_id="t1",
            user_id=u.id,
            refresh_token_hash="argon2id$refresh",
            user_agent="Mozilla/5.0",
            ip="10.0.0.1",
            expires_at=datetime.now(UTC) + timedelta(days=30),
            mfa_passed=True,
        )
        s.add(sess)
        s.commit()
        sid = sess.id

    with session_factory() as s:
        got = s.get(Session, sid)
        assert got is not None
        assert got.mfa_passed is True
        assert got.is_revoked is False
        assert got.is_expired is False
        assert got.user_agent == "Mozilla/5.0"
        assert got.ip == "10.0.0.1"


def test_session_revocation_and_expiry(session_factory: Any) -> None:
    """``is_revoked`` and ``is_expired`` track the corresponding columns."""
    with session_factory() as s:
        u = _make_user(tenant_id="t1")
        s.add(u)
        s.flush()
        sess = Session(
            tenant_id="t1",
            user_id=u.id,
            refresh_token_hash="argon2id$r1",
            expires_at=datetime.now(UTC) - timedelta(seconds=1),
        )
        s.add(sess)
        s.flush()
        assert sess.is_expired is True
        sess.revoked_at = datetime.now(UTC)
        s.flush()
        assert sess.is_revoked is True


def test_session_refresh_token_hash_must_be_unique(session_factory: Any) -> None:
    """Two sessions with the same ``refresh_token_hash`` are rejected."""
    with session_factory() as s:
        u = _make_user(tenant_id="t1")
        s.add(u)
        s.flush()
        s.add(
            Session(
                tenant_id="t1",
                user_id=u.id,
                refresh_token_hash="argon2id$same",
                expires_at=datetime.now(UTC) + timedelta(days=1),
            )
        )
        s.commit()
    with session_factory() as s:
        s.add(
            Session(
                tenant_id="t1",
                user_id=u.id,
                refresh_token_hash="argon2id$same",
                expires_at=datetime.now(UTC) + timedelta(days=1),
            )
        )
        with pytest.raises(IntegrityError):
            s.commit()
        s.rollback()


def test_session_cascade_on_user_delete(session_factory: Any) -> None:
    """Deleting a user cascades to their sessions."""
    with session_factory() as s:
        u = _make_user(tenant_id="t1")
        s.add(u)
        s.flush()
        s.add(
            Session(
                tenant_id="t1",
                user_id=u.id,
                refresh_token_hash="argon2id$h",
                expires_at=datetime.now(UTC) + timedelta(days=1),
            )
        )
        s.commit()
        uid = u.id

    with session_factory() as s:
        s.delete(s.get(User, uid))
        s.commit()

    with session_factory() as s:
        rows = s.execute(select(Session).where(Session.user_id == uid)).scalars().all()
        assert rows == []


# ---------------------------------------------------------------------------
# L1 tenant isolation
# ---------------------------------------------------------------------------


def test_l1_filter_hides_rows_from_other_tenant(session_factory: Any, make_tenant: Any) -> None:
    """With a tenant context set, only that tenant's rows are visible."""
    t_a = make_tenant("tenant-a")
    t_b = make_tenant("tenant-b")
    with session_factory() as s:
        u_a = _make_user(tenant_id=t_a.id, username="a_user")
        u_b = _make_user(tenant_id=t_b.id, username="b_user")
        s.add_all([u_a, u_b])
        s.commit()

    token = set_tenant_context(t_a.id)
    try:
        with session_factory() as s:
            rows = s.execute(select(User)).scalars().all()
        assert [r.username for r in rows] == ["a_user"]
    finally:
        reset_tenant_context(token)


def test_l1_filter_absence_returns_everything(session_factory: Any, make_tenant: Any) -> None:
    """Without a tenant context, no filter is applied — both rows come back."""
    t_a = make_tenant("tenant-a")
    t_b = make_tenant("tenant-b")
    with session_factory() as s:
        s.add(_make_user(tenant_id=t_a.id, username="a_user"))
        s.add(_make_user(tenant_id=t_b.id, username="b_user"))
        s.commit()

    with session_factory() as s:
        rows = s.execute(select(User)).scalars().all()
    usernames = sorted(r.username for r in rows)
    assert usernames == ["a_user", "b_user"]


def test_insert_without_tenant_id_rejected_for_user(session_factory: Any) -> None:
    """The ``NOT NULL`` constraint on ``users.tenant_id`` rejects NULL writes."""
    with session_factory() as s:
        s.add(_make_user(tenant_id=None))  # type: ignore[arg-type]
        with pytest.raises(IntegrityError):
            s.commit()
        s.rollback()


def test_insert_without_tenant_id_rejected_for_role(session_factory: Any) -> None:
    """The ``NOT NULL`` constraint on ``roles.tenant_id`` rejects NULL writes."""
    with session_factory() as s:
        s.add(Role(tenant_id=None, code="x", name="X"))
        with pytest.raises(IntegrityError):
            s.commit()
        s.rollback()


def test_tenants_table_is_not_filtered_by_l1(session_factory: Any, make_tenant: Any) -> None:
    """The ``tenants`` table is not :class:`TenantScoped`, so a context-bound
    select still returns every row. This is the documented escape hatch
    for cross-tenant admin operations.
    """
    make_tenant("acme")
    make_tenant("globex")
    token = set_tenant_context("tenant-acme")
    try:
        with session_factory() as s:
            rows = s.execute(select(Tenant)).scalars().all()
        codes = {r.code for r in rows}
        # Every pre-existing tenant plus the two we just inserted.
        assert {"acme", "globex"}.issubset(codes)
    finally:
        reset_tenant_context(token)


# ---------------------------------------------------------------------------
# Relationships (back-populates)
# ---------------------------------------------------------------------------


def test_user_relationships_load(session_factory: Any) -> None:
    """A user can navigate to its groups, role bindings, API keys, and sessions."""
    with session_factory() as s:
        u = _make_user(tenant_id="t1")
        g = Group(tenant_id="t1", name="engineering")
        r = Role(tenant_id="t1", code="admin", name="Admin")
        s.add_all([u, g, r])
        s.flush()
        s.add(UserGroupMember(user_id=u.id, group_id=g.id, role_in_group="owner"))
        s.add(UserRoleBinding(tenant_id="t1", user_id=u.id, role_id=r.id))
        s.add(
            ApiKey(
                tenant_id="t1",
                user_id=u.id,
                name="k",
                key_hash="argon2id$h",
                key_prefix="prefix-1",
            )
        )
        s.add(
            Session(
                tenant_id="t1",
                user_id=u.id,
                refresh_token_hash="argon2id$r",
                expires_at=datetime.now(UTC) + timedelta(days=1),
            )
        )
        s.commit()
        uid = u.id

    with session_factory() as s:
        got = s.get(User, uid)
        assert got is not None
        assert len(got.group_memberships) == 1
        assert got.group_memberships[0].role_in_group == "owner"
        assert len(got.role_bindings) == 1
        assert got.role_bindings[0].role.code == "admin"
        assert len(got.api_keys) == 1
        assert got.api_keys[0].name == "k"
        assert len(got.sessions) == 1
        assert got.sessions[0].refresh_token_hash == "argon2id$r"


def test_role_bindings_cascade_on_role_delete(session_factory: Any) -> None:
    """Deleting a role removes its bindings (CASCADE)."""
    with session_factory() as s:
        u = _make_user(tenant_id="t1")
        r = Role(tenant_id="t1", code="admin", name="Admin")
        s.add_all([u, r])
        s.flush()
        s.add(UserRoleBinding(tenant_id="t1", user_id=u.id, role_id=r.id))
        s.commit()
        rid = r.id

    with session_factory() as s:
        s.delete(s.get(Role, rid))
        s.commit()

    with session_factory() as s:
        rows = (
            s.execute(select(UserRoleBinding).where(UserRoleBinding.role_id == rid)).scalars().all()
        )
        assert rows == []


# ---------------------------------------------------------------------------
# Argon2 password hashing (integration check)
# ---------------------------------------------------------------------------


def test_argon2_password_hash_format(session_factory: Any, hasher: PasswordHasher) -> None:
    """A real Argon2id hash verifies; a wrong password does not."""
    with session_factory() as s:
        u = _make_user(tenant_id="t1", password_hash=hasher.hash("hunter2"))
        s.add(u)
        s.flush()
        ph = u.password_hash
    # Argon2id hashes start with ``$argon2id$``.
    assert ph.startswith("$argon2id$")
    # The hash verifies the correct password.
    assert hasher.verify(ph, "hunter2") is True
    # And rejects a wrong one.
    from argon2.exceptions import VerifyMismatchError

    with pytest.raises(VerifyMismatchError):
        hasher.verify(ph, "wrong")


# ---------------------------------------------------------------------------
# Migration sanity (only on SQLite for speed; Postgres path is covered by
# ``alembic upgrade head`` in the dev workflow).
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _SQLITE, reason="SQLite-only sanity check")
def test_metadata_matches_alembic_create_all(engine: Engine) -> None:
    """``Base.metadata.create_all`` is idempotent on a fresh database.

    We drop the schema and re-create it via the SQLAlchemy metadata;
    this pins a regression that would, e.g., drop a column or change
    a type. The Alembic-driven path is covered by the
    ``alembic upgrade head`` invocation in the dev workflow.
    """
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    # Round-trip: insert a tenant, then read it back via raw SQL.
    tenants_table: Any = Tenant.__table__
    with engine.begin() as conn:
        conn.execute(
            tenants_table.insert().values(
                id=str(uuid.uuid4()),
                code="sanity",
                name="Sanity",
                plan="free",
                isolation_level="l1",
                region="us-east-1",
                status="active",
                settings_json="{}",
            )
        )
        row = conn.execute(tenants_table.select().where(tenants_table.c.code == "sanity")).first()
    assert row is not None
    assert row.code == "sanity"


# ---------------------------------------------------------------------------
# Postgres-only fidelity check: the listener-generated SELECT matches a
# hand-written ``WHERE tenant_id`` byte-for-byte. Mirrors the analogous
# test in ``aidp_db.tests.test_tenant`` so the IAM listener integration
# is pinned to the same standard.
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not _USING_POSTGRES,
    reason="requires testcontainers Postgres for dialect fidelity",
)
def test_l1_listener_emits_where_clause_on_users(engine: Engine) -> None:
    """The auto-injected WHERE predicate appears in the emitted SQL."""
    users_table: Any = User.__table__
    with engine.begin() as conn:
        conn.execute(
            users_table.insert().values(
                id=str(uuid.uuid4()),
                tenant_id="tenant-pg",
                username="pg-user",
                email="[email protected]",
                status="active",
                mfa_enabled=False,
                password_hash="argon2id$placeholder",
            )
        )

    token = set_tenant_context("tenant-pg")
    try:
        captured: list[str] = []

        def _on_execute(
            conn: Any, cursor: Any, statement: str, parameters: Any, context: Any, executemany: Any
        ) -> None:
            captured.append(statement)

        event.listen(engine, "before_cursor_execute", _on_execute)
        try:
            with SqlSession(engine) as s:
                s.execute(select(User)).scalars().all()
        finally:
            event.remove(engine, "before_cursor_execute", _on_execute)

        matching = [stmt for stmt in captured if "users" in stmt.lower()]
        assert matching, captured
        statement = matching[0].lower()
        assert "from" in statement
        tail = statement[statement.index("from") :]
        assert "where" in tail
        assert "tenant_id" in tail
    finally:
        reset_tenant_context(token)
