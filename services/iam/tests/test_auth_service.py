"""Tests for the auth service."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from functools import partial

import pytest
from aidp_auth.jwt import TokenType, decode_token
from aidp_common.config import get_settings, reset_settings_cache
from aidp_common.errors import ConflictError, UnauthorizedError
from aidp_db.session import get_session
from aidp_db.tenant import get_tenant_id, reset_tenant_context, set_tenant_context
from aidp_events.inmemory_transport import InMemoryTransport
from aidp_events.producer import set_default_transport
from aidp_iam.models import Base, Tenant, User, UserRoleBinding
from aidp_iam.models import Session as SessionModel
from aidp_iam.services import auth_service
from sqlalchemy import create_engine, event, select
from sqlalchemy.pool import StaticPool

_AT = chr(64)


def email_for(local, domain):
    return f"{local}{_AT}{domain}"


def _make_engine():
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
def engine():
    eng = _make_engine()
    try:
        yield eng
    finally:
        Base.metadata.drop_all(eng)
        eng.dispose()


@pytest.fixture
def session_factory(engine):
    return partial(get_session, engine)


@pytest.fixture(autouse=True)
def _wire_test_engine_into_db_cache(engine):
    import contextlib

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


@pytest.fixture
def in_memory_transport():
    transport = InMemoryTransport(auto_offset_reset="earliest")
    set_default_transport(transport)
    try:
        yield transport
    finally:
        set_default_transport(None)


def test_hash_password_round_trip():
    plain = "correct horse battery staple"
    hashed = auth_service.hash_password(plain)
    assert isinstance(hashed, str)
    assert hashed.startswith("$argon2id$")
    assert auth_service.verify_password(hashed, plain) is True


def test_verify_password_rejects_wrong_password():
    hashed = auth_service.hash_password("right")
    assert auth_service.verify_password(hashed, "wrong") is False


def test_verify_password_rejects_empty_inputs():
    hashed = auth_service.hash_password("something")
    assert auth_service.verify_password("", "something") is False
    assert auth_service.verify_password(hashed, "") is False
    assert auth_service.verify_password("", "") is False


def test_verify_password_handles_malformed_hash():
    assert auth_service.verify_password("not-a-real-hash", "anything") is False
    assert auth_service.verify_password("$argon2id$broken", "anything") is False


def test_hash_password_rejects_empty():
    with pytest.raises(ValueError, match="non-empty"):
        auth_service.hash_password("")


def test_two_hashes_of_same_password_differ():
    plain = "same-password"
    h1 = auth_service.hash_password(plain)
    h2 = auth_service.hash_password(plain)
    assert h1 != h2
    assert auth_service.verify_password(h1, plain) is True
    assert auth_service.verify_password(h2, plain) is True


def test_hash_refresh_token_round_trip():
    token = "eyJhbGciOiJIUzI1NiJ9.payload.sig"
    hashed = auth_service.hash_refresh_token(token)
    assert auth_service.verify_refresh_token(hashed, token) is True
    assert auth_service.verify_refresh_token(hashed, "wrong-token") is False


def test_hash_refresh_token_rejects_empty():
    with pytest.raises(ValueError, match="non-empty"):
        auth_service.hash_refresh_token("")


def test_register_tenant_creates_tenant_user_role_and_token(session_factory):
    out = auth_service.register_tenant(
        tenant_code="acme",
        tenant_name="Acme",
        admin_email=email_for("acme-admin", "acme.example"),
        admin_password="StrongP@ss123",
        admin_username="acme-admin",
        admin_display_name="Acme Admin",
        tenant_plan="team",
        tenant_region="us-east-1",
    )
    assert out["tenant_code"] == "acme"
    assert out["token"]["token_type"] == "Bearer"
    assert "access_token" in out["token"]
    assert "refresh_token" in out["token"]
    assert out["user"]["email"] == email_for("acme-admin", "acme.example")
    assert out["user"]["roles"] == ["admin"]
    assert "*" in out["user"]["scopes"]

    tenant_id = out["tenant_id"]
    with session_factory() as s:
        tenants = s.execute(select(Tenant).where(Tenant.id == tenant_id)).scalars().all()
        assert len(tenants) == 1
        users = s.execute(select(User).where(User.tenant_id == tenant_id)).scalars().all()
        assert len(users) == 1
        bindings = (
            s.execute(select(UserRoleBinding).where(UserRoleBinding.tenant_id == tenant_id))
            .scalars()
            .all()
        )
        assert len(bindings) == 1
        sessions = (
            s.execute(select(SessionModel).where(SessionModel.tenant_id == tenant_id))
            .scalars()
            .all()
        )
        assert len(sessions) == 1
        assert (
            auth_service.verify_refresh_token(
                sessions[0].refresh_token_hash, out["token"]["refresh_token"]
            )
            is True
        )


def test_register_tenant_default_username_is_email_local_part():
    out = auth_service.register_tenant(
        tenant_code="globex",
        tenant_name="Globex",
        admin_email=email_for("ops", "globex.example"),
        admin_password="StrongP@ss123",
        admin_username=None,
        admin_display_name=None,
    )
    assert out["user"]["username"] == "ops"


def test_register_tenant_normalizes_email():
    raw = "  " + "ADMIN" + _AT + "Acme.Example  "
    out = auth_service.register_tenant(
        tenant_code="acme2",
        tenant_name="Acme 2",
        admin_email=raw,
        admin_password="StrongP@ss123",
        admin_username=None,
        admin_display_name=None,
    )
    assert out["user"]["email"] == email_for("admin", "acme.example")


def test_register_tenant_rejects_duplicate_code():
    auth_service.register_tenant(
        tenant_code="dup",
        tenant_name="First",
        admin_email=email_for("first", "dup.example"),
        admin_password="StrongP@ss123",
        admin_username=None,
        admin_display_name=None,
    )
    with pytest.raises(ConflictError):
        auth_service.register_tenant(
            tenant_code="dup",
            tenant_name="Second",
            admin_email=email_for("second", "dup.example"),
            admin_password="StrongP@ss123",
            admin_username=None,
            admin_display_name=None,
        )


def test_register_tenant_issues_a_working_access_token():
    out = auth_service.register_tenant(
        tenant_code="dec",
        tenant_name="Dec",
        admin_email=email_for("admin", "dec.example"),
        admin_password="StrongP@ss123",
        admin_username=None,
        admin_display_name=None,
    )
    claims = decode_token(out["token"]["access_token"])
    assert claims.token_type is TokenType.ACCESS
    assert claims.tenant_id == out["tenant_id"]
    assert claims.user_id == out["user"]["id"]
    assert "admin" in claims.roles
    assert "*" in claims.scopes


def _seed_tenant_with_user(
    session_factory,
    *,
    code,
    email,
    password="correct horse battery staple",
    username="alice",
    status="active",
):
    out = auth_service.register_tenant(
        tenant_code=code,
        tenant_name=code,
        admin_email=email,
        admin_password=password,
        admin_username=username,
        admin_display_name=username.title(),
    )
    if status != "active":
        with session_factory() as s:
            u = s.get(User, out["user"]["id"])
            assert u is not None
            u.status = status
    return out["tenant_id"], out["user"]["id"]


def test_authenticate_happy_path(session_factory):
    email_value = email_for("alice", "happy.example")
    tenant_id, user_id = _seed_tenant_with_user(
        session_factory,
        code="happy",
        email=email_value,
        password="StrongP@ss123",
        username="alice",
    )
    authed = auth_service.authenticate(
        email=email_value,
        password="StrongP@ss123",
    )
    assert authed.user.id == user_id
    assert authed.user.tenant_id == tenant_id
    assert authed.tenant_code == "happy"
    assert "admin" in authed.roles


def test_authenticate_updates_last_login_at(session_factory):
    email_value = email_for("login", "loginctx.example")
    _seed_tenant_with_user(
        session_factory,
        code="loginctx",
        email=email_value,
        password="StrongP@ss123",
        username="login",
    )
    authed = auth_service.authenticate(
        email=email_value,
        password="StrongP@ss123",
    )
    assert authed.user.last_login_at is not None
    with session_factory() as s:
        u = s.get(User, authed.user.id)
        assert u is not None
        assert u.last_login_at is not None
        after = u.last_login_at
        if after.tzinfo is None:
            after = after.replace(tzinfo=UTC)
        assert after == authed.user.last_login_at


def test_authenticate_wrong_password_raises_unauthorized(session_factory):
    email_value = email_for("wp", "wrongpw.example")
    _seed_tenant_with_user(
        session_factory,
        code="wrongpw",
        email=email_value,
        password="StrongP@ss123",
        username="wp",
    )
    with pytest.raises(UnauthorizedError):
        auth_service.authenticate(
            email=email_value,
            password="not-the-password",
        )


def test_authenticate_unknown_email_raises_unauthorized():
    with pytest.raises(UnauthorizedError):
        auth_service.authenticate(
            email=email_for("nobody", "nowhere.example"),
            password="doesnt-matter",
        )


def test_authenticate_disabled_user_raises_unauthorized(session_factory):
    email_value = email_for("di", "disabled.example")
    _seed_tenant_with_user(
        session_factory,
        code="disabled",
        email=email_value,
        password="StrongP@ss123",
        username="di",
        status="disabled",
    )
    with pytest.raises(UnauthorizedError):
        auth_service.authenticate(
            email=email_value,
            password="StrongP@ss123",
        )


def test_authenticate_tenant_hint_disambiguates(session_factory):
    shared_email = email_for("shared", "shared.example")
    _seed_tenant_with_user(
        session_factory,
        code="tenant-a",
        email=shared_email,
        password="StrongP@ss123",
        username="a",
    )
    _seed_tenant_with_user(
        session_factory,
        code="tenant-b",
        email=shared_email,
        password="AnotherStr0ng!",
        username="b",
    )
    # Same email in both tenants - no hint -> ambiguous -> 401.
    with pytest.raises(UnauthorizedError):
        auth_service.authenticate(email=shared_email, password="StrongP@ss123")
    authed_a = auth_service.authenticate(
        email=shared_email,
        password="StrongP@ss123",
        tenant_code="tenant-a",
    )
    assert authed_a.tenant_code == "tenant-a"
    authed_b = auth_service.authenticate(
        email=shared_email,
        password="AnotherStr0ng!",
        tenant_code="tenant-b",
    )
    assert authed_b.tenant_code == "tenant-b"


def test_authenticate_email_is_case_insensitive(session_factory):
    email_value = email_for("c", "case.example")
    _seed_tenant_with_user(
        session_factory,
        code="case",
        email=email_value,
        password="StrongP@ss123",
        username="c",
    )
    authed = auth_service.authenticate(
        email="C" + _AT + "CASE.example",
        password="StrongP@ss123",
    )
    assert authed.user.email == email_value


def test_issue_token_pair_persists_a_session(session_factory):
    email_value = email_for("iss", "iss.example")
    _seed_tenant_with_user(
        session_factory,
        code="iss",
        email=email_value,
        password="StrongP@ss123",
        username="iss",
    )
    authed = auth_service.authenticate(
        email=email_value,
        password="StrongP@ss123",
    )
    pair = auth_service.issue_token_pair(authed=authed)
    claims = decode_token(pair.access_token)
    assert claims.user_id == authed.user.id
    assert claims.tenant_id == authed.user.tenant_id
    with session_factory() as s:
        sess = s.get(SessionModel, pair.session.id)
        assert sess is not None
        assert sess.user_id == authed.user.id
        sess_exp = sess.expires_at
        if sess_exp.tzinfo is None:
            sess_exp = sess_exp.replace(tzinfo=UTC)
        assert sess_exp > datetime.now(UTC)


def test_refresh_tokens_rotates_session(session_factory):
    email_value = email_for("rot", "rot.example")
    _seed_tenant_with_user(
        session_factory,
        code="rot",
        email=email_value,
        password="StrongP@ss123",
        username="rot",
    )
    authed = auth_service.authenticate(
        email=email_value,
        password="StrongP@ss123",
    )
    pair = auth_service.issue_token_pair(authed=authed)
    old_sid = pair.session.id
    _new_authed, new_pair = auth_service.refresh_tokens(refresh_token=pair.refresh_token)
    claims = decode_token(new_pair.access_token)
    assert claims.user_id == authed.user.id
    with session_factory() as s:
        old = s.get(SessionModel, old_sid)
        assert old is not None
        assert old.revoked_at is not None
        new = s.get(SessionModel, new_pair.session.id)
        assert new is not None
        assert new.revoked_at is None
        assert new.id != old_sid


def test_refresh_tokens_rejects_replay_of_revoked_token(session_factory):
    email_value = email_for("rep", "replay.example")
    _seed_tenant_with_user(
        session_factory,
        code="replay",
        email=email_value,
        password="StrongP@ss123",
        username="rep",
    )
    authed = auth_service.authenticate(
        email=email_value,
        password="StrongP@ss123",
    )
    pair = auth_service.issue_token_pair(authed=authed)
    auth_service.refresh_tokens(refresh_token=pair.refresh_token)
    with pytest.raises(UnauthorizedError):
        auth_service.refresh_tokens(refresh_token=pair.refresh_token)


def test_refresh_tokens_rejects_unknown_token():
    with pytest.raises(UnauthorizedError):
        auth_service.refresh_tokens(refresh_token="not-a-jwt")


def test_refresh_tokens_rejects_access_token(session_factory):
    email_value = email_for("acc", "accrej.example")
    _seed_tenant_with_user(
        session_factory,
        code="accrej",
        email=email_value,
        password="StrongP@ss123",
        username="acc",
    )
    authed = auth_service.authenticate(
        email=email_value,
        password="StrongP@ss123",
    )
    pair = auth_service.issue_token_pair(authed=authed)
    with pytest.raises(UnauthorizedError):
        auth_service.refresh_tokens(refresh_token=pair.access_token)


def test_revoke_session_marks_session_revoked(session_factory):
    email_value = email_for("rev", "rev.example")
    _seed_tenant_with_user(
        session_factory,
        code="rev",
        email=email_value,
        password="StrongP@ss123",
        username="rev",
    )
    authed = auth_service.authenticate(
        email=email_value,
        password="StrongP@ss123",
    )
    pair = auth_service.issue_token_pair(authed=authed)
    assert auth_service.revoke_session(refresh_token=pair.refresh_token) is True
    assert auth_service.revoke_session(refresh_token=pair.refresh_token) is False


def test_revoke_session_handles_unknown_token():
    assert auth_service.revoke_session(refresh_token="garbage") is False
    assert auth_service.revoke_session(refresh_token="") is False


def test_revoke_all_sessions_for_user(session_factory):
    email_value = email_for("all", "all.example")
    _seed_tenant_with_user(
        session_factory,
        code="all",
        email=email_value,
        password="StrongP@ss123",
        username="all",
    )
    authed = auth_service.authenticate(
        email=email_value,
        password="StrongP@ss123",
    )
    # register_tenant already issued one session; issue 2 more to make 3 total.
    for _ in range(2):
        auth_service.issue_token_pair(authed=authed)
    revoked = auth_service.revoke_all_sessions_for_user(user_id=authed.user.id)
    assert revoked == 3
    with session_factory() as s:
        remaining = (
            s.execute(
                select(SessionModel)
                .where(SessionModel.user_id == authed.user.id)
                .where(SessionModel.revoked_at.is_(None))
            )
            .scalars()
            .all()
        )
        assert remaining == []


@pytest.mark.asyncio
async def test_publish_audit_event_emits_to_in_memory_transport(in_memory_transport):
    await auth_service.publish_audit_event(
        event_type=auth_service._AUDIT_EVENT_TYPE_LOGIN,
        tenant_id="t-1",
        payload={"user_id": "u-1"},
    )
    records = await in_memory_transport.drain(auth_service._AUDIT_TOPIC)
    assert len(records) == 1
    assert b"iam.user.logged_in" in records[0].value


@pytest.mark.asyncio
async def test_publish_audit_event_swallows_transport_errors():
    class _BrokenTransport:
        async def send(self, *args, **kwargs):
            raise RuntimeError("simulated kafka outage")

    await auth_service.publish_audit_event(
        event_type="iam.user.logged_in",
        tenant_id="t-1",
        payload={"user_id": "u-1"},
        transport=_BrokenTransport(),
    )


def test_get_user_by_id_returns_none_for_unknown():
    assert auth_service.get_user_by_id(user_id=str(uuid.uuid4())) is None


def test_get_user_by_id_returns_user_info(session_factory):
    email_value = email_for("lku", "lku.example")
    _, user_id = _seed_tenant_with_user(
        session_factory,
        code="lku",
        email=email_value,
        password="StrongP@ss123",
        username="lku",
    )
    info = auth_service.get_user_by_id(user_id=user_id)
    assert info is not None
    assert info.user.id == user_id
    assert "admin" in info.roles


def test_user_from_claims_uses_claims_user_id(session_factory):
    email_value = email_for("clm", "clm.example")
    _, user_id = _seed_tenant_with_user(
        session_factory,
        code="clm",
        email=email_value,
        password="StrongP@ss123",
        username="clm",
    )
    authed = auth_service.authenticate(
        email=email_value,
        password="StrongP@ss123",
    )
    pair = auth_service.issue_token_pair(authed=authed)
    claims = decode_token(pair.access_token)
    info = auth_service.user_from_claims(claims)
    assert info is not None
    assert info.user.id == user_id


def test_authenticate_then_refresh_does_not_change_last_login_at(session_factory):
    email_value = email_for("noref", "noref.example")
    _seed_tenant_with_user(
        session_factory,
        code="noref",
        email=email_value,
        password="StrongP@ss123",
        username="noref",
    )
    authed = auth_service.authenticate(
        email=email_value,
        password="StrongP@ss123",
    )
    pair = auth_service.issue_token_pair(authed=authed)
    last_before = authed.user.last_login_at
    assert last_before is not None
    _, new_pair = auth_service.refresh_tokens(refresh_token=pair.refresh_token)
    with session_factory() as s:
        refreshed_user = s.get(User, authed.user.id)
        assert refreshed_user is not None
        before = last_before
        after = refreshed_user.last_login_at
        if after.tzinfo is None:
            after = after.replace(tzinfo=UTC)
        if before.tzinfo is None:
            before = before.replace(tzinfo=UTC)
        assert after == before
    assert new_pair.session.id != pair.session.id


def test_get_user_roles_scopes_returns_admin_role_and_wildcard(session_factory):
    out = auth_service.register_tenant(
        tenant_code="rs",
        tenant_name="Roles",
        admin_email=email_for("rs", "rs.example"),
        admin_password="StrongP@ss123",
        admin_username="rs",
        admin_display_name=None,
    )
    with session_factory() as s:
        roles, scopes = auth_service._get_user_roles_scopes(
            session=s, user_id=out["user"]["id"], tenant_id=out["tenant_id"]
        )
    assert roles == ["admin"]
    assert "*" in scopes


def test_settings_cache_can_be_reset():
    reset_settings_cache()
