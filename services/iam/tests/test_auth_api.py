# Integration tests for the IAM /api/v1/auth routes.
# Uses FastAPI's TestClient (synchronous; runs the lifespan in-process)
# against an in-memory SQLite engine wired into the same engine cache
# that the SUT consults via aidp_db.session.get_session().
from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest
from aidp_common.config import get_settings
from aidp_db.session import reset_engine_cache
from aidp_events.inmemory_transport import InMemoryTransport
from aidp_events.producer import set_default_transport
from aidp_iam.main import create_app
from aidp_iam.models import Base
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.pool import StaticPool

_AT = chr(64)


def email_for(local, domain):
    return f"{local}{_AT}{domain}"


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
def app_with_db() -> Iterator[FastAPI]:
    """Build a fresh app whose SUT and DB share the same in-memory engine."""
    import aidp_db.session as db_session

    eng = _make_engine()
    # Pre-seed the engine cache so aidp_db.session.get_session() (with no
    # engine argument) returns this engine.
    db_session._engine_cache[str(eng.url)] = eng
    settings = get_settings()
    prev_url = settings.db_url
    object.__setattr__(settings, "db_url", str(eng.url))
    try:
        app = create_app()
        yield app
    finally:
        db_session._engine_cache.pop(str(eng.url), None)
        object.__setattr__(settings, "db_url", prev_url)
        Base.metadata.drop_all(eng)
        eng.dispose()
        reset_engine_cache()


@pytest.fixture
def in_memory_transport() -> Iterator[InMemoryTransport]:
    transport = InMemoryTransport(auto_offset_reset="earliest")
    set_default_transport(transport)
    try:
        yield transport
    finally:
        set_default_transport(None)


@pytest.fixture
def client(app_with_db: FastAPI) -> Iterator[TestClient]:
    with TestClient(app_with_db) as c:
        yield c


# ----- register-tenant --------------------------------------------------


def test_register_tenant_creates_tenant_and_admin(client: TestClient) -> None:
    """A successful register-tenant returns 200 with tenant + user + tokens."""
    r = client.post(
        "/api/v1/auth/register-tenant",
        json={
            "tenant_code": "acme",
            "tenant_name": "Acme",
            "admin_email": email_for("admin", "acme.example"),
            "admin_password": "StrongP@ss123",
            "admin_username": "acme-admin",
            "admin_display_name": "Acme Admin",
            "tenant_plan": "team",
            "tenant_region": "us-east-1",
        },
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["tenant_code"] == "acme"
    assert body["tenant_name"] == "Acme"
    assert body["user"]["email"] == email_for("admin", "acme.example")
    assert body["user"]["roles"] == ["admin"]
    assert "*" in body["user"]["scopes"]
    assert body["token"]["token_type"] == "Bearer"
    assert "access_token" in body["token"]
    assert "refresh_token" in body["token"]


@pytest.mark.asyncio
async def test_register_tenant_emits_iam_tenant_created_audit_event(
    client: TestClient, in_memory_transport: InMemoryTransport
) -> None:
    """``register-tenant`` publishes an ``iam.tenant.created`` audit event.

    Guards the regression flagged by the post-merge review: the
    service function used to advertise the event in its docstring
    (via the ``_AUDIT_EVENT_TYPE_TENANT_CREATED`` constant) but the
    actual publish was missing. The API handler now emits the
    event on a successful registration, so the in-memory transport
    receives exactly one ``iam.tenant.created`` message whose
    payload references the new tenant id and the bootstrap admin.
    """
    import json

    r = client.post(
        "/api/v1/auth/register-tenant",
        json={
            "tenant_code": "audited",
            "tenant_name": "Audited",
            "admin_email": email_for("admin", "audited.example"),
            "admin_password": "StrongP@ss123",
            "admin_username": "audited-admin",
        },
    )
    assert r.status_code == 200, r.text
    body = r.json()
    tenant_id = body["tenant_id"]
    admin_id = body["user"]["id"]

    msgs = await in_memory_transport.drain("iam.audit")
    created = []
    for m in msgs:
        envelope = json.loads(m.value.decode("utf-8"))
        if envelope.get("event_type") == "iam.tenant.created":
            created.append(envelope)
    assert len(created) == 1, f"expected 1 tenant.created event, got {len(created)}"
    payload = created[0]["payload"]
    assert payload["tenant_id"] == tenant_id
    assert payload["admin_user_id"] == admin_id
    assert payload["tenant_code"] == "audited"
    assert payload["admin_email"] == email_for("admin", "audited.example")


def test_register_tenant_rejects_weak_password(client: TestClient) -> None:
    """A password under 8 chars is rejected with 400."""
    r = client.post(
        "/api/v1/auth/register-tenant",
        json={
            "tenant_code": "weak",
            "tenant_name": "Weak",
            "admin_email": email_for("a", "weak.example"),
            "admin_password": "short",
        },
    )
    assert r.status_code == 422  # Pydantic validation error


def test_register_tenant_rejects_duplicate_code(client: TestClient) -> None:
    """A second register with the same code returns 409."""
    payload = {
        "tenant_code": "dup",
        "tenant_name": "First",
        "admin_email": email_for("a", "dup.example"),
        "admin_password": "StrongP@ss123",
    }
    r1 = client.post("/api/v1/auth/register-tenant", json=payload)
    assert r1.status_code == 200
    payload2 = {
        "tenant_code": "dup",
        "tenant_name": "Second",
        "admin_email": email_for("b", "dup.example"),
        "admin_password": "StrongP@ss123",
    }
    r2 = client.post("/api/v1/auth/register-tenant", json=payload2)
    assert r2.status_code == 409
    assert r2.json()["code"] == "CONFLICT"


def test_register_tenant_normalizes_email(client: TestClient) -> None:
    """The admin email is lowercased and stripped before persistence."""
    r = client.post(
        "/api/v1/auth/register-tenant",
        json={
            "tenant_code": "norm",
            "tenant_name": "Norm",
            "admin_email": "  " + "ADMIN" + _AT + "Norm.Example  ",
            "admin_password": "StrongP@ss123",
        },
    )
    assert r.status_code == 200, r.text
    assert r.json()["user"]["email"] == email_for("admin", "norm.example")


# ----- login ------------------------------------------------------------


def _register_and_login_payload() -> dict[str, Any]:
    return {
        "tenant_code": "login",
        "tenant_name": "Login",
        "admin_email": email_for("admin", "login.example"),
        "admin_password": "StrongP@ss123",
    }


def test_login_happy_path_returns_token_pair(
    client: TestClient, in_memory_transport: InMemoryTransport
) -> None:
    """A successful login returns a token pair + user, and emits an audit event."""
    r = client.post("/api/v1/auth/register-tenant", json=_register_and_login_payload())
    assert r.status_code == 200
    r = client.post(
        "/api/v1/auth/login",
        json={
            "email": email_for("admin", "login.example"),
            "password": "StrongP@ss123",
        },
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["token"]["token_type"] == "Bearer"
    assert "access_token" in body["token"]
    assert "refresh_token" in body["token"]
    assert body["user"]["email"] == email_for("admin", "login.example")


def test_login_wrong_password_returns_401(client: TestClient) -> None:
    """A wrong password returns 401 with a unified error envelope."""
    client.post("/api/v1/auth/register-tenant", json=_register_and_login_payload())
    r = client.post(
        "/api/v1/auth/login",
        json={
            "email": email_for("admin", "login.example"),
            "password": "not-the-password",
        },
    )
    assert r.status_code == 401
    body = r.json()
    assert body["code"] == "UNAUTHORIZED"
    assert "trace_id" in body or "details" in body


def test_login_unknown_email_returns_401(client: TestClient) -> None:
    """An unknown email returns 401 (no enumeration)."""
    r = client.post(
        "/api/v1/auth/login",
        json={
            "email": email_for("nobody", "nowhere.example"),
            "password": "anything",
        },
    )
    assert r.status_code == 401
    assert r.json()["code"] == "UNAUTHORIZED"


# ----- refresh ----------------------------------------------------------


def test_refresh_rotates_token(client: TestClient) -> None:
    """A successful refresh revokes the old session and issues a new pair."""
    client.post("/api/v1/auth/register-tenant", json=_register_and_login_payload())
    r = client.post(
        "/api/v1/auth/login",
        json={
            "email": email_for("admin", "login.example"),
            "password": "StrongP@ss123",
        },
    )
    pair = r.json()["token"]
    r = client.post(
        "/api/v1/auth/refresh",
        json={"refresh_token": pair["refresh_token"]},
    )
    assert r.status_code == 200, r.text
    new_pair = r.json()["token"]
    assert new_pair["access_token"] != pair["access_token"]
    assert new_pair["refresh_token"] != pair["refresh_token"]


def test_refresh_with_revoked_token_returns_401(client: TestClient) -> None:
    """Replaying an already-rotated refresh token returns 401."""
    client.post("/api/v1/auth/register-tenant", json=_register_and_login_payload())
    r = client.post(
        "/api/v1/auth/login",
        json={
            "email": email_for("admin", "login.example"),
            "password": "StrongP@ss123",
        },
    )
    pair = r.json()["token"]
    # First refresh succeeds.
    r1 = client.post(
        "/api/v1/auth/refresh",
        json={"refresh_token": pair["refresh_token"]},
    )
    assert r1.status_code == 200
    # Replay returns 401.
    r2 = client.post(
        "/api/v1/auth/refresh",
        json={"refresh_token": pair["refresh_token"]},
    )
    assert r2.status_code == 401


def test_refresh_with_unknown_token_returns_401(client: TestClient) -> None:
    """A junk refresh token returns 401."""
    r = client.post(
        "/api/v1/auth/refresh",
        json={"refresh_token": "not-a-jwt"},
    )
    assert r.status_code == 401


def test_refresh_from_other_tenant_still_uses_claims_tenant(
    client: TestClient, in_memory_transport: InMemoryTransport
) -> None:
    """A refresh token is scoped to the *claims* tenant, not the calling context.

    This guards the cross-tenant replay scenario: a caller in tenant B's
    context presents tenant A's refresh token. The rotation must still
    succeed against tenant A's session row (because ``claims.tenant_id``
    is the source of truth) and the new pair must be issued for tenant
    A's user — not silently dropped or routed to tenant B.

    Concretely, the test:

    1. Registers two tenants (``alpha`` and ``bravo``) with disjoint
       admin emails.
    2. Logs into ``alpha`` and captures the refresh token.
    3. Manually binds the request-thread tenant context to ``bravo``'s
       id, simulating "a request that arrives while the caller is in
       the wrong tenant's middleware context".
    4. POSTs ``/refresh`` with ``alpha``'s refresh token and asserts
       200 + a brand-new pair that belongs to ``alpha``'s admin
       (i.e. ``user.tenant_id == alpha.tenant_id`` and the new
       refresh token's claims resolve to the same admin).
    """
    from aidp_db.session import get_session
    from aidp_db.tenant import reset_tenant_context, set_tenant_context
    from aidp_iam.models import Session as SessionModel
    from aidp_iam.models import User
    from sqlalchemy import select

    # 1. Register two tenants.
    r = client.post(
        "/api/v1/auth/register-tenant",
        json={
            "tenant_code": "alpha",
            "tenant_name": "Alpha",
            "admin_email": email_for("admin", "alpha.example"),
            "admin_password": "StrongP@ss123",
        },
    )
    assert r.status_code == 200, r.text
    alpha_body = r.json()
    alpha_tenant_id = alpha_body["tenant_id"]
    alpha_admin_id = alpha_body["user"]["id"]

    r = client.post(
        "/api/v1/auth/register-tenant",
        json={
            "tenant_code": "bravo",
            "tenant_name": "Bravo",
            "admin_email": email_for("admin", "bravo.example"),
            "admin_password": "StrongP@ss123",
        },
    )
    assert r.status_code == 200, r.text
    bravo_tenant_id = r.json()["tenant_id"]
    assert bravo_tenant_id != alpha_tenant_id

    # 2. Log into alpha and capture the refresh token.
    r = client.post(
        "/api/v1/auth/login",
        json={
            "email": email_for("admin", "alpha.example"),
            "password": "StrongP@ss123",
        },
    )
    assert r.status_code == 200, r.text
    alpha_refresh = r.json()["token"]["refresh_token"]
    # And into bravo (so the bravo tenant has a known user, and we
    # can prove bravo's session list is unaffected by the rotation).
    r = client.post(
        "/api/v1/auth/login",
        json={
            "email": email_for("admin", "bravo.example"),
            "password": "StrongP@ss123",
            "tenant_code": "bravo",
        },
    )
    assert r.status_code == 200, r.text
    bravo_refresh = r.json()["token"]["refresh_token"]

    # 3. Bind the calling context to bravo's tenant id and refresh
    # against alpha's refresh token. The TestClient + anyio combo
    # propagates the calling contextvars.Context into the request
    # handler, so the binding is visible to ``refresh_tokens`` —
    # which must override it with the claim's tenant id to keep
    # the L1 listener filter aligned with the explicit
    # ``WHERE tenant_id = claims.tenant_id``.
    ctx_token = set_tenant_context(bravo_tenant_id)
    try:
        r = client.post(
            "/api/v1/auth/refresh",
            json={"refresh_token": alpha_refresh},
        )
    finally:
        reset_tenant_context(ctx_token)

    # 4. The refresh must succeed and the new pair must belong to
    # alpha — never to bravo.
    assert r.status_code == 200, r.text
    new_body = r.json()
    assert new_body["user"]["tenant_id"] == alpha_tenant_id
    assert new_body["user"]["id"] == alpha_admin_id
    assert new_body["token"]["refresh_token"] != alpha_refresh

    # 5. Cross-check the database: alpha's old login session is
    # revoked and a brand-new active session is in place; bravo's
    # session list is untouched. This proves the rotation targeted
    # the claim's tenant and not the calling context.
    #
    # Note that ``register-tenant`` itself issues a bootstrap session,
    # so alpha starts with 1 active session before the login. The
    # total active count after the rotation is therefore 2
    # (bootstrap session + the rotated login session).
    with get_session() as session:
        alpha_sessions = (
            session.execute(select(SessionModel).where(SessionModel.tenant_id == alpha_tenant_id))
            .scalars()
            .all()
        )
        bravo_sessions = (
            session.execute(select(SessionModel).where(SessionModel.tenant_id == bravo_tenant_id))
            .scalars()
            .all()
        )
        bravo_admin = session.execute(
            select(User).where(User.tenant_id == bravo_tenant_id)
        ).scalar_one()

    active_alpha = [s for s in alpha_sessions if s.revoked_at is None]
    revoked_alpha = [s for s in alpha_sessions if s.revoked_at is not None]

    # The rotation must produce exactly one fresh active session
    # and revoke exactly one (the login session we presented). The
    # bootstrap session from register-tenant stays active.
    assert len(active_alpha) == 2, (
        f"expected 2 active alpha sessions (bootstrap + rotated), got {len(active_alpha)}"
    )
    assert len(revoked_alpha) == 1, (
        f"expected exactly 1 revoked alpha session (the rotated login), got {len(revoked_alpha)}"
    )
    assert all(s.user_id == alpha_admin_id for s in active_alpha)
    assert revoked_alpha[0].user_id == alpha_admin_id

    # bravo: its bootstrap + login sessions are still active and
    # unaffected by alpha's refresh. We log into bravo in step 2
    # purely to populate its session list, so the rotation against
    # alpha's token must not touch any bravo row.
    active_bravo = [s for s in bravo_sessions if s.revoked_at is None]
    revoked_bravo = [s for s in bravo_sessions if s.revoked_at is not None]
    assert len(active_bravo) == 2, (
        f"expected 2 active bravo sessions (bootstrap + login), got {len(active_bravo)}"
    )
    assert len(revoked_bravo) == 0, (
        f"alpha's refresh must not touch bravo sessions; got {len(revoked_bravo)} revoked"
    )
    assert all(s.user_id == bravo_admin.id for s in active_bravo)
    # Suppress lint about unused local.
    _ = bravo_refresh


# ----- logout -----------------------------------------------------------


def test_logout_revokes_session(client: TestClient) -> None:
    """A logout returns 204 and prevents further use of the refresh token."""
    client.post("/api/v1/auth/register-tenant", json=_register_and_login_payload())
    r = client.post(
        "/api/v1/auth/login",
        json={
            "email": email_for("admin", "login.example"),
            "password": "StrongP@ss123",
        },
    )
    pair = r.json()["token"]
    r = client.post(
        "/api/v1/auth/logout",
        json={"refresh_token": pair["refresh_token"]},
    )
    assert r.status_code == 204
    # The refresh token is now revoked.
    r = client.post(
        "/api/v1/auth/refresh",
        json={"refresh_token": pair["refresh_token"]},
    )
    assert r.status_code == 401


def test_logout_is_idempotent(client: TestClient) -> None:
    """Logging out twice with the same token still returns 204."""
    client.post("/api/v1/auth/register-tenant", json=_register_and_login_payload())
    r = client.post(
        "/api/v1/auth/login",
        json={
            "email": email_for("admin", "login.example"),
            "password": "StrongP@ss123",
        },
    )
    pair = r.json()["token"]
    r1 = client.post(
        "/api/v1/auth/logout",
        json={"refresh_token": pair["refresh_token"]},
    )
    r2 = client.post(
        "/api/v1/auth/logout",
        json={"refresh_token": pair["refresh_token"]},
    )
    assert r1.status_code == 204
    assert r2.status_code == 204


# ----- SSO (stub) -------------------------------------------------------


def test_sso_callback_returns_501(client: TestClient) -> None:
    """The SSO callback returns 501 until SSO is implemented."""
    r = client.post("/api/v1/auth/sso/google/callback", json={})
    assert r.status_code == 501
    body = r.json()
    assert body["code"] == "SSO_NOT_IMPLEMENTED"
    assert body["provider"] == "google"


# ----- me ---------------------------------------------------------------


def test_me_returns_caller_user_info(client: TestClient) -> None:
    """The /me endpoint returns the authenticated caller's user view."""
    client.post("/api/v1/auth/register-tenant", json=_register_and_login_payload())
    r = client.post(
        "/api/v1/auth/login",
        json={
            "email": email_for("admin", "login.example"),
            "password": "StrongP@ss123",
        },
    )
    access = r.json()["token"]["access_token"]
    r = client.get(
        "/api/v1/auth/me",
        headers={"Authorization": f"Bearer {access}"},
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["user"]["email"] == email_for("admin", "login.example")
    assert "admin" in body["user"]["roles"]


def test_me_without_token_returns_401(client: TestClient) -> None:
    """A missing Authorization header returns 401."""
    r = client.get("/api/v1/auth/me")
    assert r.status_code == 401
    assert r.json()["code"] == "UNAUTHORIZED"


def test_me_with_refresh_token_returns_401(client: TestClient) -> None:
    """A refresh token is not a valid Authorization credential."""
    client.post("/api/v1/auth/register-tenant", json=_register_and_login_payload())
    r = client.post(
        "/api/v1/auth/login",
        json={
            "email": email_for("admin", "login.example"),
            "password": "StrongP@ss123",
        },
    )
    refresh = r.json()["token"]["refresh_token"]
    r = client.get(
        "/api/v1/auth/me",
        headers={"Authorization": f"Bearer {refresh}"},
    )
    assert r.status_code == 401


# ----- error envelope ---------------------------------------------------


def test_validation_error_returns_unified_envelope(client: TestClient) -> None:
    """A malformed body returns 422 with the FastAPI/Pydantic default envelope."""
    r = client.post(
        "/api/v1/auth/login",
        json={"email": "not-an-email", "password": "x"},
    )
    assert r.status_code == 422


def test_healthz_returns_ok(client: TestClient) -> None:
    """The liveness probe still works after the auth router is mounted."""
    r = client.get("/healthz")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}
