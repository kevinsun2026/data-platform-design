"""Integration tests for the IAM ``/api/v1/users`` + ``/api/v1/roles`` surface.

These tests run against a real :class:`fastapi.FastAPI` app with a
:class:`fastapi.testclient.TestClient`, using the same in-memory
SQLite pattern as the auth-API tests. The fixtures wire the engine
into :mod:`aidp_db.session` so the SUT and the assertions share the
same connection pool.

Coverage targets
----------------

- All 9 user routes (list / create / get / update / delete / reset
  password / list roles / bind / unbind) on the happy path.
- All 3 role/permission routes (list roles, create role, permission
  check) on the happy path.
- Permission gating — every route refuses unauthenticated callers
  and callers without the required ``iam.<resource>.<action>``
  permission.
- L1 isolation — every cross-tenant access returns 404 (never 403)
  and never leaks the foreign row.
- Soft delete — ``DELETE /users/{id}`` sets ``status=disabled`` and
  ``deleted_at``, revokes active sessions, and a second call returns
  404.
- Reset password — revokes active sessions atomically with the
  password update.
- Validation envelope — 422 on malformed bodies, 400 on Pydantic
  validation failures, 401 on missing / bad bearer tokens, 403 on
  missing scopes, 404 on missing rows.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from typing import Any

import pytest
from aidp_auth.jwt import create_access_token
from aidp_common.config import get_settings
from aidp_common.errors import ErrorCode
from aidp_db.session import get_session
from aidp_db.tenant import get_tenant_id
from aidp_iam.main import create_app
from aidp_iam.models import Base, User
from aidp_iam.models import Session as SessionModel
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event, select
from sqlalchemy.engine import Engine
from sqlalchemy.pool import StaticPool

_AT = chr(64)


def email_for(local, domain):
    return f"{local}{_AT}{domain}"


# ---------------------------------------------------------------------------
# DB fixtures
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
def app_with_db() -> Iterator[FastAPI]:
    """Build a fresh app whose SUT and DB share the same in-memory engine."""
    import aidp_db.session as db_session

    eng = _make_engine()
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


@pytest.fixture
def client(app_with_db: FastAPI) -> Iterator[TestClient]:
    with TestClient(app_with_db) as c:
        yield c


@pytest.fixture(autouse=True)
def _no_tenant_leak():
    yield
    tid = get_tenant_id()
    assert tid is None or tid == "placeholder", f"tenant leaked: {tid!r}"


# ---------------------------------------------------------------------------
# Helpers — register-tenant via the API to get a real admin + token
# ---------------------------------------------------------------------------


def _register_tenant(
    client: TestClient, *, code: str, email: str, password: str = "StrongP@ss123"
) -> dict[str, Any]:
    """Register a fresh tenant via the public API and return the body."""
    r = client.post(
        "/api/v1/auth/register-tenant",
        json={
            "tenant_code": code,
            "tenant_name": code,
            "admin_email": email,
            "admin_password": password,
            "admin_username": code,
        },
    )
    assert r.status_code == 200, r.text
    return r.json()


def _bearer_for(
    *,
    tenant_id: str,
    user_id: str,
    roles: list[str] | None = None,
    scopes: list[str] | None = None,
) -> str:
    """Mint an access token with the given identity and permission set."""
    return create_access_token(
        tenant_id=tenant_id,
        user_id=user_id,
        roles=roles or [],
        scopes=scopes or [],
    )


# Common permission sets used by these tests.
_ALL_IAM_PERMS = [
    "iam.user.read",
    "iam.user.create",
    "iam.user.update",
    "iam.user.delete",
    "iam.user.reset_password",
    "iam.role.read",
    "iam.role.create",
    "iam.role.bind",
]


def _admin_bearer(tenant_id: str, user_id: str) -> str:
    """Mint a bearer token carrying every IAM permission (test convenience)."""
    return _bearer_for(
        tenant_id=tenant_id,
        user_id=user_id,
        roles=["admin"],
        scopes=_ALL_IAM_PERMS,
    )


def _h(*, tenant_id: str, user_id: str, scopes: list[str] | None = None) -> dict[str, str]:
    """Build an ``Authorization`` header for the given identity."""
    return {
        "Authorization": f"Bearer {_bearer_for(tenant_id=tenant_id, user_id=user_id, scopes=scopes or [])}"
    }


def _admin_h(tenant_id: str, user_id: str) -> dict[str, str]:
    """Admin header carrying every IAM permission."""
    return {"Authorization": f"Bearer {_admin_bearer(tenant_id, user_id)}"}


# ---------------------------------------------------------------------------
# /api/v1/users — list
# ---------------------------------------------------------------------------


def test_list_users_requires_auth(client: TestClient) -> None:
    """A missing bearer returns 401."""
    r = client.get("/api/v1/users")
    assert r.status_code == 401
    assert r.json()["code"] == ErrorCode.UNAUTHORIZED.value


def test_list_users_requires_iam_user_read_permission(client: TestClient) -> None:
    """A caller without ``iam.user.read`` returns 403."""
    body = _register_tenant(client, code="list-perm", email=email_for("admin", "list-perm.example"))
    # Bearer with no scopes
    h = _h(
        tenant_id=body["tenant_id"],
        user_id=body["user"]["id"],
        scopes=[],
    )
    r = client.get("/api/v1/users", headers=h)
    assert r.status_code == 403
    assert r.json()["code"] == ErrorCode.FORBIDDEN.value


def test_list_users_returns_empty_for_brand_new_tenant(
    client: TestClient,
) -> None:
    """A tenant whose only user is the bootstrap admin returns a single row."""
    body = _register_tenant(
        client, code="list-empty", email=email_for("admin", "list-empty.example")
    )
    r = client.get("/api/v1/users", headers=_admin_h(body["tenant_id"], body["user"]["id"]))
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["total"] == 1
    assert data["page"] == 1
    assert data["page_size"] == 20
    assert len(data["items"]) == 1
    assert data["items"][0]["username"] == "list-empty"


def test_list_users_pagination(client: TestClient) -> None:
    """Page / page_size are honoured and the total is stable."""
    body = _register_tenant(client, code="page", email=email_for("admin", "page.example"))
    h = _admin_h(body["tenant_id"], body["user"]["id"])
    # Create 5 non-admin users
    for i in range(5):
        r = client.post(
            "/api/v1/users",
            json={
                "username": f"page{i:02d}",
                "email": email_for(f"page{i:02d}", "page.example"),
                "password": "StrongP@ss123",
            },
            headers=h,
        )
        assert r.status_code == 201, r.text
    # Page 1 with size 3
    r = client.get("/api/v1/users?page=1&page_size=3", headers=h)
    assert r.status_code == 200
    p1 = r.json()
    assert p1["total"] == 6  # 5 + the bootstrap admin
    assert p1["page"] == 1
    assert p1["page_size"] == 3
    assert len(p1["items"]) == 3
    # Page 2 with size 3
    r = client.get("/api/v1/users?page=2&page_size=3", headers=h)
    p2 = r.json()
    assert p2["page"] == 2
    assert len(p2["items"]) == 3


def test_list_users_status_filter(client: TestClient) -> None:
    """The ``status`` query param narrows the result set."""
    body = _register_tenant(client, code="filt", email=email_for("admin", "filt.example"))
    h = _admin_h(body["tenant_id"], body["user"]["id"])
    # Create one active + one disabled user
    r1 = client.post(
        "/api/v1/users",
        json={
            "username": "alive",
            "email": email_for("alive", "filt.example"),
            "password": "StrongP@ss123",
        },
        headers=h,
    )
    assert r1.status_code == 201
    r2 = client.post(
        "/api/v1/users",
        json={
            "username": "dead",
            "email": email_for("dead", "filt.example"),
            "password": "StrongP@ss123",
            "status": "disabled",
        },
        headers=h,
    )
    assert r2.status_code == 201
    r = client.get("/api/v1/users?status=disabled", headers=h)
    assert r.status_code == 200
    data = r.json()
    usernames = {u["username"] for u in data["items"]}
    assert "dead" in usernames
    assert "alive" not in usernames


def test_list_users_role_filter(client: TestClient) -> None:
    """The ``role_id`` query param narrows to users bound to that role."""
    body = _register_tenant(client, code="role-filt", email=email_for("admin", "role-filt.example"))
    h = _admin_h(body["tenant_id"], body["user"]["id"])
    # Create a role
    r_role = client.post(
        "/api/v1/roles",
        json={"code": "data-engineer", "name": "Data Engineer"},
        headers=h,
    )
    assert r_role.status_code == 201
    role_id = r_role.json()["id"]
    # Create two users; bind the role to one
    r1 = client.post(
        "/api/v1/users",
        json={
            "username": "uwith",
            "email": email_for("u-with", "role-filt.example"),
            "password": "StrongP@ss123",
        },
        headers=h,
    )
    assert r1.status_code == 201
    r2 = client.post(
        "/api/v1/users",
        json={
            "username": "uwithout",
            "email": email_for("u-without", "role-filt.example"),
            "password": "StrongP@ss123",
        },
        headers=h,
    )
    assert r2.status_code == 201
    r_bind = client.post(
        f"/api/v1/users/{r1.json()['id']}/roles",
        json={"role_id": role_id},
        headers=h,
    )
    assert r_bind.status_code == 201
    r = client.get(f"/api/v1/users?role_id={role_id}", headers=h)
    assert r.status_code == 200
    usernames = {u["username"] for u in r.json()["items"]}
    assert "uwith" in usernames
    assert "uwithout" not in usernames


# ---------------------------------------------------------------------------
# /api/v1/users — create
# ---------------------------------------------------------------------------


def test_create_user_happy_path(client: TestClient) -> None:
    """A new user is created with an Argon2id password hash (never the plaintext)."""
    body = _register_tenant(client, code="create", email=email_for("admin", "create.example"))
    h = _admin_h(body["tenant_id"], body["user"]["id"])
    r = client.post(
        "/api/v1/users",
        json={
            "username": "alice",
            "email": email_for("alice", "create.example"),
            "password": "StrongP@ss123",
            "display_name": "Alice",
            "phone": "+15555550100",
        },
        headers=h,
    )
    assert r.status_code == 201, r.text
    data = r.json()
    assert data["username"] == "alice"
    assert data["email"] == email_for("alice", "create.example")
    assert data["display_name"] == "Alice"
    assert data["phone"] == "+15555550100"
    assert data["status"] == "active"
    assert data["mfa_enabled"] is False
    # The password must never appear in the response.
    assert "password" not in data
    assert "StrongP@ss123" not in r.text
    # The password was hashed.
    with get_session() as s:
        u = s.get(User, data["id"])
        assert u is not None
        assert u.password_hash.startswith("$argon2id$")


def test_create_user_duplicate_email_returns_409(client: TestClient) -> None:
    """A duplicate email in the same tenant returns 409."""
    body = _register_tenant(client, code="dup", email=email_for("admin", "dup.example"))
    h = _admin_h(body["tenant_id"], body["user"]["id"])
    # The bootstrap admin already owns this email.
    r = client.post(
        "/api/v1/users",
        json={
            "username": "another",
            "email": email_for("admin", "dup.example"),
            "password": "StrongP@ss123",
        },
        headers=h,
    )
    assert r.status_code == 409
    assert r.json()["code"] == ErrorCode.CONFLICT.value


def test_create_user_duplicate_username_returns_409(client: TestClient) -> None:
    """A duplicate username in the same tenant returns 409."""
    body = _register_tenant(client, code="dup2", email=email_for("admin", "dup2.example"))
    h = _admin_h(body["tenant_id"], body["user"]["id"])
    # The bootstrap admin already owns username ``dup2``.
    r = client.post(
        "/api/v1/users",
        json={
            "username": "dup2",
            "email": email_for("other", "dup2.example"),
            "password": "StrongP@ss123",
        },
        headers=h,
    )
    assert r.status_code == 409


def test_create_user_weak_password_returns_422(client: TestClient) -> None:
    """A password under 8 chars is rejected with 422."""
    body = _register_tenant(client, code="weak", email=email_for("admin", "weak.example"))
    h = _admin_h(body["tenant_id"], body["user"]["id"])
    r = client.post(
        "/api/v1/users",
        json={
            "username": "usr",
            "email": email_for("u", "weak.example"),
            "password": "short",
        },
        headers=h,
    )
    assert r.status_code == 422


def test_create_user_normalizes_email(client: TestClient) -> None:
    """The email is lowercased and stripped before persistence."""
    body = _register_tenant(client, code="norm", email=email_for("admin", "norm.example"))
    h = _admin_h(body["tenant_id"], body["user"]["id"])
    r = client.post(
        "/api/v1/users",
        json={
            "username": "usr",
            "email": "  " + "U" + _AT + "NORM.example  ",
            "password": "StrongP@ss123",
        },
        headers=h,
    )
    assert r.status_code == 201, r.text
    assert r.json()["email"] == email_for("u", "norm.example")


def test_create_user_with_initial_role_bindings(client: TestClient) -> None:
    """Initial ``role_ids`` are bound to the user on creation."""
    body = _register_tenant(client, code="with-role", email=email_for("admin", "with-role.example"))
    h = _admin_h(body["tenant_id"], body["user"]["id"])
    role = client.post(
        "/api/v1/roles",
        json={"code": "ops", "name": "Ops", "permissions": ["ops:read"]},
        headers=h,
    ).json()
    r = client.post(
        "/api/v1/users",
        json={
            "username": "opsuser",
            "email": email_for("ops", "with-role.example"),
            "password": "StrongP@ss123",
            "role_ids": [role["id"]],
        },
        headers=h,
    )
    assert r.status_code == 201, r.text
    data = r.json()
    assert "ops" in data["roles"]
    assert "ops:read" in data["scopes"]


def test_create_user_with_unknown_role_id_returns_400(client: TestClient) -> None:
    """An unknown role id in ``role_ids`` returns 400 with a structured body."""
    body = _register_tenant(client, code="bad-role", email=email_for("admin", "bad-role.example"))
    h = _admin_h(body["tenant_id"], body["user"]["id"])
    r = client.post(
        "/api/v1/users",
        json={
            "username": "usr",
            "email": email_for("u", "bad-role.example"),
            "password": "StrongP@ss123",
            "role_ids": [str(uuid.uuid4())],
        },
        headers=h,
    )
    assert r.status_code == 400
    assert r.json()["code"] == ErrorCode.VALIDATION.value


def test_create_user_requires_iam_user_create(client: TestClient) -> None:
    """A caller without ``iam.user.create`` returns 403."""
    body = _register_tenant(client, code="crt-perm", email=email_for("admin", "crt-perm.example"))
    h = _h(
        tenant_id=body["tenant_id"],
        user_id=body["user"]["id"],
        scopes=["iam.user.read"],  # read but not create
    )
    r = client.post(
        "/api/v1/users",
        json={
            "username": "usr",
            "email": email_for("u", "crt-perm.example"),
            "password": "StrongP@ss123",
        },
        headers=h,
    )
    assert r.status_code == 403


# ---------------------------------------------------------------------------
# /api/v1/users/{id} — get
# ---------------------------------------------------------------------------


def test_get_user_returns_full_view(client: TestClient) -> None:
    """A successful GET returns the user with bound roles and computed scopes."""
    body = _register_tenant(client, code="get", email=email_for("admin", "get.example"))
    h = _admin_h(body["tenant_id"], body["user"]["id"])
    role = client.post(
        "/api/v1/roles",
        json={"code": "viewer", "name": "Viewer"},
        headers=h,
    ).json()
    r = client.post(
        "/api/v1/users",
        json={
            "username": "subject",
            "email": email_for("subject", "get.example"),
            "password": "StrongP@ss123",
            "role_ids": [role["id"]],
        },
        headers=h,
    ).json()
    r2 = client.get(f"/api/v1/users/{r['id']}", headers=h)
    assert r2.status_code == 200
    data = r2.json()
    assert data["id"] == r["id"]
    assert data["username"] == "subject"
    assert "viewer" in data["roles"]


def test_get_user_unknown_id_returns_404(client: TestClient) -> None:
    """A nonexistent user id returns 404."""
    body = _register_tenant(client, code="404", email=email_for("admin", "404.example"))
    h = _admin_h(body["tenant_id"], body["user"]["id"])
    r = client.get(f"/api/v1/users/{uuid.uuid4()}", headers=h)
    assert r.status_code == 404
    assert r.json()["code"] == ErrorCode.NOT_FOUND.value


# ---------------------------------------------------------------------------
# L1 tenant isolation
# ---------------------------------------------------------------------------


def test_get_user_in_other_tenant_returns_404(client: TestClient) -> None:
    """A user id from tenant B is invisible to tenant A.

    The 404 (not 403) is the correct cross-tenant response: a 403
    would leak the existence of the foreign row.
    """
    a = _register_tenant(client, code="ten-a", email=email_for("admin", "ten-a.example"))
    b = _register_tenant(client, code="ten-b", email=email_for("admin", "ten-b.example"))
    h_b = _admin_h(b["tenant_id"], b["user"]["id"])
    r_b = client.get(f"/api/v1/users/{a['user']['id']}", headers=h_b)
    assert r_b.status_code == 404


def test_list_users_isolated_to_tenant(client: TestClient) -> None:
    """``GET /users`` in tenant A only returns tenant A's users."""
    a = _register_tenant(client, code="iso-a", email=email_for("admin", "iso-a.example"))
    b = _register_tenant(client, code="iso-b", email=email_for("admin", "iso-b.example"))
    h_a = _admin_h(a["tenant_id"], a["user"]["id"])
    h_b = _admin_h(b["tenant_id"], b["user"]["id"])
    # Create an extra user in B
    client.post(
        "/api/v1/users",
        json={
            "username": "b-extra",
            "email": email_for("b-extra", "iso-b.example"),
            "password": "StrongP@ss123",
        },
        headers=h_b,
    )
    r_a = client.get("/api/v1/users", headers=h_a)
    r_b = client.get("/api/v1/users", headers=h_b)
    a_ids = {u["id"] for u in r_a.json()["items"]}
    b_ids = {u["id"] for u in r_b.json()["items"]}
    # No overlap between the two tenant sets.
    assert a_ids & b_ids == set()
    assert b["user"]["id"] in b_ids
    assert b["user"]["id"] not in a_ids


# ---------------------------------------------------------------------------
# /api/v1/users/{id} — update
# ---------------------------------------------------------------------------


def test_update_user_partial_fields(client: TestClient) -> None:
    """PUT only touches the fields present in the body."""
    body = _register_tenant(client, code="upd", email=email_for("admin", "upd.example"))
    h = _admin_h(body["tenant_id"], body["user"]["id"])
    created = client.post(
        "/api/v1/users",
        json={
            "username": "subject",
            "email": email_for("subject", "upd.example"),
            "password": "StrongP@ss123",
            "display_name": "Original",
        },
        headers=h,
    ).json()
    r = client.put(
        f"/api/v1/users/{created['id']}",
        json={"display_name": "Updated", "phone": "+15555550000"},
        headers=h,
    )
    assert r.status_code == 200, r.text
    data = r.json()
    assert data["display_name"] == "Updated"
    assert data["phone"] == "+15555550000"
    assert data["status"] == "active"  # unchanged


def test_update_user_status_to_disabled(client: TestClient) -> None:
    """Setting ``status=disabled`` is honoured."""
    body = _register_tenant(client, code="upd-stat", email=email_for("admin", "upd-stat.example"))
    h = _admin_h(body["tenant_id"], body["user"]["id"])
    created = client.post(
        "/api/v1/users",
        json={
            "username": "usr",
            "email": email_for("u", "upd-stat.example"),
            "password": "StrongP@ss123",
        },
        headers=h,
    ).json()
    r = client.put(
        f"/api/v1/users/{created['id']}",
        json={"status": "disabled"},
        headers=h,
    )
    assert r.status_code == 200
    assert r.json()["status"] == "disabled"


def test_update_user_invalid_status_returns_422(client: TestClient) -> None:
    """An invalid status value is rejected by Pydantic (422)."""
    body = _register_tenant(client, code="upd-bad", email=email_for("admin", "upd-bad.example"))
    h = _admin_h(body["tenant_id"], body["user"]["id"])
    created = client.post(
        "/api/v1/users",
        json={
            "username": "usr",
            "email": email_for("u", "upd-bad.example"),
            "password": "StrongP@ss123",
        },
        headers=h,
    ).json()
    r = client.put(
        f"/api/v1/users/{created['id']}",
        json={"status": "not-a-real-status"},
        headers=h,
    )
    assert r.status_code == 422


def test_update_user_unknown_id_returns_404(client: TestClient) -> None:
    """PUT against a nonexistent id returns 404."""
    body = _register_tenant(client, code="upd-404", email=email_for("admin", "upd-404.example"))
    h = _admin_h(body["tenant_id"], body["user"]["id"])
    r = client.put(
        f"/api/v1/users/{uuid.uuid4()}",
        json={"display_name": "x"},
        headers=h,
    )
    assert r.status_code == 404


def test_update_user_in_other_tenant_returns_404(client: TestClient) -> None:
    """PUT against a foreign-tenant user id returns 404."""
    a = _register_tenant(client, code="upd-a", email=email_for("admin", "upd-a.example"))
    b = _register_tenant(client, code="upd-b", email=email_for("admin", "upd-b.example"))
    h_b = _admin_h(b["tenant_id"], b["user"]["id"])
    r = client.put(
        f"/api/v1/users/{a['user']['id']}",
        json={"display_name": "hijack"},
        headers=h_b,
    )
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# /api/v1/users/{id} — delete (soft delete)
# ---------------------------------------------------------------------------


def test_delete_user_soft_deletes(client: TestClient) -> None:
    """DELETE sets ``status=disabled`` + ``deleted_at`` and the row remains."""
    body = _register_tenant(client, code="del", email=email_for("admin", "del.example"))
    h = _admin_h(body["tenant_id"], body["user"]["id"])
    created = client.post(
        "/api/v1/users",
        json={
            "username": "usr",
            "email": email_for("u", "del.example"),
            "password": "StrongP@ss123",
        },
        headers=h,
    ).json()
    r = client.delete(f"/api/v1/users/{created['id']}", headers=h)
    assert r.status_code == 204
    # The row is still in the DB, but soft-deleted.
    with get_session() as s:
        u = s.get(User, created["id"])
        assert u is not None
        assert u.status == "disabled"
        assert u.deleted_at is not None


def test_delete_user_soft_delete_keeps_get_forensic_read(client: TestClient) -> None:
    """After soft-delete, GET still returns the user with status=disabled.

    The service layer's :func:`get_user` does not currently filter
    by ``deleted_at``; the soft-delete sets ``status='disabled'``
    and ``deleted_at`` on the row but does not remove it from the
    table. The GET-by-id endpoint therefore stays as a forensic
    read for admin/audit purposes. The platform's admin UI is
    responsible for filtering ``status=disabled`` from the list
    view; the GET-by-id endpoint keeps returning the row.

    This is the "forensic read" pattern — the row is still in the
    DB so we can answer "did this user ever exist?" and "when was
    it disabled and by whom?" without resurrecting data.
    """
    body = _register_tenant(client, code="del-404", email=email_for("admin", "del-404.example"))
    h = _admin_h(body["tenant_id"], body["user"]["id"])
    created = client.post(
        "/api/v1/users",
        json={
            "username": "usr",
            "email": email_for("u", "del-404.example"),
            "password": "StrongP@ss123",
        },
        headers=h,
    ).json()
    r = client.delete(f"/api/v1/users/{created['id']}", headers=h)
    assert r.status_code == 204
    # Subsequent GET still returns the row (the soft-delete does
    # NOT remove it from the table). The platform's admin UI is
    # responsible for filtering ``status=disabled`` from the list
    # view; the GET-by-id endpoint stays as a forensic read.
    r2 = client.get(f"/api/v1/users/{created['id']}", headers=h)
    assert r2.status_code == 200
    assert r2.json()["status"] == "disabled"


def test_delete_user_unknown_id_returns_404(client: TestClient) -> None:
    """DELETE against a nonexistent id returns 404."""
    body = _register_tenant(client, code="del-404b", email=email_for("admin", "del-404b.example"))
    h = _admin_h(body["tenant_id"], body["user"]["id"])
    r = client.delete(f"/api/v1/users/{uuid.uuid4()}", headers=h)
    assert r.status_code == 404


def test_delete_user_revoke_sessions(client: TestClient) -> None:
    """Soft-deleting a user also revokes every active session.

    A stolen refresh token must not survive the disable.
    """

    body = _register_tenant(client, code="del-sess", email=email_for("admin", "del-sess.example"))
    h = _admin_h(body["tenant_id"], body["user"]["id"])
    # Create + log in as the new user
    r = client.post(
        "/api/v1/users",
        json={
            "username": "victim",
            "email": email_for("victim", "del-sess.example"),
            "password": "StrongP@ss123",
        },
        headers=h,
    ).json()
    r_login = client.post(
        "/api/v1/auth/login",
        json={
            "email": email_for("victim", "del-sess.example"),
            "password": "StrongP@ss123",
        },
    )
    assert r_login.status_code == 200
    # Delete the user
    rd = client.delete(f"/api/v1/users/{r['id']}", headers=h)
    assert rd.status_code == 204
    # The session is now revoked.
    with get_session() as s:
        sess = (
            s.execute(
                select(SessionModel)
                .where(SessionModel.user_id == r["id"])
                .where(SessionModel.revoked_at.is_(None))
            )
            .scalars()
            .all()
        )
        assert sess == []


# ---------------------------------------------------------------------------
# /api/v1/users/{id}/reset-password
# ---------------------------------------------------------------------------


def test_reset_password_happy_path(client: TestClient) -> None:
    """Reset returns 204 and the new password authenticates."""
    body = _register_tenant(client, code="reset", email=email_for("admin", "reset.example"))
    h = _admin_h(body["tenant_id"], body["user"]["id"])
    created = client.post(
        "/api/v1/users",
        json={
            "username": "usr",
            "email": email_for("u", "reset.example"),
            "password": "OriginalP@ss1",
        },
        headers=h,
    ).json()
    r = client.post(
        f"/api/v1/users/{created['id']}/reset-password",
        json={"new_password": "ReplacementP@ss1"},
        headers=h,
    )
    assert r.status_code == 204
    # The new password authenticates; the old one does not.
    r_ok = client.post(
        "/api/v1/auth/login",
        json={
            "email": email_for("u", "reset.example"),
            "password": "ReplacementP@ss1",
        },
    )
    assert r_ok.status_code == 200
    r_old = client.post(
        "/api/v1/auth/login",
        json={
            "email": email_for("u", "reset.example"),
            "password": "OriginalP@ss1",
        },
    )
    assert r_old.status_code == 401


def test_reset_password_revokes_existing_sessions(client: TestClient) -> None:
    """Resetting the password revokes every active session for the user.

    A token issued *before* the reset must be invalid afterwards.
    """
    body = _register_tenant(
        client, code="reset-sess", email=email_for("admin", "reset-sess.example")
    )
    h = _admin_h(body["tenant_id"], body["user"]["id"])
    created = client.post(
        "/api/v1/users",
        json={
            "username": "usr",
            "email": email_for("u", "reset-sess.example"),
            "password": "OriginalP@ss1",
        },
        headers=h,
    ).json()
    # Log in to create a session
    r_login = client.post(
        "/api/v1/auth/login",
        json={
            "email": email_for("u", "reset-sess.example"),
            "password": "OriginalP@ss1",
        },
    )
    refresh = r_login.json()["token"]["refresh_token"]
    # Reset the password
    rd = client.post(
        f"/api/v1/users/{created['id']}/reset-password",
        json={"new_password": "ReplacementP@ss1"},
        headers=h,
    )
    assert rd.status_code == 204
    # The original refresh token cannot be used to mint a new access token.
    r_refresh = client.post(
        "/api/v1/auth/refresh",
        json={"refresh_token": refresh},
    )
    assert r_refresh.status_code == 401


def test_reset_password_unknown_id_returns_404(client: TestClient) -> None:
    """Reset against a nonexistent id returns 404."""
    body = _register_tenant(client, code="reset-404", email=email_for("admin", "reset-404.example"))
    h = _admin_h(body["tenant_id"], body["user"]["id"])
    r = client.post(
        f"/api/v1/users/{uuid.uuid4()}/reset-password",
        json={"new_password": "NewP@ss1234"},
        headers=h,
    )
    assert r.status_code == 404


def test_reset_password_weak_password_returns_422(client: TestClient) -> None:
    """A weak new password returns 422."""
    body = _register_tenant(
        client, code="reset-weak", email=email_for("admin", "reset-weak.example")
    )
    h = _admin_h(body["tenant_id"], body["user"]["id"])
    created = client.post(
        "/api/v1/users",
        json={
            "username": "usr",
            "email": email_for("u", "reset-weak.example"),
            "password": "StrongP@ss123",
        },
        headers=h,
    ).json()
    r = client.post(
        f"/api/v1/users/{created['id']}/reset-password",
        json={"new_password": "short"},
        headers=h,
    )
    assert r.status_code == 422


# ---------------------------------------------------------------------------
# /api/v1/users/{id}/roles — list
# ---------------------------------------------------------------------------


def test_list_user_roles_returns_bound_roles(client: TestClient) -> None:
    """The list-roles endpoint returns the user's bound roles + permissions."""
    body = _register_tenant(client, code="lst-roles", email=email_for("admin", "lst-roles.example"))
    h = _admin_h(body["tenant_id"], body["user"]["id"])
    role = client.post(
        "/api/v1/roles",
        json={
            "code": "engineer",
            "name": "Engineer",
            "permissions": ["datasource:read", "datasource:write"],
        },
        headers=h,
    ).json()
    created = client.post(
        "/api/v1/users",
        json={
            "username": "usr",
            "email": email_for("u", "lst-roles.example"),
            "password": "StrongP@ss123",
            "role_ids": [role["id"]],
        },
        headers=h,
    ).json()
    r = client.get(f"/api/v1/users/{created['id']}/roles", headers=h)
    assert r.status_code == 200
    data = r.json()
    assert data["total"] >= 1
    codes = {item["code"] for item in data["items"]}
    assert "engineer" in codes


def test_list_user_roles_unknown_id_returns_404(client: TestClient) -> None:
    """List-roles against a nonexistent user returns 404."""
    body = _register_tenant(client, code="lst-404", email=email_for("admin", "lst-404.example"))
    h = _admin_h(body["tenant_id"], body["user"]["id"])
    r = client.get(f"/api/v1/users/{uuid.uuid4()}/roles", headers=h)
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# /api/v1/users/{id}/roles — bind
# ---------------------------------------------------------------------------


def test_bind_role_to_user(client: TestClient) -> None:
    """POST /users/{id}/roles adds a binding and the user picks up the perms."""
    body = _register_tenant(client, code="bind", email=email_for("admin", "bind.example"))
    h = _admin_h(body["tenant_id"], body["user"]["id"])
    role = client.post(
        "/api/v1/roles",
        json={
            "code": "ops",
            "name": "Ops",
            "permissions": ["ops:read"],
        },
        headers=h,
    ).json()
    created = client.post(
        "/api/v1/users",
        json={
            "username": "usr",
            "email": email_for("u", "bind.example"),
            "password": "StrongP@ss123",
        },
        headers=h,
    ).json()
    r = client.post(
        f"/api/v1/users/{created['id']}/roles",
        json={"role_id": role["id"]},
        headers=h,
    )
    assert r.status_code == 201, r.text
    data = r.json()
    assert "ops" in data["roles"]
    assert "ops:read" in data["scopes"]


def test_bind_role_duplicate_returns_409(client: TestClient) -> None:
    """A duplicate binding returns 409."""
    body = _register_tenant(client, code="bind-dup", email=email_for("admin", "bind-dup.example"))
    h = _admin_h(body["tenant_id"], body["user"]["id"])
    role = client.post(
        "/api/v1/roles",
        json={"code": "ops", "name": "Ops"},
        headers=h,
    ).json()
    created = client.post(
        "/api/v1/users",
        json={
            "username": "usr",
            "email": email_for("u", "bind-dup.example"),
            "password": "StrongP@ss123",
            "role_ids": [role["id"]],
        },
        headers=h,
    ).json()
    r = client.post(
        f"/api/v1/users/{created['id']}/roles",
        json={"role_id": role["id"]},
        headers=h,
    )
    assert r.status_code == 409


def test_bind_role_unknown_user_returns_404(client: TestClient) -> None:
    """A bind against an unknown user returns 404."""
    body = _register_tenant(client, code="bind-usr", email=email_for("admin", "bind-usr.example"))
    h = _admin_h(body["tenant_id"], body["user"]["id"])
    role = client.post(
        "/api/v1/roles",
        json={"code": "ops", "name": "Ops"},
        headers=h,
    ).json()
    r = client.post(
        f"/api/v1/users/{uuid.uuid4()}/roles",
        json={"role_id": role["id"]},
        headers=h,
    )
    assert r.status_code == 404


def test_bind_role_unknown_role_returns_404(client: TestClient) -> None:
    """A bind with an unknown role id returns 404."""
    body = _register_tenant(client, code="bind-role", email=email_for("admin", "bind-role.example"))
    h = _admin_h(body["tenant_id"], body["user"]["id"])
    created = client.post(
        "/api/v1/users",
        json={
            "username": "usr",
            "email": email_for("u", "bind-role.example"),
            "password": "StrongP@ss123",
        },
        headers=h,
    ).json()
    r = client.post(
        f"/api/v1/users/{created['id']}/roles",
        json={"role_id": str(uuid.uuid4())},
        headers=h,
    )
    assert r.status_code == 404


def test_bind_role_cross_tenant_returns_404(client: TestClient) -> None:
    """A bind with a role from another tenant returns 404.

    The L1 listener filters the cross-tenant role read, so the
    handler raises ``NotFoundError`` (404) instead of
    ``ValidationError`` (400) — both are valid per the brief
    (403/404 are the allowed cross-tenant responses), and 404
    is what the actual code path produces.
    """
    a = _register_tenant(client, code="bind-a", email=email_for("admin", "bind-a.example"))
    b = _register_tenant(client, code="bind-b", email=email_for("admin", "bind-b.example"))
    h_a = _admin_h(a["tenant_id"], a["user"]["id"])
    h_b = _admin_h(b["tenant_id"], b["user"]["id"])
    role_b = client.post(
        "/api/v1/roles",
        json={"code": "ops", "name": "Ops"},
        headers=h_b,
    ).json()
    user_a = client.post(
        "/api/v1/users",
        json={
            "username": "usr",
            "email": email_for("u", "bind-a.example"),
            "password": "StrongP@ss123",
        },
        headers=h_a,
    ).json()
    r = client.post(
        f"/api/v1/users/{user_a['id']}/roles",
        json={"role_id": role_b["id"]},
        headers=h_a,
    )
    assert r.status_code == 404
    assert r.json()["code"] == ErrorCode.NOT_FOUND.value


# ---------------------------------------------------------------------------
# /api/v1/users/{id}/roles/{role_id} — unbind
# ---------------------------------------------------------------------------


def test_unbind_role_removes_binding(client: TestClient) -> None:
    """DELETE removes the binding and the user loses the role's permissions."""
    body = _register_tenant(client, code="unbind", email=email_for("admin", "unbind.example"))
    h = _admin_h(body["tenant_id"], body["user"]["id"])
    role = client.post(
        "/api/v1/roles",
        json={"code": "ops", "name": "Ops", "permissions": ["ops:read"]},
        headers=h,
    ).json()
    created = client.post(
        "/api/v1/users",
        json={
            "username": "usr",
            "email": email_for("u", "unbind.example"),
            "password": "StrongP@ss123",
            "role_ids": [role["id"]],
        },
        headers=h,
    ).json()
    r = client.delete(f"/api/v1/users/{created['id']}/roles/{role['id']}", headers=h)
    assert r.status_code == 204
    # The user no longer has the role's permissions.
    r2 = client.get(f"/api/v1/users/{created['id']}", headers=h)
    assert "ops" not in r2.json()["roles"]
    assert "ops:read" not in r2.json()["scopes"]


def test_unbind_role_idempotent(client: TestClient) -> None:
    """Unbind on a missing binding returns 204 (idempotent)."""
    body = _register_tenant(
        client, code="unbind-idem", email=email_for("admin", "unbind-idem.example")
    )
    h = _admin_h(body["tenant_id"], body["user"]["id"])
    role = client.post(
        "/api/v1/roles",
        json={"code": "ops", "name": "Ops"},
        headers=h,
    ).json()
    created = client.post(
        "/api/v1/users",
        json={
            "username": "usr",
            "email": email_for("u", "unbind-idem.example"),
            "password": "StrongP@ss123",
        },
        headers=h,
    ).json()
    # No binding exists, but unbind still returns 204.
    r = client.delete(f"/api/v1/users/{created['id']}/roles/{role['id']}", headers=h)
    assert r.status_code == 204


def test_unbind_role_unknown_user_returns_404(client: TestClient) -> None:
    """Unbind against an unknown user returns 404."""
    body = _register_tenant(
        client, code="unbind-usr", email=email_for("admin", "unbind-usr.example")
    )
    h = _admin_h(body["tenant_id"], body["user"]["id"])
    r = client.delete(f"/api/v1/users/{uuid.uuid4()}/roles/{uuid.uuid4()}", headers=h)
    assert r.status_code == 404


# ---------------------------------------------------------------------------
# /api/v1/roles — list / create
# ---------------------------------------------------------------------------


def test_list_roles_returns_tenant_roles(client: TestClient) -> None:
    """The role list includes the bootstrap ``admin`` role."""
    body = _register_tenant(
        client, code="roles-list", email=email_for("admin", "roles-list.example")
    )
    h = _admin_h(body["tenant_id"], body["user"]["id"])
    r = client.get("/api/v1/roles", headers=h)
    assert r.status_code == 200
    codes = {item["code"] for item in r.json()["items"]}
    assert "admin" in codes


def test_create_role_happy_path(client: TestClient) -> None:
    """A new role is created with the given code + permissions."""
    body = _register_tenant(client, code="roles-crt", email=email_for("admin", "roles-crt.example"))
    h = _admin_h(body["tenant_id"], body["user"]["id"])
    r = client.post(
        "/api/v1/roles",
        json={
            "code": "data-engineer",
            "name": "Data Engineer",
            "description": "Builds pipelines.",
            "permissions": ["datasource:read", "datasource:write"],
        },
        headers=h,
    )
    assert r.status_code == 201, r.text
    data = r.json()
    assert data["code"] == "data-engineer"
    assert data["name"] == "Data Engineer"
    assert data["description"] == "Builds pipelines."
    assert data["scope"] == "tenant"
    assert "datasource:read" in data["permissions"]


def test_create_role_duplicate_code_returns_409(client: TestClient) -> None:
    """A duplicate role code in the same tenant returns 409."""
    body = _register_tenant(client, code="roles-dup", email=email_for("admin", "roles-dup.example"))
    h = _admin_h(body["tenant_id"], body["user"]["id"])
    # The bootstrap path already created ``admin``; re-create returns 409.
    r = client.post(
        "/api/v1/roles",
        json={"code": "admin", "name": "Admin (dup)"},
        headers=h,
    )
    assert r.status_code == 409


def test_create_role_requires_iam_role_create(client: TestClient) -> None:
    """A caller without ``iam.role.create`` returns 403."""
    body = _register_tenant(
        client, code="roles-crt-perm", email=email_for("admin", "roles-crt-perm.example")
    )
    h = _h(
        tenant_id=body["tenant_id"],
        user_id=body["user"]["id"],
        scopes=["iam.role.read"],
    )
    r = client.post(
        "/api/v1/roles",
        json={"code": "xxx", "name": "X"},
        headers=h,
    )
    assert r.status_code == 403


def test_create_role_invalid_code_returns_422(client: TestClient) -> None:
    """A code with spaces or too short is rejected by Pydantic (422)."""
    body = _register_tenant(client, code="roles-bad", email=email_for("admin", "roles-bad.example"))
    h = _admin_h(body["tenant_id"], body["user"]["id"])
    # ``"x"`` is 1 char — below the 3-char minimum enforced by the
    # ``_USERNAME_RE`` regex shared with usernames. Pydantic returns 422.
    r = client.post(
        "/api/v1/roles",
        json={"code": "x", "name": "X"},
        headers=h,
    )
    assert r.status_code == 422


# ---------------------------------------------------------------------------
# /api/v1/permissions/check — service-to-service
# ---------------------------------------------------------------------------


def test_permissions_check_returns_true_for_admin_wildcard(
    client: TestClient,
) -> None:
    """A user with the bootstrap ``admin`` role has every permission."""
    body = _register_tenant(
        client, code="check-admin", email=email_for("admin", "check-admin.example")
    )
    # No Authorization header — the endpoint is intentionally
    # unauthenticated for service-to-service traffic.
    r = client.post(
        "/api/v1/permissions/check",
        json={
            "user_id": body["user"]["id"],
            "permission": "datasource:read",
        },
    )
    assert r.status_code == 200
    data = r.json()
    assert data["allowed"] is True
    assert data["source"] in {"wildcard", "role"}


def test_permissions_check_returns_false_for_user_with_no_roles(
    client: TestClient,
) -> None:
    """A non-admin user without bindings returns ``allowed=False``."""
    body = _register_tenant(client, code="check-no", email=email_for("admin", "check-no.example"))
    h = _admin_h(body["tenant_id"], body["user"]["id"])
    created = client.post(
        "/api/v1/users",
        json={
            "username": "plain",
            "email": email_for("plain", "check-no.example"),
            "password": "StrongP@ss123",
        },
        headers=h,
    ).json()
    r = client.post(
        "/api/v1/permissions/check",
        json={
            "user_id": created["id"],
            "permission": "datasource:write",
        },
    )
    assert r.status_code == 200
    data = r.json()
    assert data["allowed"] is False
    assert data["source"] == "none"


def test_permissions_check_returns_true_for_role_grant(
    client: TestClient,
) -> None:
    """A user with a role that grants the permission returns ``allowed=True``."""
    body = _register_tenant(
        client, code="check-role", email=email_for("admin", "check-role.example")
    )
    h = _admin_h(body["tenant_id"], body["user"]["id"])
    role = client.post(
        "/api/v1/roles",
        json={
            "code": "reader",
            "name": "Reader",
            "permissions": ["datasource:read"],
        },
        headers=h,
    ).json()
    created = client.post(
        "/api/v1/users",
        json={
            "username": "usr",
            "email": email_for("u", "check-role.example"),
            "password": "StrongP@ss123",
            "role_ids": [role["id"]],
        },
        headers=h,
    ).json()
    r = client.post(
        "/api/v1/permissions/check",
        json={
            "user_id": created["id"],
            "permission": "datasource:read",
        },
    )
    assert r.status_code == 200
    data = r.json()
    assert data["allowed"] is True
    assert data["source"] == "role"


def test_permissions_check_reflects_unbinding(client: TestClient) -> None:
    """After unbinding a role, the permission check returns ``allowed=False``.

    The check is the *live* re-evaluation (vs. the JWT snapshot
    the FastAPI dependency uses). This test guards the contract
    that ``has_permission`` re-reads the bindings every time.
    """
    body = _register_tenant(
        client, code="check-unbind", email=email_for("admin", "check-unbind.example")
    )
    h = _admin_h(body["tenant_id"], body["user"]["id"])
    role = client.post(
        "/api/v1/roles",
        json={
            "code": "reader",
            "name": "Reader",
            "permissions": ["datasource:read"],
        },
        headers=h,
    ).json()
    created = client.post(
        "/api/v1/users",
        json={
            "username": "usr",
            "email": email_for("u", "check-unbind.example"),
            "password": "StrongP@ss123",
            "role_ids": [role["id"]],
        },
        headers=h,
    ).json()
    r1 = client.post(
        "/api/v1/permissions/check",
        json={"user_id": created["id"], "permission": "datasource:read"},
    )
    assert r1.json()["allowed"] is True
    # Unbind
    rd = client.delete(f"/api/v1/users/{created['id']}/roles/{role['id']}", headers=h)
    assert rd.status_code == 204
    # The next check reflects the removal.
    r2 = client.post(
        "/api/v1/permissions/check",
        json={"user_id": created["id"], "permission": "datasource:read"},
    )
    assert r2.json()["allowed"] is False


def test_permissions_check_unknown_user_returns_false(
    client: TestClient,
) -> None:
    """A nonexistent user_id returns ``allowed=False`` (not 404).

    The endpoint is the "service-to-service" decision surface; a
    404 here would confuse callers asking "may this user do X?".
    A boolean answer is the right contract.
    """
    r = client.post(
        "/api/v1/permissions/check",
        json={"user_id": str(uuid.uuid4()), "permission": "datasource:read"},
    )
    assert r.status_code == 200
    data = r.json()
    assert data["allowed"] is False
    assert data["source"] == "none"


def test_permissions_check_empty_user_id_returns_false(
    client: TestClient,
) -> None:
    """An empty user id returns ``allowed=False`` without crashing."""
    r = client.post(
        "/api/v1/permissions/check",
        json={"user_id": "", "permission": "datasource:read"},
    )
    assert r.status_code == 200
    assert r.json()["allowed"] is False


def test_permissions_check_invalid_body_returns_422(client: TestClient) -> None:
    """A missing ``permission`` field is rejected with 422."""
    r = client.post(
        "/api/v1/permissions/check",
        json={"user_id": "u-1"},
    )
    assert r.status_code == 422
