"""Tests for ``aidp_auth.dependencies``.

These tests use ``fastapi.testclient.TestClient`` to drive the
dependency-injection machinery end-to-end:

- ``current_user`` extracts a ``Bearer`` token from the ``Authorization``
  header, decodes it, and returns a :class:`CurrentUser` exposing
  ``tenant_id`` / ``user_id`` / ``roles`` / ``scopes``.
- ``current_user`` binds the request-scoped tenant context
  (``aidp_db.tenant.set_tenant_context``) so downstream ORM queries
  get the L1 filter.
- A missing ``Authorization`` header → 401.
- A malformed token → 401.
- An expired token → 401.
- A token with the wrong signature → 401.
- ``require_permission(perm)`` accepts the call when the user's
  ``scopes`` include ``perm``.
- ``require_permission(perm)`` accepts the call when ``scopes``
  contains the wildcard ``"*"``.
- ``require_permission(perm)`` accepts the call when ``roles``
  contains ``"admin"``.
- ``require_permission(perm)`` rejects the call (403) when none of
  the above hold.
- The unified error envelope ``{code, message, details, trace_id}`` is
  used for both 401 and 403 responses.

We also exercise a small FastAPI app that uses both dependencies to
make sure FastAPI's wiring works as expected.
"""

from __future__ import annotations

import time
from collections.abc import Generator
from datetime import timedelta
from typing import Any

import jwt as pyjwt
import pytest
from aidp_auth.dependencies import current_user, require_permission
from aidp_auth.jwt import (
    CurrentUser,
    create_access_token,
    create_refresh_token,
    decode_token,
)
from aidp_common import config as cfg
from aidp_common.errors import AppError, ForbiddenError, UnauthorizedError
from aidp_db import tenant as tenant_module
from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

# ---------------------------------------------------------------------------
# Env bootstrap — ``aidp_common.config.Settings`` requires these.
# ---------------------------------------------------------------------------

_TEST_ENV: dict[str, str] = {
    "AIDP_DB_URL": "postgresql://localhost:5432/aidp_test",
    "AIDP_REDIS_URL": "redis://localhost:6379/0",
    "AIDP_SERVICE_NAME": "aidp-auth-test",
    "AIDP_KAFKA_BROKERS": "localhost:9092",
    "AIDP_JWT_SECRET": "test-secret-for-jwt-signing-do-not-use-in-prod-32b+",
}


@pytest.fixture(autouse=True)
def _aidp_auth_env(monkeypatch: pytest.MonkeyPatch) -> Generator[None, None, None]:
    """Populate the AIDP_* env for the duration of each test."""
    for key, value in _TEST_ENV.items():
        monkeypatch.setenv(key, value)
    cfg.reset_settings_cache()
    try:
        yield
    finally:
        cfg.reset_settings_cache()


@pytest.fixture
def tenant_clear() -> None:
    """Marker fixture: this test cares about the tenant context.

    The :data:`aidp_auth.dependencies.current_user` dependency calls
    :func:`aidp_db.tenant.set_tenant_context` as a side effect, which
    binds the L1 tenant for the rest of the request. pytest does not
    give each test a fresh :class:`contextvars.Context`, so the
    binding can leak into the next test. Tests that need to assert
    on the binding either capture it from inside the request (see
    ``/me``) or pair ``set_tenant_context`` / ``reset_tenant_context``
    themselves.

    This fixture exists purely as a documentation aid: the test
    signature reads "I care about the tenant context" and gets a
    clean per-test assertion point.
    """
    return


# ---------------------------------------------------------------------------
# Test app
# ---------------------------------------------------------------------------


def _build_app() -> FastAPI:
    """Build a small FastAPI app exercising the dependencies.

    The app installs the platform's canonical :class:`AppError` →
    :class:`JSONResponse` exception handler so the dependency layer's
    :class:`UnauthorizedError` / :class:`ForbiddenError` show up on
    the wire as the platform's unified error envelope
    (``{"code","message","details","trace_id"}``).

    The handlers are ``async def`` so they share the same
    :class:`asyncio.Task` (and therefore the same
    :class:`contextvars.Context`) as the ``async def`` dependencies.
    This is the production wiring — sync handlers would break the
    tenant context propagation documented in
    :mod:`aidp_auth.dependencies`.
    """
    app = FastAPI()

    @app.exception_handler(AppError)
    def _handle_app_error(_request: Any, exc: AppError) -> JSONResponse:
        # The trace_id here is a fake; a real service wires in
        # ``aidp_common.tracing.get_trace_id()``.
        return JSONResponse(
            status_code=exc.status,
            content=exc.to_dict(trace_id="00000000000000000000000000000000"),
        )

    @app.get("/me")
    async def whoami(user: CurrentUser = Depends(current_user)) -> dict[str, Any]:
        # ``current_user`` should have also bound the tenant context.
        return {
            "tenant": user.tenant_id,
            "user": user.user_id,
            "roles": list(user.roles),
            "scopes": list(user.scopes),
            "context_tenant": tenant_module.get_tenant_id(),
        }

    @app.get("/datasources")
    async def list_datasources(
        _user: CurrentUser = Depends(require_permission("datasource:read")),
    ) -> dict[str, str]:
        return {"ok": "yes"}

    @app.post("/datasources")
    async def create_datasource(
        _user: CurrentUser = Depends(require_permission("datasource:write")),
    ) -> dict[str, str]:
        return {"created": "yes"}

    @app.get("/wildcard")
    async def wildcard(
        _user: CurrentUser = Depends(require_permission("anything:here")),
    ) -> dict[str, str]:
        return {"ok": "yes"}

    @app.get("/admin-only")
    async def admin_only(
        _user: CurrentUser = Depends(require_permission("admin:godmode")),
    ) -> dict[str, str]:
        return {"ok": "yes"}

    return app


# ---------------------------------------------------------------------------
# current_user — happy path
# ---------------------------------------------------------------------------


def test_current_user_returns_decoded_user(tenant_clear: None) -> None:
    app = _build_app()
    client = TestClient(app)
    token = create_access_token(
        tenant_id="tenant-1",
        user_id="user-1",
        roles=["data_engineer"],
        scopes=["datasource:read"],
    )
    resp = client.get("/me", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["tenant"] == "tenant-1"
    assert body["user"] == "user-1"
    assert body["roles"] == ["data_engineer"]
    assert body["scopes"] == ["datasource:read"]
    # The dependency also bound the request-scoped tenant context.
    assert body["context_tenant"] == "tenant-1"


def test_current_user_empty_scopes_and_roles(tenant_clear: None) -> None:
    app = _build_app()
    client = TestClient(app)
    token = create_access_token(tenant_id="tenant-1", user_id="user-1")
    resp = client.get("/me", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["roles"] == []
    assert body["scopes"] == []
    assert body["context_tenant"] == "tenant-1"


# ---------------------------------------------------------------------------
# current_user — failure paths
# ---------------------------------------------------------------------------


def test_current_user_missing_authorization_header() -> None:
    app = _build_app()
    client = TestClient(app)
    resp = client.get("/me")
    assert resp.status_code == 401
    body = resp.json()
    # Unified error format from the global constraint.
    assert body["code"] == "UNAUTHORIZED"
    assert "missing" in body["message"].lower() or "credentials" in body["message"].lower()
    assert "details" in body
    assert "trace_id" in body


def test_current_user_wrong_authorization_scheme() -> None:
    app = _build_app()
    client = TestClient(app)
    resp = client.get("/me", headers={"Authorization": "Basic dXNlcjpwYXNz"})
    assert resp.status_code == 401
    body = resp.json()
    assert body["code"] == "UNAUTHORIZED"


def test_current_user_malformed_bearer_value() -> None:
    app = _build_app()
    client = TestClient(app)
    resp = client.get("/me", headers={"Authorization": "Bearer not-a-jwt"})
    assert resp.status_code == 401
    body = resp.json()
    assert body["code"] == "UNAUTHORIZED"


def test_current_user_expired_token() -> None:
    app = _build_app()
    client = TestClient(app)
    expired = create_access_token(
        tenant_id="tenant-1",
        user_id="user-1",
        roles=[],
        scopes=[],
        expires_delta=timedelta(seconds=-5),
    )
    resp = client.get("/me", headers={"Authorization": f"Bearer {expired}"})
    assert resp.status_code == 401
    assert resp.json()["code"] == "UNAUTHORIZED"


def test_current_user_token_signed_with_wrong_secret() -> None:
    app = _build_app()
    client = TestClient(app)
    # Hand-craft a token with a wrong secret.
    now = int(time.time())
    payload = {
        "tenant_id": "tenant-1",
        "user_id": "user-1",
        "roles": [],
        "scopes": [],
        "token_type": "access",
        "jti": "manual",
        "iat": now,
        "exp": now + 60,
    }
    forged = pyjwt.encode(payload, "totally-different-secret-but-32b!!", algorithm="HS256")
    resp = client.get("/me", headers={"Authorization": f"Bearer {forged}"})
    assert resp.status_code == 401
    assert resp.json()["code"] == "UNAUTHORIZED"


def test_current_user_rejects_refresh_token() -> None:
    """Refresh tokens are not valid Authorization credentials."""
    app = _build_app()
    client = TestClient(app)
    refresh = create_refresh_token(tenant_id="tenant-1", user_id="user-1")
    resp = client.get("/me", headers={"Authorization": f"Bearer {refresh}"})
    assert resp.status_code == 401
    # The decoder should refuse because the access/refresh distinction
    # is encoded in ``token_type``. (Even if a refresh token were
    # structurally valid, the dependency enforces access-token-only.)
    assert resp.json()["code"] == "UNAUTHORIZED"


def test_current_user_authorization_header_case_insensitive() -> None:
    """FastAPI's headers are case-insensitive; the dependency should be too."""
    app = _build_app()
    client = TestClient(app)
    token = create_access_token(tenant_id="tenant-1", user_id="user-1")
    resp = client.get("/me", headers={"authorization": f"Bearer {token}"})
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# require_permission — happy path
# ---------------------------------------------------------------------------


def test_require_permission_accepts_exact_scope(tenant_clear: None) -> None:
    app = _build_app()
    client = TestClient(app)
    token = create_access_token(
        tenant_id="tenant-1",
        user_id="user-1",
        roles=[],
        scopes=["datasource:read"],
    )
    resp = client.get("/datasources", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 200
    assert resp.json() == {"ok": "yes"}


def test_require_permission_accepts_wildcard_scope(tenant_clear: None) -> None:
    """``"*"`` in scopes bypasses every per-permission check."""
    app = _build_app()
    client = TestClient(app)
    token = create_access_token(
        tenant_id="tenant-1",
        user_id="user-1",
        roles=[],
        scopes=["*"],
    )
    resp = client.get("/datasources", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 200
    resp = client.post("/datasources", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 200
    resp = client.get("/wildcard", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 200


def test_require_permission_accepts_admin_role(tenant_clear: None) -> None:
    """``"admin"`` in roles is the platform-wide bypass."""
    app = _build_app()
    client = TestClient(app)
    token = create_access_token(
        tenant_id="tenant-1",
        user_id="user-1",
        roles=["admin"],
        scopes=[],
    )
    resp = client.get("/admin-only", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# require_permission — failure path
# ---------------------------------------------------------------------------


def test_require_permission_rejects_missing_scope() -> None:
    app = _build_app()
    client = TestClient(app)
    token = create_access_token(
        tenant_id="tenant-1",
        user_id="user-1",
        roles=[],
        scopes=["datasource:read"],
    )
    resp = client.post("/datasources", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 403
    body = resp.json()
    assert body["code"] == "FORBIDDEN"
    assert "datasource:write" in body["message"]


def test_require_permission_rejects_no_scopes_no_admin() -> None:
    app = _build_app()
    client = TestClient(app)
    token = create_access_token(
        tenant_id="tenant-1",
        user_id="user-1",
        roles=["data_engineer"],  # not admin
        scopes=["unrelated:scope"],
    )
    resp = client.get("/datasources", headers={"Authorization": f"Bearer {token}"})
    assert resp.status_code == 403
    assert resp.json()["code"] == "FORBIDDEN"


def test_require_permission_propagates_401_when_token_missing() -> None:
    """Missing Authorization header → 401 (not 403)."""
    app = _build_app()
    client = TestClient(app)
    resp = client.get("/datasources")
    assert resp.status_code == 401
    assert resp.json()["code"] == "UNAUTHORIZED"


# ---------------------------------------------------------------------------
# Exception classes (used by FastAPI exception handlers downstream)
# ---------------------------------------------------------------------------


def test_unauthorized_error_subclass() -> None:
    """``UnauthorizedError`` is catchable as ``AppError``."""
    err = UnauthorizedError("nope")
    assert err.status == 401
    assert err.code.value == "UNAUTHORIZED"


def test_forbidden_error_subclass() -> None:
    err = ForbiddenError("nope")
    assert err.status == 403
    assert err.code.value == "FORBIDDEN"


# ---------------------------------------------------------------------------
# Direct function call (no FastAPI wrapper) — for completeness
# ---------------------------------------------------------------------------


def test_current_user_direct_call_with_token() -> None:
    """``current_user`` is an async callable; it can be awaited directly
    with a hand-built ``Request`` for unit tests that don't need the
    full FastAPI app.

    Note: the tenant context binding is scoped to the asyncio task.
    When the awaited coroutine returns and the loop tears down, the
    binding is released. To assert the binding was set, we capture
    it from inside the coroutine before returning.
    """
    import asyncio

    from fastapi import Request
    from starlette.requests import Request as StarletteRequest

    token = create_access_token(
        tenant_id="tenant-1",
        user_id="user-1",
        roles=["admin"],
        scopes=["datasource:read"],
    )

    # Build a real ``Request`` via ASGI scope. We don't need a full
    # body, just headers.
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/me",
        "headers": [(b"authorization", f"Bearer {token}".encode())],
        "query_string": b"",
    }
    request = StarletteRequest(scope)
    assert isinstance(request, Request)

    async def _drive() -> tuple[CurrentUser, str | None]:
        user = await current_user(request)
        # Capture the tenant id while still inside the request's
        # asyncio context.
        return user, tenant_module.get_tenant_id()

    user, captured_tenant = asyncio.run(_drive())
    assert user.tenant_id == "tenant-1"
    assert user.user_id == "user-1"
    assert "admin" in user.roles
    assert "datasource:read" in user.scopes
    # The binding was visible inside the coroutine.
    assert captured_tenant == "tenant-1"


def test_decode_token_round_trip_with_dependencies() -> None:
    """A token issued by ``create_access_token`` decodes via the same
    secret the dependency layer uses (no surprise decoupling)."""
    token = create_access_token(tenant_id="tenant-1", user_id="user-1", roles=[], scopes=[])
    claims = decode_token(token)
    assert claims.tenant_id == "tenant-1"


# ---------------------------------------------------------------------------
# Verify that the platform's exception types survive an HTTPException
# translation — a downstream app would install a handler that converts
# AppError to an HTTPException. Here we just ensure the types round-trip.
# ---------------------------------------------------------------------------


def test_unauthorized_error_constructible_inside_dependency() -> None:
    """The dependency raises ``UnauthorizedError``; nothing else."""
    # We can't directly observe the raised type through FastAPI's 401
    # response (the app needs a handler to translate), but the response
    # body's ``code`` field proves the dependency went through the
    # error pipeline. Verified above in ``test_current_user_*`` tests.
    err = UnauthorizedError("bad token")
    assert err.status == 401
    # Convert to dict — same path a JSON response would use.
    payload = err.to_dict(trace_id="abc-123")
    assert payload == {
        "code": "UNAUTHORIZED",
        "message": "bad token",
        "details": {},
        "trace_id": "abc-123",
    }


def test_http_exception_distinct_from_app_error() -> None:
    """Sanity: ``HTTPException`` is a FastAPI primitive, not our error.

    The dependency layer never raises ``HTTPException``; downstream
    services install an exception handler that converts ``AppError``
    into ``HTTPException`` (or ``JSONResponse``) at the boundary. This
    test documents the boundary: a handler may catch ``UnauthorizedError``
    (our error) but should *not* catch ``HTTPException`` (FastAPI's).
    """
    with pytest.raises(UnauthorizedError):
        raise UnauthorizedError("x")
    with pytest.raises(HTTPException):
        raise HTTPException(status_code=418, detail="teapot")
