"""FastAPI dependency-injection helpers for AIDP auth.

Two dependency factories live here:

- :data:`current_user` — extract the ``Authorization: Bearer <token>``
  header, decode the JWT, and return a :class:`CurrentUser`. As a side
  effect it binds the request-scoped tenant context via
  :func:`aidp_db.tenant.set_tenant_context`, so any downstream ORM
  select in the same request gets the L1 ``WHERE tenant_id = :tid``
  filter automatically (per the platform global constraint on
  mandatory tenant isolation).

- :func:`require_permission` — build a per-permission dependency that
  resolves :data:`current_user` and checks the caller's scopes. Used
  as ``Depends(require_permission("datasource:read"))`` on a route
  handler.

Errors raised by this module all flow through
:class:`aidp_common.errors.AppError`, which downstream services
translate into the platform's unified error envelope
(``{"code", "message", "details", "trace_id"}``) at the FastAPI
exception-handler boundary. The dependency layer itself never raises
``fastapi.HTTPException`` — that translation is the service's job.

Implementation note: both dependencies are ``async def`` so FastAPI
runs them on the event loop (the same :class:`asyncio.Task` as the
route handler). This matters because the L1 tenant filter reads from
a :class:`contextvars.ContextVar` set by :data:`current_user`; if the
dependency ran in a worker thread, the binding would not propagate
to the handler body (sync dependencies are dispatched via
``anyio.to_thread.run_sync``, which uses a fresh thread per call).
"""

from __future__ import annotations

from collections.abc import Callable, Coroutine
from typing import Any, Final

from aidp_common.errors import ForbiddenError, UnauthorizedError
from aidp_db import tenant as _tenant
from fastapi import Request

from aidp_auth.jwt import (
    CurrentUser,
    TokenType,
    current_user_from_token,
    decode_token,
)

#: The conventional ``Authorization`` header name (case-insensitive in
#: HTTP, but FastAPI normalizes it to lowercase). Used by
#: :func:`_extract_bearer_token`.
_AUTH_HEADER: Final = "authorization"

#: The conventional ``Bearer`` scheme prefix. Anything else (e.g. Basic)
#: is rejected with 401 — only opaque JWTs are valid AIDP credentials.
_BEARER_PREFIX: Final = "bearer "


# ---------------------------------------------------------------------------
# Header parsing
# ---------------------------------------------------------------------------


def _extract_bearer_token(request: Request) -> str:
    """Pull the raw JWT out of the ``Authorization`` header.

    Returns:
        The token string (no ``"Bearer "`` prefix).

    Raises:
        UnauthorizedError: When the header is missing, not ``Bearer``,
            or carries an empty token.
    """
    raw = request.headers.get(_AUTH_HEADER)
    if raw is None:
        raise UnauthorizedError("missing Authorization header")
    # Be lenient on the scheme capitalization: ``Bearer`` / ``bearer``
    # / ``BEARER`` all work. Anything else is a 401.
    if not raw.lower().startswith(_BEARER_PREFIX):
        raise UnauthorizedError("Authorization scheme must be Bearer")
    token = raw[len(_BEARER_PREFIX) :].strip()
    if not token:
        raise UnauthorizedError("Bearer token is empty")
    return token


# ---------------------------------------------------------------------------
# current_user dependency
# ---------------------------------------------------------------------------


async def current_user(request: Request) -> CurrentUser:
    """FastAPI dependency: decode the bearer token, return the caller.

    The dependency:

    1. Extracts the ``Authorization: Bearer <token>`` header.
    2. Decodes the JWT (signature + expiry + claim shape verified
       by :func:`aidp_auth.jwt.decode_token`).
    3. Refuses refresh tokens — only ``access`` tokens are valid
       Authorization credentials.
    4. Projects the verified :class:`TokenClaims` into a
       :class:`CurrentUser` (drops JWT internals like ``jti`` /
       ``iat`` / ``exp``).
    5. Binds the request-scoped tenant context via
       :func:`aidp_db.tenant.set_tenant_context` so any subsequent
       ORM select gets the L1 ``WHERE tenant_id = :tid`` filter.
       The binding lives in the request's :class:`asyncio.Task`
       :class:`contextvars.Context`; FastAPI creates a fresh context
       per request so concurrent requests never see each other's
       tenant binding.

    Args:
        request: The active :class:`fastapi.Request`. Supplied by
            FastAPI's dependency-injection engine — call sites
            just write ``user: CurrentUser = Depends(current_user)``.

    Returns:
        A frozen :class:`CurrentUser` exposing ``tenant_id`` /
        ``user_id`` / ``roles`` / ``scopes``.

    Raises:
        UnauthorizedError: When the header is missing / malformed,
            the token fails to decode, the signature is invalid, the
            token is expired, or the token is a refresh token.
    """
    token = _extract_bearer_token(request)
    claims = decode_token(token)
    if claims.token_type is not TokenType.ACCESS:
        # Refresh tokens are not valid resource-server credentials.
        # Surfacing this as 401 (not 403) keeps the contract simple:
        # either the request authenticates, or it does not.
        raise UnauthorizedError("refresh tokens are not valid Authorization credentials")

    user = current_user_from_token(claims)

    # Bind the tenant for the rest of the request. ``set_tenant_context``
    # returns a Token the caller could use to ``reset_tenant_context``
    # after the request, but FastAPI's per-request contextvars.Context
    # already isolates concurrent requests — by the time the next
    # request enters this code path, the ContextVar is automatically
    # reset. We expose the token via ``request.state`` for the rare
    # case where a service wants to manage the binding explicitly.
    tenant_token = _tenant.set_tenant_context(user.tenant_id)
    request.state.tenant_token = tenant_token

    return user


# ---------------------------------------------------------------------------
# require_permission dependency factory
# ---------------------------------------------------------------------------


def _user_has_permission(user: CurrentUser, permission: str) -> bool:
    """Decide whether *user* may perform *permission*.

    A permission is granted if any of the following holds:

    - ``permission`` appears in ``user.scopes``.
    - ``"*"`` appears in ``user.scopes`` (wildcard grant).
    - ``"admin"`` appears in ``user.roles`` (platform-wide bypass for
      operators / on-call).

    The wildcard + admin checks are intentional: a service that
    only ever uses the dependency does not need to know about the
    exact scope name. Operators are always allowed in.
    """
    if not permission:
        # Defensive: a permission of ``""`` would otherwise be granted
        # by the ``in scopes`` check against an empty scopes list,
        # which is almost certainly a bug at the call site.
        return False
    if "*" in user.scopes:
        return True
    if "admin" in user.roles:
        return True
    return permission in user.scopes


def require_permission(permission: str) -> Callable[..., Coroutine[Any, Any, CurrentUser]]:
    """Build a FastAPI dependency that enforces *permission*.

    The returned callable is meant to be used as
    ``Depends(require_permission("datasource:write"))`` on a route
    handler. FastAPI resolves :data:`current_user` for the request,
    checks the scope, and returns the user to the route (so the
    handler can also access the identity if needed).

    On a missing scope the dependency raises
    :class:`aidp_common.errors.ForbiddenError` (``code=FORBIDDEN``,
    ``status=403``). The handler is never entered.

    On a missing / invalid bearer token the underlying
    :data:`current_user` raises
    :class:`aidp_common.errors.UnauthorizedError`
    (``code=UNAUTHORIZED``, ``status=401``) — i.e. the 401 path
    always wins over 403.

    Args:
        permission: The required scope string, e.g.
            ``"datasource:read"``. Matched exactly against
            ``user.scopes`` (or satisfied by ``"*"`` / ``"admin"``).

    Returns:
        An async dependency function suitable for ``Depends(...)``.

    Raises:
        ValueError: When *permission* is empty.
    """
    if not permission:
        raise ValueError("permission must be a non-empty string")

    async def _check_permission(request: Request) -> CurrentUser:
        # Reuse ``current_user`` so the Authorization header is
        # parsed exactly once, and the tenant context binding
        # happens once.
        user = await current_user(request)
        if not _user_has_permission(user, permission):
            raise ForbiddenError(f"missing required permission: {permission}")
        return user

    # Set a sensible ``__name__`` so FastAPI's debug output
    # (``/docs``, error messages) is readable.
    safe_name = permission.replace(":", "_").replace(".", "_")
    _check_permission.__name__ = f"require_permission_{safe_name}"
    _check_permission.__qualname__ = _check_permission.__name__
    return _check_permission


__all__ = ["current_user", "require_permission"]
