"""RBAC permission check service for the IAM service.

This module is the authoritative place where a user's *effective* permission
set is computed. The platform's :mod:`aidp_auth.dependencies` module exposes
a FastAPI dependency that does a similar check *against the JWT claims*
(``user.scopes`` baked into the access token), but those claims are a
**snapshot** taken at token-issue time — they cannot reflect role / permission
changes that happened after the token was minted. This module is the live
re-evaluation path: it re-reads the user's bindings + role permissions from
the database and answers the question "does this user have this permission
*right now*?".

Two call sites are supported:

- :func:`has_permission` — the boolean decision used by the internal
  ``/api/v1/permissions/check`` endpoint (called by other services that need
  a permission check at request time, not at token-issue time).
- :func:`collect_user_permissions` — the full union of permissions, used by
  the user CRUD handlers to project roles + scopes onto the public user
  response.

Algorithm
---------

For a given ``(user_id, tenant_id)``:

1. Open a DB session, set the tenant context (the L1 listener auto-filters
   by ``tenant_id``).
2. Load all :class:`aidp_iam.models.UserRoleBinding` rows for the user whose
   ``expires_at`` is ``NULL`` or in the future.
3. For each binding, union the bound :class:`aidp_iam.models.Role` 's
   ``permissions`` list into a single :class:`set`.
4. The wildcard ``"*"`` is preserved as a member of the set; callers can
   use :func:`_has_wildcard` to short-circuit.

The check is **OR**-based: the user holds the permission if it appears in
the union OR if the union contains ``"*"``.

Note
----

This module does not call :func:`aidp_auth.dependencies._user_has_permission`
— that helper operates on a frozen :class:`aidp_auth.jwt.CurrentUser` whose
scopes are a JWT snapshot, not a live DB read. The two paths produce the
same answer at token-issue time but diverge as soon as an admin revokes a
binding; this module always wins for "right now" decisions.
"""

from __future__ import annotations

from dataclasses import dataclass

from aidp_common.errors import ForbiddenError
from aidp_db.session import get_session
from aidp_db.tenant import reset_tenant_context, set_tenant_context
from sqlalchemy import select
from sqlalchemy.orm import Session as SqlSession

from aidp_iam.models import Role, UserRoleBinding

#: Wildcard permission string. A role with ``"*"`` in its ``permissions``
#: list grants every permission — the equivalent of a "superuser" role.
WILDCARD: str = "*"


@dataclass(frozen=True)
class PermissionDecision:
    """The result of a permission check.

    Carries both the boolean answer and a ``source`` string so callers can
    log / trace how the decision was reached. The ``source`` field is for
    observability only — do not gate business logic on it.
    """

    allowed: bool
    #: One of ``"wildcard"`` (role carries ``"*"``), ``"role"`` (a bound
    #: role's ``permissions`` list contains the requested permission),
    #: ``"none"`` (user has no matching role binding / no roles at all).
    source: str
    #: The user's effective permission set, useful for logging / debug.
    permissions: frozenset[str]


def collect_user_permissions(
    *,
    user_id: str,
    tenant_id: str,
) -> frozenset[str]:
    """Return the union of all active role permissions for *user_id*.

    Iterates every :class:`UserRoleBinding` for the user, takes the bound
    :class:`Role` 's ``permissions`` list, and unions the result. The
    wildcard ``"*"`` is preserved as a member; callers can use
    :func:`has_permission` for the boolean decision.

    The function is the live equivalent of the ``scopes`` field embedded in
    the user's JWT. Bindings with an ``expires_at`` in the past are skipped
    so an expired role does not continue to grant permissions.

    Args:
        user_id: The per-tenant user id (UUID4 string).
        tenant_id: The tenant the user belongs to. Used to bind the
            request-scoped tenant context so the L1 listener can do its
            job; also passed to the L1 ``WHERE tenant_id = :tid`` filter
            as a defense in depth.

    Returns:
        A :class:`frozenset` of permission strings. Empty when the user
        has no active role bindings.
    """
    if not user_id or not tenant_id:
        return frozenset()

    ctx_token = set_tenant_context(tenant_id)
    try:
        with get_session() as session:
            return _collect_user_permissions_locked(
                session=session, user_id=user_id, tenant_id=tenant_id
            )
    finally:
        reset_tenant_context(ctx_token)


def _collect_user_permissions_locked(
    *,
    session: SqlSession,
    user_id: str,
    tenant_id: str,
) -> frozenset[str]:
    """Body of :func:`collect_user_permissions`; assumes tenant context is bound.

    Split out so the caller can guarantee the tenant context is reset on
    exit, regardless of which path raised.
    """
    rows = session.execute(
        select(UserRoleBinding, Role)
        .join(Role, UserRoleBinding.role_id == Role.id)
        .where(UserRoleBinding.user_id == user_id)
        .where(UserRoleBinding.tenant_id == tenant_id)
    ).all()

    permissions: set[str] = set()
    for binding, role in rows:
        if binding.is_expired:
            # Skip expired bindings — they no longer grant access.
            continue
        if role is None:  # pragma: no cover - FK guarantees a row exists
            continue
        for perm in role.permissions or []:
            permissions.add(perm)
    return frozenset(permissions)


def has_permission(
    *,
    user_id: str,
    tenant_id: str,
    permission: str,
) -> PermissionDecision:
    """Decide whether *user_id* currently has *permission*.

    The check is the live re-evaluation of the JWT-snapshot check used by
    :func:`aidp_auth.dependencies._user_has_permission`. The function is
    the one called by ``POST /api/v1/permissions/check`` (the internal
    service-to-service permission endpoint).

    Args:
        user_id: The per-tenant user id whose permission is being checked.
        tenant_id: The tenant the user belongs to. Used for L1 isolation.
        permission: The permission string to check. Must be non-empty;
            passing ``""`` raises :class:`ValueError` so a misconfigured
            caller cannot accidentally grant every check.

    Returns:
        A :class:`PermissionDecision` carrying the boolean answer, the
        ``source`` describing how the decision was reached, and the
        user's effective permission set.

    Raises:
        ValueError: When *permission* is empty.
    """
    if not permission:
        raise ValueError("permission must be a non-empty string")
    if not user_id:
        # No user = no permissions. The endpoint translates this to a
        # 404 via the API layer.
        return PermissionDecision(allowed=False, source="none", permissions=frozenset())
    if not tenant_id:
        # Same as above — without a tenant we cannot safely query.
        return PermissionDecision(allowed=False, source="none", permissions=frozenset())

    permissions = collect_user_permissions(user_id=user_id, tenant_id=tenant_id)
    if WILDCARD in permissions:
        return PermissionDecision(allowed=True, source="wildcard", permissions=permissions)
    if permission in permissions:
        return PermissionDecision(allowed=True, source="role", permissions=permissions)
    return PermissionDecision(allowed=False, source="none", permissions=permissions)


def require_permission_for_user(
    *,
    user_id: str,
    tenant_id: str,
    permission: str,
) -> PermissionDecision:
    """Convenience wrapper: raise :class:`ForbiddenError` when the check fails.

    Used by API handlers that prefer exception-flow over a boolean return
    (so the platform's unified error envelope renders a 403 in the usual
    way instead of an ``{"allowed": false}`` body). Other call sites use
    the pure :func:`has_permission` and act on the boolean directly.

    Args:
        user_id: The per-tenant user id whose permission is being checked.
        tenant_id: The tenant the user belongs to.
        permission: The permission string to check.

    Returns:
        The :class:`PermissionDecision` from :func:`has_permission` when
        the user is allowed.

    Raises:
        ValueError: When *permission* is empty.
        ForbiddenError: When the user is not allowed.
    """
    decision = has_permission(user_id=user_id, tenant_id=tenant_id, permission=permission)
    if not decision.allowed:
        # We don't echo the user's permission set in the error details
        # to avoid leaking the full role/permission layout to the caller.
        raise ForbiddenError(f"user {user_id!r} lacks permission {permission!r}")
    return decision


__all__ = [
    "WILDCARD",
    "PermissionDecision",
    "collect_user_permissions",
    "has_permission",
    "require_permission_for_user",
]
