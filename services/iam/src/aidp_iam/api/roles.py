"""HTTP routes for the IAM ``/api/v1/roles`` + ``/api/v1/permissions`` surface.

The module is the transport adapter for the role-CRUD endpoints and the
internal service-to-service permission check. The role routes (list /
create) live here; the user-facing role-binding routes (bind / unbind)
live in :mod:`aidp_iam.api.users` because they are per-user resources
from the client's perspective.

Routes
------

``GET  /api/v1/roles``
    List roles in the caller's tenant. Requires ``iam.role.read``.

``POST /api/v1/roles``
    Create a new role. Requires ``iam.role.create``.

``POST /api/v1/permissions/check``
    Service-to-service permission check. **No ``Authorization`` header
    is required** — the endpoint is intended for in-cluster calls
    from other AIDP services that already trust the network boundary.
    The caller supplies the target ``user_id`` and the ``permission``
    it wants to validate; the service computes the user's effective
    permission set and returns the boolean answer. The endpoint is
    the live re-evaluation of the JWT-snapshot check used by
    :func:`aidp_auth.dependencies._user_has_permission`.

Permission naming
-----------------

The names follow the platform's ``<service>.<resource>.<action>`` convention:

- ``iam.role.read``
- ``iam.role.create``

Cross-tenant safety
-------------------

The role routes are bound to the caller's tenant via
:func:`aidp_db.tenant.set_tenant_context` (in the service layer).
Cross-tenant access raises a :class:`aidp_common.errors.NotFoundError`
(404) instead of a 403 so the API does not leak the existence of
foreign roles.

The permission check endpoint does not have a caller context, so
:func:`aidp_iam.services.rbac.has_permission` looks up the user's
tenant id from the user record and re-evaluates the permission set
under that tenant. This is safe because user ids are UUID4 strings
(unique platform-wide) and the L1 listener guarantees the role
lookup stays inside the user's own tenant.
"""

from __future__ import annotations

import logging
from typing import Any

from aidp_auth.dependencies import require_permission
from aidp_auth.jwt import CurrentUser
from fastapi import APIRouter, Depends, status

from aidp_iam.schemas import (
    PermissionCheckRequest,
    PermissionCheckResponse,
    RoleCreateRequest,
    RoleListResponse,
    RoleResponse,
)
from aidp_iam.services import user_service
from aidp_iam.services.rbac import has_permission

_LOG = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["roles"])

# Centralized permission strings for the role routes. The user-facing
# bind / unbind endpoints (in api/users.py) define their own
# ``iam.role.bind`` constant; we re-use the same string here so a
# future registry refactor only needs to touch one block.
_PERM_ROLE_READ = "iam.role.read"
_PERM_ROLE_CREATE = "iam.role.create"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _role_to_model(d: dict[str, Any]) -> RoleResponse:
    """Project a service-layer role dict onto the Pydantic response model."""
    return RoleResponse.model_validate(d)


# ---------------------------------------------------------------------------
# LIST roles
# ---------------------------------------------------------------------------


@router.get(
    "/roles",
    response_model=RoleListResponse,
    status_code=status.HTTP_200_OK,
    summary="List every role in the caller's tenant.",
)
async def list_roles_route(
    user: CurrentUser = Depends(require_permission(_PERM_ROLE_READ)),
) -> RoleListResponse:
    """Return the tenant's role list, ordered by role code."""
    items = user_service.list_roles(tenant_id=user.tenant_id)
    return RoleListResponse(
        items=[_role_to_model(item) for item in items],
        total=len(items),
    )


# ---------------------------------------------------------------------------
# CREATE role
# ---------------------------------------------------------------------------


@router.post(
    "/roles",
    response_model=RoleResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new role in the caller's tenant.",
)
async def create_role_route(
    payload: RoleCreateRequest,
    user: CurrentUser = Depends(require_permission(_PERM_ROLE_CREATE)),
) -> RoleResponse:
    """Insert a new role. ``(tenant_id, code)`` is unique per tenant."""
    role = user_service.create_role(
        tenant_id=user.tenant_id,
        code=payload.code,
        name=payload.name,
        description=payload.description,
        scope=payload.scope,
        permissions=payload.permissions,
        created_by=user.user_id,
    )
    return _role_to_model(role)


# ---------------------------------------------------------------------------
# PERMISSIONS CHECK (internal — no require_auth)
# ---------------------------------------------------------------------------


@router.post(
    "/permissions/check",
    response_model=PermissionCheckResponse,
    status_code=status.HTTP_200_OK,
    summary="Service-to-service permission check (no auth — internal use only).",
)
async def permissions_check_route(
    payload: PermissionCheckRequest,
) -> PermissionCheckResponse:
    """Decide whether *payload.user_id* currently has *payload.permission*.

    This endpoint is the live re-evaluation of
    :func:`aidp_auth.dependencies._user_has_permission`. Unlike the
    FastAPI dependency, which operates on a JWT snapshot of the
    user's scopes, this handler re-reads the user's role bindings
    and computes the effective permission set at request time. It is
    the right call for service-to-service checks (e.g. the gateway
    asking "may this user invoke ``datasource:write``?") where the
    call must reflect the *current* role / permission state.

    The endpoint takes **no** ``Authorization`` header. The platform
    trusts the network boundary to keep this URL inside the cluster;
    misconfiguring that boundary is a deployment-level concern. We
    also accept the empty `user_id` gracefully and return
    ``{"allowed": false, "source": "none"}`` so a misconfigured
    caller does not crash.

    A nonexistent *user_id* returns ``allowed=false`` (the function
    treats a missing user as a user with no roles). Callers that need
    to distinguish "no such user" from "no such permission" should
    do their own existence check via :func:`aidp_iam.services.user_service.get_user`.
    """
    if not payload.user_id:
        return PermissionCheckResponse(
            user_id="",
            permission=payload.permission,
            allowed=False,
            source="none",
        )

    # Look up the user to learn the tenant_id, then evaluate the
    # permission set under that tenant. This is the *only* IAM
    # endpoint that needs a cross-tenant read: the caller is an
    # internal service that knows the user id but not the tenant id.
    # We bypass the L1 listener by going through a raw ``text()`` query
    # (the listener only fires for ORM ``select()`` statements) and
    # then evaluate the permission set under the user's own tenant.
    from aidp_db.session import get_session
    from sqlalchemy import text

    with get_session() as session:
        row = session.execute(
            text("SELECT tenant_id FROM users WHERE id = :uid"),
            {"uid": payload.user_id},
        ).first()

    if row is None:
        return PermissionCheckResponse(
            user_id=payload.user_id,
            permission=payload.permission,
            allowed=False,
            source="none",
        )

    target_tenant_id = row[0]
    decision = has_permission(
        user_id=payload.user_id,
        tenant_id=target_tenant_id,
        permission=payload.permission,
    )
    return PermissionCheckResponse(
        user_id=payload.user_id,
        permission=payload.permission,
        allowed=decision.allowed,
        source=decision.source,
    )


__all__ = ["router"]
