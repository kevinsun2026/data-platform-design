"""HTTP routes for the IAM ``/api/v1/users`` + ``/api/v1/roles`` surface.

The module is the transport adapter for the user / role CRUD endpoints
added in Task 9. It parses requests, calls the service layer in
:mod:`aidp_iam.services.user_service`, and projects results onto the
Pydantic response models in :mod:`aidp_iam.schemas`.

Permission naming
-----------------

Every route in this module gates on a single permission via
:func:`aidp_auth.dependencies.require_permission`. The permission
strings follow the convention ``iam.<resource>.<action>``:

==========================  =========================================
Route                        Permission required
==========================  =========================================
GET    /api/v1/users         ``iam.user.read``
POST   /api/v1/users         ``iam.user.create``
GET    /api/v1/users/{id}    ``iam.user.read``
PUT    /api/v1/users/{id}    ``iam.user.update``
DELETE /api/v1/users/{id}    ``iam.user.delete``
POST   /api/v1/users/{id}/reset-password
                              ``iam.user.reset_password``
GET    /api/v1/users/{id}/roles
                              ``iam.user.read``
POST   /api/v1/users/{id}/roles
                              ``iam.role.bind``
DELETE /api/v1/users/{id}/roles/{role_id}
                              ``iam.role.bind``
==========================  =========================================

L1 isolation
------------

All handlers take the authenticated caller's ``CurrentUser`` via the
``Depends(current_user)`` dependency. The dependency binds the
request-scoped tenant context via :func:`aidp_db.tenant.set_tenant_context`,
so every downstream ORM select is auto-filtered by ``WHERE tenant_id
= :tid``. A user in tenant A cannot read, update, or delete a user in
tenant B — the listener will return zero rows and the service layer
will surface a :class:`aidp_common.errors.NotFoundError` (404), which
is the correct cross-tenant response (a 403 would leak the existence
of the foreign row).

Error envelope
--------------

All errors flow through :class:`aidp_common.errors.AppError`; the
:class:`aidp_iam.api.errors.app_error_handler` renders the unified
``{"code", "message", "details", "trace_id"}`` envelope.
"""

from __future__ import annotations

import logging
from typing import Any

from aidp_auth.dependencies import require_permission
from aidp_auth.jwt import CurrentUser
from aidp_common.errors import NotFoundError
from fastapi import APIRouter, Depends, Path, Query, status

from aidp_iam.schemas import (
    BindRoleRequest,
    ResetPasswordRequest,
    RoleResponse,
    UserCreateRequest,
    UserListResponse,
    UserResponse,
    UserRolesResponse,
    UserUpdateRequest,
)
from aidp_iam.services import user_service

_LOG = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["users", "roles"])

# Path-level user id validator. Reusing ``min_length=1`` keeps the
# validation story uniform across all routes that take a user id.
_USER_ID_PATH = Path(..., min_length=1, max_length=64, description="Per-tenant user id.")
_ROLE_ID_PATH = Path(..., min_length=1, max_length=64, description="Per-tenant role id.")


# ---------------------------------------------------------------------------
# Permission strings
# ---------------------------------------------------------------------------
#
# Centralized so a future refactor (e.g. moving to a registry) only
# needs to touch one block. The names follow the platform's
# ``<service>.<resource>.<action>`` convention.

_PERM_USER_READ = "iam.user.read"
_PERM_USER_CREATE = "iam.user.create"
_PERM_USER_UPDATE = "iam.user.update"
_PERM_USER_DELETE = "iam.user.delete"
_PERM_USER_RESET_PASSWORD = "iam.user.reset_password"
_PERM_ROLE_BIND = "iam.role.bind"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _user_info_to_model(info: dict[str, Any]) -> UserResponse:
    """Project a service-layer user-info dict onto the Pydantic response model."""
    return UserResponse.model_validate(info)


# ---------------------------------------------------------------------------
# LIST users
# ---------------------------------------------------------------------------


@router.get(
    "/users",
    response_model=UserListResponse,
    status_code=status.HTTP_200_OK,
    summary="List users in the caller's tenant (paginated, optional filters).",
)
async def list_users_route(
    page: int = Query(1, ge=1, description="1-based page index."),
    page_size: int = Query(
        20,
        ge=1,
        le=200,
        description="Rows per page. Capped server-side at 200.",
    ),
    status_filter: str | None = Query(
        None,
        alias="status",
        max_length=16,
        description="Optional filter — restrict to users with this status.",
    ),
    role_id: str | None = Query(
        None,
        max_length=64,
        description="Optional filter — restrict to users bound to this role.",
    ),
    user: CurrentUser = Depends(require_permission(_PERM_USER_READ)),
) -> UserListResponse:
    """Return a paginated list of users in the caller's tenant."""
    result = user_service.list_users(
        tenant_id=user.tenant_id,
        page=page,
        page_size=page_size,
        status=status_filter,
        role_id=role_id,
    )
    return UserListResponse(
        items=[_user_info_to_model(item) for item in result["items"]],
        total=result["total"],
        page=result["page"],
        page_size=result["page_size"],
    )


# ---------------------------------------------------------------------------
# CREATE user
# ---------------------------------------------------------------------------


@router.post(
    "/users",
    response_model=UserResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new user in the caller's tenant.",
)
async def create_user_route(
    payload: UserCreateRequest,
    user: CurrentUser = Depends(require_permission(_PERM_USER_CREATE)),
) -> UserResponse:
    """Insert a new user, optionally with initial role bindings."""
    info = user_service.create_user(
        tenant_id=user.tenant_id,
        username=payload.username,
        email=payload.email,
        password=payload.password,
        display_name=payload.display_name,
        phone=payload.phone,
        status=payload.status,
        role_ids=payload.role_ids,
        created_by=user.user_id,
    )
    return _user_info_to_model(info)


# ---------------------------------------------------------------------------
# GET user
# ---------------------------------------------------------------------------


@router.get(
    "/users/{user_id}",
    response_model=UserResponse,
    status_code=status.HTTP_200_OK,
    summary="Get a single user by id (caller's tenant only).",
)
async def get_user_route(
    user_id: str = _USER_ID_PATH,
    user: CurrentUser = Depends(require_permission(_PERM_USER_READ)),
) -> UserResponse:
    """Return a single user-info record. 404 for cross-tenant ids."""
    info = user_service.get_user(tenant_id=user.tenant_id, user_id=user_id)
    if info is None:
        # 404 (not 403) on cross-tenant access — never leak the
        # existence of a row in another tenant.
        raise NotFoundError("user", user_id)
    return _user_info_to_model(info)


# ---------------------------------------------------------------------------
# UPDATE user
# ---------------------------------------------------------------------------


@router.put(
    "/users/{user_id}",
    response_model=UserResponse,
    status_code=status.HTTP_200_OK,
    summary="Update a user's mutable fields.",
)
async def update_user_route(
    payload: UserUpdateRequest,
    user_id: str = _USER_ID_PATH,
    user: CurrentUser = Depends(require_permission(_PERM_USER_UPDATE)),
) -> UserResponse:
    """Partial update of ``display_name`` / ``phone`` / ``status`` / ``mfa_enabled``.

    Only fields present in the request body are touched. Password
    changes go through the dedicated ``/reset-password`` route so
    the audit log records the action separately.
    """
    info = user_service.update_user(
        tenant_id=user.tenant_id,
        user_id=user_id,
        display_name=payload.display_name,
        phone=payload.phone,
        status=payload.status,
        mfa_enabled=payload.mfa_enabled,
        updated_by=user.user_id,
    )
    return _user_info_to_model(info)


# ---------------------------------------------------------------------------
# DELETE user (soft delete)
# ---------------------------------------------------------------------------


@router.delete(
    "/users/{user_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Soft-delete a user (sets ``status='disabled'`` + revokes sessions).",
)
async def delete_user_route(
    user_id: str = _USER_ID_PATH,
    user: CurrentUser = Depends(require_permission(_PERM_USER_DELETE)),
) -> None:
    """Set ``status='disabled'`` and ``deleted_at`` to the current time.

    The row is not physically removed; the soft-delete contract is the
    same as the :class:`aidp_common.models.TimestampMixin.deleted_at`
    column. All active sessions for the user are revoked so a stolen
    refresh token cannot survive the disable.
    """
    user_service.delete_user(
        tenant_id=user.tenant_id,
        user_id=user_id,
        updated_by=user.user_id,
    )
    return


# ---------------------------------------------------------------------------
# RESET PASSWORD
# ---------------------------------------------------------------------------


@router.post(
    "/users/{user_id}/reset-password",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Replace a user's password and revoke all active sessions.",
)
async def reset_password_route(
    payload: ResetPasswordRequest,
    user_id: str = _USER_ID_PATH,
    user: CurrentUser = Depends(require_permission(_PERM_USER_RESET_PASSWORD)),
) -> None:
    """Hash the new password and revoke every active refresh session.

    The two operations are committed in a single transaction so an
    observer of the new password hash is guaranteed to also see the
    revoked sessions — there is no window where a stolen refresh
    token can survive a password reset.
    """
    user_service.reset_password(
        tenant_id=user.tenant_id,
        user_id=user_id,
        new_password=payload.new_password,
        updated_by=user.user_id,
    )
    return


# ---------------------------------------------------------------------------
# LIST user roles
# ---------------------------------------------------------------------------


@router.get(
    "/users/{user_id}/roles",
    response_model=UserRolesResponse,
    status_code=status.HTTP_200_OK,
    summary="List the roles currently bound to a user.",
)
async def list_user_roles_route(
    user_id: str = _USER_ID_PATH,
    user: CurrentUser = Depends(require_permission(_PERM_USER_READ)),
) -> UserRolesResponse:
    """Return the user's bound roles (excludes expired bindings)."""
    # Existence-check the user so the API can return 404.
    if user_service.get_user(tenant_id=user.tenant_id, user_id=user_id) is None:
        raise NotFoundError("user", user_id)
    items_dicts = user_service.list_user_roles(tenant_id=user.tenant_id, user_id=user_id)
    items = [RoleResponse.model_validate(d) for d in items_dicts]
    return UserRolesResponse(user_id=user_id, items=items, total=len(items))


# ---------------------------------------------------------------------------
# BIND role
# ---------------------------------------------------------------------------


@router.post(
    "/users/{user_id}/roles",
    response_model=UserResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Bind a role to a user (caller's tenant only).",
)
async def bind_role_route(
    payload: BindRoleRequest,
    user_id: str = _USER_ID_PATH,
    user: CurrentUser = Depends(require_permission(_PERM_ROLE_BIND)),
) -> UserResponse:
    """Attach a role to a user. Refuses cross-tenant role ids."""
    info = user_service.bind_role(
        tenant_id=user.tenant_id,
        user_id=user_id,
        role_id=payload.role_id,
        granted_by=user.user_id,
    )
    return _user_info_to_model(info)


# ---------------------------------------------------------------------------
# UNBIND role
# ---------------------------------------------------------------------------


@router.delete(
    "/users/{user_id}/roles/{role_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Remove a role binding (idempotent).",
)
async def unbind_role_route(
    user_id: str = _USER_ID_PATH,
    role_id: str = _ROLE_ID_PATH,
    user: CurrentUser = Depends(require_permission(_PERM_ROLE_BIND)),
) -> None:
    """Detach a role from a user. Idempotent: a missing binding is a 204."""
    user_service.unbind_role(
        tenant_id=user.tenant_id,
        user_id=user_id,
        role_id=role_id,
    )
    return


__all__ = [
    "router",
]
