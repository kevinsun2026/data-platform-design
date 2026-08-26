"""HTTP routes for the IAM ``/api/v1/auth/*`` surface.

The module is the transport adapter: it parses requests, calls the
auth service, and projects results onto the Pydantic response
models. All database-shaped, cryptographic, and event-emitting work
lives in :mod:`aidp_iam.services.auth_service` so it can be reused
by scripts and background jobs.

Routes
------

``POST /api/v1/auth/register-tenant``
    Day-1 super-admin bootstrap. Atomic tenant + admin user + admin
    role + first token pair. Returns the new tenant id and the
    admin's tokens.

``POST /api/v1/auth/login``
    Email + password (optionally a ``tenant_code`` hint) -> access +
    refresh pair. Publishes an ``iam.user.logged_in`` audit event on
    success (best-effort).

``POST /api/v1/auth/refresh``
    Rotate a refresh token. The old session is revoked and a fresh
    pair is issued.

``POST /api/v1/auth/logout``
    Revoke a refresh token. The caller may also include the access
    token in the ``Authorization`` header to revoke the implicit
    access, but JWT access tokens are stateless so the only effect
    is the session revocation. Logout is idempotent.

``POST /api/v1/auth/sso/{provider}/callback``
    Stub. Returns ``501`` until SSO is implemented (later task).

``GET  /api/v1/auth/me``
    Return the caller's :class:`aidp_iam.schemas.UserInfo`. Requires
    a valid access-token ``Authorization: Bearer ...`` header.

Error envelope
--------------

All errors flow through :class:`aidp_common.errors.AppError`; the
:class:`aidp_iam.api.errors.app_error_handler` renders the unified
``{"code", "message", "details", "trace_id"}`` envelope.
"""

from __future__ import annotations

import logging
from typing import Any

from aidp_auth.dependencies import current_user
from aidp_auth.jwt import CurrentUser, decode_token
from aidp_common.errors import AppError, UnauthorizedError
from fastapi import APIRouter, Depends, Path, status
from fastapi.responses import JSONResponse

from aidp_iam.schemas import (
    LoginRequest,
    LoginResponse,
    LogoutRequest,
    MeResponse,
    RefreshRequest,
    RegisterTenantRequest,
    SsoCallbackResponse,
    TenantCreatedResponse,
    TokenPair,
    UserInfo,
)
from aidp_iam.services import auth_service

_LOG = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/auth", tags=["auth"])


# ---------------------------------------------------------------------------
# Response helpers
# ---------------------------------------------------------------------------


def _token_pair_to_model(pair: dict[str, Any]) -> TokenPair:
    """Project a :class:`TokenPairResult`-shaped dict onto the response model."""
    return TokenPair.model_validate(pair)


def _user_info_to_model(user: dict[str, Any]) -> UserInfo:
    """Project a user-info dict onto the response model."""
    return UserInfo.model_validate(user)


# ---------------------------------------------------------------------------
# register-tenant
# ---------------------------------------------------------------------------


@router.post(
    "/register-tenant",
    response_model=TenantCreatedResponse,
    status_code=status.HTTP_200_OK,
    summary="Register a new tenant and its bootstrap admin (Day 1 only).",
)
async def register_tenant(payload: RegisterTenantRequest) -> TenantCreatedResponse:
    """Atomically create a tenant, its admin user, and the first token pair.

    On success a best-effort ``iam.tenant.created`` audit event is
    published via :func:`auth_service.publish_audit_event`. The publish
    is fire-and-forget — its failure is logged but never propagated
    to the API caller, so a Kafka outage cannot 500 a tenant
    registration. The service function itself stays synchronous so
    it remains directly callable from CLI scripts and background
    jobs that do not run an event loop.
    """
    out = auth_service.register_tenant(
        tenant_code=payload.tenant_code,
        tenant_name=payload.tenant_name,
        admin_email=payload.admin_email,
        admin_password=payload.admin_password,
        admin_username=payload.admin_username,
        admin_display_name=payload.admin_display_name,
        tenant_plan=payload.tenant_plan,
        tenant_region=payload.tenant_region,
    )
    # Best-effort audit: the service swallows publish errors, so a
    # Kafka outage does not 500 the registration path. The event is
    # keyed to the new tenant's id (which the service just committed),
    # not the caller's tenant context — there is no "caller tenant"
    # on a Day 1 registration.
    await auth_service.publish_audit_event(
        event_type=auth_service._AUDIT_EVENT_TYPE_TENANT_CREATED,
        tenant_id=out["tenant_id"],
        payload={
            "tenant_id": out["tenant_id"],
            "tenant_code": out["tenant_code"],
            "tenant_name": out["tenant_name"],
            "admin_user_id": out["user"]["id"],
            "admin_email": out["user"]["email"],
            "plan": payload.tenant_plan,
            "region": payload.tenant_region,
        },
    )
    return TenantCreatedResponse(
        tenant_id=out["tenant_id"],
        tenant_code=out["tenant_code"],
        tenant_name=out["tenant_name"],
        user=_user_info_to_model(out["user"]),
        token=_token_pair_to_model(out["token"]),
    )


# ---------------------------------------------------------------------------
# login
# ---------------------------------------------------------------------------


@router.post(
    "/login",
    response_model=LoginResponse,
    status_code=status.HTTP_200_OK,
    summary="Authenticate with email + password and receive a token pair.",
)
async def login(payload: LoginRequest) -> LoginResponse:
    """Verify the email + password and mint a fresh access + refresh pair.

    On success, a best-effort ``iam.user.logged_in`` audit event is
    published. The audit publish failure does not affect the auth
    response.
    """
    authed = auth_service.authenticate(
        email=payload.email,
        password=payload.password,
        tenant_code=payload.tenant_code,
    )
    pair = auth_service.issue_token_pair(authed=authed)
    # Best-effort audit; the service swallows publish errors so a
    # Kafka outage does not 500 the auth path.
    await auth_service.publish_audit_event(
        event_type=auth_service._AUDIT_EVENT_TYPE_LOGIN,
        tenant_id=authed.user.tenant_id,
        payload={
            "user_id": authed.user.id,
            "tenant_id": authed.user.tenant_id,
            "email": authed.user.email,
            "ip": None,
        },
    )
    return LoginResponse(
        token=_token_pair_to_model(pair.to_dict()),
        user=_user_info_to_model(authed.to_user_info()),
    )


# ---------------------------------------------------------------------------
# refresh
# ---------------------------------------------------------------------------


@router.post(
    "/refresh",
    response_model=LoginResponse,
    status_code=status.HTTP_200_OK,
    summary="Rotate a refresh token and receive a new access + refresh pair.",
)
async def refresh(payload: RefreshRequest) -> LoginResponse:
    """Verify a refresh token, revoke its session, and issue a new pair."""
    authed, pair = auth_service.refresh_tokens(refresh_token=payload.refresh_token)
    return LoginResponse(
        token=_token_pair_to_model(pair.to_dict()),
        user=_user_info_to_model(authed.to_user_info()),
    )


# ---------------------------------------------------------------------------
# logout
# ---------------------------------------------------------------------------


@router.post(
    "/logout",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Revoke a refresh token (idempotent).",
)
async def logout(payload: LogoutRequest) -> None:
    """Soft-revoke the session that owns *payload.refresh_token*.

    Logout is idempotent: calling it twice (or with an unknown
    token) does not raise. The endpoint always returns ``204``.
    """
    revoked = auth_service.revoke_session(refresh_token=payload.refresh_token)
    if revoked:
        # Best-effort: publish the audit event with the token's claims
        # (when decodable) so the audit log knows which tenant / user
        # signed off. Decode failures are ignored — we still revoked
        # the session.
        try:
            claims = decode_token(payload.refresh_token)
            await auth_service.publish_audit_event(
                event_type=auth_service._AUDIT_EVENT_TYPE_LOGOUT,
                tenant_id=claims.tenant_id,
                payload={
                    "user_id": claims.user_id,
                    "tenant_id": claims.tenant_id,
                },
            )
        except AppError:
            pass
    return


# ---------------------------------------------------------------------------
# SSO callback (stub)
# ---------------------------------------------------------------------------


@router.post(
    "/sso/{provider}/callback",
    response_model=SsoCallbackResponse,
    status_code=status.HTTP_501_NOT_IMPLEMENTED,
    summary="SSO callback (not implemented in this build).",
)
async def sso_callback(
    provider: str = Path(..., min_length=1, max_length=64),
) -> JSONResponse:
    """Return ``501`` until SSO is implemented (later task)."""
    body = SsoCallbackResponse(
        code="SSO_NOT_IMPLEMENTED",
        message="SSO is not implemented in this build.",
        provider=provider,
    ).model_dump()
    return JSONResponse(status_code=status.HTTP_501_NOT_IMPLEMENTED, content=body)


# ---------------------------------------------------------------------------
# me
# ---------------------------------------------------------------------------


@router.get(
    "/me",
    response_model=MeResponse,
    status_code=status.HTTP_200_OK,
    summary="Return the authenticated caller's user info.",
)
def me(user: CurrentUser = Depends(current_user)) -> MeResponse:
    """Look up the caller and return their public user view.

    Re-reads the user from the database so any role / scope / status
    change that happened after the token was issued is reflected
    (e.g. an admin just revoked a binding).
    """
    info = auth_service.user_from_current_user(user)
    if info is None:
        raise UnauthorizedError("user no longer exists or is not active")
    return MeResponse(user=_user_info_to_model(info.to_user_info()))


__all__ = ["router"]
