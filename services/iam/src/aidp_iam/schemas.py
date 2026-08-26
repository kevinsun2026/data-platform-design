"""Pydantic request / response models for the IAM auth API.

The platform's REST layer keeps request and response payloads strictly
typed via Pydantic so the FastAPI dependency layer can validate input
and produce OpenAPI schemas automatically. Each model here is
``frozen=False`` (default) on the way in — we *do* want the call site
to be able to mutate validation context — and exposes a ``to_dict`` /
``model_dump_json`` path on the way out.

Wire conventions
----------------

- **Snake-case JSON keys** — Pydantic handles the deserialization for us;
  callers see ``admin_email``, not ``adminEmail``.
- **ISO 8601 timestamps** — the ``datetime`` fields round-trip as
  ``"2026-08-25T10:00:00+00:00"``.
- **No raw secrets** — the password fields live only on the request
  side; responses never echo them.

Error responses share a single envelope
``{"code": "string", "message": "string", "details": {...},
"trace_id": "string"}`` produced by :mod:`aidp_common.errors.AppError`
and rendered through the FastAPI exception handler. The brief lists
the minimal shape ``{"code", "message", "trace_id"}``; the
``details`` field is included when the underlying :class:`AppError`
provides one (it is always present, defaulting to ``{}``).
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, EmailStr, Field, field_validator

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Minimum password strength (per the platform brief — Task 8 acceptance
# criteria). The full OWASP rules live in the auth service's policy doc;
# here we only enforce the must-have so a misconfigured admin does not
# ship empty passwords into production.
_MIN_PASSWORD_LEN = 8
# Allow most printable ASCII; reject whitespace and control characters.
_PASSWORD_CHARS = re.compile(r"^[!-~]+$")

# Username: letters, digits, ``_``, ``-``, ``.``. 3-32 chars.
_USERNAME_RE = re.compile(r"^[A-Za-z0-9_.-]{3,32}$")

# Tenant code: same character set as username; restricted to lowercase
# to match the platform's convention (``acme`` not ``Acme``).
_TENANT_CODE_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{1,30}[a-z0-9]$")


def _normalize_email(value: str) -> str:
    """Lowercase + strip an email; the column constraint is case-sensitive
    on the lookup side, so we always normalize before querying.
    """
    return value.strip().lower()


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------


class RegisterTenantRequest(BaseModel):
    """Body of ``POST /api/v1/auth/register-tenant``.

    Day 1 is platform-admin-only; the route is published so the bootstrap
    process can mint the first tenant. After the first tenant exists the
    service publishes a ``tenant.created`` event so other services
    (audit, notification, billing) can react.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    tenant_code: str = Field(
        min_length=3,
        max_length=32,
        description="Stable business identifier for the tenant. Lowercase, url-safe.",
    )
    tenant_name: str = Field(
        min_length=1,
        max_length=255,
        description="Human-readable display name.",
    )
    admin_email: EmailStr = Field(
        description="Email for the bootstrap admin user. Becomes their login.",
    )
    admin_username: str | None = Field(
        default=None,
        min_length=3,
        max_length=32,
        description="Optional override. Defaults to the local part of admin_email.",
    )
    admin_password: str = Field(
        min_length=_MIN_PASSWORD_LEN,
        max_length=128,
        description="Admin password. Hashed with Argon2id before persistence.",
    )
    admin_display_name: str | None = Field(
        default=None,
        max_length=255,
    )
    tenant_plan: str = Field(
        default="team",
        max_length=32,
        description="Plan code (``free`` / ``team`` / ``enterprise``).",
    )
    tenant_region: str = Field(
        default="us-east-1",
        max_length=32,
    )

    @field_validator("tenant_code")
    @classmethod
    def _validate_tenant_code(cls, value: str) -> str:
        if not _TENANT_CODE_RE.match(value):
            raise ValueError(
                "tenant_code must be 3-32 chars, lowercase letters, digits, "
                "underscore or hyphen, and start/end with a letter or digit"
            )
        return value

    @field_validator("admin_username")
    @classmethod
    def _validate_admin_username(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if not _USERNAME_RE.match(value):
            raise ValueError("admin_username must be 3-32 chars, letters/digits/_/-/.")
        return value

    @field_validator("admin_password")
    @classmethod
    def _validate_password(cls, value: str) -> str:
        if len(value) < _MIN_PASSWORD_LEN:
            # Pydantic's own min_length validator would have already
            # raised; we re-state for the format check.
            raise ValueError(f"admin_password must be at least {_MIN_PASSWORD_LEN} characters")
        if not _PASSWORD_CHARS.match(value):
            raise ValueError(
                "admin_password must contain only printable ASCII "
                "characters (no whitespace or control codes)"
            )
        return value

    @field_validator("admin_email", mode="before")
    @classmethod
    def _normalize_admin_email(cls, value: Any) -> Any:
        if isinstance(value, str):
            return _normalize_email(value)
        return value


class LoginRequest(BaseModel):
    """Body of ``POST /api/v1/auth/login``.

    The login endpoint is the only one that takes a bare ``email`` +
    ``password`` pair. The handler resolves the email to a tenant
    (a user can only exist once across all tenants for a given email
    is the *planned* design — but for Day 1 we accept a ``tenant_code``
    hint to disambiguate bootstrap usage) and verifies the password.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    email: EmailStr = Field(description="User email (case-insensitive).")
    password: str = Field(min_length=1, max_length=128)
    tenant_code: str | None = Field(
        default=None,
        max_length=64,
        description=(
            "Optional tenant hint. When several tenants contain a user "
            "with the same email (rare but possible for shared service "
            "addresses) the hint disambiguates. When omitted, login "
            "fails for ambiguous emails to avoid leaking which tenants "
            "are configured."
        ),
    )

    @field_validator("email", mode="before")
    @classmethod
    def _normalize_email_field(cls, value: Any) -> Any:
        if isinstance(value, str):
            return _normalize_email(value)
        return value


class RefreshRequest(BaseModel):
    """Body of ``POST /api/v1/auth/refresh``.

    Accepts a previously-issued refresh token and returns a fresh
    access + refresh pair. The old refresh session is revoked (rotated)
    and a new :class:`Session` row is created so concurrent refresh
    attempts race in a single winner.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    refresh_token: str = Field(min_length=1, max_length=4096)


class LogoutRequest(BaseModel):
    """Body of ``POST /api/v1/auth/logout``.

    Both ``refresh_token`` and the implicit access token (in the
    ``Authorization`` header) are revoked. Logout is idempotent —
    calling it twice does not 404; the second call just finds nothing
    to revoke.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    refresh_token: str = Field(min_length=1, max_length=4096)


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class TokenPair(BaseModel):
    """A freshly-issued access + refresh token pair.

    Both are JWTs; the ``token_type`` matches the OAuth2 ``Bearer``
    constant so the client can drop the pair straight into the
    ``Authorization`` header on subsequent calls.
    """

    model_config = ConfigDict(extra="forbid")

    access_token: str
    refresh_token: str
    token_type: str = Field(
        default="Bearer",
        description='OAuth2 token type; always ``"Bearer"`` for AIDP.',
    )
    expires_in: int = Field(
        description="Access-token lifetime in seconds (matches AIDP_JWT_ACCESS_TOKEN_EXPIRES_MINUTES).",
    )


class UserInfo(BaseModel):
    """Public user view returned by ``/api/v1/auth/me`` and login."""

    model_config = ConfigDict(extra="forbid")

    id: str
    tenant_id: str
    username: str
    email: str
    display_name: str | None = None
    status: str
    mfa_enabled: bool
    roles: list[str] = Field(default_factory=list)
    scopes: list[str] = Field(default_factory=list)
    last_login_at: datetime | None = None
    created_at: datetime


class LoginResponse(BaseModel):
    """Response shape for ``/login``, ``/refresh`` and ``/register-tenant``."""

    model_config = ConfigDict(extra="forbid")

    token: TokenPair
    user: UserInfo


class MeResponse(BaseModel):
    """Response shape for ``/me`` — just the current user, no token."""

    model_config = ConfigDict(extra="forbid")

    user: UserInfo


class TenantCreatedResponse(BaseModel):
    """Response shape for ``/register-tenant``.

    Distinct from :class:`LoginResponse` because we also return the
    tenant summary so the bootstrap caller can store the new tenant
    id without a second roundtrip.
    """

    model_config = ConfigDict(extra="forbid")

    tenant_id: str
    tenant_code: str
    tenant_name: str
    user: UserInfo
    token: TokenPair


class SsoCallbackResponse(BaseModel):
    """Stub response for ``/auth/sso/{provider}/callback``.

    Real SSO is out of scope for Task 8. The handler returns ``501``
    (the route exists, but the feature is not implemented yet) and
    the body is a structured error.
    """

    model_config = ConfigDict(extra="forbid")

    code: str = "SSO_NOT_IMPLEMENTED"
    message: str = "SSO is not implemented in this build."
    provider: str


# ---------------------------------------------------------------------------
# User / Role management — Task 9
# ---------------------------------------------------------------------------

# The user / role request and response models share the same JSON-key
# conventions as the rest of the module. The wire format is:
#
# - snake_case fields (``display_name``)
# - ISO 8601 timestamps
# - password fields are write-only — they never appear on responses

#: Allowed user statuses. Mirrors the values in the :class:`User.status`
#: column so the API surface does not invent new vocabulary.
_USER_STATUSES = ("active", "locked", "disabled", "invited")

#: Allowed role scopes. Mirrors the values in the :class:`Role.scope` column.
_ROLE_SCOPES = ("global", "tenant")


# ---------------------------------------------------------------------------
# User request / response models
# ---------------------------------------------------------------------------


class UserCreateRequest(BaseModel):
    """Body of ``POST /api/v1/users``.

    Admin-only; the bootstrap admin (created by ``register-tenant``) and
    any later user with the ``iam.user.create`` permission can hit this
    endpoint. The role bindings are optional — a user can be created
    without any roles and have them attached later via
    ``POST /api/v1/users/{id}/roles``.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    username: str = Field(
        min_length=3,
        max_length=32,
        description="Login name. 3-32 chars; letters, digits, ``_``, ``-``, ``.``.",
    )
    email: EmailStr = Field(description="Contact email (lowercased before persistence).")
    password: str = Field(
        min_length=_MIN_PASSWORD_LEN,
        max_length=128,
        description="Initial password. Hashed with Argon2id before persistence.",
    )
    display_name: str | None = Field(default=None, max_length=255)
    phone: str | None = Field(default=None, max_length=32)
    status: str = Field(
        default="active",
        max_length=16,
        description='One of "active", "locked", "disabled", "invited".',
    )
    role_ids: list[str] = Field(
        default_factory=list,
        max_length=64,
        description="Optional initial role bindings. Each id must belong to the caller's tenant.",
    )

    @field_validator("username")
    @classmethod
    def _validate_username(cls, value: str) -> str:
        if not _USERNAME_RE.match(value):
            raise ValueError("username must be 3-32 chars, letters/digits/_/-/.")
        return value

    @field_validator("password")
    @classmethod
    def _validate_password(cls, value: str) -> str:
        if len(value) < _MIN_PASSWORD_LEN:
            raise ValueError(f"password must be at least {_MIN_PASSWORD_LEN} characters")
        if not _PASSWORD_CHARS.match(value):
            raise ValueError(
                "password must contain only printable ASCII "
                "characters (no whitespace or control codes)"
            )
        return value

    @field_validator("email", mode="before")
    @classmethod
    def _normalize_email(cls, value: Any) -> Any:
        if isinstance(value, str):
            return _normalize_email(value)
        return value

    @field_validator("status")
    @classmethod
    def _validate_status(cls, value: str) -> str:
        if value not in _USER_STATUSES:
            raise ValueError(f"status must be one of {', '.join(_USER_STATUSES)}; got {value!r}")
        return value

    @field_validator("role_ids")
    @classmethod
    def _validate_role_ids(cls, value: list[str]) -> list[str]:
        seen: set[str] = set()
        out: list[str] = []
        for rid in value:
            if not rid or not isinstance(rid, str):
                raise ValueError("role_ids must be a list of non-empty strings")
            if rid in seen:
                continue
            seen.add(rid)
            out.append(rid)
        return out


class UserUpdateRequest(BaseModel):
    """Body of ``PUT /api/v1/users/{id}``.

    All fields are optional; omitted fields are not touched. Password
    changes go through the dedicated ``/reset-password`` endpoint so
    the audit log records the action as a separate event.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    display_name: str | None = Field(default=None, max_length=255)
    phone: str | None = Field(default=None, max_length=32)
    status: str | None = Field(default=None, max_length=16)
    mfa_enabled: bool | None = Field(default=None)

    @field_validator("status")
    @classmethod
    def _validate_status(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if value not in _USER_STATUSES:
            raise ValueError(f"status must be one of {', '.join(_USER_STATUSES)}; got {value!r}")
        return value


class ResetPasswordRequest(BaseModel):
    """Body of ``POST /api/v1/users/{id}/reset-password``.

    The new password is hashed with Argon2id before persistence. The
    raw value is never echoed in the response (and is dropped by
    Pydantic after the handler reads it).
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    new_password: str = Field(
        min_length=_MIN_PASSWORD_LEN,
        max_length=128,
        description="Replacement password. Hashed with Argon2id before persistence.",
    )

    @field_validator("new_password")
    @classmethod
    def _validate_password(cls, value: str) -> str:
        if len(value) < _MIN_PASSWORD_LEN:
            raise ValueError(f"new_password must be at least {_MIN_PASSWORD_LEN} characters")
        if not _PASSWORD_CHARS.match(value):
            raise ValueError(
                "new_password must contain only printable ASCII "
                "characters (no whitespace or control codes)"
            )
        return value


class BindRoleRequest(BaseModel):
    """Body of ``POST /api/v1/users/{id}/roles``.

    ``role_id`` may refer to a role in the caller's tenant (or a
    global role). The endpoint refuses cross-tenant bindings.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    role_id: str = Field(min_length=1, max_length=64, description="Role id to bind to the user.")


# ---------------------------------------------------------------------------
# Role request / response models
# ---------------------------------------------------------------------------


class RoleCreateRequest(BaseModel):
    """Body of ``POST /api/v1/roles``.

    Roles are tenant-scoped: a role created in tenant A cannot be
    granted to a user in tenant B. ``scope=global`` is reserved for
    platform operators and currently has the same per-tenant storage
    semantics; it is exposed here so the API matches the model.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    code: str = Field(
        min_length=1,
        max_length=64,
        description="Stable business key. Unique within the tenant.",
    )
    name: str = Field(min_length=1, max_length=255, description="Human-readable display name.")
    description: str | None = Field(default=None, max_length=1024)
    scope: str = Field(
        default="tenant",
        max_length=16,
        description='One of "global", "tenant".',
    )
    permissions: list[str] = Field(
        default_factory=list,
        max_length=256,
        description='Permission strings. Use the wildcard literal "*" for a superuser grant.',
    )

    @field_validator("scope")
    @classmethod
    def _validate_scope(cls, value: str) -> str:
        if value not in _ROLE_SCOPES:
            raise ValueError(f"scope must be one of {', '.join(_ROLE_SCOPES)}; got {value!r}")
        return value

    @field_validator("permissions")
    @classmethod
    def _validate_permissions(cls, value: list[str]) -> list[str]:
        seen: set[str] = set()
        out: list[str] = []
        for perm in value:
            if not isinstance(perm, str) or not perm:
                raise ValueError("permissions must be a list of non-empty strings")
            if perm in seen:
                continue
            seen.add(perm)
            out.append(perm)
        return out

    @field_validator("code")
    @classmethod
    def _validate_code(cls, value: str) -> str:
        # Same character set as usernames — short, URL-safe, no whitespace.
        if not _USERNAME_RE.match(value):
            raise ValueError("code must be 3-32 chars, letters/digits/_/-/.")
        return value


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class UserResponse(BaseModel):
    """Public view of a user — mirrors :class:`UserInfo` but for any user.

    Used by the user CRUD endpoints (``GET`` / ``PUT`` / ``POST`` on
    ``/api/v1/users``). Includes the user's bound roles + computed
    scopes so the admin console can render the row without a second
    roundtrip.
    """

    model_config = ConfigDict(extra="forbid")

    id: str
    tenant_id: str
    username: str
    email: str
    display_name: str | None = None
    phone: str | None = None
    status: str
    mfa_enabled: bool
    last_login_at: datetime | None = None
    created_at: datetime
    updated_at: datetime
    roles: list[str] = Field(default_factory=list, description="Bound role codes.")
    scopes: list[str] = Field(default_factory=list, description="Union of bound role permissions.")


class UserListResponse(BaseModel):
    """Paginated list of users for ``GET /api/v1/users``."""

    model_config = ConfigDict(extra="forbid")

    items: list[UserResponse]
    total: int = Field(ge=0, description="Total matching users in the caller's tenant.")
    page: int = Field(ge=1, description="1-based page index (echoed from the request).")
    page_size: int = Field(ge=1, le=200, description="Echoed from the request.")


class RoleResponse(BaseModel):
    """Public view of a role — mirrors :class:`Role` without the audit fields."""

    model_config = ConfigDict(extra="forbid")

    id: str
    tenant_id: str
    code: str
    name: str
    description: str | None = None
    scope: str
    permissions: list[str]
    created_at: datetime
    updated_at: datetime


class RoleListResponse(BaseModel):
    """Response for ``GET /api/v1/roles``."""

    model_config = ConfigDict(extra="forbid")

    items: list[RoleResponse]
    total: int = Field(ge=0, description="Total roles in the caller's tenant.")


class UserRolesResponse(BaseModel):
    """Response for ``GET /api/v1/users/{id}/roles``."""

    model_config = ConfigDict(extra="forbid")

    user_id: str
    items: list[RoleResponse]
    total: int = Field(ge=0)


# ---------------------------------------------------------------------------
# Permission check (service-to-service, no auth)
# ---------------------------------------------------------------------------


class PermissionCheckRequest(BaseModel):
    """Body of ``POST /api/v1/permissions/check``.

    This is the platform's internal permission-decision endpoint. The
    caller is another AIDP service in the same trust zone; there is
    no user-level ``Authorization`` header. The caller supplies the
    target ``user_id`` and the ``permission`` it wants to validate;
    the service computes the user's effective permission set and
    returns the boolean answer.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    user_id: str = Field(
        max_length=64,
        description=(
            "Per-tenant user id whose permission is being queried. "
            "Empty / missing values are accepted and return ``allowed=false``; "
            "the handler treats them as 'no such user' rather than rejecting."
        ),
    )
    permission: str = Field(
        min_length=1,
        max_length=256,
        description='Permission string to check. The literal "*" is not allowed here — the caller is asking about a specific permission.',
    )


class PermissionCheckResponse(BaseModel):
    """Response shape for ``POST /api/v1/permissions/check``."""

    model_config = ConfigDict(extra="forbid")

    user_id: str
    permission: str
    allowed: bool = Field(
        description=(
            "``True`` when the user holds the permission directly, via a role "
            'binding whose ``permissions`` list contains it, or via the "*" '
            "wildcard. ``False`` otherwise (or when the user does not exist)."
        ),
    )
    source: str = Field(
        description=(
            'How the decision was reached. One of "wildcard", "role", "user", '
            '"none". Useful for audit / debug; do not gate business logic on it.'
        ),
    )


__all__ = [
    "BindRoleRequest",
    "LoginRequest",
    "LoginResponse",
    "LogoutRequest",
    "MeResponse",
    "PermissionCheckRequest",
    "PermissionCheckResponse",
    "RefreshRequest",
    "RegisterTenantRequest",
    "ResetPasswordRequest",
    "RoleCreateRequest",
    "RoleListResponse",
    "RoleResponse",
    "SsoCallbackResponse",
    "TenantCreatedResponse",
    "TokenPair",
    "UserCreateRequest",
    "UserInfo",
    "UserListResponse",
    "UserResponse",
    "UserRolesResponse",
    "UserUpdateRequest",
]
