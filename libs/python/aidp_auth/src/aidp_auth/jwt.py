"""JWT sign / verify for the AIDP platform.

This module is the single source of truth for issuing and decoding
AIDP access / refresh tokens. It implements the platform global
constraint: "JWT: HS256, access_token 12h, refresh_token 30d".

Wire contract
-------------

Every token is a standard RFC 7519 JWT signed with HS256. The payload
carries the following claims (all required by :class:`TokenClaims`):

``tenant_id`` (str)
    Tenant the token belongs to. ``aidp_db.tenant`` reads this to
    enforce L1 isolation on every subsequent ORM select.
``user_id`` (str)
    Per-tenant user identifier. Used as ``created_by`` / ``updated_by``
    in audit columns.
``roles`` (list[str])
    Coarse-grained group identifiers (``"admin"``, ``"data_engineer"``,
    ...). Used for role-based shortcuts (``"admin"`` bypasses
    :func:`aidp_auth.dependencies.require_permission`).
``scopes`` (list[str])
    Fine-grained permission grants (``"datasource:read"``,
    ``"datasource:write"``). The :func:`require_permission` dependency
    checks these.
``token_type`` (``"access"`` | ``"refresh"``)
    Distinguishes access tokens (carry ``scopes``) from refresh tokens
    (do not — they are used only to mint new access tokens).
``jti`` (str)
    Per-token unique id (UUID4 string). For replay / revocation tracking.
``iat`` / ``exp`` (int Unix seconds)
    Standard issue / expiry timestamps. ``exp`` is enforced by
    :func:`decode_token`; an expired token raises
    :class:`aidp_common.errors.UnauthorizedError`.

Algorithm choice
----------------

The brief pins the algorithm to **HS256**. We accept that as the default
but allow the platform to override it via ``AIDP_JWT_ALGORITHM`` for
controlled migration. :func:`decode_token` always passes the configured
algorithm explicitly to PyJWT so a token signed with a different
algorithm is rejected (defense against ``alg=none`` and HS/RS
confusion).
"""

from __future__ import annotations

import time
import uuid
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any

import jwt as pyjwt
from aidp_common.config import get_settings
from aidp_common.errors import UnauthorizedError
from pydantic import BaseModel, ConfigDict, Field, field_validator


class TokenType(str, Enum):  # noqa: UP042 - intentional str-Enum mixin
    """The kind of token carried in a JWT payload.

    ``ACCESS`` tokens are the bearer tokens sent on every API request.
    ``REFRESH`` tokens are exchanged at the auth endpoint for a new
    ``ACCESS`` token; they do not carry ``scopes``.
    """

    ACCESS = "access"
    REFRESH = "refresh"


class TokenClaims(BaseModel):
    """Verified JWT payload (immutable).

    The model is ``frozen=True`` so a handler that decodes a token and
    passes the claims around cannot accidentally mutate them and leak
    state across retries. Required-claim enforcement is handled by
    Pydantic via :attr:`extra` and explicit type declarations — a token
    missing ``tenant_id`` / ``user_id`` / ``roles`` / ``scopes`` /
    ``token_type`` / ``jti`` / ``iat`` / ``exp`` fails validation inside
    :func:`decode_token` and surfaces as :class:`UnauthorizedError`.
    """

    model_config = ConfigDict(
        extra="ignore",
        frozen=True,
        str_strip_whitespace=False,
    )

    tenant_id: str = Field(min_length=1, max_length=128)
    user_id: str = Field(min_length=1, max_length=128)
    roles: list[str] = Field(default_factory=list)
    scopes: list[str] = Field(default_factory=list)
    token_type: TokenType
    jti: str = Field(min_length=1, max_length=128)
    # ``iat`` / ``exp`` arrive as ``int`` Unix seconds from PyJWT; we
    # coerce to timezone-aware UTC :class:`datetime` so downstream code
    # never has to wonder whether ``exp`` is a number or a date.
    iat: datetime
    exp: datetime

    @field_validator("iat", "exp", mode="before")
    @classmethod
    def _epoch_to_utc(cls, value: Any) -> Any:
        """Coerce Unix-seconds ints into timezone-aware UTC datetimes."""
        if isinstance(value, datetime):
            # Already a datetime (Pydantic will reject naive ones).
            if value.tzinfo is None:
                raise ValueError("datetime claims must be timezone-aware")
            return value
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(float(value), tz=UTC)
        raise ValueError(f"unexpected claim type: {type(value).__name__}")


class CurrentUser(BaseModel):
    """The public identity view of the authenticated caller.

    This is what FastAPI handlers see (via the :data:`current_user`
    dependency). It contains only the four fields the brief specifies
    — JWT internals (``jti``, ``iat``, ``exp``, ``token_type``) are
    intentionally not exposed.

    The model is ``frozen=True`` so a handler that stores the user
    cannot mutate it and leak state across requests.
    """

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
    )

    tenant_id: str = Field(min_length=1, max_length=128)
    user_id: str = Field(min_length=1, max_length=128)
    roles: list[str] = Field(default_factory=list)
    scopes: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _now_epoch() -> int:
    """Return the current Unix time in seconds (always integer seconds)."""
    return int(time.time())


def _encode(
    *,
    claims: dict[str, Any],
    expires_delta: timedelta | None = None,
) -> tuple[dict[str, Any], str]:
    """Stamp ``iat`` / ``jti`` / ``exp`` on *claims* and return the JWT.

    Returns the mutated claims dict (useful for tests) plus the encoded
    token. The ``exp`` claim is derived from the configured access
    token lifetime when *expires_delta* is not supplied.
    """
    settings = get_settings()
    now = _now_epoch()
    claims = {**claims, "iat": now, "jti": str(uuid.uuid4())}
    if expires_delta is None:
        exp_seconds = settings.jwt_access_token_expires_minutes * 60
    else:
        exp_seconds = int(expires_delta.total_seconds())
    claims["exp"] = now + exp_seconds
    token = pyjwt.encode(claims, settings.jwt_secret, algorithm=settings.jwt_algorithm)
    return claims, token


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def create_access_token(
    *,
    tenant_id: str,
    user_id: str,
    roles: list[str] | None = None,
    scopes: list[str] | None = None,
    expires_delta: timedelta | None = None,
) -> str:
    """Sign a new HS256 access token.

    Args:
        tenant_id: The tenant the token belongs to. Encoded verbatim;
            ``aidp_db.tenant`` reads this to set the L1 isolation
            context. Must be non-empty.
        user_id: The per-tenant user id. Must be non-empty.
        roles: Coarse-grained group identifiers (e.g. ``["admin"]``).
            Defaults to ``[]`` when ``None``.
        scopes: Fine-grained permission grants (e.g.
            ``["datasource:read"]``). Defaults to ``[]`` when ``None``.
        expires_delta: Override the configured 12h lifetime; primarily
            for tests that need an already-expired token.

    Returns:
        The signed JWT string. The caller is responsible for sending it
        to the client (typically as ``Authorization: Bearer <token>``).

    Raises:
        ValueError: When ``tenant_id`` or ``user_id`` is empty.
    """
    if not tenant_id:
        raise ValueError("tenant_id must be a non-empty string")
    if not user_id:
        raise ValueError("user_id must be a non-empty string")

    claims: dict[str, Any] = {
        "tenant_id": tenant_id,
        "user_id": user_id,
        "roles": list(roles) if roles is not None else [],
        "scopes": list(scopes) if scopes is not None else [],
        "token_type": TokenType.ACCESS.value,
    }
    _, token = _encode(claims=claims, expires_delta=expires_delta)
    return token


def create_refresh_token(
    *,
    tenant_id: str,
    user_id: str,
    roles: list[str] | None = None,
    expires_delta: timedelta | None = None,
) -> str:
    """Sign a new HS256 refresh token.

    Refresh tokens are exchanged at the auth endpoint for a new access
    token. Per OAuth2 best practice they do **not** carry ``scopes`` —
    the resource server must not accept a refresh token in the
    ``Authorization`` header.

    Args:
        tenant_id: The tenant the token belongs to. Must be non-empty.
        user_id: The per-tenant user id. Must be non-empty.
        roles: Coarse-grained group identifiers copied onto the
            refresh token so a refresh handler can mint an access
            token with the same role set. Defaults to ``[]``.
        expires_delta: Override the configured 30d lifetime; primarily
            for tests.

    Returns:
        The signed JWT string. The caller is responsible for sending
        it to the client (typically as an opaque cookie or in a
        separate response body field, never as the ``Authorization``
        header).

    Raises:
        ValueError: When ``tenant_id`` or ``user_id`` is empty.
    """
    if not tenant_id:
        raise ValueError("tenant_id must be a non-empty string")
    if not user_id:
        raise ValueError("user_id must be a non-empty string")

    claims: dict[str, Any] = {
        "tenant_id": tenant_id,
        "user_id": user_id,
        "roles": list(roles) if roles is not None else [],
        # Intentionally no ``scopes``: refresh tokens are not
        # resource-server credentials.
        "token_type": TokenType.REFRESH.value,
    }
    # ``_encode`` defaults to the access-token lifetime; for refresh
    # we want the configured refresh lifetime when no override is given.
    if expires_delta is None:
        from datetime import timedelta as _td  # local import for clarity

        settings = get_settings()
        expires_delta = _td(days=settings.jwt_refresh_token_expires_days)
    _, token = _encode(claims=claims, expires_delta=expires_delta)
    return token


def decode_token(token: str) -> TokenClaims:
    """Verify a JWT and return its claims.

    The function enforces:

    - The signature matches ``AIDP_JWT_SECRET`` using the configured
      algorithm (``AIDP_JWT_ALGORITHM``, default ``"HS256"``).
    - ``exp`` is present and not in the past (PyJWT raises
      ``ExpiredSignatureError`` otherwise).
    - The payload conforms to :class:`TokenClaims` (PyJWT's
      ``options={"require": [...]}`` + Pydantic's field validators
      catch every other shape issue, including missing
      ``tenant_id`` / ``user_id`` / ``token_type``).

    Any failure — bad signature, expired, malformed, missing claim,
    wrong algorithm — is converted into
    :class:`aidp_common.errors.UnauthorizedError` so the API surface
    always emits the platform's unified error format.

    Args:
        token: The raw JWT string (no ``Bearer `` prefix).

    Returns:
        A :class:`TokenClaims` instance with the verified payload.

    Raises:
        UnauthorizedError: When the token is missing, malformed, has
            a bad signature, has expired, or is missing a required
            claim. Always with ``status=401`` and
            ``code=ErrorCode.UNAUTHORIZED``.
    """
    if not token or not isinstance(token, str):  # pyright: ignore[reportUnnecessaryIsInstance]
        raise UnauthorizedError("missing or malformed token")

    settings = get_settings()
    try:
        raw: dict[str, Any] = pyjwt.decode(
            token,
            settings.jwt_secret,
            algorithms=[settings.jwt_algorithm],
            options={
                "require": [
                    "tenant_id",
                    "user_id",
                    "token_type",
                    "jti",
                    "iat",
                    "exp",
                ],
                "verify_signature": True,
                "verify_exp": True,
            },
        )
    except pyjwt.ExpiredSignatureError as exc:
        raise UnauthorizedError("token expired") from exc
    except pyjwt.InvalidAlgorithmError as exc:
        raise UnauthorizedError("token signed with disallowed algorithm") from exc
    except pyjwt.InvalidTokenError as exc:
        # Catch-all for every other PyJWT error (bad signature,
        # malformed payload, missing claim, etc.). We surface a single
        # error class so callers don't need to import PyJWT to handle
        # auth failures.
        raise UnauthorizedError("invalid token") from exc

    try:
        return TokenClaims.model_validate(raw)
    except Exception as exc:
        # Pydantic validation (missing required field, wrong type, bad
        # token_type, etc.). We never want a malformed token to surface
        # as a 500 — the canonical response is 401.
        raise UnauthorizedError("invalid token payload") from exc


def current_user_from_token(claims: TokenClaims) -> CurrentUser:
    """Project a verified :class:`TokenClaims` into a :class:`CurrentUser`.

    The projection drops the JWT internals (``jti``, ``iat``, ``exp``,
    ``token_type``) so the dependency surface stays small and the
    handler signature does not need to import :class:`TokenType`.

    Args:
        claims: A verified :class:`TokenClaims` instance.

    Returns:
        A frozen :class:`CurrentUser` exposing only the four
        platform-facing identity fields.
    """
    return CurrentUser(
        tenant_id=claims.tenant_id,
        user_id=claims.user_id,
        roles=list(claims.roles),
        scopes=list(claims.scopes),
    )


__all__ = [
    "CurrentUser",
    "TokenClaims",
    "TokenType",
    "create_access_token",
    "create_refresh_token",
    "current_user_from_token",
    "decode_token",
]
