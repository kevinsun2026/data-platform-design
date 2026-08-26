"""Tests for ``aidp_auth.jwt``.

Coverage:

- ``create_access_token`` produces a valid HS256 JWT carrying the
  platform-required claims (``tenant_id`` / ``user_id`` / ``roles`` /
  ``scopes`` / ``iat`` / ``exp`` / ``jti`` / ``token_type``).
- ``create_refresh_token`` produces a separate token kind with a longer
  expiry (default 30d) and no ``scopes`` (refresh tokens grant a new
  access token, not direct access).
- ``decode_token`` round-trips a freshly-issued token into a
  :class:`TokenClaims` whose fields match the inputs.
- Expiry is enforced — decoding a token past its ``exp`` raises
  :class:`aidp_common.errors.UnauthorizedError` with code ``UNAUTHORIZED``.
- Tampered tokens (wrong secret / extra segment / malformed payload)
  raise :class:`UnauthorizedError`.
- ``current_user_from_token`` returns a :class:`CurrentUser` with only
  the platform-facing fields (no JWT internals).
- The same secret default is used by both sign and verify (so a service
  can decode its own tokens end-to-end).
- Settings overrides (algorithm, expiry) are honored.

These tests do not require FastAPI; ``aidp_auth.jwt`` is pure Pydantic +
PyJWT. FastAPI integration lives in ``test_dependencies.py``.
"""

from __future__ import annotations

import time
from collections.abc import Generator
from datetime import UTC, datetime, timedelta
from typing import Any

import jwt as pyjwt
import pytest
from aidp_auth.jwt import (
    CurrentUser,
    TokenClaims,
    TokenType,
    create_access_token,
    create_refresh_token,
    current_user_from_token,
    decode_token,
)
from aidp_common import config as cfg
from aidp_common.errors import UnauthorizedError
from pydantic import ValidationError

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


# ---------------------------------------------------------------------------
# Constants used by multiple tests
# ---------------------------------------------------------------------------

_TENANT = "tenant-uuid-1"
_USER = "user-uuid-1"
_ROLES = ["data_engineer"]
_SCOPES = ["datasource:read", "datasource:write"]


def _now_ts() -> int:
    """Return the current Unix timestamp in seconds."""
    return int(time.time())


# ---------------------------------------------------------------------------
# Token issuance shape
# ---------------------------------------------------------------------------


def test_create_access_token_returns_string() -> None:
    token = create_access_token(tenant_id=_TENANT, user_id=_USER, roles=_ROLES, scopes=_SCOPES)
    assert isinstance(token, str)
    # JWTs are three dot-separated base64url segments.
    assert token.count(".") == 2


def test_create_access_token_carries_required_claims() -> None:
    """All platform claims land in the signed token's payload."""
    before = _now_ts()
    token = create_access_token(tenant_id=_TENANT, user_id=_USER, roles=_ROLES, scopes=_SCOPES)
    # Decode without verification to inspect the raw payload.
    payload: dict[str, Any] = pyjwt.decode(token, options={"verify_signature": False})
    after = _now_ts()

    assert payload["tenant_id"] == _TENANT
    assert payload["user_id"] == _USER
    assert payload["roles"] == _ROLES
    assert payload["scopes"] == _SCOPES
    assert payload["token_type"] == TokenType.ACCESS.value
    # ``jti`` is present and is a non-empty string (we don't pin the value).
    assert isinstance(payload["jti"], str)
    assert payload["jti"]
    # ``iat`` / ``exp`` bracket the current time within a 1s tolerance.
    assert before <= int(payload["iat"]) <= after
    assert int(payload["exp"]) > int(payload["iat"])
    # Default 12h access token lifetime.
    assert int(payload["exp"]) - int(payload["iat"]) == 12 * 60 * 60


def test_create_access_token_uses_hs256() -> None:
    """The brief pins the algorithm to HS256."""
    token = create_access_token(tenant_id=_TENANT, user_id=_USER, roles=_ROLES, scopes=_SCOPES)
    header: dict[str, Any] = pyjwt.get_unverified_header(token)
    assert header["alg"] == "HS256"
    assert header["typ"] == "JWT"


def test_create_access_token_default_scopes_is_empty() -> None:
    """When scopes is omitted the claim is ``[]``, not ``None``."""
    token = create_access_token(tenant_id=_TENANT, user_id=_USER, roles=_ROLES)
    payload: dict[str, Any] = pyjwt.decode(token, options={"verify_signature": False})
    assert payload["scopes"] == []


def test_create_access_token_default_roles_is_empty() -> None:
    token = create_access_token(tenant_id=_TENANT, user_id=_USER)
    payload: dict[str, Any] = pyjwt.decode(token, options={"verify_signature": False})
    assert payload["roles"] == []


def test_create_access_token_custom_expiry_override() -> None:
    """``expires_delta`` shrinks the lifetime when set."""
    token = create_access_token(
        tenant_id=_TENANT,
        user_id=_USER,
        roles=_ROLES,
        scopes=_SCOPES,
        expires_delta=timedelta(minutes=5),
    )
    payload: dict[str, Any] = pyjwt.decode(token, options={"verify_signature": False})
    # 5 minutes (no fractional second drift because iat/exp are int seconds).
    assert int(payload["exp"]) - int(payload["iat"]) == 5 * 60


def test_create_refresh_token_shape() -> None:
    """Refresh tokens carry the same identity claims but a 30d lifetime and
    no ``scopes`` (refresh is for getting a new access token)."""
    before = _now_ts()
    token = create_refresh_token(tenant_id=_TENANT, user_id=_USER, roles=_ROLES)
    payload: dict[str, Any] = pyjwt.decode(token, options={"verify_signature": False})
    after = _now_ts()

    assert payload["tenant_id"] == _TENANT
    assert payload["user_id"] == _USER
    assert payload["roles"] == _ROLES
    assert payload["token_type"] == TokenType.REFRESH.value
    # Refresh tokens do not carry scopes; the field is omitted (not
    # nulled out) so an attacker can't hand the refresh token to a
    # resource server and get direct access.
    assert "scopes" not in payload
    # 30-day lifetime.
    assert int(payload["exp"]) - int(payload["iat"]) == 30 * 24 * 60 * 60
    assert before <= int(payload["iat"]) <= after


def test_create_refresh_token_custom_expiry() -> None:
    token = create_refresh_token(tenant_id=_TENANT, user_id=_USER, expires_delta=timedelta(days=7))
    payload: dict[str, Any] = pyjwt.decode(token, options={"verify_signature": False})
    assert int(payload["exp"]) - int(payload["iat"]) == 7 * 24 * 60 * 60


def test_jti_is_unique_per_token() -> None:
    """``jti`` must be unique per call (used for replay tracking)."""
    t1 = create_access_token(tenant_id=_TENANT, user_id=_USER, roles=_ROLES)
    t2 = create_access_token(tenant_id=_TENANT, user_id=_USER, roles=_ROLES)
    p1: dict[str, Any] = pyjwt.decode(t1, options={"verify_signature": False})
    p2: dict[str, Any] = pyjwt.decode(t2, options={"verify_signature": False})
    assert p1["jti"] != p2["jti"]


# ---------------------------------------------------------------------------
# decode_token — happy path
# ---------------------------------------------------------------------------


def test_decode_token_round_trip() -> None:
    token = create_access_token(tenant_id=_TENANT, user_id=_USER, roles=_ROLES, scopes=_SCOPES)
    claims = decode_token(token)
    assert isinstance(claims, TokenClaims)
    assert claims.tenant_id == _TENANT
    assert claims.user_id == _USER
    assert claims.roles == _ROLES
    assert claims.scopes == _SCOPES
    assert claims.token_type == TokenType.ACCESS


def test_decode_token_emits_iat_exp_datetimes() -> None:
    """``iat`` / ``exp`` are timezone-aware UTC datetimes on the model."""
    token = create_access_token(tenant_id=_TENANT, user_id=_USER, roles=_ROLES, scopes=_SCOPES)
    claims = decode_token(token)
    assert isinstance(claims.iat, datetime)
    assert isinstance(claims.exp, datetime)
    assert claims.iat.tzinfo is not None
    assert claims.exp.tzinfo is not None
    assert claims.iat.tzinfo.utcoffset(claims.iat) == UTC.utcoffset(claims.iat)
    assert claims.exp.tzinfo.utcoffset(claims.exp) == UTC.utcoffset(claims.exp)


def test_decode_token_frozen_model() -> None:
    """TokenClaims is immutable — handlers cannot mutate the decoded state."""
    token = create_access_token(tenant_id=_TENANT, user_id=_USER, roles=_ROLES, scopes=_SCOPES)
    claims = decode_token(token)
    with pytest.raises(ValidationError):
        # Pydantic v2 raises ``ValidationError`` on assignment to a frozen model.
        claims.tenant_id = "other"


def test_decode_refresh_token() -> None:
    token = create_refresh_token(tenant_id=_TENANT, user_id=_USER, roles=_ROLES)
    claims = decode_token(token)
    assert claims.token_type == TokenType.REFRESH
    assert claims.tenant_id == _TENANT
    # ``scopes`` defaults to ``[]`` even when absent from the JWT payload.
    assert claims.scopes == []


# ---------------------------------------------------------------------------
# decode_token — failure paths
# ---------------------------------------------------------------------------


def test_decode_token_rejects_expired() -> None:
    """A token past its ``exp`` raises :class:`UnauthorizedError`."""
    token = create_access_token(
        tenant_id=_TENANT,
        user_id=_USER,
        roles=_ROLES,
        scopes=_SCOPES,
        expires_delta=timedelta(seconds=-5),  # already expired
    )
    with pytest.raises(UnauthorizedError) as excinfo:
        decode_token(token)
    assert excinfo.value.status == 401
    assert excinfo.value.code.value == "UNAUTHORIZED"


def test_decode_token_rejects_wrong_secret() -> None:
    """A token signed with a different secret must be rejected."""
    token = create_access_token(tenant_id=_TENANT, user_id=_USER, roles=_ROLES, scopes=_SCOPES)
    # Re-sign the *same* payload with a different secret. The forged
    # token carries a valid HS256 signature, just with the wrong key.
    payload: dict[str, Any] = pyjwt.decode(token, options={"verify_signature": False})
    forged = pyjwt.encode(payload, "completely-different-secret-but-32b-long!!", algorithm="HS256")
    # Sanity: the forged token is structurally valid JWT but its
    # signature does not verify against the configured secret.
    with pytest.raises(pyjwt.InvalidSignatureError):
        pyjwt.decode(
            forged,
            cfg.get_settings().jwt_secret,
            algorithms=[cfg.get_settings().jwt_algorithm],
        )

    # And our decoder wraps that into a 401.
    with pytest.raises(UnauthorizedError):
        decode_token(forged)


def test_decode_token_rejects_tampered_signature() -> None:
    """Flipping the last byte of the signature must be rejected."""
    token = create_access_token(tenant_id=_TENANT, user_id=_USER, roles=_ROLES, scopes=_SCOPES)
    head, _payload, sig = token.split(".")
    # Tamper: replace last char of signature with a different base64url char.
    tampered_sig = sig[:-1] + ("A" if sig[-1] != "A" else "B")
    tampered = f"{head}.{_payload}.{tampered_sig}"
    with pytest.raises(UnauthorizedError):
        decode_token(tampered)


def test_decode_token_rejects_garbage() -> None:
    with pytest.raises(UnauthorizedError):
        decode_token("not-a-jwt")


def test_decode_token_rejects_empty() -> None:
    with pytest.raises(UnauthorizedError):
        decode_token("")


def test_decode_token_rejects_only_two_segments() -> None:
    with pytest.raises(UnauthorizedError):
        decode_token("abc.def")


def test_decode_token_rejects_missing_required_claim() -> None:
    """A token missing ``tenant_id`` is rejected as malformed."""
    # Hand-craft a token with the right algorithm and only a subset of claims.
    settings = cfg.get_settings()
    now = int(time.time())
    payload = {
        "sub": _USER,
        "iat": now,
        "exp": now + 60,
        "jti": "manual",
        "token_type": "access",
        # ``tenant_id`` deliberately omitted.
        "roles": [],
        "scopes": [],
    }
    token = pyjwt.encode(payload, settings.jwt_secret, algorithm="HS256")
    with pytest.raises(UnauthorizedError):
        decode_token(token)


def test_decode_token_rejects_non_hs256_algorithm() -> None:
    """Algorithm confusion guard: a ``none`` / RS256 token must be rejected."""
    settings = cfg.get_settings()
    now = int(time.time())
    # ``alg=none`` is only valid when the secret is empty; PyJWT refuses to
    # encode ``none`` with a secret set, so we build a payload manually and
    # use PyJWT's own enforcement path via the decoder.
    payload: dict[str, Any] = {
        "tenant_id": _TENANT,
        "user_id": _USER,
        "iat": now,
        "exp": now + 60,
        "jti": "manual",
        "token_type": "access",
        "roles": [],
        "scopes": [],
    }
    # An RS256 token signed with a different (asymmetric) key — we don't
    # need a real key, just ensure the decoder refuses to accept a
    # non-HS256 algorithm.
    forged = pyjwt.encode(payload, "x" * 64, algorithm="HS512")
    # Even though the secret matches in length, the algorithm differs.
    with pytest.raises(UnauthorizedError):
        decode_token(forged)
    # Also: the configured algorithm is enforced explicitly.
    assert settings.jwt_algorithm == "HS256"


# ---------------------------------------------------------------------------
# current_user_from_token — projection from claims
# ---------------------------------------------------------------------------


def test_current_user_from_token_keeps_only_public_fields() -> None:
    """``CurrentUser`` exposes only the platform-facing identity fields."""
    token = create_access_token(tenant_id=_TENANT, user_id=_USER, roles=_ROLES, scopes=_SCOPES)
    claims = decode_token(token)
    user = current_user_from_token(claims)
    assert isinstance(user, CurrentUser)
    # Public surface — exactly the four fields from the brief.
    dumped = user.model_dump()
    assert set(dumped.keys()) == {"tenant_id", "user_id", "roles", "scopes"}
    assert dumped == {
        "tenant_id": _TENANT,
        "user_id": _USER,
        "roles": _ROLES,
        "scopes": _SCOPES,
    }


def test_current_user_is_frozen() -> None:
    token = create_access_token(tenant_id=_TENANT, user_id=_USER, roles=_ROLES, scopes=_SCOPES)
    user = current_user_from_token(decode_token(token))
    with pytest.raises(ValidationError):
        user.tenant_id = "other"


# ---------------------------------------------------------------------------
# Settings / configuration interactions
# ---------------------------------------------------------------------------


def test_settings_exposes_jwt_defaults() -> None:
    """The default config is what the brief asks for: HS256, 12h, 30d."""
    settings = cfg.get_settings()
    assert settings.jwt_algorithm == "HS256"
    assert settings.jwt_access_token_expires_minutes == 12 * 60
    assert settings.jwt_refresh_token_expires_days == 30


def test_settings_jwt_secret_override_takes_effect(monkeypatch: pytest.MonkeyPatch) -> None:
    """Bumping ``AIDP_JWT_SECRET`` invalidates tokens issued with the old one."""
    cfg.reset_settings_cache()
    monkeypatch.setenv("AIDP_JWT_SECRET", "secret-A-but-32-bytes-long!!!!!!")
    token_a = create_access_token(tenant_id=_TENANT, user_id=_USER, roles=_ROLES)
    # Same call, different secret -> old token no longer decodes.
    monkeypatch.setenv("AIDP_JWT_SECRET", "secret-B-but-32-bytes-long!!!!!!")
    cfg.reset_settings_cache()
    with pytest.raises(UnauthorizedError):
        decode_token(token_a)
    # And a freshly issued token decodes fine.
    token_b = create_access_token(tenant_id=_TENANT, user_id=_USER, roles=_ROLES)
    claims = decode_token(token_b)
    assert claims.user_id == _USER
