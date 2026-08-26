"""Authentication service for the IAM service.

This module owns the *business* flow for the auth endpoints. The
HTTP layer in :mod:`aidp_iam.api.auth` is a thin transport adapter;
everything database-shaped, cryptographic, or event-emitting lives
here so the same logic can be reused by scripts, CLI commands, and
background jobs.

Responsibilities
----------------

- :func:`hash_password` / :func:`verify_password` — Argon2id-backed
  password hashing with sensible defaults.
- :func:`register_tenant` — atomic ``Tenant`` + bootstrap ``User``
  + ``Role`` + ``UserRoleBinding`` insert, all in a single DB
  transaction, plus publishing the ``tenant.created`` event.
- :func:`authenticate` — email + password → user lookup, constant-time
  Argon2 verify, ``last_login_at`` update, ``User not active`` check.
- :func:`issue_token_pair` — mint a new access + refresh JWT pair and
  persist the refresh-token hash to :class:`aidp_iam.models.Session`
  so the server can revoke / rotate it later.
- :func:`refresh_tokens` — verify a refresh token, look up its
  :class:`Session`, rotate the session (revoke old + create new),
  mint a fresh pair.
- :func:`revoke_session` — soft-revoke a session by its
  ``refresh_token_hash`` (used by logout).
- :func:`publish_audit_event` — best-effort ``iam.user.logged_in``
  audit event publication. Failures are logged but never propagated
  to the auth caller (audit is a side-effect, not a precondition).

Security notes
--------------

- Passwords are hashed with Argon2id via :mod:`argon2`. We use the
  default parameters (``PasswordHasher()``) which are the OWASP-recommended
  minimum as of this writing.
- Refresh tokens are JWTs that we *also* hash with Argon2id before
  persisting. The plaintext lives only in the response body; the
  database never sees it.
- Token rotation: :func:`refresh_tokens` revokes the old
  :class:`Session` row before issuing a new pair. This means a stolen
  refresh token can only be used once (race-on-replay protection).
- Errors are deliberately *not* the same for "user not found" and
  "bad password" — both surface as
  :class:`aidp_common.errors.UnauthorizedError` so the API does not
  leak which email addresses are registered. The Argon2 verify path
  is constant-time and uses a stub hash for unknown emails so the
  timing leak is also minimized.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Awaitable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from aidp_auth.jwt import (
    CurrentUser,
    TokenClaims,
    TokenType,
    create_access_token,
    create_refresh_token,
    decode_token,
)
from aidp_common.config import get_settings
from aidp_common.errors import (
    AppError,
    ConflictError,
    ErrorCode,
    UnauthorizedError,
    ValidationError,
)
from aidp_db.session import get_session
from aidp_db.tenant import reset_tenant_context, set_tenant_context
from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session as SqlSession

from aidp_iam.models import Session as SessionModel
from aidp_iam.models import Tenant, User, UserRoleBinding

_LOG = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: Bootstrap admin role code (per Task 8 brief: "第一个用户自动是 admin role").
#: A matching :class:`Role` row is created at tenant-registration time.
ADMIN_ROLE_CODE = "admin"

#: Bootstrap admin role's permission set — wildcard so the admin can
#: do everything within their own tenant. Other roles default to ``[]``.
ADMIN_ROLE_PERMISSIONS: list[str] = ["*"]

#: Stub Argon2id hash used as a decoy for "user not found" — keeps the
#: verify path constant-time so timing leaks do not disclose which
#: emails are registered. The hash is for the value ``"decoy-password"``;
#: we never look it up, only call :meth:`PasswordHasher.verify` on it.
_DECOY_HASH = (
    "$argon2id$v=19$m=65536,t=3,p=4$ZGVjb3ktcGFzc3dvcmQ$k+b6y9a+wSqYfGxTz4b3wF1I2K2HQyKqv+Jx0qY6FVk"
)

#: Audit-event topic and event_type.
_AUDIT_TOPIC = "iam.audit"
_AUDIT_EVENT_TYPE_LOGIN = "iam.user.logged_in"
_AUDIT_EVENT_TYPE_LOGOUT = "iam.user.logged_out"
_AUDIT_EVENT_TYPE_TENANT_CREATED = "iam.tenant.created"

#: Per-request :class:`PasswordHasher`. We hold a single instance
#: because :class:`PasswordHasher` is itself stateless (parameters are
#: encoded in the hash header).
_HASHER: PasswordHasher = PasswordHasher()


# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TokenPairResult:
    """The result of a successful login / refresh.

    Holds the public JWT strings (sent to the client), their ``exp``
    timestamps, and the server-side :class:`Session` row that owns
    the refresh token. The :attr:`expires_in` field is the access
    token lifetime in seconds — handy for clients that want to
    schedule a refresh.
    """

    access_token: str
    refresh_token: str
    expires_in: int
    access_exp: datetime
    refresh_exp: datetime
    session: SessionModel

    def to_dict(self) -> dict[str, Any]:
        """Return the public token pair as a dict (for response models)."""
        return {
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "token_type": "Bearer",
            "expires_in": self.expires_in,
        }


@dataclass(frozen=True)
class AuthenticatedUser:
    """The internal "user who just authenticated" view.

    Mirrors :class:`aidp_auth.jwt.CurrentUser` but also carries
    the :class:`aidp_iam.models.User` ORM row so the auth flow has
    direct access to ``last_login_at`` etc. The :meth:`to_user_info`
    helper projects it onto the public :class:`aidp_iam.schemas.UserInfo`
    response shape.
    """

    user: User
    roles: list[str]
    scopes: list[str]
    tenant_code: str

    def to_current_user(self) -> CurrentUser:
        """Project into a :class:`CurrentUser` for token claims."""
        return CurrentUser(
            tenant_id=self.user.tenant_id,
            user_id=self.user.id,
            roles=list(self.roles),
            scopes=list(self.scopes),
        )

    def to_user_info(self) -> dict[str, Any]:
        """Project onto the :class:`aidp_iam.schemas.UserInfo` shape."""
        return {
            "id": self.user.id,
            "tenant_id": self.user.tenant_id,
            "username": self.user.username,
            "email": self.user.email,
            "display_name": self.user.display_name,
            "status": self.user.status,
            "mfa_enabled": self.user.mfa_enabled,
            "roles": list(self.roles),
            "scopes": list(self.scopes),
            "last_login_at": self.user.last_login_at,
            "created_at": self.user.created_at,
        }


# ---------------------------------------------------------------------------
# Password hashing
# ---------------------------------------------------------------------------


def hash_password(plaintext: str) -> str:
    """Hash *plaintext* with Argon2id and return the encoded string.

    The returned value already includes the salt and parameter block,
    so a single column (``users.password_hash``) carries everything
    needed for verification.

    Args:
        plaintext: The raw password as supplied by the user.

    Returns:
        The Argon2id-encoded hash (PHC string format).

    Raises:
        ValueError: When *plaintext* is empty.
    """
    if not plaintext:
        raise ValueError("plaintext must be a non-empty string")
    return _HASHER.hash(plaintext)


def verify_password(stored_hash: str, plaintext: str) -> bool:
    """Verify *plaintext* against the Argon2id *stored_hash*.

    The function never raises on a bad password; it returns
    ``False``. :class:`argon2.exceptions.VerifyMismatchError` (and the
    other Argon2 exception types) are caught and re-mapped to
    ``False`` because a caller asking "does this match?" should not
    have to know about the library's exception hierarchy.

    Args:
        stored_hash: The Argon2id-encoded hash previously produced by
            :func:`hash_password` (or any compatible encoder).
        plaintext: The raw password to check.

    Returns:
        ``True`` if *plaintext* matches the hash, ``False`` otherwise.
        Also returns ``False`` when the hash is malformed (so a
        corrupted row behaves like a wrong password rather than
        crashing the request).
    """
    if not stored_hash or not plaintext:
        return False
    try:
        return _HASHER.verify(stored_hash, plaintext)
    except VerifyMismatchError:
        return False
    except Exception:  # pragma: no cover - defensive: invalid hash format etc.
        _LOG.exception("password verification failed unexpectedly")
        return False


def hash_refresh_token(token: str) -> str:
    """Hash a refresh token for storage in :class:`SessionModel.refresh_token_hash`.

    The refresh token is itself a JWT, so we hash the JWT string (not
    the underlying claims) with Argon2id. This keeps the database
    row opaque — even with full read access to the ``sessions`` table,
    an attacker cannot replay a refresh token.

    Args:
        token: The raw refresh JWT (as sent to the client).

    Returns:
        The Argon2id-encoded hash of the JWT.
    """
    if not token:
        raise ValueError("token must be a non-empty string")
    return _HASHER.hash(token)


def verify_refresh_token(stored_hash: str, token: str) -> bool:
    """Verify *token* against the Argon2id *stored_hash*.

    Mirrors :func:`verify_password` for the refresh-token context.
    """
    if not stored_hash or not token:
        return False
    try:
        return _HASHER.verify(stored_hash, token)
    except VerifyMismatchError:
        return False
    except Exception:  # pragma: no cover - defensive
        _LOG.exception("refresh-token verification failed unexpectedly")
        return False


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _normalize_email(value: str) -> str:
    """Lowercase + strip an email; matches :class:`User` lookup contract."""
    return value.strip().lower()


def _get_user_roles_scopes(
    session: SqlSession, user_id: str, tenant_id: str
) -> tuple[list[str], list[str]]:
    """Return ``(roles, scopes)`` for *user_id* by joining bindings + roles.

    The query pulls every :class:`UserRoleBinding` for the user, then
    aggregates the bound :class:`Role` codes and permission sets.
    Duplicate ``codes`` are deduplicated. ``scopes`` is the union of
    all ``Role.permissions`` arrays (the ``"*"`` wildcard is preserved
    so downstream :func:`aidp_auth.dependencies._user_has_permission`
    can match it).

    Note: this function does *not* set a tenant context; callers are
    expected to do so before invoking. The query intentionally
    filters by ``tenant_id`` as a defense in depth even though the L1
    listener should already restrict the result set.
    """
    rows = (
        session.execute(
            select(UserRoleBinding, User)
            .join(User, UserRoleBinding.user_id == User.id)
            .where(UserRoleBinding.user_id == user_id)
            .where(UserRoleBinding.tenant_id == tenant_id)
        )
        .scalars()
        .all()
    )
    roles: set[str] = set()
    scopes: set[str] = set()
    for binding in rows:
        if binding.role is not None:
            roles.add(binding.role.code)
            for perm in binding.role.permissions or []:
                scopes.add(perm)
    return sorted(roles), sorted(scopes)


def _build_user_info_dict(
    user: User,
    roles: list[str],
    scopes: list[str],
) -> dict[str, Any]:
    """Project *user* + computed roles/scopes onto :class:`UserInfo` dict."""
    return {
        "id": user.id,
        "tenant_id": user.tenant_id,
        "username": user.username,
        "email": user.email,
        "display_name": user.display_name,
        "status": user.status,
        "mfa_enabled": user.mfa_enabled,
        "roles": list(roles),
        "scopes": list(scopes),
        "last_login_at": user.last_login_at,
        "created_at": user.created_at,
    }


async def _maybe_await(value: Any) -> Any:
    """Await *value* if it is awaitable; return it as-is otherwise.

    The audit-publish path runs against a real Kafka producer in
    production (so the return is an awaitable) and against the
    in-memory fake in tests (which exposes a coroutine). Both
    shapes are handled by a single ``await`` on the return value.
    """
    if isinstance(value, Awaitable):
        return await value
    return value


async def publish_audit_event(
    *,
    event_type: str,
    tenant_id: str,
    payload: dict[str, Any],
    transport: Any = None,
) -> None:
    """Best-effort publish of an audit event.

    The auth flow never fails because an audit publish failed —
    audit is a side-effect, not a precondition. We log the error
    and move on. The transport is injectable so tests can capture
    emitted events without spinning up Kafka.

    Args:
        event_type: The reverse-DNS event name (e.g.
            ``"iam.user.logged_in"``).
        tenant_id: The tenant the event belongs to.
        payload: Business payload (JSON-compatible dict).
        transport: Optional transport override; falls back to the
            process-wide default from :mod:`aidp_events.producer`.
    """
    from aidp_events.producer import publish_event

    # ``set_tenant_context`` ensures ``publish_event`` finds a tenant
    # when the caller did not pass one explicitly. We restore the
    # previous context on exit so the publish does not leak into the
    # calling request.
    prev_token = set_tenant_context(tenant_id)
    try:
        try:
            await publish_event(
                topic=_AUDIT_TOPIC,
                event_type=event_type,
                payload=payload,
                tenant_id=tenant_id,
                transport=transport,
            )
        except Exception as exc:  # pragma: no cover - defensive: never block auth
            _LOG.warning(
                "audit event publish failed; continuing",
                extra={
                    "event_type": event_type,
                    "tenant_id": tenant_id,
                    "error": str(exc),
                },
                exc_info=True,
            )
    finally:
        # Reset is a no-op when nothing was set, but we do it
        # unconditionally to be safe.
        from aidp_db.tenant import reset_tenant_context

        reset_tenant_context(prev_token)


# ---------------------------------------------------------------------------
# register_tenant
# ---------------------------------------------------------------------------


def register_tenant(
    *,
    tenant_code: str,
    tenant_name: str,
    admin_email: str,
    admin_password: str,
    admin_username: str | None,
    admin_display_name: str | None,
    tenant_plan: str = "team",
    tenant_region: str = "us-east-1",
) -> dict[str, Any]:
    """Atomically create a tenant, its admin user, and the admin role binding.

    The function runs in a single SQLAlchemy transaction. If any
    step fails the entire tenant creation rolls back; the caller
    never sees a half-built tenant.

    Note on audit publishing
    ------------------------

    The ``iam.tenant.created`` audit event is published by the API
    layer in :mod:`aidp_iam.api.auth` *after* this function returns.
    Keeping the function synchronous makes it directly callable from
    CLI scripts, background jobs, and tests (which do not need an
    event loop), and the audit publish follows the same pattern as
    the login and logout handlers in the API module.

    Args:
        tenant_code: Stable business identifier. Already validated
            by :class:`aidp_iam.schemas.RegisterTenantRequest`.
        tenant_name: Human-readable display name.
        admin_email: Bootstrap admin email. Lowercased internally.
        admin_password: Plaintext; hashed with Argon2id before
            persistence.
        admin_username: Optional explicit username. Defaults to the
            local part of *admin_email* when ``None``.
        admin_display_name: Optional human display name.
        tenant_plan: Plan code. Defaults to ``"team"``.
        tenant_region: Region code. Defaults to ``"us-east-1"``.

    Returns:
        A dict with the new tenant id, the admin user, and the
        token pair. The HTTP layer projects this onto
        :class:`aidp_iam.schemas.TenantCreatedResponse` and is
        also responsible for publishing the corresponding audit
        event.

    Raises:
        ConflictError: When the tenant code or admin email is already
            registered.
        AppError: On any other database failure.
    """
    email = _normalize_email(admin_email)
    username = admin_username or email.split("@", 1)[0]
    if not username:
        # Last-ditch: UUID4 fallback. Should never hit because the
        # email regex requires at least one char before ``@``.
        username = f"admin-{uuid.uuid4().hex[:8]}"

    with get_session() as session:
        # 1. Tenant
        tenant = Tenant(
            code=tenant_code,
            name=tenant_name,
            plan=tenant_plan,
            region=tenant_region,
            status="active",
        )
        session.add(tenant)
        try:
            session.flush()
        except IntegrityError as exc:
            session.rollback()
            raise ConflictError(
                f"tenant code {tenant_code!r} already exists",
            ) from exc

        # 2. Admin user
        user = User(
            tenant_id=tenant.id,
            username=username,
            email=email,
            display_name=admin_display_name,
            status="active",
            mfa_enabled=False,
            password_hash=hash_password(admin_password),
        )
        session.add(user)
        try:
            session.flush()
        except IntegrityError as exc:
            session.rollback()
            raise ConflictError(
                f"admin user {email!r} or username {username!r} already exists "
                f"in tenant {tenant_code!r}",
            ) from exc

        # 3. Admin role (one per tenant) + binding
        from aidp_iam.models import Role  # local import to avoid cycle

        admin_role = Role(
            tenant_id=tenant.id,
            code=ADMIN_ROLE_CODE,
            name="Administrator",
            scope="tenant",
            permissions=list(ADMIN_ROLE_PERMISSIONS),
        )
        session.add(admin_role)
        try:
            session.flush()
        except IntegrityError:  # pragma: no cover - races are vanishingly rare
            session.rollback()
            raise

        session.add(
            UserRoleBinding(
                tenant_id=tenant.id,
                user_id=user.id,
                role_id=admin_role.id,
                scope_type="tenant",
            )
        )
        session.flush()

        # 4. Issue first token pair
        # We need the session id; build the pair and persist the
        # session row in the same transaction.
        pair = _issue_and_persist(
            session=session,
            user=user,
            roles=[ADMIN_ROLE_CODE],
            scopes=list(ADMIN_ROLE_PERMISSIONS),
        )
        # ``session.commit()`` happens at the end of the ``get_session``
        # block; the :class:`SessionModel` row is already added.

        # 5. Snapshot the data we want to return (the ORM session
        # closes on exit and we want detached objects).
        return {
            "tenant_id": tenant.id,
            "tenant_code": tenant.code,
            "tenant_name": tenant.name,
            "user": _build_user_info_dict(
                user,
                roles=[ADMIN_ROLE_CODE],
                scopes=list(ADMIN_ROLE_PERMISSIONS),
            ),
            "token": pair.to_dict(),
        }


# ---------------------------------------------------------------------------
# authenticate
# ---------------------------------------------------------------------------


def authenticate(
    *,
    email: str,
    password: str,
    tenant_code: str | None = None,
) -> AuthenticatedUser:
    """Verify *email* + *password* and return the authenticated user.

    The function:

    1. Looks up the user by email (case-insensitive). If a
       ``tenant_code`` is supplied, the lookup is scoped to that
       tenant; otherwise the function expects a unique match across
       tenants and refuses on ambiguity.
    2. Verifies the password (Argon2id). When the user does not
       exist, a decoy Argon2 verify is run against
       :data:`_DECOY_HASH` so the response time is the same as for
       a real user with a wrong password.
    3. Rejects ``status`` other than ``"active"`` with
       :class:`UnauthorizedError`.
    4. Updates ``last_login_at`` to the current UTC time.

    Args:
        email: The user's email (case-insensitive).
        password: The plaintext password.
        tenant_code: Optional disambiguator for cross-tenant email
            duplicates.

    Returns:
        An :class:`AuthenticatedUser` carrying the :class:`User`
        row plus resolved ``roles`` and ``scopes``.

    Raises:
        UnauthorizedError: When the email is unknown, the password
            is wrong, the user is not active, or the email is
            ambiguous across tenants without a hint.
        ValidationError: When the email is malformed (Pydantic).
    """
    email = _normalize_email(email)
    if not email:
        raise ValidationError("email must be a non-empty string")
    if not password:
        raise UnauthorizedError("invalid credentials")

    with get_session() as session:
        stmt = (
            select(User, Tenant)
            .join(Tenant, User.tenant_id == Tenant.id)
            .where(User.email == email)
        )
        if tenant_code is not None:
            stmt = stmt.where(Tenant.code == tenant_code)

        rows = session.execute(stmt).all()

        if len(rows) == 0:
            # Constant-time decoy verify.
            verify_password(_DECOY_HASH, password)
            raise UnauthorizedError("invalid credentials")
        if len(rows) > 1 and tenant_code is None:
            # Ambiguous: refuse rather than silently pick one to avoid
            # leaking which tenants share the email.
            raise UnauthorizedError("invalid credentials")

        user, tenant = rows[0]

        if user.status != "active":
            # Still run the verify to keep timing constant.
            verify_password(user.password_hash, password)
            raise UnauthorizedError("account is not active")

        if not verify_password(user.password_hash, password):
            raise UnauthorizedError("invalid credentials")

        # Update last_login_at and pull roles/scopes.
        user.last_login_at = datetime.now(UTC)
        roles, scopes = _get_user_roles_scopes(
            session=session, user_id=user.id, tenant_id=user.tenant_id
        )
        session.flush()
        # Detach the user so the caller can read attributes outside
        # the session without lazy-load errors.
        session.expunge(user)

        return AuthenticatedUser(
            user=user,
            roles=roles,
            scopes=scopes,
            tenant_code=tenant.code,
        )


# ---------------------------------------------------------------------------
# Token issuance
# ---------------------------------------------------------------------------


def _issue_and_persist(
    *,
    session: SqlSession,
    user: User,
    roles: list[str],
    scopes: list[str],
) -> TokenPairResult:
    """Mint a new access + refresh pair and persist the refresh session.

    Internal helper. Callers own *session* (it must be open and
    writeable); the function adds a :class:`SessionModel` row to it
    and returns the new :class:`TokenPairResult` (which references
    the not-yet-committed session row).

    Args:
        session: The open SQLAlchemy session.
        user: The :class:`User` for whom we mint tokens.
        roles: Coarse-grained roles to embed in the access token.
        scopes: Fine-grained scopes to embed in the access token.

    Returns:
        A :class:`TokenPairResult` carrying the JWT strings,
        their expiry timestamps, and the (uncommitted)
        :class:`SessionModel` row.
    """
    settings = get_settings()
    access_ttl = timedelta(minutes=settings.jwt_access_token_expires_minutes)
    refresh_ttl = timedelta(days=settings.jwt_refresh_token_expires_days)

    now = datetime.now(UTC)
    access_exp = now + access_ttl
    refresh_exp = now + refresh_ttl

    access_token = create_access_token(
        tenant_id=user.tenant_id,
        user_id=user.id,
        roles=roles,
        scopes=scopes,
    )
    refresh_token = create_refresh_token(
        tenant_id=user.tenant_id,
        user_id=user.id,
        roles=roles,
    )

    # Persist the refresh-token hash so the server can verify
    # / revoke it on subsequent calls.
    sess_row = SessionModel(
        tenant_id=user.tenant_id,
        user_id=user.id,
        refresh_token_hash=hash_refresh_token(refresh_token),
        expires_at=refresh_exp,
        mfa_passed=False,
    )
    session.add(sess_row)
    session.flush()

    return TokenPairResult(
        access_token=access_token,
        refresh_token=refresh_token,
        expires_in=int(access_ttl.total_seconds()),
        access_exp=access_exp,
        refresh_exp=refresh_exp,
        session=sess_row,
    )


def issue_token_pair(
    *,
    authed: AuthenticatedUser,
) -> TokenPairResult:
    """Mint a new access + refresh token pair for *authed* and persist it.

    This is the path called by :func:`aidp_iam.api.auth.login` after
    a successful :func:`authenticate`. The new :class:`SessionModel`
    row is committed before the function returns.
    """
    with get_session() as session:
        # Re-attach the user inside the new session so we can persist
        # the session row with a valid FK. ``session.get`` is a cheap
        # PK lookup; we keep the in-memory ``authed.user`` detached
        # for the caller's use.
        user_in_db = session.get(User, authed.user.id)
        if user_in_db is None:
            # Should not happen — the user was just authenticated.
            raise AppError(
                ErrorCode.INTERNAL,
                "authenticated user vanished before token issuance",
                status=500,
            )
        return _issue_and_persist(
            session=session,
            user=user_in_db,
            roles=authed.roles,
            scopes=authed.scopes,
        )


# ---------------------------------------------------------------------------
# refresh / logout
# ---------------------------------------------------------------------------


def refresh_tokens(
    *,
    refresh_token: str,
) -> tuple[AuthenticatedUser, TokenPairResult]:
    """Rotate a refresh token: verify → revoke old → issue new.

    The function is the only place where a refresh token transitions
    from "active" to "revoked + new". The rotation is atomic — the
    new session row is added and the old one is marked ``revoked_at``
    in a single transaction. Concurrent refreshes of the same
    token race: the second one finds the old session already
    revoked and fails with :class:`UnauthorizedError`.

    Args:
        refresh_token: The JWT refresh token to rotate.

    Returns:
        A tuple of ``(AuthenticatedUser, TokenPairResult)``.

    Raises:
        UnauthorizedError: When the token is missing, malformed,
            has an invalid signature, has expired, or its backing
            :class:`SessionModel` row is missing or already
            revoked.
    """
    if not refresh_token:
        raise UnauthorizedError("missing refresh token")

    # 1. Decode the JWT (signature + expiry + claim shape).
    claims = decode_token(refresh_token)
    if claims.token_type is not TokenType.REFRESH:
        raise UnauthorizedError("token is not a refresh token")

    # 1a. Bind the tenant context to the claim's tenant for the
    # duration of this function. ``claims.tenant_id`` is the source
    # of truth for which session we look up; the L1 tenant listener
    # would otherwise inject a ``WHERE tenant_id = :tid`` based on
    # whatever context the caller has bound, which can contradict
    # the explicit ``WHERE tenant_id = claims.tenant_id`` below and
    # silently return zero rows. Pinning the context to the claim
    # keeps the query and the listener filter aligned.
    ctx_token = set_tenant_context(claims.tenant_id)
    try:
        return _refresh_tokens_locked(refresh_token=refresh_token, claims=claims)
    finally:
        reset_tenant_context(ctx_token)


def _refresh_tokens_locked(
    *,
    refresh_token: str,
    claims: TokenClaims,
) -> tuple[AuthenticatedUser, TokenPairResult]:
    """Body of :func:`refresh_tokens`; assumes tenant context is already bound.

    Split out from :func:`refresh_tokens` so the caller can guarantee
    the tenant context is set before any DB query runs and reset on
    exit, regardless of which path raised. The :func:`refresh_tokens`
    public entry point is the only legitimate caller; downstream code
    should use that one.
    """
    # 2. Look up the backing session.
    with get_session() as session:
        # We can't filter by ``refresh_token_hash`` directly because
        # Argon2id hashes are salted and a constant lookup is
        # impossible. Instead, fetch all active sessions for the
        # (tenant, user) pair and verify against each.
        candidates = (
            session.execute(
                select(SessionModel)
                .where(SessionModel.tenant_id == claims.tenant_id)
                .where(SessionModel.user_id == claims.user_id)
                .where(SessionModel.revoked_at.is_(None))
            )
            .scalars()
            .all()
        )
        target: SessionModel | None = None
        for cand in candidates:
            if verify_refresh_token(cand.refresh_token_hash, refresh_token):
                target = cand
                break

        if target is None:
            raise UnauthorizedError("refresh token is not recognised")
        if target.is_expired:
            # Mark as revoked so the next attempt cannot even verify.
            target.revoked_at = datetime.now(UTC)
            session.flush()
            raise UnauthorizedError("refresh token has expired")

        # 3. Mark the old session as revoked.
        target.revoked_at = datetime.now(UTC)
        session.flush()

        # 4. Re-load the user + roles / scopes for the new pair.
        user = session.get(User, claims.user_id)
        if user is None:
            raise UnauthorizedError("user no longer exists")
        if user.status != "active":
            raise UnauthorizedError("account is not active")
        roles, scopes = _get_user_roles_scopes(
            session=session, user_id=user.id, tenant_id=user.tenant_id
        )

        # 5. Issue the new pair.
        pair = _issue_and_persist(
            session=session,
            user=user,
            roles=roles,
            scopes=scopes,
        )

        # Detach for the caller.
        session.expunge(user)
        tenant_code = (
            session.execute(
                select(Tenant.code).where(Tenant.id == user.tenant_id)
            ).scalar_one_or_none()
            or ""
        )
        authed = AuthenticatedUser(
            user=user,
            roles=roles,
            scopes=scopes,
            tenant_code=tenant_code,
        )
        return authed, pair


def revoke_session(*, refresh_token: str) -> bool:
    """Soft-revoke the :class:`SessionModel` that owns *refresh_token*.

    Returns ``True`` if a session was actually revoked, ``False`` if
    the token does not match any active session. The function is
    idempotent — calling it twice with the same token returns
    ``False`` the second time and does not raise.

    Args:
        refresh_token: The refresh JWT presented by the client.

    Returns:
        ``True`` if a session was newly revoked, ``False`` otherwise.
    """
    if not refresh_token:
        return False
    try:
        claims = decode_token(refresh_token)
    except AppError:
        return False
    if claims.token_type is not TokenType.REFRESH:
        return False

    with get_session() as session:
        candidates = (
            session.execute(
                select(SessionModel)
                .where(SessionModel.tenant_id == claims.tenant_id)
                .where(SessionModel.user_id == claims.user_id)
                .where(SessionModel.revoked_at.is_(None))
            )
            .scalars()
            .all()
        )
        target: SessionModel | None = None
        for cand in candidates:
            if verify_refresh_token(cand.refresh_token_hash, refresh_token):
                target = cand
                break
        if target is None:
            return False
        target.revoked_at = datetime.now(UTC)
        session.flush()
        return True


def revoke_all_sessions_for_user(*, user_id: str) -> int:
    """Revoke every active session for *user_id*.

    Returns the number of sessions that were newly revoked. Used by
    "log out everywhere" and by account-disable flows.
    """
    with get_session() as session:
        rows = (
            session.execute(
                select(SessionModel)
                .where(SessionModel.user_id == user_id)
                .where(SessionModel.revoked_at.is_(None))
            )
            .scalars()
            .all()
        )
        now = datetime.now(UTC)
        for r in rows:
            r.revoked_at = now
        session.flush()
        return len(rows)


# ---------------------------------------------------------------------------
# /me helpers
# ---------------------------------------------------------------------------


def get_user_by_id(*, user_id: str) -> AuthenticatedUser | None:
    """Look up a user by id and project roles + scopes.

    Returns ``None`` when the user does not exist or is not
    ``"active"``. The caller (the ``/me`` route) raises
    :class:`UnauthorizedError` on ``None``.
    """
    with get_session() as session:
        user = session.get(User, user_id)
        if user is None or user.status != "active":
            return None
        roles, scopes = _get_user_roles_scopes(
            session=session, user_id=user.id, tenant_id=user.tenant_id
        )
        tenant_code = (
            session.execute(
                select(Tenant.code).where(Tenant.id == user.tenant_id)
            ).scalar_one_or_none()
            or ""
        )
        session.expunge(user)
        return AuthenticatedUser(
            user=user,
            roles=roles,
            scopes=scopes,
            tenant_code=tenant_code,
        )


def user_from_claims(claims: TokenClaims) -> AuthenticatedUser | None:
    """Resolve a :class:`TokenClaims` to the corresponding :class:`User`.

    Helper for the ``/me`` route, which receives a verified
    :class:`TokenClaims` from the :data:`aidp_auth.dependencies.current_user`
    dependency. We re-read the user from the DB so any
    recent role / scope / status changes are reflected.
    """
    return get_user_by_id(user_id=claims.user_id)


def user_from_current_user(user: CurrentUser) -> AuthenticatedUser | None:
    """Resolve a :class:`aidp_auth.jwt.CurrentUser` to the corresponding :class:`User`.

    Thin wrapper used by the API layer (which has a ``CurrentUser``
    from the :data:`aidp_auth.dependencies.current_user` dependency
    but not the original :class:`TokenClaims`). Internally just
    delegates to :func:`get_user_by_id`.
    """
    return get_user_by_id(user_id=user.user_id)


__all__ = [
    "ADMIN_ROLE_CODE",
    "ADMIN_ROLE_PERMISSIONS",
    "AuthenticatedUser",
    "TokenPairResult",
    "authenticate",
    "get_user_by_id",
    "hash_password",
    "hash_refresh_token",
    "issue_token_pair",
    "publish_audit_event",
    "refresh_tokens",
    "register_tenant",
    "revoke_all_sessions_for_user",
    "revoke_session",
    "user_from_claims",
    "user_from_current_user",
    "verify_password",
    "verify_refresh_token",
]
