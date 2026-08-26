"""User + role business logic for the IAM service.

This module owns the *business* flow for the user / role / role-binding
endpoints. The HTTP layer in :mod:`aidp_iam.api.users` and
:mod:`aidp_iam.api.roles` is a thin transport adapter — every database
operation, password hash, integrity-error translation, and tenant-context
manipulation lives here so the same logic can be reused by scripts, CLI
commands, and background jobs.

Responsibilities
----------------

- :func:`create_user` — insert a :class:`aidp_iam.models.User` with an
  Argon2id-hashed password, plus optional initial role bindings. Refuses
  duplicates on ``(tenant_id, username)`` or ``(tenant_id, email)``.
- :func:`list_users` — paginated ``SELECT`` with optional ``status`` and
  ``role_id`` filters, restricted to the caller's tenant.
- :func:`get_user` — single-row lookup; returns ``None`` for cross-tenant
  access (the L1 listener guarantees this even if a caller tries to
  bypass the API layer).
- :func:`update_user` — partial update of mutable fields (``display_name``,
  ``phone``, ``status``, ``mfa_enabled``). Password changes go through
  :func:`reset_password` so the audit trail records the action separately.
- :func:`delete_user` — soft-delete (sets ``status="disabled"`` plus
  ``deleted_at``); does not physically remove the row.
- :func:`reset_password` — Argon2id hash a new password and persist it.
  Also revokes every active session for the user so a stolen refresh
  token cannot survive the change.
- :func:`list_user_roles` — bound roles + their permission sets.
- :func:`bind_role` — create a :class:`UserRoleBinding`. Refuses
  duplicates and cross-tenant role ids.
- :func:`unbind_role` — delete a binding (or mark it revoked via
  ``expires_at``; the implementation hard-deletes because the binding
  table is pure M2M, not an audit log).
- :func:`create_role` / :func:`list_roles` — tenant-scoped role CRUD.

Cross-tenant contract
---------------------

Every function takes ``tenant_id`` as the *source of truth* for the L1
isolation boundary. The tenant context is bound via
:func:`aidp_db.tenant.set_tenant_context` for the duration of the
transaction so the L1 ``do_orm_execute`` listener can append
``WHERE tenant_id = :tid``. Direct ``UPDATE`` / ``DELETE`` statements
(rare) filter on ``tenant_id`` explicitly as defense in depth.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from aidp_common.errors import ConflictError, NotFoundError, ValidationError
from aidp_db.session import get_session
from aidp_db.tenant import reset_tenant_context, set_tenant_context
from sqlalchemy import CursorResult, delete, func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session as SqlSession

from aidp_iam.models import Role, User, UserRoleBinding
from aidp_iam.models import Session as SessionModel
from aidp_iam.services.auth_service import hash_password
from aidp_iam.services.rbac import collect_user_permissions

_LOG = logging.getLogger(__name__)

#: Maximum page size accepted by :func:`list_users` to avoid a runaway
#: ``OFFSET`` query on a large tenant. Callers asking for more get
#: capped at this value.
_MAX_PAGE_SIZE: int = 200

#: Default page size when the caller does not specify one.
_DEFAULT_PAGE_SIZE: int = 20


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _normalize_email(value: str) -> str:
    """Lowercase + strip an email; matches the column contract."""
    return value.strip().lower()


def _validate_page_params(page: int, page_size: int) -> tuple[int, int]:
    """Clamp *page* / *page_size* to the allowed range.

    Returns:
        A ``(page, page_size)`` tuple with both values coerced into
        valid ranges. ``page`` is 1-based; ``page_size`` is capped at
        :data:`_MAX_PAGE_SIZE`. ValidationError is raised on a
        structurally invalid value so the API layer can translate
        it to a 400.
    """
    if page < 1:
        raise ValidationError("page must be >= 1", details={"page": page})
    if page_size < 1:
        raise ValidationError("page_size must be >= 1", details={"page_size": page_size})
    return page, min(page_size, _MAX_PAGE_SIZE)


def _build_user_info(
    user: User,
    *,
    roles: list[str],
    scopes: list[str],
) -> dict[str, Any]:
    """Project *user* + computed roles/scopes onto the response shape."""
    return {
        "id": user.id,
        "tenant_id": user.tenant_id,
        "username": user.username,
        "email": user.email,
        "display_name": user.display_name,
        "phone": user.phone,
        "status": user.status,
        "mfa_enabled": user.mfa_enabled,
        "last_login_at": user.last_login_at,
        "created_at": user.created_at,
        "updated_at": user.updated_at,
        "roles": list(roles),
        "scopes": list(scopes),
    }


def _load_user_roles_scopes(
    session: SqlSession, user_id: str, tenant_id: str
) -> tuple[list[str], list[str]]:
    """Return ``(roles, scopes)`` for *user_id* by joining bindings + roles.

    Mirrors the helper in :mod:`aidp_iam.services.auth_service` but
    intentionally kept local so the user CRUD layer can evolve its
    role-aggregation logic independently of the auth flow.
    """
    permissions = collect_user_permissions(user_id=user_id, tenant_id=tenant_id)
    roles_set: set[str] = set()
    rows = session.execute(
        select(UserRoleBinding, Role)
        .join(Role, UserRoleBinding.role_id == Role.id)
        .where(UserRoleBinding.user_id == user_id)
        .where(UserRoleBinding.tenant_id == tenant_id)
    ).all()
    for binding, role in rows:
        if binding.is_expired:
            continue
        if role is not None:
            roles_set.add(role.code)
    return sorted(roles_set), sorted(permissions)


def _role_to_dict(role: Role) -> dict[str, Any]:
    """Project a :class:`Role` row onto the public :class:`RoleResponse` shape."""
    return {
        "id": role.id,
        "tenant_id": role.tenant_id,
        "code": role.code,
        "name": role.name,
        "description": role.description,
        "scope": role.scope,
        "permissions": list(role.permissions or []),
        "created_at": role.created_at,
        "updated_at": role.updated_at,
    }


def _revoke_all_user_sessions(session: SqlSession, *, user_id: str, tenant_id: str) -> int:
    """Revoke every active session for *user_id* in *tenant_id*.

    Used by :func:`reset_password` so a password change immediately
    invalidates every refresh token the user holds. Returns the number
    of sessions that were newly revoked.
    """
    now = datetime.now(UTC)
    result: CursorResult[Any] = session.execute(  # type: ignore[assignment]
        update(SessionModel)
        .where(SessionModel.user_id == user_id)
        .where(SessionModel.tenant_id == tenant_id)
        .where(SessionModel.revoked_at.is_(None))
        .values(revoked_at=now)
    )
    return int(result.rowcount or 0)


# ---------------------------------------------------------------------------
# Public API — users
# ---------------------------------------------------------------------------


def create_user(
    *,
    tenant_id: str,
    username: str,
    email: str,
    password: str,
    display_name: str | None = None,
    phone: str | None = None,
    status: str = "active",
    role_ids: list[str] | None = None,
    created_by: str | None = None,
) -> dict[str, Any]:
    """Create a new user inside *tenant_id* with an Argon2id-hashed password.

    The function is atomic: a duplicate ``(tenant_id, username)`` or
    ``(tenant_id, email)`` rolls back the entire insert. The optional
    ``role_ids`` are validated against the tenant (cross-tenant role ids
    raise :class:`ValidationError` before any insert is attempted).

    Args:
        tenant_id: The tenant the new user belongs to.
        username: Login name. Validated by the Pydantic request model.
        email: Contact email. Lowercased + stripped before persistence.
        password: Plaintext; hashed with Argon2id before persistence.
        display_name: Optional human label.
        phone: Optional E.164 phone.
        status: ``"active"`` / ``"locked"`` / ``"disabled"`` / ``"invited"``.
        role_ids: Optional list of role ids to bind on creation. Each
            id must belong to *tenant_id*; cross-tenant ids raise
            :class:`ValidationError`.
        created_by: Optional user id of the admin performing the create;
            persisted via the :attr:`TimestampMixin.created_by` column.

    Returns:
        A dict shaped like the :class:`UserResponse` model — ready to be
        projected onto the API response.

    Raises:
        ConflictError: When the username or email is already taken in
            the tenant.
        ValidationError: When *role_ids* references a role outside the
            tenant, or when a structural argument is invalid.
    """
    email_normalized = _normalize_email(email)
    role_ids = list(role_ids or [])

    ctx_token = set_tenant_context(tenant_id)
    try:
        with get_session() as session:
            # Pre-validate role ids before any insert so the error path
            # is a clean ValidationError (not an IntegrityError).
            if role_ids:
                existing_roles = (
                    session.execute(select(Role).where(Role.id.in_(role_ids))).scalars().all()
                )
                found_ids = {r.id for r in existing_roles}
                missing = [rid for rid in role_ids if rid not in found_ids]
                if missing:
                    raise ValidationError(
                        "one or more role_ids do not exist",
                        details={"missing_role_ids": missing},
                    )
                # Tenant boundary: every role must belong to this tenant.
                cross_tenant = [r.id for r in existing_roles if r.tenant_id != tenant_id]
                if cross_tenant:
                    raise ValidationError(
                        "one or more role_ids belong to a different tenant",
                        details={"cross_tenant_role_ids": cross_tenant},
                    )

            user = User(
                tenant_id=tenant_id,
                username=username,
                email=email_normalized,
                display_name=display_name,
                phone=phone,
                status=status,
                mfa_enabled=False,
                password_hash=hash_password(password),
                created_by=created_by,
            )
            session.add(user)
            try:
                session.flush()
            except IntegrityError as exc:
                session.rollback()
                raise ConflictError(
                    f"username {username!r} or email {email_normalized!r} "
                    f"already exists in tenant {tenant_id!r}"
                ) from exc

            # Create role bindings. ``bind_role`` would re-bind the
            # tenant context, so we inline the work here to keep the
            # transaction tight.
            for rid in role_ids:
                session.add(
                    UserRoleBinding(
                        tenant_id=tenant_id,
                        user_id=user.id,
                        role_id=rid,
                        scope_type="tenant",
                        granted_by=created_by,
                    )
                )
            try:
                session.flush()
            except IntegrityError as exc:  # pragma: no cover - unique binding
                session.rollback()
                raise ConflictError(f"duplicate role binding for user {user.id!r}") from exc

            roles, scopes = _load_user_roles_scopes(
                session=session, user_id=user.id, tenant_id=tenant_id
            )
            return _build_user_info(user, roles=roles, scopes=scopes)
    finally:
        reset_tenant_context(ctx_token)


def list_users(
    *,
    tenant_id: str,
    page: int = 1,
    page_size: int = _DEFAULT_PAGE_SIZE,
    status: str | None = None,
    role_id: str | None = None,
) -> dict[str, Any]:
    """Return a paginated list of users in *tenant_id*.

    Filters are optional and combined with ``AND``. The total count is
    computed with a separate ``SELECT COUNT(*)`` so the response can
    report ``total`` without forcing the caller to page through every
    row.

    Args:
        tenant_id: The tenant whose users to enumerate.
        page: 1-based page index. ``page=1`` is the first page.
        page_size: Rows per page. Capped at :data:`_MAX_PAGE_SIZE`.
        status: Optional filter — restricts to users with this
            :attr:`User.status` value.
        role_id: Optional filter — restricts to users that have an
            active binding to this role.

    Returns:
        A dict shaped like :class:`UserListResponse`:

        .. code-block:: python

            {
                "items": [<user>, ...],
                "total": 42,
                "page": 1,
                "page_size": 20,
            }

    Raises:
        ValidationError: On structurally invalid pagination args.
    """
    page, page_size = _validate_page_params(page, page_size)

    ctx_token = set_tenant_context(tenant_id)
    try:
        with get_session() as session:
            # The role_id filter requires a subquery against
            # ``user_role_bindings``; we build the base ``select(User)``
            # then layer optional ``where`` clauses so the common
            # (no-filter) case stays a single-table read.
            stmt = select(User)
            count_stmt = select(func.count()).select_from(User)

            if status is not None:
                stmt = stmt.where(User.status == status)
                count_stmt = count_stmt.where(User.status == status)

            if role_id is not None:
                # Users that have an *active* binding to the given role.
                role_user_subq = (
                    select(UserRoleBinding.user_id)
                    .where(UserRoleBinding.tenant_id == tenant_id)
                    .where(UserRoleBinding.role_id == role_id)
                )
                # ``UserRoleBinding.is_expired`` is a Python property, not
                # a SQL expression, so we cannot push it into the DB.
                # Filter the active set in Python by selecting the
                # binding rows separately.
                active_user_ids = {row[0] for row in session.execute(role_user_subq).all()}
                # Drop expired bindings client-side.
                expired_ids = {
                    b.user_id
                    for b in session.execute(
                        select(UserRoleBinding)
                        .where(UserRoleBinding.tenant_id == tenant_id)
                        .where(UserRoleBinding.role_id == role_id)
                    )
                    .scalars()
                    .all()
                    if b.is_expired
                }
                active_user_ids -= expired_ids
                if not active_user_ids:
                    return {
                        "items": [],
                        "total": 0,
                        "page": page,
                        "page_size": page_size,
                    }
                stmt = stmt.where(User.id.in_(active_user_ids))
                count_stmt = count_stmt.where(User.id.in_(active_user_ids))

            total = int(session.execute(count_stmt).scalar_one() or 0)

            offset = (page - 1) * page_size
            stmt = stmt.order_by(User.created_at.asc(), User.id.asc())
            stmt = stmt.offset(offset).limit(page_size)
            users = session.execute(stmt).scalars().all()

            items: list[dict[str, Any]] = []
            for u in users:
                roles, scopes = _load_user_roles_scopes(
                    session=session, user_id=u.id, tenant_id=u.tenant_id
                )
                items.append(_build_user_info(u, roles=roles, scopes=scopes))

            return {
                "items": items,
                "total": total,
                "page": page,
                "page_size": page_size,
            }
    finally:
        reset_tenant_context(ctx_token)


def get_user(*, tenant_id: str, user_id: str) -> dict[str, Any] | None:
    """Look up a single user by id; ``None`` when not in the tenant.

    The L1 tenant filter makes the ``WHERE tenant_id = :tid`` redundant
    in practice, but we pass it explicitly as defense in depth (a row
    can only be returned when both the listener and the explicit filter
    agree).

    Args:
        tenant_id: The tenant boundary.
        user_id: The per-tenant user id.

    Returns:
        The user-info dict (matches :class:`UserResponse`), or ``None``
        when the user does not exist in the tenant.
    """
    if not user_id:
        return None
    ctx_token = set_tenant_context(tenant_id)
    try:
        with get_session() as session:
            user = session.execute(
                select(User).where(User.id == user_id).where(User.tenant_id == tenant_id)
            ).scalar_one_or_none()
            if user is None:
                return None
            roles, scopes = _load_user_roles_scopes(
                session=session, user_id=user.id, tenant_id=user.tenant_id
            )
            return _build_user_info(user, roles=roles, scopes=scopes)
    finally:
        reset_tenant_context(ctx_token)


def update_user(
    *,
    tenant_id: str,
    user_id: str,
    display_name: str | None = None,
    phone: str | None = None,
    status: str | None = None,
    mfa_enabled: bool | None = None,
    updated_by: str | None = None,
) -> dict[str, Any]:
    """Update mutable user fields and return the refreshed view.

    ``None`` fields are not touched (the field is excluded from the
    ``UPDATE`` statement). The function refuses to update users
    outside the caller's tenant; the L1 listener will silently drop
    such rows, so we raise :class:`NotFoundError` to make the
    contract explicit at the API layer.

    Args:
        tenant_id: The tenant boundary.
        user_id: The user to update.
        display_name: New display name (``None`` = no change).
        phone: New phone (``None`` = no change).
        status: New status (``None`` = no change).
        mfa_enabled: New MFA flag (``None`` = no change).
        updated_by: User id of the admin performing the update;
            persisted via the :attr:`TimestampMixin.updated_by` column.

    Returns:
        The refreshed :class:`UserResponse`-shaped dict.

    Raises:
        NotFoundError: When the user does not exist in the tenant.
    """
    fields: dict[str, Any] = {}
    if display_name is not None:
        fields["display_name"] = display_name
    if phone is not None:
        fields["phone"] = phone
    if status is not None:
        fields["status"] = status
    if mfa_enabled is not None:
        fields["mfa_enabled"] = mfa_enabled
    if updated_by is not None:
        fields["updated_by"] = updated_by

    if not fields:
        # Nothing to do; return the current view (or raise if missing).
        info = get_user(tenant_id=tenant_id, user_id=user_id)
        if info is None:
            raise NotFoundError("user", user_id)
        return info

    ctx_token = set_tenant_context(tenant_id)
    try:
        with get_session() as session:
            result: CursorResult[Any] = session.execute(  # type: ignore[assignment]
                update(User)
                .where(User.id == user_id)
                .where(User.tenant_id == tenant_id)
                .values(**fields)
            )
            if result.rowcount == 0:
                # The row either does not exist or is in a different
                # tenant. The L1 listener should already have prevented
                # cross-tenant matches, so the most likely cause is
                # "user not found".
                raise NotFoundError("user", user_id)
            session.flush()
            user = session.execute(
                select(User).where(User.id == user_id).where(User.tenant_id == tenant_id)
            ).scalar_one_or_none()
            if user is None:  # pragma: no cover - race: delete-between
                raise NotFoundError("user", user_id)
            roles, scopes = _load_user_roles_scopes(
                session=session, user_id=user.id, tenant_id=user.tenant_id
            )
            return _build_user_info(user, roles=roles, scopes=scopes)
    finally:
        reset_tenant_context(ctx_token)


def delete_user(
    *,
    tenant_id: str,
    user_id: str,
    updated_by: str | None = None,
) -> None:
    """Soft-delete *user_id* (set ``status='disabled'`` and ``deleted_at``).

    The row is not physically removed; the soft-delete contract is the
    same as :class:`aidp_common.models.TimestampMixin.deleted_at`. All
    active sessions for the user are also revoked so a stolen refresh
    token cannot survive the disable.

    Args:
        tenant_id: The tenant boundary.
        user_id: The user to soft-delete.
        updated_by: User id of the admin performing the delete;
            persisted via the :attr:`TimestampMixin.updated_by` column.

    Raises:
        NotFoundError: When the user does not exist in the tenant.
    """
    now = datetime.now(UTC)
    ctx_token = set_tenant_context(tenant_id)
    try:
        with get_session() as session:
            result: CursorResult[Any] = session.execute(  # type: ignore[assignment]
                update(User)
                .where(User.id == user_id)
                .where(User.tenant_id == tenant_id)
                .values(
                    status="disabled",
                    deleted_at=now,
                    updated_by=updated_by,
                )
            )
            if result.rowcount == 0:
                raise NotFoundError("user", user_id)
            revoked = _revoke_all_user_sessions(
                session=session, user_id=user_id, tenant_id=tenant_id
            )
            if revoked:
                _LOG.info(
                    "user soft-deleted; revoked active sessions",
                    extra={
                        "user_id": user_id,
                        "tenant_id": tenant_id,
                        "revoked_sessions": revoked,
                    },
                )
    finally:
        reset_tenant_context(ctx_token)


def reset_password(
    *,
    tenant_id: str,
    user_id: str,
    new_password: str,
    updated_by: str | None = None,
) -> None:
    """Replace the user's password hash and revoke every active session.

    The atomicity of "hash update + session revoke" is the reason the
    whole operation lives in a single transaction. A caller observing
    the new hash in the DB will *also* see the revoked sessions, so
    there is no window where a stolen refresh token can survive a
    password reset.

    Args:
        tenant_id: The tenant boundary.
        user_id: The user whose password is being reset.
        new_password: Plaintext; hashed with Argon2id before persistence.
        updated_by: User id of the admin performing the reset;
            persisted via the :attr:`TimestampMixin.updated_by` column.

    Raises:
        NotFoundError: When the user does not exist in the tenant.
    """
    new_hash = hash_password(new_password)
    ctx_token = set_tenant_context(tenant_id)
    try:
        with get_session() as session:
            result: CursorResult[Any] = session.execute(  # type: ignore[assignment]
                update(User)
                .where(User.id == user_id)
                .where(User.tenant_id == tenant_id)
                .values(
                    password_hash=new_hash,
                    updated_by=updated_by,
                )
            )
            if result.rowcount == 0:
                raise NotFoundError("user", user_id)
            revoked = _revoke_all_user_sessions(
                session=session, user_id=user_id, tenant_id=tenant_id
            )
            _LOG.info(
                "user password reset; revoked active sessions",
                extra={
                    "user_id": user_id,
                    "tenant_id": tenant_id,
                    "revoked_sessions": revoked,
                },
            )
    finally:
        reset_tenant_context(ctx_token)


# ---------------------------------------------------------------------------
# Public API — roles + role bindings
# ---------------------------------------------------------------------------


def list_user_roles(*, tenant_id: str, user_id: str) -> list[dict[str, Any]]:
    """Return the list of roles bound to *user_id* in *tenant_id*.

    Expired bindings are excluded. The function returns an empty list
    when the user has no bindings (and when the user does not exist —
    the API layer turns the empty result into a 404 via a separate
    :func:`get_user` check).
    """
    if not user_id:
        return []
    ctx_token = set_tenant_context(tenant_id)
    try:
        with get_session() as session:
            rows = (
                session.execute(
                    select(Role)
                    .join(UserRoleBinding, UserRoleBinding.role_id == Role.id)
                    .where(UserRoleBinding.user_id == user_id)
                    .where(UserRoleBinding.tenant_id == tenant_id)
                )
                .scalars()
                .all()
            )
            return [
                _role_to_dict(r)
                for r in rows
                # Filter out expired bindings in Python (the property
                # is not a SQL expression).
                if not any(
                    b.is_expired
                    for b in session.execute(
                        select(UserRoleBinding)
                        .where(UserRoleBinding.user_id == user_id)
                        .where(UserRoleBinding.role_id == r.id)
                    )
                    .scalars()
                    .all()
                )
            ]
    finally:
        reset_tenant_context(ctx_token)


def bind_role(
    *,
    tenant_id: str,
    user_id: str,
    role_id: str,
    granted_by: str | None = None,
) -> dict[str, Any]:
    """Bind *role_id* to *user_id* inside *tenant_id*.

    The function refuses:

    - cross-tenant role ids (the role must belong to the same tenant
      as the user; otherwise the L1 listener would silently drop the
      read and the call would return ``None``);
    - duplicate bindings. The DB-level unique constraint on
      ``(tenant_id, user_id, role_id, scope_type, scope_id)`` does
      *not* fire when ``scope_id`` is ``NULL`` (SQLite and PostgreSQL
      both treat ``NULL != NULL`` for uniqueness), so the service
      pre-checks for an existing active binding and raises
      :class:`ConflictError` *before* the insert;
    - binds against a non-existent user (raises :class:`NotFoundError`
      *before* attempting the insert so the API can render a 404
      instead of an opaque IntegrityError).

    Args:
        tenant_id: The tenant boundary.
        user_id: The user receiving the binding.
        role_id: The role to attach.
        granted_by: User id of the granting principal; persisted on
            the binding row for audit.

    Returns:
        A dict shaped like :class:`UserResponse`, with the freshly
        refreshed ``roles`` and ``scopes`` lists.

    Raises:
        NotFoundError: When the user or role does not exist in the
            tenant.
        ValidationError: When the role belongs to a different tenant.
        ConflictError: When the binding already exists.
    """
    ctx_token = set_tenant_context(tenant_id)
    try:
        with get_session() as session:
            user = session.execute(
                select(User).where(User.id == user_id).where(User.tenant_id == tenant_id)
            ).scalar_one_or_none()
            if user is None:
                raise NotFoundError("user", user_id)
            role = session.execute(select(Role).where(Role.id == role_id)).scalar_one_or_none()
            if role is None:
                raise NotFoundError("role", role_id)
            if role.tenant_id != tenant_id:
                # Defense in depth — the listener will already drop
                # cross-tenant reads, but a future config tweak
                # (e.g. global roles) should not silently relax the
                # boundary.
                raise ValidationError(
                    "role belongs to a different tenant",
                    details={"role_id": role_id, "role_tenant_id": role.tenant_id},
                )

            # Pre-check for an existing active binding. The DB-level
            # unique constraint on ``(tenant_id, user_id, role_id,
            # scope_type, scope_id)`` does *not* fire when
            # ``scope_id`` is ``NULL`` (SQLite / PostgreSQL treat
            # ``NULL != NULL``), so the service must enforce the
            # invariant itself.
            existing = (
                session.execute(
                    select(UserRoleBinding)
                    .where(UserRoleBinding.user_id == user_id)
                    .where(UserRoleBinding.role_id == role_id)
                    .where(UserRoleBinding.scope_type == "tenant")
                )
                .scalars()
                .first()
            )
            if existing is not None and not existing.is_expired:
                raise ConflictError(f"user {user_id!r} is already bound to role {role_id!r}")

            binding = UserRoleBinding(
                tenant_id=tenant_id,
                user_id=user_id,
                role_id=role_id,
                scope_type="tenant",
                granted_by=granted_by,
            )
            session.add(binding)
            try:
                session.flush()
            except IntegrityError as exc:  # pragma: no cover - DB belt-and-suspenders
                session.rollback()
                raise ConflictError(
                    f"user {user_id!r} is already bound to role {role_id!r}"
                ) from exc

            session.flush()
            roles, scopes = _load_user_roles_scopes(
                session=session, user_id=user.id, tenant_id=user.tenant_id
            )
            return _build_user_info(user, roles=roles, scopes=scopes)
    finally:
        reset_tenant_context(ctx_token)


def unbind_role(
    *,
    tenant_id: str,
    user_id: str,
    role_id: str,
) -> None:
    """Remove a role binding.

    Idempotent: a second call with the same ``(tenant_id, user_id,
    role_id)`` tuple is a no-op (returns ``False`` would require
    returning a value; the public API returns ``204 No Content`` in
    both cases).

    Args:
        tenant_id: The tenant boundary.
        user_id: The user losing the binding.
        role_id: The role to detach.

    Raises:
        NotFoundError: When the user does not exist in the tenant.
            Bindings are *not* existence-checked — unbind is idempotent.
    """
    ctx_token = set_tenant_context(tenant_id)
    try:
        with get_session() as session:
            # Existence-check the user so the API can return 404. We
            # do not check the binding itself (unbind is idempotent).
            user = session.execute(
                select(User).where(User.id == user_id).where(User.tenant_id == tenant_id)
            ).scalar_one_or_none()
            if user is None:
                raise NotFoundError("user", user_id)
            session.execute(
                delete(UserRoleBinding)
                .where(UserRoleBinding.tenant_id == tenant_id)
                .where(UserRoleBinding.user_id == user_id)
                .where(UserRoleBinding.role_id == role_id)
            )
    finally:
        reset_tenant_context(ctx_token)


def create_role(
    *,
    tenant_id: str,
    code: str,
    name: str,
    description: str | None = None,
    scope: str = "tenant",
    permissions: list[str] | None = None,
    created_by: str | None = None,
) -> dict[str, Any]:
    """Create a new role inside *tenant_id*.

    ``(tenant_id, code)`` is unique — duplicates raise
    :class:`ConflictError`. Permissions are stored verbatim
    (``"*"`` is preserved as a wildcard).

    Args:
        tenant_id: The tenant boundary.
        code: Stable business key. Unique within the tenant.
        name: Human-readable display name.
        description: Optional long-form description.
        scope: ``"global"`` or ``"tenant"``. Defaults to ``"tenant"``.
        permissions: Optional list of permission strings. Defaults to
            ``[]``.
        created_by: User id of the admin creating the role;
            persisted via the :attr:`TimestampMixin.created_by` column.

    Returns:
        A dict shaped like :class:`RoleResponse`.

    Raises:
        ConflictError: When a role with the same code already exists
            in the tenant.
    """
    permissions_list = list(permissions or [])
    ctx_token = set_tenant_context(tenant_id)
    try:
        with get_session() as session:
            role = Role(
                tenant_id=tenant_id,
                code=code,
                name=name,
                description=description,
                scope=scope,
                permissions=permissions_list,
                created_by=created_by,
            )
            session.add(role)
            try:
                session.flush()
            except IntegrityError as exc:
                session.rollback()
                raise ConflictError(
                    f"role code {code!r} already exists in tenant {tenant_id!r}"
                ) from exc
            return _role_to_dict(role)
    finally:
        reset_tenant_context(ctx_token)


def list_roles(*, tenant_id: str) -> list[dict[str, Any]]:
    """Return every role in *tenant_id*, ordered by ``code``.

    Args:
        tenant_id: The tenant boundary.

    Returns:
        A list of :class:`RoleResponse`-shaped dicts.
    """
    ctx_token = set_tenant_context(tenant_id)
    try:
        with get_session() as session:
            rows = (
                session.execute(
                    select(Role).where(Role.tenant_id == tenant_id).order_by(Role.code.asc())
                )
                .scalars()
                .all()
            )
            return [_role_to_dict(r) for r in rows]
    finally:
        reset_tenant_context(ctx_token)


# ---------------------------------------------------------------------------
# Public re-exports
# ---------------------------------------------------------------------------

__all__ = [
    "bind_role",
    "create_role",
    "create_user",
    "delete_user",
    "get_user",
    "list_roles",
    "list_user_roles",
    "list_users",
    "reset_password",
    "unbind_role",
    "update_user",
]
