"""Mandatory tenant isolation (L1) for ORM queries.

Platform global constraint #43 -- "租户隔离 L1 强制: 所有 DB query 通过 ORM 自动注入
``WHERE tenant_id = :current_tenant``" -- is implemented here.

Mechanism:
    1. The current request's ``tenant_id`` is stored in a :class:`ContextVar`
       (set by the auth middleware at the top of every request).
    2. A SQLAlchemy ``do_orm_execute`` event listener registered against
       :class:`sqlalchemy.orm.Session` reads the :class:`ContextVar` and, for
       every ``SELECT`` that touches a model declaring ``tenant_id``,
       appends ``WHERE tenant_id = :tenant_id`` to the statement.
    3. Writes (INSERT / UPDATE / DELETE) are not filtered automatically; the
       caller is required to set ``obj.tenant_id = get_tenant_id()`` before
       flush. The schema enforces ``NOT NULL`` so a missing tenant id fails
       at flush time, not at query time.

Why :class:`ContextVar`?
    FastAPI / asyncio run each request in its own :class:`contextvars.Context`,
    so values set in middleware are automatically isolated across concurrent
    requests without any thread-local / global-state coordination. The same
    module is also safe to use from synchronous code (the context is shared
    within a single thread / coroutine).

Bypassing the filter:
    The event listener runs for every ORM select. There is **no** built-in
    escape hatch — by design. If a service truly needs to query across
    tenants (background jobs, admin scripts), it should use a dedicated
    session that does not register the listener, or set the tenant id to a
    sentinel value that matches every row (not exposed here on purpose).
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from contextvars import ContextVar, Token
from typing import Any

from sqlalchemy import event
from sqlalchemy.orm import ORMExecuteState, Session

# ---------------------------------------------------------------------------
# Tenant context
# ---------------------------------------------------------------------------

# A request-scoped context variable. ``None`` means "no tenant set" — the
# event listener refuses to filter in that state, which is the correct
# behavior for migrations / admin jobs that should not be silently
# rewritten.
_current_tenant: ContextVar[str | None] = ContextVar("aidp_current_tenant", default=None)


def set_tenant_context(tenant_id: str) -> Token[str | None]:
    """Bind *tenant_id* as the current request's tenant.

    Returns the :class:`Token` returned by :meth:`ContextVar.set`. The caller
    is expected to :func:`reset_tenant_context` once the request ends so the
    binding does not leak into unrelated tasks. Auth middleware typically
    does::

        token = set_tenant_context(claims.tenant_id)
        try:
            response = await call_next(request)
            return response
        finally:
            reset_tenant_context(token)

    Args:
        tenant_id: The tenant id to bind. Should be the UUID-string tenant
            id from the verified JWT.

    Returns:
        Opaque token; pass it to :func:`reset_tenant_context` to roll back
        to the previous binding.
    """
    return _current_tenant.set(tenant_id)


def reset_tenant_context(token: Token[str | None]) -> None:
    """Restore the tenant context to the state it had before *token* was set.

    Args:
        token: The value returned by :func:`set_tenant_context`.
    """
    _current_tenant.reset(token)


def get_tenant_id() -> str | None:
    """Return the current tenant id, or ``None`` if no context is bound.

    Returns:
        The tenant id previously passed to :func:`set_tenant_context`, or
        ``None`` if no context is active. ``None`` means the event listener
        will *not* filter queries — be sure that is what you want.
    """
    return _current_tenant.get()


@contextlib.contextmanager
def tenant_scope(tenant_id: str) -> Iterator[None]:
    """Context manager that binds *tenant_id* for the duration of the block.

    Example::

        with tenant_scope("tenant-a"):
            users = session.execute(select(User)).scalars().all()

    Args:
        tenant_id: The tenant id to bind. See :func:`set_tenant_context` for
            the same effect without the ``with`` syntax.

    Yields:
        ``None``; the binding is rolled back on exit.
    """
    token = set_tenant_context(tenant_id)
    try:
        yield
    finally:
        reset_tenant_context(token)


# A short alias for clarity. We expose the verbose name (set_tenant_context)
# as the canonical API because the rest of the platform uses that wording,
# but ``TenantSession`` doubles as a re-export so service code can
# ``from aidp_db.tenant import TenantSession`` and chain it with other
# context managers.
TenantSession = tenant_scope


# ---------------------------------------------------------------------------
# SQLAlchemy event listener — auto-inject WHERE tenant_id
# ---------------------------------------------------------------------------


@event.listens_for(Session, "do_orm_execute")
def _add_tenant_filter(state: ORMExecuteState) -> None:
    """Inject ``WHERE tenant_id = :current_tenant`` on every ORM select.

    The listener short-circuits when:

    - the statement is not a SELECT (``state.is_select`` is ``False``);
    - no tenant context is set (``get_tenant_id()`` is ``None``);
    - the statement targets no model with a ``tenant_id`` column.

    Otherwise it appends one ``WHERE`` per entity in
    ``state.statement.column_descriptions`` whose mapped class declares
    ``tenant_id``. The predicate compares the ORM attribute (so it
    participates in the same bound-parameter / caching pipeline as any
    hand-written ``where(Model.tenant_id == ...)`` call).
    """
    if not state.is_select:
        return
    tid = get_tenant_id()
    if tid is None:
        return

    # ``column_descriptions`` covers the common shapes: ``select(Model)``,
    # ``select(Model.col)``, ``select(Model1, Model2)``, and ``select(*cols)``
    # with associated ``from_obj(Model)``. Each description carries the
    # mapped class in ``entity`` when available.
    seen_entities: set[type[Any]] = set()
    # ``state.statement`` is typed as the broad ``Executable`` root, but at
    # this point SQLAlchemy guarantees it is a ``Select`` (the listener
    # only fires for ORM select execution). Cast for the column-descriptions
    # iteration and the ``where`` chain.
    select_stmt: Any = state.statement
    for desc in select_stmt.column_descriptions:
        entity = desc.get("entity")
        if entity is None:
            continue
        if not hasattr(entity, "tenant_id"):
            continue
        if entity in seen_entities:
            continue
        seen_entities.add(entity)
        # Bind to the ORM attribute so SQLAlchemy's compile pipeline can
        # resolve the column name. Comparing against ``tid`` (a str) is
        # safe because :class:`TenantScoped.tenant_id` is a ``String(36)``.
        state.statement = select_stmt.where(entity.tenant_id == tid)


# ---------------------------------------------------------------------------
# Re-exports
# ---------------------------------------------------------------------------

__all__ = [
    "TenantSession",
    "get_tenant_id",
    "reset_tenant_context",
    "set_tenant_context",
    "tenant_scope",
]
