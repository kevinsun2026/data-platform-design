"""SQLAlchemy 2.0 engine factory and synchronous session helpers.

This module is the central entry point for talking to Postgres from AIDP
Python services. It exposes:

- :func:`get_engine` — build (or fetch a cached) :class:`Engine` for a given URL.
- :func:`get_session` — context manager yielding a single :class:`Session` with
  commit-on-exit / rollback-on-error semantics.
- :func:`with_session` — decorator wrapping a callable so it runs inside a
  :func:`get_session` block (handy for scripts and Celery jobs).

Notes:
    The codebase targets Postgres in production. We accept ``postgresql://`` /
    ``postgresql+psycopg://`` / ``postgresql+asyncpg://`` URLs but return a
    **synchronous** engine (the FastAPI HTTP layer can wrap it in a thread
    pool, and the worker / migration layer is fully sync).

    Importing this module also installs the SQLAlchemy ``do_orm_execute``
    event listener from :mod:`aidp_db.tenant`, so tenant filtering is active
    as soon as a service ``import aidp_db.session`` (the typical entry
    point) and calls :func:`aidp_db.tenant.set_tenant_context`. The
    listener is idempotent: importing the module more than once (or
    reloading it in tests) does not stack listeners.
"""

from __future__ import annotations

import contextlib
import logging
import threading
from collections.abc import Callable, Generator
from typing import Any, ParamSpec, TypeVar

from aidp_common.config import get_settings
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

import aidp_db.tenant  # noqa: F401  # side-effect: register do_orm_execute listener

_LOG = logging.getLogger(__name__)

P = ParamSpec("P")
R = TypeVar("R")

# Process-wide engine cache keyed by database URL. We deliberately cache
# engines so a service that calls ``get_engine()`` repeatedly (e.g. inside
# ``with_session``) reuses connection pools instead of leaking them.
_engine_cache: dict[str, Engine] = {}
# Guards the miss-path in :func:`get_engine` so two threads that race for
# the same URL build exactly one engine. The fast path (cache hit) takes
# the lock-free ``dict.get``; only the rare miss falls through here.
_engine_lock = threading.Lock()

# Default pool sizing. The plan calls out Postgres with moderate per-service
# QPS; tune via env vars if your workload differs.
_DEFAULT_POOL_SIZE = 5
_DEFAULT_MAX_OVERFLOW = 10
_DEFAULT_POOL_TIMEOUT_SECONDS = 30
_DEFAULT_POOL_RECYCLE_SECONDS = 1800


def _build_engine(url: str, **kwargs: Any) -> Engine:
    """Construct a fresh :class:`Engine` for *url*.

    Args:
        url: SQLAlchemy database URL. SQLite (``sqlite://``) is supported for
            unit tests; in production use ``postgresql+psycopg://...``.
        **kwargs: Forwarded to :func:`sqlalchemy.create_engine`.

    Returns:
        A new :class:`Engine` instance. The caller is responsible for
        disposing it (use :func:`dispose_engine` or let the process exit).
    """
    if url.startswith("sqlite"):
        # SQLite needs ``check_same_thread=False`` for in-memory usage from
        # multiple threads (FastAPI test fixtures share sessions across
        # threads); file-based SQLite works without it but is harmless.
        kwargs.setdefault("connect_args", {"check_same_thread": False})
    else:
        kwargs.setdefault("pool_size", _DEFAULT_POOL_SIZE)
        kwargs.setdefault("max_overflow", _DEFAULT_MAX_OVERFLOW)
        kwargs.setdefault("pool_timeout", _DEFAULT_POOL_TIMEOUT_SECONDS)
        kwargs.setdefault("pool_recycle", _DEFAULT_POOL_RECYCLE_SECONDS)
        # ``pool_pre_ping`` quietly drops dead connections (e.g. after a DB
        # restart) instead of failing the next request.
        kwargs.setdefault("pool_pre_ping", True)

    engine = create_engine(url, future=True, **kwargs)
    _LOG.info("engine created", extra={"url": _safe_url(url)})
    return engine


def _safe_url(url: str) -> str:
    """Redact credentials from a SQLAlchemy URL for logging."""
    if "@" not in url or "://" not in url:
        return url
    scheme, rest = url.split("://", 1)
    if "@" not in rest:
        return url
    _, host_part = rest.split("@", 1)
    return f"{scheme}://***@{host_part}"


def get_engine(url: str | None = None, *, cache: bool = True, **kwargs: Any) -> Engine:
    """Return an :class:`Engine` for *url* (cached by default).

    Args:
        url: SQLAlchemy database URL. When ``None`` the value is read from
            :class:`aidp_common.config.Settings.db_url` (the ``AIDP_DB_URL``
            environment variable).
        cache: When ``True`` (default) the engine is memoized by URL so
            repeated calls in the same process reuse the same pool. Pass
            ``False`` to force a fresh engine (used in tests).
        **kwargs: Forwarded to :func:`sqlalchemy.create_engine` (ignored when
            ``cache=True`` and the URL already has an engine — we return the
            cached instance unchanged).

    Returns:
        The :class:`Engine` instance.
    """
    effective_url = url or get_settings().db_url
    if cache:
        # Fast path: lock-free lookup. The vast majority of calls hit here
        # because services request the same engine URL over and over.
        cached = _engine_cache.get(effective_url)
        if cached is not None:
            return cached
        # Slow path: serialize the miss so two concurrent first-callers do
        # not each build a fresh engine and leak one. The double-check
        # inside the lock handles the "thread B arrives after thread A
        # populated the cache" case without a second build.
        with _engine_lock:
            cached = _engine_cache.get(effective_url)
            if cached is not None:
                return cached
            engine = _build_engine(effective_url, **kwargs)
            _engine_cache[effective_url] = engine
            return engine
    # ``cache=False``: build a one-shot engine and leave the cache alone.
    # The caller is responsible for disposal (see :func:`dispose_engine`).
    return _build_engine(effective_url, **kwargs)


def dispose_engine(url: str | None = None) -> None:
    """Dispose (close) the cached engine for *url* and drop it from the cache.

    Args:
        url: The URL to dispose. When ``None`` the value from
            :func:`aidp_common.config.get_settings` is used. If no cached
            engine exists for the URL the call is a silent no-op.
    """
    effective_url = url or get_settings().db_url
    engine = _engine_cache.pop(effective_url, None)
    if engine is not None:
        engine.dispose()
        _LOG.info("engine disposed", extra={"url": _safe_url(effective_url)})


def reset_engine_cache() -> None:
    """Drop every cached engine. Intended for tests; disposes all engines."""
    for engine in list(_engine_cache.values()):
        engine.dispose()
    _engine_cache.clear()


def get_session_factory(engine: Engine) -> sessionmaker[Session]:
    """Return a :class:`sessionmaker` bound to *engine*.

    Args:
        engine: The :class:`Engine` returned by :func:`get_engine`.

    Returns:
        A new :class:`sessionmaker` configured with ``expire_on_commit=False``
        (so detached attribute access after commit does not raise) and
        ``autoflush=False`` (so explicit ``flush()`` calls are the only path
        that emits SQL during a transaction).
    """
    return sessionmaker(
        bind=engine,
        expire_on_commit=False,
        autoflush=False,
        future=True,
        class_=Session,
    )


@contextlib.contextmanager
def get_session(engine: Engine | None = None) -> Generator[Session, None, None]:
    """Context manager yielding a single :class:`Session` bound to *engine*.

    The session commits on successful exit, rolls back on exception, and is
    always closed. The caller does not need to ``add()`` rows in any
    particular order — the deferred commit happens at the end of the block.

    Args:
        engine: Optional :class:`Engine`. When ``None`` the engine is fetched
            via :func:`get_engine` (i.e. from the cached ``AIDP_DB_URL``).

    Yields:
        An open :class:`Session`. Caller can read, write, and query freely.
    """
    eng = engine if engine is not None else get_engine()
    session = get_session_factory(eng)()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def with_session(
    engine: Engine | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorator wrapping *fn* in a :func:`get_session` block.

    Useful for Celery tasks, management scripts, and short-lived background
    jobs that need a single transactional unit. For request handlers, prefer
    the explicit ``with get_session() as s:`` form so the transaction
    boundary is obvious.

    Args:
        engine: Optional :class:`Engine`. When ``None`` the engine is fetched
            from :func:`get_engine`.

    Returns:
        A decorator that opens a :class:`Session` before calling *fn* and
        closes it (committing on success, rolling back on error) after.

    Example::

        @with_session()
        def create_user(name: str, *, session: Session) -> User:
            user = User(name=name, tenant_id=get_tenant_id())
            session.add(user)
            return user
    """

    # Use a sentinel so we can detect whether ``fn`` declared a ``session``
    # keyword argument; if so we inject the active session, otherwise we
    # just open/close a session for its side effects (e.g. running ``fn``
    # which does all DB work via ``session.execute`` on a session it
    # fetched itself).
    def decorator(fn: Callable[P, R]) -> Callable[P, R]:
        import inspect

        try:
            sig = inspect.signature(fn)
        except (TypeError, ValueError):  # pragma: no cover - builtins / C-ext
            sig = None
        wants_session_kw = sig is not None and "session" in sig.parameters

        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            with get_session(engine) as session:
                if wants_session_kw:
                    kwargs["session"] = session
                return fn(*args, **kwargs)

        return wrapper

    return decorator


__all__ = [
    "dispose_engine",
    "get_engine",
    "get_session",
    "get_session_factory",
    "reset_engine_cache",
    "with_session",
]
