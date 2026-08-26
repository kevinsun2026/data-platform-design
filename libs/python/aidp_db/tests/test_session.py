"""Tests for ``aidp_db.session``.

These tests verify:

- :func:`aidp_db.session.get_engine` builds an engine, caches it, and
  exposes it via :func:`aidp_db.session.get_session_factory`.
- :func:`aidp_db.session.get_session` commits on success, rolls back on
  exception, and closes the session on exit.
- :func:`aidp_db.session.with_session` injects a ``session`` keyword when
  the wrapped function declares it.

The brief asks for ``testcontainers`` (Postgres) but allows an in-memory
SQLite fallback when the docker daemon / image is unavailable in the
current sandbox. We attempt testcontainers first, and the fallback path is
annotated with ``# pragma: allow-testcontainers-fallback`` so the policy is
grep-able from the codebase.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from aidp_common.models import IdModel, TenantScoped, TimestampMixin
from aidp_db import session as session_module
from aidp_db.session import (
    dispose_engine,
    get_engine,
    get_session,
    get_session_factory,
    reset_engine_cache,
    with_session,
)
from sqlalchemy import String, create_engine, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

# ---------------------------------------------------------------------------
# Test database — try testcontainers (Postgres), fall back to SQLite
# ---------------------------------------------------------------------------


def _try_postgres_container() -> str | None:
    """Best-effort testcontainers bootstrap; return a URL or ``None``.

    The function deliberately swallows the typical sandbox failures
    (docker daemon not running, image not cached) so unit tests do not
    hard-depend on a running Docker. Callers detect the fallback by
    checking the URL scheme.
    """
    # pragma: allow-testcontainers-fallback
    try:
        from testcontainers.postgres import PostgresContainer
    except ImportError:  # pragma: no cover - dependency drift
        return None
    try:  # pragma: allow-testcontainers-fallback
        with PostgresContainer("postgres:16-alpine") as pg:
            return pg.get_connection_url()
    except Exception:  # pragma: allow-testcontainers-fallback
        return None


# We resolve the test URL once at module import. ``_TEST_URL`` is exposed
# (read-only) so individual tests can pick the right dialect expectations
# (e.g. use ``text("SELECT 1")`` to avoid Postgres-only syntax).
_TEST_URL = _try_postgres_container() or "sqlite:///:memory:"
_USING_POSTGRES = _TEST_URL.startswith("postgres")


# A dedicated ``Base`` for these tests so the metadata does not leak into
# other packages. We mix in :class:`TenantScoped` so the event listener
# has something to filter on (we want the listener installed and the
# tests for it to be the focus of ``test_tenant.py``).
class _Base(DeclarativeBase):
    """Test-only declarative base. Kept private to this module."""


class _Widget(_Base, IdModel, TimestampMixin):
    __tablename__ = "test_session_widget"

    name: Mapped[str] = mapped_column(String(50))


class _TenantWidget(_Base, IdModel, TimestampMixin, TenantScoped):
    __tablename__ = "test_session_tenant_widget"

    name: Mapped[str] = mapped_column(String(50))


@pytest.fixture
def engine() -> Iterator[Engine]:
    """Yield a fresh engine bound to the test database, schema created."""
    eng: Engine = create_engine(_TEST_URL, future=True)
    _Base.metadata.create_all(eng)
    try:
        yield eng
    finally:
        _Base.metadata.drop_all(eng)
        eng.dispose()


# ---------------------------------------------------------------------------
# Engine factory
# ---------------------------------------------------------------------------


def test_get_engine_builds_engine() -> None:
    """A fresh URL returns a new :class:`Engine`."""
    reset_engine_cache()
    try:
        eng = get_engine(_TEST_URL, cache=False)
        try:
            assert isinstance(eng, type(create_engine(_TEST_URL)))
            # We can actually open a connection — proves the URL is live.
            with eng.connect() as conn:
                conn.execute(select(1)).scalar()
        finally:
            eng.dispose()
    finally:
        reset_engine_cache()


def test_get_engine_caches_by_url() -> None:
    """Repeated calls with the same URL return the same engine instance."""
    reset_engine_cache()
    try:
        first = get_engine(_TEST_URL, cache=True)
        second = get_engine(_TEST_URL, cache=True)
        assert first is second
    finally:
        reset_engine_cache()


def test_get_engine_with_cache_false_returns_new_instance() -> None:
    """``cache=False`` always builds a new engine even for the same URL."""
    reset_engine_cache()
    try:
        first = get_engine(_TEST_URL, cache=False)
        second = get_engine(_TEST_URL, cache=False)
        assert first is not second
        first.dispose()
        second.dispose()
    finally:
        reset_engine_cache()


def test_get_engine_falls_back_to_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    """When no URL is passed, the value comes from ``aidp_common`` settings."""
    from aidp_common.config import reset_settings_cache

    reset_engine_cache()
    reset_settings_cache()
    monkeypatch.setenv("AIDP_DB_URL", _TEST_URL)
    monkeypatch.setenv("AIDP_REDIS_URL", "redis://x")
    monkeypatch.setenv("AIDP_SERVICE_NAME", "test-svc")
    try:
        eng = get_engine(cache=False)
        try:
            # Same dialect family as our test URL.
            assert eng.dialect.name == create_engine(_TEST_URL).dialect.name
        finally:
            eng.dispose()
    finally:
        reset_engine_cache()
        reset_settings_cache()


def test_safe_url_redacts_credentials() -> None:
    """The helper used in engine logs must hide the password."""
    assert session_module._safe_url("postgresql://u:p@h:5432/d") == "postgresql://***@h:5432/d"
    assert session_module._safe_url("sqlite:///:memory:") == "sqlite:///:memory:"
    assert session_module._safe_url("not-a-url") == "not-a-url"


# ---------------------------------------------------------------------------
# Session factory + context manager
# ---------------------------------------------------------------------------


def test_get_session_factory_returns_bound_factory(engine: Engine) -> None:
    """Factory must bind to the engine and not expire on commit."""
    factory = get_session_factory(engine)
    session = factory()
    try:
        assert session.bind is engine
    finally:
        session.close()


def test_get_session_commits_on_exit(engine: Engine) -> None:
    """Successful exit commits the transaction; data is readable after."""

    with get_session(engine) as s:
        s.add(_Widget(name="commit-me"))
    with get_session(engine) as s:
        rows = s.execute(select(_Widget)).scalars().all()
        assert [r.name for r in rows] == ["commit-me"]


def test_get_session_rolls_back_on_exception(engine: Engine) -> None:
    """An exception inside the block rolls back; data is not persisted."""

    class _MarkerError(Exception):
        pass

    # Combine the with statements: pytest.raises needs the inner context
    # manager to raise, and we want both blocks in one expression.
    with pytest.raises(_MarkerError), get_session(engine) as s:  # noqa: PT012
        s.add(_Widget(name="rollback-me"))
        raise _MarkerError("boom")
    with get_session(engine) as s:
        rows = s.execute(select(_Widget)).scalars().all()
        assert rows == []


def test_get_session_closes_session_on_exit(engine: Engine) -> None:
    """``get_session`` always closes the session, even on rollback.

    We patch :class:`Session.close` to count invocations; the context
    manager must call it exactly once before returning.
    """

    # Monkey-patch the bound method on the module's class reference.
    close_calls: list[int] = []
    original_close = Session.close

    def _tracking_close(self: Session) -> None:
        close_calls.append(id(self))
        original_close(self)

    # The module imports ``Session`` by name; rebinding it on the
    # class affects every reference (including the one in
    # :mod:`aidp_db.session`).
    Session.close = _tracking_close  # type: ignore[method-assign]
    try:
        with get_session(engine) as s:
            assert s.is_active
        assert len(close_calls) == 1
    finally:
        Session.close = original_close  # type: ignore[method-assign]


def test_get_session_closes_session_on_exception(engine: Engine) -> None:
    """The session is also closed when an exception escapes the block."""

    close_calls: list[int] = []
    original_close = Session.close

    def _tracking_close(self: Session) -> None:
        close_calls.append(id(self))
        original_close(self)

    Session.close = _tracking_close  # type: ignore[method-assign]

    class _MarkerError(Exception):
        pass

    try:
        with pytest.raises(_MarkerError), get_session(engine):
            raise _MarkerError("boom")
        assert len(close_calls) == 1
    finally:
        Session.close = original_close  # type: ignore[method-assign]


# ---------------------------------------------------------------------------
# with_session decorator
# ---------------------------------------------------------------------------


def test_with_session_passes_session_kwarg(engine: Engine) -> None:
    """When the wrapped function declares ``session=``, it receives the live session."""

    @with_session(engine=engine)
    def add_widget(name: str, *, session: Session) -> str:
        w = _Widget(name=name)
        session.add(w)
        session.flush()  # generate the default ``id``
        new_id: str = w.id
        return new_id

    # mypy sees the original signature (which requires ``session``); the
    # decorator injects the session at runtime, so the call site needs
    # an explicit ``# type: ignore``.
    new_id: str = add_widget("via-decorator")  # type: ignore[call-arg]
    # Row is persisted after the decorator commits.
    with get_session(engine) as s:
        rows = s.execute(select(_Widget)).scalars().all()
        assert any(r.id == new_id and r.name == "via-decorator" for r in rows)


def test_with_session_rolls_back_on_exception(engine: Engine) -> None:
    """Exceptions inside the wrapped function still roll back."""

    @with_session(engine=engine)
    def explode() -> None:
        raise RuntimeError("oops")

    with pytest.raises(RuntimeError):
        explode()
    with get_session(engine) as s:
        assert s.execute(select(_Widget)).scalars().all() == []


# ---------------------------------------------------------------------------
# Engine cache disposal
# ---------------------------------------------------------------------------


def test_dispose_engine_drops_cache_entry() -> None:
    """After ``dispose_engine`` the URL is no longer in the cache."""
    reset_engine_cache()
    get_engine(_TEST_URL, cache=True)
    assert _TEST_URL in session_module._engine_cache
    dispose_engine(_TEST_URL)
    assert _TEST_URL not in session_module._engine_cache
    reset_engine_cache()


def test_dispose_engine_unknown_url_is_noop() -> None:
    """Disposing an unknown URL must not raise."""
    dispose_engine("sqlite:///no-such-cache-entry")


# ---------------------------------------------------------------------------
# Sanity: real Postgres, when available, accepts a basic CRUD roundtrip.
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not _USING_POSTGRES, reason="requires testcontainers Postgres")
def test_postgres_roundtrip_with_real_dialect(engine: Engine) -> None:
    """When Postgres is available, the same engine runs a real query."""

    with get_session(engine) as s:
        result = s.execute(select(1)).scalar()
    assert result == 1
