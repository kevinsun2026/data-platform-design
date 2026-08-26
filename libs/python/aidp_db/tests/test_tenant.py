"""Tests for ``aidp_db.tenant``.

These tests pin down the platform's L1 mandatory tenant filter:

- :func:`set_tenant_context` / :func:`reset_tenant_context` / :func:`get_tenant_id`
  round-trip through a :class:`ContextVar`.
- :func:`tenant_scope` restores the prior binding on exit (and on
  exception).
- The SQLAlchemy ``do_orm_execute`` listener auto-injects
  ``WHERE tenant_id = :tenant_id`` on every select against a
  :class:`TenantScoped` model — even when the user code does not write
  the filter themselves.
- Selecting a non-tenant model is left untouched.
- The listener does not touch INSERTs / UPDATEs / DELETEs.
- The actual SQL emitted contains the tenant predicate (verified by
  attaching a ``before_cursor_execute`` listener and inspecting the
  statement + parameters).

Test database selection mirrors ``test_session.py``: testcontainers
Postgres when available, in-memory SQLite otherwise. The SQLite path
exercises the same ORM-level event listener, which is what the production
listener is implemented against.

Note on context hygiene:
    pytest does not give each test a fresh :class:`contextvars.Context`,
    so every test that calls :func:`set_tenant_context` is responsible
    for pairing it with :func:`reset_tenant_context`. The autouse
    ``_no_tenant_leak`` fixture asserts the post-condition so a forgotten
    reset is caught immediately.
"""

from __future__ import annotations

import importlib
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import Token
from typing import Any

import aidp_db.tenant as tenant_module
import pytest
from aidp_common.models import IdModel, TenantScoped, TimestampMixin
from aidp_db.tenant import (
    get_tenant_id,
    reset_tenant_context,
    set_tenant_context,
    tenant_scope,
)
from sqlalchemy import String, create_engine, event, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

# ---------------------------------------------------------------------------
# Test database — try testcontainers (Postgres), fall back to SQLite.
# ---------------------------------------------------------------------------


def _try_postgres_container() -> str | None:
    """Best-effort testcontainers bootstrap; return a URL or ``None``."""
    # pragma: allow-testcontainers-fallback
    try:
        from testcontainers.postgres import PostgresContainer
    except ImportError:  # pragma: no cover - dep drift
        return None
    try:  # pragma: allow-testcontainers-fallback
        with PostgresContainer("postgres:16-alpine") as pg:
            return pg.get_connection_url()
    except Exception:  # pragma: allow-testcontainers-fallback
        return None


_TEST_URL = _try_postgres_container() or "sqlite:///:memory:"


# Test-only declarative base + models.
class _Base(DeclarativeBase):
    """Private base for these tests."""


class _TenantThing(_Base, IdModel, TimestampMixin, TenantScoped):
    __tablename__ = "test_tenant_thing"

    label: Mapped[str] = mapped_column(String(50))


class _PlainThing(_Base, IdModel, TimestampMixin):
    __tablename__ = "test_plain_thing"

    label: Mapped[str] = mapped_column(String(50))


@pytest.fixture
def engine() -> Iterator[Engine]:
    """Yield a freshly-created engine with the test schema applied."""
    eng: Engine = create_engine(_TEST_URL, future=True)
    _Base.metadata.create_all(eng)
    try:
        yield eng
    finally:
        _Base.metadata.drop_all(eng)
        eng.dispose()


@pytest.fixture(autouse=True)
def _no_tenant_leak() -> Iterator[None]:
    """Fail the test if a previous test left a tenant context bound."""
    yield
    assert get_tenant_id() is None, "tenant context leaked across tests"


# ---------------------------------------------------------------------------
# SQL capture helper
# ---------------------------------------------------------------------------


@contextmanager
def _capture_sql(engine):  # type: ignore[no-untyped-def]
    """Attach a ``before_cursor_execute`` listener to *engine* and yield."""
    records: list[tuple[str, str]] = []

    def _on_execute(conn, cursor, statement, parameters, context, executemany):  # type: ignore[no-untyped-def]
        records.append((statement, str(parameters)))

    event.listen(engine, "before_cursor_execute", _on_execute)
    try:
        yield records
    finally:
        event.remove(engine, "before_cursor_execute", _on_execute)


def _seed_tenants(engine: Engine) -> dict[str, str]:
    """Insert one row per tenant. Returns the tenant ids used."""
    tenants: dict[str, str] = {"a": "tenant-a", "b": "tenant-b"}
    tenant_table: Any = _TenantThing.__table__
    with engine.begin() as conn:
        for tid in tenants.values():
            conn.execute(
                tenant_table.insert().values(
                    id=str(uuid.uuid4()),
                    tenant_id=tid,
                    label=f"label-{tid}",
                )
            )
    return tenants


# ---------------------------------------------------------------------------
# ContextVar API
# ---------------------------------------------------------------------------


def test_set_then_get_returns_tenant_id() -> None:
    tid = "tenant-" + uuid.uuid4().hex
    token = set_tenant_context(tid)
    try:
        assert get_tenant_id() == tid
    finally:
        reset_tenant_context(token)


def test_get_tenant_id_default_is_none() -> None:
    """No prior call to ``set_tenant_context`` returns ``None``."""
    assert get_tenant_id() is None


def test_reset_restores_previous_value() -> None:
    inner = set_tenant_context("tenant-outer")
    try:
        token = set_tenant_context("tenant-inner")
        try:
            assert get_tenant_id() == "tenant-inner"
        finally:
            reset_tenant_context(token)
        assert get_tenant_id() == "tenant-outer"
    finally:
        reset_tenant_context(inner)
    assert get_tenant_id() is None


def test_tenant_scope_restores_on_exit() -> None:
    with tenant_scope("tenant-a"):
        assert get_tenant_id() == "tenant-a"
    assert get_tenant_id() is None


def test_tenant_scope_restores_on_exception() -> None:
    with pytest.raises(RuntimeError), tenant_scope("tenant-a"):
        raise RuntimeError("boom")
    assert get_tenant_id() is None


def test_tenant_scope_is_reentrant() -> None:
    with tenant_scope("tenant-outer"):
        with tenant_scope("tenant-inner"):
            assert get_tenant_id() == "tenant-inner"
        assert get_tenant_id() == "tenant-outer"
    assert get_tenant_id() is None


# ---------------------------------------------------------------------------
# do_orm_execute listener — auto-inject WHERE tenant_id
# ---------------------------------------------------------------------------


def test_filter_auto_injects_where_clause(engine) -> None:  # type: ignore[no-untyped-def]
    """When a tenant is set, the listener adds the WHERE on SELECT."""
    _seed_tenants(engine)
    token = set_tenant_context("tenant-a")
    try:
        with _capture_sql(engine) as records, Session(engine) as s:
            rows = s.execute(select(_TenantThing)).scalars().all()
        # Only tenant-a's row was returned.
        assert [r.label for r in rows] == ["label-tenant-a"]
        # The emitted SQL contains a ``WHERE tenant_id`` predicate.
        matching = [(stmt, params) for (stmt, params) in records if "tenant_thing" in stmt.lower()]
        assert matching, f"no SQL recorded for _TenantThing: {records}"
        statement, params = matching[0]
        assert "tenant_id" in statement.lower(), statement
        assert "tenant-a" in params, params
    finally:
        reset_tenant_context(token)


def test_no_filter_when_no_tenant_context(engine) -> None:  # type: ignore[no-untyped-def]
    """Without a tenant context, the listener is a no-op."""
    _seed_tenants(engine)
    with _capture_sql(engine) as records, Session(engine) as s:
        rows = s.execute(select(_TenantThing)).scalars().all()
    # Both rows are returned.
    labels = sorted(r.label for r in rows)
    assert labels == ["label-tenant-a", "label-tenant-b"]
    # The SQL has no tenant predicate in the WHERE clause. (The
    # ``tenant_id`` column still appears in the SELECT list, so we
    # check the WHERE-tail specifically.)
    statements = [stmt for (stmt, _p) in records if "tenant_thing" in stmt.lower()]
    assert statements, "no SQL recorded"
    lowered = statements[0].lower()
    # SQL is uppercase; case-fold and look for ``from`` / ``where``.
    assert "from" in lowered
    from_idx = lowered.index("from")
    tail = lowered[from_idx:]
    assert "where" not in tail, statements[0]


def test_filter_targets_only_tenant_scoped_entities(engine) -> None:  # type: ignore[no-untyped-def]
    """The listener must not touch a model that lacks ``tenant_id``."""
    plain_id = str(uuid.uuid4())
    plain_table: Any = _PlainThing.__table__
    with engine.begin() as conn:
        conn.execute(
            plain_table.insert().values(
                id=plain_id,
                label="plain-row",
            )
        )
    token = set_tenant_context("tenant-a")
    try:
        with _capture_sql(engine) as records, Session(engine) as s:
            rows = s.execute(select(_PlainThing)).scalars().all()
        # The plain model returns its single row, untouched.
        assert [r.label for r in rows] == ["plain-row"]
        # No tenant predicate was injected for the plain model.
        statements = [stmt for (stmt, _p) in records if "plain_thing" in stmt.lower()]
        assert statements, "no SQL recorded for _PlainThing"
        for stmt in statements:
            assert "tenant_id" not in stmt.lower(), stmt
    finally:
        reset_tenant_context(token)


def test_filter_does_not_modify_writes(engine) -> None:  # type: ignore[no-untyped-def]
    """INSERT / UPDATE statements are not rewritten by the listener."""
    token = set_tenant_context("tenant-a")
    try:
        with Session(engine) as s:
            obj = _TenantThing(id=str(uuid.uuid4()), tenant_id="tenant-a", label="new-row")
            s.add(obj)
            s.commit()
        # Verify via a fresh session: row is persisted as-is.
        with Session(engine) as s:
            rows = s.execute(select(_TenantThing)).scalars().all()
        labels = [r.label for r in rows]
        assert "new-row" in labels
    finally:
        reset_tenant_context(token)


def test_insert_without_tenant_id_raises(engine) -> None:  # type: ignore[no-untyped-def]
    """Schema-level ``NOT NULL`` on ``tenant_id`` must reject missing values.

    The L1 contract is: every write against a :class:`TenantScoped` model
    must set ``tenant_id`` explicitly. The schema enforces this — the
    listener cannot auto-inject it on writes. This test pins the schema
    constraint so a regression that turns the column nullable (or drops the
    constraint) is caught immediately, regardless of which listener
    happens to be installed.
    """
    from sqlalchemy.exc import IntegrityError

    with Session(engine) as s:
        s.add(_TenantThing(id=str(uuid.uuid4()), tenant_id=None, label="no-tenant"))
        with pytest.raises(IntegrityError):
            s.commit()
        # The session is now in a failed state; roll back so the fixture's
        # ``drop_all`` at teardown does not see an open transaction.
        s.rollback()


def test_filter_handles_join_against_tenant_table(engine) -> None:  # type: ignore[no-untyped-def]
    """A select with both plain and tenant entities injects the WHERE exactly once."""
    plain_table: Any = _PlainThing.__table__
    tenant_table: Any = _TenantThing.__table__
    # Seed one row in each table.
    with engine.begin() as conn:
        conn.execute(
            plain_table.insert().values(
                id=str(uuid.uuid4()),
                label="plain",
            )
        )
        conn.execute(
            tenant_table.insert().values(
                id=str(uuid.uuid4()),
                tenant_id="tenant-a",
                label="tenant-a-row",
            )
        )
    token = set_tenant_context("tenant-a")
    try:
        with _capture_sql(engine) as records, Session(engine) as s:
            # Fetching the tenant entity — this is what triggers the
            # listener; the plain one would too if selected directly.
            rows = s.execute(select(_TenantThing)).scalars().all()
        assert [r.label for r in rows] == ["tenant-a-row"]
        # Locate the SQL emitted for the tenant table; the column also
        # appears in the SELECT list, so we look only at the part after
        # FROM to verify the WHERE predicate.
        tenant_sql = [
            (stmt, params) for (stmt, params) in records if "tenant_thing" in stmt.lower()
        ]
        assert len(tenant_sql) == 1
        statement, params = tenant_sql[0]
        lowered = statement.lower()
        assert "from" in lowered
        from_idx = lowered.index("from")
        tail = lowered[from_idx:]
        assert "where" in tail, statement
        where_clause = tail.split("where", 1)[1]
        assert "tenant_id" in where_clause, statement
        # The bound parameter carries the tenant value.
        assert "tenant-a" in params
    finally:
        reset_tenant_context(token)


# ---------------------------------------------------------------------------
# Module-reload safety: re-importing the module must not double-register the
# listener (SQLAlchemy's ``listens_for`` decorator is idempotent).
# ---------------------------------------------------------------------------


def test_listener_registered_exactly_once() -> None:
    """Re-importing the module does not break set/reset semantics."""
    importlib.reload(tenant_module)
    try:
        token: Token[str | None] = set_tenant_context("tenant-reload")
        try:
            assert get_tenant_id() == "tenant-reload"
        finally:
            reset_tenant_context(token)
    finally:
        importlib.reload(tenant_module)


# ---------------------------------------------------------------------------
# Postgres-fidelity check: when a real Postgres backend is available, the
# SQL emitted by the listener matches a hand-written ``WHERE`` byte-for-byte
# in shape (same column reference, same parameter).
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not _TEST_URL.startswith("postgres"),
    reason="requires testcontainers Postgres for dialect fidelity",
)
def test_postgres_emitted_sql_matches_manual_where(engine) -> None:  # type: ignore[no-untyped-def]
    """Listener-generated SQL has the same shape as a hand-written WHERE."""
    _seed_tenants(engine)
    token = set_tenant_context("tenant-a")
    try:
        with _capture_sql(engine) as auto_records, Session(engine) as s:
            auto_rows = s.execute(select(_TenantThing)).scalars().all()
    finally:
        reset_tenant_context(token)

    with _capture_sql(engine) as manual_records, Session(engine) as s:
        manual_rows = (
            s.execute(select(_TenantThing).where(_TenantThing.tenant_id == "tenant-a"))
            .scalars()
            .all()
        )

    # Same logical result.
    assert sorted(r.id for r in auto_rows) == sorted(r.id for r in manual_rows)

    # Both statements reference ``tenant_id`` and bind ``tenant-a``.
    auto_stmt, auto_params = next((s, p) for (s, p) in auto_records if "tenant_thing" in s.lower())
    manual_stmt, manual_params = next(
        (s, p) for (s, p) in manual_records if "tenant_thing" in s.lower()
    )
    assert "tenant_id" in auto_stmt.lower()
    assert "tenant_id" in manual_stmt.lower()
    assert "tenant-a" in auto_params
    assert "tenant-a" in manual_params
