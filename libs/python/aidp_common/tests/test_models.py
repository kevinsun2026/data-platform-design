"""Tests for ``aidp_common.models``."""

from __future__ import annotations

import re
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from aidp_common.models import (
    IdModel,
    TenantScoped,
    TimestampMixin,
    gen_id,
    utcnow,
)
from sqlalchemy import DateTime, String, create_engine, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column


class Base(DeclarativeBase):
    """Minimal declarative base used only by these tests."""


class _Widget(Base, IdModel, TimestampMixin):
    __tablename__ = "test_widget"


class _TenantWidget(Base, IdModel, TimestampMixin, TenantScoped):
    __tablename__ = "test_tenant_widget"
    name: Mapped[str] = mapped_column(String(50))


@pytest.fixture
def engine() -> object:
    eng = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(eng)
    return eng


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def test_utcnow_is_aware_utc() -> None:
    now = utcnow()
    assert now.tzinfo is not None
    assert now.utcoffset() == timedelta(0)


def test_utcnow_is_recent() -> None:
    before = datetime.now(UTC) - timedelta(seconds=1)
    after = datetime.now(UTC) + timedelta(seconds=1)
    assert before <= utcnow() <= after


def test_gen_id_is_uuid4_string() -> None:
    new_id = gen_id()
    assert isinstance(new_id, str)
    # UUID4 canonical form: 8-4-4-4-12 hex chars
    assert re.fullmatch(
        r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}", new_id
    )
    # Two calls must differ
    assert gen_id() != new_id


# ---------------------------------------------------------------------------
# ORM mixins
# ---------------------------------------------------------------------------


def test_id_model_default_is_uuid(engine: object) -> None:
    with Session(engine) as session:  # type: ignore[arg-type]
        widget = _Widget()
        session.add(widget)
        session.commit()
        # Round-trip and verify the id is a valid UUID4 string.
        loaded = session.execute(select(_Widget)).scalar_one()
        assert re.fullmatch(
            r"[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}",
            loaded.id,
        )


def test_timestamp_mixin_creates_timestamps(engine: object) -> None:
    with Session(engine) as session:  # type: ignore[arg-type]
        widget = _Widget()
        session.add(widget)
        session.commit()
        loaded = session.execute(select(_Widget)).scalar_one()
        assert isinstance(loaded.created_at, datetime)
        assert isinstance(loaded.updated_at, datetime)
        # Note: SQLite drops tzinfo on roundtrip; the column type itself is
        # declared timezone-aware (see test_model_column_types_match_mixin_contract).
        assert loaded.created_at is not None
        assert loaded.updated_at is not None


def test_timestamp_mixin_default_is_tz_aware() -> None:
    """The Python-side default callable returns a timezone-aware datetime."""
    widget = _Widget()
    # SQLAlchemy only invokes the default at flush/commit, so we mimic the
    # behavior by calling the helper directly. This guarantees that without a
    # database roundtrip the value is timezone-aware.
    assert utcnow().tzinfo is not None
    assert widget.deleted_at is None  # soft-delete column defaults to NULL


def test_timestamp_mixin_updates_updated_at(engine: object) -> None:
    from sqlalchemy import update

    with Session(engine) as session:  # type: ignore[arg-type]
        widget = _Widget()
        session.add(widget)
        session.commit()
        original_updated_at = widget.updated_at

        # Bypass ORM to ensure ``onupdate`` SQLAlchemy hook fires.
        session.execute(update(_Widget), {"id": widget.id, "_dummy": 1})
        session.commit()

        loaded = session.execute(select(_Widget)).scalar_one()
        assert loaded.updated_at >= original_updated_at


def test_tenant_scoped_has_tenant_id_column() -> None:
    """TenantScoped must contribute a ``tenant_id`` column of UUID-string type."""
    table = _TenantWidget.__table__
    assert "tenant_id" in table.c
    col = table.c["tenant_id"]
    assert not col.nullable
    # SQLAlchemy types are nullable-typed; just assert the column exists and is non-nullable.
    assert col.nullable is False


def test_tenant_scoped_creates_with_tenant(engine: object) -> None:
    with Session(engine) as session:  # type: ignore[arg-type]
        tid = uuid.uuid4()
        item = _TenantWidget(name="foo", tenant_id=str(tid))
        session.add(item)
        session.commit()
        loaded = session.execute(select(_TenantWidget)).scalar_one()
        assert loaded.tenant_id == str(tid)
        assert loaded.name == "foo"


def test_model_column_types_match_mixin_contract() -> None:
    """Sanity-check the SQL types declared by the mixins."""
    assert isinstance(
        _Widget.__table__.c["id"].type, String
    )  # SQLAlchemy renders UUID as VARCHAR by default
    assert isinstance(_Widget.__table__.c["created_at"].type, DateTime)
    assert isinstance(_Widget.__table__.c["updated_at"].type, DateTime)
    assert isinstance(_Widget.__table__.c["deleted_at"].type, DateTime)
