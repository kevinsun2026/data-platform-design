"""SQLAlchemy 2.0 declarative-mapped base classes shared by every service.

The mixins in this module give every table the columns required by the AIDP
global constraints (id, tenant_id, created_at, updated_at, created_by,
updated_by, deleted_at). They are designed to be combined with a service-local
``DeclarativeBase`` so cross-service models don't share a metadata registry.

Example::

    from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
    from aidp_common.models import IdModel, TimestampMixin, TenantScoped

    class Base(DeclarativeBase):
        pass

    class User(Base, IdModel, TimestampMixin, TenantScoped):
        __tablename__ = "users"
        email: Mapped[str] = mapped_column(unique=True)
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from sqlalchemy import DateTime, String
from sqlalchemy.orm import Mapped, mapped_column


def utcnow() -> datetime:
    """Return the current UTC time as a timezone-aware :class:`datetime`."""
    return datetime.now(UTC)


def gen_id() -> str:
    """Generate a new UUID4 string used as the primary key of AIDP rows."""
    return str(uuid.uuid4())


class IdModel:
    """Mixin contributing a UUID4 ``id`` primary key column."""

    id: Mapped[str] = mapped_column(
        String(36),
        primary_key=True,
        default=gen_id,
    )


class TimestampMixin:
    """Mixin contributing the standard audit / soft-delete columns.

    Columns:
        created_at: Set to :func:`utcnow` on insert. Non-null.
        updated_at: Set to :func:`utcnow` on insert and update. Non-null.
        created_by: Optional user id of the creator.
        updated_by: Optional user id of the last modifier.
        deleted_at: Soft-delete tombstone. ``NULL`` when the row is live.
    """

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utcnow,
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utcnow,
        onupdate=utcnow,
        nullable=False,
    )
    created_by: Mapped[str | None] = mapped_column(String(64), nullable=True)
    updated_by: Mapped[str | None] = mapped_column(String(64), nullable=True)
    deleted_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )


class TenantScoped:
    """Mixin contributing the mandatory ``tenant_id`` column for L1 isolation.

    The aidp_db layer auto-injects ``WHERE tenant_id = :current_tenant`` for
    every query against tables that include this mixin. Direct INSERTs must
    populate ``tenant_id`` explicitly (typically via the request-scoped
    context set by the auth middleware).
    """

    tenant_id: Mapped[str] = mapped_column(
        String(36),
        nullable=False,
        index=True,
    )


__all__ = [
    "IdModel",
    "TenantScoped",
    "TimestampMixin",
    "gen_id",
    "utcnow",
]
