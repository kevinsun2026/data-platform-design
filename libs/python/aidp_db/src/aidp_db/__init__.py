"""AIDP shared database layer.

Sub-modules:

- :mod:`aidp_db.session` — SQLAlchemy 2.0 engine factory + sync context manager.
- :mod:`aidp_db.tenant` — ``ContextVar``-backed tenant context + event listener
  that auto-injects ``WHERE tenant_id = :current_tenant`` for every ORM select
  against ``TenantScoped`` tables.
- :mod:`aidp_db.migration` — Alembic runner driven by ``AIDP_DB_URL``.
"""

from __future__ import annotations

__version__ = "0.1.0"
