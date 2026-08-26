"""AIDP IAM service.

Sub-modules:

- :mod:`aidp_iam.models` — SQLAlchemy 2.0 ORM tables for tenants, users,
  groups, roles, role bindings, API keys, and refresh sessions.
- :mod:`aidp_iam.main` — FastAPI app factory + lifespan management.
"""

from __future__ import annotations

__version__ = "0.1.0"
