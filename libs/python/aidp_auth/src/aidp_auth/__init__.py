"""AIDP shared auth library.

Sub-modules:

- :mod:`aidp_auth.jwt` — :func:`create_access_token` /
  :func:`create_refresh_token` (HS256) and :func:`decode_token` with
  platform-wide :class:`TokenClaims` / :class:`CurrentUser` models.
- :mod:`aidp_auth.dependencies` — FastAPI :data:`current_user` and
  :func:`require_permission` dependency-injection helpers that also bind
  the request-scoped :class:`aidp_db.tenant.set_tenant_context` so any
  downstream DB call gets the L1 tenant filter automatically.
"""

from __future__ import annotations

__version__ = "0.1.0"
