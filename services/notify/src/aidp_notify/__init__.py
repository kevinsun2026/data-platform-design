"""AIDP Notify service.

Sub-modules:

- :mod:`aidp_notify.models` — SQLAlchemy 2.0 ORM tables for notification
  templates, channel configurations, and the per-send log (one row per
  attempted send across every channel).
- :mod:`aidp_notify.main` — FastAPI app factory + lifespan management.
- :mod:`aidp_notify.services.renderer` — Handlebars-style ``{{var}}``
  template rendering with locale-aware variant selection.
- :mod:`aidp_notify.services.dispatcher` — Per-channel send orchestration
  (template lookup, render, retry x3, log).
- :mod:`aidp_notify.channels` — Channel implementations (email via
  :mod:`aiosmtplib`, feishu + webhook via :mod:`httpx`; SMS is a stub).
"""

from __future__ import annotations

__version__ = "0.1.0"
