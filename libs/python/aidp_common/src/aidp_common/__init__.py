"""AIDP shared base library.

Sub-modules:

- :mod:`aidp_common.config` — Pydantic Settings-backed configuration.
- :mod:`aidp_common.errors` — Unified ``AppError`` hierarchy + error codes.
- :mod:`aidp_common.logging` — JSON structured logging.
- :mod:`aidp_common.models` — SQLAlchemy 2.0 ORM mixins.
- :mod:`aidp_common.tracing` — OpenTelemetry tracer setup + helpers.
"""

from __future__ import annotations

__version__ = "0.1.0"
