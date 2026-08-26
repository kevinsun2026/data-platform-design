"""AIDP Audit service.

Sub-modules:

- :mod:`aidp_audit.models` — SQLAlchemy 2.0 declarative models for
  ``audit_events`` / ``audit_payloads`` / ``security_events``.
- :mod:`aidp_audit.crypto` — AES-256-GCM helpers for at-rest payload
  encryption (key from ``AIDP_AUDIT_PAYLOAD_KEY``).
- :mod:`aidp_audit.consumer` — Kafka consumer that subscribes to the
  ``audit.*`` topic pattern and batch-persists events.
- :mod:`aidp_audit.api.query` — tenant-scoped read-only REST API.
"""

from __future__ import annotations

__version__ = "0.1.0"
