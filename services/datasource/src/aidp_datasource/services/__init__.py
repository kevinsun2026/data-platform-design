"""Service-layer package.

Sub-modules:

- :mod:`aidp_datasource.services.credential_service` — AES-256-GCM
  credential encryption.
- :mod:`aidp_datasource.services.datasource_service` — Business
  orchestration (CRUD, soft delete, connection test, audit + Kafka
  events).
- :mod:`aidp_datasource.services.schema_service` — Schema sync +
  listing + preview + DDL (Task 15).
- :mod:`aidp_datasource.services.pii_service` — PII auto-suggestion
  via the agent-gateway ``/v1/chat/completions`` endpoint
  (Task 16).
"""

from __future__ import annotations
