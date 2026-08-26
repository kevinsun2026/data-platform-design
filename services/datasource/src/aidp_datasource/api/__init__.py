"""HTTP API package for the AIDP Datasource service.

Sub-modules:

- :mod:`aidp_datasource.api.errors` — Unified ``AppError`` envelope
  exception handler.
- :mod:`aidp_datasource.api.datasources` — Datasource CRUD + connection
  test + supported-types list endpoints under
  ``/api/v1/datasources``.
- :mod:`aidp_datasource.api.schemas` — Schema sync + listing +
  preview + DDL endpoints (Task 15).
- :mod:`aidp_datasource.api.policies` — Policy upsert / fetch +
  PII suggestion endpoints (Task 16).
"""

from __future__ import annotations
