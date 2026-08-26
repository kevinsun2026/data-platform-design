"""AIDP Datasource service.

Sub-modules:

- :mod:`aidp_datasource.models` — SQLAlchemy 2.0 ORM tables for the
  datasource registry (datasources, schemas, policies, connection-test
  history, audit log).
- :mod:`aidp_datasource.connectors` — Connector Protocol + 4 driver
  implementations (PostgreSQL / MySQL / Oracle / Hive).
- :mod:`aidp_datasource.services.credential_service` — AES-256-GCM
  at-rest encryption for the ``credentials`` blob. The key is sourced
  from the ``AIDP_DATASOURCE_CREDENTIAL_KEY`` environment variable; a
  future task wires the same slot to a KMS-backed envelope-encryption
  provider.
- :mod:`aidp_datasource.services.datasource_service` — Business
  orchestration: CRUD, soft delete, connection test, audit + Kafka
  events.
- :mod:`aidp_datasource.api.datasources` — REST handlers at
  ``/api/v1/datasources``.
- :mod:`aidp_datasource.proto.server` — Internal gRPC server exposing
  ``DataSourceService.GetConnection`` (consumed by the agent-gateway).
- :mod:`aidp_datasource.main` — FastAPI app factory + lifespan
  management, port 8005.
"""

from __future__ import annotations

__version__ = "0.1.0"
