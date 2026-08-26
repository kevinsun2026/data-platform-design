# aidp-datasource

AIDP Datasource service. Manages the registry of external database
connections (PostgreSQL / MySQL / Oracle / Hive) with
AES-256-GCM-encrypted credentials.

## Public surface

- HTTP/REST on port ``8005``:
  - ``POST   /api/v1/datasources``
  - ``GET    /api/v1/datasources?env=&kind=&tag=``
  - ``GET    /api/v1/datasources/{id}``
  - ``PUT    /api/v1/datasources/{id}``
  - ``DELETE /api/v1/datasources/{id}`` (soft delete)
  - ``POST   /api/v1/datasources/{id}/test``
  - ``GET    /api/v1/datasources/types``
  - ``GET    /healthz`` (liveness), ``GET /readyz`` (readiness)
- gRPC on port ``50051`` (internal, agent-gateway is the only
  consumer):
  - ``DataSourceService.GetConnection(GetConnectionRequest)``
- Kafka events: ``datasource.registered.v1`` /
  ``datasource.updated.v1`` / ``datasource.disabled.v1`` /
  ``datasource.test.succeeded.v1`` / ``datasource.test.failed.v1`` on
  the ``datasource.events.v1`` topic.

## Required environment

| Var | Purpose |
| --- | --- |
| ``AIDP_DB_URL`` | SQLAlchemy URL (Postgres in production) |
| ``AIDP_REDIS_URL`` | Redis URL (cache / rate limit) |
| ``AIDP_SERVICE_NAME`` | Service name for logs/traces (e.g. ``aidp-datasource``) |
| ``AIDP_KAFKA_BROKERS`` | Comma-separated Kafka bootstrap list |
| ``AIDP_JWT_SECRET`` | HS256 secret (>=32 bytes) |
| ``AIDP_DATASOURCE_CREDENTIAL_KEY`` | URL-safe-base64 of a 32-byte AES-256 key |
| ``AIDP_DATASOURCE_GRPC_PORT`` | Optional; defaults to ``50051`` |
