# AIDP IAM Service

> Identity & access management: tenants, users, groups, roles, API
> keys, and refresh sessions. Listens on **port 8001** behind the
> platform gateway.

This is the first Python service in the AIDP monorepo. It is built
on the shared libraries in `libs/python/`:

| Library | Role |
|---------|------|
| `aidp_common` | Config, logging, tracing, errors, ORM mixins |
| `aidp_db` | SQLAlchemy engine, mandatory L1 tenant filter, Alembic runner |
| `aidp_auth` | JWT sign/verify, FastAPI auth dependencies |

The HTTP layer is FastAPI 0.110 + Pydantic v2 + SQLAlchemy 2.0.

## Stack

| Component | Choice |
|-----------|--------|
| Language  | Python 3.11 |
| HTTP      | FastAPI 0.110 + uvicorn |
| ORM       | SQLAlchemy 2.0 (declarative-mapped) |
| Migrations| Alembic 1.13 |
| Auth      | JWT HS256, refresh-token server-side sessions |
| Hashing   | Argon2id (`argon2-cffi`) for passwords and API-key secrets |
| Container | distroless Python 3.11 |

## Schema (Phase 1, Task 7)

| Table | Purpose |
|-------|---------|
| `tenants` | Top-level organization. *Root* of L1 isolation. |
| `users` | Platform user; carries Argon2id password hash. |
| `groups` | Self-referential many-to-many collection of users. |
| `user_group_members` | Junction table for `users` × `groups`. |
| `roles` | RBAC role definition (permissions, scope). |
| `user_role_bindings` | `users` × `roles` with optional resource scoping + expiry. |
| `api_keys` | Long-lived bearer credentials (Argon2id-hashed, prefix-indexed). |
| `sessions` | Server-side refresh-token session record. |

Every table except `tenants` and `user_group_members` extends
`IdModel + TimestampMixin + TenantScoped` from `aidp_common.models`,
so the L1 listener auto-injects `WHERE tenant_id = :current_tenant`
on every select.

## Develop

```bash
cd services/iam

# Apply migrations to the configured AIDP_DB_URL
uv run alembic upgrade head

# Run the FastAPI app
uv run uvicorn aidp_iam.main:app --host 0.0.0.0 --port 8001 --reload

# Tests
uv run pytest                       # unit tests (SQLite fallback)
uv run pytest --cov=aidp_iam        # with coverage
uv run mypy src/                    # strict type check
uv run ruff check . && uv run ruff format .
```

### Test database

Tests prefer a testcontainers Postgres instance. When the Docker
daemon or `postgres:16-alpine` image is unavailable, the suite falls
back to an in-memory SQLite database (annotated with
`# pragma: allow-testcontainers-fallback` so the policy is grep-able
from the codebase). Both paths exercise the same SQLAlchemy ORM and
the L1 tenant filter.

## Configuration

All configuration is environment-driven (`AIDP_*`):

| Var | Default | Notes |
|-----|---------|-------|
| `AIDP_DB_URL` | (required) | SQLAlchemy URL (Postgres in prod, SQLite for tests) |
| `AIDP_REDIS_URL` | (required) | Redis connection URL |
| `AIDP_SERVICE_NAME` | `aidp-iam` | OTel resource attribute |
| `AIDP_LOG_LEVEL` | `INFO` | Root log level |
| `AIDP_ENV` | `dev` | Deployment env label |
| `AIDP_OTLP_ENDPOINT` | (unset) | OTLP gRPC endpoint (unset = no exporter) |
| `AIDP_JWT_SECRET` | dev-only fallback | JWT signing secret (HS256) |

## Container

```bash
docker build -t ghcr.io/aidp/iam:dev -f deploy/Dockerfile .
docker run --rm -p 8001:8001 \
  -e AIDP_DB_URL=postgresql+psycopg://... \
  ghcr.io/aidp/iam:dev
```

## Deploy

```bash
helm install iam ./deploy/iam \
  --namespace aidp --create-namespace \
  --set image.tag=0.1.0 \
  --set db.url=postgresql+psycopg://...
```
