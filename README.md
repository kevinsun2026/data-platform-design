# AIDP — AI Data Platform

> AI-augmented data platform that turns 4 classes of enterprise data sources
> (databases, object storage, message queues, SaaS APIs) into a unified
> retrieval + reasoning substrate for downstream AI agents.

This repository is the **monorepo root** for the AIDP platform. It is currently
in **Phase 1 (M1-M2, ~8 weeks)**: platform baseline + `datasource-service`
end-to-end. See [`docs/superpowers/plans/`](docs/superpowers/plans/) for the
detailed task breakdown (19 tasks).

---

## Repository layout

```
aidp/                                    # this repo
├── libs/python/                         # shared Python libraries
│   ├── aidp_common/                     # config, logging, tracing, errors, models
│   ├── aidp_db/                         # SQLAlchemy session + tenant filter
│   ├── aidp_auth/                       # JWT + FastAPI dependencies
│   ├── aidp_audit/                      # audit event client
│   ├── aidp_notify/                     # notification client
│   ├── aidp_events/                     # Kafka producer/consumer
│   └── aidp_llm/                        # multi-vendor LLM client
├── services/                            # deployable services
│   ├── gateway/                         # Go — API gateway (port 8000)
│   ├── iam/                             # Python — identity (port 8001)
│   ├── audit/                           # Python — audit (port 8002)
│   ├── notify/                          # Python — notify (port 8003)
│   ├── agent-gateway/                   # Python + Go BFF — AI proxy (port 8004)
│   └── datasource/                      # Python — data sources (port 8005)
├── web/                                 # Next.js 14 admin console (port 3000)
├── deploy/                              # Helm charts + ArgoCD apps
├── proto/                               # gRPC contracts
├── cli/                                 # Go — operations CLI
├── tests/                               # integration / e2e / load tests
├── docs/                                # specs, plans, API docs, runbooks
└── scripts/                             # dev tooling (setup, db seed, etc.)
```

---

## Quick start

### Prerequisites

| Tool       | Version       | Install                                                |
|------------|---------------|--------------------------------------------------------|
| Python     | 3.11+         | https://python.org                                      |
| uv         | 0.5+          | `curl -LsSf https://astral.sh/uv/install.sh \| sh`     |
| Go         | 1.22+         | `brew install go`                                       |
| Node       | 20+           | `brew install node`                                     |
| pnpm       | 9+            | `corepack enable && corepack prepare pnpm@latest --activate` |
| Docker     | 24+           | https://docker.com/products/docker-desktop             |
| Task       | 3.x           | `brew install go-task`                                  |
| pre-commit | 3.7+          | `uv tool install pre-commit`                            |

### One-shot bootstrap

```bash
git clone <repo>
cd <repo>
task setup            # installs deps, starts Postgres/Redis/Kafka via docker compose
task precommit.install
```

`task setup` is idempotent and safe to re-run.

### Daily workflow

```bash
task                  # list available tasks
task lint             # ruff + mypy + pnpm lint
task test             # pytest + pnpm test
task test.int         # integration tests only
task format           # auto-format everything
task typecheck        # mypy only
task precommit.run    # run all pre-commit hooks
```

---

## Tech stack

| Layer        | Choice                                                       |
|--------------|--------------------------------------------------------------|
| Backend      | Python 3.11 + FastAPI 0.110 + Pydantic v2 + SQLAlchemy 2.0   |
| Gateway      | Go 1.22 + Hertz + go-redis                                   |
| Frontend     | Next.js 14 (App Router) + TypeScript + Tailwind + shadcn/ui  |
| DB / Cache   | PostgreSQL 16 + pgvector + Redis 7                           |
| Events       | Kafka 3.x (KRaft)                                            |
| Observability| OpenTelemetry + Prometheus + Loki + Tempo + Grafana          |
| AI           | LangChain 0.2 + LangGraph 0.1 + openai 1.x SDK               |
| Deployment   | K8s 1.29 + Helm 3 + ArgoCD + Skaffold                        |

Full design context: [`docs/superpowers/specs/2026-08-25-ai-data-platform-design.md`](docs/superpowers/specs/2026-08-25-ai-data-platform-design.md).

---

## Conventions

- **Branch model**: trunk-based; `master` is protected.
- **Commit messages**: `feat:` / `fix:` / `chore:` / `refactor:` / `test:` / `docs:`
  prefixes (Conventional Commits).
- **Code style**: Python — google docstring + ruff; Go — godoc + golangci-lint;
  TS — TSDoc + eslint.
- **Python formatter**: `ruff format` is the canonical formatter and **replaces
  `black` by policy**. It is black-compatible (same code style, same stable
  formatting rules), so all `black` configurations transfer as-is — no
  `pyproject.toml` `[tool.black]` section is needed.
- **Tenant isolation (L1)**: enforced at the ORM layer; never write raw SQL
  that bypasses `tenant_id` filtering.
- **Service ports** (fixed):

  | Service        | Port |
  |----------------|------|
  | gateway        | 8000 |
  | iam            | 8001 |
  | audit          | 8002 |
  | notify         | 8003 |
  | agent-gateway  | 8004 |
  | datasource     | 8005 |
  | web            | 3000 |

See `.github/CODEOWNERS` for review responsibilities.

---

## License

Proprietary — internal use only. See `LICENSE` (TBD).
