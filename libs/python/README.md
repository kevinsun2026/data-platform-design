# libs/python/

Shared Python libraries used by every backend service.

Each subdirectory is a separate package published to the local uv workspace and
importable by services via `from aidp_<name> import ...`.

| Package        | Purpose                                            |
|----------------|----------------------------------------------------|
| aidp_common    | config, logging, tracing, errors, base models      |
| aidp_db        | SQLAlchemy session + tenant filter + Alembic       |
| aidp_auth      | JWT + FastAPI dependencies                         |
| aidp_audit     | audit event client                                 |
| aidp_notify    | notification client                                |
| aidp_events    | Kafka producer/consumer                            |
| aidp_llm       | multi-vendor LLM client                            |

Packages are populated by Tasks 2–5 of the Phase 1 plan.
