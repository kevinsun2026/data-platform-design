# AIDP Agent Gateway

The Agent Gateway is the platform's transparent multi-provider LLM
proxy. It exposes an OpenAI-compatible HTTP surface (so any tool
that talks to OpenAI works against the gateway out of the box) and
internally routes calls to a configured pool of upstream providers
based on:

- `model_tier` (flagship / balanced / economy)
- `task_type` (sql / etl / insight)
- per-model name (when the caller requests a specific model)

If the chosen provider fails (5xx, timeout, 429), the gateway
**fails over** to the cheapest healthy alternative in the same
tier. After **3 consecutive failures** the provider's circuit
breaker opens for **5 minutes**; calls are short-circuited to the
next provider during the cool-off. After the cool-off the breaker
moves to half-open and the next call is a probe.

Every successful call is **metered**: the gateway records the token
counts (from the upstream `usage` block), calculates the USD cost
against the provider's published price list, and asynchronously
writes the row to ClickHouse (when `AIDP_AGENT_CLICKHOUSE_URL` is
set) or to the `agent_llm_usage` Postgres table (fallback).

## Endpoints

| Method | Path | Purpose |
| ------ | ---- | ------- |
| `POST` | `/v1/chat/completions` | OpenAI-compat chat completions (with routing + failover + metering) |
| `GET`  | `/v1/models` | List registered models (OpenAI-compat, with AIDP diagnostic fields) |
| `POST` | `/api/v1/agent/credentials` | Store a per-tenant API-key override (BYOK) |
| `GET`  | `/healthz` | Liveness probe |
| `GET`  | `/readyz` | Readiness probe |

The service listens on **port 8004**.

## Build & run

```bash
# install (workspace)
uv sync --all-packages

# type-check
uv run mypy --strict services/agent-gateway

# test + coverage
cd services/agent-gateway && uv run pytest --cov=aidp_agent --cov-fail-under=80

# run locally
uv run uvicorn aidp_agent.main:app --host 0.0.0.0 --port 8004
```

## Configuration

| Env | Default | Purpose |
| --- | ------- | ------- |
| `AIDP_DB_URL` | (required) | Postgres URL for the metering fallback table |
| `AIDP_REDIS_URL` | (required) | Redis URL (kept for symmetry with other services) |
| `AIDP_KAFKA_BROKERS` | `localhost:9092` | Kafka brokers (currently unused; the gateway does not consume) |
| `AIDP_SERVICE_NAME` | `aidp-agent` | Service label for logs / traces |
| `AIDP_AGENT_CLICKHOUSE_URL` | (unset) | ClickHouse HTTP endpoint for the metering sink; when unset, rows fall back to Postgres |
| `AIDP_AGENT_TASK_TIER_OVERRIDES` | (unset) | JSON dict overriding the default `task_type -> tier` mapping |
