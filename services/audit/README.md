# aidp-audit

AIDP 审计服务（Task 10）。

- **职责**：消费所有服务发布的 `audit.*` Kafka 事件 → 批量落库；并对外提供
  `GET /api/v1/audit/events` 系列查询 API（强租户隔离）。
- **端口**：8002（与 plan 端口表一致）。
- **存储**：Postgres（生产）/ SQLite（沙箱 fallback）。Schema 由 Alembic 管理。
- **payload 加密**：`audit_payloads.ciphertext` 用 AES-256-GCM 加密（密钥来自
  `AIDP_AUDIT_PAYLOAD_KEY`）；仅授权用户能在 `GET /api/v1/audit/events/{id}`
  时拿到解密后的 payload。

## 表结构

- `audit_events` — 审计事件主表（必含 `tenant_id` + `event_id` 唯一约束）。
- `audit_payloads` — 一对一关联 `audit_events.id`，存密文 + nonce + aad。
- `security_events` — 高敏安全事件（登录失败、密码重置、提权尝试等）。

详见 [`alembic/versions/0001_initial.py`](alembic/versions/0001_initial.py)。

## Kafka 消费

- **Topic pattern**：`audit.*` —— 任意服务发布的 audit 主题都被捕获。
- **Group id**：`aidp-audit-consumer`（单一 group，所有审计事件路由到一处）。
- **幂等**：handler 通过 `idempotency_key = f"{tenant_id}:{event_id}"` 配合
  `audit_events.tenant_id + audit_events.event_id` 唯一约束保证去重。
- **批量落库**：buffer 满 100 条或满 5s（`AIDP_AUDIT_FLUSH_*` env 可配）即 flush。

## 启动

```bash
cd services/audit
uv sync
export AIDP_DB_URL=postgresql://...
export AIDP_REDIS_URL=redis://...
export AIDP_KAFKA_BROKERS=localhost:9092
export AIDP_AUDIT_PAYLOAD_KEY=<base64-32B>
uv run uvicorn aidp_audit.main:app --port 8002
```

## 测试

```bash
cd services/audit
uv run pytest
```

测试在沙箱里自动 fallback 到 SQLite + `InMemoryTransport`；当 docker 可用时
会跑 testcontainers（Postgres + Kafka）。
