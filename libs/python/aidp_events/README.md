# aidp_events

AIDP 平台共享事件总线。基于 `aiokafka`（与 FastAPI asyncio 兼容）实现 Kafka
producer / consumer，对外暴露统一的 `EventEnvelope` 模型、指数退避重试、
死信队列（DLQ）以及业务幂等键支持。

## 核心概念

- **`EventEnvelope`** — 事件传输的标准化载荷：包含 `event_id` (UUID4) /
  `tenant_id` / `occurred_at` (ISO 8601 UTC) / `producer` (服务名) /
  `event_type` / `payload` (业务字典) / `trace_id` (OTel 32-hex) /
  `event_version` / `headers` (可扩展字典)。
- **publish_event** — 异步发布事件：自动填充 `event_id` / `occurred_at` /
  `trace_id`，按 `tenant_id + event_id` 作为 Kafka key 写入，失败指数退避重试
  3 次，最终写入 `topic + ".dlq"`。
- **consume_events** — 异步 at-least-once 消费：handler 抛错时 nack 走重试，
  超出 `max_retries` 后自动投递到 DLQ；handler 接收 `idempotency_key` =
  `(tenant_id, event_id)` 作为业务去重依据。
- **Handler** — `async def handler(envelope, *, idempotency_key) -> None`，
  异常即视为 nack，成功即 commit offset。

## 用法

```python
# 1. 业务侧发布事件
from aidp_events.producer import publish_event

await publish_event(
    topic="datasource.connections",
    event_type="datasource.connection.created",
    tenant_id="tenant-uuid-...",
    payload={"connection_id": "conn-1", "type": "postgres"},
)

# 2. 业务侧消费事件
from aidp_events.consumer import consume_events


async def on_connection_created(envelope, *, idempotency_key):
    # 幂等保证：(envelope.tenant_id, envelope.event_id) 不重复处理
    async with db.transaction():
        await upsert_connection(envelope.payload, idempotency_key=idempotency_key)


await consume_events(
    topic="datasource.connections",
    group_id="audit-service",
    handler=on_connection_created,
)
```

## 关键设计

1. **OTel 集成** — `EventEnvelope.trace_id` 在无活跃 span 时退化为
   `envelope 内的 uuid4 派生 hex`（仍满足 32-hex 格式），保证消费侧可按
   `trace_id` 关联日志。
2. **Tenant 隔离在 Kafka 层靠 key 落地** — `publish_event` 把 `tenant_id`
   作为 Kafka key（`tenant_id:<event_id>`）写入；同 tenant 顺序保证由
   partition 数量（默认 broker 决定）维持。L1 隔离仍由 `aidp_db` 守门。
3. **DLQ 协议** — 重试 3 次（指数退避 100ms / 200ms / 400ms）后，写入
   `topic + ".dlq"`，原 envelope 头增加 `x-original-topic` /
   `x-retry-count` / `x-error-message`，不丢任何 payload。
4. **at-least-once** — `enable_auto_commit=False`；handler 成功后才
   `commit()`，失败抛错时 offset 保持原位；下次拉取会重试（直到 DLQ）。
5. **业务幂等键** — `consume_events` 自动从 envelope 派生
   `idempotency_key = f"{tenant_id}:{event_id}"`，handler 必须按此去重。
6. **Sandbox fallback for testcontainers** — 沙箱拉不到 `confluentinc/cp-kafka`
   镜像时退化为内存 fake transport；源码用 `# pragma: allow-testcontainers-fallback`
   标注。

## 凭据 / 部署

- Kafka 集群地址从 `AIDP_KAFKA_BROKERS`（逗号分隔）读取，默认 `localhost:9092`。
- producer 标识（`EventEnvelope.producer`）从 `AIDP_SERVICE_NAME` 读取，
  默认 `"aidp-unknown"`。
- 消费侧的 SASL/TLS 在 Phase 1 暂未启用，Task 14 接入 KMS 后统一加。
