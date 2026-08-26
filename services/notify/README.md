# aidp-notify

AIDP 通知服务（Task 11）。

- **职责**：多通道通知（email / feishu / webhook / sms）+ Handlebars 风格模板
  渲染 + 发送日志。
- **端口**：8003（与 plan 端口表一致）。
- **存储**：Postgres（生产）/ SQLite（沙箱 fallback）。Schema 由 Alembic 管理。

## 表结构

- `notification_channels` — 租户注册的通道（email 的 SMTP 配置 / feishu 的
  webhook URL / webhook 的目标 URL + 签名密钥 / sms 的供应商配置）。
- `notification_templates` — 每租户 + 每逻辑名 + 每 locale 一行模板。`locale`
  为 `"default"` 时是兜底变体。
- `notification_logs` — 每次发送尝试一行（`status` ∈ `queued` / `sent` /
  `failed`），用于"用户到底收到没？"的运维溯源。

详见 [`alembic/versions/0001_initial.py`](alembic/versions/0001_initial.py)。

## 模板渲染

模板 body 支持 `{{var}}` 风格替换，点路径用 `.` 走查；缺失变量渲染为空串。

```text
Subject: Welcome, {{user.name}}!
Body:    Your account {{user.email}} is ready.
```

locale 选择是级联式的：`zh-CN` → `zh` → `default`；每一级都查 `code` +
`locale`；全部失败时返回 404。

## 通道

| 通道 | 传输 | Mock 方式 |
| --- | --- | --- |
| `email` | SMTP via `aiosmtplib` | `unittest.mock.patch("aiosmtplib.send")` |
| `feishu` | HTTP POST via `httpx` | `httpx.AsyncClient` 注入 mock transport |
| `webhook` | HTTP POST via `httpx` | 同上 |
| `sms` | （stub） | 直接 `raise ChannelSendError` |

## 重试

`ChannelTransientError`（timeout / 5xx / 网络错误）触发重试；默认
`max_retries=3`，线性 backoff（0.2s）。`ChannelSendError`（4xx / 永久失败）
不重试。

## 启动

```bash
cd services/notify
uv run uvicorn aidp_notify.main:app --host 0.0.0.0 --port 8003
```

## 测试

```bash
cd services/notify
uv run pytest          # 单元 + 集成（默认走 SQLite fallback）
uv run pytest --cov=aidp_notify --cov-report=term-missing
```
