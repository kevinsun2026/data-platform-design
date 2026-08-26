# Phase 1 实施计划：平台基线 + 数据源管理（M1-M2）

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 8 周内交付可运行的 AIDP 平台基线 + datasource-service 完整闭环（4 类数据源），2 类数据源 + AI 增强在 M2 完成；为 Phase 2/3 奠定基础。

**Architecture:**
- Python 3.11 + FastAPI 后端 + Go 1.22 写 Gateway + BFF
- 微服务 + 事件驱动（Kafka）
- 多租户 L1 行级隔离，Day 1 起
- agent-gateway 集中代理多供应商 LLM
- Next.js 14 Web 控制台
- K8s + Helm 部署，OpenTelemetry 可观测

**Tech Stack:**
| 类别 | 选型 |
|---|---|
| 后端 | Python 3.11 + FastAPI 0.110 + Pydantic v2 + SQLAlchemy 2.0 |
| 高并发 | Go 1.22 + Hertz + go-redis |
| 前端 | Next.js 14 (App Router) + TypeScript + Tailwind + shadcn/ui + Zustand |
| 关系 DB | PostgreSQL 16 + pgvector 0.7 + Alembic |
| 缓存 / 限流 | Redis 7 |
| 事件总线 | Kafka 3.x (KRaft) + confluent-kafka-python |
| 监控 | OpenTelemetry SDK + Prometheus + Loki + Tempo + Grafana |
| 鉴权 | OIDC (Authlib) + JWT (PyJWT) + Argon2 (argon2-cffi) |
| AI | LangChain 0.2 + LangGraph 0.1 + openai 1.x SDK |
| 测试 | pytest 8 + httpx + testcontainers + Playwright |
| 部署 | K8s 1.29 + Helm 3 + ArgoCD + Skaffold |
| 工具 | uv (Python 包) + pnpm (前端) + Task (任务运行) + pre-commit |

## Global Constraints

- 所有 Python 服务必须加 mypy strict + ruff + black 三个检查（pre-commit 强制）
- 所有 Go 服务必须加 golangci-lint + go test
- 所有 PR 必须有：lint 通过 + 单测覆盖率 > 80% + 至少 1 个 reviewer
- 所有表必须有：`id` (uuid) + `tenant_id` (uuid) + `created_at` + `updated_at` + `created_by` + `updated_by` + `deleted_at` (nullable)
- 所有 API 响应必须包含 `trace_id` (从 OTel context 取)
- 所有错误响应必须用统一错误码格式：`{"code": "string", "message": "string", "trace_id": "string"}`
- 所有敏感字段（密码、token、API key）必须经 KMS 加密，DB 中只存密文 + iv + key_id
- 所有时间戳统一 UTC 存储，ISO 8601 返回
- 所有日志统一 JSON 格式，必须含 `tenant_id`、`user_id`（如有）、`service`、`trace_id`
- 所有事件发 Kafka 必须带 `event_id` (uuid) + `tenant_id` + `occurred_at` + `producer` (service name)
- 租户隔离 L1 强制：所有 DB query 通过 ORM 自动注入 `WHERE tenant_id = :current_tenant`，禁止手写裸 SQL
- 端口分配：gateway 8000 / iam 8001 / audit 8002 / notify 8003 / agent-gateway 8004 / datasource 8005 / web 3000
- 代码风格：Python 用 google docstring；Go 用 godoc；TS 用 TSDoc
- 提交信息：`feat:` / `fix:` / `chore:` / `refactor:` / `test:` / `docs:` 前缀

---

## 文件结构（Phase 1 落地后状态）

```
aidp/                                  # monorepo root
├── .github/workflows/                 # CI/CD
│   ├── ci-python.yml
│   ├── ci-go.yml
│   ├── ci-web.yml
│   └── release.yml
├── deploy/
│   ├── helm/                          # Helm charts
│   │   ├── aidp-common/              # 通用配置
│   │   ├── aidp-gateway/
│   │   ├── aidp-iam/
│   │   ├── aidp-audit/
│   │   ├── aidp-notify/
│   │   ├── aidp-agent-gateway/
│   │   ├── aidp-datasource/
│   │   └── aidp-web/
│   ├── argocd/                        # ArgoCD apps
│   ├── k8s/base/                      # 基础资源（namespace, secret, rbac）
│   └── monitoring/                    # Prom rules, Grafana dashboards
├── proto/                             # 共享 gRPC 定义
│   ├── common.proto
│   ├── datasource.proto
│   ├── agent.proto
│   └── gen/                          # 生成代码
├── libs/python/                       # 共享 Python 库
│   ├── aidp_common/                  # 基础：配置、日志、追踪、错误
│   ├── aidp_db/                      # DB session、租户注入、迁移
│   ├── aidp_auth/                    # JWT、租户上下文
│   ├── aidp_audit/                   # audit 客户端
│   ├── aidp_notify/                  # notify 客户端
│   ├── aidp_events/                  # Kafka 生产/消费
│   └── aidp_llm/                     # LLM 客户端
├── services/
│   ├── gateway/                       # Go: API Gateway
│   ├── iam/                           # Python: 租户/用户/角色
│   ├── audit/                         # Python: 审计
│   ├── notify/                        # Python: 通知
│   ├── agent-gateway/                 # Python + Go BFF: AI 代理
│   └── datasource/                    # Python: 数据源
├── web/                               # Next.js 控制台
│   ├── app/                          # App Router
│   ├── components/
│   ├── lib/                          # API client, hooks
│   ├── stores/                       # Zustand
│   └── styles/
├── cli/                               # Go: cz-cli
├── tests/
│   ├── integration/                  # 跨服务集成测试
│   ├── e2e/                          # Playwright e2e
│   └── load/                         # 性能测试
├── docs/
│   ├── superpowers/
│   │   ├── specs/                    # 设计文档
│   │   └── plans/                    # 实施计划
│   ├── api/                          # OpenAPI 文档
│   └── runbook/
├── platform-dict/                     # 共享枚举 codegen
├── scripts/                           # 工具脚本
├── Taskfile.yml                       # 任务定义
├── pyproject.toml                     # workspace 配置
├── .pre-commit-config.yaml
├── .gitignore
├── README.md
└── LICENSE
```

---

## 任务清单（Phase 1 共 19 个 task）

### Task 1: Monorepo 初始化 + 工具链

**Files:**
- Create: `pyproject.toml`
- Create: `Taskfile.yml`
- Create: `.pre-commit-config.yaml`
- Create: `.gitignore`
- Create: `.editorconfig`
- Create: `.github/CODEOWNERS`
- Create: `README.md`
- Create: `scripts/setup-dev.sh`

**Interfaces:**
- Consumes: 暂无
- Produces: 一个空 monorepo，能跑 `task lint` / `task test` 不报错

**Goal**: 建立 monorepo 基础设施，CI/CD 占位，开发者 1 脚本拉起环境

- [ ] **Step 1.1**: 初始化 git 仓库 + uv workspace

```bash
cd /Users/macbook/.mavis/workspace/data-platform-design
git init
uv init --no-readme --no-pin-python --bare
```

修改 `pyproject.toml` 为 workspace 模式（详见代码段）

- [ ] **Step 1.2**: 创建 Taskfile.yml

```yaml
version: '3'
vars:
  PYTHON: uv run python
tasks:
  lint:
    cmds:
      - uv run ruff check .
      - uv run mypy libs/ services/
      - cd web && pnpm lint
  test:
    cmds:
      - uv run pytest
      - cd web && pnpm test
  test.int:
    cmds:
      - uv run pytest tests/integration
  format:
    cmds:
      - uv run ruff format .
      - cd web && pnpm format
  setup:
    cmds:
      - bash scripts/setup-dev.sh
```

- [ ] **Step 1.3**: 写 .pre-commit-config.yaml（ruff + mypy + format + secrets scan）

- [ ] **Step 1.4**: 写 scripts/setup-dev.sh（一键安装依赖 + 启动 Postgres/Redis/Kafka via docker compose）

- [ ] **Step 1.5**: 写 README.md（项目说明 + 快速开始）

- [ ] **Step 1.6**: 测试 setup 脚本可执行

```bash
bash scripts/setup-dev.sh
task lint  # 应通过（空仓库）
```

- [ ] **Step 1.7**: Commit

```bash
git add .
git commit -m "chore: monorepo bootstrap with uv + task + pre-commit"
```

---

### Task 2: 共享 Python 库 - aidp_common

**Files:**
- Create: `libs/python/aidp_common/pyproject.toml`
- Create: `libs/python/aidp_common/src/aidp_common/__init__.py`
- Create: `libs/python/aidp_common/src/aidp_common/config.py`
- Create: `libs/python/aidp_common/src/aidp_common/logging.py`
- Create: `libs/python/aidp_common/src/aidp_common/tracing.py`
- Create: `libs/python/aidp_common/src/aidp_common/errors.py`
- Create: `libs/python/aidp_common/src/aidp_common/models.py`
- Test: `libs/python/aidp_common/tests/test_config.py`
- Test: `libs/python/aidp_common/tests/test_errors.py`

**Interfaces:**
- Consumes: 暂无
- Produces:
  ```python
  from aidp_common.config import Settings, get_settings
  from aidp_common.errors import AppError, ErrorCode, NotFoundError, ForbiddenError
  from aidp_common.models import TenantScoped, IdModel, TimestampMixin
  from aidp_common.logging import setup_logging, get_logger
  from aidp_common.tracing import setup_tracing, get_trace_id
  ```

**Goal**: 提供所有服务共用的配置、日志、追踪、错误、基础模型

- [ ] **Step 2.1**: 写测试 `test_config.py`

```python
def test_settings_loads_from_env(monkeypatch):
    monkeypatch.setenv("AIDP_DB_URL", "postgresql://test")
    monkeypatch.setenv("AIDP_REDIS_URL", "redis://test")
    monkeypatch.setenv("AIDP_SERVICE_NAME", "test-svc")
    from aidp_common.config import get_settings
    settings = get_settings()
    assert settings.db_url == "postgresql://test"
    assert settings.service_name == "test-svc"
```

- [ ] **Step 2.2**: 运行测试确认失败

```bash
cd libs/python/aidp_common && uv run pytest tests/test_config.py -v
# 预期: ModuleNotFoundError
```

- [ ] **Step 2.3**: 实现 `config.py` (pydantic-settings)

```python
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    db_url: str
    redis_url: str
    kafka_brokers: str = "localhost:9092"
    service_name: str
    log_level: str = "INFO"
    env: str = "dev"
    otlp_endpoint: str | None = None
    model_config = {"env_prefix": "AIDP_", "env_file": ".env"}

_settings: Settings | None = None
def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings
```

- [ ] **Step 2.4**: 实现 `errors.py` (统一错误码 + AppError)

```python
from enum import Enum
from typing import Any

class ErrorCode(str, Enum):
    NOT_FOUND = "NOT_FOUND"
    FORBIDDEN = "FORBIDDEN"
    UNAUTHORIZED = "UNAUTHORIZED"
    VALIDATION = "VALIDATION"
    CONFLICT = "CONFLICT"
    RATE_LIMITED = "RATE_LIMITED"
    UPSTREAM_ERROR = "UPSTREAM_ERROR"
    INTERNAL = "INTERNAL"

class AppError(Exception):
    def __init__(self, code: ErrorCode, message: str, status: int = 400, details: dict | None = None):
        self.code = code
        self.message = message
        self.status = status
        self.details = details or {}

class NotFoundError(AppError):
    def __init__(self, resource: str, id: Any):
        super().__init__(ErrorCode.NOT_FOUND, f"{resource} {id} not found", 404)

class ForbiddenError(AppError):
    def __init__(self, msg: str = "forbidden"):
        super().__init__(ErrorCode.FORBIDDEN, msg, 403)
```

- [ ] **Step 2.5**: 实现 `logging.py` (JSON structured logger)

- [ ] **Step 2.6**: 实现 `tracing.py` (OpenTelemetry setup)

- [ ] **Step 2.7**: 实现 `models.py` (基础 ORM 模型)

```python
from datetime import datetime, timezone
from sqlalchemy import DateTime
from sqlalchemy.orm import Mapped, mapped_column
import uuid

def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def gen_id() -> str:
    return str(uuid.uuid4())

class IdModel:
    id: Mapped[str] = mapped_column(primary_key=True, default=gen_id)

class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)
    created_by: Mapped[str | None] = mapped_column(nullable=True)
    updated_by: Mapped[str | None] = mapped_column(nullable=True)
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
```

- [ ] **Step 2.8**: 写测试 `test_errors.py` 验证错误抛出 + 序列化

- [ ] **Step 2.9**: 运行所有测试 + 确认通过

```bash
cd libs/python/aidp_common && uv run pytest -v --cov=aidp_common --cov-fail-under=80
```

- [ ] **Step 2.10**: Commit

```bash
git add libs/python/aidp_common
git commit -m "feat(common): shared config, errors, models, logging, tracing"
```

---

### Task 3: 共享 Python 库 - aidp_db（租户注入 + 迁移）

**Files:**
- Create: `libs/python/aidp_db/pyproject.toml`
- Create: `libs/python/aidp_db/src/aidp_db/__init__.py`
- Create: `libs/python/aidp_db/src/aidp_db/session.py`
- Create: `libs/python/aidp_db/src/aidp_db/tenant.py`
- Create: `libs/python/aidp_db/src/aidp_db/migration.py`
- Test: `libs/python/aidp_db/tests/test_session.py`
- Test: `libs/python/aidp_db/tests/test_tenant.py`

**Interfaces:**
- Consumes: `aidp_common.config.get_settings`
- Produces:
  ```python
  from aidp_db.session import get_engine, get_session, with_session
  from aidp_db.tenant import set_tenant_context, get_tenant_id, TenantSession
  from aidp_db.migration import run_migrations
  ```

**Goal**: SQLAlchemy session 管理 + 强制租户过滤 + Alembic 集成

- [ ] **Step 3.1**: 写测试 `test_session.py`（用 testcontainers 启 PG）

```python
import pytest
from testcontainers.postgres import PostgresContainer
from aidp_db.session import get_engine, get_session

@pytest.fixture(scope="module")
def pg():
    with PostgresContainer("postgres:16") as pg:
        yield pg

def test_session_creates_table(pg):
    engine = get_engine(pg.get_connection_url())
    with get_session(engine) as s:
        s.execute(text("CREATE TABLE test (id int)"))
    # 不抛错即通过
```

- [ ] **Step 3.2**: 实现 `session.py`（engine 工厂 + context manager）

- [ ] **Step 3.3**: 写测试 `test_tenant.py`（验证 WHERE tenant_id 自动注入）

```python
def test_tenant_filter_auto_injected(pg, setup_models):
    from aidp_db.tenant import set_tenant_context
    set_tenant_context("tenant-a")
    with get_session(...) as s:
        # 即使手写 SQL 不带 WHERE tenant_id，ORM 也会注入
        result = s.execute(select(MyModel)).scalars().all()
        # 实际执行的 SQL 应包含 WHERE tenant_id = 'tenant-a'
```

- [ ] **Step 3.4**: 实现 `tenant.py`（SQLAlchemy event listener 注入 WHERE）

```python
from sqlalchemy import event
from contextvars import ContextVar

_current_tenant: ContextVar[str | None] = ContextVar("current_tenant", default=None)

def set_tenant_context(tenant_id: str):
    _current_tenant.set(tenant_id)

def get_tenant_id() -> str | None:
    return _current_tenant.get()

@event.listens_for(Session, "do_orm_execute")
def _add_tenant_filter(state):
    if not state.is_select:
        return
    tid = get_tenant_id()
    if tid is None:
        return
    for desc in state.statement.column_descriptions:
        entity = desc["entity"]
        if hasattr(entity, "tenant_id"):
            state.statement = state.statement.where(entity.tenant_id == tid)
```

- [ ] **Step 3.5**: 实现 `migration.py`（alembic runner）

- [ ] **Step 3.6**: 测试通过

- [ ] **Step 3.7**: Commit

```bash
git commit -am "feat(db): session management with mandatory tenant_id filter"
```

---

### Task 4: 共享 Python 库 - aidp_events (Kafka)

**Files:**
- Create: `libs/python/aidp_events/pyproject.toml`
- Create: `libs/python/aidp_events/src/aidp_events/producer.py`
- Create: `libs/python/aidp_events/src/aidp_events/consumer.py`
- Test: `libs/python/aidp_events/tests/test_producer.py`

**Interfaces:**
- Produces:
  ```python
  from aidp_events.producer import publish_event, EventEnvelope
  from aidp_events.consumer import consume_events, Handler
  ```

**Goal**: 统一 Kafka 事件发布 / 消费

- [ ] **Step 4.1**: 写测试（用 testcontainers 启 Kafka，验证事件 round-trip）

- [ ] **Step 4.2**: 实现 `EventEnvelope` 模型（含 `event_id` / `tenant_id` / `occurred_at` / `producer` / `event_type` / `payload` / `trace_id`）

- [ ] **Step 4.3**: 实现 `publish_event`（带重试 + 死信）

- [ ] **Step 4.4**: 实现 `consume_events`（带 at-least-once + 业务幂等键）

- [ ] **Step 4.5**: Commit

---

### Task 5: 共享 Python 库 - aidp_auth (JWT + 租户上下文)

**Files:**
- Create: `libs/python/aidp_auth/pyproject.toml`
- Create: `libs/python/aidp_auth/src/aidp_auth/jwt.py`
- Create: `libs/python/aidp_auth/src/aidp_auth/dependencies.py`
- Test: `libs/python/aidp_auth/tests/test_jwt.py`

**Interfaces:**
- Produces:
  ```python
  from aidp_auth.jwt import create_access_token, decode_token
  from aidp_auth.dependencies import current_user, require_permission
  ```

**Goal**: JWT 签发/验证 + FastAPI 依赖注入

- [ ] **Step 5.1**: 测试：签发 + 解码 + 过期检测

- [ ] **Step 5.2**: 实现 `jwt.py`（HS256，access 12h，refresh 30d）

- [ ] **Step 5.3**: 实现 `dependencies.py`（FastAPI Depends 注入 CurrentUser）

- [ ] **Step 5.4**: Commit

---

### Task 6: API 网关 (Go) - 基础

**Files:**
- Create: `services/gateway/go.mod`
- Create: `services/gateway/main.go`
- Create: `services/gateway/internal/config/config.go`
- Create: `services/gateway/internal/middleware/auth.go`
- Create: `services/gateway/internal/middleware/trace.go`
- Create: `services/gateway/internal/middleware/ratelimit.go`
- Create: `services/gateway/internal/router/router.go`
- Test: `services/gateway/internal/middleware/auth_test.go`

**Interfaces:**
- Produces: HTTP 8000 端口
  - 路径前缀 `/api/v1/iam/*` → iam-service
  - `/api/v1/audit/*` → audit-service
  - `/api/v1/notify/*` → notify-service
  - `/api/v1/agent/*` → agent-gateway
  - `/api/v1/datasources/*` → datasource-service
  - `/api/v1/auth/login` → iam-service
  - `/healthz` / `/readyz` → 本服务

**Goal**: 网关能转发请求，注入 trace_id，鉴权头透传，限流

- [ ] **Step 6.1**: 初始化 Go 模块

```bash
cd services/gateway
go mod init github.com/aidp/gateway
go get github.com/cloudwego/hertz
go get github.com/redis/go-redis/v9
go get go.opentelemetry.io/otel
```

- [ ] **Step 6.2**: 写测试 `auth_test.go`（验证 token 提取 + 透传）

- [ ] **Step 6.3**: 实现配置加载（env）

- [ ] **Step 6.4**: 实现 trace 中间件（生成/透传 W3C trace context）

- [ ] **Step 6.5**: 实现 auth 中间件（提取 JWT 头，验证签名转发 user info）

- [ ] **Step 6.6**: 实现 ratelimit 中间件（基于 Redis token bucket，按 IP/租户）

- [ ] **Step 6.7**: 实现 router（按 path 前缀转发到下游）

- [ ] **Step 6.8**: 集成测试（启 1 个 mock 下游，验证转发）

- [ ] **Step 6.9**: Dockerfile + Helm chart

- [ ] **Step 6.10**: Commit

```bash
git commit -am "feat(gateway): auth + trace + ratelimit middleware + path routing"
```

---

### Task 7: IAM 服务 - 数据库 + 模型

**Files:**
- Create: `services/iam/pyproject.toml`
- Create: `services/iam/src/aidp_iam/__init__.py`
- Create: `services/iam/src/aidp_iam/main.py`
- Create: `services/iam/src/aidp_iam/models.py`
- Create: `services/iam/src/aidp_iam/alembic.ini`
- Create: `services/iam/alembic/env.py`
- Create: `services/iam/alembic/versions/0001_initial.py`
- Test: `services/iam/tests/test_models.py`

**Interfaces:**
- Produces: SQLAlchemy 模型

**Goal**: IAM 服务的 DB schema 落地

- [ ] **Step 7.1**: 写测试 `test_models.py`（创建 / 查询 / 软删）

- [ ] **Step 7.2**: 实现 `models.py`（Tenant / User / Group / Role / UserRoleBinding / ApiKey / Session / SsoConnection）

- [ ] **Step 7.3**: 写 Alembic 初始迁移 `0001_initial.py`

- [ ] **Step 7.4**: 测试迁移可执行 + 可回滚

```bash
cd services/iam
uv run alembic upgrade head
uv run alembic downgrade base
uv run alembic upgrade head
```

- [ ] **Step 7.5**: Commit

```bash
git commit -am "feat(iam): initial schema with tenant + user + role + api_key"
```

---

### Task 8: IAM 服务 - 认证 API

**Files:**
- Create: `services/iam/src/aidp_iam/api/auth.py`
- Create: `services/iam/src/aidp_iam/services/auth_service.py`
- Create: `services/iam/src/aidp_iam/schemas.py`
- Test: `services/iam/tests/test_auth_api.py`
- Test: `services/iam/tests/test_auth_service.py`

**Interfaces:**
- Produces: REST 8001
  - `POST /api/v1/auth/register-tenant` - 注册租户（Day 1 仅超管）
  - `POST /api/v1/auth/login` - 账号密码登录
  - `POST /api/v1/auth/refresh` - 刷新 token
  - `POST /api/v1/auth/logout` - 登出
  - `POST /api/v1/auth/sso/{provider}/callback` - SSO 回调
  - `GET /api/v1/auth/me` - 当前用户

**Goal**: 完整的认证流程跑通

- [ ] **Step 8.1**: 写测试 `test_auth_service.py`（密码 hash + verify + token 签发）

- [ ] **Step 8.2**: 实现 `auth_service.py`（Argon2 密码 + JWT 签发）

- [ ] **Step 8.3**: 写测试 `test_auth_api.py`（用 httpx AsyncClient + testcontainers PG）

```python
async def test_register_login_flow(pg_container):
    async with AsyncClient(app=app, base_url="http://test") as ac:
        r = await ac.post("/api/v1/auth/register-tenant", json={
            "tenant_name": "Acme",
            "admin_email": "admin@acme.com",
            "admin_password": "StrongP@ss123",
        })
        assert r.status_code == 200
        r = await ac.post("/api/v1/auth/login", json={
            "email": "admin@acme.com",
            "password": "StrongP@ss123",
        })
        assert r.status_code == 200
        assert "access_token" in r.json()
```

- [ ] **Step 8.4**: 实现 `api/auth.py`（FastAPI 路由）

- [ ] **Step 8.5**: 集成测试通过

- [ ] **Step 8.6**: Dockerfile + Helm chart

- [ ] **Step 8.7**: Commit

```bash
git commit -am "feat(iam): auth API (register/login/refresh/logout/me)"
```

---

### Task 9: IAM 服务 - 用户/角色管理 API

**Files:**
- Create: `services/iam/src/aidp_iam/api/users.py`
- Create: `services/iam/src/aidp_iam/api/roles.py`
- Create: `services/iam/src/aidp_iam/services/user_service.py`
- Create: `services/iam/src/aidp_iam/services/rbac.py`
- Test: `services/iam/tests/test_users_api.py`
- Test: `services/iam/tests/test_rbac.py`

**Interfaces:**
- Produces: REST 8001
  - `GET/POST /api/v1/users` (list, create)
  - `GET/PUT/DELETE /api/v1/users/{id}`
  - `POST /api/v1/users/{id}/reset-password`
  - `GET/POST /api/v1/roles` (list, create)
  - `POST /api/v1/users/{id}/roles` (bind)
  - `DELETE /api/v1/users/{id}/roles/{role_id}` (unbind)
  - `POST /api/v1/permissions/check` (内部权限校验)

**Goal**: 用户与角色 CRUD + RBAC 权限校验

- [ ] **Step 9.1**: 测试：创建用户 + 分配角色 + 权限校验

- [ ] **Step 9.2**: 实现 `user_service.py`

- [ ] **Step 9.3**: 实现 `rbac.py`（权限点枚举 + 检查函数）

- [ ] **Step 9.4**: 实现 API 路由

- [ ] **Step 9.5**: Commit

---

### Task 10: Audit 服务

**Files:**
- Create: `services/audit/pyproject.toml`
- Create: `services/audit/src/aidp_audit/main.py`
- Create: `services/audit/src/aidp_audit/models.py`
- Create: `services/audit/src/aidp_audit/consumer.py`
- Create: `services/audit/src/aidp_audit/api/query.py`
- Create: `services/audit/alembic/versions/0001_initial.py`
- Test: `services/audit/tests/test_consumer.py`
- Test: `services/audit/tests/test_query.py`

**Interfaces:**
- Produces:
  - Kafka 消费: `audit.*.*.*.v1`
  - REST 8002:
    - `GET /api/v1/audit/events?user_id=&action=&from=&to=&page=`
    - `GET /api/v1/audit/events/{id}` (返回解密 payload 给授权用户)
    - `GET /api/v1/audit/security-events`

**Goal**: 审计事件聚合 + 查询

- [ ] **Step 10.1**: 测试：发 100 条事件，验证落库 + 查询

- [ ] **Step 10.2**: 实现 Kafka 消费者（订阅 `audit.*` 模式，批量落库）

- [ ] **Step 10.3**: 实现查询 API（带权限过滤：只能查自己租户）

- [ ] **Step 10.4**: Commit

---

### Task 11: Notify 服务

**Files:**
- Create: `services/notify/pyproject.toml`
- Create: `services/notify/src/aidp_notify/main.py`
- Create: `services/notify/src/aidp_notify/channels/email.py`
- Create: `services/notify/src/aidp_notify/channels/feishu.py`
- Create: `services/notify/src/aidp_notify/channels/webhook.py`
- Create: `services/notify/src/aidp_notify/api/templates.py`
- Create: `services/notify/src/aidp_notify/api/send.py`
- Test: `services/notify/tests/test_send.py`

**Interfaces:**
- Produces:
  - REST 8003:
    - `POST /api/v1/notify/send` (内部接口)
    - `GET/POST /api/v1/notify/channels`
    - `GET/POST /api/v1/notify/templates`
    - `GET /api/v1/notify/logs`

**Goal**: 通知发送 + 模板管理

- [ ] **Step 11.1**: 测试：mock SMTP / 飞书 webhook，验证发送 + 重试

- [ ] **Step 11.2**: 实现 email 通道（用 aiosmtplib）

- [ ] **Step 11.3**: 实现飞书 / webhook 通道

- [ ] **Step 11.4**: 实现模板渲染（Handlebars 风格）

- [ ] **Step 11.5**: Commit

---

### Task 12: Agent Gateway - 基础代理

**Files:**
- Create: `services/agent-gateway/pyproject.toml`
- Create: `services/agent-gateway/src/aidp_agent/__init__.py`
- Create: `services/agent-gateway/src/aidp_agent/main.py`
- Create: `services/agent-gateway/src/aidp_agent/providers/base.py`
- Create: `services/agent-gateway/src/aidp_agent/providers/openai_compat.py`
- Create: `services/agent-gateway/src/aidp_agent/providers/registry.py`
- Create: `services/agent-gateway/src/aidp_agent/router.py`
- Create: `services/agent-gateway/src/aidp_agent/metering.py`
- Test: `services/agent-gateway/tests/test_router.py`
- Test: `services/agent-gateway/tests/test_metering.py`

**Interfaces:**
- Produces:
  - OpenAI-compat REST 8004:
    - `POST /v1/chat/completions` (兼容 OpenAI 协议)
    - `GET /v1/models` (列出可用模型)
    - `POST /api/v1/agent/credentials` (BYOK 管理)
  - Kafka 消费: 无（暂只做代理）
  - Kafka 生产: `agent.llm.called.v1`

**Goal**: 透明代理多供应商 LLM，含路由 + 计量

- [ ] **Step 12.1**: 测试：mock OpenAI / Anthropic / DeepSeek 三个 provider，验证路由 + failover

- [ ] **Step 12.2**: 实现 `providers/base.py`（统一接口）

```python
class LLMProvider(Protocol):
    async def chat(self, request: ChatRequest) -> AsyncIterator[ChatChunk]: ...
    async def count_tokens(self, text: str, model: str) -> int: ...
```

- [ ] **Step 12.3**: 实现 `openai_compat.py`（适配所有 OpenAI-compat 供应商）

- [ ] **Step 12.4**: 实现 `registry.py`（按 provider 注册 + 启停）

- [ ] **Step 12.5**: 实现 `router.py`（model_tier + task_type → provider 选择 + failover）

- [ ] **Step 12.6**: 实现 `metering.py`（token 计数 + 成本计算 + 异步上报 ClickHouse）

- [ ] **Step 12.7**: 集成测试（端到端：发请求 → mock provider 响应 → 验证 metering 落库）

- [ ] **Step 12.8**: Commit

---

### Task 13: Agent Gateway - MCP 集成（为 datasource Agent 准备）

**Files:**
- Create: `services/agent-gateway/src/aidp_agent/mcp/server.py`
- Create: `services/agent-gateway/src/aidp_agent/mcp/tools/datasource.py`
- Test: `services/agent-gateway/tests/test_mcp.py`

**Interfaces:**
- Produces: MCP 端点
  - SSE: `GET /mcp/sse`
  - HTTP: `POST /mcp/tools/call`
  - 工具: `datasource.list` / `datasource.get` / `datasource.test_connection`

**Goal**: 暴露 datasource 工具给外部 Agent

- [ ] **Step 13.1**: 测试：MCP client 调用工具

- [ ] **Step 13.2**: 实现 MCP server（用官方 mcp-python-sdk）

- [ ] **Step 13.3**: 实现 datasource tools（通过 gRPC 调 datasource-service）

- [ ] **Step 13.4**: Commit

---

### Task 14: Datasource 服务 - 基础 CRUD + 测试连接

**Files:**
- Create: `services/datasource/pyproject.toml`
- Create: `services/datasource/src/aidp_datasource/__init__.py`
- Create: `services/datasource/src/aidp_datasource/main.py`
- Create: `services/datasource/src/aidp_datasource/models.py`
- Create: `services/datasource/src/aidp_datasource/connectors/base.py`
- Create: `services/datasource/src/aidp_datasource/connectors/postgresql.py`
- Create: `services/datasource/src/aidp_datasource/connectors/mysql.py`
- Create: `services/datasource/src/aidp_datasource/connectors/oracle.py`
- Create: `services/datasource/src/aidp_datasource/connectors/hive.py`
- Create: `services/datasource/src/aidp_datasource/api/datasources.py`
- Create: `services/datasource/src/aidp_datasource/services/datasource_service.py`
- Create: `services/datasource/src/aidp_datasource/services/credential_service.py` (KMS 加密)
- Create: `services/datasource/alembic/versions/0001_initial.py`
- Test: `services/datasource/tests/test_credential_service.py`
- Test: `services/datasource/tests/test_datasource_service.py`
- Test: `services/datasource/tests/test_connectors.py`

**Interfaces:**
- Produces:
  - REST 8005:
    - `POST /api/v1/datasources` (create)
    - `GET /api/v1/datasources?env=&type=&tag=` (list)
    - `GET /api/v1/datasources/{id}` (detail)
    - `PUT /api/v1/datasources/{id}` (update)
    - `DELETE /api/v1/datasource/{id}` (soft delete)
    - `POST /api/v1/datasources/{id}/test` (test connection)
    - `GET /api/v1/datasources/types` (list supported connectors)
  - gRPC: `DataSourceService.GetConnection` (内部)
  - Kafka 生产: `datasource.registered.v1` / `datasource.test.succeeded.v1` / `datasource.test.failed.v1` / `datasource.disabled.v1`

**Goal**: 数据源 CRUD + 4 类连接器（PG/MySQL/Oracle/Hive）+ 凭据加密

- [ ] **Step 14.1**: 测试 credential 加密（用 testcontainers 启 Vault 或 mock KMS）

```python
def test_credential_encrypted_at_rest():
    svc = CredentialService(kms_client=mock_kms)
    plain = PlainCredentials(username="u", password="p")
    encrypted = svc.encrypt(plain)
    assert encrypted.ciphertext != "p"
    assert svc.decrypt(encrypted) == plain
```

- [ ] **Step 14.2**: 实现 `credential_service.py`（AES-256-GCM + 内存 key）

- [ ] **Step 14.3**: 写测试 `test_connectors.py`（testcontainers 起 4 个 DB，验证 connect + get_schema）

- [ ] **Step 14.4**: 实现 `connectors/base.py` 统一接口

```python
class Connector(Protocol):
    async def test(self) -> TestResult: ...
    async def get_schema(self, db: str | None) -> list[TableInfo]: ...
    async def preview(self, table: str, limit: int) -> list[dict]: ...
    async def close(self): ...
```

- [ ] **Step 14.5**: 实现 4 个连接器

- [ ] **Step 14.6**: 测试 datasource_service（创建 + 测试 + 列表 + 软删）

- [ ] **Step 14.7**: 实现 `datasource_service.py`

- [ ] **Step 14.8**: 实现 REST API

- [ ] **Step 14.9**: 实现 gRPC server（用 grpc.aio + 生成的 proto）

- [ ] **Step 14.10**: 写 Alembic 迁移

- [ ] **Step 14.11**: Dockerfile + Helm chart

- [ ] **Step 14.12**: 端到端测试（通过 gateway 调用）

- [ ] **Step 14.13**: Commit

```bash
git commit -am "feat(datasource): CRUD + 4 connectors + encrypted credentials"
```

---

### Task 15: Datasource 服务 - Schema 抓取 + 同步

**Files:**
- Create: `services/datasource/src/aidp_datasource/services/schema_service.py`
- Create: `services/datasource/src/aidp_datasource/connectors/{pg,mysql,oracle,hive}.py` (扩展 get_schema)
- Create: `services/datasource/src/aidp_datasource/api/schemas.py`
- Create: `services/datasource/src/aidp_datasource/jobs/sync_schema.py`
- Test: `services/datasource/tests/test_schema_service.py`

**Interfaces:**
- Produces (新增):
  - `POST /api/v1/datasources/{id}/sync-schema` (async, return job_id)
  - `GET /api/v1/datasources/{id}/schemas?database=`
  - `GET /api/v1/datasources/{id}/tables/{table}/preview?limit=100`
  - `GET /api/v1/datasources/{id}/tables/{table}/ddl`

**Goal**: 异步抓取 + 缓存 schema + 预览 + DDL

- [ ] **Step 15.1**: 测试：sync 100 张表，验证 fingerprint 增量检测

- [ ] **Step 15.2**: 实现 `schema_service.py`（异步任务、批处理、fingerprint 计算）

- [ ] **Step 15.3**: 扩展 4 个连接器的 `get_schema`（含列、PK、索引、行数估算）

- [ ] **Step 15.4**: 实现 `sync_schema` job（用 FastAPI BackgroundTasks → 后续接 Celery）

- [ ] **Step 15.5**: 实现 preview + DDL 端点

- [ ] **Step 15.6**: Commit

---

### Task 16: Datasource 服务 - 3 类连接器扩展 + PII 识别

**Files:**
- Create: `services/datasource/src/aidp_datasource/connectors/mongodb.py`
- Create: `services/datasource/src/aidp_datasource/connectors/doris.py`
- Create: `services/datasource/src/aidp_datasource/connectors/kafka.py`
- Create: `services/datasource/src/aidp_datasource/services/pii_service.py`
- Create: `services/datasource/src/aidp_datasource/connectors/base.py` (扩展)
- Test: `services/datasource/tests/test_pii_service.py`

**Interfaces:**
- Produces (新增):
  - `POST /api/v1/datasources/{id}/policies` (PII 规则)
  - `GET /api/v1/datasources/{id}/policies`
  - `POST /api/v1/datasources/{id}/suggest-pii` (AI 建议)

**Goal**: 7 类连接器 + AI PII 自动识别

- [ ] **Step 16.1**: 实现 MongoDB / Doris / Kafka 连接器

- [ ] **Step 16.2**: 测试 PII 服务（mock LLM 响应，验证建议列表）

- [ ] **Step 16.3**: 实现 `pii_service.py`（调 agent-gateway 的 classify PII 能力）

- [ ] **Step 16.4**: 实现 policies API

- [ ] **Step 16.5**: Commit

---

### Task 17: Web 控制台 - 基础框架 + 登录 + 数据源页

**Files:**
- Create: `web/package.json`
- Create: `web/next.config.js`
- Create: `web/tailwind.config.ts`
- Create: `web/tsconfig.json`
- Create: `web/app/layout.tsx`
- Create: `web/app/(auth)/login/page.tsx`
- Create: `web/app/(console)/layout.tsx`
- Create: `web/app/(console)/datasources/page.tsx`
- Create: `web/app/(console)/datasources/new/page.tsx`
- Create: `web/components/ui/*` (shadcn components)
- Create: `web/lib/api.ts` (API client)
- Create: `web/lib/auth.ts` (token 管理)
- Test: `web/tests/e2e/login.spec.ts` (Playwright)

**Interfaces:**
- Produces: Next.js 应用 3000 端口
  - `/login` - 登录页
  - `/datasources` - 数据源列表
  - `/datasources/new` - 新建数据源
  - `/datasources/{id}` - 详情

**Goal**: 用户能登录、看到数据源列表、创建数据源

- [ ] **Step 17.1**: 初始化 Next.js

```bash
cd web
pnpm create next-app@14 . --typescript --tailwind --app
pnpm add zustand @tanstack/react-query zod react-hook-form axios
pnpm dlx shadcn@latest init
pnpm dlx shadcn@latest add button input form card table dialog
```

- [ ] **Step 17.2**: 写 E2E 测试 `login.spec.ts`

```typescript
test('user can login and see datasources page', async ({ page }) => {
  await page.goto('/login')
  await page.fill('input[name=email]', 'admin@acme.com')
  await page.fill('input[name=password]', 'StrongP@ss123')
  await page.click('button[type=submit]')
  await expect(page).toHaveURL('/datasources')
})
```

- [ ] **Step 17.3**: 实现 `lib/api.ts`（axios + token interceptor + trace_id 注入）

- [ ] **Step 17.4**: 实现登录页 + auth store

- [ ] **Step 17.5**: 实现数据源列表页（React Query + 表格）

- [ ] **Step 17.6**: 实现新建数据源页（动态表单，按 type 加载 schema）

- [ ] **Step 17.7**: 运行 E2E 测试通过

```bash
pnpm test:e2e
```

- [ ] **Step 17.8**: Dockerfile + Helm chart

- [ ] **Step 17.9**: Commit

```bash
git commit -am "feat(web): login + datasources list + new datasource page"
```

---

### Task 18: Web 控制台 - 数据源详情 + AI 助手

**Files:**
- Create: `web/app/(console)/datasources/{id}/page.tsx`
- Create: `web/app/(console)/datasources/{id}/schemas/page.tsx`
- Create: `web/components/ai-assistant-panel.tsx`
- Test: `web/tests/e2e/datasource-detail.spec.ts`

**Interfaces:**
- Produces (新增):
  - `/datasources/{id}` - 详情（含 schema tab）
  - AI 助手侧栏（"帮我填表" + 失败诊断提示）

**Goal**: 完整的数据源管理 UX

- [ ] **Step 18.1**: 写 E2E 测试（创建 → 测试 → 看 schema → AI 助手显示 PII 建议）

- [ ] **Step 18.2**: 实现详情页

- [ ] **Step 18.3**: 实现 schema tab（含指纹变更检测显示）

- [ ] **Step 18.4**: 实现 AI 助手组件（流式 SSE 接收 LLM 响应）

- [ ] **Step 18.5**: Commit

---

### Task 19: 端到端集成测试 + 监控 + 生产化

**Files:**
- Create: `tests/integration/test_e2e_datasource.py`
- Create: `deploy/monitoring/prometheus-rules.yaml`
- Create: `deploy/monitoring/grafana-dashboards/*.json`
- Create: `deploy/k8s/base/ingress.yaml`
- Create: `docs/runbook/datasource.md`

**Goal**: 端到端跑通 + 监控告警 + 上线准备

- [ ] **Step 19.1**: 写 E2E 集成测试（完整链路：用户注册 → 登录 → 创建数据源 → 测试连接 → AI PII 识别 → 审计日志可查）

- [ ] **Step 19.2**: 写 Prometheus 告警规则（datasource-svc down / error rate > 1% / P95 > SLO / 凭据解密失败率 > 0）

- [ ] **Step 19.3**: 写 Grafana dashboard（4 个：平台总览 / 网关 / IAM / Datasource）

- [ ] **Step 19.4**: 写 ingress + TLS 配置

- [ ] **Step 19.5**: 写 runbook（常见故障处理）

- [ ] **Step 19.6**: 在 staging 环境跑通完整链路

- [ ] **Step 19.7**: 邀请 1 个真实业务方试用，收集反馈

- [ ] **Step 19.8**: Commit + 打 tag v0.1.0

```bash
git commit -am "chore(phase1): integration tests + monitoring + runbook"
git tag v0.1.0
```

---

## Phase 1 验收清单（M2 末）

- [ ] 7 个服务全部在 staging 跑通
- [ ] 端到端测试（test_e2e_datasource.py）通过
- [ ] 至少 3 个真实数据源接入（PG / MySQL / Oracle / Hive 任选）
- [ ] AI PII 识别在 1 个真实数据源上验证
- [ ] Grafana dashboard 4 个上线
- [ ] 1 个业务方用户成功注册 + 创建数据源 + 看到审计
- [ ] 所有 SLO 指标在 staging 可观测
- [ ] 等保 2.0 三级自评完成
- [ ] M3 Phase 2 启动会材料就绪

---

## 后续 Phase 计划占位（Phase 2/3/4 后续单独写 plan）

- **Phase 2 (M3-M4) plan**: studio + scheduler 完整设计 + 任务
- **Phase 3 (M5-M6) plan**: dqc + bi 完整设计 + 任务
- **Phase 4 (M7+) plan**: integration / sql-explorer / api-service / ops 详细任务

每个 Phase plan 独立维护，按本模板结构组织。
