# aidp_db

AIDP 平台共享数据库访问层。提供所有 Python 服务共用的：

- **session** — SQLAlchemy 2.0 同步 engine 工厂 + `with_session` / `get_session` context manager
- **tenant** — 基于 `ContextVar` 的租户上下文 + `do_orm_execute` 事件监听器，**自动**为 `TenantScoped`
  表的所有 SELECT 注入 `WHERE tenant_id = :current_tenant`（平台 L1 强制约束的核心实现）
- **migration** — Alembic runner：自动从 `AIDP_DB_URL` 读取目标 URL，提供 `run_migrations()` 入口

## 用法

```python
from aidp_db.session import get_engine, get_session, with_session
from aidp_db.tenant import set_tenant_context, get_tenant_id, TenantSession
from aidp_db.migration import run_migrations

# 1. 业务请求入口（auth 之后）设置租户
set_tenant_context("tenant-uuid-...")

# 2. 任何 ORM 查询都会自动加上 tenant_id 过滤
with with_session() as s:
    users = s.execute(select(User)).scalars().all()
    # 即使代码里没写 WHERE tenant_id = …，SQL 实际执行时会带上

# 3. 启动时跑迁移
run_migrations("services/iam/alembic")
```

## 关键设计

1. **强制租户过滤是 SQLAlchemy 事件级别实现的**——绕过 ORM 的裸 SQL 不会触发注入；
   这是预期的（plan global constraint：禁止手写裸 SQL），事件层注入只覆盖 ORM 查询。
2. **`ContextVar`** 在异步 FastAPI 中天然隔离请求；同步代码里也安全（线程池每个任务独立）。
3. **测试支持**：`aidp_db` 配 `testcontainers`；沙箱里拉不到镜像时退化为 SQLite in-memory，
   并在源码处用 `# pragma: allow-testcontainers-fallback` 标注。
