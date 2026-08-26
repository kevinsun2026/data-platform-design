# aidp_common

AIDP 平台共享基础库。提供所有 Python 服务共用的：

- **config** — Pydantic Settings 驱动的环境变量配置（`AIDP_*` 前缀）
- **logging** — JSON 结构化日志（`python-json-logger`）
- **tracing** — OpenTelemetry 初始化 + `trace_id` 提取
- **errors** — 统一错误码（`ErrorCode`） + `AppError` 异常族
- **models** — SQLAlchemy 2.0 风格的 ORM 基础（`IdModel` / `TimestampMixin` / `TenantScoped`）

## 用法

```python
from aidp_common.config import get_settings
from aidp_common.errors import NotFoundError, ForbiddenError
from aidp_common.logging import setup_logging, get_logger
from aidp_common.tracing import setup_tracing, get_trace_id
from aidp_common.models import TimestampMixin, IdModel, TenantScoped
```

## 凭据管理

敏感字段（密码、token、API key）的加解密接口留待 Task 14 接入 KMS。本期实现仅提供
`cryptography.fernet` 包装的占位工具，**不要**在生产环境直接使用。
