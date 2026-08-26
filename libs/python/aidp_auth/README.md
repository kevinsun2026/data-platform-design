# aidp_auth

AIDP 平台共享鉴权库。基于 `pyjwt` 实现 HS256 access / refresh token 的
签发与验证；通过 FastAPI `Depends` 注入 `CurrentUser`，并把租户
`tenant_id` 自动绑定到 `aidp_db.tenant` 的 `ContextVar`，让后续 DB
查询自动获得 L1 租户隔离。

## 核心概念

- **`create_access_token`** — 签发 access token（HS256，默认 12h
  过期，租户、用户、角色、scope 全部编码进 claims）。
- **`create_refresh_token`** — 签发 refresh token（HS256，默认 30d）。
- **`decode_token`** — 校验签名 + 过期 + claims 形状，返回
  `TokenClaims` Pydantic 模型；签名错误 / 过期 / 形状不符会抛
  `aidp_common.errors.UnauthorizedError`。
- **`current_user`** — FastAPI Depends：从 `Authorization: Bearer ...`
  header 解码 token，返回 `CurrentUser(tenant_id, user_id, roles, scopes)`，
  并在请求作用域内调用 `aidp_db.tenant.set_tenant_context(...)`。
- **`require_permission(permission)`** — 返回一个 Depends 闭包，
  校验当前用户拥有该 `scope`（或 `*` 通配、或 `admin` 角色）。

## 用法

```python
from fastapi import FastAPI, Depends
from aidp_auth.jwt import create_access_token
from aidp_auth.dependencies import current_user, require_permission

app = FastAPI()


@app.post("/login")
def login() -> dict[str, str]:
    token = create_access_token(
        tenant_id="tenant-uuid",
        user_id="user-uuid",
        roles=["data_engineer"],
        scopes=["datasource:read", "datasource:write"],
    )
    return {"access_token": token, "token_type": "bearer"}


@app.get("/me")
def whoami(user=Depends(current_user)):
    return user.model_dump()


@app.post("/datasources", dependencies=[Depends(require_permission("datasource:write"))])
def create_datasource(): ...
```

## 关键设计

1. **HS256 + KMS 注入** — `AIDP_JWT_SECRET` 由部署平台注入；默认值仅供
   本地开发，运行时未设置会显式失败。
2. **统一错误格式** — 401 / 403 错误都走 `aidp_common.errors.AppError`
   → `to_dict()` 的 `{"code","message","details","trace_id"}` 线协议。
3. **租户 ContextVar 自动绑定** — `current_user` 在返回前调用
   `aidp_db.tenant.set_tenant_context(tenant_id)`，handler 内的 ORM
   查询会自动注入 `WHERE tenant_id = :tid`，无需手工传。
4. **scope / role 校验** — `require_permission(p)` 满足任一即放行：
   `p in scopes` / `"*" in scopes` / `"admin" in roles`。
5. **MyPy strict** — `CurrentUser` / `TokenClaims` 都是 `frozen=True`
   Pydantic 模型，下游可放心传引用。

## 凭据 / 部署

- `AIDP_JWT_SECRET` — HS256 签名密钥（部署平台通过 KMS 注入）。
- `AIDP_JWT_ALGORITHM` — 默认 `HS256`，可改但需协调所有签发 / 验证点。
- `AIDP_JWT_ACCESS_TOKEN_EXPIRES_MINUTES` — 默认 720（12h）。
- `AIDP_JWT_REFRESH_TOKEN_EXPIRES_DAYS` — 默认 30。
