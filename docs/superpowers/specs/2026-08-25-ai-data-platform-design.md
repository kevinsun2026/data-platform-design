# 企业级 AI 数据全流程开发平台 · 系统设计文档

> **代号**：AIDP（AI Data Platform）
> **日期**：2026-08-25
> **作者**：Mavis（基于与用户的 brainstorm 会话）
> **状态**：已批准 → 等待实施

---

## 0. 文档目的

本文档是 AIDP 平台的整体系统功能设计，明确：
- 平台定位与差异化价值
- 整体架构与服务拓扑
- 各模块（含 4 个主线深化模块）的详细设计
- 跨切关注点（多租户 / AI Gateway / 事件 / 观测）
- 共享数据模型
- 非功能需求与 SLO
- 6 个月分阶段实施路线

文档配套：
- **可视化设计稿**（浏览器查看）：`/Users/macbook/.mavis/workspace/data-platform-design/.superpowers/brainstorm/46729-1787661797/content/`
  - `architecture.html` — 整体架构图
  - `services.html` — 服务目录
  - `cross-cutting.html` — 横切关注点
  - `module-datasource.html` — 数据源详细设计
  - `module-studio.html` — Studio 详细设计
  - `module-dqc.html` — DQC 详细设计
  - `module-bi.html` — BI 详细设计
  - `data-model.html` — 跨模块数据模型
  - `nfr.html` — 非功能需求
  - `roadmap.html` — 实施路线图

---

## 1. 平台定位

### 1.1 一句话定义

**企业级 AI 驱动的数据全生命周期开发平台**：以「数据源 → 集成 → 开发调度 → 质量稽核 → 接口服务 → BI 展示 → 治理」为主干，以 **AI Agent Mesh** 为横向智能层。

### 1.2 差异化

| 维度 | 传统数据平台 | AIDP |
|---|---|---|
| 多源支持 | 通常分套（Informatica / DataX / 自研 ETL 各一套） | 统一一个平台，覆盖 10+ 类数据源 |
| AI 集成 | 后期外挂 Co-pilot | 全流程 AI：开发、运维、查询、决策 |
| 治理 | 流程为主 | 数据驱动 + AI 辅助决策 |
| 上手成本 | 需要数据工程师 | 业务方经简单培训也能自助取数 |

### 1.3 核心约束

- **多源数据库**：关系型（Oracle / MySQL / PostgreSQL / SQL Server / 达梦）+ 大数据（Hive / Doris / StarRocks / ClickHouse）+ NoSQL（MongoDB / ES）+ 消息流（Kafka）+ Lakehouse（Hudi / Iceberg / Paimon）+ 文件 / SaaS
- **企业级**：多租户 / RBAC+ABAC / 审计 / 等保 / 灾备
- **AI 全流程**：从开发到运维，从查询到决策
- **6 个月立起完整框架**，后续迭代深化

---

## 2. 整体架构

### 2.1 架构风格

- **架构模式**：微服务 + 事件驱动（Kafka 为主，部分 NATS 轻量通知）
- **API 协议**：gRPC（服务间）+ REST（前端 / 外部 API）+ MCP（Agent ↔ 工具）
- **数据落地**：PostgreSQL（元数据 / 调度 / 配置 / 租户）+ MinIO / S3（脚本、制品、附件）+ pgvector / Qdrant（Agent 知识库）
- **AI 接入**：独立 AI Gateway，OpenAI-compat 协议，**多供应商可插拔**
- **部署**：Kubernetes（多 namespace 隔离 dev/staging/prod），Helm Chart

### 2.2 服务拓扑

```
┌─────────────────────────────────────────────────────────────────────┐
│                       用户接入层                                     │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────────────┐   │
│  │ Web Console  │    │  CLI (cz)    │    │  API 开发者门户      │   │
│  │  Next.js 14  │    │  Go          │    │  OpenAPI/Swagger     │   │
│  └──────┬───────┘    └──────┬───────┘    └──────────┬───────────┘   │
└─────────┼─────────────────────┼─────────────────────┼──────────────┘
          └─────────────────────┴─────────────────────┘
                                │
              ┌─────────────────▼─────────────────┐
              │      API Gateway (Go)             │
              │  路由 / 限流 / 鉴权 / 灰度 / 审计  │
              └─────────────────┬─────────────────┘
                                │
   ┌────────────────────────────┴────────────────────────────┐
   │            业务服务层 (按 bounded context)              │
   │  ┌──────────┐ ┌──────────┐ ┌──────────┐               │
   │  │datasource│ │studio /  │ │integration│              │
   │  │  svc     │ │scheduler │ │  svc     │               │
   │  │ (Py)     │ │ (Py)     │ │(Py+Go wrk)│              │
   │  └──────────┘ └──────────┘ └──────────┘               │
   │  ┌──────────┐ ┌──────────┐ ┌──────────┐               │
   │  │   dqc    │ │bi-       │ │  api     │               │
   │  │  svc     │ │ query    │ │ service  │               │
   │  │ (Py)     │ │ (Py+Go)  │ │  (Go)    │               │
   │  └──────────┘ └──────────┘ └──────────┘               │
   │  ┌──────────┐ ┌──────────┐ ┌──────────┐               │
   │  │  ops /   │ │metadata  │ │  sql     │               │
   │  │ diagnose │ │  /lineage│ │ explorer │               │
   │  │  (Py)    │ │  (Py)    │ │  (Py)    │               │
   │  └──────────┘ └──────────┘ └──────────┘               │
   │  ┌──────────┐ ┌──────────┐ ┌──────────┐               │
   │  │analytics │ │   iam    │ │  audit   │               │
   │  │  agent   │ │  -svc    │ │  -svc    │               │
   │  │  (Py)    │ │  (Py)    │ │  (Py)    │               │
   │  └──────────┘ └──────────┘ └──────────┘               │
   │  ┌──────────┐                                           │
   │  │ notify   │                                           │
   │  │  -svc    │                                           │
   │  │  (Py)    │                                           │
   │  └──────────┘                                           │
   └────────────────────────┬────────────────────────────────┘
                            │
   ┌────────────────────────┴────────────────────────────────┐
   │              智能能力层 (横切)                            │
   │  ┌─────────────────────────────────────────────────┐   │
   │  │   agent-gateway (Py/Go)                          │   │
   │  │  模型路由 / 密钥 / 限流 / token 计量 / 审计     │   │
   │  └────────────┬─────────────┬───────────────┘   │
   │               │             │                    │
   │   ┌───────────▼──┐  ┌───────▼──────────┐        │
   │   │ OpenAI-compat│  │ Agent Orchestrator│       │
   │   │ 多供应商代理 │  │  (LangGraph)      │        │
   │   └─────────────┘  │  - SQL Agent      │        │
   │                    │  - ETL Agent      │        │
   │                    │  - DQC Agent      │        │
   │                    │  - Insight Agent  │        │
   │                    │  - Ops Agent      │        │
   │                    └───────────────────┘        │
   │                    ┌───────────────────┐        │
   │                    │ knowledge-base    │        │
   │                    │ (pgvector RAG)    │        │
   │                    └───────────────────┘        │
   └────────────────────────┬────────────────────────────────┘
                            │
   ┌────────────────────────┴────────────────────────────────┐
   │           基础设施层 (Infra Plane)                        │
   │  PostgreSQL · Redis · Kafka · MinIO · pgvector · NATS  │
   │  K8s + Helm + OpenTelemetry + Prometheus + Loki        │
   └────────────────────────┬────────────────────────────────┘
                            │
   ┌────────────────────────┴────────────────────────────────┐
   │         数据源连接器层 (Connector Mesh)                   │
   │  Oracle · MySQL · PG · Hive · Doris · MongoDB · Kafka  │
   │  S3 / OSS / HDFS · SaaS APIs ...                        │
   └─────────────────────────────────────────────────────────┘
```

### 2.3 端到端数据流（一条 SQL 探查任务的生命周期）

```
用户 (Web/CLI)
  │ 1. 自然语言提问："看下昨天订单 GMV"
  ▼
sql-explorer
  │ 2. 调 agent-gateway
  ▼
agent-gateway
  │ 3. 路由到 LLM（OpenAI/Claude/DeepSeek 可插拔）
  │ 4. LLM 通过 MCP 调 sql-explorer.list_tables() / datasource.list()
  │ 5. 拿到 schema 后生成 SQL
  ▼
sql-explorer.execute(sql)
  │ 6. 通过 datasource-svc 拿连接器
  │ 7. 限流 + 审计 + dry-run
  │ 8. 投递到 query-engine
  ▼
query-engine (Go 沙箱执行)
  │ 9. 限制超时 / 结果集大小
  │ 10. 返回结果
  ▼
agent-gateway (再次调 LLM 总结)
  │ 11. 文本化结果 + 可选自动出图
  ▼
用户 (Web 看到表格 + 总结 + 建议 SQL)
```

---

## 3. 技术栈选型

### 3.1 选型矩阵

| 类别 | 选型 | 理由 |
|---|---|---|
| 后端主控 | Python 3.11 + FastAPI + Pydantic v2 | AI 生态最完整，迭代快 |
| 后端高并发 | Go 1.22 + Hertz / Kratos | 性能好，支撑同步 worker / API 网关 / BI 查询 |
| 前端 | Next.js 14 (App Router) + Tailwind + shadcn/ui | SSR/CSR 混用，生态丰富 |
| 图表库 | ECharts + Vega | 满足复杂 BI 可视化 |
| AI 框架 | LangChain + LangGraph + MCP | 主流，Agent 编排完备 |
| 调度 | 自研 + Prefect 借鉴 | 企业级可观测、权限需自研 |
| 工作流引擎 | Temporal（可选）| 跨服务 Saga 编排 |
| 关系 DB | PostgreSQL 16 | 主存储，含 pgvector |
| 缓存 / 限流 | Redis 7 Cluster | 通用 |
| 事件总线 | Kafka 3.x | 主流事件驱动 |
| 轻量消息 | NATS | 内部服务通知 |
| 对象存储 | MinIO / S3 / OSS | 兼容多云 |
| 向量库 | pgvector（轻）/ Qdrant（重）| Day 1 用 pgvector |
| OLAP | Doris / ClickHouse / StarRocks | 按租户选型 |
| 监控 | OpenTelemetry + Prometheus + Loki + Tempo + Grafana | 一站式 |
| 部署 | K8s + Helm + ArgoCD | GitOps |
| IaC | Terraform | 多云管理 |

### 3.2 关键架构动作（缓解 Python 短板）

| 风险点 | 缓解策略 |
|---|---|
| 高并发 API | FastAPI + Uvicorn 多 worker；关键路径用 Go 写 BFF |
| 调度稳定性 | 调度引擎独立进程（自研 + Prefect 借鉴），与 API 解耦 |
| 企业级权限 | Casbin + OPA 走策略即代码；Go 中间件通过 gRPC 集成 |
| 类型安全 | mypy strict + Pydantic v2 + 全链路 schema 校验 |

---

## 4. 服务目录

### 4.1 网关层

| 服务 | 技术 | 职责 |
|---|---|---|
| **gateway-service** | Go | 统一入口、限流、OIDC/JWT、灰度、审计、trace 透传 |

### 4.2 业务服务层（13 个）

| 服务 | 技术 | Bounded Context | 本期深化 |
|---|---|---|:---:|
| **datasource-service** | Py | 数据源管理 | ⭐ |
| **studio** | Py | 任务开发（编辑器） | ⭐ |
| **scheduler** | Py | 调度引擎 | ⭐（合并到 studio） |
| **integration-service** | Py + Go worker | 数据集成与同步 | |
| **dqc-service** | Py | 数据质量稽核 | ⭐ |
| **bi-service** | Py + Go query-engine | BI 展示 | ⭐ |
| **api-service** | Go | 数据接口生成 | |
| **ops-service** | Py | 任务运维与诊断 | |
| **metadata-service** | Py | 元数据与血缘 | |
| **sql-explorer** | Py | SQL 探查与查询 | |
| **analytics-agent** | Py | 分析域与对话 BI | |
| **iam-service** | Py | 租户 / 用户 / 角色 / 权限 | |
| **audit-service** | Py | 审计日志聚合 | |
| **notify-service** | Py | 通知 / 告警 / 订阅 | |

### 4.3 智能能力层（3 个）

| 服务 | 技术 | 职责 |
|---|---|---|
| **agent-gateway** | Py + Go BFF | OpenAI-compat 多供应商代理 / 限流 / 计量 / 审计 / 路由 |
| **agent-orchestrator** | Py (LangGraph) | 托管所有垂直 Agent（SQL / ETL / DQC / Insight / Ops） |
| **knowledge-base** | Py + pgvector | RAG 检索 / 语义模型 / 模板库 |

### 4.4 CLI 工具

| 工具 | 技术 | 说明 |
|---|---|---|
| **cz-cli** | Go | Lakehouse 命令行工具，"一条命令 = 一个完整业务动作" |

---

## 5. 横切关注点

### 5.1 多租户模型

- **共享集群、共享 schema、租户列隔离**（Day 1 默认 L1）
- 隔离强度三档：
  - L1 行级（默认）
  - L2 命名空间（schema 隔离，大客户）
  - L3 物理隔离（独立 DB 实例，金融/政府）
- 租户列贯穿所有业务表：`tenant_id` + `created_by` + `created_at` + `updated_at` + `updated_by`
- 请求上下文：Gateway 鉴权后写入 gRPC metadata / HTTP header，OTel baggage 透传
- 配额：每租户独立 QPS、并发任务、存储、token 用量；Gateway + 本地 token bucket 双重限流
- BYOK 密钥：租户可自携 LLM API key，KMS AES-256-GCM 加密存储

### 5.2 AI Gateway 内部架构

10 步内部流程：
1. 鉴权 + 租户上下文提取
2. 配额检查（Redis）· 超额 429
3. 模型路由策略匹配（model_tier + task_type + tenant_setting）
4. Prompt 模板注入
5. RAG 检索（knowledge-base）
6. Token 计量埋点
7. 调用上游（OpenAI / Claude / DeepSeek / Ollama / vLLM）
8. 流式响应转发（SSE）
9. 结果审计 + 敏感信息脱敏
10. Token 计量上报 ClickHouse

关键能力：
- **Failover**：主供应商 5xx/超时 → 降级次选；连续 3 次失败 → 熔断 5 分钟
- **本地模型**：通过 OpenAI-compat 协议接入 Ollama / vLLM，支持金融/政企隔离场景
- **密钥管理**：BYOK 优先 + 系统密钥兜底 + KMS 加密

### 5.3 事件契约

- Topic 命名：`<domain>.<aggregate>.<event-type>.<version>`，如 `studio.task.run.completed.v1`
- Schema Registry 强制（Apicurio/Confluent），兼容模式：BACKWARD / FORWARD / FULL
- 消费者模式：
  - 业务事件：at-least-once + 业务侧幂等键
  - 审计事件：fire-and-forget + DLQ
  - 状态同步：consume → upsert by (tenant_id, resource_id, version)
- DLQ 策略：每 topic 配 DLQ；30 分钟内重投 worker；超过 4 次进人工审核

### 5.4 可观测性

- **Trace**：OpenTelemetry + W3C trace context；OTLP → Tempo/Jaeger
- **Metrics**：RED 指标 + 业务指标；Prometheus → Grafana
- **Logs**：结构化 JSON，带 `trace_id` / `tenant_id` / `user_id` / `service` / `env`；Loki
- **AI 专项**：Agent 每步思考 / 工具调用 / token 消耗可视化（langfuse 风格）
- **告警**：AlertManager + 分级（P0 短信+电话 / P1 飞书+企微 / P2 邮件）
- **SLO**：API 99.9% / P95 < 500ms / 任务准时率 99.5% / 同步延迟 < 5min (P95) / BI 查询 P95 < 10s

### 5.5 错误处理 & 一致性

- 错误分类：4xx 业务错误（不重试）/ 5xx 系统错误（重试+告警）/ 429 限流
- 跨服务写操作走 Saga（Temporal workflow）；每步带 `idempotency_key`；业务表加 `UNIQUE(tenant_id, idempotency_key)`
- DLQ 落 + 重投 + 人工审核

---

## 6. 4 个主线深化模块

### 6.1 数据源管理 (datasource-service)

**职责**：数据源 CRUD + 测试连接 + Schema 抓取 + PII 识别 + 凭据加密

**核心数据模型**：
- `datasources` — 主表（含加密凭据）
- `datasource_schemas` — Schema 缓存（fingerprint 检测变更）
- `datasource_policies` — 访问策略（PII / 行过滤 / 限速）
- `connection_tests` — 连接测试历史
- `datasource_audit` — 凭据访问审计

**状态机**：Draft → Testing → Active → Disabled

**关键 API**（13 个）：CRUD + test + sync-schema + preview + DDL + GetConnection (gRPC)

**AI 集成**（6 个场景）：
- 自然语言 → 连接参数
- AI 诊断连接失败
- 自动 PII 识别
- 列语义自动注释
- 采样数据异常检测
- 凭据安全检查

**UI**：3 个核心页面（列表 / 新建 + AI 助手 / 详情 Tab）

**安全关键**：凭据只在 datasource-svc 进程内解密；其他服务调 GetConnection 拿代理 token，**不发密码原文**

### 6.2 Studio 任务开发与调度 (studio + scheduler)

**职责**：任务定义 + 版本 + DAG + 调度 + 实例 + 补数/重跑 + 血缘

**核心数据模型**（6 张表）：
- `tasks` — 主表
- `task_versions` — 不可变快照（dev/stg/prod 各有 active version）
- `task_instances` — 每次运行一条（run_date + business_date）
- `task_dependencies` — DAG 边
- `task_logs` — ClickHouse / Loki
- `task_metrics` — 运行时指标

**状态机**：Pending → Queued → Running → Success / Failed / Killed / Skipped / UpstreamFailed（带 retry）

**调度架构**：scheduler 多副本选举 + worker pool 分类型（python / sql / spark / flink）

**关键 API**（15 个）：CRUD + deploy + run + refill + cancel / rerun / skip + lineage + profile

**AI 集成**（8 个场景）：NL→任务、SQL 补全、自动依赖推断、代码评审、失败诊断、补数建议、参数推荐、Profile 解读

**UI**：3 个核心页面（工作台 / 任务编辑器+AI / DAG 视图 SVG）

**测试**：属性测试（DAG 循环检测）+ 调度集成 + 故障注入

### 6.3 DQC 数据质量稽核 (dqc-service)

**职责**：5 类规则 + 7 维质量评估 + 告警 + 健康度 + 改进建议

**7 维质量模型**：
1. 完整性 (Completeness) — 非空率
2. 准确性 (Accuracy) — 格式合规
3. 一致性 (Consistency) — 跨表/跨系统
4. 唯一性 (Uniqueness) — 主键
5. 及时性 (Timeliness) — 数据新鲜度
6. 有效性 (Validity) — 值域
7. **合规性 (Compliance)** — GDPR / 等保 / 行业法规 / 内部规范

**5 大规则类型**：行级 / 列级 / 跨表 / 趋势 / SLA

**核心数据模型**（6 张表）：rules / executions / alerts / health_scores / subscriptions / sla

**关键 API**（12 个）：CRUD + run + health 查询 + alert 认领/解决 + AI suggest

**AI 集成**（8 个场景）：
- 表画像 → 规则推荐
- 失败根因分析
- 自然语言定义规则
- 阈值自适应（双 11 允许突增 50%）
- 改进建议生成（一键生成 studio 草稿）
- 跨规则异常关联
- 告警收敛
- 对账 SQL 自动生成

**UI**：3 个核心页面（6 维仪表盘 / 规则编辑器 / 告警详情+AI 根因）

**关键测试**：
- 单元 + 集成
- **回放测试**（生产 SQL 流量在测试环境回放，验证规则不会误报；新增规则必先回放 7 天）

**告警聚合**：5 分钟内同 rule 合并；>100 条/min 触发收敛降级

### 6.4 BI 展示 (bi-service)

**职责**：看板 CRUD + 图表 + 语义层 + 查询 + 下钻 + 订阅 + 嵌入 + **对话式 BI**

**核心数据模型**（8 张表）：dashboards / versions / charts / datasets / semantic_models / subscriptions / shares / views

**查询执行架构**：Go BFF → query-engine 沙箱（超时控制 + 结果截断）→ Doris/ClickHouse/StarRocks

**关键 API**（14 个）：CRUD + publish + rollback + query + ask + subscribe + share + embed + export + ai-layout

**AI 集成**（8 个场景，最大亮点）：
- **Ask BI**（NL → 图表，4 步：意图解析 → 语义模型匹配 → SQL 生成 → 渲染）
- **AI 自动搭看板**（"CEO 周会看板" → 6-8 张图）
- **数据故事**（自动文字解读："本周 GMV 同比↑12%，主要由家电品类贡献..."）
- 异常高亮（z-score / Isolation Forest + LLM 解释）
- 智能下钻（"为什么华南区下降" → AI 自动选下钻维度）
- NL 过滤器（"上个月除北京外的华东大区"）
- 推荐合适图表
- 自动写说明文档

**UI**：3 个核心页面（消费看板 / 图表编辑器 / Ask BI 对话）

**测试**：单元 + 视觉回归（Playwright）+ 500+ 问题 AI 基准

---

## 7. 跨模块数据模型

详见可视化页 `data-model.html`。核心表族：

| 领域 | 表 | 说明 |
|---|---|---|
| IAM | tenants / users / groups / roles / user_role_bindings / abac_policies / sso_connections / api_keys / sessions | 独立 iam-service |
| Audit | audit_events / audit_payloads / security_events | 异步 Kafka 聚合 |
| Notify | notification_channels / templates / logs / user_notification_prefs | 多通道多语言 |
| AI Gateway | llm_providers / llm_credentials / llm_call_logs / llm_call_daily_agg / prompt_templates / model_aliases | 含 token 计量 |
| 共享枚举 | datasource_type / task_type / task_instance_status / dqc_rule_dimension / dqc_severity / chart_type / env / tenant_isolation_level | platform-dict 仓库 codegen |

**设计原则**：
- 每服务独立 PG schema，跨服务不直连表
- 共享数据走服务（iam / audit / notify 独立）
- 所有业务表带 5 元组（tenant_id + created_at + updated_at + created_by + updated_by）
- 软删优先（`deleted_at`），物理删走异步任务
- JSONB 用于非结构化配置，Pydantic / Zod 双向校验

---

## 8. 非功能需求

### 8.1 SLO 承诺

| SLO | 目标 |
|---|---|
| API 可用性 | 99.9%（每月 ≤ 43min 不可用）|
| API 响应 P95 | < 500ms（读） / < 2s（写）|
| 任务调度准时率 | 99.5% |
| 同步延迟 P95 | < 5min |
| BI 查询 P95 | < 10s（含缓存）|
| Gateway 吞吐 | 10K QPS / 实例 |

### 8.2 安全

- **认证**：OIDC / SAML / LDAP / 自建四路；密码 Argon2id；MFA TOTP 强制（管理员 / 生产）
- **授权**：RBAC 角色 + ABAC 属性策略（OPA）；权限点命名 `<service>.<resource>.<action>`；行级权限
- **凭据**：KMS AES-256-GCM；API Key 仅展示前 8 位；密钥轮转支持双写期 24h
- **审计**：所有写操作 + 敏感读进 audit_events；payload 加密；保留 1 年
- **网络**：全 TLS 1.3；mTLS 服务间；WAF 防 SQL 注入/XSS

### 8.3 性能

- 容量参考：1000 并发租户 · 50K 任务/天 · 1 亿 LLM token/天
- 关键路径预算：API Gateway P95 < 20ms；数据源列表 P95 < 300ms；同步 100W 行 < 5min
- 缓存：Redis 多级（5min / 按 TTL / 1h / 10min / 30min）

### 8.4 可用性

- 多副本 + 故障转移（K8s HPA + DB 主从 + Redis Sentinel/Cluster + Kafka 3 副本）
- 降级策略（每个服务都有 fallback 路径）
- 季度混沌工程
- 变更管控（PR 审批 + DB migration stg 灰度 + Feature flag）

### 8.5 合规

- PII 字段自动识别 + 脱敏（k-匿名 / 掩码 / 假名化）
- GDPR Right to be Forgotten
- 数据驻留（境内/境外分集群）
- 等保 2.0 三级 + SOC2 友好
- 依赖 SBOM + License 扫描

### 8.6 灾备

- PG 每日全量 + 6h 增量（WAL 流）
- MinIO 跨区复制 + Kafka MirrorMaker
- RPO ≤ 15min / RTO ≤ 1h
- 季度恢复演练

### 8.7 部署

- dev / stg / prod 三环境，每环境独立 namespace + DB
- CI/CD：lint + 单测 + 集成 + 镜像 + 安全扫描 → 自动 stg → 审批 + 灰度 prod
- IaC：Terraform + Helm + ArgoCD
- 可观测性：OTel + Prom + Loki + Tempo + Grafana

---

## 9. 实施路线图

### 9.1 6 个月分 4 阶段

| 阶段 | 时段 | 重点 |
|---|---|---|
| **Phase 1** | M1-M2 | 平台基线 + 数据源（4-5 人） |
| **Phase 2** | M3-M4 | Studio + Scheduler |
| **Phase 3** | M5-M6 | DQC + BI |
| **Phase 4** | M7+ | 集成 / SQL 探查 / API / Ops 等 |

### 9.2 关键里程碑

- **M2 末**：3 个真实数据源接入跑通；端到端 `数据源 → 测试 → Schema 同步 → 审计` 1 小时打通
- **M4 末**：50 个真实业务任务迁移；日均 1000+ 实例；AI 助手使用率 > 30%
- **M6 末**：20+ 看板上线；100+ DQC 规则；Ask BI 准确率 > 70%

### 9.3 团队（建议 8-10 人）

Tech Lead × 1 + 后端 × 4-5（Python 为主 + 1-2 Go）+ 前端 × 2 + AI × 1 + SRE × 1 + 产品 × 1

### 9.4 关键风险

| 风险 | 缓解 |
|---|---|
| AI 准确率不达预期 | "显示推理 + 一键撤销" + 关闭 AI 开关 + 基准常态化 |
| 多数据源适配工作量大 | M1 只做 4 类，优先覆盖 80% 用量 |
| 调度引擎稳定性 | 故障注入测试 + 先 1 租户试运行 2 周再扩大 |
| DQC 误报泛滥 | 回放测试强制 + 告警量硬上限 + 自动收敛 |
| LLM 成本失控 | 租户级配额 + 实时熔断 + 经济模型兜底 + 计量看板 |
| 企业级合规要求 | M1 末完成等保 2.0 三级自评；SSO/LDAP/审计 Day 1 可用 |

---

## 10. 附录

### 10.1 文档位置

- **设计文档**：`docs/superpowers/specs/2026-08-25-ai-data-platform-design.md`
- **可视化设计稿**：`/Users/macbook/.mavis/workspace/data-platform-design/.superpowers/brainstorm/46729-1787661797/content/`
- **项目根**：`/Users/macbook/.mavis/workspace/data-platform-design/`

### 10.2 后续文档

待编写（在 writing-plans 阶段产出）：
- 实施计划（按模块拆 task）
- 详细 API 规范（OpenAPI / Proto）
- 数据模型 ER 图（按服务）
- 部署 runbook
- SRE 手册
