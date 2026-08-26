# services/

Deployable backend services. Each service owns its own port, its own
`pyproject.toml` (Python) or `go.mod` (Go), and its own Dockerfile / Helm chart.

| Service        | Stack    | Port | Owner team      |
|----------------|----------|------|-----------------|
| gateway        | Go 1.22  | 8000 | gateway-team    |
| iam            | Python   | 8001 | iam-team        |
| audit          | Python   | 8002 | security-team   |
| notify         | Python   | 8003 | platform-team   |
| agent-gateway  | Py + Go  | 8004 | ai-team         |
| datasource     | Python   | 8005 | datasource-team |

Services are scaffolded by Tasks 6–14 of the Phase 1 plan.
