# services/gateway

AIDP API gateway. Single binary that listens on `:8000`,
applies the platform's cross-cutting middleware, and forwards
matched `/api/v1/<prefix>/*` paths to the corresponding
downstream service.

## Stack

| Component | Choice |
|-----------|--------|
| Language  | Go 1.27 (matches `go.mod`) |
| HTTP      | [CloudWeGo Hertz](https://github.com/cloudwego/hertz) |
| Rate limit | Redis token bucket (go-redis/v9) |
| Tracing   | OpenTelemetry W3C Trace Context |
| Container | distroless `static-debian12:nonroot` |

## Routes

| Path prefix            | Downstream            | Default port |
|------------------------|-----------------------|--------------|
| `/api/v1/iam/*`        | iam-service           | 8001         |
| `/api/v1/audit/*`      | audit-service         | 8002         |
| `/api/v1/notify/*`     | notify-service        | 8003         |
| `/api/v1/agent/*`      | agent-gateway         | 8004         |
| `/api/v1/datasources/*`| datasource-service    | 8005         |
| `/healthz`             | gateway (liveness)    | 8000         |
| `/readyz`              | gateway (readiness)   | 8000         |

`/api/v1/auth/login` is served by `iam-service` and is on the
public allowlist (no bearer required) so callers can obtain
a token before any other endpoint.

## Middleware order

```
trace  →  auth  →  ratelimit  →  router
```

1. **trace** (outer): ensures every request has a W3C span,
   propagates `traceparent`, and exposes the resolved
   `X-Trace-Id` header.
2. **auth**: extracts the bearer token, base64-decodes the
   JWT payload (no signature verification — that's
   iam-service's job), and attaches an `Identity` to the
   request context.
3. **ratelimit**: per-`tenant_id:user_id` (or per-IP for
   anonymous) Redis token bucket.
4. **router**: strips the `/api/v1/<prefix>/` portion and
   forwards to the upstream.

## Error envelope

Errors come back as:

```json
{
  "code": "UNAUTHORIZED",
  "message": "missing Authorization header",
  "trace_id": "0af7651916cd43dd8448eb211c80319c"
}
```

`trace_id` is the W3C 32-char trace id; cross-reference it
with the OTel collector / Tempo to find the corresponding
span and log lines.

## Configuration

All configuration is environment-driven (`AIDP_*`):

| Var                          | Default          | Notes |
|------------------------------|------------------|-------|
| `AIDP_LISTEN_ADDR`           | `:8000`          | host:port |
| `AIDP_SERVICE_NAME`          | `gateway`        | OTel resource attribute |
| `AIDP_ENV`                   | `dev`            | deployment env label |
| `AIDP_REDIS_ADDR`            | `localhost:6379` | rate-limit backend |
| `AIDP_RATE_LIMIT_RPS`        | `50`             | tokens added / second |
| `AIDP_RATE_LIMIT_BURST`      | `100`            | max bucket size |
| `AIDP_IAM_URL`               | `http://localhost:8001` | iam upstream base |
| `AIDP_AUDIT_URL`             | `http://localhost:8002` | audit upstream base |
| `AIDP_NOTIFY_URL`            | `http://localhost:8003` | notify upstream base |
| `AIDP_AGENT_URL`             | `http://localhost:8004` | agent upstream base |
| `AIDP_DATASOURCE_URL`        | `http://localhost:8005` | datasource upstream base |

## Develop

```bash
go test ./...
go test -cover ./...
golangci-lint run ./...
gofmt -l .
```

## Build & run

```bash
go build -o bin/gateway .
./bin/gateway                     # honours AIDP_* env

# Container
docker build -t ghcr.io/aidp/gateway:dev .
docker run --rm -p 8000:8000 ghcr.io/aidp/gateway:dev
```

## Deploy

```bash
helm install gateway ./deploy/gateway \
  --namespace aidp --create-namespace \
  --set image.tag=0.1.0 \
  --set downstreams.iamURL=http://iam.aidp.svc.cluster.local:8001
```
