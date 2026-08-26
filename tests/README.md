# tests/

Cross-service test suite. Layout:

- `unit/`        — fast in-process unit tests, run by `task test`
- `integration/` — multi-service / DB / Kafka tests via testcontainers, run by `task test.int`
- `e2e/`         — Playwright browser tests against the web console
- `load/`        — k6 / vegeta performance scenarios

The bootstrap test in this directory is here only so `task test` exits 0 on
the empty monorepo. Real tests land alongside each service in subsequent
Phase 1 tasks.
