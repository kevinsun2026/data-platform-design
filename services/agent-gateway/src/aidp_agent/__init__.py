"""AIDP Agent Gateway — multi-provider LLM proxy with tier-aware routing.

The package is the Phase 1 / Task 12 deliverable: a transparent proxy in
front of any OpenAI-compatible LLM (OpenAI, Anthropic via the compat
shim, DeepSeek, local vLLM, ...). It provides:

- :mod:`aidp_agent.providers.base` — the ``LLMProvider`` Protocol all
  provider implementations conform to.
- :mod:`aidp_agent.providers.openai_compat` — a single class that talks
  the OpenAI /v1/chat/completions wire protocol, reused for every
  OpenAI-compatible provider via configuration.
- :mod:`aidp_agent.providers.registry` — a process-wide registry of
  configured provider instances with start/stop/health.
- :mod:`aidp_agent.router` — ``(model_tier, task_type) -> provider``
  resolution with failover to the cheapest healthy peer in the same
  tier, and a 3-strike circuit breaker per provider (5 minute cool-off).
- :mod:`aidp_agent.metering` — token counting, USD-cost calculation, and
  asynchronous sink of every call's usage to ClickHouse (or a Postgres
  fallback when ClickHouse is not configured).
- :mod:`aidp_agent.main` — the FastAPI app factory that wires the above
  into a 3-endpoint service on port 8004.
"""

from __future__ import annotations

__all__: list[str] = []
