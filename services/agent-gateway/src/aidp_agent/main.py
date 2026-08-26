"""FastAPI application factory for the Agent Gateway.

This module is the entry point for
``uvicorn aidp_agent.main:app``. It:

- Builds the FastAPI app and registers the standard ``aidp_common``
  middleware (structured logging, trace context, error envelope).
- Owns the process-wide :class:`ProviderRegistry`, :class:`Router`,
  and :class:`MeteringDispatcher` via a tiny ``AppState`` object so
  the API layer can reach them through FastAPI's ``Depends``.
- Spawns the metering worker task on startup and stops it on
  shutdown.
- Exposes ``/healthz`` (liveness) and ``/readyz`` (readiness)
  endpoints.

The HTTP routes for chat completions, model listing, and BYOK
credentials live in :mod:`aidp_agent.api.endpoints`.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from aidp_common.config import get_settings
from aidp_common.errors import UpstreamError
from aidp_common.logging import get_logger, setup_logging
from aidp_common.tracing import setup_tracing
from fastapi import FastAPI, status

from aidp_agent.metering import (
    MeteringDispatcher,
    build_sink_from_env,
)
from aidp_agent.providers.base import LLMProvider
from aidp_agent.providers.registry import (
    ProviderRegistry,
    build_default_registry,
)
from aidp_agent.router import Router

_LOG = get_logger(__name__)


# ---------------------------------------------------------------------------
# AppState
# ---------------------------------------------------------------------------


class AppState:
    """Process-wide singletons owned by the FastAPI app.

    The state is stored on ``app.state.agent_gateway`` and also
    exposed via the :func:`get_registry` / :func:`get_router` /
    :func:`get_metering` module-level helpers (which read from
    ``app.state``). Tests can replace the state by overriding the
    helpers via :func:`fastapi.testclient.TestClient` + a dependency
    override.
    """

    def __init__(
        self,
        registry: ProviderRegistry,
        router: Router,
        metering: MeteringDispatcher,
    ) -> None:
        self.registry = registry
        self.router = router
        self.metering = metering


# ---------------------------------------------------------------------------
# Module-level accessors
# ---------------------------------------------------------------------------


_STATE: AppState | None = None


def set_state(state: AppState | None) -> None:
    """Set the process-wide :class:`AppState` (intended for tests)."""
    global _STATE
    _STATE = state


def get_registry() -> ProviderRegistry:
    """Return the process-wide :class:`ProviderRegistry`.

    Raises:
        RuntimeError: When called outside a running app (the API
            layer only ever consults this through a FastAPI
            dependency).
    """
    if _STATE is None:
        raise RuntimeError("agent-gateway AppState is not initialised")
    return _STATE.registry


def get_router() -> Router:
    """Return the process-wide :class:`Router`."""
    if _STATE is None:
        raise RuntimeError("agent-gateway AppState is not initialised")
    return _STATE.router


def get_metering() -> MeteringDispatcher:
    """Return the process-wide :class:`MeteringDispatcher`."""
    if _STATE is None:
        raise RuntimeError("agent-gateway AppState is not initialised")
    return _STATE.metering


# ---------------------------------------------------------------------------
# Build helpers (overridable by tests)
# ---------------------------------------------------------------------------


def build_state(
    *,
    registry: ProviderRegistry | None = None,
) -> AppState:
    """Build a fresh :class:`AppState`.

    Args:
        registry: Pre-built :class:`ProviderRegistry` to use. When
            ``None``, the default catalogue is loaded.
    """
    reg = registry if registry is not None else build_default_registry()
    rt = Router(reg)
    sink = build_sink_from_env()
    metering = MeteringDispatcher(sink=sink)
    return AppState(registry=reg, router=rt, metering=metering)


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    """Bring the agent-gateway up at boot and tear it down on shutdown.

    On entry:

    1. Configure structured JSON logging.
    2. Configure OpenTelemetry tracing (no-op when
       ``AIDP_OTLP_ENDPOINT`` is unset, which is the case in tests).
    3. Start the :class:`ProviderRegistry` (warm-up probes if any).
    4. Start the :class:`MeteringDispatcher` worker.

    On exit:

    5. Stop the metering worker, draining the queue first.
    6. Stop the :class:`ProviderRegistry` (close any owned HTTP
       clients).
    """
    settings = get_settings()
    setup_logging(level=settings.log_level, service_name=settings.service_name, env=settings.env)
    setup_tracing(service_name=settings.service_name, env=settings.env)

    if _STATE is None:
        # Build a default state when ``create_app`` did not pre-set
        # one. ``create_app`` always sets it, but the lifespan can
        # be driven directly by tests.
        set_state(build_state())

    _LOG.info("agent-gateway service starting", extra={"service": settings.service_name})
    # ``state`` is narrowed to ``AppState`` here: either
    # ``create_app`` already set it, or the previous block set it
    # via ``build_state``. The ``assert`` keeps mypy honest and
    # the runtime guard explicit.
    state: AppState = _STATE  # type: ignore[assignment]
    assert state is not None, "AppState must be initialised by create_app or build_state"
    try:
        await state.registry.start()
        await state.metering.start()
    except Exception as exc:  # pragma: no cover - defensive guard
        _LOG.exception("error during startup")
        raise UpstreamError("agent-gateway startup failed", details={"error": str(exc)}) from exc

    try:
        yield
    finally:
        if _STATE is not None:
            await _STATE.metering.stop()
            await _STATE.registry.stop()
        _LOG.info("agent-gateway service stopped")


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------


def create_app(
    *,
    state: AppState | None = None,
) -> FastAPI:
    """Construct the FastAPI app for the Agent Gateway.

    Args:
        state: Optional pre-built :class:`AppState`. When ``None``
            the default catalogue is loaded. Tests pass a custom
            state to inject a mocked :class:`ProviderRegistry`.
    """
    if state is not None:
        set_state(state)

    settings = get_settings()
    app = FastAPI(
        title="AIDP Agent Gateway",
        version="0.1.0",
        description=(
            "Transparent multi-provider LLM proxy (OpenAI / Anthropic / DeepSeek / ...) "
            "with tier-aware routing, failover, circuit breaking, and token-level metering."
        ),
        lifespan=lifespan,
    )

    @app.get("/healthz", status_code=status.HTTP_200_OK)
    async def healthz() -> dict[str, str]:
        """Liveness probe — confirms the process is up."""
        return {"status": "ok"}

    @app.get("/readyz", status_code=status.HTTP_200_OK)
    async def readyz() -> dict[str, Any]:
        """Readiness probe — confirms the registry is initialised.

        The agent-gateway has no external database dependency in
        Phase 1 (the metering layer's Postgres fallback is lazy
        and only activates when ``AIDP_AGENT_CLICKHOUSE_URL`` is
        unset and an actual usage event arrives). The probe
        therefore just confirms the in-process state is up.
        """
        state_local = _STATE
        if state_local is None:
            raise UpstreamError("agent-gateway state not initialised")
        return {
            "status": "ready",
            "providers": [p.config.name for p in state_local.registry.all()],
        }

    # Mount the gateway API.
    from aidp_agent.api.endpoints import router as gateway_router
    from aidp_agent.api.errors import install_app_error_handler

    install_app_error_handler(app)
    app.include_router(gateway_router)

    # Mount the MCP surface. The MCP integration is opt-out by
    # design: a deployment that does not want the MCP surface can
    # set ``AIDP_AGENT_MCP_ENABLED=0`` to skip the mount. The
    # default (the surface is enabled) matches the brief.
    import os

    if os.environ.get("AIDP_AGENT_MCP_ENABLED", "1") == "1":
        _mount_mcp(app)

    _LOG.info(
        "agent-gateway app created",
        extra={"service": settings.service_name, "port": 8004},
    )
    return app


def _mount_mcp(app: FastAPI) -> None:
    """Mount the MCP SSE sub-app and the ``/mcp/tools/call`` HTTP route.

    The function lives outside :func:`create_app` so a test that
    needs a *plain* FastAPI app (no MCP) can call :func:`create_app`
    with the env var unset and the MCP machinery is never
    imported — useful for tests that exercise other endpoints
    and want the cleanest possible surface.

    The SDK's default DNS-rebinding protection is *disabled* when
    ``AIDP_AGENT_MCP_RELAX_TRANSPORT=1`` is set. The flag exists
    so the integration-test client (and the FastAPI test
    client) can hit ``/mcp/sse`` without crafting ``Host``
    headers; production deployments leave the flag unset and
    keep the SDK's default localhost-only allow-list.
    """
    import os

    from aidp_agent.mcp.server import (
        build_mcp_router,
        build_sse_starlette_app,
        get_mcp_server,
    )

    # The HTTP shortcut endpoint. Mounted under ``/mcp`` so the
    # full path is ``POST /mcp/tools/call`` per the brief.
    app.include_router(build_mcp_router())

    # The SSE transport. We mount the SDK's Starlette app at
    # ``/mcp`` so the SDK's ``sse_path=/sse`` becomes the
    # canonical ``/mcp/sse`` (and the SDK's message endpoint
    # becomes ``/mcp/messages/``).
    transport_security: Any | None = None
    if os.environ.get("AIDP_AGENT_MCP_RELAX_TRANSPORT", "0") == "1":
        from mcp.server.transport_security import TransportSecuritySettings

        transport_security = TransportSecuritySettings(
            enable_dns_rebinding_protection=False,
        )
    sse_app = build_sse_starlette_app(
        sse_path="/sse",
        message_path="/messages/",
        transport_security=transport_security,
    )
    app.mount("/mcp", sse_app)

    _LOG.info(
        "agent-gateway MCP surface mounted",
        extra={"mcp_paths": ["/mcp/sse", "/mcp/messages/", "/mcp/tools/call", "/mcp/tools"]},
    )

    # ``get_mcp_server`` is referenced so the SDK's tool manager
    # has a chance to log warnings at import time (e.g. duplicate
    # tool names). A future task that introduces persistent
    # lifecycle hooks on the MCP server can plug in here.
    _ = get_mcp_server()


# Module-level instance so ``uvicorn aidp_agent.main:app`` works
# without an extra factory import.
app: FastAPI = create_app()


__all__ = [
    "AppState",
    "app",
    "build_state",
    "create_app",
    "get_metering",
    "get_registry",
    "get_router",
    "lifespan",
    "set_state",
]


# Configure ``logging`` defaults on import so a bare
# ``python -m aidp_agent.main`` (which does not go through
# ``create_app``) still emits structured logs.
if not logging.getLogger().handlers:  # pragma: no cover - import-time fallback
    setup_logging(service_name="aidp-agent")


# ``LLMProvider`` is exported so callers (e.g. tests) can type-hint
# against it without importing from the deeper providers package.
_ = LLMProvider
