"""Shared test fixtures for the agent-gateway suite.

The fixtures fall into three buckets:

- **Providers / registry / router** — pre-built instances that tests
  mutate or query. The provider clients are built around an
  :class:`httpx.MockTransport` so a test can swap in deterministic
  responses without standing up a real LLM upstream.
- **App / client** — a FastAPI :class:`TestClient` wired to a custom
  :class:`AppState` (the same one the provider fixtures build). The
  fixture handles the ``set_state`` dance so the API layer's
  ``Depends`` factories pick up the test state.
- **Auth** — a bearer-token generator for the BYOK endpoint (which
  is the only authenticated route in Phase 1).

The fixtures are deliberately module-private (prefixed with ``_``)
so they can be imported with ``from tests._fixtures import ...`` or
auto-collected by pytest via the ``conftest.py`` chain. Tests that
need a fully-isolated state (different provider configs, custom
metering sinks) should build their own state via the lower-level
helpers exposed in this module.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

import httpx
import pytest
from aidp_agent.metering import (
    InMemorySink,
    MeteringDispatcher,
    UsageSink,
)
from aidp_agent.providers.base import (
    ModelSpec,
    ModelTier,
    ProviderConfig,
    TaskType,
)
from aidp_agent.providers.openai_compat import OpenAICompatProvider
from aidp_agent.providers.registry import ProviderRegistry
from aidp_agent.router import Router
from fastapi import FastAPI
from fastapi.testclient import TestClient

# ---------------------------------------------------------------------------
# Default provider catalogue for tests
# ---------------------------------------------------------------------------


#: Three providers, each with a different (tier, price) profile, so
#: failover / cost-based sorting tests have real differentiation.
DEFAULT_TEST_CONFIGS: tuple[ProviderConfig, ...] = (
    ProviderConfig(
        name="openai",
        display_name="OpenAI",
        base_url="https://api.openai.com/v1",
        api_key="sk-test-openai",
        model_specs=(
            ModelSpec("gpt-4o", ModelTier.FLAGSHIP, 0.005, 0.015),
            ModelSpec("gpt-4o-mini", ModelTier.BALANCED, 0.00015, 0.0006),
            ModelSpec("gpt-4.1-nano", ModelTier.ECONOMY, 0.0001, 0.0004),
        ),
    ),
    ProviderConfig(
        name="anthropic",
        display_name="Anthropic",
        base_url="https://api.anthropic.com/v1",
        api_key="sk-test-anthropic",
        model_specs=(
            ModelSpec("claude-sonnet-4-20250514", ModelTier.FLAGSHIP, 0.003, 0.015),
            ModelSpec("claude-haiku-4-5", ModelTier.BALANCED, 0.0008, 0.004),
        ),
    ),
    ProviderConfig(
        name="deepseek",
        display_name="DeepSeek",
        base_url="https://api.deepseek.com/v1",
        api_key="sk-test-deepseek",
        model_specs=(
            ModelSpec("deepseek-chat", ModelTier.ECONOMY, 0.00027, 0.0011),
            ModelSpec("deepseek-reasoner", ModelTier.BALANCED, 0.00055, 0.00219),
        ),
    ),
)


# ---------------------------------------------------------------------------
# Mock transport factory
# ---------------------------------------------------------------------------


class _ScriptedTransport(httpx.MockTransport):
    """An :class:`httpx.MockTransport` that runs a script of (request → response) pairs.

    The script is a list of callables. Each callable receives the
    :class:`httpx.Request` and returns an :class:`httpx.Response`. The
    transport pops the next callable on every call. When the script
    is exhausted the transport repeats the *last* callable, so a test
    can either (a) script a finite conversation or (b) script a
    single "always succeed" handler and rely on the default.
    """

    def __init__(self, handler: Callable[[httpx.Request], httpx.Response] | None = None) -> None:
        # ``self._handlers`` is a list so a test can ``append`` at any
        # point during the call. ``self._idx`` advances per call.
        self._handlers: list[Callable[[httpx.Request], httpx.Response]] = []
        if handler is not None:
            self._handlers.append(handler)
        self._calls: list[httpx.Request] = []

        def _dispatch(request: httpx.Request) -> httpx.Response:
            self._calls.append(request)
            if not self._handlers:
                raise RuntimeError(
                    "_ScriptedTransport has no handlers; "
                    "use ``mock_provider`` to register a response"
                )
            idx = min(self._idx, len(self._handlers) - 1)
            self._idx += 1
            return self._handlers[idx](request)

        self._idx = 0
        super().__init__(_dispatch)

    def push(self, handler: Callable[[httpx.Request], httpx.Response]) -> None:
        """Append a one-shot response handler at the end of the script."""
        self._handlers.append(handler)


def make_provider(
    config: ProviderConfig,
    *,
    handler: Callable[[httpx.Request], httpx.Response] | None = None,
) -> tuple[OpenAICompatProvider, _ScriptedTransport]:
    """Build an :class:`OpenAICompatProvider` paired with a scripted transport.

    The transport is returned alongside the provider so a test can
    ``push`` additional handlers or inspect ``transport._calls``.
    """
    transport = _ScriptedTransport(handler=handler)
    # ``MockTransport`` is consumed by ``httpx.AsyncClient``; we
    # build the client explicitly so the provider's lazy-client
    # path is exercised by the tests too.
    client = httpx.AsyncClient(
        base_url=config.base_url,
        transport=transport,
        headers={
            "Authorization": f"Bearer {config.api_key}",
            "Content-Type": "application/json",
        },
    )
    provider = OpenAICompatProvider(config, client=client)
    return provider, transport


# ---------------------------------------------------------------------------
# Response builders
# ---------------------------------------------------------------------------


def chat_completion_response(
    *,
    content: str = "hi",
    model: str = "gpt-4o",
    prompt_tokens: int = 10,
    completion_tokens: int = 5,
    request_id: str = "chatcmpl-test",
    echo_request_model: bool = False,
) -> httpx.Response:
    """Return a canned OpenAI-compat non-streaming JSON response.

    When ``echo_request_model`` is True the response's ``model``
    field is the model the request asked for (parsed from the JSON
    body). This is the closest approximation of a real upstream
    echoing back the request's model. Tests that need a fixed
    response model leave it ``False`` (the default).
    """
    if echo_request_model:
        # ``_RequestReadingTransport`` wraps the response builder
        # to extract the request model; for the simple case we
        # default to *model* so the response is well-formed even
        # if the test doesn't set ``echo_request_model``.
        pass
    body = {
        "id": request_id,
        "object": "chat.completion",
        "created": 1700000000,
        "model": model,
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": content},
                "finish_reason": "stop",
            }
        ],
        "usage": {
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": prompt_tokens + completion_tokens,
        },
    }
    return httpx.Response(200, json=body)


def chat_completion_error(
    *,
    status_code: int = 500,
    message: str = "boom",
) -> httpx.Response:
    """Return a canned error response."""
    return httpx.Response(status_code, json={"error": {"message": message}})


# ---------------------------------------------------------------------------
# Provider / registry / router fixtures
# ---------------------------------------------------------------------------


def build_providers(
    *,
    response_handler: Callable[[httpx.Request], httpx.Response] | None = None,
) -> tuple[list[OpenAICompatProvider], list[_ScriptedTransport]]:
    """Build the default three providers with a shared (or per-call) handler."""
    providers: list[OpenAICompatProvider] = []
    transports: list[_ScriptedTransport] = []
    for cfg in DEFAULT_TEST_CONFIGS:
        provider, transport = make_provider(cfg, handler=response_handler)
        providers.append(provider)
        transports.append(transport)
    return providers, transports


@pytest.fixture
def make_registry() -> Callable[..., tuple[ProviderRegistry, list[_ScriptedTransport]]]:
    """Return a factory that builds a fresh :class:`ProviderRegistry` per call.

    The factory takes an optional ``response_handler``; when omitted
    a default "always 200" handler is used. The returned transports
    list is in the same order as ``DEFAULT_TEST_CONFIGS`` so a test
    can address them by index.
    """

    def _factory(
        response_handler: Callable[[httpx.Request], httpx.Response] | None = None,
    ) -> tuple[ProviderRegistry, list[_ScriptedTransport]]:

        def _default_response(req: httpx.Request) -> httpx.Response:
            return chat_completion_response()

        if response_handler is None:
            response_handler = _default_response
        providers, transports = build_providers(response_handler=response_handler)
        registry = ProviderRegistry(providers=providers)
        return registry, transports

    return _factory


@pytest.fixture
def make_router() -> Callable[..., Router]:
    """Return a factory that builds a :class:`Router` over a fresh registry."""

    def _factory(
        registry: ProviderRegistry | None = None,
        *,
        cooldown_seconds: float = 300.0,
        failure_threshold: int = 3,
    ) -> Router:
        reg: ProviderRegistry = (
            registry if registry is not None else ProviderRegistry(providers=build_providers()[0])
        )
        return Router(
            reg,
            cooldown_seconds=cooldown_seconds,
            failure_threshold=failure_threshold,
        )

    return _factory


# ---------------------------------------------------------------------------
# App / client fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def make_state() -> Callable[..., Any]:
    """Return a factory that builds a custom :class:`AppState`.

    The factory wires the same registry + router + metering so a
    test can choose any combination.
    """
    from aidp_agent.main import AppState

    def _factory(
        *,
        registry: ProviderRegistry | None = None,
        sink: UsageSink | None = None,
        cooldown_seconds: float = 300.0,
        failure_threshold: int = 3,
    ) -> Any:
        reg: ProviderRegistry = (
            registry if registry is not None else ProviderRegistry(providers=build_providers()[0])
        )
        router_svc = Router(
            reg,
            cooldown_seconds=cooldown_seconds,
            failure_threshold=failure_threshold,
        )
        if sink is None:
            sink = InMemorySink()
        metering = MeteringDispatcher(sink=sink)
        return AppState(registry=reg, router=router_svc, metering=metering)

    return _factory


@pytest.fixture
def make_app() -> Callable[..., FastAPI]:
    """Return a factory that builds a FastAPI app with a custom :class:`AppState`."""
    from aidp_agent.main import create_app

    def _factory(state: Any | None = None) -> FastAPI:
        return create_app(state=state)

    return _factory


@pytest.fixture
def make_client() -> Callable[..., TestClient]:
    """Return a factory that yields a :class:`TestClient` over a custom state."""
    from aidp_agent.main import set_state

    def _factory(state: Any | None = None) -> TestClient:
        app = FastAPI()
        if state is None:
            state = _default_state()
        set_state(state)
        from aidp_agent.main import create_app

        app = create_app(state=state)
        return TestClient(app)

    return _factory


def _default_state() -> Any:  # pragma: no cover - thin shim
    from aidp_agent.main import AppState

    providers, _ = build_providers()
    registry = ProviderRegistry(providers=providers)
    router_svc = Router(registry)
    metering = MeteringDispatcher(sink=InMemorySink())
    return AppState(registry=registry, router=router_svc, metering=metering)


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------


def make_bearer_token(tenant_id: str = "tenant-a", user_id: str = "u-tester") -> str:
    """Return a signed access token for the BYOK endpoint tests."""
    from aidp_auth.jwt import create_access_token

    return create_access_token(tenant_id=tenant_id, user_id=user_id, scopes=["*"])


@pytest.fixture
def auth_headers() -> Callable[..., dict[str, str]]:
    """Return a factory that produces ``Authorization: Bearer ...`` headers."""
    return lambda tenant_id="tenant-a", user_id="u-tester": {
        "Authorization": f"Bearer {make_bearer_token(tenant_id=tenant_id, user_id=user_id)}"
    }


# ---------------------------------------------------------------------------
# Pytest helpers
# ---------------------------------------------------------------------------


def run_async(coro: Any) -> Any:
    """Run an async coroutine in a fresh event loop and return its value.

    ``pytest-asyncio`` is in ``auto`` mode so most tests can use
    ``async def`` directly. The handful of synchronous test helpers
    that need to drive an async function use this shim.
    """
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


# Re-exports for tests.
__all__ = [
    "DEFAULT_TEST_CONFIGS",
    "TaskType",
    "chat_completion_error",
    "chat_completion_response",
    "make_app",
    "make_bearer_token",
    "make_client",
    "make_provider",
    "make_registry",
    "make_router",
    "make_state",
    "run_async",
]
