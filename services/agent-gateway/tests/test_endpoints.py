"""End-to-end tests for the Agent Gateway HTTP surface.

The router / metering unit tests pin the internal logic. These
tests pin the *HTTP* surface: the OpenAI-compat ``/v1/chat/
completions`` and ``/v1/models`` endpoints, and the AIDP-internal
``/api/v1/agent/credentials`` BYOK endpoint. Every upstream LLM call
is faked via the scripted :class:`_ScriptedTransport` so the suite
runs in pure-Python without contacting any real provider.

Each test builds a fresh :class:`AppState` so the global state
seeded by :func:`aidp_agent.main.create_app` is not shared between
tests. The :class:`InMemorySink` exposes the metering records so a
test can assert that an inbound request produced the right
``UsageRecord`` row.
"""

from __future__ import annotations

from collections.abc import Callable

import httpx
import pytest
from aidp_agent.main import AppState, create_app, set_state
from aidp_agent.metering import InMemorySink
from aidp_agent.providers.base import (
    ChatChunk,
    ChatRequest,
    ModelSpec,
    ModelTier,
    ProviderConfig,
    TokenUsage,
)
from aidp_agent.providers.openai_compat import OpenAICompatProvider
from aidp_agent.providers.registry import ProviderRegistry
from aidp_agent.router import Router
from fastapi.testclient import TestClient

from ._fixtures import (
    DEFAULT_TEST_CONFIGS,
    chat_completion_error,
    chat_completion_response,
    make_bearer_token,
    make_provider,
)

# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


def _build_state(
    *,
    response_handler: Callable[[httpx.Request], httpx.Response] | None = None,
    sink: InMemorySink | None = None,
) -> tuple[AppState, list[OpenAICompatProvider], InMemorySink]:
    """Build a fresh :class:`AppState` with mocked providers and a sink."""

    def _default_response(req: httpx.Request) -> httpx.Response:
        return chat_completion_response()

    if response_handler is None:
        response_handler = _default_response
    providers: list[OpenAICompatProvider] = []
    for cfg in DEFAULT_TEST_CONFIGS:
        provider, _ = make_provider(cfg, handler=response_handler)
        providers.append(provider)
    registry = ProviderRegistry(providers=providers)
    router_svc = Router(registry)
    if sink is None:
        sink = InMemorySink()
    from aidp_agent.metering import MeteringDispatcher

    metering = MeteringDispatcher(sink=sink)
    return AppState(registry=registry, router=router_svc, metering=metering), providers, sink


@pytest.fixture
def fresh_state() -> Callable[..., tuple[TestClient, AppState, InMemorySink]]:
    """Return a factory that yields a ``(TestClient, state, sink)`` triple per call."""

    def _factory(
        response_handler: Callable[[httpx.Request], httpx.Response] | None = None,
    ) -> tuple[TestClient, AppState, InMemorySink]:
        state, _, sink = _build_state(response_handler=response_handler)
        set_state(state)
        app = create_app(state=state)
        return TestClient(app), state, sink

    return _factory


# ---------------------------------------------------------------------------
# GET /v1/models
# ---------------------------------------------------------------------------


class TestListModels:
    """Pin the OpenAI-compat ``GET /v1/models`` surface."""

    def test_returns_every_model_with_diagnostics(
        self, fresh_state: Callable[..., tuple[TestClient, AppState, InMemorySink]]
    ) -> None:
        """``/v1/models`` returns every model served by every provider, with AIDP metadata."""
        client, _, _ = fresh_state()
        response = client.get("/v1/models")
        assert response.status_code == 200
        body = response.json()
        assert body["object"] == "list"
        models = {m["id"]: m for m in body["data"]}
        # Every model in the default catalogue is present.
        expected = {
            "gpt-4o",
            "gpt-4o-mini",
            "gpt-4.1-nano",
            "claude-sonnet-4-20250514",
            "claude-haiku-4-5",
            "deepseek-chat",
            "deepseek-reasoner",
        }
        assert expected <= set(models.keys())
        # Each row carries the AIDP diagnostic fields.
        for model in models.values():
            assert "aidp_tier" in model
            assert "aidp_provider" in model
            assert "aidp_circuit_state" in model

    def test_models_sorted_for_determinism(
        self, fresh_state: Callable[..., tuple[TestClient, AppState, InMemorySink]]
    ) -> None:
        """The result is sorted by ``(tier, provider, id)`` for stable pagination."""
        client, _, _ = fresh_state()
        response = client.get("/v1/models")
        rows = response.json()["data"]
        keys = [(m["aidp_tier"], m["aidp_provider"], m["id"]) for m in rows]
        assert keys == sorted(keys)


# ---------------------------------------------------------------------------
# POST /v1/chat/completions
# ---------------------------------------------------------------------------


class TestChatCompletions:
    """Pin the OpenAI-compat ``POST /v1/chat/completions`` surface."""

    def test_non_streaming_call_succeeds_and_meters(
        self, fresh_state: Callable[..., tuple[TestClient, AppState, InMemorySink]]
    ) -> None:
        """A non-streaming call returns a 200 + the OpenAI-compat envelope, and writes a metering row."""
        client, _state, sink = fresh_state()
        body = {
            "model": "gpt-4o",
            "messages": [{"role": "user", "content": "hi"}],
            "tenant_id": "tenant-a",
            "user_id": "u-1",
        }
        response = client.post("/v1/chat/completions", json=body)
        assert response.status_code == 200
        payload = response.json()
        # OpenAI-compat envelope.
        assert payload["object"] == "chat.completion"
        assert payload["model"] == "gpt-4o"
        assert payload["choices"][0]["message"]["role"] == "assistant"
        assert payload["choices"][0]["message"]["content"] == "hi"
        # AIDP extensions.
        assert payload["aidp_provider"] == "openai"
        assert payload["aidp_cost_usd"] is not None
        # The metering row was recorded.
        assert len(sink.records) == 1
        record = sink.records[0]
        assert record.provider_name == "openai"
        assert record.model == "gpt-4o"
        assert record.tenant_id == "tenant-a"
        assert record.user_id == "u-1"

    def test_streaming_call_yields_sse_chunks(
        self, fresh_state: Callable[..., tuple[TestClient, AppState, InMemorySink]]
    ) -> None:
        """A streaming call returns a ``text/event-stream`` body of SSE chunks."""
        # Build a provider whose chat returns two chunks: one content
        # chunk and one terminal chunk with usage. The endpoint
        # forwards both to the caller.
        from collections.abc import AsyncIterator

        class _ScriptedProvider(OpenAICompatProvider):
            def __init__(self) -> None:
                super().__init__(
                    config=ProviderConfig(
                        name="scripted",
                        display_name="Scripted",
                        base_url="https://api.example.com/v1",
                        api_key="sk",
                        model_specs=(ModelSpec("model-a", ModelTier.BALANCED, 0.001, 0.002),),
                    ),
                    client=httpx.AsyncClient(
                        base_url="https://api.example.com/v1",
                        transport=httpx.MockTransport(lambda req: httpx.Response(200, text="")),
                    ),
                )

            def chat(self, request: ChatRequest) -> AsyncIterator[ChatChunk]:
                async def _gen() -> AsyncIterator[ChatChunk]:
                    yield ChatChunk(
                        id="chatcmpl-stream",
                        model="model-a",
                        delta="hello ",
                    )
                    yield ChatChunk(
                        id="chatcmpl-stream",
                        model="model-a",
                        delta="world",
                        finish_reason="stop",
                        usage=TokenUsage(prompt_tokens=5, completion_tokens=2, total_tokens=7),
                    )

                return _gen()

        provider = _ScriptedProvider()
        registry = ProviderRegistry(providers=[provider])
        router_svc = Router(registry)
        from aidp_agent.metering import MeteringDispatcher

        sink = InMemorySink()
        metering = MeteringDispatcher(sink=sink)
        state = AppState(registry=registry, router=router_svc, metering=metering)
        set_state(state)
        client = TestClient(create_app(state=state))

        body = {
            "model": "model-a",
            "messages": [{"role": "user", "content": "hi"}],
            "stream": True,
        }
        with client.stream("POST", "/v1/chat/completions", json=body) as response:
            assert response.status_code == 200
            chunks = list(response.iter_lines())
        # We expect at least two ``data:`` lines (one per chunk) and a final ``[DONE]``.
        data_lines = [line for line in chunks if line.startswith("data: ") and "[DONE]" not in line]
        done_lines = [line for line in chunks if "[DONE]" in line]
        assert len(data_lines) == 2
        assert len(done_lines) == 1
        # The metering row is recorded once the stream completes.
        assert len(sink.records) == 1

    def test_call_with_model_tier_picks_cheapest(
        self, fresh_state: Callable[..., tuple[TestClient, AppState, InMemorySink]]
    ) -> None:
        """A request with ``model_tier=economy`` lands on the cheapest economy provider.

        The router's choice of model (``gpt-4.1-nano``) is
        recorded in the metering row; the upstream's response
        echoes the model the request asked for. We assert on the
        *metering* model (which is the router's decision) rather
        than the wire response (which is the upstream's echo).
        """
        client, _, sink = fresh_state()
        body = {
            "messages": [{"role": "user", "content": "hi"}],
            "model_tier": "economy",
        }
        response = client.post("/v1/chat/completions", json=body)
        assert response.status_code == 200
        payload = response.json()
        # OpenAI gpt-4.1-nano is the cheapest economy model.
        assert payload["aidp_provider"] == "openai"
        # The metering row carries the router's resolved model.
        assert len(sink.records) == 1
        assert sink.records[0].model == "gpt-4.1-nano"

    def test_call_with_task_type_uses_default_mapping(
        self, fresh_state: Callable[..., tuple[TestClient, AppState, InMemorySink]]
    ) -> None:
        """A request with ``task_type=insight`` maps to the flagship tier."""
        client, _, _sink = fresh_state()
        body = {
            "messages": [{"role": "user", "content": "hi"}],
            "task_type": "insight",
        }
        response = client.post("/v1/chat/completions", json=body)
        assert response.status_code == 200
        # flagship tier → openai's gpt-4o is the cheapest flagship model.
        assert response.json()["model"] == "gpt-4o"

    def test_invalid_model_tier_returns_422(
        self, fresh_state: Callable[..., tuple[TestClient, AppState, InMemorySink]]
    ) -> None:
        """A bogus ``model_tier`` value is rejected by the Pydantic validator."""
        client, _, _ = fresh_state()
        body = {
            "messages": [{"role": "user", "content": "hi"}],
            "model_tier": "bogus",
        }
        response = client.post("/v1/chat/completions", json=body)
        assert response.status_code == 422

    def test_empty_messages_returns_422(
        self, fresh_state: Callable[..., tuple[TestClient, AppState, InMemorySink]]
    ) -> None:
        """An empty ``messages`` array is rejected by the Pydantic ``min_length`` validator."""
        client, _, _ = fresh_state()
        body = {"model": "gpt-4o", "messages": []}
        response = client.post("/v1/chat/completions", json=body)
        assert response.status_code == 422

    def test_provider_failure_returns_502(
        self, fresh_state: Callable[..., tuple[TestClient, AppState, InMemorySink]]
    ) -> None:
        """When every provider fails, the endpoint returns a 502 with the AppError envelope."""
        client, _, _ = fresh_state(
            response_handler=lambda req: chat_completion_error(status_code=500, message="down")
        )
        body = {"model": "gpt-4o", "messages": [{"role": "user", "content": "hi"}]}
        response = client.post("/v1/chat/completions", json=body)
        assert response.status_code == 502
        payload = response.json()
        assert payload["code"] == "UPSTREAM_ERROR"
        assert "all providers failed" in payload["message"].lower()

    def test_unknown_model_returns_502(
        self, fresh_state: Callable[..., tuple[TestClient, AppState, InMemorySink]]
    ) -> None:
        """A request for a model no provider serves returns a 502."""
        client, _, _ = fresh_state()
        body = {"model": "gpt-99-unknown", "messages": [{"role": "user", "content": "hi"}]}
        response = client.post("/v1/chat/completions", json=body)
        assert response.status_code == 502


# ---------------------------------------------------------------------------
# POST /api/v1/agent/credentials (BYOK)
# ---------------------------------------------------------------------------


class TestByokCredentials:
    """Pin the per-tenant API-key override endpoint."""

    def test_store_credential_requires_auth(
        self, fresh_state: Callable[..., tuple[TestClient, AppState, InMemorySink]]
    ) -> None:
        """A missing bearer token returns 401."""
        client, _, _ = fresh_state()
        body = {"provider": "openai", "api_key": "sk-tenant-override"}
        response = client.post("/api/v1/agent/credentials", json=body)
        assert response.status_code == 401

    def test_store_credential_succeeds(
        self,
        fresh_state: Callable[..., tuple[TestClient, AppState, InMemorySink]],
    ) -> None:
        """A valid bearer token + a registered provider stores the override and returns 201."""
        client, state, _ = fresh_state()
        token = make_bearer_token(tenant_id="tenant-a", user_id="u-1")
        body = {"provider": "openai", "api_key": "sk-tenant-override"}
        response = client.post(
            "/api/v1/agent/credentials",
            json=body,
            headers={"Authorization": f"Bearer {token}"},
        )
        assert response.status_code == 201
        payload = response.json()
        assert payload["tenant_id"] == "tenant-a"
        assert payload["provider"] == "openai"
        assert payload["stored"] is True
        # The registry now resolves the override.
        assert state.registry.resolve_api_key("tenant-a", "openai") == "sk-tenant-override"

    def test_store_credential_for_unknown_provider_returns_404(
        self,
        fresh_state: Callable[..., tuple[TestClient, AppState, InMemorySink]],
    ) -> None:
        """An unknown provider name returns 404."""
        client, _, _ = fresh_state()
        token = make_bearer_token()
        body = {"provider": "openaii-typo", "api_key": "sk-tenant-override"}
        response = client.post(
            "/api/v1/agent/credentials",
            json=body,
            headers={"Authorization": f"Bearer {token}"},
        )
        assert response.status_code == 404
        assert response.json()["code"] == "NOT_FOUND"

    def test_byok_override_is_tenant_scoped(
        self,
        fresh_state: Callable[..., tuple[TestClient, AppState, InMemorySink]],
    ) -> None:
        """An override set by tenant-a is invisible to tenant-b."""
        client, state, _ = fresh_state()
        token_a = make_bearer_token(tenant_id="tenant-a")
        # Build a token for tenant-b to prove the override is
        # isolated to tenant-a (we don't actually need the
        # tenant-b token to verify isolation, but its presence in
        # the test makes the cross-tenant contract explicit).
        _token_b = make_bearer_token(tenant_id="tenant-b")
        # Tenant A sets an override.
        response = client.post(
            "/api/v1/agent/credentials",
            json={"provider": "openai", "api_key": "sk-tenant-a"},
            headers={"Authorization": f"Bearer {token_a}"},
        )
        assert response.status_code == 201
        # Tenant B's request must not be affected.
        assert state.registry.resolve_api_key("tenant-b", "openai") != "sk-tenant-a"
        # And tenant A can read it back.
        assert state.registry.resolve_api_key("tenant-a", "openai") == "sk-tenant-a"

    def test_store_credential_validates_provider_name(
        self,
        fresh_state: Callable[..., tuple[TestClient, AppState, InMemorySink]],
    ) -> None:
        """A blank provider name is rejected by the Pydantic validator."""
        client, _, _ = fresh_state()
        token = make_bearer_token()
        body = {"provider": "", "api_key": "sk-x"}
        response = client.post(
            "/api/v1/agent/credentials",
            json=body,
            headers={"Authorization": f"Bearer {token}"},
        )
        assert response.status_code == 422

    def test_store_credential_validates_api_key(
        self,
        fresh_state: Callable[..., tuple[TestClient, AppState, InMemorySink]],
    ) -> None:
        """An empty ``api_key`` is rejected."""
        client, _, _ = fresh_state()
        token = make_bearer_token()
        body = {"provider": "openai", "api_key": ""}
        response = client.post(
            "/api/v1/agent/credentials",
            json=body,
            headers={"Authorization": f"Bearer {token}"},
        )
        assert response.status_code == 422


# ---------------------------------------------------------------------------
# Health probes
# ---------------------------------------------------------------------------


class TestHealthProbes:
    """Pin the ``/healthz`` and ``/readyz`` endpoints."""

    def test_healthz_returns_ok(
        self, fresh_state: Callable[..., tuple[TestClient, AppState, InMemorySink]]
    ) -> None:
        client, _, _ = fresh_state()
        response = client.get("/healthz")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}

    def test_readyz_lists_providers(
        self, fresh_state: Callable[..., tuple[TestClient, AppState, InMemorySink]]
    ) -> None:
        client, _, _ = fresh_state()
        response = client.get("/readyz")
        assert response.status_code == 200
        payload = response.json()
        assert payload["status"] == "ready"
        assert set(payload["providers"]) == {"openai", "anthropic", "deepseek"}
