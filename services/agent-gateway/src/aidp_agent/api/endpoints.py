"""HTTP endpoints for the Agent Gateway.

The module exposes three endpoints:

- ``POST /v1/chat/completions`` — OpenAI-compat. Translates the
  request into the gateway's internal :class:`ChatRequest`, asks the
  router to drive the call (with failover + circuit breaking), then
  re-shapes the response into the OpenAI-compat envelope.
- ``GET  /v1/models`` — OpenAI-compat. Lists every model served by
  the registered providers, plus the gateway's diagnostic metadata
  (``aidp_tier`` / ``aidp_provider`` / ``aidp_circuit_state``).
- ``POST /api/v1/agent/credentials`` — AIDP-internal. BYOK: store a
  per-tenant API-key override for a named provider. The endpoint
  requires ``agent.credentials.write`` permission.

The endpoints rely on FastAPI dependency injection for the
:func:`get_registry`, :func:`get_router`, and :func:`get_metering`
factories. The factories are module-level so a test can override
them via :func:`fastapi.testclient.TestClient` + a dependency
override (see ``tests/test_router.py`` for the pattern).
"""

from __future__ import annotations

import logging
import time
from collections.abc import AsyncIterator
from typing import Annotated, Any

from aidp_auth.dependencies import require_permission
from aidp_auth.jwt import CurrentUser
from aidp_common.errors import NotFoundError, ValidationError
from aidp_common.tracing import get_trace_id
from fastapi import APIRouter, Depends, status
from fastapi.responses import StreamingResponse

from aidp_agent.metering import (
    MeteringDispatcher,
    UsageRecord,
    build_record,
    calculate_cost,
)
from aidp_agent.providers.base import (
    ChatChunk,
    ChatMessage,
    ChatRequest,
    LLMProvider,
    ModelTier,
    TaskType,
    TokenUsage,
)
from aidp_agent.providers.registry import ProviderRegistry
from aidp_agent.router import Router
from aidp_agent.schemas import (
    ChatCompletionChoice,
    ChatCompletionRequest,
    ChatCompletionResponse,
    ChatCompletionUsage,
    CredentialResponse,
    CredentialSetRequest,
    ModelInfo,
    ModelListResponse,
    WireChatMessage,
)

_LOG = logging.getLogger(__name__)

# A single router mounts all three surfaces. The OpenAI-compat paths
# live under ``/v1``; the AIDP-internal BYOK endpoint lives under
# ``/api/v1/agent``.
router = APIRouter(tags=["agent-gateway"])


# ---------------------------------------------------------------------------
# Permission strings
# ---------------------------------------------------------------------------

_PERM_CREDENTIALS_WRITE = "agent.credentials.write"


# ---------------------------------------------------------------------------
# Dependency factories (overridable by tests)
# ---------------------------------------------------------------------------


def get_registry_dep() -> ProviderRegistry:  # pragma: no cover - trivial
    """Default dependency that returns the module-level registry."""
    from aidp_agent.main import get_registry  # local import to avoid cycle

    return get_registry()


def get_router_dep() -> Router:  # pragma: no cover - trivial
    """Default dependency that returns the module-level router."""
    from aidp_agent.main import get_router  # local import to avoid cycle

    return get_router()


def get_metering_dep() -> MeteringDispatcher:  # pragma: no cover - trivial
    """Default dependency that returns the module-level metering dispatcher."""
    from aidp_agent.main import get_metering  # local import to avoid cycle

    return get_metering()


# ---------------------------------------------------------------------------
# Translation helpers
# ---------------------------------------------------------------------------


def _trace_id_str() -> str | None:
    """Return the current OTel trace id as a hex string, or ``None``.

    :func:`aidp_common.tracing.get_trace_id` returns ``int | str | None``;
    the metering layer wants a string. This helper normalises the
    return type so the build_record call below stays typed.
    """
    value = get_trace_id(as_hex=True)
    if value is None:
        return None
    return str(value)


def _wire_to_internal(
    body: ChatCompletionRequest,
    *,
    default_tenant_id: str,
    default_user_id: str | None,
) -> ChatRequest:
    """Project a wire request onto the internal :class:`ChatRequest`."""
    messages: list[ChatMessage] = []
    for msg in body.messages:
        messages.append(
            ChatMessage(
                role=msg.role,
                content=msg.content,
                name=msg.name,
            )
        )
    return ChatRequest(
        model=body.model,
        messages=messages,
        model_tier=ModelTier(body.model_tier) if body.model_tier else None,
        task_type=TaskType(body.task_type) if body.task_type else None,
        temperature=body.temperature,
        max_tokens=body.max_tokens,
        stream=body.stream,
        tenant_id=body.tenant_id or default_tenant_id,
        user_id=body.user_id or default_user_id,
        metadata=body.metadata,
    )


def _chunk_to_response(
    *,
    chunk: ChatChunk,
    provider: LLMProvider,
    cost: float,
) -> ChatCompletionResponse:
    """Project a non-streaming :class:`ChatChunk` onto the wire response."""
    usage = chunk.usage or TokenUsage()
    return ChatCompletionResponse(
        id=chunk.id or f"chatcmpl-{int(time.time() * 1000)}",
        created=int(time.time()),
        model=chunk.model,
        choices=[
            ChatCompletionChoice(
                index=0,
                message=WireChatMessage(role="assistant", content=chunk.delta or ""),
                finish_reason=chunk.finish_reason,
            )
        ],
        usage=ChatCompletionUsage(
            prompt_tokens=usage.prompt_tokens,
            completion_tokens=usage.completion_tokens,
            total_tokens=usage.total_tokens,
        ),
        aidp_provider=provider.config.name,
        aidp_cost_usd=cost,
    )


# ---------------------------------------------------------------------------
# POST /v1/chat/completions
# ---------------------------------------------------------------------------


@router.post(
    "/v1/chat/completions",
    response_model=ChatCompletionResponse,
    status_code=status.HTTP_200_OK,
    summary="OpenAI-compat chat completions (with multi-provider routing + failover).",
)
async def chat_completions(
    body: ChatCompletionRequest,
    registry: Annotated[ProviderRegistry, Depends(get_registry_dep)],
    router_svc: Annotated[Router, Depends(get_router_dep)],
    metering: Annotated[MeteringDispatcher, Depends(get_metering_dep)],
) -> ChatCompletionResponse | StreamingResponse:
    """Drive a chat-completions call through the router.

    Non-streaming callers get a single JSON response. Streaming
    callers (``"stream": true``) get a ``text/event-stream`` body of
    OpenAI-compat SSE chunks.

    The endpoint is intentionally unauthenticated in Phase 1 (the
    brief does not call for IAM integration yet). The tenant
    context is read from the request body (``tenant_id``) and
    defaults to ``"default"`` so a smoke test can hit the endpoint
    without a JWT. A follow-up task can add ``require_permission``
    once the agent-gateway's permission set is settled.
    """
    internal_request = _wire_to_internal(
        body,
        default_tenant_id="default",
        default_user_id=None,
    )
    if body.stream:
        return StreamingResponse(
            _stream_response(internal_request, router_svc, metering),
            media_type="text/event-stream",
        )
    decision, chunk = await router_svc.route_and_call(internal_request)
    usage = chunk.usage or TokenUsage()
    cost = calculate_cost(usage, decision.primary.config, model=decision.resolved_model)
    record = build_record(
        request=internal_request,
        provider_name=decision.primary.config.name,
        model=decision.resolved_model,
        usage=usage,
        cost=cost,
        trace_id=_trace_id_str(),
    )
    metering.record(record)
    return _chunk_to_response(chunk=chunk, provider=decision.primary, cost=cost)


async def _stream_response(
    internal_request: ChatRequest,
    router_svc: Router,
    metering: MeteringDispatcher,
) -> AsyncIterator[bytes]:
    """Yield SSE-encoded OpenAI-compat chunks for a streaming request."""
    decision, iterator = await router_svc.route_and_stream(internal_request)
    final_chunk: ChatChunk | None = None
    async for chunk in iterator:
        final_chunk = chunk
        payload = _chunk_to_stream_payload(chunk, decision.primary.config.name)
        yield f"data: {payload}\n\n".encode()
    yield b"data: [DONE]\n\n"
    if final_chunk is not None and final_chunk.usage is not None:
        cost = calculate_cost(
            final_chunk.usage,
            decision.primary.config,
            model=decision.resolved_model,
        )
        record = build_record(
            request=internal_request,
            provider_name=decision.primary.config.name,
            model=decision.resolved_model,
            usage=final_chunk.usage,
            cost=cost,
            trace_id=_trace_id_str(),
        )
        metering.record(record)


def _chunk_to_stream_payload(chunk: ChatChunk, provider_name: str) -> str:
    """Serialize a :class:`ChatChunk` as the OpenAI-compat SSE JSON payload."""
    import json

    payload: dict[str, Any] = {
        "id": chunk.id,
        "object": "chat.completion.chunk",
        "created": int(time.time()),
        "model": chunk.model,
        "choices": [
            {
                "index": 0,
                "delta": {"role": "assistant", "content": chunk.delta},
                "finish_reason": chunk.finish_reason,
            }
        ],
        "aidp_provider": provider_name,
    }
    if chunk.usage is not None:
        payload["usage"] = {
            "prompt_tokens": chunk.usage.prompt_tokens,
            "completion_tokens": chunk.usage.completion_tokens,
            "total_tokens": chunk.usage.total_tokens,
        }
    return json.dumps(payload, ensure_ascii=False)


# ---------------------------------------------------------------------------
# GET /v1/models
# ---------------------------------------------------------------------------


@router.get(
    "/v1/models",
    response_model=ModelListResponse,
    status_code=status.HTTP_200_OK,
    summary="List available models (OpenAI-compat).",
)
async def list_models(
    registry: Annotated[ProviderRegistry, Depends(get_registry_dep)],
    router_svc: Annotated[Router, Depends(get_router_dep)],
) -> ModelListResponse:
    """Return every model served by every registered provider.

    The list is sorted by ``(tier, provider, model)`` so the result
    is deterministic across calls. Each row includes the AIDP
    diagnostic metadata so a dashboard can colour-code the model
    list by circuit-breaker state.
    """
    items: list[ModelInfo] = []
    for provider in registry.all():
        health = router_svc.health_for(provider)
        for spec in provider.config.model_specs:
            items.append(
                ModelInfo(
                    id=spec.name,
                    created=int(time.time()),
                    owned_by=provider.config.name,
                    aidp_tier=spec.tier.value,
                    aidp_provider=provider.config.name,
                    aidp_input_cost_per_1k=spec.input_cost_per_1k,
                    aidp_output_cost_per_1k=spec.output_cost_per_1k,
                    aidp_circuit_state=health.state.value,
                )
            )
    items.sort(key=lambda m: (m.aidp_tier, m.aidp_provider, m.id))
    return ModelListResponse(data=items)


# ---------------------------------------------------------------------------
# POST /api/v1/agent/credentials
# ---------------------------------------------------------------------------


@router.post(
    "/api/v1/agent/credentials",
    response_model=CredentialResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Store a per-tenant API-key override (BYOK).",
)
async def set_credentials(
    body: CredentialSetRequest,
    user: Annotated[CurrentUser, Depends(require_permission(_PERM_CREDENTIALS_WRITE))],
    registry: Annotated[ProviderRegistry, Depends(get_registry_dep)],
) -> CredentialResponse:
    """Store a BYOK API-key override for the caller's tenant.

    A 404 is returned when the named provider is not registered —
    we deliberately do not auto-register so a typo (``"openaii"``)
    is loud, not silent.
    """
    if registry.get(body.provider) is None:
        raise NotFoundError("provider", body.provider)
    registry.set_byok(
        tenant_id=user.tenant_id,
        provider_name=body.provider,
        api_key=body.api_key,
    )
    return CredentialResponse(
        tenant_id=user.tenant_id,
        provider=body.provider,
        stored=True,
    )


__all__ = [
    "UsageRecord",
    "get_metering_dep",
    "get_registry_dep",
    "get_router_dep",
    "router",
]


_ = ValidationError  # silence unused-import linters
