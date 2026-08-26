"""Pydantic v2 wire models for the Agent Gateway HTTP surface.

The HTTP layer projects :class:`aidp_agent.providers.base.ChatRequest`
and the providers' :class:`ChatChunk` onto these models. The split
mirrors the rest of the platform: wire models are Pydantic, internal
models are dataclasses / provider-defined Pydantic models.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

# ---------------------------------------------------------------------------
# OpenAI-compat chat completions
# ---------------------------------------------------------------------------


class WireChatMessage(BaseModel):
    """One element of the OpenAI-compat ``messages`` array.

    The gateway accepts both the legacy ``"content": "string"`` shape
    and the structured ``"content": [{"type": "text", "text": "..."}]``
    shape so callers using either style work without translation.
    """

    model_config = ConfigDict(extra="ignore")

    role: Literal["system", "user", "assistant", "tool"]
    content: str | list[dict[str, Any]]
    name: str | None = None


class ChatCompletionRequest(BaseModel):
    """Body of ``POST /v1/chat/completions``.

    The model is intentionally permissive (``extra="ignore"``) so a
    caller that sends OpenAI-only fields (``top_p``, ``stop``,
    ``presence_penalty`` ...) is not rejected with a 400 — the
    gateway ignores them and forwards the rest to the upstream.
    """

    model_config = ConfigDict(extra="ignore")

    model: str | None = None
    messages: list[WireChatMessage] = Field(min_length=1)
    temperature: float | None = None
    max_tokens: int | None = None
    stream: bool = False
    # AIDP-specific extensions. ``model_tier`` and ``task_type`` are
    # hints the router uses when *model* is omitted; ``tenant_id`` and
    # ``user_id`` are stamped on the metering row.
    model_tier: Literal["flagship", "balanced", "economy"] | None = None
    task_type: Literal["sql", "etl", "insight"] | None = None
    tenant_id: str | None = None
    user_id: str | None = None
    metadata: dict[str, str] = Field(default_factory=dict)


class ChatCompletionChoice(BaseModel):
    """One element of the OpenAI-compat ``choices`` array."""

    model_config = ConfigDict(extra="ignore")

    index: int
    message: WireChatMessage
    finish_reason: str | None = None


class ChatCompletionUsage(BaseModel):
    """The OpenAI-compat ``usage`` block."""

    model_config = ConfigDict(extra="ignore")

    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0


class ChatCompletionResponse(BaseModel):
    """Body of ``POST /v1/chat/completions`` (non-streaming)."""

    model_config = ConfigDict(extra="ignore")

    id: str
    object: str = "chat.completion"
    created: int
    model: str
    choices: list[ChatCompletionChoice]
    usage: ChatCompletionUsage
    # AIDP-specific: which provider answered. Surfaced so the caller
    # can audit cost / latency per provider.
    aidp_provider: str | None = None
    aidp_cost_usd: float | None = None


# ---------------------------------------------------------------------------
# Models listing
# ---------------------------------------------------------------------------


class ModelInfo(BaseModel):
    """One element of the OpenAI-compat ``data`` array."""

    model_config = ConfigDict(extra="ignore")

    id: str
    object: str = "model"
    created: int
    owned_by: str
    # AIDP-specific extensions
    aidp_tier: Literal["flagship", "balanced", "economy"]
    aidp_provider: str
    aidp_input_cost_per_1k: float
    aidp_output_cost_per_1k: float
    aidp_circuit_state: str


class ModelListResponse(BaseModel):
    """Body of ``GET /v1/models``."""

    model_config = ConfigDict(extra="ignore")

    object: str = "list"
    data: list[ModelInfo]


# ---------------------------------------------------------------------------
# BYOK credentials
# ---------------------------------------------------------------------------


class CredentialSetRequest(BaseModel):
    """Body of ``POST /api/v1/agent/credentials``.

    The endpoint stores a per-tenant API-key override for the named
    provider. The provider's actual key (``config.api_key``) is the
    default; the override wins whenever a request for that tenant
    routes to that provider.
    """

    model_config = ConfigDict(extra="forbid")

    provider: str = Field(
        min_length=1,
        max_length=64,
        description="Provider name (must match a registered provider).",
    )
    api_key: str = Field(
        min_length=1,
        max_length=512,
        description="The tenant's API key for *provider*.",
    )

    @field_validator("provider")
    @classmethod
    def _validate_provider(cls, value: str) -> str:
        normalized = value.strip().lower()
        if not normalized:
            raise ValueError("provider must be a non-empty string")
        return normalized


class CredentialResponse(BaseModel):
    """Body of ``POST /api/v1/agent/credentials``."""

    model_config = ConfigDict(extra="forbid")

    tenant_id: str
    provider: str
    stored: bool = True


__all__ = [
    "ChatCompletionChoice",
    "ChatCompletionRequest",
    "ChatCompletionResponse",
    "ChatCompletionUsage",
    "CredentialResponse",
    "CredentialSetRequest",
    "ModelInfo",
    "ModelListResponse",
    "WireChatMessage",
]
