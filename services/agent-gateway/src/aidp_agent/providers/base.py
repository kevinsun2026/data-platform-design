"""Provider abstractions for the Agent Gateway.

This module defines the small set of types every concrete LLM provider
implementation in :mod:`aidp_agent.providers` conforms to. The goal is
to let the rest of the codebase (:mod:`aidp_agent.router`,
:mod:`aidp_agent.metering`, :mod:`aidp_agent.api`) reason about
"providers" generically, without ever importing the OpenAI-compat
client directly.

Design choices
--------------

- The request / response shapes are Pydantic models, not raw ``dict``s,
  so a router test can construct fixtures with full type-checked
  field access and the OpenAI-compat translator can be a thin
  dict-to-model adapter without re-validating.
- The Protocol uses ``AsyncIterator[ChatChunk]`` for streaming
  responses. Even non-streaming responses are surfaced as a single
  element iterator so the router and metering code can stay
  uniform across the two modes.
- ``LLMProvider`` is a :class:`typing.Protocol`, not an ABC. The router
  accepts any object that implements the shape — including in-process
  mocks used by the test suite. Concrete classes declare
  ``LLMProvider`` only as a typing aid.
- ``count_tokens`` is a method on the provider because different
  providers (and different models from the same provider) use different
  tokenisers; the gateway never assumes a single shared tokeniser.
- :class:`ProviderError` and :class:`ProviderTransientError` are
  sentinel exceptions used by the router's failover + circuit-breaker
  logic. A :class:`ProviderTransientError` is retried on the same
  provider and triggers failover; a plain :class:`ProviderError` is
  surfaced directly to the caller.
"""

from __future__ import annotations

import abc
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Literal, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field

# ---------------------------------------------------------------------------
# Model tier / task type
# ---------------------------------------------------------------------------


class ModelTier(str, Enum):  # noqa: UP042 - intentional str-Enum mixin
    """Cost-vs-capability tier for a model.

    - ``FLAGSHIP``: highest-capability, highest-cost model (e.g. GPT-4o,
      Claude Sonnet 4, DeepSeek-V3 for hard reasoning). Used for
      ``insight`` tasks by default.
    - ``BALANCED``: mid-tier model (e.g. GPT-4o-mini, Claude Haiku,
      DeepSeek-V3). The default for ``etl`` tasks.
    - ``ECONOMY``: cheapest, lowest-capability model (e.g. local 7B
      model, ``gpt-4.1-nano``). Used for ``sql`` tasks and for
      high-volume batch processing.
    """

    FLAGSHIP = "flagship"
    BALANCED = "balanced"
    ECONOMY = "economy"


class TaskType(str, Enum):  # noqa: UP042 - intentional str-Enum mixin
    """The work-type hint attached to a chat-completions call.

    The router uses this to pick the right tier when the request does
    not name a model. The mapping is encoded in
    :data:`aidp_agent.router.DEFAULT_TASK_TO_TIER` and is overridable
    per deployment.
    """

    SQL = "sql"
    ETL = "etl"
    INSIGHT = "insight"


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------


class ChatMessage(BaseModel):
    """One element of the ``messages`` array.

    Mirrors the OpenAI shape verbatim so the OpenAI-compat translator
    is a pass-through. ``name`` and ``function_call`` are not exposed
    here; the gateway only supports the modern
    ``{"role": "user"|"assistant"|"system", "content": "..."}`` shape
    plus the structured ``{"type": "text", "text": "..."}`` content
    used by multi-modal inputs.
    """

    model_config = ConfigDict(extra="ignore")

    role: Literal["system", "user", "assistant", "tool"]
    content: str | list[dict[str, Any]]
    name: str | None = None


class ChatRequest(BaseModel):
    """The gateway's internal ``ChatRequest`` shape.

    Providers translate this to whatever the upstream wire shape looks
    like (most are OpenAI-compat; Anthropic is a thin adapter on top).
    The gateway stores a single canonical request type so
    router / metering / API can be provider-agnostic.

    Attributes:
        model: The model the caller asked for. Optional: when omitted
            the router picks a default for the resolved
            ``model_tier`` + ``task_type``.
        messages: The conversation history.
        model_tier: The tier hint (used by the router when *model* is
            ``None``). Optional — defaults to ``None`` and falls back to
            the request's ``task_type`` default tier.
        task_type: The work-type hint.
        temperature: Sampling temperature. ``None`` lets the upstream
            decide.
        max_tokens: Hard cap on completion tokens. ``None`` lets the
            upstream decide.
        stream: When ``True`` the provider returns an iterator of
            ``ChatChunk``; when ``False`` the iterator yields exactly
            one element with the full response.
        tenant_id: Tenant the call belongs to (passed through to
            metering for per-tenant cost reporting).
        user_id: Optional user id the call belongs to.
        metadata: Free-form key/value bag forwarded to the metering
            sink (e.g. trace id, plan tier). Providers may also read
            ``metadata["trace_id"]`` for their own logging.
    """

    model_config = ConfigDict(extra="forbid")

    model: str | None = None
    messages: list[ChatMessage] = Field(min_length=1)
    model_tier: ModelTier | None = None
    task_type: TaskType | None = None
    temperature: float | None = None
    max_tokens: int | None = None
    stream: bool = False
    tenant_id: str = "default"
    user_id: str | None = None
    metadata: dict[str, str] = Field(default_factory=dict)


class TokenUsage(BaseModel):
    """Token counts for one chat-completions call.

    The fields are deliberately named to match the OpenAI /v1/chat/
    completions ``usage`` object so a single translator can map
    every upstream shape to this model.
    """

    model_config = ConfigDict(extra="ignore")

    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0


class ChatChunk(BaseModel):
    """One element of the provider's streaming response.

    The ``delta`` carries the partial content. For a non-streaming
    call the provider yields a single ``ChatChunk`` whose ``finish_reason``
    is set to ``"stop"`` (or whatever the upstream returned) and whose
    ``delta`` is the full assistant message.

    The gateway never inspects ``delta`` / ``finish_reason`` for
    correctness — it forwards the chunk to the caller and attaches the
    ``usage`` to the *terminal* chunk so the metering layer can read
    it once per call.
    """

    model_config = ConfigDict(extra="ignore")

    id: str
    model: str
    delta: str = ""
    finish_reason: str | None = None
    usage: TokenUsage | None = None
    # ``raw`` carries the original upstream JSON so the API can return
    # it verbatim in OpenAI-compat mode without losing fields the
    # gateway does not model (e.g. ``logprobs``).
    raw: dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Provider configuration
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ModelSpec:
    """One logical model a provider serves.

    Different models on the same vendor can live in different
    :class:`ModelTier`s and have different per-1K prices. The
    :class:`ProviderConfig` carries a tuple of :class:`ModelSpec` so
    a single provider instance can advertise, e.g., ``gpt-4o``
    (flagship, $5/$15) and ``gpt-4.1-nano`` (economy, $0.10/$0.40)
    on the same upstream URL.
    """

    name: str
    tier: ModelTier
    input_cost_per_1k: float = 0.0
    output_cost_per_1k: float = 0.0


@dataclass(frozen=True)
class ProviderConfig:
    """Static configuration for one provider instance.

    A provider is a binding of (a) a wire protocol class (today every
    concrete provider is an OpenAI-compat instance pointed at a
    different ``base_url``), (b) one or more :class:`ModelSpec`s, and
    (c) shared defaults (timeout, default tier when a model is
    unnamed, ...).

    Attributes:
        name: Stable identifier used in logs, metering rows, and the
            ``provider`` field of the gateway's ``/v1/models`` response.
            Conventionally the vendor slug (``"openai"`` /
            ``"anthropic"`` / ``"deepseek"``).
        display_name: Human-readable label surfaced via
            ``/v1/models``.
        base_url: Upstream ``https://api.openai.com/v1``-style URL.
        api_key: API key. The BYOK store (Task 12.3) overrides this
            per-tenant at request time.
        model_specs: The model catalogue served by this provider. Each
            :class:`ModelSpec` carries its own tier + price. The
            router uses the per-model tier; a request that names a
            model is matched against the specs first.
        default_tier: Tier used when the request names a model this
            provider serves but the spec is missing. Defaults to
            :attr:`ModelTier.BALANCED`.
        timeout_seconds: HTTP timeout for a single upstream call.
        max_retries: Per-call retry budget (consumed by the provider
            itself before the gateway's failover kicks in).
    """

    name: str
    display_name: str
    base_url: str
    api_key: str
    model_specs: tuple[ModelSpec, ...]
    default_tier: ModelTier = ModelTier.BALANCED
    timeout_seconds: float = 30.0
    max_retries: int = 0
    extra: dict[str, Any] = field(default_factory=dict)

    # ------------------------------------------------------------------
    # Convenience accessors used by the router + metering layer
    # ------------------------------------------------------------------

    @property
    def models(self) -> tuple[str, ...]:
        """Return the logical model names served by this provider."""
        return tuple(spec.name for spec in self.model_specs)

    def tier_for(self, model: str | None) -> ModelTier:
        """Return the tier for *model*, or :attr:`default_tier` when unknown."""
        if model is None:
            return self.default_tier
        for spec in self.model_specs:
            if spec.name == model:
                return spec.tier
        return self.default_tier

    def price_for(self, model: str | None) -> tuple[float, float]:
        """Return ``(input_cost_per_1k, output_cost_per_1k)`` for *model*.

        Falls back to ``(0.0, 0.0)`` when the model is unknown — a
        local mirror that does not publish a price reports zero
        rather than raising.
        """
        if model is not None:
            for spec in self.model_specs:
                if spec.name == model:
                    return spec.input_cost_per_1k, spec.output_cost_per_1k
        return 0.0, 0.0


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class ProviderError(Exception):
    """A provider raised a permanent (non-retryable) error.

    The router surfaces this directly to the caller via the
    :class:`aidp_common.errors.UpstreamError` envelope. 4xx responses
    from the upstream are typically permanent; ``5xx`` and network
    errors are :class:`ProviderTransientError` instead.
    """

    def __init__(
        self,
        message: str,
        *,
        provider: str,
        status_code: int | None = None,
        body: str | None = None,
    ) -> None:
        super().__init__(message)
        self.provider = provider
        self.status_code = status_code
        self.body = body

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return (
            f"ProviderError(provider={self.provider!r}, "
            f"status={self.status_code}, message={super().__str__()!r})"
        )


class ProviderTransientError(ProviderError):
    """A provider raised a transient (retryable) error.

    The router treats this as "try the next provider in the failover
    list, and add a strike to this provider's circuit breaker".
    5xx responses, network errors, and timeouts all map here.
    """


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class LLMProvider(Protocol):
    """The contract every concrete provider satisfies.

    The protocol is structural — the test suite passes plain objects
    that quack like this without inheriting from anything. The router
    does an ``isinstance(provider, LLMProvider)`` check only as a
    defensive guard; the actual call site uses the duck-typed methods.

    Methods:
        chat: Drive a single chat-completions call. Returns an
            async iterator of :class:`ChatChunk`; non-streaming calls
            yield exactly one element with the full response.
        count_tokens: Estimate the number of tokens a piece of text
            would consume for the given *model*. The gateway calls
            this *before* the upstream call when the request asks for
            a pre-flight cost estimate.
        health: Return a snapshot of the provider's circuit state.
            The router consults this to decide whether the provider
            is currently usable.
    """

    config: ProviderConfig

    def chat(self, request: ChatRequest) -> AsyncIterator[ChatChunk]: ...

    async def count_tokens(self, text: str, model: str) -> int: ...

    async def health(self) -> ProviderHealth: ...


class ProviderState(str, Enum):  # noqa: UP042 - intentional str-Enum mixin
    """Coarse state of a provider's circuit breaker."""

    CLOSED = "closed"  # healthy; calls flow through
    OPEN = "open"  # tripped; calls are short-circuited to the next provider
    HALF_OPEN = "half_open"  # cool-off elapsed; next call is a probe


@dataclass(frozen=True)
class ProviderHealth:
    """Snapshot of a provider's circuit state.

    Attributes:
        state: Current breaker state.
        consecutive_failures: Number of consecutive failed calls since
            the last success. Resets to 0 on any success.
        opened_at: Wall-clock time the breaker last tripped. ``None``
            when the breaker is currently closed.
        cooldown_seconds: Configured cool-off window.
    """

    state: ProviderState
    consecutive_failures: int
    opened_at: float | None
    cooldown_seconds: float


# ---------------------------------------------------------------------------
# Abstract base for the rare case where a provider does need inheritance
# ---------------------------------------------------------------------------


class BaseProvider(abc.ABC):
    """Convenience base class for providers that prefer inheritance.

    Most concrete providers are :class:`OpenAICompatProvider` instances
    configured differently; this base is here so a custom
    non-OpenAI-compat provider (e.g. a future Anthropic native client)
    does not have to re-implement ``config`` plumbing.
    """

    def __init__(self, config: ProviderConfig) -> None:
        self.config = config

    @abc.abstractmethod
    def chat(self, request: ChatRequest) -> AsyncIterator[ChatChunk]:
        """Yield chat-completion chunks for *request*."""

    @abc.abstractmethod
    async def count_tokens(self, text: str, model: str) -> int:
        """Estimate the token count of *text* for *model*."""

    @abc.abstractmethod
    async def health(self) -> ProviderHealth:
        """Return a snapshot of the provider's circuit state."""


__all__ = [
    "BaseProvider",
    "ChatChunk",
    "ChatMessage",
    "ChatRequest",
    "LLMProvider",
    "ModelSpec",
    "ModelTier",
    "ProviderConfig",
    "ProviderError",
    "ProviderHealth",
    "ProviderState",
    "ProviderTransientError",
    "TaskType",
    "TokenUsage",
]
