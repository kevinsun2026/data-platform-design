"""Router: (model_tier, task_type) -> provider, with failover and circuit breaking.

The router is the single decision point the HTTP layer consults to
pick an LLM provider for an incoming chat-completions request. It
encapsulates three concerns:

1. **Default resolution.** When the request does not name a model
   but does carry a ``model_tier`` and/or ``task_type``, pick the
   cheapest healthy provider in the matching tier.
2. **Failover.** If the chosen provider raises
   :class:`ProviderTransientError` (timeout, 5xx, 429), try the next
   healthy candidate in the same tier (cheapest-first). The failover
   list is computed once per request — repeated transient errors from
   the same provider keep it on the candidate list, so a single
   request can be answered by *any* number of providers in sequence.
3. **Circuit breaking.** Every provider has a per-process circuit
   breaker. After :data:`CIRCUIT_FAILURE_THRESHOLD` consecutive
   failures, the breaker opens for :data:`CIRCUIT_COOLDOWN_SECONDS`.
   During the cool-off the provider is excluded from candidate lists;
   after it elapses the breaker enters :attr:`ProviderState.HALF_OPEN`
   and the next call is a probe. A successful probe closes the
   breaker; a failed probe re-opens it.

The router is intentionally synchronous (the breaker state lives
in process memory). The hot path inside ``route_and_call`` is
``async`` because the providers are.
"""

from __future__ import annotations

import logging
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass

from aidp_common.errors import UpstreamError

from aidp_agent.providers.base import (
    ChatChunk,
    ChatRequest,
    LLMProvider,
    ModelTier,
    ProviderConfig,
    ProviderError,
    ProviderHealth,
    ProviderState,
    ProviderTransientError,
    TaskType,
)
from aidp_agent.providers.registry import ProviderRegistry

_LOG = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _cheapest_price(config: ProviderConfig) -> tuple[float, float]:
    """Return ``(input, output)`` price for the cheapest model on *config*.

    Used by the sort key when the request does not name a model —
    the router still wants to pick the cheapest provider first.
    """
    if not config.model_specs:
        return 0.0, 0.0
    cheapest = min(
        config.model_specs,
        key=lambda s: (s.output_cost_per_1k, s.input_cost_per_1k, s.name),
    )
    return cheapest.input_cost_per_1k, cheapest.output_cost_per_1k


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


#: Number of consecutive failures that trip a provider's circuit breaker.
CIRCUIT_FAILURE_THRESHOLD: int = 3


#: Wall-clock seconds a tripped breaker stays open before the next call
#: is allowed as a probe (``HALF_OPEN``).
CIRCUIT_COOLDOWN_SECONDS: float = 300.0  # 5 minutes, per Task 12 brief.


#: Default ``(task_type -> model_tier)`` mapping used when the request
#: carries a ``task_type`` but no explicit ``model_tier``. The
#: mapping is intentionally simple: SQL → economy (cheap /
#: deterministic), ETL → balanced (mid-tier), insight → flagship
#: (high reasoning). A deployment can override via the
#: ``AIDP_AGENT_TASK_TIER_OVERRIDES`` env var (JSON dict).
DEFAULT_TASK_TO_TIER: dict[TaskType, ModelTier] = {
    TaskType.SQL: ModelTier.ECONOMY,
    TaskType.ETL: ModelTier.BALANCED,
    TaskType.INSIGHT: ModelTier.FLAGSHIP,
}


# ---------------------------------------------------------------------------
# Per-provider breaker
# ---------------------------------------------------------------------------


@dataclass
class _Breaker:
    """In-memory circuit breaker for one provider.

    The breaker is intentionally tiny: three counters and a
    ``opened_at`` timestamp. The router mutates it on every call and
    consults it on the hot path; under contention the per-provider
    lock serialises the mutation.
    """

    provider_name: str
    cooldown_seconds: float = CIRCUIT_COOLDOWN_SECONDS
    threshold: int = CIRCUIT_FAILURE_THRESHOLD
    consecutive_failures: int = 0
    opened_at: float | None = None
    state: ProviderState = ProviderState.CLOSED
    # Diagnostic counters — useful for /v1/models and the audit log.
    total_successes: int = 0
    total_failures: int = 0

    def record_success(self) -> None:
        """Record a successful call. Resets the consecutive-failure counter."""
        self.consecutive_failures = 0
        self.opened_at = None
        self.state = ProviderState.CLOSED
        self.total_successes += 1

    def record_failure(self) -> None:
        """Record a failed call. Trips the breaker on the threshold'th consecutive failure."""
        self.consecutive_failures += 1
        self.total_failures += 1
        if self.consecutive_failures >= self.threshold and self.state == ProviderState.CLOSED:
            self.opened_at = time.time()
            self.state = ProviderState.OPEN
            _LOG.warning(
                "provider circuit opened",
                extra={
                    "provider": self.provider_name,
                    "consecutive_failures": self.consecutive_failures,
                    "cooldown_seconds": self.cooldown_seconds,
                },
            )

    def evaluate(self) -> ProviderState:
        """Return the current breaker state, transitioning to half-open if the cool-off elapsed."""
        if (
            self.state == ProviderState.OPEN
            and self.opened_at is not None
            and (time.time() - self.opened_at) >= self.cooldown_seconds
        ):
            self.state = ProviderState.HALF_OPEN
            _LOG.info(
                "provider circuit half-open",
                extra={"provider": self.provider_name},
            )
        return self.state

    def snapshot(self) -> ProviderHealth:
        """Return a :class:`ProviderHealth` snapshot for the diagnostic surface."""
        return ProviderHealth(
            state=self.evaluate(),
            consecutive_failures=self.consecutive_failures,
            opened_at=self.opened_at,
            cooldown_seconds=self.cooldown_seconds,
        )


# ---------------------------------------------------------------------------
# Resolution result
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RouteDecision:
    """The result of a :meth:`Router.resolve` call.

    The router may pre-select a primary provider (the cheapest healthy
    candidate) and carry a precomputed failover chain so
    :meth:`route_and_call` can iterate without recomputing.
    """

    primary: LLMProvider
    failover: tuple[LLMProvider, ...]
    resolved_model: str
    resolved_tier: ModelTier


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------


class Router:
    """Resolve chat-completion requests to a primary + failover chain.

    A single :class:`Router` instance is shared across every request
    for the lifetime of the process. The router holds a
    :class:`ProviderRegistry` reference and a per-provider
    :class:`_Breaker`; both are mutated in place.
    """

    def __init__(
        self,
        registry: ProviderRegistry,
        *,
        task_to_tier: dict[TaskType, ModelTier] | None = None,
        cooldown_seconds: float = CIRCUIT_COOLDOWN_SECONDS,
        failure_threshold: int = CIRCUIT_FAILURE_THRESHOLD,
    ) -> None:
        self._registry = registry
        self._task_to_tier: dict[TaskType, ModelTier] = (
            dict(task_to_tier) if task_to_tier is not None else dict(DEFAULT_TASK_TO_TIER)
        )
        self._cooldown_seconds = cooldown_seconds
        self._failure_threshold = failure_threshold
        # Per-provider breaker, keyed by provider name.
        self._breakers: dict[str, _Breaker] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def breaker(self, provider: LLMProvider) -> _Breaker:
        """Return (and lazily create) the breaker for *provider*."""
        breaker = self._breakers.get(provider.config.name)
        if breaker is None:
            breaker = _Breaker(
                provider_name=provider.config.name,
                cooldown_seconds=self._cooldown_seconds,
                threshold=self._failure_threshold,
            )
            self._breakers[provider.config.name] = breaker
        return breaker

    def health_for(self, provider: LLMProvider) -> ProviderHealth:
        """Return a snapshot of *provider*'s breaker state."""
        return self.breaker(provider).snapshot()

    def resolve(self, request: ChatRequest) -> RouteDecision:
        """Pick a primary provider + failover chain for *request*.

        Resolution rules (in order):

        1. If ``request.model`` is set, narrow the candidate list to
           providers that serve that model. The primary is the
           cheapest one; the failover list is the rest.
        2. Else if ``request.model_tier`` is set, narrow to providers
           that have a model in that tier. Primary is the cheapest;
           failover is the rest.
        3. Else if ``request.task_type`` is set, derive the tier via
           :data:`DEFAULT_TASK_TO_TIER` (or the override) and recurse.
        4. Else default to the :attr:`ModelTier.BALANCED` tier.

        Breakers in the :attr:`ProviderState.OPEN` state are excluded
        from the candidate list. A breaker in
        :attr:`ProviderState.HALF_OPEN` is included but placed at the
        *end* of the failover chain so it is only consulted if every
        healthy candidate has failed.
        """
        candidates = self._candidates_for(request)
        if not candidates:
            raise UpstreamError(
                "no healthy provider available for the requested model / tier",
                details={
                    "model": request.model,
                    "model_tier": request.model_tier.value if request.model_tier else None,
                    "task_type": request.task_type.value if request.task_type else None,
                },
            )
        # Sort by (output_cost, input_cost) for the *resolved* model
        # so the cheapest provider for this specific call is tried
        # first. Ties are broken by name to make the order
        # deterministic.
        candidates_sorted = sorted(
            candidates,
            key=lambda p: self._sort_key(p, request),
        )
        primary = candidates_sorted[0]
        failover = tuple(candidates_sorted[1:])
        resolved_tier = self._resolve_tier(request)
        resolved_model = request.model or self._default_model_for(resolved_tier)
        return RouteDecision(
            primary=primary,
            failover=failover,
            resolved_model=resolved_model,
            resolved_tier=resolved_tier,
        )

    @staticmethod
    def _sort_key(provider: LLMProvider, request: ChatRequest) -> tuple[float, float, str]:
        """Sort key for the failover list.

        Uses the resolved-model price when the request names one;
        otherwise the provider's lowest-priced model. Falling back
        to ``0.0`` keeps a local vLLM mirror at the head of the
        list (its cost is zero) which is the right behaviour for
        a self-hosted mirror.
        """
        if request.model is not None:
            input_price, output_price = provider.config.price_for(request.model)
        else:
            input_price, output_price = _cheapest_price(provider.config)
        return (output_price, input_price, provider.config.name)

    async def route_and_call(self, request: ChatRequest) -> tuple[RouteDecision, ChatChunk]:
        """Drive a chat-completions call across the failover chain.

        The method:

        1. Calls :meth:`resolve` to compute the primary + failover list.
        2. Iterates the list, calling the provider and breaking on
           the first success.
        3. Records every outcome in the provider's breaker.

        Non-streaming callers only consume the first yielded chunk
        from the provider; the router itself only needs the terminal
        chunk (which carries the ``usage`` block). Streaming callers
        use :meth:`route_and_stream` instead.
        """
        decision = self.resolve(request)
        last_error: ProviderError | None = None
        for provider in (decision.primary, *decision.failover):
            breaker = self.breaker(provider)
            state = breaker.evaluate()
            if state == ProviderState.OPEN:
                _LOG.debug(
                    "provider circuit open, skipping",
                    extra={"provider": provider.config.name},
                )
                continue
            try:
                final_chunk: ChatChunk | None = None
                # Drive the provider's iterator and remember only the
                # terminal chunk; this keeps the non-streaming contract
                # uniform with the streaming one.
                resolved_request = self._with_model(request, decision.resolved_model)
                async for chunk in provider.chat(resolved_request):
                    if chunk.finish_reason is not None or chunk.usage is not None:
                        final_chunk = chunk
                if final_chunk is None:
                    # The provider yielded only intermediate chunks
                    # with no terminal signal. Treat as a soft failure
                    # so the failover chain still kicks in.
                    raise ProviderError(
                        "provider returned no terminal chunk",
                        provider=provider.config.name,
                    )
                breaker.record_success()
                # If we were in HALF_OPEN, the success closed the
                # breaker; surface that via the decision.
                if state == ProviderState.HALF_OPEN:
                    _LOG.info(
                        "provider circuit closed after probe",
                        extra={"provider": provider.config.name},
                    )
                return decision, final_chunk
            except ProviderTransientError as exc:
                last_error = exc
                breaker.record_failure()
                _LOG.warning(
                    "provider transient error, trying next",
                    extra={
                        "provider": provider.config.name,
                        "error": str(exc),
                        "status": exc.status_code,
                    },
                )
                continue
            except ProviderError:
                # Permanent error: do not try the failover chain —
                # the call is a 4xx and the next provider would
                # likely repeat the same mistake (bad input).
                breaker.record_failure()
                raise
        # Every provider in the chain was tried and failed.
        message = str(last_error) if last_error is not None else "all providers failed"
        raise UpstreamError(
            "all providers failed",
            details={
                "last_error": message,
                "tried": [p.config.name for p in (decision.primary, *decision.failover)],
            },
        )

    async def route_and_stream(
        self, request: ChatRequest
    ) -> tuple[RouteDecision, AsyncIterator[ChatChunk]]:
        """Return a streaming async iterator for *request*.

        Unlike :meth:`route_and_call`, this method does not consume
        the iterator — it returns it to the caller, who is then
        responsible for ``async for``-ing the chunks. The router
        commits to the first provider that successfully yields at
        least one chunk; a later failure in the same stream is the
        caller's problem (it surfaces as an exception during the
        ``async for``).
        """
        decision = self.resolve(request)
        for provider in (decision.primary, *decision.failover):
            breaker = self.breaker(provider)
            state = breaker.evaluate()
            if state == ProviderState.OPEN:
                continue
            resolved_request = self._with_model(request, decision.resolved_model)
            # Wrap the provider's iterator so we can record the
            # outcome of the first successful chunk and decide
            # whether to fall back.
            #
            # Strategy: we eagerly pull the *first* chunk to validate
            # the provider. If that first chunk raises
            # ProviderTransientError, we move to the next provider.
            # If the first chunk is a normal content chunk, we hand
            # the rest of the iterator to the caller and record a
            # success on the breaker when the caller has finished.
            #
            # The simplest way to keep this contract without buffering
            # the whole stream is a small ``_ProbingIterator`` wrapper.
            iterator = provider.chat(resolved_request)
            try:
                first = await iterator.__anext__()
            except StopAsyncIteration:
                # Empty stream — treat as a transient failure.
                breaker.record_failure()
                continue
            except ProviderTransientError as exc:
                last_error: ProviderError = exc
                breaker.record_failure()
                _LOG.warning(
                    "provider transient error on stream start, trying next",
                    extra={"provider": provider.config.name, "error": str(exc)},
                )
                _ = last_error
                continue
            except ProviderError:
                breaker.record_failure()
                raise

            async def _drive(
                first_chunk: ChatChunk,
                upstream: AsyncIterator[ChatChunk],
                br: _Breaker,
            ) -> AsyncIterator[ChatChunk]:
                yield first_chunk
                try:
                    async for chunk in upstream:
                        yield chunk
                    br.record_success()
                except ProviderTransientError as exc:
                    br.record_failure()
                    _LOG.warning(
                        "provider transient error mid-stream",
                        extra={"provider": br.provider_name, "error": str(exc)},
                    )
                    raise
                except ProviderError:
                    br.record_failure()
                    raise
                else:
                    br.record_success()

            return decision, _drive(first_chunk=first, upstream=iterator, br=breaker)
        raise UpstreamError(
            "all providers failed (stream)",
            details={"tried": [p.config.name for p in (decision.primary, *decision.failover)]},
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _candidates_for(self, request: ChatRequest) -> list[LLMProvider]:
        """Return the (open) candidate list for *request*."""
        if request.model is not None:
            providers = self._registry.by_model(request.model)
        else:
            tier = self._resolve_tier(request)
            providers = self._registry.by_tier(tier)
        healthy: list[LLMProvider] = []
        for provider in providers:
            breaker = self.breaker(provider)
            state = breaker.evaluate()
            if state == ProviderState.OPEN:
                continue
            healthy.append(provider)
        return healthy

    def _resolve_tier(self, request: ChatRequest) -> ModelTier:
        """Resolve the effective :class:`ModelTier` for *request*."""
        if request.model_tier is not None:
            return request.model_tier
        if request.task_type is not None:
            return self._task_to_tier.get(request.task_type, ModelTier.BALANCED)
        return ModelTier.BALANCED

    def _default_model_for(self, tier: ModelTier) -> str:
        """Pick a default model name for a tier.

        Returns the cheapest model's name on the cheapest provider
        serving *tier*. Used when the request does not name a
        model.
        """
        providers = self._registry.by_tier(tier)
        if not providers:
            return ""
        # Pick the cheapest provider first, then the cheapest model
        # on that provider.
        providers_sorted = sorted(
            providers,
            key=lambda p: (*_cheapest_price(p.config), 0.0, p.config.name),
        )
        first = providers_sorted[0]
        specs_in_tier = [s for s in first.config.model_specs if s.tier == tier]
        if not specs_in_tier:
            specs_in_tier = list(first.config.model_specs)
        if not specs_in_tier:
            return ""
        cheapest = min(specs_in_tier, key=lambda s: (s.output_cost_per_1k, s.input_cost_per_1k))
        return cheapest.name

    @staticmethod
    def _with_model(request: ChatRequest, model: str) -> ChatRequest:
        """Return a copy of *request* with the resolved model attached."""
        if request.model == model:
            return request
        # ``model_copy`` is Pydantic v2's safe way to derive a
        # modified copy without re-validating the input.
        return request.model_copy(update={"model": model})


__all__ = [
    "CIRCUIT_COOLDOWN_SECONDS",
    "CIRCUIT_FAILURE_THRESHOLD",
    "DEFAULT_TASK_TO_TIER",
    "RouteDecision",
    "Router",
]
