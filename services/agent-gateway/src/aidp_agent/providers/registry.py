"""Process-wide registry of configured LLM providers.

The :class:`ProviderRegistry` is the small object the rest of the
codebase talks to when it needs an LLM. It:

- Holds a mapping from provider name to a configured
  :class:`OpenAICompatProvider` (or any other :class:`LLMProvider`
  implementation).
- Indexes the same providers by (a) the model names they serve and
  (b) their per-model :class:`ModelTier` so the router can do
  constant-time candidate lookup.
- Exposes a ``start()`` / ``stop()`` lifecycle so the FastAPI lifespan
  can wire provider clients up and down alongside the rest of the
  service.
- Provides a ``BYOK`` (Bring Your Own Key) override: a per-tenant
  ``{provider_name: api_key}`` mapping stored in the in-memory
  credentials map. Production deployments back this with the
  ``AgentCredentials`` table (see Task 12.3 schema below) but the
  in-memory cache is the source of truth at request time.
"""

from __future__ import annotations

import logging
import threading
from collections import defaultdict
from collections.abc import Iterable
from typing import Any

from aidp_agent.providers.base import (
    LLMProvider,
    ModelSpec,
    ModelTier,
    ProviderConfig,
)
from aidp_agent.providers.openai_compat import OpenAICompatProvider

_LOG = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Default provider catalogue
# ---------------------------------------------------------------------------


#: A baseline provider catalogue. Each entry mirrors the shape of the
#: YAML config the platform ships; the real production build loads the
#: same shape from ``AIDP_AGENT_PROVIDERS`` (a JSON env var) or a
#: secret-mounted file. The defaults below exist so the test suite
#: and local dev loops can run without any extra configuration.
DEFAULT_PROVIDER_CONFIGS: tuple[ProviderConfig, ...] = (
    ProviderConfig(
        name="openai",
        display_name="OpenAI",
        base_url="https://api.openai.com/v1",
        api_key="sk-openai-placeholder",
        model_specs=(
            ModelSpec("gpt-4o", ModelTier.FLAGSHIP, 0.005, 0.015),
            ModelSpec("gpt-4o-mini", ModelTier.BALANCED, 0.00015, 0.0006),
            ModelSpec("gpt-4.1-nano", ModelTier.ECONOMY, 0.0001, 0.0004),
        ),
    ),
    ProviderConfig(
        name="anthropic",
        display_name="Anthropic (OpenAI-compat)",
        base_url="https://api.anthropic.com/v1",
        api_key="sk-anthropic-placeholder",
        model_specs=(
            ModelSpec("claude-sonnet-4-20250514", ModelTier.FLAGSHIP, 0.003, 0.015),
            ModelSpec("claude-haiku-4-5", ModelTier.BALANCED, 0.0008, 0.004),
        ),
    ),
    ProviderConfig(
        name="deepseek",
        display_name="DeepSeek",
        base_url="https://api.deepseek.com/v1",
        api_key="sk-deepseek-placeholder",
        model_specs=(
            ModelSpec("deepseek-chat", ModelTier.ECONOMY, 0.00027, 0.0011),
            ModelSpec("deepseek-reasoner", ModelTier.BALANCED, 0.00055, 0.00219),
        ),
    ),
)


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


class ProviderRegistry:
    """In-process catalogue of configured providers.

    Thread-safety: the registry is guarded by a single lock because
    it is mutated at startup (load) and at BYOK-update time
    (rare, admin-only). The hot path — looking up a provider by
    name — acquires the lock only when reading the
    :data:`_by_name` / :data:`_by_model` dicts; we copy references
    under the lock and use them without holding it. The lock is
    short-lived and uncontended in steady state.
    """

    def __init__(self, providers: Iterable[LLMProvider] | None = None) -> None:
        self._lock = threading.RLock()
        self._providers: dict[str, LLMProvider] = {}
        # Index by model name → provider. A single model name may be
        # served by multiple providers (e.g. ``gpt-4o`` on OpenAI
        # and a local vLLM mirror). The router picks among them.
        self._by_model: dict[str, list[LLMProvider]] = defaultdict(list)
        # Index by tier → list of providers in that tier. The router
        # uses this when the request asks for a tier but not a model.
        self._by_tier: dict[ModelTier, list[LLMProvider]] = defaultdict(list)
        # Per-tenant BYOK override: tenant_id → {provider_name: api_key}.
        # The :meth:`resolve_api_key` hook returns the override when
        # present and falls back to the provider's configured key.
        self._byok: dict[str, dict[str, str]] = defaultdict(dict)
        if providers is not None:
            for provider in providers:
                self.register(provider)

    # ------------------------------------------------------------------
    # Build helpers
    # ------------------------------------------------------------------

    @classmethod
    def from_configs(
        cls,
        configs: Iterable[ProviderConfig],
        *,
        client_factory: Any = None,
    ) -> ProviderRegistry:
        """Build a registry from raw :class:`ProviderConfig` objects.

        Args:
            configs: One :class:`ProviderConfig` per provider to load.
            client_factory: Optional callable taking a :class:`ProviderConfig`
                and returning an ``httpx.AsyncClient`` (or compatible
                object). When ``None`` the providers create their own
                client lazily. Tests pass a ``respx``-backed factory
                so the upstream HTTP is mocked.
        """
        providers: list[LLMProvider] = []
        for cfg in configs:
            client = client_factory(cfg) if client_factory is not None else None
            providers.append(OpenAICompatProvider(cfg, client=client))
        return cls(providers=providers)

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

    def register(self, provider: LLMProvider) -> None:
        """Insert (or replace) *provider* in the registry."""
        with self._lock:
            self._providers[provider.config.name] = provider
            for spec in provider.config.model_specs:
                # Avoid duplicates if a provider is re-registered.
                if provider not in self._by_model[spec.name]:
                    self._by_model[spec.name].append(provider)
                tier_list = self._by_tier[spec.tier]
                if provider not in tier_list:
                    tier_list.append(provider)
            # Index the provider under its default tier too, so a
            # request that omits both model and task_type still finds
            # the provider when the default tier matches.
            if not any(
                spec.tier == provider.config.default_tier for spec in provider.config.model_specs
            ):
                tier_list = self._by_tier[provider.config.default_tier]
                if provider not in tier_list:
                    tier_list.append(provider)

    def unregister(self, name: str) -> None:
        """Remove the provider with the given *name* (no-op if absent)."""
        with self._lock:
            provider = self._providers.pop(name, None)
            if provider is None:
                return
            for spec in provider.config.model_specs:
                bucket = self._by_model.get(spec.name)
                if bucket and provider in bucket:
                    bucket.remove(provider)
                tier_bucket = self._by_tier.get(spec.tier)
                if tier_bucket and provider in tier_bucket:
                    tier_bucket.remove(provider)

    # ------------------------------------------------------------------
    # Lookup
    # ------------------------------------------------------------------

    def get(self, name: str) -> LLMProvider | None:
        """Return the provider registered as *name* (or ``None``)."""
        with self._lock:
            return self._providers.get(name)

    def all(self) -> list[LLMProvider]:
        """Return a snapshot list of every registered provider."""
        with self._lock:
            return list(self._providers.values())

    def by_model(self, model: str) -> list[LLMProvider]:
        """Return every provider that serves *model* (insertion order)."""
        with self._lock:
            return list(self._by_model.get(model, ()))

    def by_tier(self, tier: ModelTier) -> list[LLMProvider]:
        """Return every provider configured at *tier* (insertion order)."""
        with self._lock:
            return list(self._by_tier.get(tier, ()))

    # ------------------------------------------------------------------
    # BYOK
    # ------------------------------------------------------------------

    def set_byok(self, tenant_id: str, provider_name: str, api_key: str) -> None:
        """Set the per-tenant API key override for *provider_name*."""
        with self._lock:
            self._byok[tenant_id][provider_name] = api_key

    def clear_byok(self, tenant_id: str, provider_name: str) -> None:
        """Drop the per-tenant API key override (no-op if absent)."""
        with self._lock:
            bucket = self._byok.get(tenant_id)
            if bucket is not None:
                bucket.pop(provider_name, None)
                if not bucket:
                    self._byok.pop(tenant_id, None)

    def resolve_api_key(self, tenant_id: str, provider_name: str) -> str:
        """Return the API key to use for a call.

        Per-tenant BYOK override wins; falls back to the provider's
        configured key. The provider's own client picks up the key
        via :meth:`OpenAICompatProvider.set_api_key` (a test seam) or
        via the ``AIDP_AGENT_BYOK_HEADER`` request header.
        """
        with self._lock:
            override = self._byok.get(tenant_id, {}).get(provider_name)
        if override is not None:
            return override
        provider = self.get(provider_name)
        if provider is None:
            return ""
        return provider.config.api_key

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Bring every provider up.

        For :class:`OpenAICompatProvider` this is currently a no-op
        (the client is created lazily on first call). The method
        exists so a future provider that needs a warm-up probe
        (e.g. an Anthropic-native client that pings /v1/models) can
        hook in here without changing the registry contract.
        """
        _LOG.info(
            "agent-gateway provider registry started",
            extra={"provider_count": len(self.all())},
        )

    async def stop(self) -> None:
        """Tear every provider down."""
        providers = self.all()
        for provider in providers:
            aclose = getattr(provider, "aclose", None)
            if aclose is not None:
                try:
                    result = aclose()
                    # ``aclose`` may be a coroutine.
                    if hasattr(result, "__await__"):
                        await result
                except Exception:  # pragma: no cover - shutdown best-effort
                    _LOG.exception(
                        "error closing provider", extra={"provider": provider.config.name}
                    )
        with self._lock:
            self._providers.clear()
            self._by_model.clear()
            self._by_tier.clear()
        _LOG.info("agent-gateway provider registry stopped")


# ---------------------------------------------------------------------------
# Default factory
# ---------------------------------------------------------------------------


def build_default_registry() -> ProviderRegistry:
    """Build a :class:`ProviderRegistry` from :data:`DEFAULT_PROVIDER_CONFIGS`.

    Used by ``create_app`` in the absence of explicit configuration.
    Tests typically build a custom registry via
    :meth:`ProviderRegistry.from_configs` with mocked ``httpx`` clients.
    """
    return ProviderRegistry.from_configs(DEFAULT_PROVIDER_CONFIGS)


__all__ = [
    "DEFAULT_PROVIDER_CONFIGS",
    "ProviderRegistry",
    "build_default_registry",
]
