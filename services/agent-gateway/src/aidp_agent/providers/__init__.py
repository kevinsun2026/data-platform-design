"""Provider package for the Agent Gateway.

This package is the single source of truth for talking to upstream LLMs.
The default implementation — :class:`OpenAICompatProvider` in
:mod:`aidp_agent.providers.openai_compat` — handles every OpenAI-compat
vendor (OpenAI, Anthropic via the compat shim, DeepSeek, local vLLM,
...). A future Anthropic-native provider would land in a sibling
module without changing the public surface.
"""

from __future__ import annotations

from aidp_agent.providers.base import (
    BaseProvider,
    ChatChunk,
    ChatMessage,
    ChatRequest,
    LLMProvider,
    ModelTier,
    ProviderConfig,
    ProviderError,
    ProviderHealth,
    ProviderState,
    ProviderTransientError,
    TaskType,
    TokenUsage,
)
from aidp_agent.providers.openai_compat import OpenAICompatProvider
from aidp_agent.providers.registry import ProviderRegistry, build_default_registry

__all__ = [
    "BaseProvider",
    "ChatChunk",
    "ChatMessage",
    "ChatRequest",
    "LLMProvider",
    "ModelTier",
    "OpenAICompatProvider",
    "ProviderConfig",
    "ProviderError",
    "ProviderHealth",
    "ProviderRegistry",
    "ProviderState",
    "ProviderTransientError",
    "TaskType",
    "TokenUsage",
    "build_default_registry",
]
