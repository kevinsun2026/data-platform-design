"""OpenAI-compat provider — the gateway's only wire-protocol implementation.

The OpenAI /v1/chat/completions protocol is the de-facto lingua franca
for hosted LLMs. OpenAI itself, DeepSeek, Together, Groq, vLLM, llama.cpp's
server mode, and Anthropic's beta compat endpoint all speak it (more or
less). This module is a single ``httpx.AsyncClient``-backed client
that can target any of them by changing ``ProviderConfig.base_url``.

The implementation deliberately avoids a dependency on the official
``openai`` SDK:

- The SDK pins a specific version of ``httpx`` that conflicts with
  the rest of the platform.
- The wire format is small and stable; the SDK's value-add (retries,
  pagination) is already covered by the router + circuit breaker.
- The test suite patches ``httpx`` directly (via ``respx``) to fake
  upstream responses; a thin httpx client makes that trivial.

Streaming
---------

When ``ChatRequest.stream`` is ``True`` the provider opens an
``httpx.AsyncClient.stream("POST", url, ...)`` and yields one
:class:`ChatChunk` per SSE event. When ``False`` it opens a normal
``POST`` and yields a single chunk with the full response. The router
treats both modes uniformly — the API layer is the only place that
needs to know whether to wrap the response in ``StreamingResponse``.
"""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict

from aidp_agent.providers.base import (
    ChatChunk,
    ChatMessage,
    ChatRequest,
    ProviderConfig,
    ProviderError,
    ProviderHealth,
    ProviderState,
    ProviderTransientError,
    TokenUsage,
)

_LOG = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Wire shapes (private — only the translator below touches them)
# ---------------------------------------------------------------------------


class _WireMessage(BaseModel):
    """One element of the upstream ``messages`` array."""

    model_config = ConfigDict(extra="ignore")

    role: str
    content: Any
    name: str | None = None


class _WireRequest(BaseModel):
    """The minimal OpenAI-compat request body the gateway sends.

    Most fields are forwarded verbatim. The ``stream`` flag is set by
    the translator based on :attr:`ChatRequest.stream`; the upstream
    always sees the canonical OpenAI shape.
    """

    model_config = ConfigDict(extra="ignore")

    model: str
    messages: list[_WireMessage]
    temperature: float | None = None
    max_tokens: int | None = None
    stream: bool = False
    # The "user" field tags the upstream call with a stable identifier
    # for abuse detection; we set it to the tenant id + user id.
    user: str | None = None


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class OpenAICompatProvider:
    """An LLMProvider that talks the OpenAI /v1/chat/completions wire protocol.

    Every concrete "provider" in the gateway is a configured instance
    of this class: the only differences between OpenAI, Anthropic-via-compat,
    DeepSeek, and a local vLLM are ``base_url`` and ``api_key``, which
    live on :class:`ProviderConfig`.

    The client is intentionally state-light: it owns one
    :class:`httpx.AsyncClient` (created lazily and closed via
    :meth:`aclose`) and the immutable :class:`ProviderConfig`. The
    circuit breaker lives in :mod:`aidp_agent.router` so the provider
    remains a pure pass-through.
    """

    #: Public attribute the router inspects.
    config: ProviderConfig

    def __init__(self, config: ProviderConfig, *, client: httpx.AsyncClient | None = None) -> None:
        self.config = config
        self._client: httpx.AsyncClient | None = client
        # ``_owns_client`` distinguishes an injected test client (whose
        # lifecycle is owned by the test) from a runtime-created
        # client (whose lifecycle is owned by ``aclose``).
        self._owns_client: bool = client is None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def aclose(self) -> None:
        """Close the underlying ``httpx.AsyncClient`` (if owned by us)."""
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None:
            if not self._owns_client:
                raise RuntimeError(
                    "OpenAICompatProvider has no client and does not own one; "
                    "pass a client at construction time"
                )
            self._client = httpx.AsyncClient(
                base_url=self.config.base_url,
                timeout=httpx.Timeout(self.config.timeout_seconds),
                headers={
                    "Authorization": f"Bearer {self.config.api_key}",
                    "Content-Type": "application/json",
                },
            )
        return self._client

    # ------------------------------------------------------------------
    # LLMProvider interface
    # ------------------------------------------------------------------

    async def count_tokens(self, text: str, model: str) -> int:
        """Estimate the token count of *text* for *model*.

        The OpenAI-compat protocol does not expose a token-count
        endpoint, so we use a cheap heuristic: ~4 characters per token
        for English / Latin scripts, with a +20% safety margin to
        account for non-Latin scripts (CJK averages ~1.5 chars/token).
        The estimate is *only* used for cost previews and metering
        fallbacks; the authoritative count comes from the upstream
        ``usage`` block on the response.
        """
        # ``max(1, ...)`` ensures a non-empty string still counts as
        # at least one token (an upstream that returned 0 would skew
        # the cost report).
        return max(1, int(len(text) / 3.2))

    async def health(self) -> ProviderHealth:
        """Return a synthetic "healthy" snapshot.

        The provider itself does not run a circuit breaker; that lives
        in :class:`aidp_agent.router.Router`. The health method is
        here so the router can poll for a *provider-level* status
        (e.g. a static "this provider is admin-disabled" flag) without
        branching on provider type. Subclasses can override to
        surface real upstream health.
        """
        return ProviderHealth(
            state=ProviderState.CLOSED,
            consecutive_failures=0,
            opened_at=None,
            cooldown_seconds=0.0,
        )

    def chat(self, request: ChatRequest) -> AsyncIterator[ChatChunk]:
        """Return an async iterator of chat-completion chunks.

        Non-streaming calls return a single-element iterator; streaming
        calls yield one :class:`ChatChunk` per SSE event. The caller
        is expected to drive the iterator with ``async for``.
        """
        if request.stream:
            return self._stream_chat(request)
        return self._collect_chat(request)

    # ------------------------------------------------------------------
    # Internal: translate + send
    # ------------------------------------------------------------------

    def _to_wire(self, request: ChatRequest, *, resolved_model: str) -> _WireRequest:
        """Project :class:`ChatRequest` onto the OpenAI wire shape."""
        messages: list[_WireMessage] = []
        for msg in request.messages:
            messages.append(
                _WireMessage(
                    role=msg.role,
                    content=msg.content,
                    name=msg.name,
                )
            )
        user_tag: str | None = None
        if request.user_id is not None:
            user_tag = f"{request.tenant_id}:{request.user_id}"
        return _WireRequest(
            model=resolved_model,
            messages=messages,
            temperature=request.temperature,
            max_tokens=request.max_tokens,
            stream=request.stream,
            user=user_tag,
        )

    async def _collect_chat(self, request: ChatRequest) -> AsyncIterator[ChatChunk]:
        """Non-streaming chat: POST and yield one terminal chunk."""
        if request.model is None:
            raise ProviderError(
                "non-streaming chat requires an explicit model",
                provider=self.config.name,
            )
        wire = self._to_wire(request, resolved_model=request.model)
        client = self._ensure_client()
        try:
            response = await client.post(
                "/chat/completions",
                json=wire.model_dump(exclude_none=True),
            )
        except httpx.TimeoutException as exc:
            raise ProviderTransientError(
                f"upstream timeout: {exc}",
                provider=self.config.name,
            ) from exc
        except httpx.HTTPError as exc:
            raise ProviderTransientError(
                f"upstream network error: {exc}",
                provider=self.config.name,
            ) from exc
        self._raise_for_status(response)
        body = response.json()
        chunk = self._build_non_streaming_chunk(body)
        yield chunk

    async def _stream_chat(self, request: ChatRequest) -> AsyncIterator[ChatChunk]:
        """Streaming chat: open a streaming POST and yield one chunk per SSE event."""
        if request.model is None:
            raise ProviderError(
                "streaming chat requires an explicit model",
                provider=self.config.name,
            )
        wire = self._to_wire(request, resolved_model=request.model)
        wire_dict = wire.model_dump(exclude_none=True)
        client = self._ensure_client()
        # ``httpx.AsyncClient.stream`` returns a context manager; we
        # use ``send`` + a manual read so we can parse SSE line by line
        # without buffering the whole response.
        try:
            request_obj = client.build_request(
                "POST",
                "/chat/completions",
                json=wire_dict,
            )
            response = await client.send(request_obj, stream=True)
        except httpx.TimeoutException as exc:
            raise ProviderTransientError(
                f"upstream timeout: {exc}",
                provider=self.config.name,
            ) from exc
        except httpx.HTTPError as exc:
            raise ProviderTransientError(
                f"upstream network error: {exc}",
                provider=self.config.name,
            ) from exc
        try:
            if response.status_code >= 400:
                # Read the body so we can include it in the error.
                body_text = await response.aread()
                await response.aclose()
                raise self._build_status_error(response.status_code, body_text)
            chunk_id = ""
            chunk_model = request.model
            final_usage: TokenUsage | None = None
            finish_reason: str | None = None
            buffer = ""
            async for raw in response.aiter_text():
                buffer += raw
                # SSE events are separated by a blank line.
                while "\n\n" in buffer:
                    event, buffer = buffer.split("\n\n", 1)
                    for line in event.splitlines():
                        line = line.strip()
                        if not line or not line.startswith("data:"):
                            continue
                        payload = line[len("data:") :].strip()
                        if payload == "[DONE]":
                            break
                        try:
                            data = json.loads(payload)
                        except json.JSONDecodeError:
                            _LOG.warning(
                                "ignoring malformed SSE payload",
                                extra={"provider": self.config.name, "payload": payload[:200]},
                            )
                            continue
                        chunk_id = data.get("id", chunk_id)
                        chunk_model = data.get("model", chunk_model)
                        usage_block = data.get("usage")
                        if usage_block:
                            final_usage = TokenUsage.model_validate(usage_block)
                        choices = data.get("choices") or []
                        if choices:
                            choice = choices[0]
                            delta = choice.get("delta") or {}
                            content = delta.get("content") or ""
                            finish_reason = choice.get("finish_reason") or finish_reason
                            if content or finish_reason:
                                yield ChatChunk(
                                    id=chunk_id,
                                    model=chunk_model,
                                    delta=content,
                                    finish_reason=finish_reason,
                                    usage=final_usage,
                                    raw=data,
                                )
            # Some providers send a trailing [DONE] event without a
            # final ``choices`` block; if so the loop has already
            # yielded the last content chunk and we just need to
            # attach the accumulated usage.
            if final_usage is not None and finish_reason is not None:
                yield ChatChunk(
                    id=chunk_id,
                    model=chunk_model,
                    delta="",
                    finish_reason=finish_reason,
                    usage=final_usage,
                    raw={},
                )
        finally:
            await response.aclose()

    # ------------------------------------------------------------------
    # Internal: response → ChatChunk
    # ------------------------------------------------------------------

    def _build_non_streaming_chunk(self, body: dict[str, Any]) -> ChatChunk:
        """Project a non-streaming JSON body onto a single :class:`ChatChunk`."""
        choices = body.get("choices") or []
        delta = ""
        finish_reason: str | None = None
        if choices:
            choice = choices[0]
            message = choice.get("message") or {}
            delta = message.get("content") or ""
            finish_reason = choice.get("finish_reason")
        usage_block = body.get("usage")
        usage: TokenUsage | None = None
        if usage_block:
            usage = TokenUsage.model_validate(usage_block)
        return ChatChunk(
            id=str(body.get("id", "")),
            model=str(
                body.get(
                    "model",
                    self.config.model_specs[0].name if self.config.model_specs else "unknown",
                )
            ),
            delta=delta,
            finish_reason=finish_reason,
            usage=usage,
            raw=body,
        )

    # ------------------------------------------------------------------
    # Internal: error mapping
    # ------------------------------------------------------------------

    def _raise_for_status(self, response: httpx.Response) -> None:
        """Raise the right exception for a non-2xx response."""
        if response.status_code < 400:
            return
        # Read the body so it can be surfaced in the error.
        try:
            body_text = response.text
        except Exception:  # pragma: no cover - defensive
            body_text = ""
        raise self._build_status_error(response.status_code, body_text)

    def _build_status_error(self, status_code: int, body_text: str | bytes) -> ProviderError:
        """Map an HTTP status code onto the right exception class."""
        body_str = (
            body_text.decode("utf-8", errors="replace")
            if isinstance(body_text, bytes)
            else body_text
        )
        # 4xx → permanent; 5xx → transient. 408 (Request Timeout) and
        # 429 (Too Many Requests) are transient even though they're
        # 4xx: they signal a back-off-able condition.
        if status_code in (408, 429) or status_code >= 500:
            return ProviderTransientError(
                f"upstream {status_code}: {body_str[:200]}",
                provider=self.config.name,
                status_code=status_code,
                body=body_str,
            )
        return ProviderError(
            f"upstream {status_code}: {body_str[:200]}",
            provider=self.config.name,
            status_code=status_code,
            body=body_str,
        )


__all__ = ["OpenAICompatProvider"]


# ``ChatMessage`` and ``ChatRequest`` are re-exported via
# :mod:`aidp_agent.providers.__init__`; this module only needs them as
# typing aids.
_ = (ChatMessage, ChatRequest)
