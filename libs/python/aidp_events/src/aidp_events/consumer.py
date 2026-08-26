"""``consume_events`` — the platform's unified event-consume entry point.

The function subscribes to a topic on behalf of *group_id*, dispatches
each record to the user-supplied handler, and guarantees:

- **at-least-once delivery** — the offset is committed only after the
  handler returns successfully. A crash between handler success and
  commit results in re-delivery on the next consume.
- **in-process retry** — when the handler raises, the same record is
  re-attempted up to ``max_retries`` times. After the budget is
  exhausted the record is forwarded to ``<topic>.dlq`` with diagnostic
  headers (``x-original-topic`` / ``x-retry-count`` / ``x-error-message``).
- **business idempotency** — the handler is called with
  ``(envelope, *, idempotency_key="<tenant_id>:<event_id>")`` so the
  service can dedupe by tenant + event id.

A poison-pill record (e.g. non-JSON value) is forwarded to the DLQ
immediately so the consumer never blocks on a single bad record.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Awaitable, Callable
from typing import Any, Protocol, runtime_checkable

from aidp_common.config import get_settings
from aidp_common.errors import UpstreamError

from aidp_events.envelope import EventEnvelope
from aidp_events.transport import Transport, WireMessage

_LOG = logging.getLogger(__name__)


@runtime_checkable
class Handler(Protocol):
    """Async event handler.

    Implementations are called as ``await handler(envelope, *,
    idempotency_key="...")``. The ``idempotency_key`` is the platform's
    recommended dedup key (``"{tenant_id}:{event_id}"``). Handlers that
    have not been updated to accept ``idempotency_key`` are still
    supported — :func:`consume_events` detects the signature and
    adapts the call.
    """

    async def __call__(
        self,
        envelope: EventEnvelope,
        *,
        idempotency_key: str,
    ) -> None: ...


# A bare ``async def`` is a valid ``Handler`` too; the alias below is
# a convenience for type hints. The PlatformHandler type uses ``**kwargs``
# so that any callable — regardless of whether it declares
# ``idempotency_key`` — is assignable. :func:`consume_events` adapts
# the call based on the runtime signature.
AsyncHandler = Callable[[EventEnvelope], Awaitable[None]]
IdempotentAsyncHandler = Callable[[EventEnvelope, str], Awaitable[None]]
# The universal type used in public APIs: any ``async def`` with at
# least one positional arg is assignable. ``**kwargs`` absorbs the
# optional ``idempotency_key`` keyword so mypy does not reject
# implementations that don't declare it.
PlatformHandler = Callable[..., Awaitable[None]]


def _default_transport_singleton() -> Transport:
    from aidp_events.kafka_transport import KafkaTransport

    settings = get_settings()
    return KafkaTransport(
        bootstrap_servers=settings.kafka_brokers,
        client_id=settings.service_name,
    )


_default_transport: Transport | None = None


def set_default_transport(transport: Transport | None) -> None:
    """Override the process-wide default transport (used by tests)."""
    global _default_transport
    _default_transport = transport


def get_default_transport() -> Transport:
    global _default_transport
    if _default_transport is None:
        _default_transport = _default_transport_singleton()
    return _default_transport


def _handler_accepts_idempotency_key(handler: Callable[..., Any]) -> bool:
    """Return ``True`` if *handler* declares an ``idempotency_key`` kwarg.

    The check handles both ``async def`` and ``def`` handlers; it looks
    at the signature, not the function type. The platform's
    recommended handler signature is::

        async def handler(envelope: EventEnvelope, *, idempotency_key: str) -> None: ...
    """
    try:
        sig = inspect.signature(handler)
    except (TypeError, ValueError):  # pragma: no cover - C callables
        return False
    return "idempotency_key" in sig.parameters


async def _dispatch_handler(
    handler: PlatformHandler,
    envelope: EventEnvelope,
    idempotency_key: str,
    *,
    accepts_key: bool,
) -> None:
    """Call *handler* with the right signature.

    If the handler declares ``idempotency_key``, the key is passed as a
    keyword argument. Otherwise the bare-envelope signature is used.
    """
    if accepts_key:
        await handler(envelope, idempotency_key=idempotency_key)
    else:
        await handler(envelope)


async def _send_to_dlq(
    transport: Transport,
    *,
    original_topic: str,
    record: WireMessage,
    envelope: EventEnvelope | None,
    error: BaseException,
    retry_count: int,
) -> None:
    """Forward a poison-pill or exhausted-retry record to ``<topic>.dlq``.

    The original *value* / *key* are preserved verbatim so an operator
    can inspect (or replay) the record. Diagnostic headers
    (``x-original-topic`` / ``x-retry-count`` / ``x-error-message``) are
    added on top of any user-supplied headers.
    """
    dlq_topic = f"{original_topic}.dlq"
    headers: list[tuple[str, bytes]] = [
        *record.headers,
        ("x-original-topic", original_topic.encode("utf-8")),
        ("x-retry-count", str(retry_count).encode("utf-8")),
        ("x-error-message", str(error).encode("utf-8")),
    ]
    if envelope is not None:
        # Surface the parsed event id in a structured header so a
        # downstream consumer can dedupe or alert.
        headers.append(("x-event-id", envelope.event_id.encode("utf-8")))
        headers.append(("x-tenant-id", envelope.tenant_id.encode("utf-8")))
    key_str = record.key.decode("utf-8") if record.key is not None else ""
    try:
        await transport.send_raw(
            topic=dlq_topic,
            key=key_str,
            value=record.value,
            headers=headers,
        )
    except Exception as exc:
        _LOG.error(
            "consume_events DLQ forward failed",
            extra={
                "topic": original_topic,
                "dlq_topic": dlq_topic,
                "error": str(exc),
            },
        )
        raise UpstreamError(
            "Consumer DLQ forward failed",
            details={
                "topic": original_topic,
                "dlq_topic": dlq_topic,
                "forward_error": str(exc),
            },
        ) from exc


async def consume_events(
    topic: str,
    group_id: str,
    handler: PlatformHandler,
    *,
    transport: Transport | None = None,
    max_retries: int = 3,
    poll_timeout: float = 0.1,
    auto_offset_reset: str = "latest",
) -> None:
    """Consume records from *topic* as part of *group_id*.

    Args:
        topic: Source Kafka topic.
        group_id: Consumer group id. Multiple consumers in the same
            group share partitions; consumers in different groups
            receive independent copies of every record.
        handler: Async callable invoked per record. Two signatures are
            supported:

            - ``async def handler(envelope) -> None`` — basic.
            - ``async def handler(envelope, *, idempotency_key) -> None``
              — recommended; the platform passes
              ``f"{tenant_id}:{event_id}"`` so the handler can dedupe
              at the business layer.

        transport: Transport instance. Defaults to the process-wide
            :class:`KafkaTransport`; tests inject the in-memory variant.
        max_retries: Per-record in-process retry budget. ``3`` means
            the handler is called at most 3 times before the record is
            DLQ'd and the offset advanced.
        poll_timeout: Seconds to wait for a record before yielding to
            the event loop (default ``0.1``). Lower values reduce
            shutdown latency; higher values reduce idle CPU.
        auto_offset_reset: ``"latest"`` (default, production) skips
            records that arrived before the group was created;
            ``"earliest"`` (typical for tests + replay scenarios)
            replays them.

    The coroutine runs until cancelled. The intended usage is::

        task = asyncio.create_task(consume_events(...))
        # ... later, in FastAPI lifespan shutdown ...
        task.cancel()
    """
    if not topic:
        raise ValueError("topic must be a non-empty string")
    if not group_id:
        raise ValueError("group_id must be a non-empty string")
    if max_retries < 1:
        raise ValueError("max_retries must be >= 1")
    if auto_offset_reset not in {"latest", "earliest"}:
        raise ValueError(
            f"auto_offset_reset must be 'latest' or 'earliest', got {auto_offset_reset!r}"
        )

    transport = transport or get_default_transport()
    if not getattr(transport, "_started", True):
        start = getattr(transport, "start", None)
        if callable(start):
            await start()

    accepts_key = _handler_accepts_idempotency_key(handler)
    _LOG.info(
        "consume_events started",
        extra={
            "topic": topic,
            "group_id": group_id,
            "max_retries": max_retries,
            "idempotent_handler": accepts_key,
            "auto_offset_reset": auto_offset_reset,
        },
    )

    try:
        async for record in transport.consume(
            group_id=group_id,
            topics=[topic],
            poll_timeout=poll_timeout,
            auto_offset_reset=auto_offset_reset,
        ):
            await _process_record(
                transport=transport,
                group_id=group_id,
                topic=topic,
                record=record,
                handler=handler,
                accepts_key=accepts_key,
                max_retries=max_retries,
            )
    except asyncio.CancelledError:
        _LOG.info(
            "consume_events cancelled",
            extra={"topic": topic, "group_id": group_id},
        )
        raise


async def _process_record(
    *,
    transport: Transport,
    group_id: str,
    topic: str,
    record: WireMessage,
    handler: PlatformHandler,
    accepts_key: bool,
    max_retries: int,
) -> None:
    """Dispatch *record* to *handler* with retry / DLQ on failure."""
    # 1. Try to parse the envelope. A poison pill is forwarded to the
    # DLQ immediately so the consumer never blocks on a single bad
    # record.
    envelope: EventEnvelope | None = None
    try:
        envelope = EventEnvelope.model_validate_json(record.value)
    except Exception as exc:
        _LOG.warning(
            "consume_events: invalid envelope, forwarding to DLQ",
            extra={
                "topic": topic,
                "offset": record.offset,
                "error": str(exc),
            },
        )
        await _send_to_dlq(
            transport,
            original_topic=topic,
            record=record,
            envelope=None,
            error=exc,
            retry_count=0,
        )
        # Commit past the poison pill so we don't re-deliver it.
        await transport.commit(
            group_id=group_id,
            topic=record.topic,
            partition=record.partition,
            offset=record.offset + 1,
        )
        return

    idempotency_key = f"{envelope.tenant_id}:{envelope.event_id}"
    last_exc: BaseException | None = None

    # 2. In-process retry. We re-invoke the handler up to ``max_retries``
    # times for the same record before giving up. The retry count is
    # tracked in-process — we do not re-publish to the source topic
    # (that would create an infinite loop on permanent failure).
    for attempt in range(1, max_retries + 1):
        try:
            await _dispatch_handler(
                handler,
                envelope,
                idempotency_key,
                accepts_key=accepts_key,
            )
            # Success — commit the offset so a re-consume (e.g. after
            # a crash) does not re-deliver this record.
            await transport.commit(
                group_id=group_id,
                topic=record.topic,
                partition=record.partition,
                offset=record.offset + 1,
            )
            return
        except Exception as exc:
            last_exc = exc
            _LOG.warning(
                "consume_events: handler raised, retrying",
                extra={
                    "topic": topic,
                    "event_id": envelope.event_id,
                    "tenant_id": envelope.tenant_id,
                    "attempt": attempt,
                    "max_retries": max_retries,
                    "error": str(exc),
                },
            )
            # Linear backoff for the in-process retry: 0ms, 50ms, 100ms.
            # This is a no-op on the first attempt; subsequent attempts
            # yield to the event loop so cancellation propagates.
            if attempt < max_retries:
                await asyncio.sleep(0.05 * attempt)

    # 3. Out of retries. Forward to DLQ so an operator can inspect
    # the record and the original error.
    _LOG.error(
        "consume_events: exhausted retries, forwarding to DLQ",
        extra={
            "topic": topic,
            "event_id": envelope.event_id,
            "tenant_id": envelope.tenant_id,
            "retries": max_retries,
            "last_error": str(last_exc),
        },
    )
    await _send_to_dlq(
        transport,
        original_topic=topic,
        record=record,
        envelope=envelope,
        error=last_exc or RuntimeError("handler raised without exception"),
        retry_count=max_retries,
    )
    # Commit past the DLQ'd record so we don't re-deliver it forever.
    await transport.commit(
        group_id=group_id,
        topic=record.topic,
        partition=record.partition,
        offset=record.offset + 1,
    )


__all__ = [
    "AsyncHandler",
    "Handler",
    "IdempotentAsyncHandler",
    "PlatformHandler",
    "consume_events",
    "get_default_transport",
    "set_default_transport",
]
