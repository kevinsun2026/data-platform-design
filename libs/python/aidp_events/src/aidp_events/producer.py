"""``publish_event`` — the platform's unified event-publish entry point.

The function:

1. Builds an :class:`EventEnvelope` with the required platform fields
   (``event_id`` / ``occurred_at`` / ``trace_id``) populated.
2. Serializes it to JSON and publishes via the supplied transport.
3. Retries on transient failures with exponential backoff.
4. After the retry budget is exhausted, forwards the original message
   to ``<topic>.dlq`` with diagnostic headers — the original payload
   is never lost.

The transport is dependency-injected so tests can use the in-memory
transport and production code can use ``aiokafka`` without changing
the call site.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from aidp_common.config import get_settings
from aidp_common.errors import UpstreamError

from aidp_events.envelope import EventEnvelope, new_envelope
from aidp_events.transport import Transport

_LOG = logging.getLogger(__name__)


def _default_transport_singleton() -> Transport:
    """Resolve the process-wide transport.

    Lazy import to avoid a circular dependency on :mod:`aidp_events.kafka_transport`
    (which in turn imports from this module's package). The function
    returns a freshly-built :class:`KafkaTransport` pointed at
    ``AIDP_KAFKA_BROKERS``; the caller is responsible for invoking
    ``await transport.start()`` before publishing.
    """
    from aidp_events.kafka_transport import KafkaTransport

    settings = get_settings()
    return KafkaTransport(
        bootstrap_servers=settings.kafka_brokers,
        client_id=settings.service_name,
    )


# Process-wide default transport. Set by ``set_default_transport`` (mostly
# useful in tests) and lazily initialized on first use.
_default_transport: Transport | None = None


def set_default_transport(transport: Transport | None) -> None:
    """Override the process-wide default transport (used by tests).

    Passing ``None`` resets the lazy initialization.
    """
    global _default_transport
    _default_transport = transport


def get_default_transport() -> Transport:
    """Return the process-wide default transport, building it if needed."""
    global _default_transport
    if _default_transport is None:
        _default_transport = _default_transport_singleton()
    return _default_transport


async def publish_event(
    topic: str,
    event_type: str,
    payload: dict[str, Any],
    *,
    tenant_id: str | None = None,
    producer: str | None = None,
    event_version: int = 1,
    headers: dict[str, str] | None = None,
    transport: Transport | None = None,
    max_retries: int = 3,
    backoff_base: float = 0.1,
    backoff_cap: float = 5.0,
) -> EventEnvelope:
    """Publish an event to *topic* with retry + DLQ semantics.

    Args:
        topic: Destination Kafka topic.
        event_type: Reverse-DNS-style event name
            (``"datasource.connection.created"``).
        payload: Business payload (JSON-compatible dict).
        tenant_id: Tenant id; pulled from :func:`aidp_db.tenant.get_tenant_id`
            if not provided.
        producer: Producer name; defaults to ``AIDP_SERVICE_NAME``.
        event_version: Integer schema version of *payload* (default 1).
        headers: Free-form record headers.
        transport: Transport instance. Defaults to the process-wide
            :class:`KafkaTransport`; tests inject the in-memory variant.
        max_retries: Number of additional attempts after the first send
            (default ``3`` → 1 initial + 3 retries = 4 attempts total).
        backoff_base: First backoff in seconds (default 0.1s). Subsequent
            backoffs double up to *backoff_cap*.
        backoff_cap: Maximum backoff in seconds (default 5s).

    Returns:
        The :class:`EventEnvelope` that was actually sent to the
        broker (with auto-populated ``event_id`` / ``occurred_at`` /
        ``trace_id``). Useful for tests and downstream tracing.

    Raises:
        aidp_common.errors.UpstreamError: When even the DLQ forward
            fails. This is the only failure path that propagates to
            the caller — the original event is preserved on the DLQ
            whenever the broker is reachable.
    """
    if not topic:
        raise ValueError("topic must be a non-empty string")
    if not event_type:
        raise ValueError("event_type must be a non-empty string")
    if max_retries < 0:
        raise ValueError("max_retries must be >= 0")

    if tenant_id is None:
        # Pull the tenant from the request-scoped context set by
        # ``aidp_db.tenant.set_tenant_context`` (typically called by
        # the auth middleware). Falls back to ``None`` for admin /
        # platform-internal events that have no tenant.
        from aidp_db.tenant import get_tenant_id

        tenant_id = get_tenant_id()
    if tenant_id is None:
        raise ValueError(
            "tenant_id is required: pass it explicitly or set a tenant context "
            "via aidp_db.tenant.set_tenant_context(...) before publishing"
        )

    envelope = new_envelope(
        event_type=event_type,
        tenant_id=tenant_id,
        payload=payload,
        producer=producer,
        event_version=event_version,
        headers=headers,
    )

    transport = transport or get_default_transport()
    if not getattr(transport, "_started", True):
        # Best-effort lazy start for the in-memory transport. Real
        # Kafka requires the caller to ``start()`` during the service
        # lifespan; we only start the fake automatically.
        start = getattr(transport, "start", None)
        if callable(start):
            await start()

    kafka_key = f"{tenant_id}:{envelope.event_id}"
    record_headers: list[tuple[str, bytes]] = [
        (name, value.encode("utf-8")) for name, value in envelope.headers.items()
    ]
    value_bytes = envelope.model_dump_json().encode("utf-8")

    last_exc: BaseException | None = None
    for attempt in range(max_retries + 1):
        try:
            await transport.send(
                topic=topic,
                key=kafka_key,
                value=value_bytes,
                headers=record_headers,
            )
            _LOG.info(
                "event published",
                extra={
                    "topic": topic,
                    "event_id": envelope.event_id,
                    "event_type": event_type,
                    "tenant_id": tenant_id,
                    "attempt": attempt + 1,
                },
            )
            return envelope
        except Exception as exc:
            last_exc = exc
            if attempt < max_retries:
                backoff = min(backoff_cap, backoff_base * (2**attempt))
                _LOG.warning(
                    "publish_event retrying after error",
                    extra={
                        "topic": topic,
                        "event_id": envelope.event_id,
                        "attempt": attempt + 1,
                        "backoff": backoff,
                        "error": str(exc),
                    },
                )
                await asyncio.sleep(backoff)

    # All attempts failed. Forward to the DLQ — the original envelope
    # is preserved, plus diagnostic headers so an operator can
    # reconstruct what happened.
    dlq_topic = f"{topic}.dlq"
    dlq_headers: list[tuple[str, bytes]] = [
        *record_headers,
        ("x-original-topic", topic.encode("utf-8")),
        ("x-retry-count", str(max_retries).encode("utf-8")),
        ("x-error-message", str(last_exc).encode("utf-8")),
    ]
    try:
        await transport.send(
            topic=dlq_topic,
            key=kafka_key,
            value=value_bytes,
            headers=dlq_headers,
        )
    except Exception as exc:
        _LOG.error(
            "publish_event DLQ forward failed",
            extra={
                "topic": topic,
                "dlq_topic": dlq_topic,
                "event_id": envelope.event_id,
                "error": str(exc),
            },
        )
        raise UpstreamError(
            "Kafka publish failed and DLQ forward also failed",
            details={
                "topic": topic,
                "dlq_topic": dlq_topic,
                "event_id": envelope.event_id,
                "publish_error": str(last_exc),
                "dlq_error": str(exc),
            },
        ) from exc

    _LOG.error(
        "publish_event exhausted retries; forwarded to DLQ",
        extra={
            "topic": topic,
            "dlq_topic": dlq_topic,
            "event_id": envelope.event_id,
            "retries": max_retries,
            "last_error": str(last_exc),
        },
    )
    return envelope


__all__ = ["get_default_transport", "publish_event", "set_default_transport"]
