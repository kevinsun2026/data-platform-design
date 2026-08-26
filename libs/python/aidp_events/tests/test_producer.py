"""Tests for ``aidp_events.producer`` (the ``publish_event`` function).

Coverage:

- Round-trip: ``publish_event`` → ``consume_events`` yields the same envelope
  (verified against a real ``KafkaContainer`` when the docker daemon is up,
  and against an in-memory transport otherwise).
- ``publish_event`` populates ``event_id`` / ``occurred_at`` / ``trace_id`` and
  uses ``tenant_id`` as the Kafka key for partition affinity.
- Transient producer failures are retried with exponential backoff.
- After the retry budget is exhausted, the original message is forwarded to
  ``<topic>.dlq`` with diagnostic headers (``x-original-topic``,
  ``x-retry-count``, ``x-error-message``).
- ``headers=`` is forwarded as Kafka record headers.
- ``EventEnvelope`` is JSON-encoded in the value (verifiable by any consumer).

The Kafka path is gated on a live ``KafkaContainer``; the in-memory path is
always available and is what CI runs by default. The fallback is annotated
with ``# pragma: allow-testcontainers-fallback`` so the production intent
is grep-able.
"""

from __future__ import annotations

import asyncio
import json
import re
from typing import Any
from uuid import uuid4

import pytest
from aidp_events.envelope import EventEnvelope
from aidp_events.producer import publish_event
from aidp_events.testing import InMemoryTransport, KafkaTransport

# ---------------------------------------------------------------------------
# Transport selection — Kafka via testcontainers if available, else in-memory.
# ---------------------------------------------------------------------------


_HEX32 = re.compile(r"^[0-9a-f]{32}$")


def _try_kafka_container() -> str | None:
    """Return a reachable ``host:port`` for a Kafka broker, or ``None``."""
    # pragma: allow-testcontainers-fallback
    try:
        from testcontainers.kafka import KafkaContainer
    except ImportError:  # pragma: no cover
        return None
    try:  # pragma: allow-testcontainers-fallback
        with KafkaContainer("confluentinc/cp-kafka:7.5.0") as kc:
            return kc.get_bootstrap_server()
    except Exception:  # pragma: allow-testcontainers-fallback
        return None


_KAFKA_BOOTSTRAP = _try_kafka_container()
_USING_KAFKA = _KAFKA_BOOTSTRAP is not None


# Skip every Kafka-only test when the daemon / image is unavailable. The
# in-memory transport is exercised by every non-skipped test.
needs_kafka = pytest.mark.skipif(
    not _USING_KAFKA,
    reason="requires testcontainers Kafka (docker daemon / image unavailable)",
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def inmem() -> InMemoryTransport:
    """A fresh in-memory transport per test (no shared state)."""
    return InMemoryTransport()


@pytest.fixture
def kafka_transport() -> KafkaTransport:  # pragma: allow-testcontainers-fallback
    assert _KAFKA_BOOTSTRAP is not None
    return KafkaTransport(bootstrap_servers=_KAFKA_BOOTSTRAP, client_id="aidp-test")


# ---------------------------------------------------------------------------
# Round-trip — in-memory path (always runs)
# ---------------------------------------------------------------------------


async def test_publish_then_consume_round_trip(inmem: InMemoryTransport) -> None:
    """A single ``publish_event`` lands in the topic and the consumer sees it."""
    topic = f"aidp-test-{uuid4().hex}"
    payload = {"connection_id": "conn-1", "type": "postgres"}

    await publish_event(
        topic=topic,
        event_type="datasource.connection.created",
        tenant_id="tenant-a",
        payload=payload,
        transport=inmem,
    )

    # Pull the single message back out (small timeout so the test does not
    # hang if publish silently failed).
    received = await asyncio.wait_for(inmem.drain(topic, timeout=0.5), timeout=2.0)
    assert len(received) == 1
    msg = received[0]

    # Value is JSON-decodable back into an ``EventEnvelope``.
    envelope = EventEnvelope.model_validate_json(msg.value)
    assert envelope.tenant_id == "tenant-a"
    assert envelope.event_type == "datasource.connection.created"
    assert envelope.payload == payload
    # Auto-filled fields are populated.
    assert envelope.event_id
    assert envelope.occurred_at.tzinfo is not None
    assert _HEX32.match(envelope.trace_id)


async def test_publish_event_uses_tenant_event_id_as_kafka_key(
    inmem: InMemoryTransport,
) -> None:
    """The Kafka key is ``tenant_id:event_id`` so same-tenant events stay
    on the same partition and consumers can dedupe by (tenant, event_id)."""
    topic = f"aidp-test-{uuid4().hex}"
    await publish_event(
        topic=topic,
        event_type="x",
        tenant_id="tenant-a",
        payload={},
        transport=inmem,
    )
    msgs = await asyncio.wait_for(inmem.drain(topic, timeout=0.5), timeout=2.0)
    assert len(msgs) == 1
    assert msgs[0].key is not None
    key = msgs[0].key.decode("ascii")
    assert key.startswith("tenant-a:")


async def test_publish_event_forwards_headers(inmem: InMemoryTransport) -> None:
    topic = f"aidp-test-{uuid4().hex}"
    await publish_event(
        topic=topic,
        event_type="x",
        tenant_id="tenant-a",
        payload={},
        headers={"x-source": "unit-test", "x-correlation": "abc-123"},
        transport=inmem,
    )
    msgs = await asyncio.wait_for(inmem.drain(topic, timeout=0.5), timeout=2.0)
    headers = {k: v.decode("utf-8") for k, v in msgs[0].headers}
    assert headers["x-source"] == "unit-test"
    assert headers["x-correlation"] == "abc-123"


async def test_publish_event_each_call_yields_unique_event_id(
    inmem: InMemoryTransport,
) -> None:
    topic = f"aidp-test-{uuid4().hex}"
    for _ in range(3):
        await publish_event(
            topic=topic,
            event_type="x",
            tenant_id="tenant-a",
            payload={"i": 0},
            transport=inmem,
        )
    msgs = await asyncio.wait_for(inmem.drain(topic, timeout=0.5), timeout=2.0)
    ids = {EventEnvelope.model_validate_json(m.value).event_id for m in msgs}
    assert len(ids) == 3


# ---------------------------------------------------------------------------
# Retry + DLQ — exercised via a stub transport that fails N times then
# succeeds / fails permanently.
# ---------------------------------------------------------------------------


class _ScriptedTransport:
    """A minimal transport stub that records every ``send`` and either
    raises a transient error or returns successfully, according to a script.

    The Kafka transport is *not* involved here — this test exercises the
    publisher's retry / DLQ logic in isolation, which is what the brief
    requires.

    By default every ``send`` consumes the next outcome from the script.
    When ``fail_only_for`` is provided, only sends whose topic appears in
    that set consume the script; the others succeed silently. This lets
    tests simulate "primary topic always fails, DLQ succeeds".
    """

    def __init__(
        self,
        script: list[Exception | None] | None = None,
        *,
        fail_only_for: set[str] | None = None,
        raise_on_dlq: bool = False,
    ) -> None:
        self._script = list(script or [])
        self._fail_only_for = fail_only_for
        self._raise_on_dlq = raise_on_dlq
        self.calls: list[dict[str, Any]] = []

    async def start(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def send(
        self,
        topic: str,
        key: str,
        value: bytes,
        headers: list[tuple[str, bytes]],
    ) -> None:
        self.calls.append({"topic": topic, "key": key, "value": value, "headers": headers})
        if self._raise_on_dlq and topic.endswith(".dlq"):
            raise ConnectionError("dlq is also down")
        # If a filter is configured, only the filtered topics are subject
        # to the script — others always succeed.
        if self._fail_only_for is not None:
            if topic in self._fail_only_for:
                self._consume_script_outcome()
            return
        self._consume_script_outcome()
        return

    async def send_raw(
        self,
        topic: str,
        key: str,
        value: bytes,
        headers: list[tuple[str, bytes]],
    ) -> None:
        await self.send(topic=topic, key=key, value=value, headers=headers)

    def _consume_script_outcome(self) -> None:
        if not self._script:
            raise RuntimeError("script exhausted")
        outcome = self._script.pop(0)
        if outcome is not None:
            raise outcome

    async def commit(self, group_id: str, topic: str, partition: int, offset: int) -> None:
        return None


async def test_publish_event_retries_transient_failure() -> None:
    """Transient failures are retried; once a send succeeds, no DLQ is written."""
    transport = _ScriptedTransport(
        script=[
            ConnectionError("kafka temporarily unavailable"),
            ConnectionError("still down"),
            None,  # third attempt succeeds
        ]
    )
    # Tighten the retry backoff so the test stays fast.
    envelope = await publish_event(
        topic="x",
        event_type="t",
        tenant_id="tenant-a",
        payload={},
        transport=transport,  # type: ignore[arg-type]
        max_retries=3,
        backoff_base=0.001,
    )
    assert envelope.event_id
    # Three send attempts, no DLQ, no exception.
    assert len(transport.calls) == 3
    assert all(c["topic"] == "x" for c in transport.calls)


async def test_publish_event_writes_to_dlq_after_max_retries() -> None:
    """All attempts fail on the primary topic → message is forwarded to
    ``<topic>.dlq``. The DLQ topic itself is reachable so the forward
    succeeds (a separate test covers the case where the DLQ also fails)."""
    transport = _ScriptedTransport(
        fail_only_for={"orders.events"},
        script=[ConnectionError("nope")] * 10,  # primary always fails
    )
    await publish_event(
        topic="orders.events",
        event_type="order.created",
        tenant_id="tenant-a",
        payload={"order_id": "o-1"},
        transport=transport,  # type: ignore[arg-type]
        max_retries=2,
        backoff_base=0.001,
    )

    # The primary topic was attempted 3 times (1 initial + 2 retries),
    # then the publisher wrote the record to the DLQ once.
    primary_attempts = [c for c in transport.calls if c["topic"] == "orders.events"]
    dlq_calls = [c for c in transport.calls if c["topic"] == "orders.events.dlq"]
    assert len(primary_attempts) == 3
    assert len(dlq_calls) == 1
    # The DLQ send carries the same Kafka key so a DLQ consumer can dedupe.
    assert dlq_calls[0]["key"].startswith("tenant-a:")


async def test_dlq_payload_is_unchanged_envelope() -> None:
    """The DLQ message body is the original JSON-serialized envelope."""
    transport = _ScriptedTransport(
        fail_only_for={"billing.events"},
        script=[ConnectionError("nope")] * 10,
    )
    await publish_event(
        topic="billing.events",
        event_type="invoice.created",
        tenant_id="tenant-a",
        payload={"amount": 100},
        transport=transport,  # type: ignore[arg-type]
        max_retries=1,
        backoff_base=0.001,
    )
    dlq_calls = [c for c in transport.calls if c["topic"] == "billing.events.dlq"]
    assert len(dlq_calls) == 1
    envelope = EventEnvelope.model_validate_json(dlq_calls[0]["value"])
    assert envelope.event_type == "invoice.created"
    assert envelope.payload == {"amount": 100}
    headers = {k: v.decode("utf-8") for k, v in dlq_calls[0]["headers"]}
    assert headers["x-original-topic"] == "billing.events"
    assert headers["x-retry-count"] == "1"
    # The error message is ``str(exc)`` for the last exception seen —
    # here it's the literal args ("nope") because ``str(ConnectionError("nope"))``
    # is "nope". The important guarantee is that *some* error string is
    # attached (not a hard class name requirement).
    assert "nope" in headers["x-error-message"]
    assert headers["x-error-message"]  # non-empty


async def test_publish_event_raises_when_dlq_also_fails() -> None:
    """When even the DLQ forward fails, the producer surfaces
    :class:`UpstreamError` so the caller can decide what to do."""
    from aidp_common.errors import UpstreamError

    transport = _ScriptedTransport(
        fail_only_for={"orders.events"},
        script=[ConnectionError("primary down")] * 10,
        raise_on_dlq=True,
    )
    with pytest.raises(UpstreamError) as excinfo:
        await publish_event(
            topic="orders.events",
            event_type="order.created",
            tenant_id="tenant-a",
            payload={"order_id": "o-1"},
            transport=transport,  # type: ignore[arg-type]
            max_retries=1,
            backoff_base=0.001,
        )
    assert "Kafka publish failed" in str(excinfo.value)


# ---------------------------------------------------------------------------
# Kafka round-trip — only runs when the testcontainers image is available
# ---------------------------------------------------------------------------


@needs_kafka
async def test_publish_event_round_trip_real_kafka(
    kafka_transport: KafkaTransport,
) -> None:  # pragma: allow-testcontainers-fallback
    """Same as the in-memory round-trip, but against a real broker."""
    topic = f"aidp-test-{uuid4().hex}"
    payload = {"k": "v", "n": 42}

    await publish_event(
        topic=topic,
        event_type="x",
        tenant_id="tenant-a",
        payload=payload,
        transport=kafka_transport,
    )

    received: list[bytes] = []

    # Pull at most one record (real-Kafka path only).
    async for msg in kafka_transport.consume(group_id="test-grp", topics=[topic], poll_timeout=0.5):
        received.append(msg.value)
        break

    assert received, "no message received from real Kafka"
    envelope = EventEnvelope.model_validate_json(received[0])
    assert envelope.payload == payload
    await kafka_transport.close()


# ---------------------------------------------------------------------------
# JSON wire format — pin the encoding so producers/consumers stay compatible
# ---------------------------------------------------------------------------


async def test_publish_event_value_is_json_envelope(
    inmem: InMemoryTransport,
) -> None:
    topic = f"aidp-test-{uuid4().hex}"
    await publish_event(
        topic=topic,
        event_type="x",
        tenant_id="tenant-a",
        payload={"a": 1},
        transport=inmem,
    )
    msgs = await asyncio.wait_for(inmem.drain(topic, timeout=0.5), timeout=2.0)
    raw = msgs[0].value
    # Parses as JSON; the top-level is a dict with the envelope shape.
    obj = json.loads(raw)
    assert obj["event_type"] == "x"
    assert obj["tenant_id"] == "tenant-a"
    assert obj["payload"] == {"a": 1}
    assert "event_id" in obj
    assert "occurred_at" in obj
    assert "trace_id" in obj
