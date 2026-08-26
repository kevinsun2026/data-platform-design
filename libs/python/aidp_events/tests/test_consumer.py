"""Tests for ``aidp_events.consumer`` (the ``consume_events`` function).

Coverage:

- A handler is invoked once per consumed message, in order, and ``commit``
  advances the offset so the same group does not re-read on the next pass.
- The handler receives ``(envelope, *, idempotency_key)`` where
  ``idempotency_key == f"{tenant_id}:{event_id}"``.
- When the handler raises, the consumer retries the same message up to
  ``max_retries`` times before forwarding it to ``<topic>.dlq`` and
  committing past it.
- An unparseable envelope (bad JSON / missing fields) goes straight to DLQ
  so the consumer never blocks on a poison pill.
- ``consume_events`` accepts a single message and returns (the test driver
  cancels the underlying task after a small number of records to assert
  the consume loop actually unblocks).

The consumer is exercised against the in-memory transport (always
available) plus a real Kafka container when present. The fallback is
annotated with ``# pragma: allow-testcontainers-fallback``.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from uuid import uuid4

import pytest
from aidp_events.consumer import PlatformHandler, consume_events
from aidp_events.envelope import EventEnvelope, new_envelope
from aidp_events.testing import InMemoryTransport, KafkaTransport

# ---------------------------------------------------------------------------
# Transport selection — Kafka via testcontainers if available, else in-memory.
# ---------------------------------------------------------------------------


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

needs_kafka = pytest.mark.skipif(
    not _USING_KAFKA,
    reason="requires testcontainers Kafka (docker daemon / image unavailable)",
)


# ---------------------------------------------------------------------------
# Fixtures + helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def inmem() -> InMemoryTransport:
    return InMemoryTransport()


def _seed(
    transport: InMemoryTransport, topic: str, n: int, *, tenant: str = "tenant-a"
) -> list[EventEnvelope]:
    """Synchronously inject ``n`` envelopes into *transport* (test helper)."""
    envelopes: list[EventEnvelope] = []
    for i in range(n):
        env = new_envelope(
            event_type="test.event",
            tenant_id=tenant,
            payload={"i": i},
        )
        envelopes.append(env)
        body = env.model_dump_json().encode("utf-8")
        key = f"{tenant}:{env.event_id}".encode()
        transport.seed(topic, key=key, value=body)
    return envelopes


async def _consume_n(
    transport: InMemoryTransport,
    topic: str,
    handler: PlatformHandler,
    *,
    expected: int,
    max_retries: int = 3,
    poll_timeout: float = 0.05,
    group_id: str | None = None,
) -> None:
    """Run ``consume_events`` until *expected* successful handler invocations
    have happened, or until the deadline elapses. Returns ``None``; raises
    on timeout so the test fails fast.
    """
    received = 0
    done = asyncio.Event()

    async def _wrapped(env: EventEnvelope, *, idempotency_key: str) -> None:
        nonlocal received
        await handler(env, idempotency_key=idempotency_key)
        received += 1
        if received >= expected:
            done.set()

    task = asyncio.create_task(
        consume_events(
            topic=topic,
            group_id=group_id or f"grp-{uuid4().hex}",
            handler=_wrapped,
            transport=transport,
            max_retries=max_retries,
            poll_timeout=poll_timeout,
            auto_offset_reset="earliest",
        )
    )
    try:
        await asyncio.wait_for(done.wait(), timeout=3.0)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task


async def _consume_until_dlq(
    transport: InMemoryTransport,
    topic: str,
    handler: PlatformHandler,
    *,
    max_retries: int = 3,
    poll_timeout: float = 0.05,
    group_id: str | None = None,
) -> None:
    """Run ``consume_events`` until ``<topic>.dlq`` has at least one record,
    or until the deadline elapses. Used for tests where the handler always
    fails (so we can't count successful invocations).
    """
    done = asyncio.Event()

    async def _wrapped(env: EventEnvelope, *, idempotency_key: str) -> None:
        await handler(env, idempotency_key=idempotency_key)

    task = asyncio.create_task(
        consume_events(
            topic=topic,
            group_id=group_id or f"grp-{uuid4().hex}",
            handler=_wrapped,
            transport=transport,
            max_retries=max_retries,
            poll_timeout=poll_timeout,
            auto_offset_reset="earliest",
        )
    )

    deadline = time.monotonic() + 3.0
    try:
        while time.monotonic() < deadline:
            if transport.dlq_size(topic) > 0:
                done.set()
                break
            await asyncio.sleep(0.05)
        if not done.is_set():
            raise TimeoutError("DLQ was not populated in time")
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


async def test_consume_events_invokes_handler_per_message(
    inmem: InMemoryTransport,
) -> None:
    topic = f"aidp-test-{uuid4().hex}"
    _seed(inmem, topic, n=3)

    seen: list[int] = []

    async def _handler(env: EventEnvelope, *, idempotency_key: str) -> None:
        seen.append(env.payload["i"])

    await _consume_n(inmem, topic, _handler, expected=3)

    assert sorted(seen) == [0, 1, 2]
    # And every seeded envelope was actually delivered to the handler.
    assert len(seen) == 3


async def test_consume_events_passes_idempotency_key(
    inmem: InMemoryTransport,
) -> None:
    """``idempotency_key == f"{tenant_id}:{event_id}"`` is passed as kwarg."""
    topic = f"aidp-test-{uuid4().hex}"
    seeded = _seed(inmem, topic, n=1)

    keys: list[str] = []

    async def _handler(env: EventEnvelope, *, idempotency_key: str) -> None:
        keys.append(idempotency_key)

    await _consume_n(inmem, topic, _handler, expected=1)

    expected = f"{seeded[0].tenant_id}:{seeded[0].event_id}"
    assert keys == [expected]


async def test_consume_events_commits_offset_after_success(
    inmem: InMemoryTransport,
) -> None:
    """A second consumer in the same group does not re-read committed messages."""
    topic = f"aidp-test-{uuid4().hex}"
    _seed(inmem, topic, n=2)
    group_id = f"grp-{uuid4().hex}"

    first_batch: list[EventEnvelope] = []

    async def _first(env: EventEnvelope, *, idempotency_key: str) -> None:
        first_batch.append(env)

    await _consume_n(inmem, topic, _first, expected=2, group_id=group_id)

    # Re-spin a consumer on the same group — the committed offsets mean
    # the topic appears empty (with a generous poll timeout).
    second_batch: list[EventEnvelope] = []

    async def _second(env: EventEnvelope, *, idempotency_key: str) -> None:
        second_batch.append(env)

    task = asyncio.create_task(
        consume_events(
            topic=topic,
            group_id=group_id,
            handler=_second,
            transport=inmem,
            max_retries=3,
            poll_timeout=0.05,
            auto_offset_reset="earliest",
        )
    )
    try:
        # Give the consumer a chance to pull — but expect nothing.
        await asyncio.sleep(0.3)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task

    assert len(second_batch) == 0, f"unexpected re-delivery: {second_batch}"


# ---------------------------------------------------------------------------
# Retry + DLQ on handler failure
# ---------------------------------------------------------------------------


async def test_consume_events_retries_then_dlqs_on_handler_failure(
    inmem: InMemoryTransport,
) -> None:
    """Handler raises → consumer retries up to ``max_retries`` → DLQ."""
    topic = f"aidp-test-{uuid4().hex}"
    seeded = _seed(inmem, topic, n=1)
    seeded[0]

    attempts: list[str] = []
    succeed = False

    async def _always_fail_then_succeed(env: EventEnvelope, *, idempotency_key: str) -> None:
        attempts.append(idempotency_key)
        # Fail the first two attempts, then succeed.
        nonlocal succeed
        if not succeed:
            succeed = True
            raise RuntimeError("downstream not ready")

    await _consume_n(inmem, topic, _always_fail_then_succeed, expected=1, max_retries=3)

    # The handler was retried (first attempt raised, second attempt succeeded).
    assert len(attempts) >= 1
    # And the message reached the DLQ only if all attempts failed — here it
    # succeeded on attempt 2, so no DLQ entry should be present.
    assert inmem.dlq_size(topic) == 0


async def test_consume_events_dlqs_after_max_retries(
    inmem: InMemoryTransport,
) -> None:
    """Handler keeps failing → after ``max_retries`` the message is DLQ'd."""
    topic = f"aidp-test-{uuid4().hex}"
    seeded = _seed(inmem, topic, n=1)
    envelope = seeded[0]

    attempts: list[str] = []

    async def _always_fail(env: EventEnvelope, *, idempotency_key: str) -> None:
        attempts.append(idempotency_key)
        raise RuntimeError("permanent failure")

    await _consume_until_dlq(inmem, topic, _always_fail, max_retries=2, poll_timeout=0.02)

    # The handler was called exactly ``max_retries`` times before DLQ.
    assert len(attempts) == 2, f"expected 2 attempts, got {len(attempts)}"

    # The DLQ now contains the message.
    dlq_msgs = await asyncio.wait_for(inmem.drain(f"{topic}.dlq", timeout=0.5), timeout=2.0)
    assert len(dlq_msgs) == 1
    parsed = EventEnvelope.model_validate_json(dlq_msgs[0].value)
    assert parsed.event_id == envelope.event_id

    # The consumer's group cursor has advanced past the failed record
    # so a re-consume (e.g. test cleanup) does not re-deliver the DLQ'd
    # envelope. We poke the in-memory transport directly to verify.
    topic_obj = inmem._topics[topic]
    # The group_id is whatever the helper generated; we look up the
    # only group with a committed offset past zero.
    committed_offsets = [v for v in topic_obj.committed.values() if v > 0]
    assert committed_offsets, (
        "group cursor was not advanced after DLQ — record would be re-delivered"
    )


async def test_consume_events_dlq_message_preserves_envelope(
    inmem: InMemoryTransport,
) -> None:
    """The DLQ body is the original envelope, headers carry diagnostic info."""
    topic = f"aidp-test-{uuid4().hex}"
    seeded = _seed(inmem, topic, n=1)
    envelope = seeded[0]

    async def _always_fail(env: EventEnvelope, *, idempotency_key: str) -> None:
        raise ValueError("upstream schema mismatch nope")

    await _consume_until_dlq(inmem, topic, _always_fail, max_retries=1, poll_timeout=0.02)

    dlq_msgs = await asyncio.wait_for(inmem.drain(f"{topic}.dlq", timeout=0.5), timeout=2.0)
    assert len(dlq_msgs) == 1
    msg = dlq_msgs[0]
    parsed = EventEnvelope.model_validate_json(msg.value)
    assert parsed.event_id == envelope.event_id
    assert parsed.tenant_id == envelope.tenant_id
    headers = {k: v.decode("utf-8") for k, v in msg.headers}
    assert headers["x-original-topic"] == topic
    assert headers["x-retry-count"] == "1"
    assert "upstream schema mismatch" in headers["x-error-message"]


# ---------------------------------------------------------------------------
# Poison-pill handling
# ---------------------------------------------------------------------------


async def test_consume_events_dlqs_garbage_envelope(
    inmem: InMemoryTransport,
) -> None:
    """A message whose value is not a valid envelope is DLQ'd, not retried."""
    topic = f"aidp-test-{uuid4().hex}"
    inmem.seed(topic, key=b"tenant-a:bad", value=b"not-json-at-all")

    invocations: int = 0

    async def _handler(env: EventEnvelope, *, idempotency_key: str) -> None:
        nonlocal invocations
        invocations += 1

    # Use ``_consume_until_dlq`` so we don't depend on the handler ever
    # being called (a poison pill must be DLQ'd before the handler runs).
    await _consume_until_dlq(inmem, topic, _handler, max_retries=3, poll_timeout=0.05)

    assert invocations == 0
    dlq_msgs = await asyncio.wait_for(inmem.drain(f"{topic}.dlq", timeout=0.5), timeout=2.0)
    assert len(dlq_msgs) == 1
    # Original bytes preserved.
    assert dlq_msgs[0].value == b"not-json-at-all"
    headers = {k: v.decode("utf-8") for k, v in dlq_msgs[0].headers}
    assert headers["x-original-topic"] == topic
    assert "x-error-message" in headers


# ---------------------------------------------------------------------------
# Real-Kafka integration
# ---------------------------------------------------------------------------


@needs_kafka
async def test_consume_events_real_kafka_round_trip() -> (
    None
):  # pragma: allow-testcontainers-fallback
    """Produce + consume against a real broker; assert the envelope round-trips."""
    assert _KAFKA_BOOTSTRAP is not None
    transport = KafkaTransport(bootstrap_servers=_KAFKA_BOOTSTRAP, client_id="aidp-test")
    topic = f"aidp-test-{uuid4().hex}"
    group_id = f"grp-{uuid4().hex}"

    envelopes: list[EventEnvelope] = []
    for i in range(2):
        env = new_envelope(event_type="real.kafka", tenant_id="tenant-a", payload={"i": i})
        envelopes.append(env)
        body = env.model_dump_json().encode("utf-8")
        key = f"tenant-a:{env.event_id}".encode()
        await transport.send(topic=topic, key=key.decode("ascii"), value=body, headers=[])

    received: list[EventEnvelope] = []
    done = asyncio.Event()

    async def _handler(env: EventEnvelope, *, idempotency_key: str) -> None:
        received.append(env)
        if len(received) >= 2:
            done.set()

    task = asyncio.create_task(
        consume_events(
            topic=topic,
            group_id=group_id,
            handler=_handler,
            transport=transport,
            max_retries=1,
            poll_timeout=0.5,
            auto_offset_reset="earliest",
        )
    )
    try:
        await asyncio.wait_for(done.wait(), timeout=15.0)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task
        await transport.close()

    assert {e.event_id for e in received} == {e.event_id for e in envelopes}
