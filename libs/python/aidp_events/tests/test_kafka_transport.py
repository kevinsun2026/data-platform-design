"""Tests for ``aidp_events.kafka_transport.KafkaTransport``.

The transport wraps ``aiokafka``; here we test its lifecycle and
serialization logic by injecting fake ``AIOKafkaProducer`` /
``AIOKafkaConsumer`` substitutes through the private
``_producer_factory`` / ``_consumer_factory`` hooks. Real-broker tests
live in :mod:`tests.test_producer` /
:mod:`tests.test_consumer` (skipped when testcontainers is unavailable).

Coverage:

- :meth:`start` / :meth:`close` lifecycle (idempotency).
- :meth:`send` raises when not started; calls ``send_and_wait`` with the
  correct topic / key / value / headers.
- :meth:`consume` is an async generator that yields ``WireMessage``
  records and commits offsets per record (at-least-once).
- :meth:`commit` is a no-op (the real commit happens inline in
  :meth:`consume` because aiokafka's consumer holds the connection).
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncGenerator
from typing import Any

import pytest
from aidp_events.kafka_transport import KafkaTransport
from aidp_events.transport import WireMessage

# ---------------------------------------------------------------------------
# Fakes for aiokafka clients
# ---------------------------------------------------------------------------


class _FakeRecord:
    """Mimics ``aiokafka.ConsumerRecord`` for the consumer fake."""

    def __init__(
        self,
        *,
        topic: str,
        partition: int,
        offset: int,
        key: bytes | None,
        value: bytes,
        headers: list[tuple[str, bytes]] | None = None,
    ) -> None:
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.key = key
        self.value = value
        # aiokafka yields headers as list[tuple[str, bytes | str]]; we
        # normalize to bytes for the transport.
        self.headers = headers or []

    @property
    def topic_partition(self) -> Any:
        from aiokafka import TopicPartition  # type: ignore[import-untyped]

        return TopicPartition(self.topic, self.partition)


class _FakeProducer:
    """In-memory ``AIOKafkaProducer`` substitute.

    Records every :meth:`send_and_wait` invocation. The optional
    ``fail_first_n`` arg lets a test force N consecutive failures
    (e.g. for retry/DLQ coverage).
    """

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        client_id: str,
        fail_first_n: int = 0,
        raise_on_send: bool = False,
        **_kwargs: Any,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self._fail_first_n = fail_first_n
        self._raise_on_send = raise_on_send
        self.started = False
        self.stopped = False
        self.calls: list[dict[str, Any]] = []

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def send_and_wait(
        self,
        topic: str,
        value: bytes,
        key: bytes | None,
        headers: list[tuple[str, bytes]],
    ) -> None:
        self.calls.append({"topic": topic, "value": value, "key": key, "headers": list(headers)})
        if self._raise_on_send:
            raise ConnectionError("kafka down")
        if self._fail_first_n > 0:
            self._fail_first_n -= 1
            raise ConnectionError("transient")


class _FakeConsumer:
    """In-memory ``AIOKafkaConsumer`` substitute.

    Holds a queue of :class:`_FakeRecord` objects and yields them via
    ``getmany``. Each :meth:`commit` advances the committed offset
    so the next call reflects the new position.
    """

    def __init__(
        self,
        *topics: str,
        bootstrap_servers: str,
        group_id: str | None,
        client_id: str | None,
        auto_offset_reset: str,
        max_poll_records: int,
        request_timeout_ms: int,
        records: list[_FakeRecord] | None = None,
        **_kwargs: Any,
    ) -> None:
        self.topics = list(topics)
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.client_id = client_id
        self.auto_offset_reset = auto_offset_reset
        self.max_poll_records = max_poll_records
        self.request_timeout_ms = request_timeout_ms
        self._records: list[_FakeRecord] = list(records or [])
        self._cursor: dict[tuple[str, int], int] = {}
        self._commits: list[dict[tuple[str, int], int]] = []
        self.started = False
        self.stopped = False

    def push(self, record: _FakeRecord) -> None:
        """Append a record (test helper)."""
        self._records.append(record)

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    async def getmany(self, timeout_ms: int = 1000) -> dict[tuple[str, int], list[_FakeRecord]]:
        # ``getmany`` returns immediately with whatever is queued. We
        # return a single batch per call to keep the test simple.
        if not self._records:
            await asyncio.sleep(timeout_ms / 1000)
            return {}
        batch = self._records[: self.max_poll_records]
        del self._records[: self.max_poll_records]
        out: dict[tuple[str, int], list[_FakeRecord]] = {}
        for record in batch:
            tp = (record.topic, record.partition)
            out.setdefault(tp, []).append(record)
        return out

    async def commit(self, offsets: dict[Any, int]) -> None:
        # aiokafka's commit takes ``{TopicPartition: OffsetAndMetadata}``
        # in production, but the transport only passes ``{TopicPartition: int}``,
        # which is the simpler overload accepted by the lib.
        normalized: dict[tuple[str, int], int] = {}
        for tp, off in offsets.items():
            normalized[(tp.topic, tp.partition)] = off
        self._commits.append(normalized)


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


async def test_kafka_transport_start_is_idempotent() -> None:
    """``start()`` creates exactly one producer even when called twice."""
    created: list[_FakeProducer] = []

    def _factory(**kwargs: Any) -> _FakeProducer:
        p = _FakeProducer(**kwargs)
        created.append(p)
        return p

    transport = KafkaTransport(
        bootstrap_servers="localhost:9092",
        client_id="test",
        _producer_factory=_factory,
    )
    await transport.start()
    await transport.start()  # second call is a no-op
    assert len(created) == 1
    assert created[0].started is True
    await transport.close()
    assert created[0].stopped is True


async def test_kafka_transport_close_is_idempotent() -> None:
    transport = KafkaTransport(
        bootstrap_servers="localhost:9092",
        client_id="test",
        _producer_factory=_FakeProducer,
    )
    await transport.start()
    await transport.close()
    await transport.close()  # second call is a no-op
    assert transport._started is False


async def test_kafka_transport_send_requires_start() -> None:
    transport = KafkaTransport(
        bootstrap_servers="localhost:9092",
        client_id="test",
        _producer_factory=_FakeProducer,
    )
    with pytest.raises(RuntimeError, match="start"):
        await transport.send(topic="t", key="k", value=b"v", headers=[])


# ---------------------------------------------------------------------------
# send
# ---------------------------------------------------------------------------


async def test_kafka_transport_send_forwards_arguments() -> None:
    producer = _FakeProducer(bootstrap_servers="x", client_id="x")
    transport = KafkaTransport(
        bootstrap_servers="localhost:9092",
        client_id="test",
        _producer_factory=lambda **kwargs: producer,
    )
    await transport.start()
    await transport.send(
        topic="orders",
        key="tenant-a:evt-1",
        value=b'{"event_type":"x"}',
        headers=[("x-source", b"cli")],
    )
    assert len(producer.calls) == 1
    call = producer.calls[0]
    assert call["topic"] == "orders"
    assert call["key"] == b"tenant-a:evt-1"
    assert call["value"] == b'{"event_type":"x"}'
    assert call["headers"] == [("x-source", b"cli")]
    await transport.close()


async def test_kafka_transport_send_propagates_broker_error() -> None:
    """Transient producer errors bubble up so the producer's retry loop engages."""
    transport = KafkaTransport(
        bootstrap_servers="localhost:9092",
        client_id="test",
        _producer_factory=lambda **kwargs: _FakeProducer(
            bootstrap_servers=kwargs["bootstrap_servers"],
            client_id=kwargs["client_id"],
            raise_on_send=True,
        ),
    )
    await transport.start()
    with pytest.raises(ConnectionError):
        await transport.send(topic="t", key="k", value=b"v", headers=[])
    await transport.close()


# ---------------------------------------------------------------------------
# consume
# ---------------------------------------------------------------------------


async def test_kafka_transport_consume_yields_records_and_commits() -> None:
    """``consume`` is an async iterator that yields records and commits offsets."""
    records = [
        _FakeRecord(
            topic="orders",
            partition=0,
            offset=10,
            key=b"tenant-a:1",
            value=b'{"a": 1}',
            headers=[("x-source", b"cli")],
        ),
        _FakeRecord(
            topic="orders",
            partition=0,
            offset=11,
            key=b"tenant-a:2",
            value=b'{"a": 2}',
        ),
    ]
    consumer = _FakeConsumer(
        "orders",
        bootstrap_servers="x",
        group_id="g",
        client_id="c",
        auto_offset_reset="earliest",
        max_poll_records=500,
        request_timeout_ms=15000,
        records=records,
    )
    transport = KafkaTransport(
        bootstrap_servers="localhost:9092",
        client_id="test",
        auto_offset_reset="earliest",
        _consumer_factory=lambda *a, **kw: consumer,
    )

    delivered: list[WireMessage] = []
    # The transport returns an ``AsyncIterator`` (the contract); we cast
    # to ``AsyncGenerator`` so ``aclose()`` is available for clean teardown.
    gen: AsyncGenerator[WireMessage, None] = transport.consume(  # type: ignore[assignment]
        group_id="g", topics=["orders"], poll_timeout=0.01
    )

    # Use ``aclose()`` to deterministically tear the generator down:
    # this throws ``GeneratorExit`` into the suspended point, which
    # runs the transport's ``finally`` block (calling ``consumer.stop()``).
    # ``aclose`` returns once the gen is fully cleaned up, so the
    # assertions below see the final state.
    try:
        async for record in gen:
            delivered.append(record)
            if len(delivered) >= 2:
                break
    finally:
        await gen.aclose()

    assert [m.offset for m in delivered] == [10, 11]
    assert delivered[0].value == b'{"a": 1}'
    assert delivered[0].headers == [("x-source", b"cli")]
    # At-least-once: the post-yield ``commit()`` for record 10 ran
    # before the gen suspended on the second yield; the gen was
    # closed while suspended on the second yield, so the commit for
    # record 11 may or may not have run. The at-least-once contract
    # is "first commit has fired so a re-consume resumes at offset
    # 11, not 10".
    assert consumer._commits, "no commit was issued — at-least-once broken"
    first_commit = consumer._commits[0]
    assert first_commit == {("orders", 0): 11}, first_commit
    assert consumer.stopped is True  # finally block ran via aclose


async def test_kafka_transport_consume_no_records_blocks_then_returns() -> None:
    """When the consumer returns empty batches, ``consume`` waits via
    ``getmany`` (which itself sleeps ``timeout_ms``) and re-polls."""
    consumer = _FakeConsumer(
        "empty",
        bootstrap_servers="x",
        group_id="g",
        client_id="c",
        auto_offset_reset="latest",
        max_poll_records=500,
        request_timeout_ms=15000,
        records=[],
    )
    transport = KafkaTransport(
        bootstrap_servers="localhost:9092",
        client_id="test",
        _consumer_factory=lambda *a, **kw: consumer,
    )
    # Open the generator in a task. The transport's poll loop will
    # spin on the empty consumer. We cancel the task to trigger the
    # ``finally`` block, then explicitly close the generator so the
    # ``await consumer.stop()`` is awaited (cancellation does not
    # necessarily run synchronous code in the finally).
    gen: AsyncGenerator[WireMessage, None] = transport.consume(  # type: ignore[assignment]
        group_id="g", topics=["empty"], poll_timeout=0.02
    )

    async def _drain() -> None:
        async for _ in gen:
            pytest.fail("no records were expected")
        return

    task = asyncio.create_task(_drain())
    # Let the poll loop spin a few times.
    await asyncio.sleep(0.05)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    # The task is now done, but the gen may still be suspended. Call
    # ``aclose`` to deterministically run the transport's ``finally``
    # block (which calls ``consumer.stop()``).
    await gen.aclose()
    assert consumer.stopped is True


# ---------------------------------------------------------------------------
# commit
# ---------------------------------------------------------------------------


async def test_kafka_transport_commit_is_noop() -> None:
    """``commit`` is intentionally a no-op — see the class docstring."""
    transport = KafkaTransport(
        bootstrap_servers="localhost:9092",
        client_id="test",
        _producer_factory=_FakeProducer,
    )
    # Must not raise even though no consumer is running.
    await transport.commit(group_id="g", topic="t", partition=0, offset=42)
