"""``aiokafka``-backed implementation of :class:`aidp_events.transport.Transport`.

This is the production transport. It is intentionally thin: it owns one
:class:`aiokafka.AIOKafkaProducer` (shared across ``send`` calls) and
spawns a fresh :class:`aiokafka.AIOKafkaConsumer` per ``consume()`` call
(each consumer holds its own broker connection — typical Kafka pattern).

The class is safe to instantiate at import time; the underlying client
is created lazily on :meth:`start` so the constructor does not block.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer  # type: ignore[import-untyped]

from aidp_events.transport import WireMessage

_LOG = logging.getLogger(__name__)


class KafkaTransport:
    """``aiokafka``-backed :class:`Transport` implementation.

    Args:
        bootstrap_servers: Comma-separated Kafka broker list
            (``"host1:9092,host2:9092"``).
        client_id: Client identifier attached to every Kafka request
            (helps broker-side debugging).
        consumer_max_poll_records: ``max_poll_records`` hint for the
            consumer. Defaults to ``500``.

    The transport is process-wide; a single instance is enough to back
    every producer / consumer in one service. ``start()`` and
    ``close()`` are idempotent and safe to call from FastAPI
    ``lifespan``.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        client_id: str = "aidp-events",
        consumer_max_poll_records: int = 500,
        auto_offset_reset: str = "latest",
        *,
        _producer_factory: Any = None,
        _consumer_factory: Any = None,
    ) -> None:
        """Build a new :class:`KafkaTransport`.

        Args:
            bootstrap_servers: Comma-separated Kafka broker list.
            client_id: Client id attached to every request.
            consumer_max_poll_records: ``max_poll_records`` hint.
            auto_offset_reset: ``"latest"`` (default) or ``"earliest"``;
                applied to brand-new consumer groups.
            _producer_factory: **Test-only.** Callable that returns a
                producer instance (``AIOKafkaProducer``-compatible).
                Defaults to :class:`AIOKafkaProducer`.
            _consumer_factory: **Test-only.** Callable that returns a
                consumer instance (``AIOKafkaConsumer``-compatible).
                Defaults to :class:`AIOKafkaConsumer`.
        """
        self._bootstrap_servers = bootstrap_servers
        self._client_id = client_id
        self._consumer_max_poll_records = consumer_max_poll_records
        self._auto_offset_reset = auto_offset_reset
        self._producer_factory = _producer_factory or AIOKafkaProducer
        self._consumer_factory = _consumer_factory or AIOKafkaConsumer
        self._producer: Any = None
        self._lock = asyncio.Lock()
        self._started = False

    # -- lifecycle ---------------------------------------------------------

    async def start(self) -> None:
        """Connect the shared producer. Idempotent."""
        async with self._lock:
            if self._started:
                return
            producer = self._producer_factory(
                bootstrap_servers=self._bootstrap_servers,
                client_id=self._client_id,
                acks="all",
                enable_idempotence=True,
                linger_ms=5,
                request_timeout_ms=15_000,
            )
            await producer.start()
            self._producer = producer
            self._started = True
            _LOG.info(
                "kafka transport started",
                extra={"bootstrap": self._bootstrap_servers, "client_id": self._client_id},
            )

    async def close(self) -> None:
        """Stop the shared producer. Idempotent."""
        async with self._lock:
            if not self._started:
                return
            assert self._producer is not None
            try:
                await self._producer.stop()
            finally:
                self._producer = None
                self._started = False
                _LOG.info("kafka transport closed")

    # -- send --------------------------------------------------------------

    async def send(
        self,
        topic: str,
        key: str,
        value: bytes,
        headers: list[tuple[str, bytes]],
    ) -> None:
        """Publish a single record. Raises on broker unavailability."""
        if not self._started or self._producer is None:
            raise RuntimeError("KafkaTransport.start() must be called before send()")
        key_bytes = key.encode("utf-8") if key is not None else None
        # ``send_and_wait`` blocks until the broker acknowledges. With
        # ``acks="all"`` and ``enable_idempotence=True`` this gives us
        # at-least-once + dedupe-on-retry on the producer side.
        await self._producer.send_and_wait(
            topic=topic,
            value=value,
            key=key_bytes,
            headers=list(headers),
        )

    async def send_raw(
        self,
        topic: str,
        key: str,
        value: bytes,
        headers: list[tuple[str, bytes]],
    ) -> None:
        """``send`` without retry. Used by the consumer's DLQ forward path."""
        # Same implementation as :meth:`send`; the retry/DLQ wrapping
        # lives in the producer module, not in the transport.
        await self.send(topic=topic, key=key, value=value, headers=headers)

    # -- consume -----------------------------------------------------------

    async def commit(self, group_id: str, topic: str, partition: int, offset: int) -> None:
        """``commit`` is a no-op for this transport — the consumer
        commits inline when it iterates (see :meth:`consume`).

        The signature is here to satisfy the :class:`Transport` protocol.
        For aiokafka, offset commits happen against the *consumer* owned
        by the running task, not against a process-wide handle, so the
        no-op is correct: the consumer is the only one that can commit.
        """
        # Intentional no-op. The actual commit is performed in
        # :meth:`consume` via ``consumer.commit({TopicPartition: ...})``.
        del group_id, topic, partition, offset

    def consume(
        self,
        group_id: str,
        topics: list[str],
        *,
        poll_timeout: float = 0.1,
        auto_offset_reset: str | None = None,
    ) -> AsyncIterator[WireMessage]:
        """Yield records from *topics* for *group_id*.

        The consumer starts at the last committed offset; new groups
        fall back to ``auto_offset_reset`` (default ``"latest"`` —
        only new messages). Offset commits happen in-line (after every
        record), giving at-least-once delivery.
        ``enable_auto_commit=False`` is mandatory so the handler error
        path does not accidentally ack a failed record.
        """
        reset = auto_offset_reset or self._auto_offset_reset
        return self._consume(
            group_id=group_id,
            topics=topics,
            poll_timeout=poll_timeout,
            auto_offset_reset=reset,
        )

    async def _consume(
        self,
        group_id: str,
        topics: list[str],
        *,
        poll_timeout: float,
        auto_offset_reset: str,
    ) -> AsyncIterator[WireMessage]:
        consumer = self._consumer_factory(
            *topics,
            bootstrap_servers=self._bootstrap_servers,
            group_id=group_id,
            client_id=f"{self._client_id}-{group_id}",
            enable_auto_commit=False,
            auto_offset_reset=auto_offset_reset,
            max_poll_records=self._consumer_max_poll_records,
            request_timeout_ms=15_000,
        )
        await consumer.start()
        try:
            # Translate ``poll_timeout`` (seconds, float) to ``timeout_ms``
            # for ``getmany``. Minimum 50ms so we never spin the event loop.
            timeout_ms = max(50, int(poll_timeout * 1000))
            while True:
                batches = await consumer.getmany(timeout_ms=timeout_ms)
                if not batches:
                    # ``getmany`` returns an empty dict on timeout; yield
                    # control to the loop so cancellation propagates.
                    await asyncio.sleep(0)
                    continue
                for _tp, records in batches.items():
                    for record in records:
                        yield WireMessage(
                            topic=record.topic,
                            partition=record.partition,
                            offset=record.offset,
                            key=record.key,
                            value=record.value,
                            headers=[
                                (name, value if isinstance(value, bytes) else str(value).encode())
                                for name, value in (record.headers or [])
                            ],
                        )
                        # Commit per-record for at-least-once + the
                        # tightest possible resume point on crash.
                        await consumer.commit({record.topic_partition: record.offset + 1})
        finally:
            await consumer.stop()


__all__ = ["KafkaTransport"]
