"""In-memory :class:`aidp_events.transport.Transport` for tests + sandbox.

This implementation faithfully reproduces the Kafka contract that
:meth:`aidp_events.producer.publish_event` and
:meth:`aidp_events.consumer.consume_events` rely on:

- Topics are partitioned (single partition, but the offset model is
  faithful to Kafka's).
- Consumer groups track per-``(group, topic)`` offset cursors.
- ``auto_offset_reset`` defaults to ``latest`` (only new messages are
  visible to a brand-new group), matching the production
  :class:`KafkaTransport`.
- ``commit`` advances the cursor; ``consume`` resumes from there.
- Headers are stored as ``list[tuple[str, bytes]]`` to match aiokafka.

The implementation is intentionally not thread-safe — asyncio only.
The :class:`InMemoryTransport` is exposed from :mod:`aidp_events.testing`
so tests can introspect state (e.g. DLQ size, seeded messages) without
peeking at private attributes.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from collections.abc import AsyncIterator
from dataclasses import dataclass, field

from aidp_events.transport import Transport, WireMessage


@dataclass
class _Topic:
    """A single Kafka topic — in-memory.

    ``records`` holds every record ever sent. ``committed[group]`` is
    the *next-to-read* offset for *group* — i.e. the smallest offset
    not yet processed. The cursor is advanced only by :meth:`commit`,
    mirroring Kafka's at-least-once contract: a record that hasn't been
    committed is re-delivered on the next consume.
    """

    records: deque[WireMessage] = field(default_factory=deque)
    next_offset: int = 0
    # ``group_id -> next-to-read offset``. Missing entry means "no
    # commit yet — fall back to ``auto_offset_reset`` on the next read".
    committed: dict[str, int] = field(default_factory=dict)

    def append(self, msg: WireMessage) -> None:
        msg.offset = self.next_offset
        self.records.append(msg)
        self.next_offset += 1

    def next_for(self, group_id: str, *, auto_offset_reset: str) -> WireMessage | None:
        """Return the next record for *group_id* (does not advance the
        committed offset).

        Used by ``consume``; the caller is expected to call
        :meth:`commit` after successfully processing the record. If the
        caller crashes before the commit, the next ``next_for`` call
        returns the same record — at-least-once.

        ``auto_offset_reset`` mirrors Kafka's semantics:

        - ``"earliest"`` — a brand-new group starts at the first
          available record (offset 0).
        - ``"latest"`` — a brand-new group starts past the current
          high-watermark (only new records are seen).
        """
        start = self.committed.get(group_id)
        if start is None:
            # Brand-new group — apply the reset policy.
            start = 0 if auto_offset_reset == "earliest" else self.next_offset
            # Stash so the next call without a commit re-reads from here.
            self.committed[group_id] = start
        if start >= len(self.records):
            return None
        return self.records[start]

    def commit(self, group_id: str, offset: int) -> None:
        # ``offset`` is the *next-to-read* position, so we store it as-is.
        self.committed[group_id] = offset

    def has_unread(self, group_id: str, *, auto_offset_reset: str) -> bool:
        start = self.committed.get(group_id)
        if start is None:
            start = 0 if auto_offset_reset == "earliest" else self.next_offset
        return start < len(self.records)


class InMemoryTransport:
    """A pure-Python :class:`Transport` for unit tests and the sandbox.

    Construction is cheap; ``start()`` is a no-op. ``close()`` clears
    all state so the same instance can be reused after a teardown
    (callers usually just create a new one — that is fine too).

    The transport defaults to ``auto_offset_reset="earliest"`` so
    tests can ``seed`` records and immediately see them. Production
    code uses :class:`KafkaTransport` which defaults to ``"latest"``
    (the Kafka default for new consumer groups).
    """

    def __init__(self, *, auto_offset_reset: str = "earliest") -> None:
        if auto_offset_reset not in {"earliest", "latest"}:
            raise ValueError(
                f"auto_offset_reset must be 'earliest' or 'latest', got {auto_offset_reset!r}"
            )
        self._auto_offset_reset = auto_offset_reset
        self._topics: dict[str, _Topic] = {}
        self._lock = asyncio.Lock()
        # Async events used to wake up ``consume`` callers when a
        # new record arrives (no busy-spinning).
        self._wake: dict[str, asyncio.Event] = defaultdict(asyncio.Event)
        self._closed = False

    # -- introspection helpers (for tests) -------------------------------

    def dlq_size(self, topic: str) -> int:
        """Return the number of records currently in ``topic + '.dlq'``."""
        dlq = self._topics.get(f"{topic}.dlq")
        return len(dlq.records) if dlq is not None else 0

    def seed(self, topic: str, *, key: bytes, value: bytes) -> None:
        """Append a pre-built record to *topic* (test helper)."""
        msg = WireMessage(
            topic=topic,
            partition=0,
            offset=0,  # will be assigned by ``_Topic.append``
            key=key,
            value=value,
            headers=[],
        )
        topic_obj = self._topics.setdefault(topic, _Topic())
        topic_obj.append(msg)
        self._wake[topic].set()

    async def drain(self, topic: str, *, timeout: float = 0.5) -> list[WireMessage]:
        """Return every currently-stored record on *topic* (test helper)."""
        topic_obj = self._topics.get(topic)
        if topic_obj is None:
            return []
        return list(topic_obj.records)

    # -- lifecycle --------------------------------------------------------

    async def start(self) -> None:
        self._closed = False

    async def close(self) -> None:
        self._closed = True
        # Wake every waiting consumer so they see the closed flag.
        for event in self._wake.values():
            event.set()

    # -- send -------------------------------------------------------------

    async def send(
        self,
        topic: str,
        key: str,
        value: bytes,
        headers: list[tuple[str, bytes]],
    ) -> None:
        async with self._lock:
            topic_obj = self._topics.setdefault(topic, _Topic())
            key_bytes = key.encode("utf-8") if key is not None else None
            topic_obj.append(
                WireMessage(
                    topic=topic,
                    partition=0,
                    offset=0,  # assigned by ``append``
                    key=key_bytes,
                    value=value,
                    headers=list(headers),
                )
            )
            # Notify any consumer waiting on this topic.
            self._wake[topic].set()

    async def send_raw(
        self,
        topic: str,
        key: str,
        value: bytes,
        headers: list[tuple[str, bytes]],
    ) -> None:
        await self.send(topic=topic, key=key, value=value, headers=headers)

    # -- commit -----------------------------------------------------------

    async def commit(self, group_id: str, topic: str, partition: int, offset: int) -> None:
        del partition  # single-partition topics in the fake
        async with self._lock:
            topic_obj = self._topics.setdefault(topic, _Topic())
            topic_obj.commit(group_id, offset)

    # -- consume ----------------------------------------------------------

    def consume(
        self,
        group_id: str,
        topics: list[str],
        *,
        poll_timeout: float = 0.1,
        auto_offset_reset: str | None = None,
    ) -> AsyncIterator[WireMessage]:
        return self._consume(
            group_id=group_id,
            topics=topics,
            poll_timeout=poll_timeout,
            auto_offset_reset=auto_offset_reset or self._auto_offset_reset,
        )

    async def _consume(
        self,
        group_id: str,
        topics: list[str],
        *,
        poll_timeout: float,
        auto_offset_reset: str,
    ) -> AsyncIterator[WireMessage]:
        while not self._closed:
            delivered = False
            for topic in topics:
                topic_obj = self._topics.get(topic)
                if topic_obj is None:
                    continue
                async with self._lock:
                    msg = topic_obj.next_for(group_id, auto_offset_reset=auto_offset_reset)
                if msg is not None:
                    delivered = True
                    yield msg
            if not delivered:
                # Wait for a new record (or the close flag). We translate
                # ``poll_timeout`` to the event wait so cancellation
                # propagates via :meth:`asyncio.Event.wait`.
                event = self._wake[topics[0]] if topics else asyncio.Event()
                try:
                    await asyncio.wait_for(event.wait(), timeout=poll_timeout)
                except TimeoutError:
                    pass
                else:
                    event.clear()


# ``Transport`` is a structural protocol — ``isinstance`` works at runtime
# as long as the class provides the right methods. The class is
# registered explicitly so static checkers see it.
assert isinstance(InMemoryTransport(), Transport)  # runtime sanity check


__all__ = ["InMemoryTransport"]
