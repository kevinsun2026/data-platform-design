"""Transport abstraction over the Kafka event bus.

The platform has two implementations:

- :class:`KafkaTransport` — backed by ``aiokafka`` (production).
- :class:`InMemoryTransport` — pure-Python, used by unit tests and by
  sandbox environments where the testcontainers image is unavailable.

Both implement the :class:`Transport` protocol so :func:`publish_event` and
:func:`consume_events` can be exercised end-to-end without a real broker
in CI. The two implementations share the same wire contract (envelope
JSON, headers as ``list[tuple[str, bytes]]``) so a test written against
the fake passes byte-for-byte against Kafka.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Protocol, runtime_checkable

# ---------------------------------------------------------------------------
# Wire types
# ---------------------------------------------------------------------------


class WireMessage:
    """A single record on the wire (one Kafka record, one DLQ entry).

    Attributes:
        topic: The Kafka topic the record was written to.
        partition: Kafka partition (always ``0`` for the in-memory transport).
        offset: Per-partition offset. Strictly monotonically increasing per
            ``(topic, partition)`` pair.
        key: Record key (bytes). For the AIDP envelope this is
            ``tenant_id:event_id`` so same-tenant events stay on the
            same partition.
        value: Record value (bytes). For envelope topics this is
            :meth:`EventEnvelope.model_dump_json`.
        headers: Record headers as ``(name, value)`` pairs.
    """

    __slots__ = ("headers", "key", "offset", "partition", "topic", "value")

    def __init__(
        self,
        *,
        topic: str,
        partition: int,
        offset: int,
        key: bytes | None,
        value: bytes,
        headers: list[tuple[str, bytes]],
    ) -> None:
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.key = key
        self.value = value
        self.headers = list(headers)

    def __repr__(self) -> str:  # pragma: no cover - debugging only
        return (
            f"WireMessage(topic={self.topic!r}, partition={self.partition}, "
            f"offset={self.offset}, key={self.key!r})"
        )


# ---------------------------------------------------------------------------
# Transport protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class Transport(Protocol):
    """Minimal transport interface used by the producer and consumer.

    Both implementations follow the same lifecycle:

    1. ``start()`` opens the underlying connection.
    2. ``send()`` / ``commit()`` / ``consume()`` are called concurrently.
    3. ``close()`` tears the connection down.

    The producer never calls ``consume()``; the consumer never calls
    ``send()`` outside of the DLQ forward path (which is a special case
    that goes through :func:`aidp_events.producer.publish_event`-style
    send, *not* via the transport directly — see :mod:`aidp_events.consumer`).
    """

    async def start(self) -> None:
        """Open the underlying connection (idempotent)."""
        ...

    async def close(self) -> None:
        """Close the connection (idempotent)."""
        ...

    async def send(
        self,
        topic: str,
        key: str,
        value: bytes,
        headers: list[tuple[str, bytes]],
    ) -> None:
        """Publish a single record to *topic* with the given key/value/headers.

        Implementations must guarantee that ``send`` raises on broker
        unavailability (so the producer's retry loop can engage). On
        success the record is durably accepted by the broker (in the
        in-memory case, persisted in a Python list).
        """
        ...

    async def commit(self, group_id: str, topic: str, partition: int, offset: int) -> None:
        """Commit *offset* as the next-to-read position for *group_id*.

        Implementations must persist this so a subsequent ``consume()``
        with the same ``group_id`` resumes from ``offset + 1``.
        """
        ...

    def consume(
        self,
        group_id: str,
        topics: list[str],
        *,
        poll_timeout: float = 0.1,
        auto_offset_reset: str = "latest",
    ) -> AsyncIterator[WireMessage]:
        """Yield records from *topics* for *group_id* in offset order.

        The consumer starts at the last committed offset for the group;
        for new groups, implementations fall back to ``auto_offset_reset``
        (typically ``"latest"`` — only new messages are seen; tests use
        ``"earliest"`` so seeded records are visible). The returned
        iterator must be cancellable: stopping the consuming task
        should release the underlying connection promptly.
        """
        ...

    async def send_raw(
        self,
        topic: str,
        key: str,
        value: bytes,
        headers: list[tuple[str, bytes]],
    ) -> None:
        """Publish a record *without* retry / DLQ wrapping.

        The consumer uses this for the DLQ forward path. It bypasses
        the producer's retry loop on purpose — by the time we are
        DLQ'ing, the producer has already exhausted its budget, and
        any further failure is a hard error that should surface.
        """
        ...


__all__ = ["Transport", "WireMessage"]
