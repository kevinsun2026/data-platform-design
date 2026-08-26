"""AIDP shared event bus (Kafka producer/consumer with envelope, retry, DLQ).

Sub-modules:

- :mod:`aidp_events.envelope` — Canonical ``EventEnvelope`` Pydantic model.
- :mod:`aidp_events.producer` — :func:`publish_event` (with retry + DLQ).
- :mod:`aidp_events.consumer` — :func:`consume_events` (at-least-once +
  idempotency key + DLQ on max retries).
- :mod:`aidp_events.transport` — Transport abstraction over ``aiokafka`` plus
  an in-memory fake transport used when the sandbox cannot pull a Kafka image.
"""

from __future__ import annotations

__version__ = "0.1.0"
