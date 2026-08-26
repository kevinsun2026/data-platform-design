"""Test-only helpers re-exported for the ``aidp_events`` test suite.

The real transport classes live in their respective modules (production
code). This module re-exports them so the test files have a single
import surface (``aidp_events.testing``) regardless of which
implementation the sandbox can actually run.

Do not import this module from production code — it is *only* meant to
make the test files readable and to keep the production surface clean.
"""

from __future__ import annotations

from aidp_events.inmemory_transport import InMemoryTransport
from aidp_events.kafka_transport import KafkaTransport

__all__ = ["InMemoryTransport", "KafkaTransport"]
