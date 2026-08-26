"""Tests for ``aidp_events.envelope`` (the ``EventEnvelope`` Pydantic model).

These tests pin the wire format every service depends on:

- All required fields are present (``event_id`` / ``tenant_id`` / ``occurred_at``
  / ``producer`` / ``event_type`` / ``payload`` / ``trace_id`` / ``event_version``
  / ``headers``).
- ``event_id`` is a valid UUID4 string.
- ``occurred_at`` round-trips as ISO 8601 in UTC.
- ``trace_id`` is always 32 lowercase hex chars.
- ``payload`` is a dict (the wire is JSON).
- ``model_dump`` + ``model_validate`` round-trip.
- ``headers`` is an optional dict.

This file does not exercise Kafka. It is pure Pydantic validation.
"""

from __future__ import annotations

import json
import re
import uuid
from datetime import UTC, datetime

import pytest
from aidp_events.envelope import EventEnvelope, new_envelope
from pydantic import ValidationError

_HEX32 = re.compile(r"^[0-9a-f]{32}$")


def _sample_envelope() -> EventEnvelope:
    return new_envelope(
        event_type="datasource.connection.created",
        tenant_id="tenant-1",
        payload={"id": "conn-1"},
    )


# ---------------------------------------------------------------------------
# Field presence + shape
# ---------------------------------------------------------------------------


def test_envelope_has_all_required_fields() -> None:
    """Every field the platform promises is present on a fresh envelope."""
    env = _sample_envelope()
    # Sanity-check the literal attribute set matches the brief.
    expected = {
        "event_id",
        "tenant_id",
        "occurred_at",
        "producer",
        "event_type",
        "payload",
        "trace_id",
        "event_version",
        "headers",
    }
    assert expected.issubset(set(env.model_dump().keys()))


def test_event_id_is_uuid4_string() -> None:
    env = _sample_envelope()
    # ``uuid.UUID(...)`` validates the shape and version (4).
    parsed = uuid.UUID(env.event_id)
    assert parsed.version == 4
    # Stringifies canonically.
    assert env.event_id == str(parsed)


def test_tenant_id_is_preserved() -> None:
    env = new_envelope(event_type="x", tenant_id="tenant-xyz", payload={}, event_version=1)
    assert env.tenant_id == "tenant-xyz"


def test_occurred_at_is_timezone_aware_utc() -> None:
    env = _sample_envelope()
    assert env.occurred_at.tzinfo is not None
    assert env.occurred_at.utcoffset() == UTC.utcoffset(env.occurred_at)
    # Within a few seconds of "now" — the factory stamps it at build time.
    delta = (datetime.now(UTC) - env.occurred_at).total_seconds()
    assert -1 <= delta <= 5


def test_producer_defaults_to_settings_service_name(monkeypatch: pytest.MonkeyPatch) -> None:
    """The factory pulls ``AIDP_SERVICE_NAME`` via ``aidp_common.config``."""
    from aidp_common import config as cfg

    cfg.reset_settings_cache()
    monkeypatch.setenv("AIDP_SERVICE_NAME", "datasource-service")
    env = new_envelope(event_type="x", tenant_id="t", payload={})
    assert env.producer == "datasource-service"
    cfg.reset_settings_cache()
    monkeypatch.delenv("AIDP_SERVICE_NAME", raising=False)


def test_producer_explicit_override_wins() -> None:
    env = new_envelope(event_type="x", tenant_id="t", payload={}, producer="custom-producer")
    assert env.producer == "custom-producer"


def test_payload_is_dict() -> None:
    env = _sample_envelope()
    assert isinstance(env.payload, dict)
    assert env.payload == {"id": "conn-1"}


def test_event_type_is_preserved() -> None:
    env = new_envelope(event_type="audit.event.recorded", tenant_id="t", payload={"k": "v"})
    assert env.event_type == "audit.event.recorded"


def test_event_version_default_is_one() -> None:
    env = new_envelope(event_type="x", tenant_id="t", payload={})
    assert env.event_version == 1


def test_event_version_override() -> None:
    env = new_envelope(event_type="x", tenant_id="t", payload={}, event_version=2)
    assert env.event_version == 2


def test_headers_default_is_empty_dict() -> None:
    env = _sample_envelope()
    assert env.headers == {}


def test_headers_are_preserved() -> None:
    env = new_envelope(event_type="x", tenant_id="t", payload={}, headers={"x-source": "cli"})
    assert env.headers == {"x-source": "cli"}


# ---------------------------------------------------------------------------
# trace_id
# ---------------------------------------------------------------------------


def test_trace_id_is_32_lowercase_hex() -> None:
    env = _sample_envelope()
    assert isinstance(env.trace_id, str)
    assert _HEX32.match(env.trace_id), env.trace_id


def test_trace_id_differs_across_envelopes() -> None:
    """No OTel span is active in the test, so the factory falls back to
    a per-envelope UUID-derived 32-hex trace id."""
    a = _sample_envelope()
    b = _sample_envelope()
    assert a.trace_id != b.trace_id


def test_trace_id_uses_otel_when_span_active() -> None:
    """When a recording OTel span is active, its trace id wins."""
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )

    # Use a private TracerProvider so we don't fight the global one.
    provider = TracerProvider(resource=Resource.create({"service.name": "t"}))
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    tracer = provider.get_tracer(__name__)

    with tracer.start_as_current_span("test-span"):
        env = new_envelope(event_type="x", tenant_id="t", payload={})

    expected_hex = format(exporter.get_finished_spans()[0].context.trace_id, "032x")
    assert env.trace_id == expected_hex
    provider.shutdown()


# ---------------------------------------------------------------------------
# Serialization round-trip
# ---------------------------------------------------------------------------


def test_envelope_round_trip_through_json() -> None:
    env = _sample_envelope()
    raw = json.dumps(env.model_dump(mode="json"))
    parsed = EventEnvelope.model_validate_json(raw)
    assert parsed == env


def test_envelope_dump_mode_json_makes_iso_string() -> None:
    env = _sample_envelope()
    dumped = env.model_dump(mode="json")
    assert isinstance(dumped["occurred_at"], str)
    # The string parses back to a UTC datetime.
    parsed = datetime.fromisoformat(dumped["occurred_at"])
    assert parsed.tzinfo is not None


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def test_payload_must_be_dict() -> None:
    with pytest.raises(ValidationError):
        new_envelope(event_type="x", tenant_id="t", payload=["not", "a", "dict"])  # type: ignore[arg-type]


def test_empty_payload_is_allowed() -> None:
    env = new_envelope(event_type="x", tenant_id="t", payload={})
    assert env.payload == {}


def test_headers_must_be_dict_when_provided() -> None:
    with pytest.raises(ValidationError):
        new_envelope(
            event_type="x",
            tenant_id="t",
            payload={},
            headers="not-a-dict",  # type: ignore[arg-type]
        )
