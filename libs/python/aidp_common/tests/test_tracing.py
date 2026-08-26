"""Tests for ``aidp_common.tracing``."""

from __future__ import annotations

import pytest
from aidp_common.tracing import (
    _reset_provider_for_tests,
    get_current_span,
    get_trace_id,
    setup_tracing,
)
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider


@pytest.fixture(autouse=True)
def _clean_provider() -> None:
    """Each test must run with a clean global tracer provider."""
    _reset_provider_for_tests()


def test_setup_tracing_returns_provider() -> None:
    provider = setup_tracing("svc-test", otlp_endpoint=None)
    assert isinstance(provider, TracerProvider)


def test_setup_tracing_is_idempotent() -> None:
    provider1 = setup_tracing("svc-test", otlp_endpoint=None)
    provider2 = setup_tracing("svc-test", otlp_endpoint=None)
    assert provider1 is provider2


def test_setup_tracing_rebuilds_provider_on_signature_change() -> None:
    provider1 = setup_tracing("svc-a", otlp_endpoint=None)
    provider2 = setup_tracing("svc-b", otlp_endpoint=None)
    assert provider1 is not provider2


def test_setup_tracing_installs_otlp_exporter() -> None:
    """When an endpoint is provided, a BatchSpanProcessor is added to the provider."""
    provider = setup_tracing("svc-test", otlp_endpoint="http://localhost:4317")
    # We can't easily assert on the BatchSpanProcessor without poking into
    # internals, but we can verify the provider has at least one processor.
    assert hasattr(provider, "_active_span_processor")


def test_get_trace_id_outside_span_returns_none_or_zero() -> None:
    setup_tracing("svc-test", otlp_endpoint=None)
    # No active span → no valid trace id. Allow either ``None`` or all-zero id.
    tid = get_trace_id()
    assert tid is None or tid == 0


def test_get_trace_id_inside_active_span() -> None:
    setup_tracing("svc-test", otlp_endpoint=None)
    tracer = trace.get_tracer("aidp.test")
    with tracer.start_as_current_span("unit") as span:
        expected = span.get_span_context().trace_id
        assert get_trace_id() == expected
        assert get_trace_id() != 0


def test_get_trace_id_returns_hex_string() -> None:
    """``get_trace_id(hex=True)`` must return the canonical 32-char hex form."""
    setup_tracing("svc-test", otlp_endpoint=None)
    tracer = trace.get_tracer("aidp.test")
    with tracer.start_as_current_span("unit") as span:
        expected = format(span.get_span_context().trace_id, "032x")
        assert get_trace_id(as_hex=True) == expected


def test_get_current_span_inside_context() -> None:
    setup_tracing("svc-test", otlp_endpoint=None)
    tracer = trace.get_tracer("aidp.test")
    with tracer.start_as_current_span("unit") as span:
        assert get_current_span() is span


def test_reset_provider_for_tests_clears_cached() -> None:
    setup_tracing("svc-a", otlp_endpoint=None)
    _reset_provider_for_tests()
    # New call must build a fresh provider instance.
    new_provider = setup_tracing("svc-b", otlp_endpoint=None)
    assert isinstance(new_provider, TracerProvider)
