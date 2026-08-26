"""Tests for ``aidp_common.logging``."""

from __future__ import annotations

import io
import json
import logging

import pytest
from aidp_common.logging import JsonFormatter, get_logger, setup_logging
from aidp_common.tracing import _reset_provider_for_tests, setup_tracing
from opentelemetry import trace


def _capture_root(level: str = "INFO") -> io.StringIO:
    """Attach a StringIO handler emitting JSON to the root logger and return the buffer."""
    setup_logging(level=level, service_name="test-svc", env="unit-test")
    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    # Remove any prior StringIO handlers we attached in earlier tests.
    for h in list(root.handlers):
        if isinstance(h, logging.StreamHandler) and getattr(h, "_aidp_test_buf", None):
            root.removeHandler(h)
    handler._aidp_test_buf = buf  # type: ignore[attr-defined]
    root.addHandler(handler)
    return buf


def test_setup_logging_configures_root_level() -> None:
    setup_logging(level="DEBUG", service_name="svc", env="dev")
    assert logging.getLogger().level == logging.DEBUG


def test_get_logger_returns_named_logger() -> None:
    setup_logging(level="INFO", service_name="svc", env="dev")
    logger = get_logger("aidp.test")
    assert isinstance(logger, logging.Logger)
    assert logger.name == "aidp.test"


def test_setup_logging_is_idempotent() -> None:
    """Calling ``setup_logging`` twice does not duplicate handlers."""
    setup_logging(level="INFO", service_name="svc", env="dev")
    handlers_after_first = list(logging.getLogger().handlers)
    setup_logging(level="DEBUG", service_name="svc", env="dev")
    handlers_after_second = list(logging.getLogger().handlers)
    assert len(handlers_after_second) == len(handlers_after_first)


def test_setup_logging_replaces_handlers_on_change() -> None:
    setup_logging(level="INFO", service_name="svc", env="dev")
    setup_logging(level="WARNING", service_name="svc", env="dev")
    assert logging.getLogger().level == logging.WARNING


def test_json_formatter_emits_valid_json() -> None:
    buf = _capture_root("INFO")
    logging.getLogger().info("hello", extra={"tenant_id": "t-1", "trace_id": "tr-1"})
    line = buf.getvalue().strip().splitlines()[-1]
    payload = json.loads(line)
    assert payload["message"] == "hello"
    assert payload["level"] == "INFO"
    assert payload["service"] == "test-svc"
    assert payload["env"] == "unit-test"
    assert payload["tenant_id"] == "t-1"
    assert payload["trace_id"] == "tr-1"
    assert "timestamp" in payload


def test_json_formatter_handles_exception() -> None:
    buf = _capture_root("INFO")
    try:
        raise ValueError("boom")
    except ValueError:
        logging.getLogger().info("oops", exc_info=True)
    line = buf.getvalue().strip().splitlines()[-1]
    payload = json.loads(line)
    # ``python-json-logger`` emits tracebacks under the ``exc_info`` key.
    assert "exc_info" in payload
    assert "ValueError" in payload["exc_info"]
    assert "boom" in payload["exc_info"]


def test_json_formatter_includes_level_alias() -> None:
    buf = _capture_root("WARNING")
    logging.getLogger().warning("careful")
    line = buf.getvalue().strip().splitlines()[-1]
    payload = json.loads(line)
    # We normalize ``levelname`` → ``level`` for grep-friendliness.
    assert payload["level"] == "WARNING"


def test_auto_injects_trace_id_from_otel_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``_BoundFilter`` must pull the active span's trace id into the record
    even when the call site did not pass ``extra={"trace_id": ...}``."""
    # ``setup_tracing`` reads from ``aidp_common.config`` which validates
    # required ``AIDP_*`` env vars. Set them so the test works in isolation.
    monkeypatch.setenv("AIDP_DB_URL", "postgresql://x")
    monkeypatch.setenv("AIDP_REDIS_URL", "redis://x")
    monkeypatch.setenv("AIDP_SERVICE_NAME", "svc")
    _reset_provider_for_tests()
    setup_tracing("svc-test", otlp_endpoint=None)
    buf = _capture_root("INFO")
    tracer = trace.get_tracer("aidp.test")
    with tracer.start_as_current_span("unit") as span:
        expected_hex = format(span.get_span_context().trace_id, "032x")
        # Call site deliberately omits ``trace_id``; the filter must auto-fill.
        logging.getLogger().info("hello", extra={"tenant_id": "t-1"})
    line = buf.getvalue().strip().splitlines()[-1]
    payload = json.loads(line)
    assert payload["trace_id"] == expected_hex
    assert len(payload["trace_id"]) == 32  # canonical 32-char lower-case hex
    # Sanity: tenant_id was not lost.
    assert payload["tenant_id"] == "t-1"


def test_explicit_trace_id_in_extra_wins_over_auto_injection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A caller-supplied ``trace_id`` must not be overwritten by the OTel one."""
    monkeypatch.setenv("AIDP_DB_URL", "postgresql://x")
    monkeypatch.setenv("AIDP_REDIS_URL", "redis://x")
    monkeypatch.setenv("AIDP_SERVICE_NAME", "svc")
    _reset_provider_for_tests()
    setup_tracing("svc-test", otlp_endpoint=None)
    buf = _capture_root("INFO")
    tracer = trace.get_tracer("aidp.test")
    with tracer.start_as_current_span("unit"):
        logging.getLogger().info("hello", extra={"trace_id": "caller-supplied"})
    line = buf.getvalue().strip().splitlines()[-1]
    payload = json.loads(line)
    assert payload["trace_id"] == "caller-supplied"


def test_no_trace_id_field_when_no_active_span(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Outside any active span the filter must not emit a ``null`` ``trace_id``."""
    monkeypatch.setenv("AIDP_DB_URL", "postgresql://x")
    monkeypatch.setenv("AIDP_REDIS_URL", "redis://x")
    monkeypatch.setenv("AIDP_SERVICE_NAME", "svc")
    _reset_provider_for_tests()
    setup_tracing("svc-test", otlp_endpoint=None)
    buf = _capture_root("INFO")
    logging.getLogger().info("hello")
    line = buf.getvalue().strip().splitlines()[-1]
    payload = json.loads(line)
    assert "trace_id" not in payload
