"""OpenTelemetry tracing setup shared by every AIDP Python service.

This module provides a single entry point (:func:`setup_tracing`) that
configures a process-wide :class:`opentelemetry.sdk.trace.TracerProvider` and
optional OTLP gRPC exporter, plus helpers (:func:`get_trace_id`,
:func:`get_current_span`) that downstream services and middleware use to
correlate logs / errors with traces.
"""

from __future__ import annotations

import logging

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from aidp_common.config import get_settings

_LOG = logging.getLogger(__name__)

# Track the provider we install so ``setup_tracing`` is idempotent.
_installed_provider: TracerProvider | None = None
_installed_signature: tuple[str, str | None] | None = None


def setup_tracing(
    service_name: str | None = None,
    env: str | None = None,
    otlp_endpoint: str | None = None,
) -> TracerProvider:
    """Configure a process-wide :class:`TracerProvider` for the service.

    Safe to call multiple times: if the previous call used the same
    ``(service_name, otlp_endpoint)`` pair, the same provider is returned.
    Calling again with different arguments rebuilds the provider so the new
    settings take effect.

    Args:
        service_name: Logical service name attached as a resource attribute.
            Defaults to the ``AIDP_SERVICE_NAME`` env value.
        env: Deployment environment label. Defaults to ``AIDP_ENV``.
        otlp_endpoint: OTLP gRPC endpoint. ``None`` skips exporter setup
            (spans are still created but never exported). When provided, a
            :class:`BatchSpanProcessor` with the gRPC exporter is installed.

    Returns:
        The active :class:`TracerProvider`. The same instance is also set
        as the global provider via :func:`opentelemetry.trace.set_tracer_provider`.
    """
    global _installed_provider, _installed_signature

    settings = get_settings()
    name = service_name or settings.service_name
    environment = env or settings.env
    endpoint = otlp_endpoint if otlp_endpoint is not None else settings.otlp_endpoint
    signature = (name, endpoint)

    if _installed_provider is not None and _installed_signature == signature:
        return _installed_provider

    resource = Resource.create(
        {
            "service.name": name,
            "service.namespace": "aidp",
            "deployment.environment": environment,
        }
    )
    provider = TracerProvider(resource=resource)

    if endpoint:
        try:
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
                OTLPSpanExporter,
            )

            exporter = OTLPSpanExporter(endpoint=endpoint, insecure=True)
            provider.add_span_processor(BatchSpanProcessor(exporter))
            _LOG.info("OTLP span exporter installed", extra={"endpoint": endpoint})
        except Exception as exc:  # pragma: no cover - network / import edge cases
            _LOG.warning(
                "Failed to install OTLP exporter; continuing without export",
                extra={"endpoint": endpoint, "error": str(exc)},
            )

    trace.set_tracer_provider(provider)
    _installed_provider = provider
    _installed_signature = signature
    return provider


def get_trace_id(as_hex: bool = False) -> int | str | None:
    """Return the trace id of the currently active span, if any.

    Args:
        as_hex: When ``True`` the trace id is returned as the canonical
            32-character lower-case hex string. When ``False`` the integer
            value is returned. ``None`` is returned when no span is active.

    Returns:
        The current trace id, or ``None`` if no span is active.
    """
    span = trace.get_current_span()
    if span is None:
        return None
    ctx = span.get_span_context()
    if not ctx or ctx.trace_id == 0:
        return None
    if as_hex:
        return format(ctx.trace_id, "032x")
    return ctx.trace_id


def get_current_span() -> trace.Span:
    """Return the currently active :class:`opentelemetry.trace.Span`.

    This is a thin wrapper around :func:`opentelemetry.trace.get_current_span`
    that always returns a :class:`Span` (the no-op ``NonRecordingSpan`` when
    no real span is active), simplifying downstream call sites.
    """
    return trace.get_current_span()


def _reset_provider_for_tests() -> None:
    """Drop the cached provider so the next ``setup_tracing`` rebuilds it."""
    global _installed_provider, _installed_signature
    _installed_provider = None
    _installed_signature = None


__all__ = [
    "get_current_span",
    "get_trace_id",
    "setup_tracing",
]
