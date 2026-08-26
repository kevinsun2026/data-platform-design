"""Error envelope + exception handler for the Agent Gateway.

The gateway uses :class:`aidp_common.errors.AppError` for every
domain failure. This module installs the single
``AppError`` exception handler that renders the platform's unified
``{code, message, details, trace_id}`` envelope.

Without this handler, an unhandled :class:`AppError` would surface
as a 500 with FastAPI's default body — which would break the
OpenAI-compat callers that expect a specific error shape.
"""

from __future__ import annotations

from typing import Any

from aidp_common.errors import AppError
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from opentelemetry import trace


def _current_trace_id() -> str | None:
    """Return the active OTel trace id, or ``None`` when no span is recording."""
    span = trace.get_current_span()
    if span is None:
        return None
    ctx = span.get_span_context()
    if ctx is None or not ctx.trace_id:
        return None
    return format(ctx.trace_id, "032x")


def install_app_error_handler(app: FastAPI) -> None:
    """Register a single ``AppError`` exception handler on *app*."""

    @app.exception_handler(AppError)
    async def _handle_app_error(request: Request, exc: AppError) -> JSONResponse:
        body: dict[str, Any] = exc.to_dict(trace_id=_current_trace_id())
        return JSONResponse(
            status_code=exc.status,
            content=body,
        )


__all__ = ["install_app_error_handler"]
