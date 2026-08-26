"""Error-translation glue for the Notify API.

The Notify service uses :class:`aidp_common.errors.AppError` for every
domain failure. Without an exception handler those exceptions bubble
up and FastAPI returns ``500``. This module installs a single
``AppError`` handler that renders the platform's unified
``{"code", "message", "details", "trace_id"}`` envelope.

This is the same pattern :mod:`aidp_iam.api.errors` and
:mod:`aidp_audit.api.errors` ship — kept verbatim so the platform's
error contract stays uniform across services.
"""

from __future__ import annotations

from typing import Any

from aidp_common.errors import AppError
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from opentelemetry import trace


def _current_trace_id() -> str | None:
    """Return the active OTel trace id, or ``None`` when no span is recording.

    The id is a 32-character lowercase hex string per the OTel spec;
    we format via the same path :mod:`aidp_events.envelope` uses so
    every service emits the same shape.
    """
    span = trace.get_current_span()
    if span is None:
        return None
    ctx = span.get_span_context()
    if ctx is None or not ctx.trace_id:
        return None
    return format(ctx.trace_id, "032x")


def install_app_error_handler(app: FastAPI) -> None:
    """Register a single ``AppError`` exception handler on *app*.

    Any :class:`aidp_common.errors.AppError` raised by a handler is
    converted to a :class:`fastapi.responses.JSONResponse` with the
    error's HTTP status code and a body matching the platform's
    unified error envelope:

    .. code-block:: json

        {
            "code": "NOT_FOUND",
            "message": "notification template user.welcome@zh-CN not found",
            "details": {},
            "trace_id": "0af7651916cd43dd8448eb211c80319c"
        }
    """

    @app.exception_handler(AppError)
    async def _handle_app_error(request: Request, exc: AppError) -> JSONResponse:
        body: dict[str, Any] = exc.to_dict(trace_id=_current_trace_id())
        return JSONResponse(
            status_code=exc.status,
            content=body,
        )


__all__ = ["install_app_error_handler"]
