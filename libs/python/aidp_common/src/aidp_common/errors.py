"""Unified error model for AIDP Python services.

All domain failures should raise (or wrap into) a subclass of :class:`AppError`.
The wire format produced by :meth:`AppError.to_dict` is the canonical error
response body used by every service:

.. code-block:: json

    {
        "code": "NOT_FOUND",
        "message": "User u-123 not found",
        "details": {},
        "trace_id": "0af7651916cd43dd8448eb211c80319c"
    }
"""

from __future__ import annotations

from enum import Enum
from typing import Any


class ErrorCode(str, Enum):  # noqa: UP042 - intentional str-Enum mixin
    """Canonical error codes returned to API clients."""

    NOT_FOUND = "NOT_FOUND"
    FORBIDDEN = "FORBIDDEN"
    UNAUTHORIZED = "UNAUTHORIZED"
    VALIDATION = "VALIDATION"
    CONFLICT = "CONFLICT"
    RATE_LIMITED = "RATE_LIMITED"
    UPSTREAM_ERROR = "UPSTREAM_ERROR"
    INTERNAL = "INTERNAL"


class AppError(Exception):
    """Base class for all AIDP application-level errors.

    Args:
        code: A canonical :class:`ErrorCode` value.
        message: Human-readable description. Must be safe to surface to clients.
        status: HTTP status code to use when this error crosses the wire.
        details: Optional structured context. Must be JSON-serializable.
    """

    def __init__(
        self,
        code: ErrorCode,
        message: str,
        status: int = 400,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status
        self.details: dict[str, Any] = details if details is not None else {}

    def to_dict(self, trace_id: str | None = None) -> dict[str, Any]:
        """Serialize the error to a JSON-ready dict.

        Args:
            trace_id: OpenTelemetry trace id to attach to the response. When
                provided, the response also carries the ``trace_id`` field so
                operators can correlate client reports with traces.
        """
        payload: dict[str, Any] = {
            "code": self.code.value,
            "message": self.message,
            "details": self.details,
        }
        if trace_id is not None:
            payload["trace_id"] = trace_id
        return payload

    def __repr__(self) -> str:  # pragma: no cover - debugging helper
        return f"{type(self).__name__}(code={self.code!r}, status={self.status}, message={self.message!r})"


class NotFoundError(AppError):
    """A resource was requested but does not exist (HTTP 404)."""

    def __init__(self, resource: str, id: Any) -> None:  # noqa: A002 - brief API uses ``id``
        super().__init__(
            ErrorCode.NOT_FOUND,
            f"{resource} {id} not found",
            status=404,
        )
        self.resource = resource
        self.resource_id = id


class ForbiddenError(AppError):
    """The caller is authenticated but not allowed to perform the action (HTTP 403)."""

    def __init__(self, msg: str = "forbidden") -> None:
        super().__init__(ErrorCode.FORBIDDEN, msg, status=403)


class UnauthorizedError(AppError):
    """The caller is not authenticated (HTTP 401)."""

    def __init__(self, msg: str = "unauthorized") -> None:
        super().__init__(ErrorCode.UNAUTHORIZED, msg, status=401)


class ValidationError(AppError):
    """Request input failed validation (HTTP 400)."""

    def __init__(
        self, msg: str = "validation failed", details: dict[str, Any] | None = None
    ) -> None:
        super().__init__(ErrorCode.VALIDATION, msg, status=400, details=details)


class ConflictError(AppError):
    """The request collides with existing state (HTTP 409)."""

    def __init__(self, msg: str = "conflict") -> None:
        super().__init__(ErrorCode.CONFLICT, msg, status=409)


class RateLimitedError(AppError):
    """The caller has been throttled (HTTP 429)."""

    def __init__(self, msg: str = "rate limited") -> None:
        super().__init__(ErrorCode.RATE_LIMITED, msg, status=429)


class UpstreamError(AppError):
    """A downstream dependency failed (HTTP 502)."""

    def __init__(self, msg: str = "upstream error", details: dict[str, Any] | None = None) -> None:
        super().__init__(ErrorCode.UPSTREAM_ERROR, msg, status=502, details=details)


class InternalError(AppError):
    """An unexpected server-side failure (HTTP 500)."""

    def __init__(self, msg: str = "internal server error") -> None:
        super().__init__(ErrorCode.INTERNAL, msg, status=500)


__all__ = [
    "AppError",
    "ConflictError",
    "ErrorCode",
    "ForbiddenError",
    "InternalError",
    "NotFoundError",
    "RateLimitedError",
    "UnauthorizedError",
    "UpstreamError",
    "ValidationError",
]
