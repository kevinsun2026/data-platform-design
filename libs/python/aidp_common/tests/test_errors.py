"""Tests for ``aidp_common.errors``."""

from __future__ import annotations

import pytest
from aidp_common.errors import (
    AppError,
    ConflictError,
    ErrorCode,
    ForbiddenError,
    InternalError,
    NotFoundError,
    RateLimitedError,
    UnauthorizedError,
    UpstreamError,
    ValidationError,
)


def test_error_code_values() -> None:
    assert ErrorCode.NOT_FOUND == "NOT_FOUND"
    assert ErrorCode.FORBIDDEN == "FORBIDDEN"
    assert ErrorCode.UNAUTHORIZED == "UNAUTHORIZED"
    assert ErrorCode.VALIDATION == "VALIDATION"
    assert ErrorCode.CONFLICT == "CONFLICT"
    assert ErrorCode.RATE_LIMITED == "RATE_LIMITED"
    assert ErrorCode.UPSTREAM_ERROR == "UPSTREAM_ERROR"
    assert ErrorCode.INTERNAL == "INTERNAL"


def test_error_code_is_str() -> None:
    """ErrorCode must serialize to its plain string value for JSON responses."""
    # In Python 3.11+, ``str(SomeEnum.X)`` returns ``"ClassName.X"`` by design,
    # but ``isinstance(x, str)`` still holds because of the ``str, Enum`` mixin.
    assert isinstance(ErrorCode.NOT_FOUND, str)
    # The value is what we ship over the wire (used by ``AppError.to_dict``).
    assert ErrorCode.NOT_FOUND.value == "NOT_FOUND"
    # And it compares equal to the literal string.
    assert ErrorCode.NOT_FOUND == "NOT_FOUND"


def test_app_error_basic() -> None:
    err = AppError(ErrorCode.INTERNAL, "boom", status=500, details={"k": "v"})
    assert err.code is ErrorCode.INTERNAL
    assert err.message == "boom"
    assert err.status == 500
    assert err.details == {"k": "v"}


def test_app_error_defaults() -> None:
    err = AppError(ErrorCode.VALIDATION, "bad")
    assert err.status == 400
    assert err.details == {}


def test_app_error_is_exception() -> None:
    """AppError must be catchable as a regular Exception."""
    with pytest.raises(AppError) as excinfo:
        raise AppError(ErrorCode.INTERNAL, "x")
    assert excinfo.value.code is ErrorCode.INTERNAL


def test_not_found_error_format() -> None:
    err = NotFoundError("User", "u-123")
    assert err.code is ErrorCode.NOT_FOUND
    assert err.status == 404
    assert "User" in err.message
    assert "u-123" in err.message


def test_forbidden_error_default() -> None:
    err = ForbiddenError()
    assert err.code is ErrorCode.FORBIDDEN
    assert err.status == 403
    assert err.message == "forbidden"


def test_forbidden_error_custom_message() -> None:
    err = ForbiddenError("nope")
    assert err.message == "nope"
    assert err.status == 403


def test_unauthorized_error() -> None:
    err = UnauthorizedError("missing token")
    assert err.code is ErrorCode.UNAUTHORIZED
    assert err.status == 401
    assert err.message == "missing token"


def test_validation_error() -> None:
    err = ValidationError("field required", details={"field": "email"})
    assert err.code is ErrorCode.VALIDATION
    assert err.status == 400
    assert err.details == {"field": "email"}


def test_conflict_error() -> None:
    err = ConflictError("already exists")
    assert err.code is ErrorCode.CONFLICT
    assert err.status == 409


def test_rate_limited_error() -> None:
    err = RateLimitedError("slow down")
    assert err.code is ErrorCode.RATE_LIMITED
    assert err.status == 429


def test_upstream_error() -> None:
    err = UpstreamError("payment gateway timeout", details={"upstream": "stripe"})
    assert err.code is ErrorCode.UPSTREAM_ERROR
    assert err.status == 502
    assert err.details == {"upstream": "stripe"}


def test_internal_error() -> None:
    err = InternalError("kaboom")
    assert err.code is ErrorCode.INTERNAL
    assert err.status == 500


def test_to_dict_shape() -> None:
    """The wire format must be ``{code, message, details}`` for API responses."""
    err = NotFoundError("Order", "o-1")
    payload = err.to_dict()
    assert payload == {
        "code": "NOT_FOUND",
        "message": "Order o-1 not found",
        "details": {},
    }


def test_to_dict_includes_trace_id_when_set() -> None:
    err = NotFoundError("Order", "o-1")
    payload = err.to_dict(trace_id="abc-123")
    assert payload["trace_id"] == "abc-123"


def test_subclass_can_be_caught_as_app_error() -> None:
    """Handlers can do ``except AppError`` to catch every domain error."""
    with pytest.raises(AppError):
        raise NotFoundError("X", 1)
    with pytest.raises(AppError):
        raise ForbiddenError()
    with pytest.raises(AppError):
        raise InternalError()
