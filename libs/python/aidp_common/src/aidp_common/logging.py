"""Structured JSON logging for AIDP Python services.

Log records are emitted as single-line JSON objects that always include
``service`` / ``env`` / ``level`` / ``timestamp`` plus any ``extra=`` fields
the caller passes (e.g. ``tenant_id``, ``trace_id``, ``user_id``). The
formatter is a thin wrapper over :class:`pythonjsonlogger.jsonlogger.JsonFormatter`
that normalizes a few field names for grep-friendliness.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

from pythonjsonlogger.json import JsonFormatter as _BaseJsonFormatter

# Fields emitted on every record regardless of payload. ``None`` means
# "populate lazily from the bound logger".
_DEFAULT_RECORD_FIELDS: dict[str, str | None] = {
    "service": None,
    "env": None,
}


class JsonFormatter(_BaseJsonFormatter):
    """AIDP-flavoured JSON formatter.

    - Normalizes ``levelname`` to a short ``level`` key.
    - Forwards ``extra=`` fields verbatim (``tenant_id`` / ``trace_id`` ...).
    - Strips the ``taskName`` field Python 3.12+ adds.
    """

    def add_fields(
        self,
        log_record: dict[str, Any],
        record: logging.LogRecord,
        message_dict: dict[str, Any],
    ) -> None:
        super().add_fields(log_record, record, message_dict)
        # Short level key.
        log_record["level"] = record.levelname
        # Always include a millisecond-precision ISO timestamp.
        log_record["timestamp"] = self.formatTime(record, self.datefmt)
        # Drop noisy Python 3.12+ fields.
        log_record.pop("taskName", None)
        # Populate service / env from the bound logger if available.
        for field in _DEFAULT_RECORD_FIELDS:
            if field in log_record:
                continue
            bound = getattr(record, field, None)
            if bound is not None:
                log_record[field] = bound


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

_configured: bool = False
_bound_service: str = "aidp-unknown"
_bound_env: str = "dev"


def setup_logging(
    level: str = "INFO",
    service_name: str | None = None,
    env: str | None = None,
    stream: Any | None = None,
) -> None:
    """Configure the root logger to emit JSON to ``stderr``.

    Safe to call multiple times: handlers are replaced, never duplicated.

    Args:
        level: Root log level (case-insensitive: ``"DEBUG"``, ``"info"`` ...).
        service_name: Service label attached to every record. Falls back to
            ``aidp-unknown`` when ``None``.
        env: Deployment environment label attached to every record.
        stream: Output stream. Defaults to :data:`sys.stderr`.
    """
    global _configured, _bound_service, _bound_env

    _bound_service = service_name or _bound_service
    _bound_env = env or _bound_env

    root = logging.getLogger()
    root.setLevel(level.upper())

    # Always tear down AIDP-managed handlers before installing new ones so the
    # function is idempotent and reconfiguration (e.g. level change) is clean.
    for handler in list(root.handlers):
        if getattr(handler, "_aidp_managed", False):
            root.removeHandler(handler)

    handler = logging.StreamHandler(stream if stream is not None else sys.stderr)
    handler.setFormatter(JsonFormatter("%(timestamp)s %(level)s %(name)s %(message)s"))
    handler._aidp_managed = True  # type: ignore[attr-defined]
    root.addHandler(handler)

    # Install bound attributes on the root logger so child loggers inherit
    # ``service`` / ``env`` when they do ``logging.getLogger(__name__)``.
    #
    # We use a lightweight mechanism: stash on the root logger and let the
    # formatter pull it off ``record`` via attribute access.
    root.service = _bound_service  # type: ignore[attr-defined]
    root.env = _bound_env  # type: ignore[attr-defined]

    # Propagate via Filter so every child log emits these fields too.
    service = _bound_service
    environment = _bound_env

    class _BoundFilter(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            if not hasattr(record, "service"):
                record.service = service
            if not hasattr(record, "env"):
                record.env = environment
            # Auto-inject the current OTel trace id so log→trace correlation
            # never silently breaks when call sites forget ``extra={"trace_id":
            # get_trace_id(...)}``. A caller-provided ``trace_id`` always wins.
            # The import is lazy to avoid a circular dependency on
            # ``aidp_common.tracing`` (which itself uses ``logging``). When no
            # span is active we leave the attribute unset so it does not appear
            # as ``null`` in the JSON output.
            if not getattr(record, "trace_id", None):
                from aidp_common.tracing import get_trace_id

                trace_id = get_trace_id(as_hex=True)
                if trace_id is not None:
                    record.trace_id = trace_id
            return True

    handler.addFilter(_BoundFilter())
    _configured = True


def get_logger(name: str) -> logging.Logger:
    """Return a named logger. The root logger must be set up first via :func:`setup_logging`."""
    return logging.getLogger(name)


def is_configured() -> bool:
    """Return ``True`` once :func:`setup_logging` has run at least once."""
    return _configured


__all__ = ["JsonFormatter", "get_logger", "is_configured", "setup_logging"]
