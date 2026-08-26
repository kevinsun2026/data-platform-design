"""Base class + error types for the Notify service's channel layer.

A :class:`Channel` is the seam between the dispatcher and the
underlying transport. The interface is deliberately tiny:

- :meth:`Channel.send` does the I/O. The dispatcher wraps it with
  retry + log persistence.
- The channel raises :class:`ChannelTransientError` for an I/O
  problem that the dispatcher should retry (SMTP timeout, HTTP 5xx).
- The channel raises :class:`ChannelSendError` for a permanent
  failure (HTTP 4xx, bad recipient). The dispatcher does *not*
  retry on a permanent error.
- The channel returns a :class:`SendOutcome` on success.

The channel layer does not depend on FastAPI, the ORM, or the Pydantic
schemas. It only imports the standard library + the transport
libraries (``aiosmtplib`` / ``httpx``). This keeps the unit tests
lightweight and means the channels are reusable from a future CLI
script or background worker.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class SendOutcome:
    """The terminal result of a single channel send attempt.

    Attributes:
        response_code: Transport status code. For SMTP this is the
            ``status`` integer from the SMTP reply tuple; for HTTP this
            is the response status code. ``None`` when the transport
            does not surface a numeric code (e.g. an SMTP connection
            error before the server responded).
        detail: Free-form text detail (e.g. SMTP server greeting, HTTP
            response reason). ``None`` when no detail is available.
            Kept short so a single log row never grows unboundedly.
    """

    response_code: int | None
    detail: str | None = None


class ChannelSendError(Exception):
    """A non-retryable channel failure (bad recipient, 4xx, ...)."""

    def __init__(
        self,
        message: str,
        *,
        response_code: int | None = None,
        detail: str | None = None,
    ) -> None:
        super().__init__(message)
        self.response_code = response_code
        self.detail = detail


class ChannelTransientError(Exception):
    """A retryable channel failure (timeout, 5xx, connection refused, ...)."""

    def __init__(
        self,
        message: str,
        *,
        response_code: int | None = None,
        detail: str | None = None,
    ) -> None:
        super().__init__(message)
        self.response_code = response_code
        self.detail = detail


class Channel(ABC):
    """Abstract base for a transport implementation.

    Subclasses override :meth:`send` and are stateless. The dispatcher
    instantiates one channel per :class:`Channel` type at startup so a
    hot send loop does not pay the construction cost on every attempt.
    """

    @abstractmethod
    async def send(
        self,
        *,
        config: Mapping[str, Any],
        recipient: str,
        subject: str,
        body: str,
        content_type: str,
    ) -> SendOutcome:
        """Send *body* to *recipient* via the transport described by *config*.

        Args:
            config: Channel-specific transport descriptor parsed out of
                :attr:`NotificationChannel.config_json`. The shape is
                documented on the concrete channel.
            recipient: Target address (email / phone / webhook URL /
                user-id — depends on the channel).
            subject: Rendered subject line. Empty for channels that
                do not have a subject concept (sms / webhook).
            body: Rendered body. For ``webhook`` channels this is the
                JSON payload; for ``email`` it is the MIME body.
            content_type: ``"text/plain"`` / ``"text/html"`` / ``"json"``.

        Returns:
            A :class:`SendOutcome` on success.

        Raises:
            ChannelSendError: When the failure is permanent (4xx, bad
                recipient, malformed config). The dispatcher will not
                retry.
            ChannelTransientError: When the failure is transient
                (timeout, 5xx, DNS failure). The dispatcher will
                retry up to ``max_retries`` times.
        """
        raise NotImplementedError  # pragma: no cover - abstract method


__all__ = [
    "Channel",
    "ChannelSendError",
    "ChannelTransientError",
    "SendOutcome",
]
