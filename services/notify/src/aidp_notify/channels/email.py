"""Email channel implementation for the Notify service.

Sends a single rendered message via SMTP using :mod:`aiosmtplib`. The
channel parses its own ``config_json`` shape; the API layer treats the
blob as opaque.

Config shape
------------

``config`` must contain:

- ``host`` (str, required) — SMTP server hostname.
- ``port`` (int, required) — SMTP server port (typically ``25``,
  ``465`` for implicit TLS, ``587`` for STARTTLS).
- ``username`` (str, optional) — SMTP auth username.
- ``password`` (str, optional) — SMTP auth password.
- ``from_addr`` (str, required) — the ``From:`` header value.
- ``use_tls`` (bool, default ``True``) — when ``True`` and the port is
  not implicitly TLS (``465``), STARTTLS is negotiated. The
  :func:`aiosmtplib.send` helper accepts ``use_tls`` as a flag.
- ``timeout`` (float, default ``30.0``) — connect / read timeout in
  seconds. Forwarded to :func:`aiosmtplib.send`.

The recipient is the email address (``config`` does not carry a
``to_addr`` because the API caller supplies it). The subject + body
are forwarded verbatim after the template renderer has done the
``{{var}}`` substitution.

Error classification
--------------------

- :class:`ChannelSendError` for a 4xx-style SMTP reply
  (``5xx`` codes are also surfaced as transient; see below) and for
  bad config (missing ``host`` etc.).
- :class:`ChannelTransientError` for a connection-level failure
  (``ConnectionRefusedError``, ``TimeoutError``, SMTP ``4xx`` codes,
  or any 5xx reply) so the dispatcher will retry up to
  ``max_retries`` times.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import aiosmtplib

from aidp_notify.channels.base import (
    Channel,
    ChannelSendError,
    ChannelTransientError,
    SendOutcome,
)

# SMTP status codes that should be treated as transient (4xx) and
# retried. The first digit of an SMTP reply describes the category:
# 2xx success, 3xx intermediate, 4xx transient, 5xx permanent. The
# transient set is the union of 4xx and 5xx — both indicate the
# server is having a problem right now rather than the message being
# malformed.
_TRANSIENT_SMTP_PREFIXES: tuple[str, ...] = ("4", "5")


class EmailChannel(Channel):
    """Send a message via SMTP using :mod:`aiosmtplib`."""

    async def send(
        self,
        *,
        config: Mapping[str, Any],
        recipient: str,
        subject: str,
        body: str,
        content_type: str,
    ) -> SendOutcome:
        """Send *body* to *recipient* over SMTP.

        Args:
            config: SMTP transport descriptor (see module docstring).
            recipient: Destination email address.
            subject: Rendered subject line.
            body: Rendered body.
            content_type: ``"text/plain"`` or ``"text/html"`` (default
                ``"text/plain"``).

        Returns:
            A :class:`SendOutcome` whose ``response_code`` is the SMTP
            status (e.g. ``250``) and ``detail`` is the server's
            reply line.

        Raises:
            ChannelSendError: Permanent failure (missing config).
            ChannelTransientError: Transient failure (network error,
                4xx, 5xx). The dispatcher will retry.
        """
        host = self._str_config(config, "host")
        port = self._int_config(config, "port")
        from_addr = self._str_config(config, "from_addr")
        username = self._optional_str_config(config, "username")
        password = self._optional_str_config(config, "password")
        use_tls_raw = config.get("use_tls", True)
        use_tls = bool(use_tls_raw) if isinstance(use_tls_raw, (bool, int)) else True
        timeout_raw = config.get("timeout", 30.0)
        timeout = float(timeout_raw) if isinstance(timeout_raw, (int, float)) else 30.0

        message = self._build_mime(
            from_addr=from_addr,
            to_addr=recipient,
            subject=subject,
            body=body,
            content_type=content_type,
        )

        try:
            # ``aiosmtplib.send`` opens a fresh SMTP connection per
            # call; for a notification workload the connection
            # overhead is negligible compared to the DNS + TLS
            # handshake cost. A future optimization can reuse a long
            # lived SMTP client (out of scope for Phase 1).
            recipient_responses, response_text = await aiosmtplib.send(
                message,
                hostname=host,
                port=port,
                username=username,
                password=password,
                use_tls=use_tls,
                timeout=timeout,
            )
        except (TimeoutError, OSError, aiosmtplib.SMTPException) as exc:
            # ``OSError`` covers DNS / refused / network unreachable.
            # ``aiosmtplib.SMTPException`` covers protocol errors
            # (TLS handshake, AUTH failure on a 5xx server, etc.).
            # All are surfaced as transient so the dispatcher retries.
            raise ChannelTransientError(
                f"smtp send failed: {exc}",
                detail=str(exc)[:512] or None,
            ) from exc

        # ``recipient_responses`` is a ``dict[str, SMTPResponse]``
        # keyed by recipient address. Each ``SMTPResponse`` is a
        # ``NamedTuple`` with ``code`` and ``message`` fields
        # (a tuple-shaped object — we use the ``[0]`` index to be
        # agnostic to whether the mock returns the namedtuple or a
        # plain tuple). We surface the *last* (= strongest) status
        # code as the outcome; the textual reply is the per-recipient
        # message.
        last_response: aiosmtplib.SMTPResponse | None = None
        for value in recipient_responses.values():
            last_response = value
        if last_response is None:
            # Defensive: aiosmtplib always returns at least one
            # entry for a successful send, but a guard keeps mypy
            # happy without an ``assert``.
            return SendOutcome(
                response_code=None, detail=response_text[:512] if response_text else None
            )
        # ``SMTPResponse`` is a ``NamedTuple`` so ``[0]`` is the
        # integer status code and ``[1]`` is the server's reply
        # line, regardless of the mock shape.
        response_code_int = int(last_response[0])
        response_msg = str(last_response[1]) if len(last_response) > 1 else (response_text or "")
        code_str = str(response_code_int)
        if code_str.startswith(_TRANSIENT_SMTP_PREFIXES):
            raise ChannelTransientError(
                f"smtp server reported {response_code_int}: {response_msg!r}",
                response_code=response_code_int,
                detail=response_msg[:512] if response_msg else None,
            )
        return SendOutcome(
            response_code=response_code_int,
            detail=response_msg[:512] if response_msg else None,
        )

    @staticmethod
    def _str_config(config: Mapping[str, Any], key: str) -> str:
        value = config.get(key)
        if not isinstance(value, str) or not value:
            raise ChannelSendError(f"email channel config missing required string: {key}")
        return value

    @staticmethod
    def _int_config(config: Mapping[str, Any], key: str) -> int:
        value = config.get(key)
        if isinstance(value, bool) or not isinstance(value, int):
            # ``bool`` is a subclass of ``int``; reject it explicitly.
            raise ChannelSendError(f"email channel config missing required int: {key}")
        return value

    @staticmethod
    def _optional_str_config(config: Mapping[str, Any], key: str) -> str | None:
        value = config.get(key)
        if value is None:
            return None
        if not isinstance(value, str):
            raise ChannelSendError(f"email channel config field {key!r} must be a string")
        return value or None

    @staticmethod
    def _build_mime(
        *,
        from_addr: str,
        to_addr: str,
        subject: str,
        body: str,
        content_type: str,
    ) -> str:
        """Render a minimal RFC 5322 message body.

        The implementation uses ``email.message.EmailMessage`` so the
        output is standards-compliant (correct line folding, header
        encoding for non-ASCII subjects, etc.). The returned string
        is the wire shape that :func:`aiosmtplib.send` accepts.
        """
        from email.message import EmailMessage

        msg = EmailMessage()
        msg["From"] = from_addr
        msg["To"] = to_addr
        msg["Subject"] = subject
        if content_type == "text/html":
            msg.set_content("This message requires an HTML-capable client.")
            msg.add_alternative(body, subtype="html")
        else:
            msg.set_content(body)
        # ``EmailMessage.as_string`` returns a fully-encoded string
        # that ``aiosmtplib.send`` can drop directly onto the wire.
        return msg.as_string()


__all__ = ["EmailChannel"]
