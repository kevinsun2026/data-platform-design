"""AIDP Notify channel implementations.

Each module in this package exposes a :class:`Channel` (concrete
subclass of :class:`Channel` in :mod:`aidp_notify.channels.base`) that
the dispatcher can call to send one rendered message. The contract:

- The channel receives a fully-rendered subject + body (Handlebars
  substitution already done) plus a recipient address.
- The channel is responsible for parsing its own ``config_json`` blob
  (host/port for SMTP, URL for webhook, etc.).
- The channel returns a :class:`SendOutcome` so the dispatcher can
  record the result in the log.

The four channels (email / feishu / webhook / sms) live in their own
modules so each can be tested in isolation with the right mocking
strategy (``aiosmtplib`` is mocked via :mod:`unittest.mock`,
``httpx.AsyncClient`` is mocked per-test).
"""

from aidp_notify.channels.base import (
    Channel,
    ChannelSendError,
    ChannelTransientError,
    SendOutcome,
)
from aidp_notify.channels.email import EmailChannel
from aidp_notify.channels.feishu import FeishuChannel
from aidp_notify.channels.sms import SmsChannel
from aidp_notify.channels.webhook import WebhookChannel


def get_channel(channel_type: str) -> Channel:
    """Return the :class:`Channel` implementation for *channel_type*.

    Centralized factory used by the dispatcher. Raises :class:`ValueError`
    for an unknown channel name so the dispatcher can surface a
    validation error before opening any I/O.
    """
    normalized = channel_type.strip().lower()
    if normalized == "email":
        return EmailChannel()
    if normalized == "feishu":
        return FeishuChannel()
    if normalized == "webhook":
        return WebhookChannel()
    if normalized == "sms":
        return SmsChannel()
    raise ValueError(f"unknown channel type: {channel_type!r}")


__all__ = [
    "Channel",
    "ChannelSendError",
    "ChannelTransientError",
    "EmailChannel",
    "FeishuChannel",
    "SendOutcome",
    "SmsChannel",
    "WebhookChannel",
    "get_channel",
]
