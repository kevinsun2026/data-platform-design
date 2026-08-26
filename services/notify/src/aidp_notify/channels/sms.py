"""SMS channel stub for the Notify service.

The platform's Phase 1 spec lists SMS as one of the four supported
channels but defers a real provider integration (Aliyun / Twilio /
Tencent Cloud) to a later task. The stub:

- Validates the required config keys (``provider`` / ``api_key`` /
  ``from_number``) so a misconfigured row is caught at the API
  layer instead of silently at send time.
- Surfaces a deterministic :class:`ChannelSendError` (NOT a transient
  error) so the dispatcher records a ``failed`` log row instead of
  spinning on retries.

When the real provider lands the implementation will be replaced
in-place. The public contract (:class:`Channel.send`) stays the same
so the dispatcher / API / log row do not need to change.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from aidp_notify.channels.base import Channel, ChannelSendError, SendOutcome


class SmsChannel(Channel):
    """Stub SMS channel.

    Always raises :class:`ChannelSendError` with a clear "not
    implemented" message. The dispatcher still writes a
    ``failed`` log row so an operator can audit every attempt.
    """

    async def send(
        self,
        *,
        config: Mapping[str, Any],
        recipient: str,
        subject: str,
        body: str,
        content_type: str,
    ) -> SendOutcome:
        """Stub send that validates config and refuses to deliver.

        Args:
            config: Must contain ``provider`` (str), ``api_key`` (str),
                ``from_number`` (str). The values are not consumed
                because the provider integration is out of Phase 1
                scope; the channel only checks the shape.
            recipient: Destination phone number (E.164 format).
            subject: Ignored.
            body: Rendered SMS body.
            content_type: Ignored.

        Returns:
            Never returns; always raises :class:`ChannelSendError`.

        Raises:
            ChannelSendError: Always (provider integration is not
                implemented in Phase 1).
        """
        for key in ("provider", "api_key", "from_number"):
            value = config.get(key)
            if not isinstance(value, str) or not value:
                raise ChannelSendError(f"sms channel config missing required string: {key}")
        if not recipient:
            raise ChannelSendError("sms recipient is empty")
        raise ChannelSendError(
            "sms provider integration is not implemented in Phase 1",
            detail="configure a real provider (aliyun / twilio / tencent) in a follow-up task",
        )


__all__ = ["SmsChannel"]
