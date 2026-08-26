"""Feishu (Lark) bot webhook channel implementation for the Notify service.

Posts the rendered message to a Feishu bot incoming-webhook URL using
:class:`httpx.AsyncClient`. The shape of the body follows the
`Feishu bot documentation <https://open.feishu.cn/document/client-docs/bot-v3/add-custom-bot>`_:

.. code-block:: json

    {
        "msg_type": "text",
        "content": {"text": "<rendered body>"}
    }

When the rendered body parses as a JSON object, the channel forwards
it verbatim under ``content`` (this is how the platform's structured
notifications reach a Feishu bot that has a custom card configured).

Config shape
------------

``config`` must contain:

- ``webhook_url`` (str, required) — the Feishu bot incoming URL.

The recipient is **not** used at the transport layer (Feishu bots
deliver to the room the bot is in). The API still requires a
``recipient`` string in the request so the log row has a meaningful
``recipient`` value; the dispatcher forwards it unchanged.

Error classification
--------------------

- :class:`ChannelSendError` for HTTP 4xx responses (bad URL, the bot
  was removed from the room, auth signature failure). These are
  permanent and not retried.
- :class:`ChannelTransientError` for HTTP 5xx and network errors
  (``httpx.RequestError``). The dispatcher retries up to
  ``max_retries`` times.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

import httpx

from aidp_notify.channels.base import (
    Channel,
    ChannelSendError,
    ChannelTransientError,
    SendOutcome,
)


class FeishuChannel(Channel):
    """Post a message to a Feishu bot incoming webhook."""

    async def send(
        self,
        *,
        config: Mapping[str, Any],
        recipient: str,
        subject: str,
        body: str,
        content_type: str,
    ) -> SendOutcome:
        """POST the rendered body to the Feishu bot webhook.

        Args:
            config: Must contain ``webhook_url`` (str).
            recipient: Forwarded to the log row only; not used by the
                transport.
            subject: Ignored (Feishu bots do not have a subject
                concept; the rendered body carries the full message).
            body: Rendered body. When this parses as a JSON object it
                is forwarded verbatim under ``content``; otherwise it
                is wrapped in ``content.text``.
            content_type: ``"json"`` forces the structured forward;
                anything else wraps the body in ``content.text``.

        Returns:
            A :class:`SendOutcome` whose ``response_code`` is the
            Feishu ``StatusCode`` field (typically ``0`` on success)
            and ``detail`` is the ``msg`` field.
        """
        webhook_url = config.get("webhook_url")
        if not isinstance(webhook_url, str) or not webhook_url:
            raise ChannelSendError("feishu channel config missing required string: webhook_url")

        # Build the Feishu envelope. Per the bot spec the wrapper
        # object is fixed; only ``content`` varies.
        if content_type == "json" or _looks_like_json(body):
            envelope = {"msg_type": "post", "content": {"raw": json.loads(body)}}
        else:
            envelope = {"msg_type": "text", "content": {"text": body}}

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(webhook_url, json=envelope)
        except httpx.RequestError as exc:
            raise ChannelTransientError(
                f"feishu webhook request failed: {exc}",
                detail=str(exc)[:512] or None,
            ) from exc

        status = response.status_code
        if 500 <= status <= 599:
            raise ChannelTransientError(
                f"feishu webhook returned {status}",
                response_code=status,
                detail=response.text[:512] or None,
            )
        if 400 <= status <= 499:
            raise ChannelSendError(
                f"feishu webhook returned {status}",
                response_code=status,
                detail=response.text[:512] or None,
            )

        # Feishu's success response carries a ``StatusCode`` field
        # (0 on success, non-zero on logical failure). We surface it
        # through the outcome so the dispatcher can log it; a
        # non-zero StatusCode is a permanent failure (the bot rejected
        # the message, not a transport problem).
        try:
            payload: dict[str, Any] = response.json()
        except (ValueError, json.JSONDecodeError):
            payload = {}
        feishu_code = payload.get("StatusCode") if isinstance(payload, dict) else None
        if isinstance(feishu_code, int) and feishu_code != 0:
            raise ChannelSendError(
                f"feishu webhook reported StatusCode={feishu_code}",
                response_code=feishu_code,
                detail=str(payload.get("msg"))[:512] if payload.get("msg") else None,
            )
        return SendOutcome(
            response_code=feishu_code if isinstance(feishu_code, int) else status,
            detail=str(payload.get("msg"))[:512] if payload.get("msg") else None,
        )


def _looks_like_json(body: str) -> bool:
    """Return ``True`` when *body* parses as a JSON object (not a list/scalar)."""
    stripped = body.strip()
    if not stripped.startswith("{"):
        return False
    try:
        parsed: object = json.loads(stripped)
    except (ValueError, json.JSONDecodeError):
        return False
    return isinstance(parsed, dict)


__all__ = ["FeishuChannel"]
