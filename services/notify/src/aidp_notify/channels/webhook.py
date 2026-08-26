"""Generic webhook channel implementation for the Notify service.

Posts the rendered body to a tenant-supplied HTTPS URL using
:class:`httpx.AsyncClient`. This is the "everything else" channel: a
tenant that wants to pipe a notification into Slack, Microsoft Teams,
PagerDuty, or any other HTTP-receiving system registers a single
``webhook`` channel pointing at that service.

Config shape
------------

``config`` must contain:

- ``url`` (str, required) — target URL. Must be ``http://`` or
  ``https://`` (the channel validates the prefix and rejects anything
  else as a permanent misconfiguration).

``config`` may contain:

- ``headers`` (dict[str, str], optional) — extra request headers
  (e.g. ``Authorization``).
- ``signing_secret`` (str, optional) — when present, the channel
  adds an ``X-AIDP-Signature: sha256=<hex>`` header computed as
  ``HMAC-SHA256(signing_secret, body)``. The receiver uses the same
  secret to verify that the request originated from the notify
  service. A non-empty ``signing_secret`` is required for any tenant
  that wants cryptographic authenticity on the receiving end.

The rendered body is forwarded verbatim as the request body. The
``subject`` is forwarded as the ``X-AIDP-Subject`` header so a
receiver that wants a Slack-style "title" can use it without
duplicating it in the body. ``content_type`` controls the
``Content-Type`` header (``"text/plain"`` / ``"text/html"`` /
``"json"``).

Error classification
--------------------

- :class:`ChannelSendError` for HTTP 4xx (bad URL, auth failure,
  malformed signature). These are permanent and not retried.
- :class:`ChannelTransientError` for HTTP 5xx and network errors
  (``httpx.RequestError``). The dispatcher retries up to
  ``max_retries`` times.
"""

from __future__ import annotations

import hashlib
import hmac
from collections.abc import Mapping
from typing import Any

import httpx

from aidp_notify.channels.base import (
    Channel,
    ChannelSendError,
    ChannelTransientError,
    SendOutcome,
)


class WebhookChannel(Channel):
    """Post the rendered body to a tenant-supplied HTTPS URL."""

    async def send(
        self,
        *,
        config: Mapping[str, Any],
        recipient: str,
        subject: str,
        body: str,
        content_type: str,
    ) -> SendOutcome:
        """POST *body* to ``config['url']`` (or ``recipient``).

        Args:
            config: Must contain ``url`` (str). May contain ``headers``
                and ``signing_secret``.
            recipient: Optional recipient override. When present and
                ``config['url']`` is not, the channel uses
                ``recipient`` as the URL. When both are present the
                config wins. This lets a caller send the same template
                to a different URL per request without re-creating
                the channel row.
            subject: Rendered subject line; forwarded as the
                ``X-AIDP-Subject`` header.
            body: Rendered body. Forwarded verbatim.
            content_type: ``"text/plain"`` / ``"text/html"`` /
                ``"json"``.

        Returns:
            A :class:`SendOutcome` whose ``response_code`` is the
            HTTP status and ``detail`` is the response reason.

        Raises:
            ChannelSendError: Permanent failure (bad config, 4xx).
            ChannelTransientError: Transient failure (5xx, network).
        """
        url_raw = config.get("url") or recipient
        if not isinstance(url_raw, str) or not url_raw:
            raise ChannelSendError("webhook channel config missing url and recipient is empty")
        if not (url_raw.startswith("http://") or url_raw.startswith("https://")):
            raise ChannelSendError(
                f"webhook url must start with http:// or https:// (got {url_raw!r})"
            )

        headers_raw = config.get("headers") or {}
        if not isinstance(headers_raw, dict):
            raise ChannelSendError("webhook config 'headers' must be a dict[str, str]")
        # ``Any`` cast: we accept any value type for the header value
        # and stringify it; an HTTP header is always a string on the
        # wire regardless of what the caller stored.
        headers: dict[str, str] = {
            str(k): v if isinstance(v, str) else str(v) for k, v in headers_raw.items()
        }

        content_type_header = _content_type_header(content_type)
        headers.setdefault("Content-Type", content_type_header)
        if subject:
            headers.setdefault("X-AIDP-Subject", subject)
        if subject:
            headers.setdefault("X-AIDP-Recipient", recipient)

        signing_secret_raw = config.get("signing_secret")
        if isinstance(signing_secret_raw, str) and signing_secret_raw:
            signature = hmac.new(
                signing_secret_raw.encode("utf-8"),
                body.encode("utf-8"),
                hashlib.sha256,
            ).hexdigest()
            headers.setdefault("X-AIDP-Signature", f"sha256={signature}")

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(url_raw, content=body, headers=headers)
        except httpx.RequestError as exc:
            raise ChannelTransientError(
                f"webhook request failed: {exc}",
                detail=str(exc)[:512] or None,
            ) from exc

        status = response.status_code
        if 500 <= status <= 599:
            raise ChannelTransientError(
                f"webhook returned {status}",
                response_code=status,
                detail=response.text[:512] or None,
            )
        if 400 <= status <= 499:
            raise ChannelSendError(
                f"webhook returned {status}",
                response_code=status,
                detail=response.text[:512] or None,
            )
        return SendOutcome(response_code=status, detail=response.reason_phrase)


def _content_type_header(content_type: str) -> str:
    """Map the public ``content_type`` to a wire ``Content-Type`` value."""
    if content_type == "json":
        return "application/json"
    if content_type == "text/html":
        return "text/html; charset=utf-8"
    # Default to plain text; covers ``"text/plain"`` and any
    # unrecognised value (the API validator already restricts the
    # input to the three documented values).
    return "text/plain; charset=utf-8"


__all__ = ["WebhookChannel"]
