"""Pydantic v2 request / response models for the Notify service.

The HTTP layer in :mod:`aidp_notify.api` projects ORM rows onto these
models. The split is the platform-standard
``api → service → model → schema`` layering: the ORM models are mutable
SQLAlchemy rows, while the Pydantic models are the immutable wire
shape that the client sees.

Notes on Pydantic v2
--------------------

- Every model has ``model_config = ConfigDict(extra="forbid", ...)`` so
  a misnamed field from a downstream caller surfaces as a 400 instead
  of being silently dropped.
- ``template_code`` is the public identifier; the service uses
  ``code`` internally because Pydantic / SQLAlchemy naming is split.
- The send API accepts a ``template_code`` + ``vars`` + ``locale`` +
  ``recipient`` shape. The channel is selected at send time via
  ``channel`` (default = the first enabled row for the chosen type) or
  via the ``channel_id`` field for advanced callers.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

# ---------------------------------------------------------------------------
# NotificationChannel
# ---------------------------------------------------------------------------


class ChannelConfig(BaseModel):
    """Channel-specific transport descriptor (opaque to the schema).

    The actual keys depend on the channel type:

    - ``email``: ``host``, ``port``, ``username``, ``password``,
      ``from_addr``, ``use_tls`` (bool, default ``True``).
    - ``feishu``: ``webhook_url`` (mandatory).
    - ``webhook``: ``url`` (mandatory), optional ``headers`` (dict),
      optional ``signing_secret`` (HMAC-SHA256 over the body).
    - ``sms``: ``provider``, ``api_key``, ``from_number`` — stub today.

    The notify service does not validate these keys at the API layer
    (they are environment-specific). The channel implementation
    validates the shape it expects.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=False)

    # The base model is intentionally open: callers send arbitrary
    # string keys. We type it as ``dict[str, Any]`` so Pydantic accepts
    # any shape, while still requiring the field to be present.
    pass


class ChannelCreateRequest(BaseModel):
    """Body of ``POST /api/v1/notify/channels``."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=False)

    channel: str = Field(
        min_length=1, max_length=16, description="Logical channel type (email/feishu/webhook/sms)."
    )
    name: str = Field(
        min_length=1, max_length=128, description="Human-readable label (unique per channel)."
    )
    enabled: bool = Field(default=True, description="Soft-disable flag; false skips the channel.")
    config: dict[str, Any] = Field(
        default_factory=dict,
        description="Channel-specific transport descriptor (host/port/secret/etc).",
    )

    @field_validator("channel")
    @classmethod
    def _validate_channel(cls, value: str) -> str:
        normalized = value.strip().lower()
        if normalized not in {"email", "feishu", "webhook", "sms"}:
            raise ValueError("channel must be one of: email, feishu, webhook, sms")
        return normalized


class ChannelResponse(BaseModel):
    """Body of ``GET /api/v1/notify/channels`` (and the POST response)."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=False)

    id: str = Field(min_length=1, max_length=36)
    tenant_id: str = Field(min_length=1, max_length=36)
    channel: str
    name: str
    enabled: bool
    config: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime


# ---------------------------------------------------------------------------
# NotificationTemplate
# ---------------------------------------------------------------------------


class TemplateCreateRequest(BaseModel):
    """Body of ``POST /api/v1/notify/templates``."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=False)

    code: str = Field(
        min_length=1,
        max_length=128,
        description="Logical template name (e.g. ``user.welcome``).",
    )
    locale: str = Field(
        default="default",
        min_length=1,
        max_length=16,
        description="BCP-47-ish locale tag; ``default`` is the fallback variant.",
    )
    subject: str = Field(default="", max_length=512, description="Subject line (Handlebars).")
    body: str = Field(min_length=1, description="Body (Handlebars).")
    content_type: str = Field(
        default="text/plain",
        min_length=1,
        max_length=32,
        description="MIME / encoding hint (text/plain, text/html, json).",
    )

    @field_validator("locale")
    @classmethod
    def _validate_locale(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("locale must be a non-empty string")
        return normalized

    @field_validator("content_type")
    @classmethod
    def _validate_content_type(cls, value: str) -> str:
        normalized = value.strip().lower()
        if normalized not in {"text/plain", "text/html", "json"}:
            raise ValueError("content_type must be one of: text/plain, text/html, json")
        return normalized


class TemplateResponse(BaseModel):
    """Body of ``GET /api/v1/notify/templates`` (and the POST response)."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=False)

    id: str = Field(min_length=1, max_length=36)
    tenant_id: str = Field(min_length=1, max_length=36)
    code: str
    locale: str
    subject: str
    body: str
    content_type: str
    created_at: datetime


# ---------------------------------------------------------------------------
# Send API
# ---------------------------------------------------------------------------


class SendRequest(BaseModel):
    """Body of ``POST /api/v1/notify/send``.

    The dispatcher picks the channel based on ``channel`` (one of
    ``email`` / ``feishu`` / ``webhook`` / ``sms``). If ``channel_id`` is
    also supplied the dispatcher must match; otherwise it uses the
    first enabled channel of the requested type for the tenant.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=False)

    channel: str = Field(
        min_length=1, max_length=16, description="Logical channel (email/feishu/webhook/sms)."
    )
    template_code: str = Field(
        min_length=1, max_length=128, description="Logical template name (e.g. ``user.welcome``)."
    )
    locale: str = Field(
        default="default",
        min_length=1,
        max_length=16,
        description="Locale tag for template selection; ``default`` falls back.",
    )
    recipient: str = Field(
        min_length=1,
        max_length=512,
        description="Address / user-id / webhook URL the message goes to.",
    )
    vars: dict[str, Any] = Field(
        default_factory=dict,
        description="Variables for Handlebars substitution (``{{var}}`` placeholders).",
    )
    channel_id: str | None = Field(
        default=None,
        min_length=1,
        max_length=36,
        description="Optional explicit channel id; must match ``channel``.",
    )
    max_retries: int = Field(
        default=3,
        ge=1,
        le=10,
        description="Retry budget; the dispatcher attempts up to this many times.",
    )

    @field_validator("channel")
    @classmethod
    def _validate_channel(cls, value: str) -> str:
        normalized = value.strip().lower()
        if normalized not in {"email", "feishu", "webhook", "sms"}:
            raise ValueError("channel must be one of: email, feishu, webhook, sms")
        return normalized

    @field_validator("locale")
    @classmethod
    def _validate_locale(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("locale must be a non-empty string")
        return normalized


class SendResponse(BaseModel):
    """Body of ``POST /api/v1/notify/send``.

    Returns the terminal status of the dispatch. When the send
    ultimately fails after exhausting retries, ``status`` is
    ``"failed"`` and ``error`` carries the last error string; the
    :class:`aidp_common.errors.UpstreamError` envelope is **not** used
    here because the HTTP call itself succeeded (the message just
    could not be delivered).
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=False)

    log_id: str = Field(min_length=1, max_length=36)
    channel: str
    template_code: str
    locale: str
    status: str = Field(description="``sent`` on success, ``failed`` on terminal failure.")
    attempts: int = Field(ge=1, description="Total attempts made (1..max_retries).")
    error: str | None = None


# ---------------------------------------------------------------------------
# Logs
# ---------------------------------------------------------------------------


class LogResponse(BaseModel):
    """Body of ``GET /api/v1/notify/logs`` (single row)."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=False)

    id: str = Field(min_length=1, max_length=36)
    tenant_id: str = Field(min_length=1, max_length=36)
    template_code: str
    locale: str
    channel: str
    channel_id: str | None
    recipient: str
    subject_rendered: str
    body_rendered: str
    status: str
    attempt: int
    response_code: int | None
    error: str | None
    sent_at: datetime | None
    created_at: datetime


class PageMeta(BaseModel):
    """Pagination metadata for list endpoints."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=False)

    page: int = Field(ge=1)
    page_size: int = Field(ge=1, le=200)
    total: int = Field(ge=0)


class LogListResponse(BaseModel):
    """Body of ``GET /api/v1/notify/logs``."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=False)

    items: list[LogResponse]
    page: PageMeta


__all__ = [
    "ChannelConfig",
    "ChannelCreateRequest",
    "ChannelResponse",
    "LogListResponse",
    "LogResponse",
    "PageMeta",
    "SendRequest",
    "SendResponse",
    "TemplateCreateRequest",
    "TemplateResponse",
]
