"""SQLAlchemy 2.0 declarative models for the AIDP Notify service.

This module is the schema source of truth for the notify service. Every
table participates in the platform's mandatory L1 tenant isolation:

- :class:`NotificationChannel` — one row per (tenant, channel-type) endpoint
  the tenant has registered (e.g. an SMTP relay for email, a Feishu bot
  webhook URL, a generic webhook URL). The transport details live here so
  an admin can rotate the SMTP password or the webhook secret without
  touching templates.
- :class:`NotificationTemplate` — one row per (tenant, name, locale) for
  the per-locale template body. The same logical template can have
  multiple locale variants; sending picks the variant that best matches
  the request's locale (see :mod:`aidp_notify.services.renderer`).
- :class:`NotificationLog` — append-only send log. Every send attempt
  (queued / sent / failed / retry) is recorded with the rendered subject
  + body, the channel id, the response code, and any error text. This
  is the operator's window into "did the user actually get the
  password-reset email?".
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from aidp_common.models import IdModel, TenantScoped, TimestampMixin, utcnow
from sqlalchemy import (
    JSON,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# ---------------------------------------------------------------------------
# Declarative base — service-local metadata, per the AIDP convention that
# each service owns its own ``MetaData`` so cross-service imports do not
# leak. Alembic's ``env.py`` and the test fixtures import it directly from
# this module.
# ---------------------------------------------------------------------------


class Base(DeclarativeBase):
    """SQLAlchemy declarative base for the Notify service."""


# ---------------------------------------------------------------------------
# NotificationChannel
# ---------------------------------------------------------------------------


class NotificationChannel(Base, IdModel, TimestampMixin, TenantScoped):
    """A tenant-registered notification endpoint.

    Each row binds a logical channel type (``email`` / ``feishu`` /
    ``webhook`` / ``sms``) to a concrete transport descriptor
    (``config_json``). For example, an ``email`` row holds the SMTP
    ``host`` / ``port`` / ``username`` / ``password`` / ``from_addr`` /
    ``use_tls`` values; a ``webhook`` row holds the ``url`` + optional
    ``headers`` and ``signing_secret``.

    Attributes:
        tenant_id: Tenant the channel belongs to (L1 isolation key).
        channel: Logical channel type. One of ``"email"``, ``"feishu"``,
            ``"webhook"``, ``"sms"``. Stored as a short string for
            portability; the dispatch layer validates membership.
        name: Human-readable label (e.g. ``"ops-oncall-smtp"``). Unique
            per tenant + channel so a tenant can have several ``email``
            channels side-by-side.
        enabled: Soft-disable flag — ``False`` causes the dispatcher to
            skip the channel without raising.
        config_json: Channel-specific transport descriptor (opaque to
            the schema; the channel implementation parses it).
    """

    __tablename__ = "notification_channels"

    tenant_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("tenants.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    channel: Mapped[str] = mapped_column(String(16), nullable=False)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    enabled: Mapped[bool] = mapped_column(
        Integer, nullable=False, default=1
    )  # SQLite: 0/1; SQLAlchemy maps bool to int on sqlite.
    config_json: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)

    __table_args__ = (
        UniqueConstraint(
            "tenant_id", "channel", "name", name="uq_notification_channels_tenant_name"
        ),
        Index("ix_notification_channels_tenant_channel", "tenant_id", "channel"),
    )

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return (
            f"NotificationChannel(id={self.id!r}, tenant_id={self.tenant_id!r}, "
            f"channel={self.channel!r}, name={self.name!r})"
        )


# ---------------------------------------------------------------------------
# NotificationTemplate
# ---------------------------------------------------------------------------


class NotificationTemplate(Base, IdModel, TimestampMixin, TenantScoped):
    """A tenant-owned notification template, locale-scoped.

    A logical template (``code = "user.welcome"``) can have multiple
    rows — one per ``locale`` — so the dispatcher can pick the best
    variant for a given request. The ``default_locale`` row is the
    fallback when the request does not specify a locale or the requested
    variant is missing.

    Attributes:
        tenant_id: Tenant the template belongs to (L1 isolation key).
        code: Logical template name (e.g. ``"user.welcome"``,
            ``"iam.password_reset"``). Unique per (tenant, code, locale).
        locale: BCP-47-ish locale tag (``"en-US"``, ``"zh-CN"``,
            ``"ja-JP"``). The value ``"default"`` is reserved for the
            fallback variant. Empty / ``None`` is not allowed.
        subject: Subject line (Handlebars-rendered for email). For
            channels that have no subject concept (sms / generic
            webhook) the field is still populated and ignored.
        body: Body (Handlebars-rendered). For ``webhook`` channels the
            field is JSON-encoded into the request body verbatim.
        content_type: MIME / encoding hint. ``"text/plain"`` or
            ``"text/html"`` for email; ``"json"`` for webhook. Stored
            for the channel implementation's convenience.
    """

    __tablename__ = "notification_templates"

    tenant_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("tenants.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    code: Mapped[str] = mapped_column(String(128), nullable=False)
    locale: Mapped[str] = mapped_column(String(16), nullable=False, default="default")
    subject: Mapped[str] = mapped_column(String(512), nullable=False, default="")
    body: Mapped[str] = mapped_column(Text, nullable=False)
    content_type: Mapped[str] = mapped_column(String(32), nullable=False, default="text/plain")

    __table_args__ = (
        UniqueConstraint(
            "tenant_id", "code", "locale", name="uq_notification_templates_tenant_code_locale"
        ),
        Index("ix_notification_templates_tenant_code", "tenant_id", "code"),
    )

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return (
            f"NotificationTemplate(id={self.id!r}, tenant_id={self.tenant_id!r}, "
            f"code={self.code!r}, locale={self.locale!r})"
        )


# ---------------------------------------------------------------------------
# NotificationLog
# ---------------------------------------------------------------------------


class NotificationLog(Base, IdModel, TimestampMixin, TenantScoped):
    """Append-only per-send audit row.

    Every call to :func:`aidp_notify.services.dispatcher.dispatch` writes
    one row per attempted send, plus a row per retry. The terminal
    status (``sent`` / ``failed``) is the row that records the final
    outcome; the intermediate retries (``queued``) let an operator
    reconstruct the timeline of a flaky email send.

    Attributes:
        tenant_id: Tenant the send belongs to (L1 isolation key).
        template_code: The logical template the dispatcher resolved.
            Stored verbatim even when the row represents a failed
            render, so an operator can see *what* was attempted.
        locale: Locale actually used (may differ from the request's
            ``locale`` when the dispatcher fell back to the default).
        channel: Logical channel that carried the send.
        channel_id: FK to :class:`NotificationChannel.id`. ``NULL``
            when the channel was missing at send time (we still log
            the failed lookup so an operator can audit the gap).
        recipient: Address / user-id / webhook URL the message went
            to. Stored verbatim; no PII redaction.
        subject_rendered: The rendered subject line (Handlebars
            substitution complete). Empty for channels that have no
            subject.
        body_rendered: The rendered body. For ``webhook`` channels the
            value is the JSON body that was POSTed. Capped at 8 KiB
            so a giant template cannot blow up the log row.
        status: One of ``"queued"`` (intermediate retry), ``"sent"``
            (terminal success), ``"failed"`` (terminal failure). The
            values are the public contract; new codes require a
            schema migration.
        attempt: 1-based retry index. The first attempt is ``1``; the
            third retry is ``3``. Matches the ``max_retries=3`` knob
            in :mod:`aidp_notify.services.dispatcher`.
        response_code: HTTP / SMTP status code, or ``None`` when the
            transport did not surface one.
        error: Truncated error string (``str(exc)``) for ``"failed"``
            rows. ``None`` for ``"queued"`` / ``"sent"``.
        sent_at: When the terminal status was reached. ``NULL`` for
            ``"queued"`` rows (the timestamp of the original
            insertion is ``created_at``).
    """

    __tablename__ = "notification_logs"

    tenant_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("tenants.id", ondelete="RESTRICT"),
        nullable=False,
        index=True,
    )
    template_code: Mapped[str] = mapped_column(String(128), nullable=False)
    locale: Mapped[str] = mapped_column(String(16), nullable=False, default="default")
    channel: Mapped[str] = mapped_column(String(16), nullable=False)
    channel_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey("notification_channels.id", ondelete="SET NULL"),
        nullable=True,
    )
    recipient: Mapped[str] = mapped_column(String(512), nullable=False)
    subject_rendered: Mapped[str] = mapped_column(String(512), nullable=False, default="")
    body_rendered: Mapped[str] = mapped_column(Text, nullable=False, default="")
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="queued")
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    response_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error: Mapped[str | None] = mapped_column(String(2048), nullable=True)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("ix_notification_logs_tenant_template", "tenant_id", "template_code"),
        Index("ix_notification_logs_tenant_status", "tenant_id", "status"),
        Index("ix_notification_logs_tenant_created_at", "tenant_id", "created_at"),
    )

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return (
            f"NotificationLog(id={self.id!r}, tenant_id={self.tenant_id!r}, "
            f"status={self.status!r}, channel={self.channel!r}, attempt={self.attempt!r})"
        )


__all__ = [
    "Base",
    "NotificationChannel",
    "NotificationLog",
    "NotificationTemplate",
    "utcnow",
]
