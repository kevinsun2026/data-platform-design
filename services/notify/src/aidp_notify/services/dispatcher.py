"""Send orchestration for the Notify service.

This module is the single entry point for actually sending a
notification. The flow:

1. Look up the :class:`NotificationChannel` row for the request
   (``channel`` / ``channel_id``). ``enabled=False`` rows are skipped
   (the dispatcher raises :class:`NotFoundError` rather than silently
   failing so a misconfigured tenant is loud, not quiet).
2. Look up the :class:`NotificationTemplate` row via
   :func:`aidp_notify.services.renderer.select_template` (locale
   cascade + ``"default"`` fallback).
3. Render the subject + body via
   :func:`aidp_notify.services.renderer.render`.
4. Hand the rendered pair to the channel's
   :meth:`Channel.send` (selected by ``channel`` type).
5. On a :class:`ChannelTransientError`, retry up to ``max_retries``
   times (default 3) with a short linear backoff. A
   :class:`ChannelSendError` is recorded immediately without
   retrying.
6. Every attempt is recorded in the :class:`NotificationLog` table so
   an operator can see the full retry timeline.

The function is async; the channel layer is the only place that
opens an SMTP / HTTP connection, and the call site is the only place
that mutates the database.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from aidp_common.errors import NotFoundError, ValidationError
from aidp_db.session import get_session
from aidp_db.tenant import tenant_scope
from sqlalchemy import select
from sqlalchemy.orm import Session

from aidp_notify.channels import Channel, ChannelSendError, ChannelTransientError, get_channel
from aidp_notify.models import NotificationChannel, NotificationLog
from aidp_notify.services.renderer import render, select_template

_LOG = logging.getLogger(__name__)


#: Default retry budget. Matches the brief's "send failure retry 3 times".
DEFAULT_MAX_RETRIES: int = 3


#: Default delay between retries (seconds). A short linear backoff
#: keeps the dispatcher responsive (a stuck SMTP server recovers in
#: < 5s) while not hammering the target on the first transient
#: error. 0.2s x ``max_retries`` keeps the total worst-case send time
#: under a second.
DEFAULT_RETRY_DELAY: float = 0.2


#: Maximum body length (characters) we will store in the
#: :class:`NotificationLog` row. Beyond this we truncate with an
#: ellipsis so a giant template does not blow up the log row.
_BODY_LOG_LIMIT: int = 8192


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DispatchResult:
    """The terminal outcome of a :func:`dispatch` call.

    Attributes:
        log_id: Id of the final :class:`NotificationLog` row (the
            ``"sent"`` or ``"failed"`` row, not the intermediate
            ``"queued"`` rows).
        status: ``"sent"`` on success, ``"failed"`` on terminal
            failure.
        attempts: Total number of attempts made (1..max_retries).
        error: Last error string (``str(exc)``) when *status* is
            ``"failed"``; ``None`` on success.
    """

    log_id: str
    status: str
    attempts: int
    error: str | None = None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _select_channel(
    session: Session,
    *,
    tenant_id: str,
    channel: str,
    channel_id: str | None,
) -> NotificationChannel:
    """Pick the :class:`NotificationChannel` row for this send.

    Args:
        session: Active SQLAlchemy session.
        tenant_id: Tenant the send belongs to.
        channel: Logical channel type (``email`` / ``feishu`` / ...).
        channel_id: Optional explicit id; must match *channel*.

    Returns:
        The matching :class:`NotificationChannel` row.

    Raises:
        NotFoundError: When no row matches the (tenant, channel,
            channel_id) triple.
        ValidationError: When *channel_id* is given but its
            ``channel`` field does not match *channel* (cross-type
            mismatch — a tenant cannot send an email payload through
            a webhook row).
    """
    if channel_id is not None:
        row = session.execute(
            select(NotificationChannel).where(
                NotificationChannel.tenant_id == tenant_id,
                NotificationChannel.id == channel_id,
            )
        ).scalar_one_or_none()
        if row is None:
            raise NotFoundError("notification channel", channel_id)
        if row.channel != channel:
            raise ValidationError(
                f"channel_id={channel_id} is type {row.channel!r}, not {channel!r}",
                details={"channel_id": channel_id, "requested_channel": channel},
            )
        if not row.enabled:
            raise ValidationError(
                f"channel {channel_id} is disabled",
                details={"channel_id": channel_id},
            )
        return row

    # No explicit id — pick the first enabled row of the requested
    # type. ``ORDER BY id`` is deterministic so two matching rows
    # always resolve to the same channel.
    row = session.execute(
        select(NotificationChannel)
        .where(
            NotificationChannel.tenant_id == tenant_id,
            NotificationChannel.channel == channel,
            NotificationChannel.enabled == True,  # noqa: E712 - SQLAlchemy == operator
        )
        .order_by(NotificationChannel.id)
        .limit(1)
    ).scalar_one_or_none()
    if row is None:
        raise NotFoundError("notification channel", f"{channel} (no enabled row)")
    return row


def _truncate(value: str, limit: int) -> str:
    """Truncate *value* to at most *limit* characters, appending an ellipsis."""
    if len(value) <= limit:
        return value
    return value[: max(0, limit - 3)] + "..."


def _log_attempt(
    session: Session,
    *,
    tenant_id: str,
    template_code: str,
    locale: str,
    channel: str,
    channel_id: str | None,
    recipient: str,
    subject_rendered: str,
    body_rendered: str,
    status: str,
    attempt: int,
    response_code: int | None,
    error: str | None,
    sent_at: datetime | None,
) -> NotificationLog:
    """Insert one :class:`NotificationLog` row and return it.

    The function deliberately does **not** commit the surrounding
    transaction — the dispatcher owns the commit boundary so a
    half-written batch is rolled back atomically.
    """
    row = NotificationLog(
        tenant_id=tenant_id,
        template_code=template_code,
        locale=locale,
        channel=channel,
        channel_id=channel_id,
        recipient=recipient,
        subject_rendered=_truncate(subject_rendered, 512),
        body_rendered=_truncate(body_rendered, _BODY_LOG_LIMIT),
        status=status,
        attempt=attempt,
        response_code=response_code,
        error=_truncate(error, 2048) if error is not None else None,
        sent_at=sent_at,
    )
    session.add(row)
    session.flush()
    return row


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


async def dispatch(
    *,
    tenant_id: str,
    channel: str,
    template_code: str,
    locale: str,
    recipient: str,
    vars_: dict[str, Any] | None = None,
    channel_id: str | None = None,
    max_retries: int = DEFAULT_MAX_RETRIES,
    retry_delay: float = DEFAULT_RETRY_DELAY,
    # Test seam: pass a pre-built :class:`Channel` to bypass the
    # factory (e.g. a mock channel for unit tests). ``None`` means
    # "use the factory".
    channel_impl: Channel | None = None,
) -> DispatchResult:
    """Send a rendered notification with retry + log persistence.

    Args:
        tenant_id: Tenant the send belongs to.
        channel: Logical channel type (``email`` / ``feishu`` /
            ``webhook`` / ``sms``).
        template_code: Logical template name.
        locale: Locale tag (e.g. ``"zh-CN"``); ``"default"`` falls
            through to the per-tenant fallback row.
        recipient: Address / phone / webhook URL.
        vars_: Handlebars variables for the template renderer.
        channel_id: Optional explicit channel id.
        max_retries: Retry budget. 1 means "no retries" (single
            attempt). The default 3 matches the brief.
        retry_delay: Sleep between retries (seconds).
        channel_impl: Test seam for substituting a custom
            :class:`Channel` implementation.

    Returns:
        A :class:`DispatchResult` describing the terminal outcome.

    Raises:
        NotFoundError: When the channel or template row is missing.
        ValidationError: When the request is internally inconsistent
            (e.g. *channel_id*'s type does not match *channel*).
    """
    impl: Channel = channel_impl or get_channel(channel)
    vars_ = vars_ or {}

    with tenant_scope(tenant_id), get_session() as session:
        # Pre-fetch the channel + template (and resolve the actual
        # locale used). We do the DB work in one transaction so a
        # single failure rolls back every side-effect.
        ch_row = _select_channel(
            session, tenant_id=tenant_id, channel=channel, channel_id=channel_id
        )
        template_row = select_template(
            session, tenant_id=tenant_id, code=template_code, locale=locale
        )
        actual_locale = template_row.locale
        rendered_subject = render(template_row.subject, vars_)
        rendered_body = render(template_row.body, vars_)

        # For JSON channels the body is forwarded as a structured
        # object (the template body is the JSON source). The render
        # step already substituted ``{{var}}``; if the result is
        # still a JSON object the channel will forward it verbatim.
        # We do **not** json.dumps here because the webhook channel
        # wants the raw string body.
        _ = json  # silence unused-import linters; kept for future refactors

        channel_pk = ch_row.id
        config: dict[str, object] = dict(ch_row.config_json)

        # ----------------------------------------------------------------
        # Attempt loop
        # ----------------------------------------------------------------
        last_error: str | None = None
        attempts_made = 0
        final_log: NotificationLog | None = None
        sent_at: datetime | None = None

        for attempt_index in range(1, max_retries + 1):
            attempts_made = attempt_index
            is_final_attempt = attempt_index == max_retries
            try:
                outcome = await impl.send(
                    config=config,
                    recipient=recipient,
                    subject=rendered_subject,
                    body=rendered_body,
                    content_type=template_row.content_type,
                )
            except ChannelSendError as exc:
                # Permanent failure — log + break.
                last_error = str(exc)
                final_log = _log_attempt(
                    session,
                    tenant_id=tenant_id,
                    template_code=template_code,
                    locale=actual_locale,
                    channel=channel,
                    channel_id=channel_pk,
                    recipient=recipient,
                    subject_rendered=rendered_subject,
                    body_rendered=rendered_body,
                    status="failed",
                    attempt=attempt_index,
                    response_code=exc.response_code,
                    error=last_error,
                    sent_at=datetime.now(UTC),
                )
                _LOG.warning(
                    "notification send failed (permanent)",
                    extra={
                        "tenant_id": tenant_id,
                        "channel": channel,
                        "template": template_code,
                        "attempt": attempt_index,
                    },
                )
                break
            except ChannelTransientError as exc:
                last_error = str(exc)
                if is_final_attempt:
                    final_log = _log_attempt(
                        session,
                        tenant_id=tenant_id,
                        template_code=template_code,
                        locale=actual_locale,
                        channel=channel,
                        channel_id=channel_pk,
                        recipient=recipient,
                        subject_rendered=rendered_subject,
                        body_rendered=rendered_body,
                        status="failed",
                        attempt=attempt_index,
                        response_code=exc.response_code,
                        error=last_error,
                        sent_at=datetime.now(UTC),
                    )
                    _LOG.warning(
                        "notification send failed (transient, retries exhausted)",
                        extra={
                            "tenant_id": tenant_id,
                            "channel": channel,
                            "template": template_code,
                            "attempt": attempt_index,
                        },
                    )
                    break
                # Record the retry row and loop.
                _log_attempt(
                    session,
                    tenant_id=tenant_id,
                    template_code=template_code,
                    locale=actual_locale,
                    channel=channel,
                    channel_id=channel_pk,
                    recipient=recipient,
                    subject_rendered=rendered_subject,
                    body_rendered=rendered_body,
                    status="queued",
                    attempt=attempt_index,
                    response_code=exc.response_code,
                    error=last_error,
                    sent_at=None,
                )
                _LOG.info(
                    "notification send transient error, will retry",
                    extra={
                        "tenant_id": tenant_id,
                        "channel": channel,
                        "template": template_code,
                        "attempt": attempt_index,
                    },
                )
                await asyncio.sleep(retry_delay)
                continue
            except Exception as exc:  # pragma: no cover - defensive guard
                # An unexpected exception is a permanent failure for
                # logging purposes (we cannot predict whether a retry
                # would help). The channel layer should have wrapped
                # its exceptions in ChannelSendError /
                # ChannelTransientError; anything else is a bug.
                last_error = f"unexpected error: {exc}"
                final_log = _log_attempt(
                    session,
                    tenant_id=tenant_id,
                    template_code=template_code,
                    locale=actual_locale,
                    channel=channel,
                    channel_id=channel_pk,
                    recipient=recipient,
                    subject_rendered=rendered_subject,
                    body_rendered=rendered_body,
                    status="failed",
                    attempt=attempt_index,
                    response_code=None,
                    error=last_error,
                    sent_at=datetime.now(UTC),
                )
                _LOG.exception("notification send unexpected error")
                break
            else:
                # Success.
                sent_at = datetime.now(UTC)
                final_log = _log_attempt(
                    session,
                    tenant_id=tenant_id,
                    template_code=template_code,
                    locale=actual_locale,
                    channel=channel,
                    channel_id=channel_pk,
                    recipient=recipient,
                    subject_rendered=rendered_subject,
                    body_rendered=rendered_body,
                    status="sent",
                    attempt=attempt_index,
                    response_code=outcome.response_code,
                    error=None,
                    sent_at=sent_at,
                )
                _LOG.info(
                    "notification sent",
                    extra={
                        "tenant_id": tenant_id,
                        "channel": channel,
                        "template": template_code,
                        "attempt": attempt_index,
                    },
                )
                break

        if final_log is None:  # pragma: no cover - defensive guard
            # Should be unreachable: the loop always either succeeds
            # (sets final_log) or hits a break (also sets final_log).
            raise RuntimeError("dispatcher loop exited without a final log row")

        # ``session`` is committed by the ``get_session`` context
        # manager on normal exit. ``final_log.id`` is populated by
        # the ``flush()`` inside ``_log_attempt``.
        return DispatchResult(
            log_id=final_log.id,
            status=final_log.status,
            attempts=attempts_made,
            error=last_error if final_log.status == "failed" else None,
        )


__all__ = [
    "DEFAULT_MAX_RETRIES",
    "DEFAULT_RETRY_DELAY",
    "DispatchResult",
    "dispatch",
]
