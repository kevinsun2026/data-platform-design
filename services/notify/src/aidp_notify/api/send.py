"""Send / channel / log endpoints for the Notify service.

The module is the transport adapter for the rest of the
``/api/v1/notify`` surface:

- ``POST /api/v1/notify/send`` — internal send entry point
  (calls the dispatcher; logs every attempt).
- ``GET  /api/v1/notify/channels`` — list registered channels.
- ``POST /api/v1/notify/channels`` — register a new channel.
- ``GET  /api/v1/notify/logs`` — paginated per-send log
  (filter by channel / status / template_code).

L1 isolation
------------

Every handler takes the authenticated caller's :class:`CurrentUser`
via the :data:`current_user` dependency. The dependency binds the
request-scoped tenant context via
:func:`aidp_db.tenant.set_tenant_context`, so every downstream ORM
select is auto-filtered by ``WHERE tenant_id = :tid``.
"""

from __future__ import annotations

import logging

from aidp_auth.dependencies import require_permission
from aidp_auth.jwt import CurrentUser
from aidp_common.errors import ConflictError, NotFoundError, ValidationError
from aidp_db.session import get_session
from fastapi import APIRouter, Depends, Path, Query, status
from sqlalchemy import func, select

from aidp_notify.models import NotificationChannel, NotificationLog
from aidp_notify.schemas import (
    ChannelCreateRequest,
    ChannelResponse,
    LogListResponse,
    LogResponse,
    PageMeta,
    SendRequest,
    SendResponse,
)
from aidp_notify.services.dispatcher import dispatch

_LOG = logging.getLogger(__name__)

# A single router mounts all three surfaces (send / channels / logs)
# under the ``/api/v1/notify`` prefix. The previous Task 10 / Task 9
# services mount per-resource sub-routers in the same FastAPI app;
# here the surface is small enough that one router keeps the import
# graph in :mod:`aidp_notify.main` tiny.
router = APIRouter(prefix="/api/v1/notify", tags=["notify"])


# ---------------------------------------------------------------------------
# Permission strings
# ---------------------------------------------------------------------------

_PERM_SEND = "notify.send"
_PERM_CHANNEL_READ = "notify.channel.read"
_PERM_CHANNEL_WRITE = "notify.channel.write"
_PERM_LOG_READ = "notify.log.read"


# ---------------------------------------------------------------------------
# Row projection
# ---------------------------------------------------------------------------


def _row_to_channel(row: NotificationChannel) -> ChannelResponse:
    """Project a :class:`NotificationChannel` ORM row onto the wire shape."""
    return ChannelResponse.model_validate(
        {
            "id": row.id,
            "tenant_id": row.tenant_id,
            "channel": row.channel,
            "name": row.name,
            "enabled": bool(row.enabled),
            "config": dict(row.config_json),
            "created_at": row.created_at,
        }
    )


def _row_to_log(row: NotificationLog) -> LogResponse:
    """Project a :class:`NotificationLog` ORM row onto the wire shape."""
    return LogResponse.model_validate(
        {
            "id": row.id,
            "tenant_id": row.tenant_id,
            "template_code": row.template_code,
            "locale": row.locale,
            "channel": row.channel,
            "channel_id": row.channel_id,
            "recipient": row.recipient,
            "subject_rendered": row.subject_rendered,
            "body_rendered": row.body_rendered,
            "status": row.status,
            "attempt": row.attempt,
            "response_code": row.response_code,
            "error": row.error,
            "sent_at": row.sent_at,
            "created_at": row.created_at,
        }
    )


# ---------------------------------------------------------------------------
# POST /api/v1/notify/send
# ---------------------------------------------------------------------------


@router.post(
    "/send",
    response_model=SendResponse,
    status_code=status.HTTP_200_OK,
    summary="Send a rendered notification (with retry + log persistence).",
)
async def send_notification(
    body: SendRequest,
    user: CurrentUser = Depends(require_permission(_PERM_SEND)),
) -> SendResponse:
    """Send one notification for the caller's tenant.

    The handler delegates to :func:`aidp_notify.services.dispatcher.dispatch`,
    which performs the template lookup, rendering, retry loop, and
    log writes. The terminal status is returned in the response so
    the caller can react without re-querying the log; the full
    timeline (one row per attempt) is in :class:`NotificationLog`.

    A 200 with ``status="failed"`` is returned when the send
    ultimately fails after exhausting the retry budget. A
    :class:`NotFoundError` (404) is returned when the channel or
    template row is missing. A :class:`ValidationError` (400) is
    returned when the request is internally inconsistent (e.g.
    *channel_id*'s type does not match *channel*).
    """
    result = await dispatch(
        tenant_id=user.tenant_id,
        channel=body.channel,
        template_code=body.template_code,
        locale=body.locale,
        recipient=body.recipient,
        vars_=body.vars,
        channel_id=body.channel_id,
        max_retries=body.max_retries,
    )
    return SendResponse(
        log_id=result.log_id,
        channel=body.channel,
        template_code=body.template_code,
        locale=body.locale,
        status=result.status,
        attempts=result.attempts,
        error=result.error,
    )


# ---------------------------------------------------------------------------
# GET /api/v1/notify/channels
# ---------------------------------------------------------------------------


@router.get(
    "/channels",
    response_model=list[ChannelResponse],
    status_code=status.HTTP_200_OK,
    summary="List registered notification channels for the caller's tenant.",
)
def list_channels(
    channel: str | None = Query(
        default=None,
        min_length=1,
        max_length=16,
        description="Filter by logical channel type (email/feishu/webhook/sms).",
    ),
    user: CurrentUser = Depends(require_permission(_PERM_CHANNEL_READ)),
) -> list[ChannelResponse]:
    """Return channels for the caller's tenant, optionally filtered by type."""
    with get_session() as session:
        stmt = select(NotificationChannel)
        if channel is not None:
            normalized = channel.strip().lower()
            if normalized not in {"email", "feishu", "webhook", "sms"}:
                raise ValidationError(
                    f"unknown channel type: {channel!r}",
                    details={"channel": channel},
                )
            stmt = stmt.where(NotificationChannel.channel == normalized)
        stmt = stmt.where(NotificationChannel.tenant_id == user.tenant_id)
        rows = (
            session.execute(stmt.order_by(NotificationChannel.channel, NotificationChannel.name))
            .scalars()
            .all()
        )
    return [_row_to_channel(row) for row in rows]


# ---------------------------------------------------------------------------
# POST /api/v1/notify/channels
# ---------------------------------------------------------------------------


@router.post(
    "/channels",
    response_model=ChannelResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Register a new notification channel.",
)
def create_channel(
    body: ChannelCreateRequest,
    user: CurrentUser = Depends(require_permission(_PERM_CHANNEL_WRITE)),
) -> ChannelResponse:
    """Register a new channel for the caller's tenant.

    A ``409 Conflict`` is returned when a row with the same
    ``(tenant_id, channel, name)`` already exists.
    """
    with get_session() as session:
        existing = session.execute(
            select(NotificationChannel).where(
                NotificationChannel.tenant_id == user.tenant_id,
                NotificationChannel.channel == body.channel,
                NotificationChannel.name == body.name,
            )
        ).scalar_one_or_none()
        if existing is not None:
            raise ConflictError(
                f"channel {body.channel}/{body.name} already exists",
            )
        row = NotificationChannel(
            tenant_id=user.tenant_id,
            channel=body.channel,
            name=body.name,
            enabled=1 if body.enabled else 0,
            config_json=dict(body.config),
        )
        session.add(row)
        session.flush()
        return _row_to_channel(row)


# ---------------------------------------------------------------------------
# GET /api/v1/notify/channels/{channel_id}
# ---------------------------------------------------------------------------


@router.get(
    "/channels/{channel_id}",
    response_model=ChannelResponse,
    status_code=status.HTTP_200_OK,
    summary="Fetch one notification channel by id.",
)
def get_channel(
    channel_id: str = Path(..., min_length=1, max_length=36),
    user: CurrentUser = Depends(require_permission(_PERM_CHANNEL_READ)),
) -> ChannelResponse:
    """Return one channel row. 404 on miss / cross-tenant probe."""
    with get_session() as session:
        row = session.execute(
            select(NotificationChannel).where(
                NotificationChannel.id == channel_id,
                NotificationChannel.tenant_id == user.tenant_id,
            )
        ).scalar_one_or_none()
        if row is None:
            raise NotFoundError("notification channel", channel_id)
        return _row_to_channel(row)


# ---------------------------------------------------------------------------
# GET /api/v1/notify/logs
# ---------------------------------------------------------------------------


@router.get(
    "/logs",
    response_model=LogListResponse,
    status_code=status.HTTP_200_OK,
    summary="List notification send log entries for the caller's tenant.",
)
def list_logs(
    template_code: str | None = Query(
        default=None,
        min_length=1,
        max_length=128,
        description="Filter by logical template name.",
    ),
    channel: str | None = Query(
        default=None,
        min_length=1,
        max_length=16,
        description="Filter by logical channel type (email/feishu/webhook/sms).",
    ),
    status_filter: str | None = Query(
        default=None,
        min_length=1,
        max_length=16,
        alias="status",
        description="Filter by send status (queued/sent/failed).",
    ),
    page: int = Query(default=1, ge=1, le=10_000),
    page_size: int = Query(default=50, ge=1, le=200),
    user: CurrentUser = Depends(require_permission(_PERM_LOG_READ)),
) -> LogListResponse:
    """Return send log rows for the caller's tenant, paginated.

    The result is ordered by ``created_at`` desc so the most recent
    send is the first row. A failed-then-retried send produces
    multiple ``NotificationLog`` rows (one per attempt); the
    ``status`` filter lets an operator surface only the terminal
    rows (``sent`` / ``failed``).
    """
    with get_session() as session:
        stmt = select(NotificationLog)
        count_stmt = select(func.count()).select_from(NotificationLog)
        if template_code is not None:
            stmt = stmt.where(NotificationLog.template_code == template_code)
            count_stmt = count_stmt.where(NotificationLog.template_code == template_code)
        if channel is not None:
            normalized = channel.strip().lower()
            if normalized not in {"email", "feishu", "webhook", "sms"}:
                raise ValidationError(
                    f"unknown channel type: {channel!r}",
                    details={"channel": channel},
                )
            stmt = stmt.where(NotificationLog.channel == normalized)
            count_stmt = count_stmt.where(NotificationLog.channel == normalized)
        if status_filter is not None:
            normalized_status = status_filter.strip().lower()
            if normalized_status not in {"queued", "sent", "failed"}:
                raise ValidationError(
                    f"unknown status: {status_filter!r}",
                    details={"status": status_filter},
                )
            stmt = stmt.where(NotificationLog.status == normalized_status)
            count_stmt = count_stmt.where(NotificationLog.status == normalized_status)
        # L1 enforcement: explicit + listener.
        stmt = stmt.where(NotificationLog.tenant_id == user.tenant_id)
        count_stmt = count_stmt.where(NotificationLog.tenant_id == user.tenant_id)
        total = int(session.execute(count_stmt).scalar() or 0)
        rows = (
            session.execute(
                stmt.order_by(NotificationLog.created_at.desc())
                .offset((page - 1) * page_size)
                .limit(page_size)
            )
            .scalars()
            .all()
        )
    return LogListResponse(
        items=[_row_to_log(row) for row in rows],
        page=PageMeta(page=page, page_size=page_size, total=total),
    )


# ---------------------------------------------------------------------------
# GET /api/v1/notify/logs/{log_id}
# ---------------------------------------------------------------------------


@router.get(
    "/logs/{log_id}",
    response_model=LogResponse,
    status_code=status.HTTP_200_OK,
    summary="Fetch one notification log entry by id.",
)
def get_log(
    log_id: str = Path(..., min_length=1, max_length=36),
    user: CurrentUser = Depends(require_permission(_PERM_LOG_READ)),
) -> LogResponse:
    """Return one log row. 404 on miss / cross-tenant probe."""
    with get_session() as session:
        row = session.execute(
            select(NotificationLog).where(
                NotificationLog.id == log_id,
                NotificationLog.tenant_id == user.tenant_id,
            )
        ).scalar_one_or_none()
        if row is None:
            raise NotFoundError("notification log", log_id)
        return _row_to_log(row)


__all__ = ["router"]
