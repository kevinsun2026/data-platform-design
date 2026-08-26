"""Template CRUD endpoints for the Notify service.

The module is the transport adapter for the ``/api/v1/notify/templates``
surface. It parses the request, persists the row, and projects the
ORM model onto the Pydantic response shape from
:mod:`aidp_notify.schemas`.

L1 isolation
------------

Every handler takes the authenticated caller's :class:`CurrentUser`
via the :data:`current_user` dependency. The dependency binds the
request-scoped tenant context via
:func:`aidp_db.tenant.set_tenant_context`, so every downstream ORM
select is auto-filtered by ``WHERE tenant_id = :tid``. A user in
tenant A cannot read, update, or delete a template in tenant B — the
listener will return zero rows and the service layer will surface a
:class:`aidp_common.errors.NotFoundError` (404).
"""

from __future__ import annotations

import logging

from aidp_auth.dependencies import require_permission
from aidp_auth.jwt import CurrentUser
from aidp_common.errors import ConflictError, NotFoundError
from aidp_db.session import get_session
from fastapi import APIRouter, Depends, Query, status
from sqlalchemy import select

from aidp_notify.models import NotificationTemplate
from aidp_notify.schemas import TemplateCreateRequest, TemplateResponse

_LOG = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/notify/templates", tags=["notify-templates"])


# ---------------------------------------------------------------------------
# Permission strings
# ---------------------------------------------------------------------------

_PERM_TEMPLATE_READ = "notify.template.read"
_PERM_TEMPLATE_WRITE = "notify.template.write"


# ---------------------------------------------------------------------------
# Row projection
# ---------------------------------------------------------------------------


def _row_to_template(row: NotificationTemplate) -> TemplateResponse:
    """Project a :class:`NotificationTemplate` ORM row onto the wire shape."""
    return TemplateResponse.model_validate(
        {
            "id": row.id,
            "tenant_id": row.tenant_id,
            "code": row.code,
            "locale": row.locale,
            "subject": row.subject,
            "body": row.body,
            "content_type": row.content_type,
            "created_at": row.created_at,
        }
    )


# ---------------------------------------------------------------------------
# GET /api/v1/notify/templates
# ---------------------------------------------------------------------------


@router.get(
    "",
    response_model=list[TemplateResponse],
    status_code=status.HTTP_200_OK,
    summary="List notification templates for the caller's tenant.",
)
def list_templates(
    code: str | None = Query(
        default=None,
        min_length=1,
        max_length=128,
        description="Filter by logical template name.",
    ),
    user: CurrentUser = Depends(require_permission(_PERM_TEMPLATE_READ)),
) -> list[TemplateResponse]:
    """Return templates for the caller's tenant.

    Optional ``?code=`` filter narrows to a single logical template
    (which may have multiple locale variants). The L1 listener
    auto-injects ``WHERE tenant_id = :tid``; the explicit filter is
    documentation of intent.
    """
    with get_session() as session:
        stmt = select(NotificationTemplate)
        if code is not None:
            stmt = stmt.where(NotificationTemplate.code == code)
        stmt = stmt.where(NotificationTemplate.tenant_id == user.tenant_id)
        rows = (
            session.execute(stmt.order_by(NotificationTemplate.code, NotificationTemplate.locale))
            .scalars()
            .all()
        )
    return [_row_to_template(row) for row in rows]


# ---------------------------------------------------------------------------
# POST /api/v1/notify/templates
# ---------------------------------------------------------------------------


@router.post(
    "",
    response_model=TemplateResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new notification template (or a new locale variant).",
)
def create_template(
    body: TemplateCreateRequest,
    user: CurrentUser = Depends(require_permission(_PERM_TEMPLATE_WRITE)),
) -> TemplateResponse:
    """Create a notification template for the caller's tenant.

    A ``409 Conflict`` is returned when a row with the same
    ``(tenant_id, code, locale)`` already exists. A new locale
    variant of an existing template is created with the same
    ``code`` and a different ``locale``.
    """
    with get_session() as session:
        existing = session.execute(
            select(NotificationTemplate).where(
                NotificationTemplate.tenant_id == user.tenant_id,
                NotificationTemplate.code == body.code,
                NotificationTemplate.locale == body.locale,
            )
        ).scalar_one_or_none()
        if existing is not None:
            raise ConflictError(
                f"template {body.code}@{body.locale} already exists",
            )
        row = NotificationTemplate(
            tenant_id=user.tenant_id,
            code=body.code,
            locale=body.locale,
            subject=body.subject,
            body=body.body,
            content_type=body.content_type,
        )
        session.add(row)
        session.flush()
        return _row_to_template(row)


# ---------------------------------------------------------------------------
# GET /api/v1/notify/templates/{template_id}
# ---------------------------------------------------------------------------


@router.get(
    "/{template_id}",
    response_model=TemplateResponse,
    status_code=status.HTTP_200_OK,
    summary="Fetch one notification template by id.",
)
def get_template(
    template_id: str,
    user: CurrentUser = Depends(require_permission(_PERM_TEMPLATE_READ)),
) -> TemplateResponse:
    """Return one template row.

    A 404 is returned for an id that does not exist (or belongs to
    a different tenant). The L1 listener + the explicit
    ``tenant_id == user.tenant_id`` filter ensure a cross-tenant
    probe is indistinguishable from a missing row.
    """
    with get_session() as session:
        row = session.execute(
            select(NotificationTemplate).where(
                NotificationTemplate.id == template_id,
                NotificationTemplate.tenant_id == user.tenant_id,
            )
        ).scalar_one_or_none()
        if row is None:
            raise NotFoundError("notification template", template_id)
        return _row_to_template(row)


__all__ = ["router"]
