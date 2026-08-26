"""Tests for the Notify service send + template + channel + log API.

The tests pin the contract that :mod:`aidp_notify.api.send` and
:mod:`aidp_notify.api.templates` ship in Task 11:

- ``POST /api/v1/notify/send`` renders a template, sends it through the
  chosen channel (mocked), records every attempt in the log, and
  retries up to 3 times on a transient error.
- ``GET/POST /api/v1/notify/templates`` — locale-aware template CRUD.
- ``GET/POST /api/v1/notify/channels`` — channel CRUD.
- ``GET /api/v1/notify/logs`` — paginated per-send log (filter by
  channel / status / template_code).
- Authentication is required on every endpoint — a missing / invalid
  bearer token returns 401.
- L1 isolation is enforced on every select (cross-tenant probes
  return 404, not 200 with data).
- The unified ``AppError`` envelope is rendered for every domain
  failure.

The tests use an in-memory SQLite engine wired into the same
``aidp_db.session`` cache the SUT consults. The schema is created
with ``Base.metadata.create_all``; the cross-tenant foreign key to
``tenants.id`` is bypassed by creating a bare ``tenants`` table in
the same engine so the L1 listener + FK constraint both function in
the test fixture.

Channel mocking
---------------

The transport layers (``aiosmtplib.send`` for email, the implicit
``httpx.AsyncClient.post`` for feishu + webhook) are patched with
``unittest.mock``. The patches are applied per-test so the test
suite runs in pure-Python (no real SMTP / HTTP server) and the
failure paths are easy to trigger.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from aidp_auth.jwt import create_access_token
from aidp_db.session import get_session
from aidp_notify.models import (
    Base,
    NotificationChannel,
    NotificationLog,
    NotificationTemplate,
)
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import Column, String, Table, create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import StaticPool

# ---------------------------------------------------------------------------
# Engine + tenants fixture (mirrors the audit test layout)
# ---------------------------------------------------------------------------


def _build_engine() -> Engine:
    """Build a fresh in-memory SQLite engine with FK enforcement on."""
    # Register a stub ``tenants`` table on the notify ``Base.metadata``
    # so the notify tables' ``ForeignKey("tenants.id")`` references
    # resolve at ``create_all`` time. The stub mirrors the IAM
    # service's tenants schema at the column level.
    Table(
        "tenants",
        Base.metadata,
        Column("id", String(36), primary_key=True),
        Column("code", String(64), nullable=False, unique=True),
        Column("name", String(255), nullable=False),
        Column("plan", String(32), nullable=False, server_default="free"),
        Column(
            "isolation_level",
            String(16),
            nullable=False,
            server_default="l1",
        ),
        Column(
            "region",
            String(32),
            nullable=False,
            server_default="us-east-1",
        ),
        Column("status", String(16), nullable=False, server_default="active"),
        extend_existing=True,
    )

    eng = create_engine(
        "sqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    from sqlalchemy import event as _event

    @_event.listens_for(eng, "connect")
    def _enable_fk(dbapi_conn: Any, _conn_record: Any) -> None:  # pragma: no cover - test helper
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA foreign_keys=ON")
        cur.close()

    Base.metadata.create_all(eng)
    return eng


def _insert_tenant(*, eng: Engine, tenant_id: str, code: str) -> None:
    """Insert a row into the synthetic ``tenants`` table."""
    with eng.begin() as conn:
        conn.execute(
            text("INSERT INTO tenants (id, code, name) VALUES (:id, :code, :name)"),
            {"id": tenant_id, "code": code, "name": code},
        )


@pytest.fixture
def in_memory_engine() -> Iterator[Engine]:
    """Yield a fresh in-memory SQLite engine with the notify schema applied."""
    eng = _build_engine()
    try:
        yield eng
    finally:
        Base.metadata.drop_all(eng)
        eng.dispose()


@pytest.fixture
def wired_engine(monkeypatch: pytest.MonkeyPatch, in_memory_engine: Engine) -> Iterator[Engine]:
    """Wire the in-memory engine into the SUT's session cache."""
    import aidp_db.session as db_session

    monkeypatch.setattr(db_session, "_engine_cache", {str(in_memory_engine.url): in_memory_engine})
    from aidp_common.config import get_settings

    monkeypatch.setattr(get_settings(), "db_url", str(in_memory_engine.url))

    # Insert the synthetic tenants the tests will reference.
    _insert_tenant(eng=in_memory_engine, tenant_id="tenant-a", code="acme")
    _insert_tenant(eng=in_memory_engine, tenant_id="tenant-b", code="globex")
    try:
        yield in_memory_engine
    finally:
        db_session.reset_engine_cache()


@pytest.fixture
def app(monkeypatch: pytest.MonkeyPatch, wired_engine: Engine) -> Iterator[FastAPI]:
    """Build a fresh notify app with the test engine wired in."""
    from aidp_notify import main as notify_main

    # The lifespan normally runs Alembic migrations; the test
    # engine is already up to date. Re-create the app with the
    # default lifespan so the middleware + error handler are
    # exercised; the TestClient will call lifespan on entry.
    app = notify_main.create_app()
    try:
        yield app
    finally:
        pass


@pytest.fixture
def client(app: FastAPI) -> Iterator[TestClient]:
    """Wrap the notify app in a synchronous ``TestClient``."""
    with TestClient(app) as c:
        yield c


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _bearer(
    tenant_id: str, user_id: str = "u-tester", scopes: list[str] | None = None
) -> dict[str, str]:
    """Return an ``Authorization: Bearer ...`` header for *tenant_id*."""
    token = create_access_token(tenant_id=tenant_id, user_id=user_id, scopes=scopes or ["*"])
    return {"Authorization": f"Bearer {token}"}


def _seed_channel(
    *,
    tenant_id: str = "tenant-a",
    channel: str = "webhook",
    name: str = "ops-default",
    enabled: bool = True,
    config: dict[str, Any] | None = None,
) -> str:
    """Insert a channel row directly via the ORM. Returns its id."""
    cfg = config if config is not None else {"url": "https://example.test/hook"}
    with get_session() as session:
        row = NotificationChannel(
            tenant_id=tenant_id,
            channel=channel,
            name=name,
            enabled=1 if enabled else 0,
            config_json=cfg,
        )
        session.add(row)
        session.flush()
        session.refresh(row)
        return row.id


def _seed_template(
    *,
    tenant_id: str = "tenant-a",
    code: str = "user.welcome",
    locale: str = "default",
    subject: str = "Welcome, {{user.name}}!",
    body: str = "Hi {{user.name}}, your account {{user.email}} is ready.",
    content_type: str = "text/plain",
) -> str:
    """Insert a template row directly via the ORM. Returns its id."""
    with get_session() as session:
        row = NotificationTemplate(
            tenant_id=tenant_id,
            code=code,
            locale=locale,
            subject=subject,
            body=body,
            content_type=content_type,
        )
        session.add(row)
        session.flush()
        session.refresh(row)
        return row.id


def _seed_log(
    *,
    tenant_id: str,
    template_code: str,
    channel: str = "webhook",
    status: str = "sent",
    attempts: int = 1,
    recipient: str = "u-test",
) -> str:
    """Insert one log row directly via the ORM. Returns its id."""
    with get_session() as session:
        row = NotificationLog(
            tenant_id=tenant_id,
            template_code=template_code,
            locale="default",
            channel=channel,
            recipient=recipient,
            subject_rendered="hello",
            body_rendered="world",
            status=status,
            attempt=attempts,
            response_code=200,
            error=None,
            sent_at=datetime.now(UTC),
        )
        session.add(row)
        session.flush()
        session.refresh(row)
        return row.id


# ---------------------------------------------------------------------------
# Auth + envelope shape
# ---------------------------------------------------------------------------


def test_send_requires_authentication(client: TestClient) -> None:
    """A missing bearer token returns 401."""
    resp = client.post(
        "/api/v1/notify/send",
        json={
            "channel": "webhook",
            "template_code": "user.welcome",
            "recipient": "u-test",
        },
    )
    assert resp.status_code == 401
    body = resp.json()
    assert body["code"] == "UNAUTHORIZED"


def test_list_logs_requires_authentication(client: TestClient) -> None:
    """A missing bearer token on the log list returns 401."""
    resp = client.get("/api/v1/notify/logs")
    assert resp.status_code == 401


def test_list_templates_requires_authentication(client: TestClient) -> None:
    """A missing bearer token on the template list returns 401."""
    resp = client.get("/api/v1/notify/templates")
    assert resp.status_code == 401


def test_list_channels_requires_authentication(client: TestClient) -> None:
    """A missing bearer token on the channel list returns 401."""
    resp = client.get("/api/v1/notify/channels")
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Template renderer (Handlebars)
# ---------------------------------------------------------------------------


def test_renderer_substitutes_simple_placeholders() -> None:
    """A flat ``{{var}}`` placeholder resolves to the value."""
    from aidp_notify.services.renderer import render

    assert render("hello {{name}}", {"name": "world"}) == "hello world"


def test_renderer_resolves_dot_paths() -> None:
    """A dot-path placeholder walks the variables dict."""
    from aidp_notify.services.renderer import render

    assert (
        render("{{user.name}} <{{user.email}}>", {"user": {"name": "a", "email": "b"}}) == "a <b>"
    )


def test_renderer_missing_path_is_empty() -> None:
    """A missing path renders as the empty string (no leaked ``{{...}}``)."""
    from aidp_notify.services.renderer import render

    assert render("hi {{user.name}}", {"user": {}}) == "hi "


def test_renderer_handles_non_string_values() -> None:
    """Ints / bools / None are coerced to their string form."""
    from aidp_notify.services.renderer import render

    assert render("{{n}} {{b}} {{none}}", {"n": 42, "b": True, "none": None}) == "42 true "


def test_renderer_passthrough_when_no_placeholders() -> None:
    """A template without ``{{...}}`` is returned verbatim (fast path)."""
    from aidp_notify.services.renderer import render

    assert render("plain text", {"user": "ignored"}) == "plain text"


def test_renderer_does_not_support_helpers() -> None:
    """Handlebars-style helpers / partials are intentionally not supported."""
    from aidp_notify.services.renderer import render

    # A helper invocation should leave the literal text in place
    # (we only replace ``{{var}}`` placeholders, not ``{{#if}}`` /
    # ``{{/if}}`` / ``{{> partial}}``).
    assert render("{{#if x}}body{{/if}}", {"x": True}) == "{{#if x}}body{{/if}}"


def test_renderer_locale_prefix_cascade() -> None:
    """``_locale_prefixes`` walks progressively shorter locale tags."""
    from aidp_notify.services.renderer import _locale_prefixes

    assert _locale_prefixes("zh-CN") == ["zh-CN", "zh"]
    assert _locale_prefixes("zh-Hant-HK") == ["zh-Hant-HK", "zh-Hant", "zh"]
    assert _locale_prefixes("en") == ["en"]
    assert _locale_prefixes("") == []


def test_renderer_selects_exact_locale_match(client: TestClient, wired_engine: Engine) -> None:
    """An exact locale match wins over the language fallback."""
    _seed_channel()
    _seed_template(
        code="user.welcome", locale="default", subject="default subject", body="default body"
    )
    _seed_template(code="user.welcome", locale="zh-CN", subject="zh-CN subject", body="zh-CN body")

    with patch("aidp_notify.services.dispatcher.get_channel") as mock_factory:
        impl = mock_factory.return_value
        impl.send = AsyncMock(return_value=_outcome(200, "ok"))

        resp = client.post(
            "/api/v1/notify/send",
            json={
                "channel": "webhook",
                "template_code": "user.welcome",
                "locale": "zh-CN",
                "recipient": "u-test",
                "vars": {"user": {"name": "alice", "email": "a@x"}},
            },
            headers=_bearer("tenant-a"),
        )
    assert resp.status_code == 200
    impl.send.assert_awaited_once()
    kwargs = impl.send.await_args.kwargs
    assert kwargs["subject"] == "zh-CN subject"
    assert kwargs["body"] == "zh-CN body"  # locale match wins over the default variant


def test_renderer_falls_back_to_default_locale(client: TestClient, wired_engine: Engine) -> None:
    """A request for a missing locale falls back to the default variant."""
    _seed_channel()
    _seed_template(
        code="user.welcome", locale="default", subject="default subject", body="default body"
    )

    with patch("aidp_notify.services.dispatcher.get_channel") as mock_factory:
        impl = mock_factory.return_value
        impl.send = AsyncMock(return_value=_outcome(200, "ok"))

        resp = client.post(
            "/api/v1/notify/send",
            json={
                "channel": "webhook",
                "template_code": "user.welcome",
                "locale": "ja-JP",
                "recipient": "u-test",
                "vars": {},
            },
            headers=_bearer("tenant-a"),
        )
    assert resp.status_code == 200
    kwargs = mock_factory.return_value.send.await_args.kwargs
    assert kwargs["subject"] == "default subject"


def test_renderer_missing_template_returns_404(client: TestClient, wired_engine: Engine) -> None:
    """A request for a template that does not exist returns 404."""
    _seed_channel()
    with patch("aidp_notify.services.dispatcher.get_channel") as mock_factory:
        mock_factory.return_value.send = AsyncMock(return_value=_outcome(200, "ok"))
        resp = client.post(
            "/api/v1/notify/send",
            json={
                "channel": "webhook",
                "template_code": "no.such.template",
                "recipient": "u-test",
            },
            headers=_bearer("tenant-a"),
        )
    assert resp.status_code == 404
    body = resp.json()
    assert body["code"] == "NOT_FOUND"


# ---------------------------------------------------------------------------
# Email channel (mocked SMTP)
# ---------------------------------------------------------------------------


def _outcome(code: int | None, detail: str | None = None) -> Any:
    from aidp_notify.channels.base import SendOutcome

    return SendOutcome(response_code=code, detail=detail)


@pytest.mark.asyncio
async def test_email_channel_sends_via_mocked_smtp() -> None:
    """The email channel calls ``aiosmtplib.send`` and returns the SMTP code."""
    from aidp_notify.channels.email import EmailChannel

    config = {
        "host": "smtp.example.test",
        "port": 587,
        "from_addr": "no-reply@example.test",
        "use_tls": True,
        "timeout": 10.0,
    }
    with patch(
        "aidp_notify.channels.email.aiosmtplib.send",
        new=AsyncMock(return_value=({"user@example.test": (250, "OK")}, "queued")),
    ) as mock_send:
        outcome = await EmailChannel().send(
            config=config,
            recipient="user@example.test",
            subject="hi",
            body="hello",
            content_type="text/plain",
        )
    assert outcome.response_code == 250
    assert outcome.detail == "OK"
    mock_send.assert_awaited_once()


@pytest.mark.asyncio
async def test_email_channel_classifies_4xx_as_transient() -> None:
    """An SMTP 4xx reply raises :class:`ChannelTransientError` (will retry)."""
    from aidp_notify.channels.base import ChannelTransientError
    from aidp_notify.channels.email import EmailChannel

    config = {
        "host": "smtp.example.test",
        "port": 25,
        "from_addr": "no-reply@example.test",
    }
    with (
        patch(
            "aidp_notify.channels.email.aiosmtplib.send",
            new=AsyncMock(
                return_value=({"user@example.test": (421, "Service not available")}, "queued")
            ),
        ),
        pytest.raises(ChannelTransientError) as exc_info,
    ):
        await EmailChannel().send(
            config=config,
            recipient="user@example.test",
            subject="hi",
            body="hello",
            content_type="text/plain",
        )
    assert exc_info.value.response_code == 421


@pytest.mark.asyncio
async def test_email_channel_classifies_timeout_as_transient() -> None:
    """An ``asyncio.TimeoutError`` raises :class:`ChannelTransientError`."""

    from aidp_notify.channels.base import ChannelTransientError
    from aidp_notify.channels.email import EmailChannel

    config = {
        "host": "smtp.example.test",
        "port": 25,
        "from_addr": "no-reply@example.test",
    }
    with (
        patch(
            "aidp_notify.channels.email.aiosmtplib.send",
            new=AsyncMock(side_effect=TimeoutError()),
        ),
        pytest.raises(ChannelTransientError),
    ):
        await EmailChannel().send(
            config=config,
            recipient="user@example.test",
            subject="hi",
            body="hello",
            content_type="text/plain",
        )


@pytest.mark.asyncio
async def test_email_channel_rejects_missing_config() -> None:
    """A channel row without ``host`` raises :class:`ChannelSendError`."""
    from aidp_notify.channels.base import ChannelSendError
    from aidp_notify.channels.email import EmailChannel

    with pytest.raises(ChannelSendError):
        await EmailChannel().send(
            config={"port": 25, "from_addr": "x"},  # no host
            recipient="user@example.test",
            subject="hi",
            body="hello",
            content_type="text/plain",
        )


@pytest.mark.asyncio
async def test_email_channel_rejects_missing_port() -> None:
    """A channel row without ``port`` raises :class:`ChannelSendError`."""
    from aidp_notify.channels.base import ChannelSendError
    from aidp_notify.channels.email import EmailChannel

    with pytest.raises(ChannelSendError):
        await EmailChannel().send(
            config={"host": "smtp.x", "from_addr": "x"},  # no port
            recipient="user@example.test",
            subject="hi",
            body="hello",
            content_type="text/plain",
        )


@pytest.mark.asyncio
async def test_email_channel_rejects_string_port() -> None:
    """A non-int ``port`` raises :class:`ChannelSendError`."""
    from aidp_notify.channels.base import ChannelSendError
    from aidp_notify.channels.email import EmailChannel

    with pytest.raises(ChannelSendError):
        await EmailChannel().send(
            config={"host": "smtp.x", "port": "25", "from_addr": "x"},
            recipient="user@example.test",
            subject="hi",
            body="hello",
            content_type="text/plain",
        )


@pytest.mark.asyncio
async def test_email_channel_rejects_missing_from_addr() -> None:
    """A channel row without ``from_addr`` raises :class:`ChannelSendError`."""
    from aidp_notify.channels.base import ChannelSendError
    from aidp_notify.channels.email import EmailChannel

    with pytest.raises(ChannelSendError):
        await EmailChannel().send(
            config={"host": "smtp.x", "port": 25},  # no from_addr
            recipient="user@example.test",
            subject="hi",
            body="hello",
            content_type="text/plain",
        )


@pytest.mark.asyncio
async def test_email_channel_rejects_non_string_password() -> None:
    """A non-string ``password`` raises :class:`ChannelSendError`."""
    from aidp_notify.channels.base import ChannelSendError
    from aidp_notify.channels.email import EmailChannel

    with pytest.raises(ChannelSendError):
        await EmailChannel().send(
            config={"host": "smtp.x", "port": 25, "from_addr": "x", "password": 12345},
            recipient="user@example.test",
            subject="hi",
            body="hello",
            content_type="text/plain",
        )


@pytest.mark.asyncio
async def test_email_channel_renders_html_alternative() -> None:
    """``content_type=text/html`` adds an ``text/html`` MIME alternative."""

    from aidp_notify.channels.email import EmailChannel

    config = {
        "host": "smtp.example.test",
        "port": 25,
        "from_addr": "no-reply@example.test",
    }
    with patch(
        "aidp_notify.channels.email.aiosmtplib.send",
        new=AsyncMock(
            return_value=({"user@example.test": (250, "OK")}, "ok"),
        ),
    ) as mock_send:
        await EmailChannel().send(
            config=config,
            recipient="user@example.test",
            subject="hi",
            body="<h1>hello</h1>",
            content_type="text/html",
        )
    # The call's first positional arg is the MIME body string.
    assert mock_send.await_args is not None
    message_str = mock_send.await_args.args[0]
    assert "multipart/alternative" in message_str
    assert "<h1>hello</h1>" in message_str


@pytest.mark.asyncio
async def test_email_channel_handles_empty_recipient_responses() -> None:
    """An empty ``recipient_responses`` dict returns ``None`` response_code."""
    from aidp_notify.channels.email import EmailChannel

    config = {
        "host": "smtp.example.test",
        "port": 25,
        "from_addr": "no-reply@example.test",
    }
    with patch(
        "aidp_notify.channels.email.aiosmtplib.send",
        new=AsyncMock(return_value=({}, "queued")),
    ):
        outcome = await EmailChannel().send(
            config=config,
            recipient="user@example.test",
            subject="hi",
            body="hello",
            content_type="text/plain",
        )
    assert outcome.response_code is None
    assert outcome.detail == "queued"


# ---------------------------------------------------------------------------
# Feishu + webhook channels (mocked httpx)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_feishu_channel_posts_to_webhook() -> None:
    """The feishu channel wraps the body in the standard envelope."""
    import httpx
    from aidp_notify.channels.feishu import FeishuChannel

    config = {"webhook_url": "https://open.feishu.cn/hook/xyz"}
    response = httpx.Response(200, json={"StatusCode": 0, "msg": "ok"})
    with patch.object(httpx.AsyncClient, "post", new=AsyncMock(return_value=response)) as mock_post:
        outcome = await FeishuChannel().send(
            config=config,
            recipient="ignored",
            subject="ignored",
            body="hello world",
            content_type="text/plain",
        )
    assert outcome.response_code == 0
    assert outcome.detail == "ok"
    # The envelope shape is the standard Feishu bot contract.
    assert mock_post.await_args is not None
    call_kwargs = mock_post.await_args.kwargs
    payload = call_kwargs["json"]
    assert payload["msg_type"] == "text"
    assert payload["content"]["text"] == "hello world"


@pytest.mark.asyncio
async def test_feishu_channel_classifies_5xx_as_transient() -> None:
    """An HTTP 5xx reply raises :class:`ChannelTransientError`."""
    import httpx
    from aidp_notify.channels.base import ChannelTransientError
    from aidp_notify.channels.feishu import FeishuChannel

    config = {"webhook_url": "https://open.feishu.cn/hook/xyz"}
    response = httpx.Response(500, text="server error")
    with (
        patch.object(httpx.AsyncClient, "post", new=AsyncMock(return_value=response)),
        pytest.raises(ChannelTransientError),
    ):
        await FeishuChannel().send(
            config=config,
            recipient="ignored",
            subject="ignored",
            body="hi",
            content_type="text/plain",
        )


@pytest.mark.asyncio
async def test_feishu_channel_classifies_4xx_as_permanent() -> None:
    """An HTTP 4xx reply raises :class:`ChannelSendError` (no retry)."""
    import httpx
    from aidp_notify.channels.base import ChannelSendError
    from aidp_notify.channels.feishu import FeishuChannel

    config = {"webhook_url": "https://open.feishu.cn/hook/xyz"}
    response = httpx.Response(400, text="bad url")
    with (
        patch.object(httpx.AsyncClient, "post", new=AsyncMock(return_value=response)),
        pytest.raises(ChannelSendError),
    ):
        await FeishuChannel().send(
            config=config,
            recipient="ignored",
            subject="ignored",
            body="hi",
            content_type="text/plain",
        )


@pytest.mark.asyncio
async def test_feishu_channel_classifies_feishu_error_code_as_permanent() -> None:
    """A non-zero Feishu ``StatusCode`` raises :class:`ChannelSendError`."""
    import httpx
    from aidp_notify.channels.base import ChannelSendError
    from aidp_notify.channels.feishu import FeishuChannel

    config = {"webhook_url": "https://open.feishu.cn/hook/xyz"}
    response = httpx.Response(200, json={"StatusCode": 9499, "msg": "rate limited"})
    with (
        patch.object(httpx.AsyncClient, "post", new=AsyncMock(return_value=response)),
        pytest.raises(ChannelSendError) as exc_info,
    ):
        await FeishuChannel().send(
            config=config,
            recipient="ignored",
            subject="ignored",
            body="hi",
            content_type="text/plain",
        )
    assert exc_info.value.response_code == 9499


@pytest.mark.asyncio
async def test_feishu_channel_rejects_missing_webhook_url() -> None:
    """A channel row without ``webhook_url`` raises :class:`ChannelSendError`."""
    from aidp_notify.channels.base import ChannelSendError
    from aidp_notify.channels.feishu import FeishuChannel

    with pytest.raises(ChannelSendError):
        await FeishuChannel().send(
            config={},
            recipient="ignored",
            subject="ignored",
            body="hi",
            content_type="text/plain",
        )


@pytest.mark.asyncio
async def test_feishu_channel_classifies_network_error_as_transient() -> None:
    """An ``httpx.RequestError`` raises :class:`ChannelTransientError`."""
    import httpx
    from aidp_notify.channels.base import ChannelTransientError
    from aidp_notify.channels.feishu import FeishuChannel

    config = {"webhook_url": "https://open.feishu.cn/hook/xyz"}
    with (
        patch.object(
            httpx.AsyncClient,
            "post",
            new=AsyncMock(side_effect=httpx.ConnectError("dns fail")),
        ),
        pytest.raises(ChannelTransientError),
    ):
        await FeishuChannel().send(
            config=config,
            recipient="ignored",
            subject="ignored",
            body="hi",
            content_type="text/plain",
        )


@pytest.mark.asyncio
async def test_feishu_channel_forwards_json_body_verbatim() -> None:
    """``content_type=json`` forwards a JSON body under ``content.raw``."""
    import httpx
    from aidp_notify.channels.feishu import FeishuChannel

    config = {"webhook_url": "https://open.feishu.cn/hook/xyz"}
    response = httpx.Response(200, json={"StatusCode": 0})
    with patch.object(httpx.AsyncClient, "post", new=AsyncMock(return_value=response)) as mock_post:
        await FeishuChannel().send(
            config=config,
            recipient="ignored",
            subject="ignored",
            body='{"k":"v"}',
            content_type="json",
        )
    assert mock_post.await_args is not None
    payload = mock_post.await_args.kwargs["json"]
    assert payload["msg_type"] == "post"
    assert payload["content"] == {"raw": {"k": "v"}}


@pytest.mark.asyncio
async def test_feishu_channel_handles_non_json_response() -> None:
    """A non-JSON success response surfaces the HTTP status as the outcome."""
    import httpx
    from aidp_notify.channels.feishu import FeishuChannel

    config = {"webhook_url": "https://open.feishu.cn/hook/xyz"}
    response = httpx.Response(200, text="<html>not json</html>")
    with patch.object(httpx.AsyncClient, "post", new=AsyncMock(return_value=response)):
        outcome = await FeishuChannel().send(
            config=config,
            recipient="ignored",
            subject="ignored",
            body="hi",
            content_type="text/plain",
        )
    assert outcome.response_code == 200


@pytest.mark.asyncio
async def test_feishu_looks_like_json_helper() -> None:
    """``_looks_like_json`` distinguishes objects from lists / scalars / garbage."""
    from aidp_notify.channels.feishu import _looks_like_json

    assert _looks_like_json('{"a": 1}') is True
    assert _looks_like_json("[]") is False
    assert _looks_like_json("not json") is False
    assert _looks_like_json("") is False
    assert _looks_like_json("[1, 2]") is False
    assert _looks_like_json("123") is False


@pytest.mark.asyncio
async def test_webhook_channel_posts_body_verbatim() -> None:
    """The webhook channel forwards the body verbatim with the right content-type."""
    import httpx
    from aidp_notify.channels.webhook import WebhookChannel

    config = {"url": "https://hooks.example.test/x"}
    response = httpx.Response(200, text="ok")
    with patch.object(httpx.AsyncClient, "post", new=AsyncMock(return_value=response)) as mock_post:
        outcome = await WebhookChannel().send(
            config=config,
            recipient="ignored",
            subject="my-subject",
            body='{"event":"x"}',
            content_type="json",
        )
    assert outcome.response_code == 200
    assert mock_post.await_args is not None
    call_kwargs = mock_post.await_args.kwargs
    assert call_kwargs["content"] == '{"event":"x"}'
    assert call_kwargs["headers"]["Content-Type"] == "application/json"
    assert call_kwargs["headers"]["X-AIDP-Subject"] == "my-subject"


@pytest.mark.asyncio
async def test_webhook_channel_adds_signature_header_when_secret_present() -> None:
    """A ``signing_secret`` adds the ``X-AIDP-Signature`` header."""
    import hashlib
    import hmac

    import httpx
    from aidp_notify.channels.webhook import WebhookChannel

    config = {"url": "https://hooks.example.test/x", "signing_secret": "shh"}
    response = httpx.Response(200, text="ok")
    with patch.object(httpx.AsyncClient, "post", new=AsyncMock(return_value=response)) as mock_post:
        await WebhookChannel().send(
            config=config,
            recipient="ignored",
            subject="",
            body="hello",
            content_type="text/plain",
        )
    expected = "sha256=" + hmac.new(b"shh", b"hello", hashlib.sha256).hexdigest()
    assert mock_post.await_args is not None
    headers = mock_post.await_args.kwargs["headers"]
    assert headers["X-AIDP-Signature"] == expected


@pytest.mark.asyncio
async def test_webhook_channel_classifies_5xx_as_transient() -> None:
    """An HTTP 5xx reply raises :class:`ChannelTransientError`."""
    import httpx
    from aidp_notify.channels.base import ChannelTransientError
    from aidp_notify.channels.webhook import WebhookChannel

    response = httpx.Response(503, text="overloaded")
    with (
        patch.object(httpx.AsyncClient, "post", new=AsyncMock(return_value=response)),
        pytest.raises(ChannelTransientError),
    ):
        await WebhookChannel().send(
            config={"url": "https://hooks.example.test/x"},
            recipient="ignored",
            subject="",
            body="hi",
            content_type="text/plain",
        )


@pytest.mark.asyncio
async def test_webhook_channel_rejects_bad_url() -> None:
    """A non-http(s) URL raises :class:`ChannelSendError`."""
    from aidp_notify.channels.base import ChannelSendError
    from aidp_notify.channels.webhook import WebhookChannel

    with pytest.raises(ChannelSendError):
        await WebhookChannel().send(
            config={"url": "ftp://bad.example.test/"},
            recipient="ignored",
            subject="",
            body="hi",
            content_type="text/plain",
        )


# ---------------------------------------------------------------------------
# SMS stub
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_sms_channel_is_a_stub() -> None:
    """The SMS channel refuses to deliver (provider integration deferred)."""
    from aidp_notify.channels.base import ChannelSendError
    from aidp_notify.channels.sms import SmsChannel

    with pytest.raises(ChannelSendError):
        await SmsChannel().send(
            config={"provider": "aliyun", "api_key": "k", "from_number": "+1234567890"},
            recipient="+1234567890",
            subject="",
            body="hi",
            content_type="text/plain",
        )


@pytest.mark.asyncio
async def test_sms_channel_rejects_missing_config() -> None:
    """A channel row without ``api_key`` raises :class:`ChannelSendError`."""
    from aidp_notify.channels.base import ChannelSendError
    from aidp_notify.channels.sms import SmsChannel

    with pytest.raises(ChannelSendError):
        await SmsChannel().send(
            config={"provider": "aliyun", "from_number": "+1234567890"},
            recipient="+1234567890",
            subject="",
            body="hi",
            content_type="text/plain",
        )


# ---------------------------------------------------------------------------
# Dispatcher: success / retry / permanent failure
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dispatcher_writes_sent_log_on_success(
    client: TestClient, wired_engine: Engine
) -> None:
    """A successful send writes one ``sent`` log row."""
    _seed_channel()
    _seed_template()
    with patch("aidp_notify.services.dispatcher.get_channel") as mock_factory:
        impl = mock_factory.return_value
        impl.send = AsyncMock(return_value=_outcome(200, "ok"))

        resp = client.post(
            "/api/v1/notify/send",
            json={
                "channel": "webhook",
                "template_code": "user.welcome",
                "recipient": "u-test",
                "vars": {"user": {"name": "alice", "email": "a@x"}},
            },
            headers=_bearer("tenant-a"),
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "sent"
    assert body["attempts"] == 1
    assert body["error"] is None
    # The log row was committed; ``get_session`` reads through the
    # same engine the SUT wrote to.
    with get_session() as session:
        rows = session.query(NotificationLog).all()
    assert len(rows) == 1
    assert rows[0].status == "sent"
    assert rows[0].response_code == 200


@pytest.mark.asyncio
async def test_dispatcher_retries_three_times_on_transient(
    client: TestClient, wired_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A transient error triggers 3 attempts; the dispatcher then writes ``failed``."""
    from aidp_notify.channels.base import ChannelTransientError
    from aidp_notify.services import dispatcher as dispatcher_module

    _seed_channel()
    _seed_template()
    # Speed up the retry so the test does not sleep 0.2s x 3.
    monkeypatch.setattr(dispatcher_module, "DEFAULT_RETRY_DELAY", 0.0)

    with patch("aidp_notify.services.dispatcher.get_channel") as mock_factory:
        impl = mock_factory.return_value
        impl.send = AsyncMock(
            side_effect=ChannelTransientError("boom", response_code=503, detail="overloaded")
        )

        resp = client.post(
            "/api/v1/notify/send",
            json={
                "channel": "webhook",
                "template_code": "user.welcome",
                "recipient": "u-test",
            },
            headers=_bearer("tenant-a"),
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "failed"
    assert body["attempts"] == 3
    assert "boom" in (body["error"] or "")
    # 3 attempts → 2 queued rows + 1 failed row.
    with get_session() as session:
        rows = session.query(NotificationLog).order_by(NotificationLog.attempt).all()
    assert [r.status for r in rows] == ["queued", "queued", "failed"]
    assert [r.attempt for r in rows] == [1, 2, 3]
    assert impl.send.await_count == 3


@pytest.mark.asyncio
async def test_dispatcher_recovers_after_transient_retry(
    client: TestClient, wired_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A transient error followed by a success is recorded as ``sent``."""
    from aidp_notify.channels.base import ChannelTransientError, SendOutcome
    from aidp_notify.services import dispatcher as dispatcher_module

    _seed_channel()
    _seed_template()
    monkeypatch.setattr(dispatcher_module, "DEFAULT_RETRY_DELAY", 0.0)

    with patch("aidp_notify.services.dispatcher.get_channel") as mock_factory:
        impl = mock_factory.return_value
        impl.send = AsyncMock(
            side_effect=[
                ChannelTransientError("first try", response_code=503),
                SendOutcome(response_code=200, detail="ok"),
            ]
        )

        resp = client.post(
            "/api/v1/notify/send",
            json={
                "channel": "webhook",
                "template_code": "user.welcome",
                "recipient": "u-test",
            },
            headers=_bearer("tenant-a"),
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "sent"
    assert body["attempts"] == 2
    # 1 queued + 1 sent.
    with get_session() as session:
        rows = session.query(NotificationLog).order_by(NotificationLog.attempt).all()
    assert [r.status for r in rows] == ["queued", "sent"]


@pytest.mark.asyncio
async def test_dispatcher_does_not_retry_on_permanent(
    client: TestClient, wired_engine: Engine
) -> None:
    """A :class:`ChannelSendError` records ``failed`` without retrying."""
    from aidp_notify.channels.base import ChannelSendError

    _seed_channel()
    _seed_template()
    with patch("aidp_notify.services.dispatcher.get_channel") as mock_factory:
        impl = mock_factory.return_value
        impl.send = AsyncMock(side_effect=ChannelSendError("bad recipient", response_code=400))

        resp = client.post(
            "/api/v1/notify/send",
            json={
                "channel": "webhook",
                "template_code": "user.welcome",
                "recipient": "u-test",
            },
            headers=_bearer("tenant-a"),
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "failed"
    assert body["attempts"] == 1
    impl.send.assert_awaited_once()
    with get_session() as session:
        rows = session.query(NotificationLog).all()
    assert len(rows) == 1
    assert rows[0].status == "failed"
    assert "bad recipient" in (rows[0].error or "")


@pytest.mark.asyncio
async def test_dispatcher_honours_max_retries(
    client: TestClient, wired_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``max_retries=1`` means a single attempt (no retry)."""
    from aidp_notify.channels.base import ChannelTransientError
    from aidp_notify.services import dispatcher as dispatcher_module

    _seed_channel()
    _seed_template()
    monkeypatch.setattr(dispatcher_module, "DEFAULT_RETRY_DELAY", 0.0)

    with patch("aidp_notify.services.dispatcher.get_channel") as mock_factory:
        impl = mock_factory.return_value
        impl.send = AsyncMock(side_effect=ChannelTransientError("boom", response_code=503))
        resp = client.post(
            "/api/v1/notify/send",
            json={
                "channel": "webhook",
                "template_code": "user.welcome",
                "recipient": "u-test",
                "max_retries": 1,
            },
            headers=_bearer("tenant-a"),
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "failed"
    assert body["attempts"] == 1
    impl.send.assert_awaited_once()


# ---------------------------------------------------------------------------
# Channel + log + template CRUD + L1 isolation
# ---------------------------------------------------------------------------


def test_create_template_and_list(client: TestClient, wired_engine: Engine) -> None:
    """Create a template, then list it."""
    resp = client.post(
        "/api/v1/notify/templates",
        json={
            "code": "user.welcome",
            "locale": "default",
            "subject": "hi",
            "body": "hello",
        },
        headers=_bearer("tenant-a"),
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["code"] == "user.welcome"
    assert body["locale"] == "default"

    list_resp = client.get(
        "/api/v1/notify/templates?code=user.welcome",
        headers=_bearer("tenant-a"),
    )
    assert list_resp.status_code == 200
    items = list_resp.json()
    assert len(items) == 1
    assert items[0]["id"] == body["id"]


def test_create_template_conflict_on_duplicate_locale(
    client: TestClient, wired_engine: Engine
) -> None:
    """Creating the same ``(code, locale)`` twice returns 409."""
    payload = {
        "code": "user.welcome",
        "locale": "default",
        "subject": "hi",
        "body": "hello",
    }
    headers = _bearer("tenant-a")
    first = client.post("/api/v1/notify/templates", json=payload, headers=headers)
    assert first.status_code == 201
    second = client.post("/api/v1/notify/templates", json=payload, headers=headers)
    assert second.status_code == 409
    assert second.json()["code"] == "CONFLICT"


def test_template_create_rejects_bad_content_type(client: TestClient, wired_engine: Engine) -> None:
    """A Pydantic-validated ``content_type`` is enforced at the API boundary."""
    resp = client.post(
        "/api/v1/notify/templates",
        json={
            "code": "x",
            "subject": "x",
            "body": "x",
            "content_type": "application/zip",
        },
        headers=_bearer("tenant-a"),
    )
    assert resp.status_code == 422


def test_create_channel_and_list(client: TestClient, wired_engine: Engine) -> None:
    """Create a channel, then list it."""
    resp = client.post(
        "/api/v1/notify/channels",
        json={
            "channel": "webhook",
            "name": "ops-default",
            "config": {"url": "https://example.test/hook"},
        },
        headers=_bearer("tenant-a"),
    )
    assert resp.status_code == 201
    body = resp.json()
    assert body["channel"] == "webhook"
    assert body["name"] == "ops-default"
    assert body["config"] == {"url": "https://example.test/hook"}

    list_resp = client.get(
        "/api/v1/notify/channels?channel=webhook",
        headers=_bearer("tenant-a"),
    )
    assert list_resp.status_code == 200
    items = list_resp.json()
    assert len(items) == 1
    assert items[0]["id"] == body["id"]


def test_create_channel_conflict_on_duplicate_name(
    client: TestClient, wired_engine: Engine
) -> None:
    """Creating the same ``(channel, name)`` twice returns 409."""
    payload = {
        "channel": "email",
        "name": "ops-default",
        "config": {"host": "smtp.x", "port": 25, "from_addr": "a@b"},
    }
    headers = _bearer("tenant-a")
    first = client.post("/api/v1/notify/channels", json=payload, headers=headers)
    assert first.status_code == 201
    second = client.post("/api/v1/notify/channels", json=payload, headers=headers)
    assert second.status_code == 409
    assert second.json()["code"] == "CONFLICT"


def test_create_channel_rejects_unknown_type(client: TestClient, wired_engine: Engine) -> None:
    """A Pydantic-validated ``channel`` is enforced at the API boundary."""
    resp = client.post(
        "/api/v1/notify/channels",
        json={"channel": "pigeon", "name": "x", "config": {}},
        headers=_bearer("tenant-a"),
    )
    assert resp.status_code == 422


def test_send_returns_404_when_channel_missing(client: TestClient, wired_engine: Engine) -> None:
    """A send for a tenant with no enabled channel returns 404."""
    _seed_template()  # template exists, but no channel row
    with patch("aidp_notify.services.dispatcher.get_channel") as mock_factory:
        mock_factory.return_value.send = AsyncMock(return_value=_outcome(200, "ok"))
        resp = client.post(
            "/api/v1/notify/send",
            json={
                "channel": "webhook",
                "template_code": "user.welcome",
                "recipient": "u-test",
            },
            headers=_bearer("tenant-a"),
        )
    assert resp.status_code == 404
    body = resp.json()
    assert body["code"] == "NOT_FOUND"
    assert "channel" in body["message"]


def test_send_400_when_channel_id_type_mismatch(client: TestClient, wired_engine: Engine) -> None:
    """A ``channel_id`` of the wrong type returns 400."""
    email_id = _seed_channel(
        channel="email",
        name="smtp",
        config={
            "host": "smtp.x",
            "port": 25,
            "from_addr": "a@b",
        },
    )
    _seed_template()
    with patch("aidp_notify.services.dispatcher.get_channel") as mock_factory:
        mock_factory.return_value.send = AsyncMock(return_value=_outcome(200, "ok"))
        resp = client.post(
            "/api/v1/notify/send",
            json={
                "channel": "webhook",
                "channel_id": email_id,
                "template_code": "user.welcome",
                "recipient": "u-test",
            },
            headers=_bearer("tenant-a"),
        )
    assert resp.status_code == 400
    body = resp.json()
    assert body["code"] == "VALIDATION"


def test_send_rejects_disabled_channel(client: TestClient, wired_engine: Engine) -> None:
    """An explicit ``channel_id`` pointing at a disabled row returns 400."""
    disabled_id = _seed_channel(enabled=False)
    _seed_template()
    with patch("aidp_notify.services.dispatcher.get_channel") as mock_factory:
        mock_factory.return_value.send = AsyncMock(return_value=_outcome(200, "ok"))
        resp = client.post(
            "/api/v1/notify/send",
            json={
                "channel": "webhook",
                "channel_id": disabled_id,
                "template_code": "user.welcome",
                "recipient": "u-test",
            },
            headers=_bearer("tenant-a"),
        )
    assert resp.status_code == 400
    body = resp.json()
    assert body["code"] == "VALIDATION"
    assert "disabled" in body["message"]


# ---------------------------------------------------------------------------
# L1 isolation
# ---------------------------------------------------------------------------


def test_log_list_is_isolated_by_tenant(client: TestClient, wired_engine: Engine) -> None:
    """The log list returns only the caller's tenant's rows."""
    _seed_log(tenant_id="tenant-a", template_code="x")
    _seed_log(tenant_id="tenant-b", template_code="x")
    resp = client.get("/api/v1/notify/logs", headers=_bearer("tenant-a"))
    assert resp.status_code == 200
    body = resp.json()
    assert body["page"]["total"] == 1
    assert all(item["tenant_id"] == "tenant-a" for item in body["items"])


def test_template_get_cross_tenant_returns_404(client: TestClient, wired_engine: Engine) -> None:
    """A template id that exists in tenant A returns 404 to tenant B."""
    template_id = _seed_template(tenant_id="tenant-a")
    resp = client.get(
        f"/api/v1/notify/templates/{template_id}",
        headers=_bearer("tenant-b"),
    )
    assert resp.status_code == 404
    body = resp.json()
    assert body["code"] == "NOT_FOUND"


def test_channel_get_cross_tenant_returns_404(client: TestClient, wired_engine: Engine) -> None:
    """A channel id that exists in tenant A returns 404 to tenant B."""
    channel_id = _seed_channel(tenant_id="tenant-a")
    resp = client.get(
        f"/api/v1/notify/channels/{channel_id}",
        headers=_bearer("tenant-b"),
    )
    assert resp.status_code == 404


def test_log_get_cross_tenant_returns_404(client: TestClient, wired_engine: Engine) -> None:
    """A log id that exists in tenant A returns 404 to tenant B."""
    log_id = _seed_log(tenant_id="tenant-a", template_code="x")
    resp = client.get(
        f"/api/v1/notify/logs/{log_id}",
        headers=_bearer("tenant-b"),
    )
    assert resp.status_code == 404


def test_send_isolated_by_tenant(client: TestClient, wired_engine: Engine) -> None:
    """A send in tenant A does not see tenant B's channel or template."""
    _seed_channel(tenant_id="tenant-b", name="tenant-b-only")
    _seed_template(tenant_id="tenant-b")
    with patch("aidp_notify.services.dispatcher.get_channel") as mock_factory:
        mock_factory.return_value.send = AsyncMock(return_value=_outcome(200, "ok"))
        resp = client.post(
            "/api/v1/notify/send",
            json={
                "channel": "webhook",
                "template_code": "user.welcome",
                "recipient": "u-test",
            },
            headers=_bearer("tenant-a"),
        )
    # No channel or template for tenant A → 404.
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Log list filters + pagination
# ---------------------------------------------------------------------------


def test_log_list_paginates(client: TestClient, wired_engine: Engine) -> None:
    """The log list paginates by ``page`` / ``page_size``."""
    for _ in range(5):
        _seed_log(tenant_id="tenant-a", template_code="x")
    resp = client.get(
        "/api/v1/notify/logs?page=1&page_size=3",
        headers=_bearer("tenant-a"),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["page"]["page"] == 1
    assert body["page"]["page_size"] == 3
    assert body["page"]["total"] == 5
    assert len(body["items"]) == 3


def test_log_list_filters_by_status(client: TestClient, wired_engine: Engine) -> None:
    """``status`` filter narrows the result to the matching rows."""
    _seed_log(tenant_id="tenant-a", template_code="x", status="sent")
    _seed_log(tenant_id="tenant-a", template_code="x", status="failed")
    resp = client.get(
        "/api/v1/notify/logs?status=sent",
        headers=_bearer("tenant-a"),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["page"]["total"] == 1
    assert body["items"][0]["status"] == "sent"


def test_log_list_filters_by_channel(client: TestClient, wired_engine: Engine) -> None:
    """``channel`` filter narrows the result to the matching rows."""
    _seed_log(tenant_id="tenant-a", template_code="x", channel="webhook")
    _seed_log(tenant_id="tenant-a", template_code="x", channel="email")
    resp = client.get(
        "/api/v1/notify/logs?channel=email",
        headers=_bearer("tenant-a"),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["page"]["total"] == 1
    assert body["items"][0]["channel"] == "email"


def test_log_list_filters_by_template(client: TestClient, wired_engine: Engine) -> None:
    """``template_code`` filter narrows the result to the matching rows."""
    _seed_log(tenant_id="tenant-a", template_code="a")
    _seed_log(tenant_id="tenant-a", template_code="b")
    resp = client.get(
        "/api/v1/notify/logs?template_code=a",
        headers=_bearer("tenant-a"),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["page"]["total"] == 1
    assert body["items"][0]["template_code"] == "a"


def test_log_list_rejects_unknown_channel_filter(client: TestClient, wired_engine: Engine) -> None:
    """An unknown channel filter returns 400."""
    resp = client.get(
        "/api/v1/notify/logs?channel=pigeon",
        headers=_bearer("tenant-a"),
    )
    assert resp.status_code == 400
    assert resp.json()["code"] == "VALIDATION"


def test_log_list_rejects_unknown_status_filter(client: TestClient, wired_engine: Engine) -> None:
    """An unknown status filter returns 400."""
    resp = client.get(
        "/api/v1/notify/logs?status=bounced",
        headers=_bearer("tenant-a"),
    )
    assert resp.status_code == 400
    assert resp.json()["code"] == "VALIDATION"


# ---------------------------------------------------------------------------
# App smoke
# ---------------------------------------------------------------------------


def test_healthz(client: TestClient) -> None:
    """``/healthz`` returns 200 even without a database ping."""
    resp = client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_readyz_pings_database(client: TestClient) -> None:
    """``/readyz`` confirms the database is reachable."""
    resp = client.get("/readyz")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ready"
    assert body["database"] in {"sqlite", "postgresql"}


# ---------------------------------------------------------------------------
# Misc helper coverage
# ---------------------------------------------------------------------------


def test_get_channel_factory_raises_for_unknown() -> None:
    """The channel factory raises :class:`ValueError` for an unknown type."""
    from aidp_notify.channels import get_channel

    with pytest.raises(ValueError, match="pigeon"):
        get_channel("pigeon")


def test_log_truncates_oversized_body() -> None:
    """A giant body is truncated in the log row (cap at 8 KiB)."""
    from aidp_notify.services.dispatcher import _truncate

    big = "x" * 10_000
    truncated = _truncate(big, 100)
    assert len(truncated) == 100
    assert truncated.endswith("...")


@pytest.mark.asyncio
async def test_webhook_url_rejects_empty_string() -> None:
    """An empty URL raises :class:`ChannelSendError`."""
    from aidp_notify.channels.base import ChannelSendError
    from aidp_notify.channels.webhook import WebhookChannel

    with pytest.raises(ChannelSendError):
        await WebhookChannel().send(
            config={"url": ""},
            recipient="ignored",
            subject="",
            body="hi",
            content_type="text/plain",
        )
