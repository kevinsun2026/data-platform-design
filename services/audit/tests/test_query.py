"""Tests for the Audit service query API.

The tests pin the contract that :mod:`aidp_audit.api.query` ships
in Task 10:

- ``GET /api/v1/audit/events`` returns paginated events for the
  caller's tenant. Filters (``user_id`` / ``action`` / ``from`` /
  ``to`` / ``event_type`` / ``outcome``) narrow the result.
- ``GET /api/v1/audit/events/{id}`` returns the row with the
  *decrypted* payload. Cross-tenant probes are blocked by the L1
  filter (404, not 200).
- ``GET /api/v1/audit/security-events`` returns the security
  events for the caller's tenant.
- Authentication is required on every endpoint — a missing /
  invalid bearer token returns 401.
- The L1 isolation listener auto-injects ``WHERE tenant_id =``
  on every select; the test exercises the cross-tenant guard
  explicitly so a regression in the listener is caught.

The tests use an in-memory SQLite engine wired into the same
``aidp_db.session`` cache the SUT consults. The schema is created
with ``Base.metadata.create_all``; the cross-tenant foreign key
to ``tenants.id`` is bypassed by creating a bare ``tenants`` table
in the same engine so the L1 listener + FK constraint both
function in the test fixture.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pytest
from aidp_audit.consumer import flush_batch
from aidp_audit.models import AidpAuditEvent, Base
from aidp_auth.jwt import create_access_token
from aidp_db.session import get_session
from aidp_events.envelope import new_envelope
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import StaticPool

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _build_engine() -> Engine:
    """Build a fresh in-memory SQLite engine with FK enforcement on."""
    from sqlalchemy import Column, String, Table

    # Register a stub ``tenants`` table on the audit ``Base.metadata``
    # so the audit tables' ``ForeignKey("tenants.id")`` references
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
    def _enable_fk(dbapi_conn, _conn_record):  # pragma: no cover - test helper
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
    """Yield a fresh in-memory SQLite engine with the audit schema applied."""
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
    """Build a fresh audit app with the consumer disabled and the test engine wired in."""
    from aidp_audit import main as audit_main

    monkeypatch.setattr(audit_main, "DISABLE_CONSUMER", True)
    # Force re-create the app so it picks up the new DISABLE_CONSUMER.
    app = audit_main.create_app()
    try:
        yield app
    finally:
        # Re-enable the consumer for the next test (the module-level
        # ``app`` is a shared singleton).
        monkeypatch.setattr(audit_main, "DISABLE_CONSUMER", False)


@pytest.fixture
def client(app: FastAPI) -> Iterator[TestClient]:
    """Wrap the audit app in a synchronous ``TestClient``."""
    with TestClient(app) as c:
        yield c


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _bearer(tenant_id: str, user_id: str = "u-tester") -> dict[str, str]:
    """Return an ``Authorization: Bearer ...`` header for *tenant_id*."""
    token = create_access_token(tenant_id=tenant_id, user_id=user_id)
    return {"Authorization": f"Bearer {token}"}


async def _seed(eng: Engine, *, tenant_id: str, n: int) -> list[str]:
    """Write *n* audit events for *tenant_id* via :func:`flush_batch`."""
    now = datetime.now(UTC)
    envelopes = [
        (
            "iam.audit",
            new_envelope(
                event_type="iam.user.logged_in",
                tenant_id=tenant_id,
                payload={
                    "user_id": f"u-{i}",
                    "ip": f"10.0.0.{i % 256}",
                },
                event_id=f"e-{uuid.uuid4().hex}",
                occurred_at=now - timedelta(seconds=i),
            ),
        )
        for i in range(n)
    ]
    await flush_batch(envelopes)
    # Return the event_ids in occurred_at desc order (most recent first).
    with get_session() as session:
        rows = (
            session.execute(
                __import__("sqlalchemy")
                .select(AidpAuditEvent)
                .where(AidpAuditEvent.tenant_id == tenant_id)
                .order_by(AidpAuditEvent.occurred_at.desc())
            )
            .scalars()
            .all()
        )
    return [row.id for row in rows]


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------


def test_list_events_requires_authentication(client: TestClient) -> None:
    """A missing bearer token returns 401."""
    resp = client.get("/api/v1/audit/events")
    assert resp.status_code == 401
    body = resp.json()
    assert body["code"] == "UNAUTHORIZED"


def test_list_events_rejects_invalid_token(client: TestClient) -> None:
    """A garbage bearer token returns 401 (decoder refuses unknown signature)."""
    resp = client.get(
        "/api/v1/audit/events",
        headers={"Authorization": "Bearer not-a-jwt"},
    )
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# GET /api/v1/audit/events
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_events_returns_paginated_results(
    client: TestClient, wired_engine: Engine
) -> None:
    """The list endpoint paginates by ``page`` / ``page_size``."""
    await _seed(wired_engine, tenant_id="tenant-a", n=5)
    resp = client.get(
        "/api/v1/audit/events?page=1&page_size=3",
        headers=_bearer("tenant-a"),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["page"]["page"] == 1
    assert body["page"]["page_size"] == 3
    assert body["page"]["total"] == 5
    assert len(body["items"]) == 3
    # Items are sorted by occurred_at desc.
    occurred = [item["occurred_at"] for item in body["items"]]
    assert occurred == sorted(occurred, reverse=True)


@pytest.mark.asyncio
async def test_list_events_filters_by_user_id(client: TestClient, wired_engine: Engine) -> None:
    """``user_id`` filters to the ``actor_user_id`` column."""
    # Seed two events for u-1, three for u-2.
    now = datetime.now(UTC)
    envelopes = []
    for i in range(2):
        envelopes.append(
            (
                "iam.audit",
                new_envelope(
                    event_type="iam.user.logged_in",
                    tenant_id="tenant-a",
                    payload={"user_id": "u-1", "ip": "10.0.0.1"},
                    event_id=f"e-u1-{i}",
                    occurred_at=now - timedelta(seconds=i),
                ),
            )
        )
    for i in range(3):
        envelopes.append(
            (
                "iam.audit",
                new_envelope(
                    event_type="iam.user.logged_in",
                    tenant_id="tenant-a",
                    payload={"user_id": "u-2", "ip": "10.0.0.2"},
                    event_id=f"e-u2-{i}",
                    occurred_at=now - timedelta(seconds=10 + i),
                ),
            )
        )
    await flush_batch(envelopes)

    resp = client.get(
        "/api/v1/audit/events?user_id=u-1",
        headers=_bearer("tenant-a"),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["page"]["total"] == 2
    assert all(item["actor_user_id"] == "u-1" for item in body["items"])


@pytest.mark.asyncio
async def test_list_events_filters_by_action(client: TestClient, wired_engine: Engine) -> None:
    """``action`` filters on the derived action column."""
    now = datetime.now(UTC)
    envelopes = [
        (
            "iam.audit",
            new_envelope(
                event_type="iam.user.logged_in",
                tenant_id="tenant-a",
                payload={"user_id": "u-1"},
                event_id=f"e-{uuid.uuid4().hex}",
                occurred_at=now - timedelta(seconds=i),
            ),
        )
        for i in range(2)
    ]
    envelopes.append(
        (
            "iam.audit",
            new_envelope(
                event_type="datasource.connection.created",
                tenant_id="tenant-a",
                payload={"user_id": "u-1", "connection_id": "c-1"},
                event_id=f"e-{uuid.uuid4().hex}",
                occurred_at=now,
            ),
        )
    )
    await flush_batch(envelopes)
    resp = client.get(
        "/api/v1/audit/events?action=created",
        headers=_bearer("tenant-a"),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["page"]["total"] == 1
    assert body["items"][0]["action"] == "created"


@pytest.mark.asyncio
async def test_list_events_filters_by_time_range(client: TestClient, wired_engine: Engine) -> None:
    """``from`` / ``to`` narrow the ``occurred_at`` window."""
    now = datetime.now(UTC)
    await flush_batch(
        [
            (
                "iam.audit",
                new_envelope(
                    event_type="iam.user.logged_in",
                    tenant_id="tenant-a",
                    payload={"user_id": "u-1"},
                    event_id="e-old",
                    occurred_at=now - timedelta(days=10),
                ),
            ),
            (
                "iam.audit",
                new_envelope(
                    event_type="iam.user.logged_in",
                    tenant_id="tenant-a",
                    payload={"user_id": "u-1"},
                    event_id="e-new",
                    occurred_at=now,
                ),
            ),
        ]
    )
    # ``Z`` suffix avoids the URL-encoded ``+00:00`` colon that
    # Pydantic's URL parser refuses to decode in a query string.
    from_iso = (now - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    resp = client.get(
        f"/api/v1/audit/events?from={from_iso}",
        headers=_bearer("tenant-a"),
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["page"]["total"] == 1
    assert body["items"][0]["event_id"] == "e-new"


# ---------------------------------------------------------------------------
# L1 isolation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_list_events_is_tenant_isolated(client: TestClient, wired_engine: Engine) -> None:
    """``tenant-a`` cannot see ``tenant-b``'s events."""
    await _seed(wired_engine, tenant_id="tenant-a", n=3)
    await _seed(wired_engine, tenant_id="tenant-b", n=5)
    resp = client.get(
        "/api/v1/audit/events",
        headers=_bearer("tenant-a"),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["page"]["total"] == 3
    assert all(item["tenant_id"] == "tenant-a" for item in body["items"])


@pytest.mark.asyncio
async def test_get_event_by_id_cross_tenant_returns_404(
    client: TestClient, wired_engine: Engine
) -> None:
    """A tenant-a caller requesting a tenant-b event id gets 404."""
    await _seed(wired_engine, tenant_id="tenant-b", n=1)
    with get_session() as session:
        other_id = (
            session.execute(
                __import__("sqlalchemy")
                .select(AidpAuditEvent)
                .where(AidpAuditEvent.tenant_id == "tenant-b")
            )
            .scalar_one()
            .id
        )
    resp = client.get(
        f"/api/v1/audit/events/{other_id}",
        headers=_bearer("tenant-a"),
    )
    assert resp.status_code == 404
    body = resp.json()
    assert body["code"] == "NOT_FOUND"


# ---------------------------------------------------------------------------
# GET /api/v1/audit/events/{id}
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_event_by_id_returns_decrypted_payload(
    client: TestClient, wired_engine: Engine
) -> None:
    """The detail endpoint returns the payload as a JSON dict (decrypted)."""
    original = {"user_id": "u-42", "ip": "10.0.0.42", "extra": {"a": 1, "b": [2, 3]}}
    await flush_batch(
        [
            (
                "iam.audit",
                new_envelope(
                    event_type="iam.user.logged_in",
                    tenant_id="tenant-a",
                    payload=original,
                    event_id="e-detail-1",
                ),
            )
        ]
    )
    with get_session() as session:
        row = session.execute(
            __import__("sqlalchemy")
            .select(AidpAuditEvent)
            .where(AidpAuditEvent.event_id == "e-detail-1")
        ).scalar_one()
        event_id = row.id
    resp = client.get(
        f"/api/v1/audit/events/{event_id}",
        headers=_bearer("tenant-a"),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["event_id"] == "e-detail-1"
    assert body["payload"] == original
    assert body["actor_user_id"] == "u-42"
    assert body["actor_ip"] == "10.0.0.42"


@pytest.mark.asyncio
async def test_get_event_by_id_missing_returns_404(
    client: TestClient, wired_engine: Engine
) -> None:
    """A non-existent id returns 404."""
    resp = client.get(
        "/api/v1/audit/events/00000000-0000-0000-0000-000000000000",
        headers=_bearer("tenant-a"),
    )
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /api/v1/audit/security-events
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_security_events_endpoint_returns_only_security_promotions(
    client: TestClient, wired_engine: Engine
) -> None:
    """Only events matching ``SECURITY_EVENT_TYPES`` appear in security_events."""
    await flush_batch(
        [
            (
                "iam.audit",
                new_envelope(
                    event_type="iam.user.login_failed",
                    tenant_id="tenant-a",
                    payload={"user_id": "u-1", "reason": "bad_password"},
                    event_id="e-sec",
                ),
            ),
            (
                "iam.audit",
                new_envelope(
                    event_type="iam.user.logged_in",
                    tenant_id="tenant-a",
                    payload={"user_id": "u-1"},
                    event_id="e-ok",
                ),
            ),
        ]
    )
    resp = client.get(
        "/api/v1/audit/security-events",
        headers=_bearer("tenant-a"),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["page"]["total"] == 1
    assert body["items"][0]["event_type"] == "iam.user.login_failed"
    assert body["items"][0]["reason"] == "bad_password"


@pytest.mark.asyncio
async def test_security_events_endpoint_is_tenant_isolated(
    client: TestClient, wired_engine: Engine
) -> None:
    """Cross-tenant access on the security endpoint is blocked too."""
    await flush_batch(
        [
            (
                "iam.audit",
                new_envelope(
                    event_type="iam.user.login_failed",
                    tenant_id="tenant-b",
                    payload={"user_id": "u-1"},
                    event_id="e-sec-b",
                ),
            )
        ]
    )
    resp = client.get(
        "/api/v1/audit/security-events",
        headers=_bearer("tenant-a"),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["page"]["total"] == 0
    assert body["items"] == []


@pytest.mark.asyncio
async def test_security_events_filter_by_severity(client: TestClient, wired_engine: Engine) -> None:
    """``severity`` filter narrows the security list."""
    await flush_batch(
        [
            (
                "iam.audit",
                new_envelope(
                    event_type="iam.user.login_failed",
                    tenant_id="tenant-a",
                    payload={"user_id": "u-1", "severity": "error"},
                    event_id="e-sec-error",
                ),
            ),
            (
                "iam.audit",
                new_envelope(
                    event_type="iam.user.locked",
                    tenant_id="tenant-a",
                    payload={"user_id": "u-1"},
                    event_id="e-sec-warn",
                ),
            ),
        ]
    )
    resp = client.get(
        "/api/v1/audit/security-events?severity=error",
        headers=_bearer("tenant-a"),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["page"]["total"] == 1
    assert body["items"][0]["severity"] == "error"


# ---------------------------------------------------------------------------
# Error envelope
# ---------------------------------------------------------------------------


def test_error_envelope_shape(client: TestClient) -> None:
    """An unauthenticated request surfaces the platform's error envelope."""
    resp = client.get("/api/v1/audit/events")
    assert resp.status_code == 401
    body = resp.json()
    # ``trace_id`` is only attached when an OTel span is recording
    # (it is optional in the envelope contract).
    assert {"code", "message", "details"}.issubset(body.keys())
    assert body["code"] == "UNAUTHORIZED"
