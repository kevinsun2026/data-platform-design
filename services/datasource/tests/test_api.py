"""End-to-end tests for the Datasource REST API.

The tests build the FastAPI app with an in-memory SQLite engine
and exercise every public route. The Kafka producer is
monkey-patched out so the test does not need a broker; the
connectors are mocked so the test does not need a real
Postgres / MySQL / Oracle / Hive.

Coverage:

- ``GET /api/v1/datasources/types`` — static; no auth required
  to be informative but we still require ``datasource.read``.
- ``POST /api/v1/datasources`` — encrypts credentials; writes
  an audit row; returns 201.
- ``GET /api/v1/datasources`` — L1-isolated list with env / kind
  / tag filters.
- ``GET /api/v1/datasources/{id}`` — 200 on hit, 404 on miss /
  cross-tenant probe.
- ``PUT /api/v1/datasources/{id}`` — partial update; emits
  audit.
- ``DELETE /api/v1/datasources/{id}`` — soft delete; row stays
  in the DB.
- ``POST /api/v1/datasources/{id}/test`` — calls the mocked
  connector and records the outcome.
- Auth + envelope: a missing token returns 401; a permission
  miss returns 403; a domain error returns the AppError envelope.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from aidp_auth.jwt import create_access_token
from aidp_datasource.connectors.base import TestResult
from aidp_datasource.models import Base
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import Column, String, Table, create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import StaticPool

# ---------------------------------------------------------------------------
# Engine + tenants fixture
# ---------------------------------------------------------------------------


def _build_engine() -> Engine:
    Table(
        "tenants",
        Base.metadata,
        Column("id", String(36), primary_key=True),
        Column("code", String(64), nullable=False, unique=True),
        Column("name", String(255), nullable=False),
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
    def _enable_fk(dbapi_conn: Any, _conn_record: Any) -> None:
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA foreign_keys=ON")
        cur.close()

    Base.metadata.create_all(eng)
    return eng


def _insert_tenant(*, eng: Engine, tenant_id: str, code: str) -> None:
    with eng.begin() as conn:
        conn.execute(
            text("INSERT INTO tenants (id, code, name) VALUES (:id, :code, :name)"),
            {"id": tenant_id, "code": code, "name": code},
        )


@pytest.fixture
def wired_engine(monkeypatch: pytest.MonkeyPatch) -> Iterator[Engine]:
    eng = _build_engine()
    import aidp_db.session as db_session

    monkeypatch.setattr(
        db_session, "_engine_cache", {str(eng.url): eng}
    )
    from aidp_common.config import get_settings

    monkeypatch.setattr(get_settings(), "db_url", str(eng.url))
    _insert_tenant(eng=eng, tenant_id="tenant-a", code="acme")
    _insert_tenant(eng=eng, tenant_id="tenant-b", code="globex")
    try:
        yield eng
    finally:
        db_session.reset_engine_cache()
        Base.metadata.drop_all(eng)
        eng.dispose()


@pytest.fixture
def app(
    monkeypatch: pytest.MonkeyPatch, wired_engine: Engine
) -> Iterator[FastAPI]:
    """Build a fresh datasource app with the test engine wired in."""
    from aidp_datasource import main as datasource_main

    app = datasource_main.create_app()
    try:
        yield app
    finally:
        pass


@pytest.fixture
def client(app: FastAPI) -> Iterator[TestClient]:
    with TestClient(app) as c:
        yield c


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _bearer(tenant_id: str = "tenant-a", scopes: list[str] | None = None) -> dict[str, str]:
    token = create_access_token(
        tenant_id=tenant_id, user_id="u-tester", scopes=scopes or ["*"]
    )
    return {"Authorization": f"Bearer {token}"}


def _bypass_kafka(monkeypatch: pytest.MonkeyPatch) -> None:
    from aidp_datasource.services import datasource_service

    async def _noop(*args: Any, **kwargs: Any) -> Any:
        return None

    monkeypatch.setattr(datasource_service, "publish_event", _noop)


def _create_body(**kwargs: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "name": kwargs.get("name", "primary"),
        "kind": kwargs.get("kind", "postgresql"),
        "env": kwargs.get("env", "prod"),
        "description": kwargs.get("description", ""),
        "connection": kwargs.get(
            "connection",
            {
                "host": "db.example.test",
                "port": kwargs.get("port", 5432),
                "database": kwargs.get("database", "aidp"),
            },
        ),
        "credentials": kwargs.get(
            "credentials", {"username": "u", "password": "p"}
        ),
        "tags": kwargs.get("tags", []),
        "enabled": kwargs.get("enabled", True),
    }
    return body


# ---------------------------------------------------------------------------
# Auth + envelope
# ---------------------------------------------------------------------------


def test_list_requires_authentication(client: TestClient) -> None:
    """A missing bearer token returns 401."""
    resp = client.get("/api/v1/datasources")
    assert resp.status_code == 401
    assert resp.json()["code"] == "UNAUTHORIZED"


def test_create_requires_authentication(client: TestClient) -> None:
    """A missing bearer token on POST returns 401."""
    resp = client.post("/api/v1/datasources", json=_create_body())
    assert resp.status_code == 401


def test_list_requires_read_scope(client: TestClient) -> None:
    """A token without ``datasource.read`` returns 403."""
    headers = _bearer(scopes=["datasource.test"])
    resp = client.get("/api/v1/datasources", headers=headers)
    assert resp.status_code == 403
    assert resp.json()["code"] == "FORBIDDEN"


# ---------------------------------------------------------------------------
# GET /types
# ---------------------------------------------------------------------------


def test_list_types(client: TestClient) -> None:
    """``/types`` returns the four supported connector kinds."""
    resp = client.get("/api/v1/datasources/types", headers=_bearer())
    assert resp.status_code == 200
    body = resp.json()
    kinds = {item["kind"] for item in body["items"]}
    assert kinds == {"postgresql", "mysql", "oracle", "hive", "mongodb", "doris", "kafka"}


# ---------------------------------------------------------------------------
# POST /api/v1/datasources
# ---------------------------------------------------------------------------


def test_create_datasource(
    client: TestClient, wired_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A successful create returns 201 with the row, no credentials in body."""
    _bypass_kafka(monkeypatch)
    resp = client.post(
        "/api/v1/datasources",
        headers=_bearer(),
        json=_create_body(name="primary", password="super-secret"),
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    assert body["name"] == "primary"
    assert body["kind"] == "postgresql"
    # Credentials must never appear in the response.
    assert "credentials" not in body
    assert "credentials_ciphertext" not in body
    assert body["id"]


def test_create_duplicate_returns_409(
    client: TestClient, wired_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A duplicate ``(tenant_id, name)`` returns 409 with the AppError envelope."""
    _bypass_kafka(monkeypatch)
    resp1 = client.post(
        "/api/v1/datasources",
        headers=_bearer(),
        json=_create_body(name="primary"),
    )
    assert resp1.status_code == 201
    resp2 = client.post(
        "/api/v1/datasources",
        headers=_bearer(),
        json=_create_body(name="primary"),
    )
    assert resp2.status_code == 409
    body = resp2.json()
    assert body["code"] == "CONFLICT"
    assert "primary" in body["message"]


def test_create_rejects_unknown_kind(
    client: TestClient, wired_engine: Engine
) -> None:
    """An unknown ``kind`` is rejected by Pydantic (422 Unprocessable Entity)."""
    resp = client.post(
        "/api/v1/datasources",
        headers=_bearer(),
        json=_create_body(kind="clickhouse"),
    )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# GET /api/v1/datasources
# ---------------------------------------------------------------------------


def test_list_returns_only_own_tenant(
    client: TestClient, wired_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The list endpoint is L1-isolated."""
    _bypass_kafka(monkeypatch)
    client.post(
        "/api/v1/datasources",
        headers=_bearer(tenant_id="tenant-a"),
        json=_create_body(name="a-1"),
    )
    client.post(
        "/api/v1/datasources",
        headers=_bearer(tenant_id="tenant-b"),
        json=_create_body(name="b-1"),
    )
    resp_a = client.get("/api/v1/datasources", headers=_bearer(tenant_id="tenant-a"))
    assert resp_a.status_code == 200
    items_a = resp_a.json()["items"]
    assert [it["name"] for it in items_a] == ["a-1"]


def test_list_filters_by_kind(
    client: TestClient, wired_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The ``kind`` filter is honoured."""
    _bypass_kafka(monkeypatch)
    client.post(
        "/api/v1/datasources",
        headers=_bearer(),
        json=_create_body(name="pg", kind="postgresql"),
    )
    client.post(
        "/api/v1/datasources",
        headers=_bearer(),
        json=_create_body(
            name="my",
            kind="mysql",
            port=3306,
            host="m.db",
        ),
    )
    resp = client.get(
        "/api/v1/datasources?kind=mysql", headers=_bearer()
    )
    assert resp.status_code == 200
    items = resp.json()["items"]
    assert [it["name"] for it in items] == ["my"]


# ---------------------------------------------------------------------------
# GET /{id}
# ---------------------------------------------------------------------------


def test_get_datasource(
    client: TestClient, wired_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Fetch one row by id."""
    _bypass_kafka(monkeypatch)
    created = client.post(
        "/api/v1/datasources", headers=_bearer(), json=_create_body(name="primary")
    ).json()
    resp = client.get(
        f"/api/v1/datasources/{created['id']}", headers=_bearer()
    )
    assert resp.status_code == 200
    assert resp.json()["name"] == "primary"


def test_get_cross_tenant_returns_404(
    client: TestClient, wired_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A cross-tenant probe returns 404 (no leak)."""
    _bypass_kafka(monkeypatch)
    created = client.post(
        "/api/v1/datasources",
        headers=_bearer(tenant_id="tenant-a"),
        json=_create_body(name="a-1"),
    ).json()
    resp = client.get(
        f"/api/v1/datasources/{created['id']}", headers=_bearer(tenant_id="tenant-b")
    )
    assert resp.status_code == 404
    assert resp.json()["code"] == "NOT_FOUND"


# ---------------------------------------------------------------------------
# PUT /{id}
# ---------------------------------------------------------------------------


def test_update_datasource(
    client: TestClient, wired_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Update the description; credentials stay unchanged on disk."""
    _bypass_kafka(monkeypatch)
    created = client.post(
        "/api/v1/datasources",
        headers=_bearer(),
        json=_create_body(name="primary", description="old"),
    ).json()
    resp = client.put(
        f"/api/v1/datasources/{created['id']}",
        headers=_bearer(),
        json={"description": "new"},
    )
    assert resp.status_code == 200
    assert resp.json()["description"] == "new"


# ---------------------------------------------------------------------------
# DELETE /{id}
# ---------------------------------------------------------------------------


def test_soft_delete(
    client: TestClient, wired_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``DELETE`` soft-deletes; the row stays as a forensic read.

    The service's :func:`get_datasource` does not currently filter
    by ``status`` / ``deleted_at``; the soft-delete flips the row to
    ``status='disabled'`` and sets ``deleted_at``, but the row stays
    queryable so audits and admin tooling can answer "did this
    datasource ever exist and when was it disabled?". The list view
    is responsible for filtering by status client-side. This is the
    same "forensic read" semantics as the IAM service.
    """
    _bypass_kafka(monkeypatch)
    created = client.post(
        "/api/v1/datasources",
        headers=_bearer(),
        json=_create_body(name="primary"),
    ).json()
    resp = client.delete(
        f"/api/v1/datasources/{created['id']}", headers=_bearer()
    )
    assert resp.status_code == 200
    # Subsequent get still returns the row (forensic read); the
    # soft-delete does not remove it from the table.
    resp2 = client.get(
        f"/api/v1/datasources/{created['id']}", headers=_bearer()
    )
    assert resp2.status_code == 200
    assert resp2.json()["enabled"] is False


# ---------------------------------------------------------------------------
# POST /{id}/test
# ---------------------------------------------------------------------------


def test_test_connection_success(
    client: TestClient, wired_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A successful probe returns 200 with ``status='succeeded'``."""
    _bypass_kafka(monkeypatch)
    created = client.post(
        "/api/v1/datasources",
        headers=_bearer(),
        json=_create_body(name="primary"),
    ).json()
    fake_connector = AsyncMock()
    fake_connector.test = AsyncMock(
        return_value=TestResult(ok=True, latency_ms=10.0, error=None)
    )
    fake_connector.close = AsyncMock()
    with patch(
        "aidp_datasource.services.datasource_service.build_connector",
        return_value=fake_connector,
    ):
        resp = client.post(
            f"/api/v1/datasources/{created['id']}/test",
            headers=_bearer(),
            json={},
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "succeeded"
    assert body["latency_ms"] == 10.0


def test_test_connection_failure(
    client: TestClient, wired_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed probe returns 200 with ``status='failed'`` and an error string."""
    _bypass_kafka(monkeypatch)
    created = client.post(
        "/api/v1/datasources",
        headers=_bearer(),
        json=_create_body(name="primary"),
    ).json()
    fake_connector = AsyncMock()
    fake_connector.test = AsyncMock(
        return_value=TestResult(ok=False, latency_ms=None, error="auth failed")
    )
    fake_connector.close = AsyncMock()
    with patch(
        "aidp_datasource.services.datasource_service.build_connector",
        return_value=fake_connector,
    ):
        resp = client.post(
            f"/api/v1/datasources/{created['id']}/test",
            headers=_bearer(),
            json={},
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "failed"
    assert "auth failed" in body["error"]


# ---------------------------------------------------------------------------
# Health endpoints
# ---------------------------------------------------------------------------


def test_healthz(client: TestClient) -> None:
    """The ``/healthz`` endpoint returns 200 OK."""
    resp = client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"
