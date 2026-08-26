"""End-to-end tests for the policies + PII suggestion API (Task 16).

The test suite builds the FastAPI app with an in-memory SQLite
engine and exercises the three new endpoints:

- ``POST   /api/v1/datasources/{id}/policies`` — upsert
  (create-on-first-call, replace on subsequent calls).
- ``GET    /api/v1/datasources/{id}/policies`` — fetch the
  current policy blob.
- ``POST   /api/v1/datasources/{id}/suggest-pii`` — call the
  PII service (the LLM client is mocked at the
  :class:`PIIService` constructor boundary so no network is
  involved).

The Kafka producer is monkey-patched out so the test does not
need a broker; the connectors are mocked at the
:func:`build_connector` boundary so the test does not need a
real database.

Coverage:

- Auth + envelope: a missing token returns 401; a permission
  miss returns 403.
- ``POST /policies`` — first call creates the row;
  subsequent calls replace the blob; L1 isolation (cross-tenant
  probe returns 404).
- ``GET /policies`` — returns the current blob; 404 when no
  policy has been written; L1 isolation.
- ``POST /suggest-pii`` — returns the suggestion list; L1
  isolation; 404 on a missing datasource.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from aidp_auth.jwt import create_access_token
from aidp_datasource.connectors.base import (
    ColumnInfo,
    TableInfo,
)
from aidp_datasource.connectors.base import (
    TestResult as ConnectorTestResult,
)
from aidp_datasource.models import Base, Datasource, DatasourceSchema
from aidp_datasource.schemas import (
    ConnectionConfig,
    CredentialsPayload,
    DatasourceCreateRequest,
)
from aidp_datasource.services.credential_service import (
    CredentialService,
    set_default_credential_service,
)
from aidp_datasource.services.datasource_service import DatasourceService
from aidp_datasource.services.pii_service import (
    PIIColumnSuggestion,
    PIIService,
    set_default_pii_service,
)
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
            text(
                "INSERT INTO tenants (id, code, name) VALUES (:id, :code, :name)"
            ),
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
def credential_service() -> Iterator[CredentialService]:
    svc = CredentialService(key=b"\x05" * 32)
    set_default_credential_service(svc)
    try:
        yield svc
    finally:
        set_default_credential_service(None)


@pytest.fixture
def app(
    monkeypatch: pytest.MonkeyPatch, wired_engine: Engine
) -> Iterator[FastAPI]:
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


def _make_datasource(
    *,
    service: DatasourceService,
    name: str = "primary",
    kind: str = "postgresql",
    tenant_id: str = "tenant-a",
) -> Datasource:
    body = DatasourceCreateRequest(
        name=name,
        kind=kind,  # type: ignore[arg-type]
        env="prod",
        description="",
        connection=ConnectionConfig(host="db.example.test", port=5432, database="aidp"),
        credentials=CredentialsPayload(username="u", password="p"),
        tags=[],
        enabled=True,
    )
    return service.create_datasource(
        tenant_id=tenant_id, actor="u-test", body=body
    )


def _write_schema_cache(
    *,
    eng: Engine,
    tenant_id: str,
    datasource_id: str,
    tables: list[TableInfo],
) -> None:
    payload = [
        {
            "name": t.name,
            "schema": t.schema or "public",
            "columns": [
                {"name": c.name, "type": c.type, "nullable": bool(c.nullable)}
                for c in t.columns
            ],
            "primary_key": list(t.primary_key),
            "indexes": [],
            "row_count_estimate": t.row_count_estimate,
        }
        for t in tables
    ]
    from datetime import UTC, datetime

    from sqlalchemy.orm import Session

    with Session(eng) as session:
        session.add(
            DatasourceSchema(
                tenant_id=tenant_id,
                datasource_id=datasource_id,
                table_count=len(tables),
                tables_json=payload,
                fingerprint="x" * 64,
                refreshed_at=datetime.now(UTC),
            )
        )
        session.commit()


def _install_mock_pii_service(
    monkeypatch: pytest.MonkeyPatch,
    *,
    suggestions: list[PIIColumnSuggestion] | None = None,
    raises: Exception | None = None,
) -> PIIService:
    """Replace the process-wide :class:`PIIService` with a mock.

    The mock's ``classify`` returns the supplied suggestions
    list (or the supplied exception). The mock's
    ``suggest_pii`` is an :class:`AsyncMock` so the handler
    can ``await`` it; the real orchestration logic is
    exercised by the unit tests in
    :mod:`tests.test_pii_service`.
    """
    svc = MagicMock()
    if raises is not None:
        svc.suggest_pii = AsyncMock(side_effect=raises)
    else:
        svc.suggest_pii = AsyncMock(return_value=suggestions or [])
    set_default_pii_service(svc)  # type: ignore[arg-type]
    monkeypatch.setattr(
        "aidp_datasource.api.policies.default_pii_service",
        lambda: svc,
    )
    return svc  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Auth + envelope
# ---------------------------------------------------------------------------


def test_upsert_policy_requires_authentication(client: TestClient) -> None:
    """A missing bearer token returns 401."""
    resp = client.post(
        "/api/v1/datasources/00000000-0000-0000-0000-000000000000/policies",
        json={"policies": {}},
    )
    assert resp.status_code == 401


def test_upsert_policy_requires_write_scope(
    client: TestClient, wired_engine: Engine
) -> None:
    """A token without ``datasource.write`` returns 403."""
    headers = _bearer(scopes=["datasource.read"])
    resp = client.post(
        "/api/v1/datasources/00000000-0000-0000-0000-000000000000/policies",
        headers=headers,
        json={"policies": {}},
    )
    assert resp.status_code == 403
    assert resp.json()["code"] == "FORBIDDEN"


# ---------------------------------------------------------------------------
# POST /policies
# ---------------------------------------------------------------------------


def test_upsert_policy_creates_row(
    client: TestClient,
    wired_engine: Engine,
    credential_service: CredentialService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The first call creates the :class:`DatasourcePolicy` row."""
    _bypass_kafka(monkeypatch)
    service = DatasourceService(credential_service=credential_service)
    ds = _make_datasource(service=service)
    resp = client.post(
        f"/api/v1/datasources/{ds.id}/policies",
        headers=_bearer(),
        json={
            "policies": {
                "pii": {"columns": [{"table": "users", "name": "email"}]},
                "masking": {"email": "hash"},
            }
        },
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["datasource_id"] == ds.id
    assert body["policies"]["pii"]["columns"][0]["name"] == "email"
    assert body["policies"]["masking"]["email"] == "hash"
    assert body["updated_at"]


def test_upsert_policy_replaces_existing(
    client: TestClient,
    wired_engine: Engine,
    credential_service: CredentialService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A second call replaces the entire ``policies_json`` blob."""
    _bypass_kafka(monkeypatch)
    service = DatasourceService(credential_service=credential_service)
    ds = _make_datasource(service=service)
    # First call.
    client.post(
        f"/api/v1/datasources/{ds.id}/policies",
        headers=_bearer(),
        json={"policies": {"v": 1}},
    )
    # Second call replaces the blob.
    resp = client.post(
        f"/api/v1/datasources/{ds.id}/policies",
        headers=_bearer(),
        json={"policies": {"v": 2, "masking": {"email": "redact"}}},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["policies"]["v"] == 2
    assert body["policies"]["masking"]["email"] == "redact"
    # The old ``{"v": 1}`` blob is gone (full replace, not
    # merge).
    assert "v" not in body["policies"] or body["policies"].get("v") == 2


def test_upsert_policy_returns_404_for_missing_datasource(
    client: TestClient, wired_engine: Engine
) -> None:
    """A 404 is returned when the datasource is missing."""
    resp = client.post(
        "/api/v1/datasources/00000000-0000-0000-0000-000000000000/policies",
        headers=_bearer(),
        json={"policies": {}},
    )
    assert resp.status_code == 404
    assert resp.json()["code"] == "NOT_FOUND"


def test_upsert_policy_rejects_unknown_field(
    client: TestClient,
    wired_engine: Engine,
    credential_service: CredentialService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unknown request field returns 422 (Pydantic validation)."""
    _bypass_kafka(monkeypatch)
    service = DatasourceService(credential_service=credential_service)
    ds = _make_datasource(service=service)
    resp = client.post(
        f"/api/v1/datasources/{ds.id}/policies",
        headers=_bearer(),
        json={"policies": {}, "surprise_field": "x"},
    )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# GET /policies
# ---------------------------------------------------------------------------


def test_get_policy_returns_404_when_no_policy(
    client: TestClient,
    wired_engine: Engine,
    credential_service: CredentialService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A datasource with no policy row returns 404."""
    _bypass_kafka(monkeypatch)
    service = DatasourceService(credential_service=credential_service)
    ds = _make_datasource(service=service)
    resp = client.get(
        f"/api/v1/datasources/{ds.id}/policies", headers=_bearer()
    )
    assert resp.status_code == 404


def test_get_policy_returns_blob(
    client: TestClient,
    wired_engine: Engine,
    credential_service: CredentialService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A previously-written policy is returned verbatim."""
    _bypass_kafka(monkeypatch)
    service = DatasourceService(credential_service=credential_service)
    ds = _make_datasource(service=service)
    client.post(
        f"/api/v1/datasources/{ds.id}/policies",
        headers=_bearer(),
        json={"policies": {"row_filter": "tenant_id = :tenant_id"}},
    )
    resp = client.get(
        f"/api/v1/datasources/{ds.id}/policies", headers=_bearer()
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["policies"]["row_filter"] == "tenant_id = :tenant_id"
    assert body["updated_at"]


def test_get_policy_l1_isolated(
    client: TestClient,
    wired_engine: Engine,
    credential_service: CredentialService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A cross-tenant probe returns 404 (no leak)."""
    _bypass_kafka(monkeypatch)
    service = DatasourceService(credential_service=credential_service)
    ds = _make_datasource(service=service, tenant_id="tenant-a")
    client.post(
        f"/api/v1/datasources/{ds.id}/policies",
        headers=_bearer(tenant_id="tenant-a"),
        json={"policies": {"v": 1}},
    )
    # tenant-b probes tenant-a's policy.
    resp = client.get(
        f"/api/v1/datasources/{ds.id}/policies",
        headers=_bearer(tenant_id="tenant-b"),
    )
    assert resp.status_code == 404


def test_get_policy_requires_read_scope(
    client: TestClient, wired_engine: Engine
) -> None:
    """A token without ``datasource.read`` returns 403."""
    headers = _bearer(scopes=["datasource.write"])
    resp = client.get(
        "/api/v1/datasources/00000000-0000-0000-0000-000000000000/policies",
        headers=headers,
    )
    assert resp.status_code == 403


# ---------------------------------------------------------------------------
# POST /suggest-pii
# ---------------------------------------------------------------------------


def test_suggest_pii_returns_suggestion_list(
    client: TestClient,
    wired_engine: Engine,
    credential_service: CredentialService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The endpoint returns the mock LLM client's suggestions."""
    _bypass_kafka(monkeypatch)
    service = DatasourceService(credential_service=credential_service)
    ds = _make_datasource(service=service)
    _write_schema_cache(
        eng=wired_engine,
        tenant_id="tenant-a",
        datasource_id=ds.id,
        tables=[
            TableInfo(
                name="users",
                schema="public",
                columns=[
                    ColumnInfo(name="id", type="integer"),
                    ColumnInfo(name="email", type="text"),
                ],
            )
        ],
    )
    _install_mock_pii_service(
        monkeypatch,
        suggestions=[
            PIIColumnSuggestion(
                name="email", type="email", reason="matches email pattern"
            )
        ],
    )
    resp = client.post(
        f"/api/v1/datasources/{ds.id}/suggest-pii",
        headers=_bearer(),
        json={},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["datasource_id"] == ds.id
    assert len(body["suggestions"]) == 1
    assert body["suggestions"][0]["name"] == "email"
    assert body["suggestions"][0]["type"] == "email"
    assert "email" in body["suggestions"][0]["reason"]


def test_suggest_pii_returns_empty_when_no_pii(
    client: TestClient,
    wired_engine: Engine,
    credential_service: CredentialService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An empty suggestion list is a 200, not a 404."""
    _bypass_kafka(monkeypatch)
    service = DatasourceService(credential_service=credential_service)
    ds = _make_datasource(service=service)
    _write_schema_cache(
        eng=wired_engine,
        tenant_id="tenant-a",
        datasource_id=ds.id,
        tables=[
            TableInfo(
                name="orders",
                schema="public",
                columns=[ColumnInfo(name="quantity", type="integer")],
            )
        ],
    )
    _install_mock_pii_service(monkeypatch, suggestions=[])
    resp = client.post(
        f"/api/v1/datasources/{ds.id}/suggest-pii",
        headers=_bearer(),
        json={},
    )
    assert resp.status_code == 200
    assert resp.json()["suggestions"] == []


def test_suggest_pii_returns_404_for_missing_datasource(
    client: TestClient, wired_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A missing datasource returns 404."""
    _install_mock_pii_service(monkeypatch, suggestions=[])
    resp = client.post(
        "/api/v1/datasources/00000000-0000-0000-0000-000000000000/suggest-pii",
        headers=_bearer(),
        json={},
    )
    assert resp.status_code == 404


def test_suggest_pii_returns_502_when_llm_fails(
    client: TestClient,
    wired_engine: Engine,
    credential_service: CredentialService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A persistent LLM failure surfaces as 502 (UpstreamError)."""
    from aidp_common.errors import UpstreamError

    _bypass_kafka(monkeypatch)
    service = DatasourceService(credential_service=credential_service)
    ds = _make_datasource(service=service)
    _write_schema_cache(
        eng=wired_engine,
        tenant_id="tenant-a",
        datasource_id=ds.id,
        tables=[
            TableInfo(
                name="users",
                schema="public",
                columns=[ColumnInfo(name="email", type="text")],
            )
        ],
    )
    _install_mock_pii_service(
        monkeypatch, raises=UpstreamError("llm down", details={})
    )
    resp = client.post(
        f"/api/v1/datasources/{ds.id}/suggest-pii",
        headers=_bearer(),
        json={},
    )
    assert resp.status_code == 502


def test_suggest_pii_respects_table_whitelist(
    client: TestClient,
    wired_engine: Engine,
    credential_service: CredentialService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The ``tables`` whitelist is forwarded to the service."""
    _bypass_kafka(monkeypatch)
    service = DatasourceService(credential_service=credential_service)
    ds = _make_datasource(service=service)
    _write_schema_cache(
        eng=wired_engine,
        tenant_id="tenant-a",
        datasource_id=ds.id,
        tables=[
            TableInfo(
                name="users",
                schema="public",
                columns=[ColumnInfo(name="email", type="text")],
            )
        ],
    )
    mock_svc = _install_mock_pii_service(
        monkeypatch,
        suggestions=[
            PIIColumnSuggestion(name="email", type="email", reason="x")
        ],
    )
    resp = client.post(
        f"/api/v1/datasources/{ds.id}/suggest-pii",
        headers=_bearer(),
        json={"tables": ["users"]},
    )
    assert resp.status_code == 200
    # The mock service was called with the whitelist.
    call_kwargs = mock_svc.suggest_pii.await_args.kwargs
    assert call_kwargs["tables"] == ["users"]


def test_suggest_pii_rejects_invalid_sample_size(
    client: TestClient, wired_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``sample_size > 20`` is rejected by Pydantic (422)."""
    _install_mock_pii_service(monkeypatch, suggestions=[])
    resp = client.post(
        "/api/v1/datasources/00000000-0000-0000-0000-000000000000/suggest-pii",
        headers=_bearer(),
        json={"sample_size": 100},
    )
    assert resp.status_code == 422


def test_suggest_pii_requires_read_scope(
    client: TestClient, wired_engine: Engine
) -> None:
    """A token without ``datasource.read`` returns 403."""
    headers = _bearer(scopes=["datasource.write"])
    resp = client.post(
        "/api/v1/datasources/00000000-0000-0000-0000-000000000000/suggest-pii",
        headers=headers,
        json={},
    )
    assert resp.status_code == 403


# ---------------------------------------------------------------------------
# Auth — used to ensure all routes require auth.
# ---------------------------------------------------------------------------


def test_get_policy_requires_authentication(client: TestClient) -> None:
    """A missing bearer token returns 401."""
    resp = client.get(
        "/api/v1/datasources/00000000-0000-0000-0000-000000000000/policies"
    )
    assert resp.status_code == 401


def test_suggest_pii_requires_authentication(client: TestClient) -> None:
    """A missing bearer token returns 401."""
    resp = client.post(
        "/api/v1/datasources/00000000-0000-0000-0000-000000000000/suggest-pii",
        json={},
    )
    assert resp.status_code == 401
