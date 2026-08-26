"""Tests for the Datasource schema cache (Task 15).

The test suite pins the contract that
:mod:`aidp_datasource.services.schema_service` and
:mod:`aidp_datasource.jobs.sync_schema` ship in Task 15:

- :func:`compute_fingerprint` is a stable, order-independent
  SHA-256 of the canonicalised table list. The same logical
  schema always produces the same hash, regardless of how
  the connector ordered the rows. Row-count estimates do
  *not* contribute to the hash.
- :meth:`SchemaService.sync_schema` opens a live connector,
  replaces the cached snapshot, and reports whether the
  fingerprint changed relative to the prior snapshot.
- :meth:`SchemaService.preview_table` proxies to the
  connector; :meth:`get_table_ddl` renders the cached
  snapshot to ``CREATE TABLE`` SQL.
- :func:`enqueue_sync_schema_job` + :func:`run_sync_schema_job`
  form the in-process background runner; the registry
  carries the job state across the boundary.
- The four API endpoints
  (``POST /sync-schema``, ``GET /schemas``,
  ``GET /tables/{table}/preview``, ``GET /tables/{table}/ddl``)
  honour the L1 isolation contract.

The connector is mocked so the test does not need a real
Postgres / MySQL / Oracle / Hive instance. The in-memory
SQLite engine mirrors the production schema (the new
``fingerprint`` column is added by :class:`Base.metadata`).
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
from collections.abc import Iterator
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aidp_auth.jwt import create_access_token
from aidp_common.errors import NotFoundError
from aidp_datasource.connectors.base import (
    ColumnInfo,
    IndexInfo,
    TableInfo,
)
from aidp_datasource.jobs.sync_schema import (
    SchemaSyncJobRegistry,
    _take_run_args,
    enqueue_sync_schema_job,
    get_job_registry,
    run_sync_schema_job,
    set_job_registry,
)
from aidp_datasource.models import Base, Datasource
from aidp_datasource.schemas import (
    ConnectionConfig,
    CredentialsPayload,
    DatasourceCreateRequest,
    DatasourceKind,
)
from aidp_datasource.services.credential_service import (
    CredentialService,
    set_default_credential_service,
)
from aidp_datasource.services.datasource_service import (
    DatasourceService,
)
from aidp_datasource.services.schema_service import (
    SchemaService,
    SchemaSyncJob,
    compute_fingerprint,
    default_schema_service,
    set_default_schema_service,
)
from aidp_db.session import get_session
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
def credential_service() -> Iterator[CredentialService]:
    """A fresh credential service backed by a deterministic key."""
    svc = CredentialService(key=b"\x03" * 32)
    set_default_credential_service(svc)
    try:
        yield svc
    finally:
        set_default_credential_service(None)


# ---------------------------------------------------------------------------
# Datasource factory
# ---------------------------------------------------------------------------


def _make_datasource(
    *,
    wired_engine: Engine,
    service: DatasourceService,
    name: str = "primary",
    kind: DatasourceKind = "postgresql",
    host: str = "db.example.test",
    port: int = 5432,
    database: str = "aidp",
    tenant_id: str = "tenant-a",
) -> Datasource:
    body = DatasourceCreateRequest(
        name=name,
        kind=kind,
        env="prod",
        description="",
        connection=ConnectionConfig(host=host, port=port, database=database),
        credentials=CredentialsPayload(username="u", password="p"),
        tags=[],
        enabled=True,
    )
    return service.create_datasource(
        tenant_id=tenant_id, actor="u-test", body=body
    )


# ---------------------------------------------------------------------------
# Connector stub
# ---------------------------------------------------------------------------


def _make_fake_connector(
    *,
    tables: list[TableInfo] | None = None,
    preview_rows: list[tuple[Any, ...]] | None = None,
    preview_columns: list[str] | None = None,
    get_schema_error: Exception | None = None,
    preview_error: Exception | None = None,
) -> Any:
    """Build an async-mock connector.

    The mock satisfies the four :class:`Connector` methods.
    The :meth:`close` method is a no-op so the ``finally``
    block in :class:`SchemaService` does not raise.

    Args:
        preview_rows: A list of positional rows. Each row is
            zipped with ``preview_columns`` to produce a
            ``column → value`` dict (the wire shape
            :class:`Connector.preview` returns).
    """
    fake = MagicMock()
    fake.KIND = "postgresql"
    fake._closed = False
    if get_schema_error is not None:
        fake.get_schema = AsyncMock(side_effect=get_schema_error)
    else:
        fake.get_schema = AsyncMock(return_value=tables or [])
    if preview_error is not None:
        fake.preview = AsyncMock(side_effect=preview_error)
    else:
        cols = preview_columns or []
        rows = preview_rows or []
        fake.preview = AsyncMock(
            return_value=[dict(zip(cols, row, strict=True)) for row in rows]
        )
    fake.close = AsyncMock()
    fake.test = AsyncMock()
    return fake


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


def _bearer(
    tenant_id: str = "tenant-a", scopes: list[str] | None = None
) -> dict[str, str]:
    token = create_access_token(
        tenant_id=tenant_id, user_id="u-tester", scopes=scopes or ["*"]
    )
    return {"Authorization": f"Bearer {token}"}


def _bypass_kafka(monkeypatch: pytest.MonkeyPatch) -> None:
    from aidp_datasource.services import datasource_service

    async def _noop(*args: Any, **kwargs: Any) -> Any:
        return None

    monkeypatch.setattr(datasource_service, "publish_event", _noop)


def _table(
    name: str,
    *,
    schema: str | None = "public",
    columns: list[ColumnInfo] | None = None,
    primary_key: list[str] | None = None,
    indexes: list[IndexInfo] | None = None,
    row_count_estimate: int | None = None,
) -> TableInfo:
    return TableInfo(
        name=name,
        schema=schema,
        columns=columns or [ColumnInfo(name="id", type="integer", nullable=False)],
        primary_key=list(primary_key or []),
        indexes=list(indexes or []),
        row_count_estimate=row_count_estimate,
    )


# ---------------------------------------------------------------------------
# compute_fingerprint
# ---------------------------------------------------------------------------


def test_compute_fingerprint_is_deterministic() -> None:
    """The same logical schema produces the same digest."""
    t1 = _table("users", columns=[ColumnInfo("id", "integer"), ColumnInfo("name", "text")])
    t2 = _table("orders", columns=[ColumnInfo("id", "integer")])
    fp1 = compute_fingerprint([t1, t2])
    fp2 = compute_fingerprint([t1, t2])
    assert fp1 == fp2
    assert len(fp1) == 64


def test_compute_fingerprint_is_order_independent() -> None:
    """Reversing the table list yields the same digest."""
    t1 = _table("users", columns=[ColumnInfo("id", "integer")])
    t2 = _table("orders", columns=[ColumnInfo("id", "integer")])
    assert compute_fingerprint([t1, t2]) == compute_fingerprint([t2, t1])


def test_compute_fingerprint_ignores_row_count_estimate() -> None:
    """Row-count drift does not flip the fingerprint (it is not a schema change)."""
    t1 = _table("users", row_count_estimate=10)
    t2 = _table("users", row_count_estimate=999_999)
    assert compute_fingerprint([t1]) == compute_fingerprint([t2])


def test_compute_fingerprint_detects_column_change() -> None:
    """Adding a column flips the fingerprint."""
    t1 = _table("users", columns=[ColumnInfo("id", "integer")])
    t2 = _table("users", columns=[ColumnInfo("id", "integer"), ColumnInfo("email", "text")])
    assert compute_fingerprint([t1]) != compute_fingerprint([t2])


def test_compute_fingerprint_detects_type_change() -> None:
    """Changing a column's type flips the fingerprint."""
    t1 = _table("users", columns=[ColumnInfo("id", "integer")])
    t2 = _table("users", columns=[ColumnInfo("id", "text")])
    assert compute_fingerprint([t1]) != compute_fingerprint([t2])


def test_compute_fingerprint_detects_pk_change() -> None:
    """Adding a primary key flips the fingerprint."""
    t1 = _table("users", primary_key=[])
    t2 = _table("users", primary_key=["id"])
    assert compute_fingerprint([t1]) != compute_fingerprint([t2])


def test_compute_fingerprint_detects_index_change() -> None:
    """Adding a secondary index flips the fingerprint."""
    t1 = _table("users", indexes=[])
    t2 = _table(
        "users",
        indexes=[IndexInfo(name="ix_email", columns=["email"], unique=True)],
    )
    assert compute_fingerprint([t1]) != compute_fingerprint([t2])


def test_compute_fingerprint_for_empty_schema() -> None:
    """An empty schema produces the SHA-256 of the empty payload (deterministic)."""
    fp = compute_fingerprint([])
    assert fp == compute_fingerprint([])
    # The empty payload still hashes to a valid digest; the
    # concrete value is the SHA-256 of the canonical JSON
    # ``[]`` — pinned so a future change to the canonical
    # format is a conscious decision.
    expected = hashlib.sha256(b"[]").hexdigest()
    assert fp == expected


def test_compute_fingerprint_matches_manual_canonical_form() -> None:
    """The digest matches a hand-rolled canonical JSON of the same input."""
    t1 = _table(
        "users",
        schema="public",
        columns=[
            ColumnInfo("id", "integer", nullable=False),
            ColumnInfo("email", "text", nullable=True),
        ],
        primary_key=["id"],
        indexes=[IndexInfo(name="ix_email", columns=["email"], unique=True)],
    )
    payload = [
        {
            "name": "users",
            "schema": "public",
            "columns": [
                {"name": "id", "type": "integer", "nullable": False},
                {"name": "email", "type": "text", "nullable": True},
            ],
            "primary_key": ["id"],
            "indexes": [
                {
                    "name": "ix_email",
                    "columns": ["email"],
                    "unique": True,
                }
            ],
        }
    ]
    expected = hashlib.sha256(
        json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")
    ).hexdigest()
    assert compute_fingerprint([t1]) == expected


# ---------------------------------------------------------------------------
# SchemaService.sync_schema
# ---------------------------------------------------------------------------


@pytest.fixture
def schema_service(
    credential_service: CredentialService,
) -> Iterator[SchemaService]:
    svc = SchemaService(credential_service=credential_service)
    set_default_schema_service(svc)
    try:
        yield svc
    finally:
        set_default_schema_service(None)


def test_sync_schema_writes_snapshot(
    wired_engine: Engine,
    schema_service: SchemaService,
    datasource_service: DatasourceService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A successful sync writes a new snapshot row + audit row."""
    _bypass_kafka(monkeypatch)
    ds = _make_datasource(wired_engine=wired_engine, service=datasource_service)
    fake = _make_fake_connector(
        tables=[
            _table("users", columns=[ColumnInfo("id", "integer")]),
            _table("orders", columns=[ColumnInfo("id", "integer")]),
        ]
    )
    monkeypatch.setattr(
        "aidp_datasource.services.schema_service.build_connector",
        lambda **kwargs: fake,
    )
    result = schema_service.sync_schema(
        tenant_id="tenant-a", actor="u", datasource_id=ds.id
    )
    assert result.status == "succeeded", f"unexpected: {result}"
    assert result.table_count == 2
    assert result.changed is True
    assert result.prior_fingerprint is None
    assert result.error is None
    assert len(result.fingerprint) == 64
    # The snapshot row is in the DB.
    with get_session() as session:
        from aidp_datasource.models import DatasourceSchema
        from sqlalchemy import select

        rows = (
            session.execute(
                select(DatasourceSchema).where(
                    DatasourceSchema.datasource_id == ds.id
                )
            )
            .scalars()
            .all()
        )
    assert len(rows) == 1
    assert rows[0].fingerprint == result.fingerprint
    assert rows[0].table_count == 2


def test_sync_schema_reports_no_change_when_unchanged(
    wired_engine: Engine,
    schema_service: SchemaService,
    datasource_service: DatasourceService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A second sync with the same schema reports ``changed=False``."""
    _bypass_kafka(monkeypatch)
    ds = _make_datasource(wired_engine=wired_engine, service=datasource_service)
    tables = [_table("users", columns=[ColumnInfo("id", "integer")])]
    fake = _make_fake_connector(tables=tables)
    monkeypatch.setattr(
        "aidp_datasource.services.schema_service.build_connector",
        lambda **kwargs: fake,
    )
    first = schema_service.sync_schema(
        tenant_id="tenant-a", actor="u", datasource_id=ds.id
    )
    assert first.changed is True
    second = schema_service.sync_schema(
        tenant_id="tenant-a", actor="u", datasource_id=ds.id
    )
    assert second.status == "succeeded"
    assert second.changed is False
    assert second.fingerprint == first.fingerprint
    assert second.prior_fingerprint == first.fingerprint


def test_sync_schema_reports_change_when_schema_drifted(
    wired_engine: Engine,
    schema_service: SchemaService,
    datasource_service: DatasourceService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A second sync with a new column reports ``changed=True``."""
    _bypass_kafka(monkeypatch)
    ds = _make_datasource(wired_engine=wired_engine, service=datasource_service)
    fake = MagicMock()
    fake.KIND = "postgresql"
    fake.get_schema = AsyncMock(
        side_effect=[
            [_table("users", columns=[ColumnInfo("id", "integer")])],
            [
                _table(
                    "users",
                    columns=[
                        ColumnInfo("id", "integer"),
                        ColumnInfo("email", "text"),
                    ],
                )
            ],
        ]
    )
    fake.close = AsyncMock()
    monkeypatch.setattr(
        "aidp_datasource.services.schema_service.build_connector",
        lambda **kwargs: fake,
    )
    first = schema_service.sync_schema(
        tenant_id="tenant-a", actor="u", datasource_id=ds.id
    )
    second = schema_service.sync_schema(
        tenant_id="tenant-a", actor="u", datasource_id=ds.id
    )
    assert first.changed is True
    assert second.changed is True
    assert second.fingerprint != first.fingerprint


def test_sync_schema_raises_not_found(
    wired_engine: Engine,
    schema_service: SchemaService,
) -> None:
    """A missing datasource raises :class:`NotFoundError`."""
    with pytest.raises(NotFoundError):
        schema_service.sync_schema(
            tenant_id="tenant-a",
            actor="u",
            datasource_id="00000000-0000-0000-0000-000000000000",
        )


def test_sync_schema_captures_introspection_failure(
    wired_engine: Engine,
    schema_service: SchemaService,
    datasource_service: DatasourceService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A connector failure is captured as ``status='failed'``."""
    _bypass_kafka(monkeypatch)
    ds = _make_datasource(wired_engine=wired_engine, service=datasource_service)
    fake = _make_fake_connector(
        get_schema_error=RuntimeError("connection refused")
    )
    monkeypatch.setattr(
        "aidp_datasource.services.schema_service.build_connector",
        lambda **kwargs: fake,
    )
    result = schema_service.sync_schema(
        tenant_id="tenant-a", actor="u", datasource_id=ds.id
    )
    assert result.status == "failed"
    assert "connection refused" in (result.error or "")
    assert result.table_count == 0


def test_sync_schema_uses_supplied_database_override(
    wired_engine: Engine,
    schema_service: SchemaService,
    datasource_service: DatasourceService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The ``database`` kwarg is forwarded to the connector's ``get_schema``."""
    _bypass_kafka(monkeypatch)
    ds = _make_datasource(wired_engine=wired_engine, service=datasource_service)
    fake = _make_fake_connector(tables=[])
    monkeypatch.setattr(
        "aidp_datasource.services.schema_service.build_connector",
        lambda **kwargs: fake,
    )
    schema_service.sync_schema(
        tenant_id="tenant-a",
        actor="u",
        datasource_id=ds.id,
        database="override",
    )
    fake.get_schema.assert_awaited_with("override")


# ---------------------------------------------------------------------------
# SchemaService.list_schemas / preview_table / get_table_ddl
# ---------------------------------------------------------------------------


def test_list_schemas_returns_latest_snapshot(
    wired_engine: Engine,
    schema_service: SchemaService,
    datasource_service: DatasourceService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``list_schemas`` returns the most recent snapshot row."""
    _bypass_kafka(monkeypatch)
    ds = _make_datasource(wired_engine=wired_engine, service=datasource_service)
    fake = _make_fake_connector(
        tables=[_table("users", columns=[ColumnInfo("id", "integer")])]
    )
    monkeypatch.setattr(
        "aidp_datasource.services.schema_service.build_connector",
        lambda **kwargs: fake,
    )
    schema_service.sync_schema(tenant_id="tenant-a", actor="u", datasource_id=ds.id)
    snap = schema_service.list_schemas(tenant_id="tenant-a", datasource_id=ds.id)
    assert snap.table_count == 1
    assert snap.fingerprint
    assert snap.refreshed_at is not None


def test_list_schemas_raises_when_no_snapshot(
    wired_engine: Engine,
    schema_service: SchemaService,
    datasource_service: DatasourceService,
) -> None:
    """A registered datasource with no snapshot yet raises 404."""
    ds = _make_datasource(wired_engine=wired_engine, service=datasource_service)
    with pytest.raises(NotFoundError):
        schema_service.list_schemas(tenant_id="tenant-a", datasource_id=ds.id)


def test_list_schemas_raises_on_cross_tenant_probe(
    wired_engine: Engine,
    schema_service: SchemaService,
    datasource_service: DatasourceService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A tenant-b probe of tenant-a's datasource raises 404."""
    _bypass_kafka(monkeypatch)
    ds = _make_datasource(wired_engine=wired_engine, service=datasource_service)
    fake = _make_fake_connector(tables=[_table("users")])
    monkeypatch.setattr(
        "aidp_datasource.services.schema_service.build_connector",
        lambda **kwargs: fake,
    )
    schema_service.sync_schema(tenant_id="tenant-a", actor="u", datasource_id=ds.id)
    with pytest.raises(NotFoundError):
        schema_service.list_schemas(tenant_id="tenant-b", datasource_id=ds.id)


def test_preview_table_proxies_to_connector(
    wired_engine: Engine,
    schema_service: SchemaService,
    datasource_service: DatasourceService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``preview_table`` calls the connector and returns the rows."""
    _bypass_kafka(monkeypatch)
    ds = _make_datasource(wired_engine=wired_engine, service=datasource_service)
    fake = _make_fake_connector(
        preview_rows=[(1, "a"), (2, "b")],
        preview_columns=["id", "email"],
    )
    monkeypatch.setattr(
        "aidp_datasource.services.schema_service.build_connector",
        lambda **kwargs: fake,
    )
    rows = schema_service.preview_table(
        tenant_id="tenant-a", datasource_id=ds.id, table="users", limit=10
    )
    assert rows == [{"id": 1, "email": "a"}, {"id": 2, "email": "b"}]
    fake.preview.assert_awaited_with("users", 10)


def test_preview_table_raises_on_connector_error(
    wired_engine: Engine,
    schema_service: SchemaService,
    datasource_service: DatasourceService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A connector error during preview is re-raised."""
    _bypass_kafka(monkeypatch)
    ds = _make_datasource(wired_engine=wired_engine, service=datasource_service)
    fake = _make_fake_connector(preview_error=RuntimeError("table not found"))
    monkeypatch.setattr(
        "aidp_datasource.services.schema_service.build_connector",
        lambda **kwargs: fake,
    )
    with pytest.raises(RuntimeError) as exc_info:
        schema_service.preview_table(
            tenant_id="tenant-a", datasource_id=ds.id, table="users", limit=10
        )
    assert "table not found" in str(exc_info.value)


def test_get_table_ddl_empty_snapshot_returns_empty_string(
    wired_engine: Engine,
    schema_service: SchemaService,
    datasource_service: DatasourceService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An empty (no tables) snapshot returns the empty DDL string."""
    _bypass_kafka(monkeypatch)
    ds = _make_datasource(wired_engine=wired_engine, service=datasource_service)
    fake = _make_fake_connector(tables=[])
    monkeypatch.setattr(
        "aidp_datasource.services.schema_service.build_connector",
        lambda **kwargs: fake,
    )
    schema_service.sync_schema(tenant_id="tenant-a", actor="u", datasource_id=ds.id)
    ddl = schema_service.get_table_ddl(
        tenant_id="tenant-a", datasource_id=ds.id, table="public.users"
    )
    assert ddl == ""


def test_get_table_ddl_matches_bare_table_name(
    wired_engine: Engine,
    schema_service: SchemaService,
    datasource_service: DatasourceService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A caller can look up a table by bare name (no schema prefix)."""
    _bypass_kafka(monkeypatch)
    ds = _make_datasource(wired_engine=wired_engine, service=datasource_service)
    fake = _make_fake_connector(
        tables=[
            _table(
                "users",
                schema="public",
                columns=[ColumnInfo("id", "integer", nullable=False)],
            )
        ]
    )
    monkeypatch.setattr(
        "aidp_datasource.services.schema_service.build_connector",
        lambda **kwargs: fake,
    )
    schema_service.sync_schema(tenant_id="tenant-a", actor="u", datasource_id=ds.id)
    ddl = schema_service.get_table_ddl(
        tenant_id="tenant-a", datasource_id=ds.id, table="users"
    )
    assert "CREATE TABLE" in ddl


def test_get_table_ddl_raises_when_datasource_missing(
    wired_engine: Engine,
    schema_service: SchemaService,
) -> None:
    """A missing datasource raises :class:`NotFoundError` (snapshot path)."""
    with pytest.raises(NotFoundError):
        schema_service.get_table_ddl(
            tenant_id="tenant-a",
            datasource_id="00000000-0000-0000-0000-000000000000",
            table="public.users",
        )


def test_preview_table_raises_when_datasource_missing(
    wired_engine: Engine,
    schema_service: SchemaService,
) -> None:
    """A missing datasource raises :class:`NotFoundError` on preview."""
    with pytest.raises(NotFoundError):
        schema_service.preview_table(
            tenant_id="tenant-a",
            datasource_id="00000000-0000-0000-0000-000000000000",
            table="users",
            limit=10,
        )


def test_sync_schema_truncates_long_error_messages(
    wired_engine: Engine,
    schema_service: SchemaService,
    datasource_service: DatasourceService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A long error string is truncated before it lands on the result."""
    _bypass_kafka(monkeypatch)
    ds = _make_datasource(wired_engine=wired_engine, service=datasource_service)
    long_msg = "x" * 5000
    fake = _make_fake_connector(get_schema_error=RuntimeError(long_msg))
    monkeypatch.setattr(
        "aidp_datasource.services.schema_service.build_connector",
        lambda **kwargs: fake,
    )
    result = schema_service.sync_schema(
        tenant_id="tenant-a", actor="u", datasource_id=ds.id
    )
    assert result.status == "failed"
    # Truncation caps at 1024 chars and appends ``...``.
    assert result.error is not None
    assert len(result.error) <= 1024
    assert result.error.endswith("...")


def test_schema_sync_job_to_dict() -> None:
    """``SchemaSyncJob.to_dict`` returns a JSON-ready dict."""
    job = SchemaSyncJob(
        job_id="j-1",
        datasource_id="ds-1",
        tenant_id="t-1",
        status="succeeded",
        fingerprint="abc",
        table_count=2,
        changed=True,
    )
    d = job.to_dict()
    assert d["job_id"] == "j-1"
    assert d["status"] == "succeeded"
    assert d["fingerprint"] == "abc"
    assert d["table_count"] == 2
    assert d["changed"] is True
    assert d["error"] is None
    assert d["finished_at"] is None
    assert isinstance(d["created_at"], str)


def test_get_table_ddl_renders_create_table(
    wired_engine: Engine,
    schema_service: SchemaService,
    datasource_service: DatasourceService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``get_table_ddl`` renders a ``CREATE TABLE`` statement from the snapshot."""
    _bypass_kafka(monkeypatch)
    ds = _make_datasource(wired_engine=wired_engine, service=datasource_service)
    fake = _make_fake_connector(
        tables=[
            _table(
                "users",
                schema="public",
                columns=[
                    ColumnInfo("id", "integer", nullable=False),
                    ColumnInfo("email", "text", nullable=True),
                ],
                primary_key=["id"],
                indexes=[
                    IndexInfo(name="ix_email", columns=["email"], unique=True)
                ],
            )
        ]
    )
    monkeypatch.setattr(
        "aidp_datasource.services.schema_service.build_connector",
        lambda **kwargs: fake,
    )
    schema_service.sync_schema(tenant_id="tenant-a", actor="u", datasource_id=ds.id)
    ddl = schema_service.get_table_ddl(
        tenant_id="tenant-a", datasource_id=ds.id, table="public.users"
    )
    assert "CREATE TABLE" in ddl
    assert '"id"' in ddl
    assert '"email"' in ddl
    assert "PRIMARY KEY" in ddl
    assert "CREATE UNIQUE INDEX" in ddl
    assert "ix_email" in ddl


def test_get_table_ddl_handles_hive_dialect(
    wired_engine: Engine,
    schema_service: SchemaService,
    datasource_service: DatasourceService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Hive uses backticks for identifiers (different quote style from PG)."""
    _bypass_kafka(monkeypatch)
    ds = _make_datasource(
        wired_engine=wired_engine,
        service=datasource_service,
        name="hive-ds",
        kind="hive",
        host="hive.example",
        port=10000,
        database="default",
    )
    fake = _make_fake_connector(
        tables=[
            _table(
                "users",
                schema="default",
                columns=[ColumnInfo("id", "bigint", nullable=False)],
            )
        ]
    )
    monkeypatch.setattr(
        "aidp_datasource.services.schema_service.build_connector",
        lambda **kwargs: fake,
    )
    schema_service.sync_schema(tenant_id="tenant-a", actor="u", datasource_id=ds.id)
    ddl = schema_service.get_table_ddl(
        tenant_id="tenant-a", datasource_id=ds.id, table="default.users"
    )
    assert "`id`" in ddl
    assert "`default`.`users`" in ddl


def test_get_table_ddl_unknown_table_raises(
    wired_engine: Engine,
    schema_service: SchemaService,
    datasource_service: DatasourceService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A table that is not in the snapshot raises :class:`NotFoundError`."""
    _bypass_kafka(monkeypatch)
    ds = _make_datasource(wired_engine=wired_engine, service=datasource_service)
    fake = _make_fake_connector(tables=[_table("users")])
    monkeypatch.setattr(
        "aidp_datasource.services.schema_service.build_connector",
        lambda **kwargs: fake,
    )
    schema_service.sync_schema(tenant_id="tenant-a", actor="u", datasource_id=ds.id)
    with pytest.raises(NotFoundError):
        schema_service.get_table_ddl(
            tenant_id="tenant-a",
            datasource_id=ds.id,
            table="public.orders",
        )


# ---------------------------------------------------------------------------
# Job registry + background runner
# ---------------------------------------------------------------------------


@pytest.fixture
def fresh_registry() -> Iterator[SchemaSyncJobRegistry]:
    reg = SchemaSyncJobRegistry()
    set_job_registry(reg)
    try:
        yield reg
    finally:
        set_job_registry(None)


def test_enqueue_creates_pending_job(
    fresh_registry: SchemaSyncJobRegistry,
) -> None:
    """``enqueue_sync_schema_job`` creates a ``pending`` job and stashes run args."""
    job = enqueue_sync_schema_job(
        tenant_id="tenant-a",
        actor="u",
        datasource_id="ds-1",
    )
    assert job.status == "pending"
    assert job.fingerprint is None
    # The run args are stashed so the background task can
    # pick them up.
    assert _take_run_args(job.job_id) is not None
    # A second pop returns ``None`` (consumed).
    assert _take_run_args(job.job_id) is None


@pytest.fixture
def datasource_service() -> DatasourceService:
    return DatasourceService()


def test_registry_create_and_get(
    fresh_registry: SchemaSyncJobRegistry,
) -> None:
    """``create`` + ``get`` round-trip a job handle."""
    job = fresh_registry.create(datasource_id="ds-1", tenant_id="tenant-a")
    assert fresh_registry.get(job_id=job.job_id) is job


def test_registry_update_replaces_fields(
    fresh_registry: SchemaSyncJobRegistry,
) -> None:
    """``update`` mutates the stored copy and returns the new handle."""
    job = fresh_registry.create(datasource_id="ds-1", tenant_id="tenant-a")
    updated = fresh_registry.update(
        job_id=job.job_id,
        status="running",
    )
    assert updated.status == "running"
    # The handle is replaced (immutable dataclass semantics).
    assert fresh_registry.get(job_id=job.job_id) is updated
    # ``created_at`` is preserved across updates.
    assert updated.created_at == job.created_at


def test_registry_update_rejects_unknown_field(
    fresh_registry: SchemaSyncJobRegistry,
) -> None:
    """An unknown field is rejected with :class:`TypeError` (typo guard)."""
    job = fresh_registry.create(datasource_id="ds-1", tenant_id="tenant-a")
    with pytest.raises(TypeError) as exc_info:
        fresh_registry.update(job_id=job.job_id, not_a_field="x")
    assert "not_a_field" in str(exc_info.value)


def test_registry_update_missing_job_raises(
    fresh_registry: SchemaSyncJobRegistry,
) -> None:
    """An unknown job id raises :class:`KeyError`."""
    with pytest.raises(KeyError):
        fresh_registry.update(job_id="missing", status="running")


def test_run_sync_schema_job_executes_and_updates(
    wired_engine: Engine,
    schema_service: SchemaService,
    datasource_service: DatasourceService,
    fresh_registry: SchemaSyncJobRegistry,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The background runner drives the service and updates the registry."""
    _bypass_kafka(monkeypatch)
    ds = _make_datasource(wired_engine=wired_engine, service=datasource_service)
    fake = _make_fake_connector(
        tables=[_table("users", columns=[ColumnInfo("id", "integer")])]
    )
    monkeypatch.setattr(
        "aidp_datasource.services.schema_service.build_connector",
        lambda **kwargs: fake,
    )
    job = enqueue_sync_schema_job(
        tenant_id="tenant-a",
        actor="u",
        datasource_id=ds.id,
        service=schema_service,
        registry=fresh_registry,
    )
    result = run_sync_schema_job(job.job_id, registry=fresh_registry)
    assert result is not None
    assert result.status == "succeeded"
    final = fresh_registry.get(job_id=job.job_id)
    assert final is not None
    assert final.status == "succeeded"
    assert final.fingerprint == result.fingerprint
    assert final.finished_at is not None
    assert final.error is None


def test_run_sync_schema_job_records_failure(
    wired_engine: Engine,
    schema_service: SchemaService,
    datasource_service: DatasourceService,
    fresh_registry: SchemaSyncJobRegistry,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A connector failure during a background run records ``status='failed'``."""
    _bypass_kafka(monkeypatch)
    ds = _make_datasource(wired_engine=wired_engine, service=datasource_service)
    fake = _make_fake_connector(
        get_schema_error=RuntimeError("network down")
    )
    monkeypatch.setattr(
        "aidp_datasource.services.schema_service.build_connector",
        lambda **kwargs: fake,
    )
    job = enqueue_sync_schema_job(
        tenant_id="tenant-a",
        actor="u",
        datasource_id=ds.id,
        service=schema_service,
        registry=fresh_registry,
    )
    run_sync_schema_job(job.job_id, registry=fresh_registry)
    final = fresh_registry.get(job_id=job.job_id)
    assert final is not None
    assert final.status == "failed"
    assert "network down" in (final.error or "")
    assert final.finished_at is not None


def test_run_sync_schema_job_without_args_is_noop(
    fresh_registry: SchemaSyncJobRegistry,
) -> None:
    """Calling the runner with no stashed args returns ``None`` (idempotent)."""
    job = fresh_registry.create(datasource_id="ds-1", tenant_id="tenant-a")
    result = run_sync_schema_job(job.job_id, registry=fresh_registry)
    assert result is None
    final = fresh_registry.get(job_id=job.job_id)
    assert final is not None
    assert final.status == "pending"


# ---------------------------------------------------------------------------
# HTTP layer
# ---------------------------------------------------------------------------


@pytest.fixture
def app(
    wired_engine: Engine,
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


@pytest.fixture
def created_ds(
    client: TestClient,
    wired_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, Any]:
    _bypass_kafka(monkeypatch)
    body = {
        "name": "primary",
        "kind": "postgresql",
        "env": "prod",
        "description": "",
        "connection": {
            "host": "db.example.test",
            "port": 5432,
            "database": "aidp",
        },
        "credentials": {"username": "u", "password": "p"},
        "tags": [],
        "enabled": True,
    }
    resp = client.post(
        "/api/v1/datasources", headers=_bearer(), json=body
    )
    assert resp.status_code == 201, resp.text
    return cast(dict[str, Any], resp.json())


def test_sync_schema_endpoint_returns_202_with_job_id(
    client: TestClient,
    created_ds: dict[str, Any],
    wired_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``POST /sync-schema`` returns 202 + a job id, and the BackgroundTasks runs."""
    _bypass_kafka(monkeypatch)
    fake = _make_fake_connector(
        tables=[_table("users", columns=[ColumnInfo("id", "integer")])]
    )
    with patch(
        "aidp_datasource.services.schema_service.build_connector",
        return_value=fake,
    ):
        resp = client.post(
            f"/api/v1/datasources/{created_ds['id']}/sync-schema",
            headers=_bearer(),
        )
    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert "job_id" in body
    assert body["datasource_id"] == created_ds["id"]
    assert body["status"] == "pending"
    # The background task ran (FastAPI TestClient awaits it).
    list_resp = client.get(
        f"/api/v1/datasources/{created_ds['id']}/schemas",
        headers=_bearer(),
    )
    assert list_resp.status_code == 200, list_resp.text


def test_sync_schema_404_for_missing_datasource(
    client: TestClient,
    wired_engine: Engine,
) -> None:
    """A 404 is returned when the datasource is missing."""
    resp = client.post(
        "/api/v1/datasources/00000000-0000-0000-0000-000000000000/sync-schema",
        headers=_bearer(),
    )
    assert resp.status_code == 404
    assert resp.json()["code"] == "NOT_FOUND"


def test_schemas_endpoint_returns_snapshot(
    client: TestClient,
    created_ds: dict[str, Any],
    wired_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``GET /schemas`` returns the snapshot's fingerprint + tables."""
    _bypass_kafka(monkeypatch)
    fake = _make_fake_connector(
        tables=[
            _table("users", columns=[ColumnInfo("id", "integer")]),
        ]
    )
    with patch(
        "aidp_datasource.services.schema_service.build_connector",
        return_value=fake,
    ):
        # Force a sync.
        sync_resp = client.post(
            f"/api/v1/datasources/{created_ds['id']}/sync-schema",
            headers=_bearer(),
        )
        assert sync_resp.status_code == 202
    resp = client.get(
        f"/api/v1/datasources/{created_ds['id']}/schemas",
        headers=_bearer(),
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["datasource_id"] == created_ds["id"]
    assert body["table_count"] == 1
    assert len(body["fingerprint"]) == 64
    assert body["tables"][0]["name"] == "users"
    assert body["tables"][0]["columns"][0]["name"] == "id"


def test_schemas_endpoint_404_when_no_snapshot(
    client: TestClient,
    created_ds: dict[str, Any],
    wired_engine: Engine,
) -> None:
    """A 404 is returned when no snapshot has been taken yet."""
    resp = client.get(
        f"/api/v1/datasources/{created_ds['id']}/schemas",
        headers=_bearer(),
    )
    assert resp.status_code == 404


def test_schemas_endpoint_isolates_tenants(
    client: TestClient,
    created_ds: dict[str, Any],
    wired_engine: Engine,
) -> None:
    """A cross-tenant ``GET /schemas`` returns 404."""
    resp = client.get(
        f"/api/v1/datasources/{created_ds['id']}/schemas",
        headers=_bearer(tenant_id="tenant-b"),
    )
    assert resp.status_code == 404


def test_preview_endpoint_returns_rows(
    client: TestClient,
    created_ds: dict[str, Any],
    wired_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``GET /tables/{table}/preview`` returns up to ``limit`` rows."""
    _bypass_kafka(monkeypatch)
    fake = _make_fake_connector(
        preview_rows=[(1, "a@b.test"), (2, "c@d.test")],
        preview_columns=["id", "email"],
    )
    with patch(
        "aidp_datasource.services.schema_service.build_connector",
        return_value=fake,
    ):
        resp = client.get(
            f"/api/v1/datasources/{created_ds['id']}/tables/users/preview",
            headers=_bearer(),
        )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["table"] == "users"
    assert body["row_count"] == 2
    assert body["columns"] == ["id", "email"]
    assert body["rows"] == [
        {"id": 1, "email": "a@b.test"},
        {"id": 2, "email": "c@d.test"},
    ]


def test_preview_endpoint_caps_limit(
    client: TestClient,
    created_ds: dict[str, Any],
    wired_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``limit`` is forwarded to the connector as-is (the connector caps it later)."""
    _bypass_kafka(monkeypatch)
    fake = _make_fake_connector(preview_rows=[], preview_columns=["id"])
    with patch(
        "aidp_datasource.services.schema_service.build_connector",
        return_value=fake,
    ):
        resp = client.get(
            f"/api/v1/datasources/{created_ds['id']}/tables/users/preview?limit=5",
            headers=_bearer(),
        )
    assert resp.status_code == 200
    fake.preview.assert_awaited_with("users", 5)


def test_preview_endpoint_rejects_oversized_limit(
    client: TestClient,
    created_ds: dict[str, Any],
) -> None:
    """A limit > 1000 is rejected with 422 (Pydantic validation)."""
    resp = client.get(
        f"/api/v1/datasources/{created_ds['id']}/tables/users/preview?limit=10000",
        headers=_bearer(),
    )
    assert resp.status_code == 422


def test_ddl_endpoint_returns_create_table(
    client: TestClient,
    created_ds: dict[str, Any],
    wired_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``GET /tables/{table}/ddl`` returns the rendered DDL."""
    _bypass_kafka(monkeypatch)
    fake = _make_fake_connector(
        tables=[
            _table(
                "users",
                schema="public",
                columns=[
                    ColumnInfo("id", "integer", nullable=False),
                    ColumnInfo("email", "text", nullable=True),
                ],
                primary_key=["id"],
            )
        ]
    )
    with patch(
        "aidp_datasource.services.schema_service.build_connector",
        return_value=fake,
    ):
        sync_resp = client.post(
            f"/api/v1/datasources/{created_ds['id']}/sync-schema",
            headers=_bearer(),
        )
        assert sync_resp.status_code == 202
        resp = client.get(
            f"/api/v1/datasources/{created_ds['id']}/tables/public.users/ddl",
            headers=_bearer(),
        )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["schema_kind"] == "postgresql"
    assert "CREATE TABLE" in body["ddl"]
    assert "PRIMARY KEY" in body["ddl"]


def test_ddl_endpoint_404_for_unknown_table(
    client: TestClient,
    created_ds: dict[str, Any],
    wired_engine: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 404 is returned when the table is not in the snapshot."""
    _bypass_kafka(monkeypatch)
    fake = _make_fake_connector(tables=[_table("users")])
    with patch(
        "aidp_datasource.services.schema_service.build_connector",
        return_value=fake,
    ):
        client.post(
            f"/api/v1/datasources/{created_ds['id']}/sync-schema",
            headers=_bearer(),
        )
    resp = client.get(
        f"/api/v1/datasources/{created_ds['id']}/tables/public.orders/ddl",
        headers=_bearer(),
    )
    assert resp.status_code == 404


def test_ddl_endpoint_404_for_missing_datasource(
    client: TestClient,
    wired_engine: Engine,
) -> None:
    """A 404 is returned when the DDL endpoint's datasource is missing."""
    resp = client.get(
        "/api/v1/datasources/00000000-0000-0000-0000-000000000000/tables/users/ddl",
        headers=_bearer(),
    )
    assert resp.status_code == 404


def test_endpoints_require_authentication(
    client: TestClient,
    created_ds: dict[str, Any],
) -> None:
    """A missing bearer token returns 401 on every new endpoint."""
    paths = [
        f"/api/v1/datasources/{created_ds['id']}/sync-schema",
        f"/api/v1/datasources/{created_ds['id']}/schemas",
        f"/api/v1/datasources/{created_ds['id']}/tables/users/preview",
        f"/api/v1/datasources/{created_ds['id']}/tables/users/ddl",
    ]
    for path in paths:
        resp = client.post(path) if path.endswith("/sync-schema") else client.get(path)
        assert resp.status_code == 401, (path, resp.text)


# ---------------------------------------------------------------------------
# Helpers used to silence the linter on imports touched by the
# the ``_take_run_args`` re-export above.
# ---------------------------------------------------------------------------


def test_take_run_args_returns_none_for_missing(
    fresh_registry: SchemaSyncJobRegistry,
) -> None:
    """``_take_run_args`` returns ``None`` for an unknown job id."""
    assert _take_run_args("nope") is None


# Suppress unused-import warnings for symbols re-exported only
# for the test suite's introspection.
_ = (asyncio, base64, MagicMock, AsyncMock, set_default_schema_service, default_schema_service, get_job_registry)
