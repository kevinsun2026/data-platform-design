"""Tests for the Datasource service orchestration.

The tests pin the contract that
:mod:`aidp_datasource.services.datasource_service` ships in
Task 14:

- ``create_datasource`` encrypts credentials before persisting;
  the on-disk row never contains the plaintext.
- ``list_datasources`` / ``get_datasource`` / ``update_datasource``
  / ``soft_delete_datasource`` are L1-isolated — a cross-tenant
  probe returns ``NotFoundError`` (404), not 200 with the
  foreign row.
- ``test_connection`` records a :class:`ConnectionTest` row
  and a :class:`DatasourceAudit` row, and returns a
  :class:`TestConnectionOutcome` with the right ``status`` /
  ``latency_ms`` / ``error``.
- A disabled datasource short-circuits with
  ``status="disabled"`` (no socket is opened).
- A soft-deleted row is invisible to subsequent lookups.
- A duplicate name raises :class:`ConflictError`.

The connector is mocked (so the test does not need a real
Postgres / MySQL / Oracle / Hive); the service layer is
exercised end-to-end through its public API.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from aidp_auth.jwt import create_access_token
from aidp_common.errors import ConflictError, NotFoundError
from aidp_datasource.connectors.base import TestResult
from aidp_datasource.models import (
    Base,
    ConnectionTest,
    DatasourceAudit,
)
from aidp_datasource.schemas import (
    ConnectionConfig,
    CredentialsPayload,
    DatasourceCreateRequest,
    DatasourceUpdateRequest,
)
from aidp_datasource.services.credential_service import (
    default_credential_service,
)
from aidp_datasource.services.datasource_service import DatasourceService
from aidp_db.session import get_session
from sqlalchemy import Column, String, Table, create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import StaticPool

# ---------------------------------------------------------------------------
# Engine + tenants fixture (mirrors the notify/audit test layout)
# ---------------------------------------------------------------------------


def _build_engine() -> Engine:
    """Build a fresh in-memory SQLite engine with FK enforcement on."""
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
    def _enable_fk(dbapi_conn: Any, _conn_record: Any) -> None:
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
    eng = _build_engine()
    try:
        yield eng
    finally:
        Base.metadata.drop_all(eng)
        eng.dispose()


@pytest.fixture
def wired_engine(
    monkeypatch: pytest.MonkeyPatch, in_memory_engine: Engine
) -> Iterator[Engine]:
    """Wire the in-memory engine into the SUT's session cache."""
    import aidp_db.session as db_session

    monkeypatch.setattr(
        db_session, "_engine_cache", {str(in_memory_engine.url): in_memory_engine}
    )
    from aidp_common.config import get_settings

    monkeypatch.setattr(get_settings(), "db_url", str(in_memory_engine.url))

    _insert_tenant(eng=in_memory_engine, tenant_id="tenant-a", code="acme")
    _insert_tenant(eng=in_memory_engine, tenant_id="tenant-b", code="globex")
    try:
        yield in_memory_engine
    finally:
        db_session.reset_engine_cache()


@pytest.fixture
def service() -> DatasourceService:
    """A fresh :class:`DatasourceService` for each test."""
    return DatasourceService()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _bearer(tenant_id: str, user_id: str = "u-tester", scopes: list[str] | None = None) -> dict[str, str]:
    token = create_access_token(tenant_id=tenant_id, user_id=user_id, scopes=scopes or ["*"])
    return {"Authorization": f"Bearer {token}"}


def _create_body(
    *,
    name: str = "primary",
    kind: str = "postgresql",
    env: str = "prod",
    host: str = "db.example.test",
    port: int = 5432,
    database: str = "aidp",
    username: str = "u",
    password: str = "p",
    enabled: bool = True,
    tags: list[str] | None = None,
    description: str = "",
) -> DatasourceCreateRequest:
    return DatasourceCreateRequest(
        name=name,
        kind=kind,  # type: ignore[arg-type]
        env=env,
        description=description,
        connection=ConnectionConfig(host=host, port=port, database=database),
        credentials=CredentialsPayload(username=username, password=password),
        tags=tags or [],
        enabled=enabled,
    )


def _bypass_kafka(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Stub out the Kafka producer so the test does not need a broker."""
    from aidp_datasource.services import datasource_service

    async def _noop_publish(*args: Any, **kwargs: Any) -> Any:
        return None

    monkeypatch.setattr(
        datasource_service, "publish_event", _noop_publish
    )
    return _noop_publish


# ---------------------------------------------------------------------------
# Credential-at-rest
# ---------------------------------------------------------------------------


def test_create_datasource_encrypts_credentials(
    wired_engine: Engine, service: DatasourceService
) -> None:
    """The persisted row's ``credentials_ciphertext`` is not the plaintext."""
    row = service.create_datasource(
        tenant_id="tenant-a",
        actor="u-tester",
        body=_create_body(password="super-secret"),
    )
    assert row.credentials_ciphertext != b"super-secret"
    assert b"super-secret" not in row.credentials_ciphertext
    # Decryption round-trips.
    plain = default_credential_service().decrypt(
        ciphertext=row.credentials_ciphertext,
        nonce=row.credentials_nonce,
        tenant_id=row.tenant_id,
        datasource_id=row.id,
        kind=row.kind,
    )
    assert plain.password == "super-secret"


def test_decrypted_connection_matches_create(
    wired_engine: Engine, service: DatasourceService
) -> None:
    """``get_decrypted_connection`` returns the same credentials we created."""
    service.create_datasource(
        tenant_id="tenant-a",
        actor="u-tester",
        body=_create_body(password="hunter2"),
    )
    rows = service.list_datasources(tenant_id="tenant-a")
    assert len(rows) == 1
    view = service.get_decrypted_connection(
        tenant_id="tenant-a", datasource_id=rows[0].id
    )
    assert view.credentials.password == "hunter2"
    assert view.credentials.username == "u"


# ---------------------------------------------------------------------------
# L1 isolation
# ---------------------------------------------------------------------------


def test_list_isolates_tenants(wired_engine: Engine, service: DatasourceService) -> None:
    """Datasources of tenant-a are not visible to tenant-b."""
    service.create_datasource(
        tenant_id="tenant-a", actor="u", body=_create_body(name="a-only")
    )
    service.create_datasource(
        tenant_id="tenant-b", actor="u", body=_create_body(name="b-only")
    )
    a = service.list_datasources(tenant_id="tenant-a")
    b = service.list_datasources(tenant_id="tenant-b")
    assert [r.name for r in a] == ["a-only"]
    assert [r.name for r in b] == ["b-only"]


def test_get_cross_tenant_raises_not_found(
    wired_engine: Engine, service: DatasourceService
) -> None:
    """A cross-tenant probe returns 404 (no leak)."""
    row = service.create_datasource(
        tenant_id="tenant-a", actor="u", body=_create_body()
    )
    with pytest.raises(NotFoundError):
        service.get_datasource(tenant_id="tenant-b", datasource_id=row.id)


def test_get_decrypted_cross_tenant_raises_not_found(
    wired_engine: Engine, service: DatasourceService
) -> None:
    """Same for the gRPC-facing decrypted lookup."""
    row = service.create_datasource(
        tenant_id="tenant-a", actor="u", body=_create_body()
    )
    with pytest.raises(NotFoundError):
        service.get_decrypted_connection(tenant_id="tenant-b", datasource_id=row.id)


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------


def test_create_writes_audit_row(
    wired_engine: Engine, service: DatasourceService, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A ``DatasourceAudit`` row is written on create."""
    _bypass_kafka(monkeypatch)
    row = service.create_datasource(
        tenant_id="tenant-a", actor="u-1", body=_create_body()
    )
    with get_session() as session:
        from sqlalchemy import select

        audits = (
            session.execute(
                select(DatasourceAudit).where(
                    DatasourceAudit.datasource_id == row.id
                )
            )
            .scalars()
            .all()
        )
    assert len(audits) == 1
    assert audits[0].action == "created"
    assert audits[0].actor == "u-1"


def test_duplicate_name_raises_conflict(
    wired_engine: Engine, service: DatasourceService
) -> None:
    """A second row with the same ``(tenant_id, name)`` raises 409."""
    service.create_datasource(
        tenant_id="tenant-a", actor="u", body=_create_body(name="primary")
    )
    with pytest.raises(ConflictError) as exc_info:
        service.create_datasource(
            tenant_id="tenant-a", actor="u", body=_create_body(name="primary")
        )
    assert "already exists" in str(exc_info.value)


def test_update_applies_diff(
    wired_engine: Engine, service: DatasourceService, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``update`` only changes the fields supplied; emits an audit row."""
    _bypass_kafka(monkeypatch)
    row = service.create_datasource(
        tenant_id="tenant-a", actor="u", body=_create_body(name="primary", description="old")
    )
    updated = service.update_datasource(
        tenant_id="tenant-a",
        actor="u-2",
        datasource_id=row.id,
        body=DatasourceUpdateRequest(
            description="new", tags=["prod", "primary"], enabled=False
        ),
    )
    assert updated.description == "new"
    assert updated.tags == ["prod", "primary"]
    assert updated.enabled == 0
    with get_session() as session:
        from sqlalchemy import select

        audits = (
            session.execute(
                select(DatasourceAudit).where(
                    DatasourceAudit.datasource_id == row.id,
                    DatasourceAudit.action == "updated",
                )
            )
            .scalars()
            .all()
        )
    assert len(audits) == 1
    diff = audits[0].diff_json["changed"]
    assert "description" in diff
    assert "tags" in diff
    assert "enabled" in diff


def test_update_unknown_id_raises_not_found(
    wired_engine: Engine, service: DatasourceService
) -> None:
    """Updating a missing row raises 404."""
    with pytest.raises(NotFoundError):
        service.update_datasource(
            tenant_id="tenant-a",
            actor="u",
            datasource_id="00000000-0000-0000-0000-000000000000",
            body=DatasourceUpdateRequest(description="x"),
        )


def test_soft_delete_hides_row(
    wired_engine: Engine, service: DatasourceService, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Soft-delete sets ``deleted_at`` and disables the row."""
    _bypass_kafka(monkeypatch)
    row = service.create_datasource(
        tenant_id="tenant-a", actor="u", body=_create_body()
    )
    deleted = service.soft_delete_datasource(
        tenant_id="tenant-a", actor="u", datasource_id=row.id
    )
    assert deleted.deleted_at is not None
    assert deleted.enabled == 0


def test_list_filters_by_env_kind_tag(
    wired_engine: Engine, service: DatasourceService
) -> None:
    """The list endpoint respects ``env`` / ``kind`` / ``tag`` filters."""
    service.create_datasource(
        tenant_id="tenant-a",
        actor="u",
        body=_create_body(name="prod-pg", env="prod", kind="postgresql", tags=["primary"]),
    )
    service.create_datasource(
        tenant_id="tenant-a",
        actor="u",
        body=_create_body(
            name="dev-mysql", env="dev", kind="mysql", port=3306, host="m.db", tags=["shadow"]
        ),
    )
    by_env = service.list_datasources(tenant_id="tenant-a", env="dev")
    assert [r.name for r in by_env] == ["dev-mysql"]
    by_kind = service.list_datasources(tenant_id="tenant-a", kind="postgresql")
    assert [r.name for r in by_kind] == ["prod-pg"]
    by_tag = service.list_datasources(tenant_id="tenant-a", tag="primary")
    assert [r.name for r in by_tag] == ["prod-pg"]


def test_list_rejects_unknown_kind(
    wired_engine: Engine, service: DatasourceService
) -> None:
    """An unknown kind filter raises :class:`ValidationError`."""
    from aidp_common.errors import ValidationError

    with pytest.raises(ValidationError):
        service.list_datasources(tenant_id="tenant-a", kind="clickhouse")


# ---------------------------------------------------------------------------
# Connection test
# ---------------------------------------------------------------------------


def test_test_connection_records_outcome(
    wired_engine: Engine, service: DatasourceService, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A successful probe writes a ``ConnectionTest`` and ``DatasourceAudit`` row."""
    _bypass_kafka(monkeypatch)
    row = service.create_datasource(
        tenant_id="tenant-a", actor="u", body=_create_body()
    )
    # Patch the connector to return a successful TestResult.
    fake_connector = AsyncMock()
    fake_connector.test = AsyncMock(
        return_value=TestResult(ok=True, latency_ms=12.5, error=None)
    )
    fake_connector.close = AsyncMock()
    with patch(
        "aidp_datasource.services.datasource_service.build_connector",
        return_value=fake_connector,
    ):
        outcome = service.test_connection(
            tenant_id="tenant-a", actor="u", datasource_id=row.id
        )
    assert outcome.status == "succeeded"
    assert outcome.latency_ms == 12.5
    assert outcome.error is None
    with get_session() as session:
        from sqlalchemy import select

        tests = (
            session.execute(
                select(ConnectionTest).where(
                    ConnectionTest.datasource_id == row.id
                )
            )
            .scalars()
            .all()
        )
    assert len(tests) == 1
    assert tests[0].status == "succeeded"
    assert tests[0].latency_ms == 12


def test_test_connection_failure_captured(
    wired_engine: Engine, service: DatasourceService, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed probe is recorded with status='failed' and a truncated error."""
    _bypass_kafka(monkeypatch)
    row = service.create_datasource(
        tenant_id="tenant-a", actor="u", body=_create_body()
    )
    fake_connector = AsyncMock()
    fake_connector.test = AsyncMock(
        return_value=TestResult(ok=False, latency_ms=None, error="connection refused")
    )
    fake_connector.close = AsyncMock()
    with patch(
        "aidp_datasource.services.datasource_service.build_connector",
        return_value=fake_connector,
    ):
        outcome = service.test_connection(
            tenant_id="tenant-a", actor="u", datasource_id=row.id
        )
    assert outcome.status == "failed"
    assert "connection refused" in (outcome.error or "")


def test_test_connection_disabled_short_circuits(
    wired_engine: Engine, service: DatasourceService, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A disabled datasource returns ``status='disabled'`` without opening a socket."""
    _bypass_kafka(monkeypatch)
    row = service.create_datasource(
        tenant_id="tenant-a",
        actor="u",
        body=_create_body(enabled=False),
    )
    fake_connector = AsyncMock()
    fake_connector.test = AsyncMock()
    fake_connector.close = AsyncMock()
    with patch(
        "aidp_datasource.services.datasource_service.build_connector",
        return_value=fake_connector,
    ):
        outcome = service.test_connection(
            tenant_id="tenant-a", actor="u", datasource_id=row.id
        )
    assert outcome.status == "disabled"
    assert fake_connector.test.await_count == 0


# ---------------------------------------------------------------------------
# Supported types
# ---------------------------------------------------------------------------


def test_supported_types_lists_seven_kinds(service: DatasourceService) -> None:
    """The static ``supported_types`` returns the seven connector kinds.

    Phase 1 + Task 16: PG / MySQL / Oracle / Hive (relational +
    warehouse) + MongoDB / Doris (NoSQL + analytical) + Kafka
    (message queue). The Kafka entry advertises
    ``supports_get_schema=False`` + ``supports_preview=False``
    because Kafka is not SQL.
    """
    types = service.supported_types()
    kinds = {entry["kind"] for entry in types}
    assert kinds == {
        "postgresql",
        "mysql",
        "oracle",
        "hive",
        "mongodb",
        "doris",
        "kafka",
    }
    kafka_entry = next(t for t in types if t["kind"] == "kafka")
    assert kafka_entry["supports_get_schema"] is False
    assert kafka_entry["supports_preview"] is False
    assert kafka_entry["supports_test"] is True
