"""Tests for the seven datasource connectors.

The connectors are tested in two modes:

- **Happy-path / failure-mode via mocks**: ``PostgresConnector`` /
  ``MySQLConnector`` / ``OracleConnector`` / ``HiveConnector`` /
  ``MongoDBConnector`` / ``DorisConnector`` /
  ``KafkaConnector`` delegate to async driver functions that we
  patch with :class:`unittest.mock.AsyncMock`. This lets us
  exercise the retry / error-projection / SQL-emission paths
  without requiring a real database.
- **Direct ``build_connector`` dispatch**: we verify that the
  factory returns the right concrete class for each
  :class:`DatasourceKind` value, and that an unknown kind
  raises :class:`ValueError`.

The brief notes: "testcontainers 时再接真 DB" — the testcontainers
gate is not in scope for Phase 1 unit testing, so the mock-based
path is the deliverable.

A future task can add testcontainers-based integration tests
that exercise the real drivers; those should be marked
``@pytest.mark.integration`` and deselected by default.

Task 16 (Phase 1+) adds three more connectors — MongoDB /
Doris / Kafka. The MongoDB + Doris connectors reuse the
SQL-shaped test mocks; the Kafka connector is
intentionally different (it has no SQL methods) and gets
dedicated tests below.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aidp_datasource.connectors.base import (
    BaseConnector,
    ConnectorError,
    build_connector,
    is_connector,
)
from aidp_datasource.connectors.doris import DorisConnector
from aidp_datasource.connectors.hive import HiveConnector
from aidp_datasource.connectors.kafka import KafkaConnector
from aidp_datasource.connectors.mongodb import MongoDBConnector
from aidp_datasource.connectors.mysql import MySQLConnector
from aidp_datasource.connectors.oracle import OracleConnector
from aidp_datasource.connectors.postgresql import PostgresConnector
from aidp_datasource.schemas import (
    KIND_DORIS,
    KIND_HIVE,
    KIND_KAFKA,
    KIND_MONGODB,
    KIND_MYSQL,
    KIND_ORACLE,
    KIND_POSTGRESQL,
    ConnectionConfig,
    CredentialsPayload,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _connection() -> ConnectionConfig:
    """A reusable connection descriptor."""
    return ConnectionConfig(
        host="localhost",
        port=5432,
        database="aidp",
        options={},
    )


def _credentials() -> CredentialsPayload:
    """A reusable credential blob."""
    return CredentialsPayload(username="u", password="p", extra={})


# ---------------------------------------------------------------------------
# Factory dispatch
# ---------------------------------------------------------------------------


def test_build_connector_postgres() -> None:
    """The factory returns a :class:`PostgresConnector` for ``postgresql``."""
    conn = build_connector(
        kind=KIND_POSTGRESQL,
        connection=_connection(),
        credentials=_credentials(),
    )
    assert isinstance(conn, PostgresConnector)
    assert conn.KIND == "postgresql"
    assert is_connector(conn)


def test_build_connector_mysql() -> None:
    """The factory returns a :class:`MySQLConnector` for ``mysql``."""
    conn = build_connector(
        kind=KIND_MYSQL,
        connection=_connection(),
        credentials=_credentials(),
    )
    assert isinstance(conn, MySQLConnector)
    assert conn.KIND == "mysql"


def test_build_connector_oracle() -> None:
    """The factory returns a :class:`OracleConnector` for ``oracle``."""
    conn = build_connector(
        kind=KIND_ORACLE,
        connection=ConnectionConfig(
            host="localhost", port=1521, database="ORCL", options={}
        ),
        credentials=_credentials(),
    )
    assert isinstance(conn, OracleConnector)
    assert conn.KIND == "oracle"


def test_build_connector_hive() -> None:
    """The factory returns a :class:`HiveConnector` for ``hive``."""
    conn = build_connector(
        kind=KIND_HIVE,
        connection=ConnectionConfig(
            host="localhost", port=10000, database="default", options={}
        ),
        credentials=_credentials(),
    )
    assert isinstance(conn, HiveConnector)
    assert conn.KIND == "hive"


def test_build_connector_mongodb() -> None:
    """The factory returns a :class:`MongoDBConnector` for ``mongodb``."""
    conn = build_connector(
        kind=KIND_MONGODB,
        connection=_connection(),
        credentials=_credentials(),
    )
    assert isinstance(conn, MongoDBConnector)
    assert conn.KIND == "mongodb"
    assert is_connector(conn)


def test_build_connector_doris() -> None:
    """The factory returns a :class:`DorisConnector` for ``doris``."""
    conn = build_connector(
        kind=KIND_DORIS,
        connection=ConnectionConfig(
            host="localhost", port=9030, database="aidp"
        ),
        credentials=_credentials(),
    )
    assert isinstance(conn, DorisConnector)
    assert conn.KIND == "doris"
    assert is_connector(conn)


def test_build_connector_kafka() -> None:
    """The factory returns a :class:`KafkaConnector` for ``kafka``."""
    conn = build_connector(
        kind=KIND_KAFKA,
        connection=ConnectionConfig(
            host="localhost", port=9092, database=None
        ),
        credentials=_credentials(),
    )
    assert isinstance(conn, KafkaConnector)
    assert conn.KIND == "kafka"
    assert is_connector(conn)


def test_build_connector_unknown_kind_raises() -> None:
    """An unknown kind raises :class:`ValueError`."""
    with pytest.raises(ValueError) as exc_info:
        build_connector(
            kind="clickhouse",  # type: ignore[arg-type]
            connection=_connection(),
            credentials=_credentials(),
        )
    assert "clickhouse" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Postgres: happy + failure paths
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_postgres_test_succeeds() -> None:
    """A successful ``SELECT 1`` round-trip yields ``ok=True`` with latency."""
    conn = PostgresConnector(
        connection=_connection(),
        credentials=_credentials(),
    )
    fake_conn = AsyncMock()
    fake_conn.fetchval = AsyncMock(return_value=1)
    with patch.object(conn, "_open", AsyncMock(return_value=fake_conn)):
        result = await conn.test(timeout_seconds=2.0)
    assert result.ok is True
    assert result.error is None
    assert result.latency_ms is not None
    assert result.latency_ms >= 0


@pytest.mark.asyncio
async def test_postgres_test_fails_when_driver_raises() -> None:
    """A driver-side error is captured in the ``error`` field, not raised."""
    conn = PostgresConnector(
        connection=_connection(),
        credentials=_credentials(),
    )
    with patch.object(
        conn,
        "_open",
        AsyncMock(side_effect=RuntimeError("connection refused")),
    ):
        result = await conn.test()
    assert result.ok is False
    assert result.error is not None
    assert "connection refused" in result.error


@pytest.mark.asyncio
async def test_postgres_test_fails_when_closed() -> None:
    """Calling ``test()`` on a closed connector short-circuits."""
    conn = PostgresConnector(
        connection=_connection(),
        credentials=_credentials(),
    )
    await conn.close()
    result = await conn.test()
    assert result.ok is False
    assert "closed" in (result.error or "")


@pytest.mark.asyncio
async def test_postgres_get_schema_projects_tables() -> None:
    """``get_schema`` projects the ``information_schema`` rows into ``TableInfo``."""
    conn = PostgresConnector(
        connection=_connection(),
        credentials=_credentials(),
    )
    fake_conn = AsyncMock()

    async def _fake_fetch(sql: str, *args: Any) -> list[dict[str, Any]]:
        if "information_schema.tables" in sql:
            return [
                {"table_schema": "public", "table_name": "users"},
                {"table_schema": "public", "table_name": "orders"},
            ]
        if "information_schema.columns" in sql:
            return [
                {
                    "table_schema": "public",
                    "table_name": "users",
                    "column_name": "id",
                    "data_type": "integer",
                    "is_nullable": "NO",
                },
                {
                    "table_schema": "public",
                    "table_name": "users",
                    "column_name": "email",
                    "data_type": "text",
                    "is_nullable": "YES",
                },
            ]
        return []

    fake_conn.fetch = AsyncMock(side_effect=_fake_fetch)
    fake_conn.escape_identifier = lambda ident: '"' + ident + '"'
    with patch.object(conn, "_open", AsyncMock(return_value=fake_conn)):
        tables = await conn.get_schema(database="aidp")
    assert len(tables) == 2
    users = next(t for t in tables if t.name == "users")
    assert users.schema == "public"
    assert len(users.columns) == 2
    assert users.columns[0].name == "id"
    assert users.columns[0].nullable is False
    assert users.columns[1].name == "email"
    assert users.columns[1].nullable is True


@pytest.mark.asyncio
async def test_postgres_preview_returns_dicts() -> None:
    """``preview`` returns each row as a column→value dict."""
    conn = PostgresConnector(
        connection=_connection(),
        credentials=_credentials(),
    )
    fake_conn = AsyncMock()
    fake_record = {"id": 1, "email": "a@b.test"}
    fake_conn.fetch = AsyncMock(return_value=[fake_record])
    fake_conn.escape_identifier = lambda ident: '"' + ident + '"'
    with patch.object(conn, "_open", AsyncMock(return_value=fake_conn)):
        rows = await conn.preview("users", limit=10)
    assert rows == [{"id": 1, "email": "a@b.test"}]


# ---------------------------------------------------------------------------
# MySQL
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_mysql_test_succeeds() -> None:
    """A successful ``SELECT 1`` yields ``ok=True`` with latency."""
    conn = MySQLConnector(
        connection=ConnectionConfig(host="localhost", port=3306, database="aidp"),
        credentials=_credentials(),
    )
    fake_conn = AsyncMock()
    cursor_cm = AsyncMock()
    cursor_cm.__aenter__ = AsyncMock(return_value=cursor_cm)
    cursor_cm.__aexit__ = AsyncMock(return_value=None)
    cursor_cm.execute = AsyncMock()
    cursor_cm.fetchone = AsyncMock(return_value=(1,))
    fake_conn.cursor = MagicMock(return_value=cursor_cm)
    with patch.object(conn, "_open", AsyncMock(return_value=fake_conn)):
        result = await conn.test()
    assert result.ok is True


@pytest.mark.asyncio
async def test_mysql_test_fails_on_driver_error() -> None:
    """A driver error is captured, not raised."""
    conn = MySQLConnector(
        connection=ConnectionConfig(host="localhost", port=3306, database="aidp"),
        credentials=_credentials(),
    )
    with patch.object(
        conn, "_open", AsyncMock(side_effect=RuntimeError("access denied"))
    ):
        result = await conn.test()
    assert result.ok is False
    assert "access denied" in (result.error or "")


# ---------------------------------------------------------------------------
# Oracle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_oracle_requires_service_name() -> None:
    """The connector refuses to open without a service_name."""
    conn = OracleConnector(
        connection=ConnectionConfig(host="localhost", port=1521, database=None),
        credentials=_credentials(),
    )
    with pytest.raises(ConnectorError) as exc_info:
        await conn._open(timeout_seconds=1.0)
    assert "service_name" in str(exc_info.value)


@pytest.mark.asyncio
async def test_oracle_test_succeeds() -> None:
    """A successful ``SELECT 1 FROM DUAL`` yields ``ok=True`` with latency."""
    conn = OracleConnector(
        connection=ConnectionConfig(
            host="localhost", port=1521, database="ORCLPDB1"
        ),
        credentials=_credentials(),
    )
    fake_conn = AsyncMock()
    cursor_cm = AsyncMock()
    cursor_cm.__aenter__ = AsyncMock(return_value=cursor_cm)
    cursor_cm.__aexit__ = AsyncMock(return_value=None)
    cursor_cm.execute = AsyncMock()
    cursor_cm.fetchone = AsyncMock(return_value=(1,))
    fake_conn.cursor = MagicMock(return_value=cursor_cm)
    with patch.object(conn, "_open", AsyncMock(return_value=fake_conn)):
        result = await conn.test()
    assert result.ok is True


# ---------------------------------------------------------------------------
# Hive
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_hive_test_succeeds_via_thread() -> None:
    """A successful ``SELECT 1`` (via ``asyncio.to_thread``) yields ``ok=True``."""
    conn = HiveConnector(
        connection=ConnectionConfig(host="localhost", port=10000, database="default"),
        credentials=_credentials(),
    )

    class _FakePyhiveConn:
        def cursor(self) -> Any:
            class _Cur:
                def execute(self, sql: str) -> None:
                    pass

                def fetchone(self) -> tuple[int]:
                    return (1,)

                def close(self) -> None:
                    pass

            return _Cur()

        def close(self) -> None:
            pass

    with patch.object(conn, "_open_sync", return_value=_FakePyhiveConn()):
        result = await conn.test()
    assert result.ok is True


@pytest.mark.asyncio
async def test_hive_get_schema_rejects_unsafe_table_name() -> None:
    """Hive's ``DESCRIBE`` is not parameter-bound; unsafe names are rejected."""
    conn = HiveConnector(
        connection=ConnectionConfig(host="localhost", port=10000, database="default"),
        credentials=_credentials(),
    )
    with pytest.raises(ConnectorError) as exc_info:
        await conn._describe(object(), "users; DROP TABLE x")
    assert "not safe" in str(exc_info.value)


# ---------------------------------------------------------------------------
# MongoDB
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_mongodb_test_succeeds() -> None:
    """A successful ``ping`` yields ``ok=True`` with latency."""
    conn = MongoDBConnector(
        connection=_connection(),
        credentials=_credentials(),
    )

    class _FakeCollection:
        def command(self, name: str) -> dict[str, int]:
            assert name == "ping"
            return {"ok": 1}

    class _FakeAdmin:
        def command(self, name: str) -> dict[str, int]:
            return {"ok": 1}

    class _FakeClient:
        def __init__(self) -> None:
            self.admin = _FakeAdmin()

        def close(self) -> None:
            pass

    with patch.object(conn, "_open_sync", return_value=_FakeClient()):
        result = await conn.test()
    assert result.ok is True
    assert result.error is None


@pytest.mark.asyncio
async def test_mongodb_test_fails_on_driver_error() -> None:
    """A driver error is captured, not raised."""
    conn = MongoDBConnector(
        connection=_connection(),
        credentials=_credentials(),
    )
    with patch.object(
        conn,
        "_open_sync",
        side_effect=RuntimeError("auth failed"),
    ):
        result = await conn.test()
    assert result.ok is False
    assert "auth failed" in (result.error or "")


def test_mongodb_infer_bson_type_classifies() -> None:
    """The BSON-type helper maps Python values to the right labels."""
    import datetime as _dt

    from aidp_datasource.connectors.mongodb import _infer_bson_type

    assert _infer_bson_type(None) == "null"
    assert _infer_bson_type(True) == "bool"
    assert _infer_bson_type(1) == "int"
    assert _infer_bson_type(1.5) == "double"
    assert _infer_bson_type("x") == "string"
    assert _infer_bson_type([1, 2]) == "array"
    assert _infer_bson_type({"a": 1}) == "object"
    assert _infer_bson_type(_dt.datetime(2025, 1, 1)) == "date"
    assert _infer_bson_type(_dt.date(2025, 1, 1)) == "date"
    assert _infer_bson_type(object()) == "mixed"
    # ``bool`` is a subclass of ``int``; the helper must
    # return ``"bool"`` for ``True`` / ``False``.
    assert _infer_bson_type(False) == "bool"


def test_mongodb_project_columns_picks_first_observation_order() -> None:
    """The column order is first-observation across the sample."""
    from aidp_datasource.connectors.mongodb import _project_columns

    sample = [
        {"email": "a@b.test", "name": "Alice"},
        {"name": "Bob", "email": "c@d.test", "id": 1},
    ]
    columns = _project_columns(sample=sample)
    assert [c.name for c in columns] == ["email", "name", "id"]
    # The type merges from both observations: ``name`` was
    # a string in both, ``email`` was a string in both, so
    # they should both be ``"string"`` and not nullable.
    email = next(c for c in columns if c.name == "email")
    assert email.type == "string"
    assert email.nullable is False
    name = next(c for c in columns if c.name == "name")
    assert name.type == "string"


def test_mongodb_project_columns_promotes_to_mixed_on_type_change() -> None:
    """A field observed with two distinct types becomes ``"mixed"`` + nullable."""
    from aidp_datasource.connectors.mongodb import _project_columns

    sample = [
        {"value": 1},
        {"value": "two"},
    ]
    columns = _project_columns(sample=sample)
    assert len(columns) == 1
    assert columns[0].type == "mixed"
    assert columns[0].nullable is True


def test_mongodb_project_columns_handles_null_value() -> None:
    """A ``None`` value is reported as nullable + ``"null"`` type."""
    from aidp_datasource.connectors.mongodb import _project_columns

    columns = _project_columns(sample=[{"x": None}])
    assert columns[0].type == "null"
    assert columns[0].nullable is True


def test_mongodb_list_topics_not_implemented() -> None:
    """MongoDB inherits the ``NotImplementedError`` for ``list_topics``."""
    conn = MongoDBConnector(
        connection=_connection(),
        credentials=_credentials(),
    )
    with pytest.raises(NotImplementedError):
        asyncio_run_sync(conn.list_topics())


def test_mongodb_get_topic_schema_not_implemented() -> None:
    """MongoDB inherits the ``NotImplementedError`` for ``get_topic_schema``."""
    conn = MongoDBConnector(
        connection=_connection(),
        credentials=_credentials(),
    )
    with pytest.raises(NotImplementedError):
        asyncio_run_sync(conn.get_topic_schema("events"))


@pytest.mark.asyncio
async def test_mongodb_get_schema_returns_collections() -> None:
    """``get_schema`` lists collections + projects a sample to columns."""
    conn = MongoDBConnector(
        connection=_connection(),
        credentials=_credentials(),
    )

    class _FakeCursor:
        def limit(self, n: int) -> _FakeCursor:
            return self

        def __iter__(self) -> Any:
            return iter([])

    class _FakeCollection:
        def __init__(self, name: str, docs: list[dict[str, Any]], count: int) -> None:
            self._name = name
            self._docs = docs
            self._count = count

        def estimated_document_count(self) -> int:
            return self._count

        def find(self, _filter: dict[str, Any], limit: int = 0) -> _FakeCursor:
            return _FakeCursor()

    class _FakeDB:
        def __init__(self) -> None:
            self._collections: dict[str, _FakeCollection] = {
                "users": _FakeCollection(
                    "users",
                    [
                        {"_id": 1, "email": "a@b.test", "name": "Alice"},
                        {"_id": 2, "email": "c@d.test", "name": "Bob"},
                    ],
                    42,
                ),
                "orders": _FakeCollection(
                    "orders",
                    [{"_id": 1, "quantity": 3}],
                    100,
                ),
                "system.users": _FakeCollection("system.users", [], 0),
            }

        def list_collections(self) -> list[dict[str, str]]:
            return [{"name": n} for n in self._collections]

        def __getitem__(self, name: str) -> _FakeCollection:
            return self._collections[name]

    class _FakeClient:
        def __init__(self) -> None:
            self._db = _FakeDB()

        def __getitem__(self, name: str) -> _FakeDB:
            return self._db

        def close(self) -> None:
            pass

    with patch.object(conn, "_open_sync", return_value=_FakeClient()):
        tables = await conn.get_schema(database="aidp")
    names = {t.name for t in tables}
    assert "users" in names
    assert "orders" in names
    assert "system.users" not in names


@pytest.mark.asyncio
async def test_mongodb_get_schema_requires_database() -> None:
    """``get_schema`` raises when no database is configured."""
    conn = MongoDBConnector(
        connection=ConnectionConfig(host="localhost", port=27017, database=None),
        credentials=_credentials(),
    )
    with pytest.raises(ConnectorError) as exc_info:
        await conn.get_schema()
    assert "database" in str(exc_info.value)


@pytest.mark.asyncio
async def test_mongodb_get_schema_handles_estimated_count_failure() -> None:
    """A failing ``estimated_document_count`` becomes ``None``."""
    conn = MongoDBConnector(
        connection=_connection(),
        credentials=_credentials(),
    )

    class _FakeCursor:
        def limit(self, n: int) -> _FakeCursor:
            return self

        def __iter__(self) -> Any:
            return iter([])

    class _FakeCollection:
        def estimated_document_count(self) -> int:
            raise RuntimeError("count failed")

        def find(self, _filter: dict[str, Any], limit: int = 0) -> _FakeCursor:
            return _FakeCursor()

    class _FakeDB:
        def __init__(self) -> None:
            self._coll = _FakeCollection()

        def list_collections(self) -> list[dict[str, str]]:
            return [{"name": "users"}]

        def __getitem__(self, name: str) -> _FakeCollection:
            return self._coll

    class _FakeClient:
        def __init__(self) -> None:
            self._db = _FakeDB()

        def __getitem__(self, name: str) -> _FakeDB:
            return self._db

        def close(self) -> None:
            pass

    with patch.object(conn, "_open_sync", return_value=_FakeClient()):
        tables = await conn.get_schema(database="aidp")
    assert tables[0].row_count_estimate is None


@pytest.mark.asyncio
async def test_mongodb_get_schema_handles_sample_failure() -> None:
    """A failing sample fetch returns ``[]`` columns for the collection."""
    conn = MongoDBConnector(
        connection=_connection(),
        credentials=_credentials(),
    )

    class _FakeCollection:
        def estimated_document_count(self) -> int:
            return 5

        def find(self, _filter: dict[str, Any], limit: int = 0) -> Any:
            raise RuntimeError("find failed")

    class _FakeDB:
        def __init__(self) -> None:
            self._coll = _FakeCollection()

        def list_collections(self) -> list[dict[str, str]]:
            return [{"name": "users"}]

        def __getitem__(self, name: str) -> _FakeCollection:
            return self._coll

    class _FakeClient:
        def __init__(self) -> None:
            self._db = _FakeDB()

        def __getitem__(self, name: str) -> _FakeDB:
            return self._db

        def close(self) -> None:
            pass

    with patch.object(conn, "_open_sync", return_value=_FakeClient()):
        tables = await conn.get_schema(database="aidp")
    assert tables[0].columns == []


@pytest.mark.asyncio
async def test_mongodb_preview_returns_documents() -> None:
    """``preview`` returns each document as a ``field → value`` dict."""
    conn = MongoDBConnector(
        connection=_connection(),
        credentials=_credentials(),
    )

    class _FakeCursor:
        def limit(self, n: int) -> _FakeCursor:
            return self

        def __iter__(self) -> Any:
            return iter([{"_id": 1, "email": "a@b.test"}])

    class _FakeCollection:
        def find(self, _filter: dict[str, Any], limit: int = 0) -> _FakeCursor:
            return _FakeCursor()

    class _FakeDB:
        def __getitem__(self, name: str) -> _FakeCollection:
            return _FakeCollection()

    class _FakeClient:
        def __init__(self) -> None:
            self._db = _FakeDB()

        def __getitem__(self, name: str) -> _FakeDB:
            return self._db

        def close(self) -> None:
            pass

    with patch.object(conn, "_open_sync", return_value=_FakeClient()):
        rows = await conn.preview("aidp.users", limit=10)
    assert rows == [{"_id": 1, "email": "a@b.test"}]


@pytest.mark.asyncio
async def test_mongodb_preview_rejects_underspecified_table() -> None:
    """``preview`` raises when no database + collection is supplied."""
    conn = MongoDBConnector(
        connection=ConnectionConfig(host="localhost", port=27017, database=None),
        credentials=_credentials(),
    )
    with pytest.raises(ConnectorError):
        await conn.preview("just_a_name", limit=10)


# ---------------------------------------------------------------------------
# Doris
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_doris_test_succeeds_via_thread() -> None:
    """A successful ``SELECT 1`` (via ``asyncio.to_thread``) yields ``ok=True``."""
    conn = DorisConnector(
        connection=ConnectionConfig(
            host="localhost", port=9030, database="aidp"
        ),
        credentials=_credentials(),
    )

    class _FakeCursor:
        def execute(self, sql: str) -> None:
            assert sql == "SELECT 1"

        def fetchone(self) -> tuple[int]:
            return (1,)

        def close(self) -> None:
            pass

    class _FakeConnection:
        def cursor(self) -> _FakeCursor:
            return _FakeCursor()

        def close(self) -> None:
            pass

    with patch.object(conn, "_open_sync", return_value=_FakeConnection()):
        result = await conn.test()
    assert result.ok is True


@pytest.mark.asyncio
async def test_doris_test_fails_on_driver_error() -> None:
    """A driver error is captured, not raised."""
    conn = DorisConnector(
        connection=ConnectionConfig(
            host="localhost", port=9030, database="aidp"
        ),
        credentials=_credentials(),
    )
    with patch.object(
        conn,
        "_open_sync",
        side_effect=RuntimeError("access denied"),
    ):
        result = await conn.test()
    assert result.ok is False
    assert "access denied" in (result.error or "")


def test_doris_select_limit_rejects_unsafe_name() -> None:
    """Unsafe table names are refused (Doris binds identifiers only via concat)."""
    conn = DorisConnector(
        connection=ConnectionConfig(
            host="localhost", port=9030, database="aidp"
        ),
        credentials=_credentials(),
    )
    with pytest.raises(ConnectorError) as exc_info:
        conn._select_limit(object(), "users; DROP TABLE x", 10)  # type: ignore[arg-type]
    assert "not safe" in str(exc_info.value)


def test_doris_list_topics_not_implemented() -> None:
    """Doris inherits the ``NotImplementedError`` for ``list_topics``."""
    conn = DorisConnector(
        connection=ConnectionConfig(
            host="localhost", port=9030, database="aidp"
        ),
        credentials=_credentials(),
    )
    with pytest.raises(NotImplementedError):
        asyncio_run_sync(conn.list_topics())


@pytest.mark.asyncio
async def test_doris_get_schema_returns_tableinfo() -> None:
    """``get_schema`` projects the 5 ``information_schema`` queries into ``TableInfo``."""
    conn = DorisConnector(
        connection=ConnectionConfig(
            host="localhost", port=9030, database="aidp"
        ),
        credentials=_credentials(),
    )

    class _FakeCursor:
        def __init__(self, plan: list[Any]) -> None:
            self._plan = plan
            self._i = 0

        def execute(self, sql: str, *args: Any) -> None:
            self._i += 1

        def fetchall(self) -> list[tuple[Any, ...]]:
            return list(self._plan[self._i - 1]) if self._i - 1 < len(self._plan) else []

        def close(self) -> None:
            pass

    plan: list[Any] = [
        # 1) tables list
        [("users",), ("orders",)],
        # 2) columns
        [
            ("users", "id", "bigint", "NO"),
            ("users", "email", "varchar", "YES"),
            ("orders", "id", "bigint", "NO"),
        ],
        # 3) PKs
        [("users", "id", 1)],
        # 4) indexes
        [],
        # 5) row counts
        [("users", 1000), ("orders", 50)],
    ]

    class _FakeConnection:
        def cursor(self) -> _FakeCursor:
            return _FakeCursor(plan)

        def close(self) -> None:
            pass

    fake = _FakeConnection()
    with patch.object(conn, "_open_sync", return_value=fake):
        tables = await conn.get_schema(database="aidp")
    names = {t.name for t in tables}
    assert names == {"users", "orders"}
    users = next(t for t in tables if t.name == "users")
    assert users.row_count_estimate == 1000
    assert users.primary_key == ["id"]
    col_by_name = {c.name: c for c in users.columns}
    assert col_by_name["id"].type == "bigint"
    assert col_by_name["id"].nullable is False
    assert col_by_name["email"].nullable is True


@pytest.mark.asyncio
async def test_doris_get_schema_requires_database() -> None:
    """``get_schema`` raises when no database is configured."""
    conn = DorisConnector(
        connection=ConnectionConfig(host="localhost", port=9030, database=None),
        credentials=_credentials(),
    )
    with pytest.raises(ConnectorError):
        await conn.get_schema()


@pytest.mark.asyncio
async def test_doris_get_schema_handles_empty_database() -> None:
    """An empty database returns ``[]`` (no tables, no error)."""
    conn = DorisConnector(
        connection=ConnectionConfig(
            host="localhost", port=9030, database="aidp"
        ),
        credentials=_credentials(),
    )

    class _FakeCursor:
        def __init__(self) -> None:
            self._i = 0

        def execute(self, sql: str, *args: Any) -> None:
            self._i += 1

        def fetchall(self) -> list[tuple[Any, ...]]:
            return []

        def close(self) -> None:
            pass

    class _FakeConnection:
        def cursor(self) -> _FakeCursor:
            return _FakeCursor()

        def close(self) -> None:
            pass

    with patch.object(conn, "_open_sync", return_value=_FakeConnection()):
        tables = await conn.get_schema(database="aidp")
    assert tables == []


@pytest.mark.asyncio
async def test_doris_get_schema_captures_introspection_failure() -> None:
    """A driver error during introspection becomes a ``ConnectorError``."""
    conn = DorisConnector(
        connection=ConnectionConfig(
            host="localhost", port=9030, database="aidp"
        ),
        credentials=_credentials(),
    )

    class _FakeCursor:
        def execute(self, sql: str, *args: Any) -> None:
            raise RuntimeError("table not found")

        def close(self) -> None:
            pass

    class _FakeConnection:
        def cursor(self) -> _FakeCursor:
            return _FakeCursor()

        def close(self) -> None:
            pass

    with patch.object(conn, "_open_sync", return_value=_FakeConnection()):
        with pytest.raises(ConnectorError) as exc_info:
            await conn.get_schema(database="aidp")
    assert "doris schema introspection failed" in str(exc_info.value)


@pytest.mark.asyncio
async def test_doris_preview_returns_rows() -> None:
    """``preview`` returns each row as a ``column → value`` dict."""
    conn = DorisConnector(
        connection=ConnectionConfig(
            host="localhost", port=9030, database="aidp"
        ),
        credentials=_credentials(),
    )

    class _FakeCursor:
        def __init__(self) -> None:
            self.description = [("id",), ("email",)]

        def execute(self, sql: str) -> None:
            pass

        def fetchall(self) -> list[tuple[Any, ...]]:
            return [(1, "a@b.test"), (2, "c@d.test")]

        def close(self) -> None:
            pass

    class _FakeConnection:
        def cursor(self) -> _FakeCursor:
            return _FakeCursor()

        def close(self) -> None:
            pass

    with patch.object(conn, "_open_sync", return_value=_FakeConnection()):
        rows = await conn.preview("users", limit=10)
    assert rows == [
        {"id": 1, "email": "a@b.test"},
        {"id": 2, "email": "c@d.test"},
    ]


@pytest.mark.asyncio
async def test_doris_preview_with_qualified_table() -> None:
    """A ``db.table`` argument strips the ``db.`` prefix."""
    conn = DorisConnector(
        connection=ConnectionConfig(
            host="localhost", port=9030, database="aidp"
        ),
        credentials=_credentials(),
    )

    class _FakeCursor:
        def __init__(self) -> None:
            self.description = [("id",)]
            self._executed: list[str] = []

        def execute(self, sql: str) -> None:
            self._executed.append(sql)

        def fetchall(self) -> list[tuple[Any, ...]]:
            return [(1,)]

        def close(self) -> None:
            pass

    cursor = _FakeCursor()
    class _FakeConnection:
        def cursor(self) -> _FakeCursor:
            return cursor

        def close(self) -> None:
            pass

    with patch.object(conn, "_open_sync", return_value=_FakeConnection()):
        await conn.preview("aidp.users", limit=10)
    assert "`users`" in cursor._executed[0]
    assert "aidp" not in cursor._executed[0]


# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------


def test_kafka_get_schema_raises_not_implemented() -> None:
    """Kafka's ``get_schema`` raises :class:`NotImplementedError`."""
    conn = KafkaConnector(
        connection=ConnectionConfig(host="localhost", port=9092, database=None),
        credentials=_credentials(),
    )
    with pytest.raises(NotImplementedError) as exc_info:
        asyncio_run_sync(conn.get_schema())
    assert "kafka connector" in str(exc_info.value)


def test_kafka_preview_raises_not_implemented() -> None:
    """Kafka's ``preview`` raises :class:`NotImplementedError`."""
    conn = KafkaConnector(
        connection=ConnectionConfig(host="localhost", port=9092, database=None),
        credentials=_credentials(),
    )
    with pytest.raises(NotImplementedError) as exc_info:
        asyncio_run_sync(conn.preview("topic", limit=10))
    assert "kafka connector" in str(exc_info.value)


def test_kafka_build_bootstrap_servers_uses_options_when_present() -> None:
    """The ``bootstrap_servers`` option overrides the host:port default."""
    conn = KafkaConnector(
        connection=ConnectionConfig(
            host="localhost",
            port=9092,
            database=None,
            options={"bootstrap_servers": "broker1:9092,broker2:9092"},
        ),
        credentials=_credentials(),
    )
    assert (
        conn._build_bootstrap_servers()
        == "broker1:9092,broker2:9092"
    )


def test_kafka_build_bootstrap_servers_falls_back_to_host_port() -> None:
    """Without ``bootstrap_servers``, the connector uses host:port."""
    conn = KafkaConnector(
        connection=ConnectionConfig(host="localhost", port=9092, database=None),
        credentials=_credentials(),
    )
    assert conn._build_bootstrap_servers() == "localhost:9092"


def test_kafka_parse_avro_schema_extracts_fields() -> None:
    """The Avro parser returns a flat field list in declaration order."""
    from aidp_datasource.connectors.kafka import _parse_avro_schema

    fields = _parse_avro_schema(json.dumps({
        "type": "record",
        "name": "User",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "email", "type": ["null", "string"]},
        ],
    }))
    assert [f.name for f in fields] == ["id", "email"]
    assert fields[0].type == "long"
    assert fields[0].nullable is False
    assert fields[1].type == "string"
    assert fields[1].nullable is True  # [null, string] → nullable


def test_kafka_parse_avro_schema_returns_empty_for_non_record() -> None:
    """A non-record Avro schema returns ``[]``."""
    from aidp_datasource.connectors.kafka import _parse_avro_schema

    fields = _parse_avro_schema('"string"')
    assert fields == []


def test_kafka_parse_avro_schema_handles_complex_types() -> None:
    """The Avro parser handles arrays, maps, and nested records."""
    from aidp_datasource.connectors.kafka import _avro_type_label

    # Array of string.
    label, nullable = _avro_type_label({"type": "array", "items": "string"})
    assert label == "array<string>"
    assert nullable is False
    # Map of long.
    label, _ = _avro_type_label({"type": "map", "values": "long"})
    assert label == "map<string,long>"
    # Nested record.
    label, _ = _avro_type_label(
        {"type": "record", "name": "Address", "fields": []}
    )
    assert label == "record"
    # Union with two non-null types.
    label, nullable = _avro_type_label(["string", "long"])
    assert label == "union"
    assert nullable is True
    # Unknown dict type.
    label, _ = _avro_type_label({"type": "unknown_thing"})
    assert label == "unknown"


def test_kafka_parse_json_schema_extracts_properties() -> None:
    """The JSON-Schema parser returns the top-level properties."""
    from aidp_datasource.connectors.kafka import _parse_json_schema

    fields = _parse_json_schema(json.dumps({
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "email": {"type": ["string", "null"]},
        },
    }))
    assert [f.name for f in fields] == ["id", "email"]
    # The ``email`` field is nullable because ``null`` is in
    # its type union.
    email = next(f for f in fields if f.name == "email")
    assert email.nullable is True


def test_kafka_parse_json_schema_handles_non_object() -> None:
    """A non-object JSON-Schema returns ``[]``."""
    from aidp_datasource.connectors.kafka import _parse_json_schema

    assert _parse_json_schema("not json") == []
    assert _parse_json_schema('"a string"') == []


def test_kafka_get_topic_schema_protobuf() -> None:
    """A Protobuf Schema Registry response yields ``format='protobuf'``."""
    conn = KafkaConnector(
        connection=ConnectionConfig(
            host="localhost", port=9092, database=None,
            options={"schema_registry_url": "http://sr.test:8081"},
        ),
        credentials=_credentials(),
    )
    import sys
    import types

    class _FakeResp:
        status_code = 200
        text = ""
        def json(self) -> Any:
            return {
                "schemaType": "protobuf",
                "schema": "unused",
            }

    class _FakeAsyncClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self._resp = _FakeResp()

        async def __aenter__(self) -> _FakeAsyncClient:
            return self

        async def __aexit__(self, *args: Any) -> None:
            pass

        async def get(self, url: str, auth: Any = None) -> Any:
            return self._resp

    fake_httpx = types.ModuleType("httpx")
    fake_httpx.AsyncClient = _FakeAsyncClient
    monkeypatch_ = pytest.MonkeyPatch()
    try:
        monkeypatch_.setitem(sys.modules, "httpx", fake_httpx)
        result = asyncio_run_sync(conn.get_topic_schema("events"))
        assert result.format == "protobuf"
        assert result.fields == []
    finally:
        monkeypatch_.undo()


def test_kafka_get_topic_schema_registry_invalid_json(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An invalid JSON Schema Registry response is wrapped in ``ConnectorError``."""
    import sys
    import types

    conn = KafkaConnector(
        connection=ConnectionConfig(
            host="localhost", port=9092, database=None,
            options={"schema_registry_url": "http://sr.test:8081"},
        ),
        credentials=_credentials(),
    )

    class _FakeResp:
        status_code = 200
        text = ""
        def json(self) -> Any:
            raise ValueError("bad json")

    class _FakeAsyncClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self._resp = _FakeResp()

        async def __aenter__(self) -> _FakeAsyncClient:
            return self

        async def __aexit__(self, *args: Any) -> None:
            pass

        async def get(self, url: str, auth: Any = None) -> Any:
            return self._resp

    fake_httpx = types.ModuleType("httpx")
    fake_httpx.AsyncClient = _FakeAsyncClient
    monkeypatch.setitem(sys.modules, "httpx", fake_httpx)
    with pytest.raises(ConnectorError) as exc_info:
        asyncio_run_sync(conn.get_topic_schema("events"))
    assert "not valid JSON" in str(exc_info.value)


def test_kafka_schemaless_inference_handles_json_values() -> None:
    """The JSON inference helper classifies types correctly."""
    from aidp_datasource.connectors.kafka import (
        _json_type_label,
        _merge_json_field,
    )

    accumulator: dict[str, dict[str, Any]] = {}
    _merge_json_field(accumulator=accumulator, field="email", value="a@b.test")
    assert accumulator["email"]["type"] == "string"
    _merge_json_field(accumulator=accumulator, field="count", value=42)
    assert accumulator["count"]["type"] == "number"
    _merge_json_field(accumulator=accumulator, field="count", value="forty-two")
    assert accumulator["count"]["type"] == "mixed"
    assert accumulator["count"]["nullable"] is True
    # Sanity: the JSON type label classifies common scalars.
    assert _json_type_label(True) == "boolean"
    assert _json_type_label(None) == "null"


def test_kafka_get_topic_schema_schemaless_when_no_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without a ``schema_registry_url``, the connector infers a JSON shape."""
    conn = KafkaConnector(
        connection=ConnectionConfig(host="localhost", port=9092, database=None),
        credentials=_credentials(),
    )

    class _FakeRecord:
        def __init__(self, value: Any) -> None:
            self.value = value

    class _FakeConsumer:
        def __init__(self) -> None:
            self._records: list[_FakeRecord] = [
                _FakeRecord(b'{"id": 1, "email": "a@b.test"}'),
            ]

        async def start(self) -> None:
            pass

        async def stop(self) -> None:
            pass

        async def getmany(self, timeout_ms: int, max_records: int) -> dict[Any, list[Any]]:
            # Return a dict keyed by ``(topic, partition)``
            # tuples (the real :class:`aiokafka.TopicPartition`
            # shape); the value is a list of records.
            return {("events", 0): self._records}

    fake_mod = MagicMock()
    fake_mod.AIOKafkaConsumer = MagicMock(return_value=_FakeConsumer())
    monkeypatch.setattr(conn, "_ensure_driver", lambda: fake_mod)
    result = asyncio_run_sync(conn.get_topic_schema("events"))
    assert result.topic == "events"
    assert result.format == "json"
    field_names = [f.name for f in result.fields]
    assert "id" in field_names
    assert "email" in field_names


def test_kafka_get_topic_schema_schemaless_bytes_when_no_sample(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-JSON sample reports ``format='bytes'`` and empty fields."""
    conn = KafkaConnector(
        connection=ConnectionConfig(host="localhost", port=9092, database=None),
        credentials=_credentials(),
    )

    class _FakeRecord:
        def __init__(self, value: Any) -> None:
            self.value = value

    class _FakeConsumer:
        async def start(self) -> None:
            pass

        async def stop(self) -> None:
            pass

        async def getmany(self, timeout_ms: int, max_records: int) -> dict[Any, list[Any]]:
            return {("events", 0): [_FakeRecord(b"\x00\x01not json")]}

    fake_mod = MagicMock()
    fake_mod.AIOKafkaConsumer = MagicMock(return_value=_FakeConsumer())
    monkeypatch.setattr(conn, "_ensure_driver", lambda: fake_mod)
    result = asyncio_run_sync(conn.get_topic_schema("events"))
    assert result.format == "bytes"
    assert result.fields == []


def test_kafka_get_topic_schema_registry_avro(monkeypatch: pytest.MonkeyPatch) -> None:
    """A Schema Registry ``avro`` response is parsed to ``TopicFieldInfo``."""
    import sys
    import types

    conn = KafkaConnector(
        connection=ConnectionConfig(
            host="localhost", port=9092, database=None,
            options={"schema_registry_url": "http://sr.test:8081"},
        ),
        credentials=_credentials(),
    )

    class _FakeAsyncClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self._resp = MagicMock()
            self._resp.status_code = 200
            self._resp.json = MagicMock(
                return_value={
                    "schemaType": "avro",
                    "schema": json.dumps(
                        {
                            "type": "record",
                            "name": "User",
                            "fields": [
                                {"name": "id", "type": "long"},
                                {"name": "email", "type": ["null", "string"]},
                            ],
                        }
                    ),
                }
            )

        async def __aenter__(self) -> _FakeAsyncClient:
            return self

        async def __aexit__(self, *args: Any) -> None:
            pass

        async def get(self, url: str, auth: Any = None) -> Any:
            return self._resp

    # Inject a fake ``httpx`` module into ``sys.modules`` so
    # the connector's ``import httpx`` finds it.
    fake_httpx = types.ModuleType("httpx")
    fake_httpx.AsyncClient = _FakeAsyncClient
    monkeypatch.setitem(sys.modules, "httpx", fake_httpx)
    result = asyncio_run_sync(conn.get_topic_schema("events"))
    assert result.format == "avro"
    assert [f.name for f in result.fields] == ["id", "email"]


def test_kafka_get_topic_schema_registry_404_falls_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 404 from the Schema Registry falls back to the schemaless path."""
    import sys
    import types

    conn = KafkaConnector(
        connection=ConnectionConfig(
            host="localhost", port=9092, database=None,
            options={"schema_registry_url": "http://sr.test:8081"},
        ),
        credentials=_credentials(),
    )

    class _FakeResp:
        status_code = 404
        text = "not found"
        def json(self) -> Any:
            return {}

    class _FakeAsyncClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self._resp = _FakeResp()

        async def __aenter__(self) -> _FakeAsyncClient:
            return self

        async def __aexit__(self, *args: Any) -> None:
            pass

        async def get(self, url: str, auth: Any = None) -> Any:
            return self._resp

    fake_httpx = types.ModuleType("httpx")
    fake_httpx.AsyncClient = _FakeAsyncClient
    monkeypatch.setitem(sys.modules, "httpx", fake_httpx)
    # A consumer that returns no records → ``format='bytes'``,
    # ``fields=[]``.
    class _FakeConsumer:
        async def start(self) -> None:
            pass

        async def stop(self) -> None:
            pass

        async def getmany(self, timeout_ms: int, max_records: int) -> dict[Any, list[Any]]:
            return {}

    fake_mod = MagicMock()
    fake_mod.AIOKafkaConsumer = MagicMock(return_value=_FakeConsumer())
    monkeypatch.setattr(conn, "_ensure_driver", lambda: fake_mod)
    result = asyncio_run_sync(conn.get_topic_schema("events"))
    assert result.format == "bytes"
    assert result.fields == []


def test_kafka_get_topic_schema_registry_server_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 5xx from the Schema Registry is wrapped in ``ConnectorError``."""
    import sys
    import types

    conn = KafkaConnector(
        connection=ConnectionConfig(
            host="localhost", port=9092, database=None,
            options={"schema_registry_url": "http://sr.test:8081"},
        ),
        credentials=_credentials(),
    )

    class _FakeResp:
        status_code = 500
        text = "internal error"
        def json(self) -> Any:
            return {}

    class _FakeAsyncClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self._resp = _FakeResp()

        async def __aenter__(self) -> _FakeAsyncClient:
            return self

        async def __aexit__(self, *args: Any) -> None:
            pass

        async def get(self, url: str, auth: Any = None) -> Any:
            return self._resp

    fake_httpx = types.ModuleType("httpx")
    fake_httpx.AsyncClient = _FakeAsyncClient
    monkeypatch.setitem(sys.modules, "httpx", fake_httpx)
    with pytest.raises(ConnectorError) as exc_info:
        asyncio_run_sync(conn.get_topic_schema("events"))
    assert "500" in str(exc_info.value)


def test_kafka_list_topics_projects_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``list_topics`` projects the broker metadata into :class:`TopicInfo`."""
    conn = KafkaConnector(
        connection=ConnectionConfig(host="localhost", port=9092, database=None),
        credentials=_credentials(),
    )

    class _FakeAdmin:
        async def list_topics(self) -> dict[str, dict[str, Any]]:
            return {
                "events": {
                    "partitions": [
                        {"replicas": [1, 2, 3]},
                        {"replicas": [1, 2, 3]},
                    ]
                },
                "metrics": {
                    "partitions": [
                        {"replicas": [1]},
                    ]
                },
                # System topics are filtered out.
                "__consumer_offsets": {
                    "partitions": [{"replicas": [1]}]
                },
            }

        async def close(self) -> None:
            pass

        async def start(self) -> None:
            pass

    fake_mod = MagicMock()
    fake_mod.admin.AIOKafkaAdminClient = MagicMock(return_value=_FakeAdmin())
    monkeypatch.setattr(conn, "_ensure_driver", lambda: fake_mod)
    result = asyncio_run_sync(conn.list_topics())
    names = {t.name for t in result}
    assert names == {"events", "metrics"}
    events = next(t for t in result if t.name == "events")
    assert events.partition_count == 2
    assert events.replication_factor == 3


def test_kafka_list_topics_with_empty_partitions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A topic with no partitions reports ``partition_count=0``."""
    conn = KafkaConnector(
        connection=ConnectionConfig(host="localhost", port=9092, database=None),
        credentials=_credentials(),
    )

    class _FakeAdmin:
        async def list_topics(self) -> dict[str, Any]:
            return {
                "empty_topic": {"partitions": []},
                "no_partitions_key": {},
            }

        async def close(self) -> None:
            pass

        async def start(self) -> None:
            pass

    fake_mod = MagicMock()
    fake_mod.admin.AIOKafkaAdminClient = MagicMock(return_value=_FakeAdmin())
    monkeypatch.setattr(conn, "_ensure_driver", lambda: fake_mod)
    result = asyncio_run_sync(conn.list_topics())
    assert {t.name for t in result} == {"empty_topic", "no_partitions_key"}


def test_kafka_admin_close_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    """``_close_admin`` is a no-op when no admin client is open."""
    conn = KafkaConnector(
        connection=ConnectionConfig(host="localhost", port=9092, database=None),
        credentials=_credentials(),
    )
    asyncio_run_sync(conn._close_admin())  # no-op, no error
    assert conn._admin is None


# ---------------------------------------------------------------------------
# BaseConnector contract
# ---------------------------------------------------------------------------


def test_is_connector_returns_true_for_subclasses() -> None:
    """All seven connectors satisfy the structural Protocol."""
    for ctor in (
        PostgresConnector,
        MySQLConnector,
        OracleConnector,
        HiveConnector,
        MongoDBConnector,
        DorisConnector,
        KafkaConnector,
    ):
        conn = ctor(
            connection=ConnectionConfig(
                host="h", port=1234, database="d", options={}
            ),
            credentials=_credentials(),
        )
        assert is_connector(conn)
        assert isinstance(conn, BaseConnector)


def test_is_connector_returns_false_for_plain_object() -> None:
    """A plain class without the four methods does not satisfy the Protocol."""
    class _Not:
        KIND = "postgresql"

    assert is_connector(_Not()) is False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def asyncio_run_sync(coro: Any) -> Any:
    """Drive a coroutine to completion from a sync test.

    The connector protocol is async, but the Kafka / Doris
    ``NotImplementedError`` paths and the schemaless-schema
    helper tests are simpler to write as sync. The helper
    uses :func:`asyncio.run` so the coroutine is fully
    isolated from the test's event loop (the event loop is
    re-created for each call).
    """
    import asyncio

    return asyncio.run(coro)
