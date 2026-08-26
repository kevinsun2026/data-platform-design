"""Connector Protocol — the contract every datasource driver satisfies.

The seven driver implementations (:mod:`aidp_datasource.connectors.postgresql`,
:mod:`aidp_datasource.connectors.mysql`, :mod:`aidp_datasource.connectors.oracle`,
:mod:`aidp_datasource.connectors.hive`,
:mod:`aidp_datasource.connectors.mongodb`,
:mod:`aidp_datasource.connectors.doris`,
:mod:`aidp_datasource.connectors.kafka`) all expose the same
:class:`Connector` Protocol so the datasource service can dispatch
on ``kind`` without an ``if/elif`` ladder.

The Protocol is structural (``typing.Protocol``) — concrete classes
do not need to inherit from it. The :func:`is_connector` helper
performs a runtime sanity check for tests + service bootstrap.

Lifecycle
---------

Connectors are short-lived: the service builds a fresh connector
per request (one ``test()`` or ``get_schema()`` call), invokes
:meth:`Connector.close` in a ``finally`` block, and discards the
instance. Driver connections are opened lazily inside ``test()`` /
``get_schema()`` / ``preview()`` so the cost of building the
connector (driver import, URL parsing) is paid even for the
``GET /api/v1/datasources/types`` endpoint, but the cost of opening
a real socket is paid only when actually needed.

Non-SQL kinds
-------------

Kafka is **not** a SQL store, so the brief scopes its connector to
``list_topics()`` + ``get_topic_schema(topic)`` (i.e. the schema of
the messages on a topic, not a database schema). The SQL-flavored
methods (:meth:`Connector.get_schema` and :meth:`Connector.preview`)
raise :class:`NotImplementedError` for the Kafka connector; the
:class:`BaseConnector` base class wires the no-op default so a
SQL-only caller that accidentally invokes them on Kafka fails
loudly. Symmetrically, the five SQL connectors (PG / MySQL /
Oracle / Hive / MongoDB / Doris) raise :class:`NotImplementedError`
on the Kafka-only methods.
"""

from __future__ import annotations

import abc
import time
from dataclasses import dataclass, field
from typing import Any, ClassVar, Protocol, runtime_checkable

from aidp_datasource.schemas import ConnectionConfig, CredentialsPayload, DatasourceKind

# ---------------------------------------------------------------------------
# Public DTOs
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ColumnInfo:
    """One column in a remote table.

    Attributes:
        name: Column name (case as the driver returns it).
        type: Driver-supplied type label (e.g. ``"integer"`` for PG,
            ``"NUMBER(10,2)"`` for Oracle). Stored verbatim — the
            agent-gateway interprets the label per kind.
        nullable: Whether the column allows NULL.
    """

    name: str
    type: str
    nullable: bool = True


@dataclass(frozen=True)
class IndexInfo:
    """One secondary index on a remote table.

    The schema is the minimum the schema service needs to render
    DDL and to feed a downstream catalog. We deliberately do not
    surface storage parameters (fillfactor, tablespace, etc.) — the
    brief scopes Phase 1 to "name + columns + uniqueness".

    Attributes:
        name: Index name (case as the driver returns it).
        columns: Ordered list of column names that the index covers.
            Composite indexes preserve column order. For expression
            indexes the driver may return ``"(col)"`` style strings;
            we store them verbatim so the DDL re-emit is a copy.
        unique: ``True`` for unique indexes (``UNIQUE`` /
            ``UNIQUE INDEX``); ``False`` for non-unique secondary
            indexes. Primary-key indexes are *not* returned here —
            they are surfaced via :attr:`TableInfo.primary_key`.
    """

    name: str
    columns: list[str] = field(default_factory=list)
    unique: bool = False


@dataclass(frozen=True)
class TableInfo:
    """One table in a remote schema.

    Attributes:
        name: Table name.
        schema: Schema / database the table lives in (``"public"`` for
            PG, the user for MySQL, the owner for Oracle, the
            database for Hive). ``None`` when the driver does not
            expose the concept.
        columns: Column descriptors. Populated eagerly by
            :meth:`Connector.get_schema` in Phase 1+ (the brief's
            Task 15 contract).
        primary_key: Ordered column names that form the table's
            primary key. Empty when the table has no PK (e.g. a
            Hive-managed table that is not ``ORC`` with
            ``pk``-style constraints, or a regular MySQL heap
            table).
        indexes: Secondary (non-PK) indexes. Empty for tables that
            have no secondary indexes. PK indexes are *not*
            duplicated here — see :attr:`primary_key`.
        row_count_estimate: Approximate row count from the engine's
            catalog (e.g. ``pg_class.reltuples``,
            ``information_schema.tables.table_rows``,
            ``all_tables.num_rows``). ``None`` when the driver
            cannot surface a value cheaply (e.g. Hive without
            ``ANALYZE TABLE``). This is a *hint* for the agent;
            a precise count still requires ``SELECT COUNT(*)``.
    """

    name: str
    schema: str | None = None
    columns: list[ColumnInfo] = field(default_factory=list)
    primary_key: list[str] = field(default_factory=list)
    indexes: list[IndexInfo] = field(default_factory=list)
    row_count_estimate: int | None = None


@dataclass(frozen=True)
class TestResult:
    """Outcome of a ``Connector.test()`` call.

    Attributes:
        ok: ``True`` on a successful probe (``SELECT 1`` or driver
            equivalent).
        latency_ms: Wall-clock latency of the probe. ``None`` when
            the test failed before the connection was attempted
            (e.g. an authentication error returned synchronously).
        error: Driver-side error string (``str(exc)``). ``None`` on
            success.
    """

    ok: bool
    latency_ms: float | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "latency_ms": self.latency_ms,
            "error": self.error,
        }


# ---------------------------------------------------------------------------
# Kafka-only DTOs
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TopicInfo:
    """One topic in a Kafka cluster.

    Kafka topics are not SQL tables — they are append-only message
    streams — so a :class:`TopicInfo` carries the minimum the
    operator UI / PII service needs to render a list:

    Attributes:
        name: The topic name (verbatim, case as the broker returns
            it).
        partition_count: Number of partitions. The brief surfaces
            this so the agent can warn the operator about a
            ``--partitions 1`` misconfig that kills throughput.
        replication_factor: The cluster's replication factor for
            the topic. ``None`` when the broker did not surface a
            value (older Kafka, or auth-restricted cluster).
    """

    name: str
    partition_count: int = 0
    replication_factor: int | None = None


@dataclass(frozen=True)
class TopicFieldInfo:
    """One field inside a topic's message schema.

    Attributes:
        name: The field name (verbatim, as the Schema Registry /
            Avro / Protobuf definition returns it).
        type: The field's type label. For Avro / Protobuf we use
            the canonical string (``"string"`` / ``"long"`` /
            ``"record"``); for JSON topics the connector reports
            the inferred JSON type (``"string"`` / ``"number"`` /
            ``"boolean"`` / ``"object"`` / ``"array"`` / ``"null"``).
        nullable: ``True`` when the field can be absent (``null``
            in JSON; ``["null", "string"]`` union in Avro; optional
            in Protobuf). ``False`` otherwise. ``True`` for the
            "no schema" case where every field is treated as
            nullable because the broker cannot prove otherwise.
    """

    name: str
    type: str
    nullable: bool = True


@dataclass(frozen=True)
class TopicSchema:
    """The schema of one Kafka topic.

    Kafka topics may have a Schema-Registry-stored schema (Avro /
    Protobuf / JSON-Schema) or be **schemaless** (raw JSON / bytes).
    The connector surfaces both shapes through this DTO so the
    downstream consumer (the PII service, the agent-gateway) does
    not need to special-case.

    Attributes:
        topic: The topic name (echoed for the caller's convenience).
        format: The wire format. One of
            ``"avro"`` / ``"protobuf"`` / ``"json_schema"`` /
            ``"json"`` / ``"bytes"`` / ``"unknown"``.
            ``"unknown"`` is the conservative default when the
            connector could not classify the topic (e.g. no Schema
            Registry configured and the topic is empty so the
            sample could not infer a format).
        fields: The field list, in declaration order. Empty for
            the ``"bytes"`` / ``"unknown"`` formats where the
            connector cannot surface a schema.
    """

    topic: str
    format: str
    fields: list[TopicFieldInfo] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Connector Protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class Connector(Protocol):
    """The contract every datasource driver satisfies.

    Implementations are concrete classes (no ABC) because the
    drivers come from four different third-party packages; an
    abstract base would force us to vendor a stub. The
    :func:`is_connector` runtime check is the substitute.

    All methods are async because the seven driver packages we use
    (``asyncpg`` / ``aiomysql`` / ``oracledb`` (async mode) /
    ``pyhive`` / ``pymongo`` (async via ``asyncio.to_thread``) /
    ``pymysql`` (async via ``asyncio.to_thread``) / ``aiokafka``)
    are async-first; sync drivers would require a
    ``run_in_executor`` wrapper that hides latency spikes.

    The factory :func:`aidp_datasource.connectors.build_connector`
    returns the right concrete connector for a given
    ``Datasource.kind``. The factory lazy-imports the driver
    modules so the platform never pays the import cost for a
    driver that is not in use.

    Non-SQL kinds
    ^^^^^^^^^^^^^

    Kafka is **not** a SQL store, so the brief scopes its
    connector to :meth:`list_topics` + :meth:`get_topic_schema`
    (the schema of the messages on a topic, not a database
    schema). The SQL-flavored methods (:meth:`get_schema` and
    :meth:`preview`) raise :class:`NotImplementedError` for the
    Kafka connector; symmetrically, the six SQL connectors raise
    :class:`NotImplementedError` on the Kafka-only methods. The
    :class:`BaseConnector` base class wires the no-op default so
    a misrouted call fails loudly with a clear error.
    """

    #: The :data:`aidp_datasource.schemas.DatasourceKind` this
    #: connector handles. Set as a class attribute on the
    #: concrete class so the factory can do an ``isinstance``-style
    #: dispatch without instantiating.
    KIND: ClassVar[DatasourceKind]

    async def test(self, *, timeout_seconds: float | None = None) -> TestResult:
        """Open a probe connection, run a ``SELECT 1`` (or equivalent), close.

        Returns:
            A :class:`TestResult` describing the outcome. The
            method never raises for connection-level failures;
            those are returned as ``ok=False`` with ``error`` set.
            The only exception path is for invalid configuration
            (e.g. a missing ``host``), which is a programming bug
            in the caller — we still wrap it in a :class:`TestResult`
            for safety so a single bad row cannot crash the
            request handler.
        """
        ...

    async def get_schema(self, database: str | None = None) -> list[TableInfo]:
        """List the tables in *database* (or the connection's default).

        Returns:
            A list of :class:`TableInfo`. Each :class:`TableInfo`
            carries the table's columns, primary key, secondary
            indexes, and an estimated row count when the engine
            surfaces one cheaply. The list may be empty when the
            database has no user tables; the method does not raise
            for that case.

        Raises:
            ConnectorError: When the introspection query fails
                (auth, network, SQL error). The :class:`ConnectorError`
                carries the driver-side error message.
            NotImplementedError: When the connector is not SQL
                (Kafka) — the brief scopes Kafka to
                :meth:`list_topics` + :meth:`get_topic_schema` only.
        """
        ...

    async def preview(
        self, table: str, limit: int = 100
    ) -> list[dict[str, Any]]:
        """Fetch up to *limit* rows from *table* as a list of dicts.

        Each dict is a ``column → value`` mapping. Type coercion is
        left to the driver (dates, decimals, etc.); the caller
        (the agent-gateway) is responsible for serialising to
        JSON-safe types.

        Returns:
            A list of row dicts. The list may be empty when the
            table is empty or *limit* is ``0``.

        Raises:
            ConnectorError: When the query fails (auth, network,
                SQL error).
            NotImplementedError: When the connector is not SQL
                (Kafka).
        """
        ...

    async def list_topics(self) -> list[TopicInfo]:
        """List the topics on the Kafka cluster.

        Only the Kafka connector implements this. The other six
        SQL connectors raise :class:`NotImplementedError`.

        Returns:
            A list of :class:`TopicInfo` (one per topic). The
            list is sorted by topic name so the result is
            deterministic across calls.

        Raises:
            ConnectorError: When the cluster metadata fetch
                fails (auth, network, broker outage).
        """
        ...

    async def get_topic_schema(self, topic: str) -> TopicSchema:
        """Return the schema of one Kafka topic.

        Only the Kafka connector implements this. The other six
        SQL connectors raise :class:`NotImplementedError`.

        Args:
            topic: The topic name.

        Returns:
            A :class:`TopicSchema` carrying the topic's wire
            format (``"avro"`` / ``"protobuf"`` /
            ``"json_schema"`` / ``"json"`` / ``"bytes"`` /
            ``"unknown"``) and the field list (in declaration
            order for the Schema-Registry formats; inferred
            from a sample for ``"json"``; empty for
            ``"bytes"`` / ``"unknown"``).

        Raises:
            ConnectorError: When the lookup fails (auth,
                network, broker outage, no Schema Registry
                configured for an Avro/Protobuf topic).
        """
        ...

    async def close(self) -> None:
        """Release any held resources (connection pool, etc.). Idempotent."""
        ...


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class ConnectorError(RuntimeError):
    """A connector operation failed.

    Distinct from a "test-connection" failure (:class:`TestResult`):
    a :class:`ConnectorError` is for operations that *must* succeed
    to return useful data (schema introspection, preview). Test
    connection failures are returned as ``ok=False`` so the API
    caller can render the failure without an exception.
    """

    def __init__(self, message: str, *, kind: DatasourceKind) -> None:
        super().__init__(message)
        self.kind = kind


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def is_connector(obj: object) -> bool:
    """Return ``True`` when *obj* satisfies the :class:`Connector` Protocol.

    Cheap structural check used by tests to assert that a stub
    matches the contract. Uses :func:`hasattr` rather than
    :func:`isinstance` because the concrete classes do not
    inherit from a shared base.
    """
    return (
        hasattr(obj, "KIND")
        and hasattr(obj, "test")
        and hasattr(obj, "get_schema")
        and hasattr(obj, "preview")
        and hasattr(obj, "list_topics")
        and hasattr(obj, "get_topic_schema")
        and hasattr(obj, "close")
    )


def build_connector(
    *,
    kind: DatasourceKind,
    connection: ConnectionConfig,
    credentials: CredentialsPayload,
) -> Connector:
    """Build a fresh :class:`Connector` for *kind*.

    The seven driver modules are imported lazily so an operator
    who registers only Postgres datasources never pays the
    import cost of ``oracledb`` / ``pyhive`` / ``pymongo`` /
    ``pymysql`` / ``aiokafka``.

    Args:
        kind: One of ``"postgresql"`` / ``"mysql"`` / ``"oracle"`` /
            ``"hive"`` / ``"mongodb"`` / ``"doris"`` / ``"kafka"``.
        connection: The non-secret connection descriptor.
        credentials: The plaintext credentials (typically freshly
            decrypted by :class:`CredentialService`).

    Returns:
        A :class:`Connector` ready for ``test()`` /
        ``get_schema()`` / ``preview()`` (SQL kinds) or
        ``test()`` / ``list_topics()`` / ``get_topic_schema()``
        (Kafka).

    Raises:
        ValueError: When *kind* is not one of the seven supported
            values.
    """
    if kind == "postgresql":
        from aidp_datasource.connectors.postgresql import PostgresConnector

        return PostgresConnector(connection=connection, credentials=credentials)
    if kind == "mysql":
        from aidp_datasource.connectors.mysql import MySQLConnector

        return MySQLConnector(connection=connection, credentials=credentials)
    if kind == "oracle":
        from aidp_datasource.connectors.oracle import OracleConnector

        return OracleConnector(connection=connection, credentials=credentials)
    if kind == "hive":
        from aidp_datasource.connectors.hive import HiveConnector

        return HiveConnector(connection=connection, credentials=credentials)
    if kind == "mongodb":
        from aidp_datasource.connectors.mongodb import MongoDBConnector

        return MongoDBConnector(connection=connection, credentials=credentials)
    if kind == "doris":
        from aidp_datasource.connectors.doris import DorisConnector

        return DorisConnector(connection=connection, credentials=credentials)
    if kind == "kafka":
        from aidp_datasource.connectors.kafka import KafkaConnector

        return KafkaConnector(connection=connection, credentials=credentials)
    raise ValueError(f"unsupported datasource kind: {kind!r}")


# ---------------------------------------------------------------------------
# Internal helper
# ---------------------------------------------------------------------------


async def _time_millis(coro: Any) -> tuple[Any, float | None, BaseException | None]:
    """Await *coro* and return ``(result, latency_ms, exc_or_None)``.

    The helper does **not** raise. The 3-tuple returns the
    exception object (if any) so the caller can render the
    error string. ``latency_ms`` is ``None`` when *coro* raised
    before completing.

    Returns:
        A 3-tuple ``(result, latency_ms, exception)``. On
        success the exception is ``None`` and the result is
        whatever *coro* returned.
    """
    started = time.perf_counter()
    try:
        value = await coro
    except BaseException as exc:
        return None, None, exc
    elapsed_ms = (time.perf_counter() - started) * 1000.0
    return value, elapsed_ms, None


# ---------------------------------------------------------------------------
# Base class (helpful, not required)
# ---------------------------------------------------------------------------


class BaseConnector(abc.ABC):
    """Convenience base class for the four concrete connectors.

    The :class:`Connector` Protocol is structural, so concrete
    connectors do not need to inherit from this base. We provide
    it anyway because every concrete connector repeats the same
    scaffolding (a ``_closed`` flag, a ``close()`` no-op when
    nothing is open, a ``_run_test`` helper that wraps a probe
    coroutine in a :class:`TestResult`).

    The base class is **not** a :class:`Connector` itself — it
    only provides helpers. Concrete subclasses must provide the
    four Protocol methods (``test`` / ``get_schema`` / ``preview``
    / ``close``).
    """

    #: Subclasses set this in their ``__init__``.
    KIND: ClassVar[DatasourceKind]

    def __init__(
        self,
        *,
        connection: ConnectionConfig,
        credentials: CredentialsPayload,
    ) -> None:
        self._connection = connection
        self._credentials = credentials
        self._closed = False

    @abc.abstractmethod
    async def _probe(self, *, timeout_seconds: float | None) -> None:
        """Open a connection + run a ``SELECT 1`` (or equivalent)."""
        ...

    async def test(self, *, timeout_seconds: float | None = None) -> TestResult:
        """Default :meth:`Connector.test` implementation.

        Wraps :meth:`_probe` in a :class:`TestResult` (latency +
        error string). Subclasses get this implementation for free.
        """
        if self._closed:
            return TestResult(ok=False, error="connector is closed")
        _value, latency_ms, exc = await _time_millis(
            self._probe(timeout_seconds=timeout_seconds)
        )
        if exc is not None:
            return TestResult(ok=False, error=_truncate(str(exc)))
        return TestResult(ok=True, latency_ms=latency_ms, error=None)

    async def close(self) -> None:
        """Default no-op :meth:`Connector.close` (subclasses override)."""
        self._closed = True

    # ------------------------------------------------------------------
    # Kafka-only methods — default to NotImplementedError for SQL
    # connectors. The Kafka connector overrides both; the six SQL
    # connectors inherit the default and the call fails loudly
    # with a clear error message rather than silently returning an
    # empty list.
    # ------------------------------------------------------------------

    async def list_topics(self) -> list[TopicInfo]:
        """Default :meth:`Connector.list_topics` implementation.

        Raises :class:`NotImplementedError` for SQL connectors
        (PG / MySQL / Oracle / Hive / MongoDB / Doris). The Kafka
        connector overrides this.
        """
        raise NotImplementedError(
            f"list_topics is not supported by the {self.KIND!r} connector "
            "(only the 'kafka' connector implements topic listing)"
        )

    async def get_topic_schema(self, topic: str) -> TopicSchema:
        """Default :meth:`Connector.get_topic_schema` implementation.

        Raises :class:`NotImplementedError` for SQL connectors.
        The Kafka connector overrides this.
        """
        raise NotImplementedError(
            f"get_topic_schema is not supported by the {self.KIND!r} "
            "connector (only the 'kafka' connector implements topic "
            "schema lookup)"
        )


def _truncate(value: str, *, limit: int = 1024) -> str:
    """Cap a string at *limit* characters with a trailing ellipsis marker.

    Used for the ``error`` field of :class:`TestResult` and the
    :class:`ConnectionTest.error` column so a giant driver-side
    traceback does not blow up the row.
    """
    if len(value) <= limit:
        return value
    return value[: limit - 3] + "..."


__all__ = [
    "BaseConnector",
    "ColumnInfo",
    "Connector",
    "ConnectorError",
    "IndexInfo",
    "TableInfo",
    "TestResult",
    "TopicFieldInfo",
    "TopicInfo",
    "TopicSchema",
    "build_connector",
    "is_connector",
]
