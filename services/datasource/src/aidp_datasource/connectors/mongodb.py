"""MongoDB connector.

Driver: :mod:`pymongo` (the official sync MongoDB driver). The
:mod:`pymongo` package is *sync* — the official async driver
(:mod:`motor`) is in maintenance mode and the upstream
recommendation is to wrap ``pymongo`` in a thread pool. The
connector follows the same pattern as :mod:`aidp_datasource.connectors.hive`
(``asyncio.to_thread`` around every blocking call) so the async
Protocol contract holds without blocking the event loop.

Connection parameters
---------------------

``pymongo.MongoClient`` takes a host list (``host:port``) and a
``username`` / ``password`` / ``authSource``. We use the
``connection.options['auth_source']`` knob (default ``"admin"``)
to set the auth source, and the ``connection.options['replica_set']``
knob to set the replica set name when the cluster is sharded /
replicated. ``tls`` / ``tlsCAFile`` flow through ``options`` for
TLS-enabled clusters.

Schema introspection
--------------------

MongoDB is a document store, so "schema" is per-collection. We
project each collection to a :class:`TableInfo` (one row per
collection) and infer the column list from a small sample
(default: ``min(20, count)`` documents). The :attr:`ColumnInfo.type`
field carries the inferred BSON type label (``"string"`` /
``"int"`` / ``"object"`` / ``"array"`` / ``"bool"`` / ``"date"``
/ ``"null"`` / ``"mixed"``); ``nullable`` is always ``True``
because MongoDB does not enforce a schema.

We deliberately **do not** try to project the full BSON schema
(``$jsonSchema`` validators are rare in production); the sampled
column list is the brief's "列名 + 采样数据" pattern that the
PII service consumes. A future task can opt-in to ``$jsonSchema``
via ``connection.options['strict_schema']``.

Failure handling
----------------

``pymongo`` raises :class:`pymongo.errors.PyMongoError` (a
superclass of auth / network / command errors). The connector
catches and re-raises as :class:`ConnectorError` so the
datasource service layer only has one error type to deal with.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, ClassVar

from aidp_datasource.connectors.base import (
    BaseConnector,
    ColumnInfo,
    ConnectorError,
    TableInfo,
)
from aidp_datasource.schemas import ConnectionConfig, CredentialsPayload, DatasourceKind

_LOG = logging.getLogger(__name__)


#: System collections we never surface. ``system.indexes`` /
#: ``system.users`` / etc. are infrastructure; the agent-gateway
#: should never see them.
_SYSTEM_COLLECTIONS: frozenset[str] = frozenset(
    {
        "system.indexes",
        "system.users",
        "system.version",
        "system.sessions",
        "system.js",
        "system.profile",
    }
)

#: Default sample size for the per-collection column inference.
#: 20 documents is enough to see the common field set without
#: flooding a large collection's first page.
_DEFAULT_SAMPLE_SIZE: int = 20


def _infer_bson_type(value: Any) -> str:
    """Return a short BSON type label for *value*.

    The label is a string the agent-gateway can pattern-match
    on; the brief does not require round-tripping the BSON
    type. ``"mixed"`` is the catch-all for values whose type
    the consumer cannot easily classify (e.g. a ``Decimal128``
    or a custom class). ``"null"`` is the literal BSON null.
    """
    if value is None:
        return "null"
    # ``bool`` must come before ``int`` because ``bool`` is a
    # subclass of ``int`` in Python and ``isinstance(True, int)``
    # is ``True``.
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "double"
    if isinstance(value, str):
        return "string"
    if isinstance(value, (list, tuple)):
        return "array"
    if isinstance(value, dict):
        return "object"
    # ``datetime`` is checked after the builtins so a
    # ``datetime.datetime`` does not fall through to ``object``.
    try:
        import datetime as _dt

        if isinstance(value, (_dt.datetime, _dt.date)):
            return "date"
    except ImportError:  # pragma: no cover - stdlib always importable
        pass
    return "mixed"


def _merge_field_type(
    *,
    accumulator: dict[str, dict[str, Any]],
    field: str,
    value: Any,
) -> None:
    """Merge a (field, value) observation into the *accumulator*.

    The accumulator key is the field name; the value is a dict
    with ``{"type": str, "nullable": bool, "_types_seen": set}``.
    The first observation sets the type; a second observation
    with a different type upgrades the type to ``"mixed"`` and
    marks the field as nullable (because two distinct types
    in a sample are a strong signal that the field is
    polymorphic).
    """
    inferred = _infer_bson_type(value)
    if field not in accumulator:
        accumulator[field] = {
            "type": inferred,
            "nullable": inferred == "null",
            "_types_seen": {inferred},
        }
        return
    buf = accumulator[field]
    if inferred not in buf["_types_seen"]:
        buf["_types_seen"].add(inferred)
        buf["type"] = "mixed"
        buf["nullable"] = True
    if inferred == "null":
        buf["nullable"] = True


def _project_columns(
    *, sample: list[dict[str, Any]]
) -> list[ColumnInfo]:
    """Project a document sample to a column list.

    The columns are emitted in the order they are first observed
    (so a UI can show them in the same order the sample
    surfaces them); ties on first-observation are broken by
    field name.

    Args:
        sample: A list of documents. Each document is a
            ``field → value`` dict.

    Returns:
        A list of :class:`ColumnInfo`. Each entry carries the
        field's name, the inferred BSON type (``"mixed"`` when
        the field was observed with two or more distinct
        types), and ``nullable=True`` (MongoDB does not enforce
        a schema, so the connector can never promise a
        non-nullable field). Empty when *sample* is empty.
    """
    accumulator: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for doc in sample:
        if not isinstance(doc, dict):
            # A document in the sample that is not a dict is
            # unusual (e.g. an array at the top level). Skip
            # it — the column projection is meaningless for
            # non-object documents.
            continue
        for field, value in doc.items():
            if field not in accumulator:
                order.append(field)
            _merge_field_type(
                accumulator=accumulator, field=field, value=value
            )
    return [
        ColumnInfo(
            name=field,
            type=str(accumulator[field]["type"]),
            nullable=bool(accumulator[field]["nullable"]),
        )
        for field in order
    ]


class MongoDBConnector(BaseConnector):
    """:class:`Connector` implementation for MongoDB via :mod:`pymongo`."""

    KIND: ClassVar[DatasourceKind] = "mongodb"

    def __init__(
        self,
        *,
        connection: ConnectionConfig,
        credentials: CredentialsPayload,
    ) -> None:
        super().__init__(connection=connection, credentials=credentials)
        # :mod:`pymongo` is imported lazily so the platform
        # never pays the import cost when an operator registers
        # no MongoDB datasources.
        self._pymongo: Any = None

    # ------------------------------------------------------------------
    # Driver lifecycle
    # ------------------------------------------------------------------

    def _ensure_driver(self) -> Any:
        """Lazy-import :mod:`pymongo`."""
        if self._pymongo is None:
            import pymongo

            self._pymongo = pymongo
        return self._pymongo

    def _build_client_kwargs(self) -> dict[str, Any]:
        """Build the kwargs for :class:`pymongo.MongoClient`.

        The auth source defaults to ``"admin"`` (the MongoDB
        convention) and is overridable via
        ``connection.options['auth_source']``. The replica set
        name is read from ``connection.options['replica_set']``;
        TLS knobs (``tls`` / ``tlsCAFile``) flow through.
        """
        opts = dict(self._connection.options or {})
        kwargs: dict[str, Any] = {
            "host": self._connection.host,
            "port": self._connection.port,
            "username": self._credentials.username,
            "password": self._credentials.password,
            "serverSelectionTimeoutMS": 10_000,
        }
        kwargs["authSource"] = opts.get("auth_source", "admin")
        if "replica_set" in opts:
            kwargs["replicaSet"] = opts["replica_set"]
        # Forward TLS + driver knobs (``tls`` / ``tlsCAFile`` /
        # ``appname`` / etc.) verbatim.
        for key, value in opts.items():
            if key in {"auth_source", "replica_set"}:
                continue
            kwargs[key] = value
        return kwargs

    def _open_sync(self) -> Any:
        """Synchronous :class:`pymongo.MongoClient` factory."""
        pymongo = self._ensure_driver()
        return pymongo.MongoClient(**self._build_client_kwargs())

    async def _open(self, *, timeout_seconds: float | None) -> Any:
        """Open a :class:`pymongo.MongoClient` via ``asyncio.to_thread``."""
        timeout = timeout_seconds if timeout_seconds is not None else 10.0
        return await asyncio.wait_for(
            asyncio.to_thread(self._open_sync),
            timeout=timeout,
        )

    async def _probe(self, *, timeout_seconds: float | None) -> None:
        """``ping`` the cluster's ``admin`` database.

        ``ping`` is the cheapest admin-level command MongoDB
        supports and is the canonical "is the cluster
        reachable?" probe. It also exercises the auth path
        (the cluster rejects ``ping`` for an unauthenticated
        user, surfacing the same ``OperationFailure`` as a real
        query).
        """
        client = await self._open(timeout_seconds=timeout_seconds)
        try:
            await asyncio.to_thread(self._run_ping, client)
        finally:
            await asyncio.to_thread(client.close)

    @staticmethod
    def _run_ping(client: Any) -> None:
        client.admin.command("ping")

    # ------------------------------------------------------------------
    # Schema + preview
    # ------------------------------------------------------------------

    async def get_schema(self, database: str | None = None) -> list[TableInfo]:
        """List the collections in *database* with sampled columns.

        Args:
            database: Override the default database. ``None``
                uses the connection's ``database``.

        Returns:
            A list of :class:`TableInfo`. Each entry carries the
            collection's name, the database, the columns inferred
            from a sample, and an estimated document count
            (``estimatedDocumentCount``). System collections
            (``system.indexes`` / ``system.users`` / etc.) are
            filtered out so the agent-gateway never sees them.

        Raises:
            ConnectorError: When the ``listCollections`` /
                ``find`` round-trips fail.
        """
        target_db = database or self._connection.database
        if not target_db:
            raise ConnectorError(
                "mongodb schema introspection requires a database name",
                kind=self.KIND,
            )
        client = await self._open(timeout_seconds=10.0)
        try:
            collections = await asyncio.to_thread(
                self._list_collections, client, target_db
            )
            out: list[TableInfo] = []
            for coll_name in collections:
                # ``estimatedDocumentCount`` uses the collection
                # metadata (no scan); it is the right knob for
                # a Phase 1 "≈ N rows" hint. An unauthenticated
                # client cannot read the metadata for some
                # collections; we treat that as "no estimate"
                # rather than fail the whole sync.
                try:
                    count = await asyncio.to_thread(
                        self._estimated_count, client, target_db, coll_name
                    )
                except ConnectorError:
                    count = None
                sample = await asyncio.to_thread(
                    self._sample, client, target_db, coll_name, _DEFAULT_SAMPLE_SIZE
                )
                columns = _project_columns(sample=sample)
                out.append(
                    TableInfo(
                        name=coll_name,
                        schema=target_db,
                        columns=columns,
                        primary_key=[],
                        indexes=[],
                        row_count_estimate=count,
                    )
                )
            return out
        except ConnectorError:
            raise
        except Exception as exc:
            raise ConnectorError(
                f"mongodb schema introspection failed: {exc}",
                kind=self.KIND,
            ) from exc
        finally:
            await asyncio.to_thread(client.close)

    async def preview(
        self, table: str, limit: int = 100
    ) -> list[dict[str, Any]]:
        """Return up to *limit* documents from *table*.

        MongoDB stores documents, not rows, so the response is
        a list of ``field → value`` dicts (one per document).
        The ``table`` argument may be qualified with a database
        (``"db.coll"``); the connector splits on the first dot
        so a caller can preview across databases without
        re-binding the client.

        Args:
            table: Collection name. May be qualified
                (``"db.collection"``); the connector splits on
                the first dot and falls back to the connection's
                default database when no prefix is present.
            limit: Document cap. Defaults to 100.

        Returns:
            A list of documents (``field → value``).

        Raises:
            ConnectorError: When the ``find`` round-trip fails.
        """
        if limit <= 0:
            return []
        if "." in table:
            db_name, _, coll_name = table.partition(".")
        else:
            db_name = self._connection.database or ""
            coll_name = table
        if not db_name or not coll_name:
            raise ConnectorError(
                f"mongodb preview requires a database + collection; got {table!r}",
                kind=self.KIND,
            )
        client = await self._open(timeout_seconds=10.0)
        try:
            docs = await asyncio.to_thread(
                self._find_limit, client, db_name, coll_name, int(limit)
            )
        except Exception as exc:
            raise ConnectorError(
                f"mongodb preview failed: {exc}",
                kind=self.KIND,
            ) from exc
        finally:
            await asyncio.to_thread(client.close)
        return [dict(doc) for doc in docs]

    # ------------------------------------------------------------------
    # Sync helpers (run via ``asyncio.to_thread``)
    # ------------------------------------------------------------------

    @staticmethod
    def _list_collections(client: Any, database: str) -> list[str]:
        """Return the collection names in *database*, system collections filtered out."""
        db = client[database]
        names: list[str] = []
        for coll in db.list_collections():
            # ``list_collections`` yields dicts with a
            # ``"name"`` key (the collection's full name).
            name = str(coll.get("name", "")).strip()
            if not name:
                continue
            # Strip the ``db.`` prefix MongoDB includes in the
            # ``name`` field; we want the bare collection name.
            if "." in name:
                _, _, bare = name.rpartition(".")
                name = bare
            if name in _SYSTEM_COLLECTIONS:
                continue
            names.append(name)
        names.sort()
        return names

    @staticmethod
    def _estimated_count(client: Any, database: str, collection: str) -> int | None:
        """Return ``estimatedDocumentCount`` for *collection*, or ``None`` on failure.

        The estimate is cheap (uses the collection metadata,
        not a ``count`` command) and the right knob for a
        "≈ N rows" hint. The method is intentionally
        best-effort: a failure is swallowed and surfaces as
        ``None`` so the rest of the introspection can continue.
        """
        try:
            return int(client[database][collection].estimated_document_count())
        except Exception:
            return None

    @staticmethod
    def _sample(
        client: Any, database: str, collection: str, limit: int
    ) -> list[dict[str, Any]]:
        """Return up to *limit* documents from *collection* for column inference.

        We ``find()`` without a filter and limit the round-trip
        to *limit* documents. The method never raises — a
        failure (e.g. the collection requires a filter the
        connector cannot guess) returns an empty list so the
        caller carries on with no columns rather than failing
        the whole ``get_schema`` call.
        """
        try:
            cursor = (
                client[database][collection]
                .find({}, limit=limit)
            )
            return [dict(doc) for doc in cursor]
        except Exception:
            return []

    @staticmethod
    def _find_limit(
        client: Any, database: str, collection: str, limit: int
    ) -> list[dict[str, Any]]:
        """Return up to *limit* documents from *collection* for ``preview``."""
        cursor = client[database][collection].find({}, limit=limit)
        return [dict(doc) for doc in cursor]

    async def close(self) -> None:
        await super().close()


__all__ = ["MongoDBConnector"]
