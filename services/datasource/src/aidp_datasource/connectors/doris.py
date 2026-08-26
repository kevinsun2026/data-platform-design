"""Apache Doris connector.

Driver: :mod:`pymysql` (a pure-Python MySQL client that
implements the MySQL protocol). Doris exposes a MySQL-compatible
wire format on the FE (frontend) so the connector reuses
:mod:`pymysql` rather than pulling in a separate driver. The
driver is *sync* — the connector wraps every blocking call in
``asyncio.to_thread`` (same pattern as the Hive / MongoDB
connectors) so the async Protocol contract holds without
blocking the event loop.

Connection parameters
---------------------

Doris's MySQL endpoint takes ``host`` / ``port`` (default
``9030``) / ``user`` / ``password`` / ``database``. The connector
folds ``connection.options`` into the :class:`pymysql.Connection`
call so driver-specific knobs (``connect_timeout`` / ``charset``
/ ``ssl``) flow through naturally. The query port (default
``9030``) is the FE's MySQL protocol port; the BE HTTP port
(``8030`` / ``8040``) is irrelevant for our introspection use
case (Doris returns the schema from the FE via SQL).

Schema introspection
--------------------

:meth:`get_schema` queries ``information_schema.tables`` filtered
to ``TABLE_TYPE = 'BASE TABLE'`` and ``TABLE_SCHEMA = <db>``.
We skip the ``information_schema`` / ``mysql`` system catalogs
so the agent-gateway never has to filter them out itself. The
column / PK / index / row-count lookups follow the MySQL
pattern (see :mod:`aidp_datasource.connectors.mysql` for the
SQL details) because Doris reuses the MySQL ``information_schema``
schema.

Doris-specific differences from MySQL:

- ``TABLE_TYPE`` may also include ``"VIEW"`` and ``"MATERIALIZED
  VIEW"``; we filter to ``"BASE TABLE"`` to keep the
  agent-gateway's mental model the same as the MySQL connector.
- Doris does not support foreign keys; the
  ``information_schema.key_column_usage`` join still works
  for the primary-key lookup.
- ``information_schema.statistics`` works the same way as MySQL;
  the column / index projection is identical.

Failure handling
----------------

``pymysql`` raises :class:`pymysql.MySQLError` (a superclass of
auth / network / SQL errors). The connector catches and
re-raises as :class:`ConnectorError` so the datasource service
layer only has one error type to deal with.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, ClassVar

from aidp_datasource.connectors.base import (
    BaseConnector,
    ColumnInfo,
    ConnectorError,
    IndexInfo,
    TableInfo,
)
from aidp_datasource.schemas import ConnectionConfig, CredentialsPayload, DatasourceKind

_LOG = logging.getLogger(__name__)


#: System schemas we never surface. ``information_schema`` /
#: ``mysql`` are the MySQL standard catalogs (Doris reuses
#: them); ``__internal_schema__`` is Doris's internal catalog.
_SYSTEM_SCHEMAS: frozenset[str] = frozenset(
    {"information_schema", "mysql", "__internal_schema__"}
)


class DorisConnector(BaseConnector):
    """:class:`Connector` implementation for Apache Doris via :mod:`pymysql`."""

    KIND: ClassVar[DatasourceKind] = "doris"

    def __init__(
        self,
        *,
        connection: ConnectionConfig,
        credentials: CredentialsPayload,
    ) -> None:
        super().__init__(connection=connection, credentials=credentials)
        # :mod:`pymysql` is imported lazily so the platform
        # never pays the import cost when an operator registers
        # no Doris datasources.
        self._pymysql: Any = None

    # ------------------------------------------------------------------
    # Driver lifecycle
    # ------------------------------------------------------------------

    def _ensure_driver(self) -> Any:
        if self._pymysql is None:
            import pymysql  # type: ignore[import-untyped]

            self._pymysql = pymysql
        return self._pymysql

    def _build_connect_kwargs(self) -> dict[str, Any]:
        """Build the kwargs for :class:`pymysql.Connection`.

        The driver is sync; we wrap the call in
        ``asyncio.to_thread`` (see :meth:`_open`). Driver-specific
        knobs (``connect_timeout`` / ``charset`` / ``ssl``) flow
        through ``connection.options``.
        """
        opts = dict(self._connection.options or {})
        kwargs: dict[str, Any] = {
            "host": self._connection.host,
            "port": self._connection.port,
            "user": self._credentials.username,
            "password": self._credentials.password,
            "database": self._connection.database or "",
            "autocommit": True,
            # The cursor must return rows as dicts (so
            # ``cursor.fetchall()`` + ``zip(columns, row)`` keeps
            # the wire shape consistent with the MySQL connector).
            "cursorclass": None,  # set per-call
        }
        for key, value in opts.items():
            kwargs[key] = value
        return kwargs

    def _open_sync(self) -> Any:
        """Synchronous :class:`pymysql.Connection` factory."""
        pymysql = self._ensure_driver()
        kwargs = self._build_connect_kwargs()
        # We use the default cursor (tuple) and project to
        # ``dict`` in :meth:`preview` (matches the MySQL
        # connector's behaviour). The ``cursorclass`` knob is
        # left to ``connection.options`` for callers that need
        # ``DictCursor``; the default is fine for the
        # ``information_schema`` queries.
        kwargs.pop("cursorclass", None)
        return pymysql.connect(**kwargs)

    async def _open(self, *, timeout_seconds: float | None) -> Any:
        """Open a :class:`pymysql.Connection` via ``asyncio.to_thread``."""
        timeout = timeout_seconds if timeout_seconds is not None else 10.0
        return await asyncio.wait_for(
            asyncio.to_thread(self._open_sync),
            timeout=timeout,
        )

    async def _probe(self, *, timeout_seconds: float | None) -> None:
        """Open a connection, run ``SELECT 1``, close."""
        conn = await self._open(timeout_seconds=timeout_seconds)
        try:
            await asyncio.to_thread(self._run_select1, conn)
        finally:
            await asyncio.to_thread(conn.close)

    @staticmethod
    def _run_select1(conn: Any) -> None:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT 1")
            cursor.fetchone()
        finally:
            cursor.close()

    # ------------------------------------------------------------------
    # Schema + preview
    # ------------------------------------------------------------------

    async def get_schema(self, database: str | None = None) -> list[TableInfo]:
        """Return the base tables in *database*.

        Args:
            database: Override the default database. ``None``
                uses the connection's ``database``.

        Returns:
            A list of :class:`TableInfo`. Each entry carries the
            table's columns, primary key, secondary indexes, and
            an estimated row count (``information_schema.tables.
            table_rows``). System schemas (``information_schema``
            / ``mysql`` / ``__internal_schema__``) are filtered
            out so the agent-gateway never sees them.

        Raises:
            ConnectorError: When any of the introspection
                queries fail.
        """
        target_db = database or self._connection.database
        if not target_db:
            raise ConnectorError(
                "doris schema introspection requires a database name",
                kind=self.KIND,
            )
        conn = await self._open(timeout_seconds=10.0)
        try:
            table_rows, column_rows, pk_rows, index_rows, rowcount_rows = (
                await asyncio.to_thread(
                    self._collect_introspection, conn, target_db
                )
            )
        except ConnectorError:
            raise
        except Exception as exc:
            raise ConnectorError(
                f"doris schema introspection failed: {exc}",
                kind=self.KIND,
            ) from exc
        finally:
            await asyncio.to_thread(conn.close)

        # Project the result into ``TableInfo`` / ``ColumnInfo``.
        # The projection mirrors the MySQL connector so the two
        # drivers share the same downstream contract.
        tables_by_key: dict[str, list[ColumnInfo]] = {}
        for row in column_rows:
            table_name, column_name, data_type, is_nullable = row
            tables_by_key.setdefault(table_name, []).append(
                ColumnInfo(
                    name=column_name,
                    type=data_type,
                    nullable=(str(is_nullable).upper() == "YES"),
                )
            )
        pk_by_key: dict[str, list[str]] = {}
        for table_name, column_name, _ordinal in pk_rows:
            pk_by_key.setdefault(table_name, []).append(column_name)
        # Index projection: group columns by (table, index) so a
        # composite index becomes a single ``IndexInfo`` with
        # multiple columns in ``SEQ_IN_INDEX`` order. PK indexes
        # are filtered out by the ``INDEX_NAME = 'PRIMARY'``
        # exclusion (mirrors the MySQL connector).
        indexes_by_key: dict[str, list[IndexInfo]] = {}
        index_bufs: dict[tuple[str, str], dict[str, Any]] = {}
        for table_name, index_name, column_name, _seq, non_unique in index_rows:
            buf_key = (table_name, index_name)
            buf = index_bufs.setdefault(
                buf_key,
                {"columns": [], "unique": non_unique == 0},
            )
            buf["columns"].append(column_name)
        for (table_name, index_name), buf in index_bufs.items():
            indexes_by_key.setdefault(table_name, []).append(
                IndexInfo(
                    name=index_name,
                    columns=list(buf["columns"]),
                    unique=bool(buf["unique"]),
                )
            )
        # Row-count projection: ``TABLE_ROWS`` is a ``BIGINT`` but
        # Doris may return ``None`` for an empty / never-analyzed
        # table. Surface that as ``None`` for the consumer.
        rowcount_by_key: dict[str, int | None] = {}
        for table_name, table_rows_count in rowcount_rows:
            if table_rows_count is None:
                rowcount_by_key[table_name] = None
            else:
                rowcount_by_key[table_name] = int(table_rows_count)
        return [
            TableInfo(
                name=name,
                schema=target_db,
                columns=tables_by_key.get(name, []),
                primary_key=list(pk_by_key.get(name, [])),
                indexes=list(indexes_by_key.get(name, [])),
                row_count_estimate=rowcount_by_key.get(name),
            )
            for (name,) in table_rows
        ]

    async def preview(
        self, table: str, limit: int = 100
    ) -> list[dict[str, Any]]:
        """Return up to *limit* rows from *table*.

        MySQL identifiers are quoted with backticks; we do *not*
        allow a user-supplied name to inject extra statements
        because ``table`` is escaped via :meth:`cursor.execute`
        and the limit is bound separately. Schema-qualified
        names are accepted (``"db.table"``) but we do not split
        them — Doris is already bound to the connection's
        ``database``.

        Args:
            table: Table name. May be qualified (``"db.table"``);
                the connector strips the ``db.`` prefix.
            limit: Row cap. Defaults to 100.

        Returns:
            A list of row dicts (``column → value``).

        Raises:
            ConnectorError: When the ``SELECT`` fails.
        """
        if limit <= 0:
            return []
        if "." in table:
            _, _, bare = table.partition(".")
        else:
            bare = table
        conn = await self._open(timeout_seconds=10.0)
        try:
            rows, columns = await asyncio.to_thread(
                self._select_limit, conn, bare, int(limit)
            )
        except Exception as exc:
            raise ConnectorError(
                f"doris preview failed: {exc}",
                kind=self.KIND,
            ) from exc
        finally:
            await asyncio.to_thread(conn.close)
        return [dict(zip(columns, row, strict=True)) for row in rows]

    # ------------------------------------------------------------------
    # Sync helpers (run via ``asyncio.to_thread``)
    # ------------------------------------------------------------------

    @staticmethod
    def _collect_introspection(
        conn: Any, target_db: str
    ) -> tuple[
        list[tuple[str]],
        list[tuple[str, str, str, str]],
        list[tuple[str, str, int]],
        list[tuple[str, str, str, int, int]],
        list[tuple[str, int | None]],
    ]:
        """Run the four ``information_schema`` queries in one connection.

        Returns:
            A 5-tuple of ``(table_rows, column_rows, pk_rows,
            index_rows, rowcount_rows)`` ready for projection.
            Each row is a tuple of positional columns (the same
            shape the MySQL connector returns).
        """
        # The system-schema filter rejects ``information_schema``
        # / ``mysql`` / Doris's internal ``__internal_schema__``
        # catalog so the agent-gateway never has to filter them
        # out itself.
        schema_filter = (
            f"AND TABLE_SCHEMA NOT IN ({','.join(['%s'] * len(_SYSTEM_SCHEMAS))})"
        )
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT TABLE_NAME FROM information_schema.tables "
                "WHERE TABLE_SCHEMA = %s "
                "AND TABLE_TYPE = 'BASE TABLE' "
                f"{schema_filter} "
                "ORDER BY TABLE_NAME",
                (target_db, *_SYSTEM_SCHEMAS),
            )
            table_rows = list(cursor.fetchall())
            if not table_rows:
                return [], [], [], [], []
            table_names = [row[0] for row in table_rows]
            placeholders = ",".join(["%s"] * len(table_names))
            # Column lookup.
            cursor.execute(
                f"SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE "
                f"FROM information_schema.columns "
                f"WHERE TABLE_SCHEMA = %s "
                f"{schema_filter} "
                f"AND TABLE_NAME IN ({placeholders}) "
                f"ORDER BY TABLE_NAME, ORDINAL_POSITION",
                (target_db, *_SYSTEM_SCHEMAS, *table_names),
            )
            column_rows = list(cursor.fetchall())
            # Primary-key lookup. ``REFERENCED_TABLE_NAME`` is
            # ``NULL`` for primary-key rows so it is a cheap
            # filter (mirrors the MySQL connector).
            cursor.execute(
                f"SELECT kcu.TABLE_NAME, kcu.COLUMN_NAME, kcu.ORDINAL_POSITION "
                f"FROM information_schema.key_column_usage kcu "
                f"JOIN information_schema.table_constraints tc "
                f"  ON kcu.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA "
                f"  AND kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME "
                f"WHERE kcu.TABLE_SCHEMA = %s "
                f"{schema_filter} "
                f"  AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY' "
                f"  AND kcu.TABLE_NAME IN ({placeholders}) "
                f"ORDER BY kcu.TABLE_NAME, kcu.ORDINAL_POSITION",
                (target_db, *_SYSTEM_SCHEMAS, *table_names),
            )
            pk_rows = list(cursor.fetchall())
            # Secondary-index lookup. ``INDEX_NAME = 'PRIMARY'``
            # excludes the implicit PK index (mirrors MySQL).
            cursor.execute(
                f"SELECT TABLE_NAME, INDEX_NAME, COLUMN_NAME, "
                f"       SEQ_IN_INDEX, NON_UNIQUE "
                f"FROM information_schema.statistics "
                f"WHERE TABLE_SCHEMA = %s "
                f"{schema_filter} "
                f"  AND INDEX_NAME != 'PRIMARY' "
                f"  AND TABLE_NAME IN ({placeholders}) "
                f"ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX",
                (target_db, *_SYSTEM_SCHEMAS, *table_names),
            )
            index_rows = list(cursor.fetchall())
            # Row-count estimate. Doris populates ``TABLE_ROWS``
            # from the BE's per-table row count; the value is
            # ``None`` for an empty / never-analyzed table.
            cursor.execute(
                f"SELECT TABLE_NAME, TABLE_ROWS "
                f"FROM information_schema.tables "
                f"WHERE TABLE_SCHEMA = %s "
                f"AND TABLE_TYPE = 'BASE TABLE' "
                f"{schema_filter}",
                (target_db, *_SYSTEM_SCHEMAS),
            )
            rowcount_rows = list(cursor.fetchall())
        finally:
            cursor.close()
        return table_rows, column_rows, pk_rows, index_rows, rowcount_rows

    @staticmethod
    def _select_limit(
        conn: Any, table: str, limit: int
    ) -> tuple[list[tuple[Any, ...]], list[str]]:
        """Synchronous ``SELECT * LIMIT`` (called via ``asyncio.to_thread``).

        The table name is restricted to alphanumerics +
        underscore (a defensive allow-list; ``pymysql`` does
        not bind identifiers, so we cannot rely on parameter
        substitution here).
        """
        if not table.replace("_", "").isalnum():
            raise ConnectorError(
                f"doris table name is not safe: {table!r}",
                kind="doris",
            )
        cursor = conn.cursor()
        try:
            cursor.execute(f"SELECT * FROM `{table}` LIMIT {int(limit)}")
            columns = [d[0] for d in cursor.description] if cursor.description else []
            rows = list(cursor.fetchall())
            return rows, columns
        finally:
            cursor.close()

    async def close(self) -> None:
        await super().close()


__all__ = ["DorisConnector"]
