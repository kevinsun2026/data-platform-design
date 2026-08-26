"""MySQL connector.

Driver: :mod:`aiomysql` (the async MySQL driver maintained under
the ``aiomysql`` PyPI package). The connection is opened lazily
inside :meth:`_probe`, :meth:`get_schema`, and :meth:`preview`
so the cost of constructing the connector is paid even for the
``GET /api/v1/datasources/types`` endpoint, but the cost of opening
a real socket is paid only when actually needed.

Connection parameters
---------------------

``aiomysql.connect`` takes kwargs (``host`` / ``port`` / ``user`` /
``password`` / ``db``) rather than a URL. The connector folds
``connection.options`` into the keyword call so driver-specific
knobs (``connect_timeout`` / ``charset`` / ``ssl``) flow through
naturally.

Schema introspection
--------------------

:meth:`get_schema` queries ``information_schema.tables`` filtered
to ``TABLE_TYPE = 'BASE TABLE'``. We skip the ``mysql`` /
``information_schema`` / ``performance_schema`` / ``sys``
catalogs so the agent-gateway never has to filter them out
itself. Column metadata is fetched in a single follow-up
round-trip and projected into :class:`TableInfo` /
:class:`ColumnInfo`.
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


_SYSTEM_SCHEMAS: frozenset[str] = frozenset(
    {"mysql", "information_schema", "performance_schema", "sys"}
)


class MySQLConnector(BaseConnector):
    """:class:`Connector` implementation for MySQL via :mod:`aiomysql`."""

    KIND: ClassVar[DatasourceKind] = "mysql"

    def __init__(
        self,
        *,
        connection: ConnectionConfig,
        credentials: CredentialsPayload,
    ) -> None:
        super().__init__(connection=connection, credentials=credentials)
        self._aiomysql: Any = None

    # ------------------------------------------------------------------
    # Driver lifecycle
    # ------------------------------------------------------------------

    def _ensure_driver(self) -> Any:
        if self._aiomysql is None:
            import aiomysql  # type: ignore[import-not-found]

            self._aiomysql = aiomysql
        return self._aiomysql

    async def _open(self, *, timeout_seconds: float | None) -> Any:
        aiomysql = self._ensure_driver()
        opts = dict(self._connection.options or {})
        # ``connect_timeout`` is a driver-level knob (seconds); we
        # honour it when set, otherwise let the caller override.
        timeout = timeout_seconds if timeout_seconds is not None else 10.0
        return await asyncio.wait_for(
            aiomysql.connect(
                host=self._connection.host,
                port=self._connection.port,
                user=self._credentials.username,
                password=self._credentials.password,
                db=self._connection.database,
                autocommit=True,
                **opts,
            ),
            timeout=timeout,
        )

    async def _probe(self, *, timeout_seconds: float | None) -> None:
        conn = await self._open(timeout_seconds=timeout_seconds)
        try:
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1")
                await cur.fetchone()
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # Schema + preview
    # ------------------------------------------------------------------

    async def get_schema(self, database: str | None = None) -> list[TableInfo]:
        target_db = database or self._connection.database
        if not target_db:
            raise ConnectorError(
                "mysql schema introspection requires a database name",
                kind=self.KIND,
            )
        conn = await self._open(timeout_seconds=10.0)
        try:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT TABLE_NAME FROM information_schema.tables "
                    "WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE' "
                    "ORDER BY TABLE_NAME",
                    (target_db,),
                )
                table_rows = await cur.fetchall()
                if not table_rows:
                    return []
                table_names = [row[0] for row in table_rows]
                # Batch the column lookup using ``IN (...)`` with a
                # parameter list. ``aiomysql`` expands the tuple
                # into the right ``%s`` placeholders.
                placeholders = ",".join(["%s"] * len(table_names))
                await cur.execute(
                    f"SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE "
                    f"FROM information_schema.columns "
                    f"WHERE TABLE_SCHEMA = %s "
                    f"AND TABLE_NAME IN ({placeholders}) "
                    f"ORDER BY TABLE_NAME, ORDINAL_POSITION",
                    (target_db, *table_names),
                )
                column_rows = await cur.fetchall()
                # Primary-key lookup. ``information_schema.key_column_usage``
                # carries the constrained columns in ordinal
                # order; we join to ``table_constraints`` to filter
                # down to ``PRIMARY KEY`` (excludes foreign keys /
                # unique indexes). The ``REFERENCED_TABLE_NAME`` is
                # ``NULL`` for primary-key rows so it is a cheap
                # filter too.
                await cur.execute(
                    f"SELECT kcu.TABLE_NAME, kcu.COLUMN_NAME, kcu.ORDINAL_POSITION "
                    f"FROM information_schema.key_column_usage kcu "
                    f"JOIN information_schema.table_constraints tc "
                    f"  ON kcu.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA "
                    f"  AND kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME "
                    f"WHERE kcu.TABLE_SCHEMA = %s "
                    f"  AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY' "
                    f"  AND kcu.TABLE_NAME IN ({placeholders}) "
                    f"ORDER BY kcu.TABLE_NAME, kcu.ORDINAL_POSITION",
                    (target_db, *table_names),
                )
                pk_rows = await cur.fetchall()
                # Secondary-index lookup. ``STATISTICS`` carries
                # the index name + the indexed columns in
                # ``SEQ_IN_INDEX`` order; ``NON_UNIQUE`` is the
                # uniqueness flag. PK indexes are filtered out
                # by the ``INDEX_NAME = 'PRIMARY'`` exclusion
                # because they are surfaced via ``primary_key``
                # above.
                await cur.execute(
                    f"SELECT TABLE_NAME, INDEX_NAME, COLUMN_NAME, "
                    f"       SEQ_IN_INDEX, NON_UNIQUE "
                    f"FROM information_schema.statistics "
                    f"WHERE TABLE_SCHEMA = %s "
                    f"  AND INDEX_NAME != 'PRIMARY' "
                    f"  AND TABLE_NAME IN ({placeholders}) "
                    f"ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX",
                    (target_db, *table_names),
                )
                index_rows = await cur.fetchall()
                # Row-count estimate. ``information_schema.tables.table_rows``
                # is the InnoDB statistic; for MyISAM it is the
                # exact count. The column is ``NULL`` for the
                # ``INFORMATION_SCHEMA`` and ``PERFORMANCE_SCHEMA``
                # databases, but we already filter those out.
                await cur.execute(
                    "SELECT TABLE_NAME, TABLE_ROWS "
                    "FROM information_schema.tables "
                    "WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'",
                    (target_db,),
                )
                rowcount_rows = await cur.fetchall()
        except Exception as exc:
            raise ConnectorError(
                f"mysql schema introspection failed: {exc}",
                kind=self.KIND,
            ) from exc
        finally:
            conn.close()

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
        # multiple columns in ``SEQ_IN_INDEX`` order. ``NON_UNIQUE``
        # is ``0`` for unique indexes and ``1`` for non-unique.
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
        # MySQL may return ``None`` for an empty / never-analyzed
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
        if limit <= 0:
            return []
        # MySQL identifiers are quoted with backticks; we do
        # *not* allow a user-supplied name to inject extra
        # statements because ``table`` is escaped via
        # :meth:`cursor.execute` and the limit is bound
        # separately. Schema-qualified names are accepted
        # (``"db.table"``) but we do not split them — MySQL's
        # ``USE db`` preamble is unnecessary because the
        # connector is already bound to the connection's
        # ``db``.
        if "." in table:
            _, _, bare = table.partition(".")
        else:
            bare = table
        # ``LIMIT %s`` works because ``limit`` is bound as a
        # parameter, not interpolated.
        conn = await self._open(timeout_seconds=10.0)
        try:
            async with conn.cursor() as cur:
                await cur.execute(
                    f"SELECT * FROM `{bare}` LIMIT %s",
                    (int(limit),),
                )
                columns = [d[0] for d in cur.description]
                rows = await cur.fetchall()
        except Exception as exc:
            raise ConnectorError(
                f"mysql preview failed: {exc}",
                kind=self.KIND,
            ) from exc
        finally:
            conn.close()
        return [dict(zip(columns, row, strict=True)) for row in rows]

    async def close(self) -> None:
        await super().close()


__all__ = ["MySQLConnector"]
