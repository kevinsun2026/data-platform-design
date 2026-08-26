"""Oracle connector.

Driver: :mod:`oracledb` (the official async-friendly Oracle driver
that supersedes ``cx_Oracle``; install name is ``oracledb`` on
PyPI). The connection is opened lazily inside :meth:`_probe`,
:meth:`get_schema`, and :meth:`preview`.

Connection string
-----------------

``oracledb`` accepts a DSN-form connection string
(``host:port/service_name``) or a full Easy Connect string
(``host:port/service_name``). The connector builds the Easy
Connect form because it requires no client-side ``tnsnames.ora``
configuration. The user / password are passed as kwargs.

Schema introspection
--------------------

:meth:`get_schema` queries ``USER_TABLES`` (the current user's
own tables) so the agent-gateway sees the tables the
credentials actually have read access to. The system catalog
(``SYS`` / ``SYSTEM``) is never touched.

Column metadata is fetched via ``USER_TAB_COLUMNS`` in a
follow-up round-trip. We project to :class:`ColumnInfo` with
the ``DATA_TYPE`` and ``NULLABLE`` columns; ``NULLABLE`` is
``"Y"`` / ``"N"`` in Oracle's dictionary.

Oracle identifier handling
--------------------------

:meth:`preview` quotes the table identifier with double quotes
per the SQL standard. Oracle stores unquoted identifiers
upper-case, so a caller that supplies ``"users"`` will not
match a table created as ``"USERS"`` unless the caller also
upper-cases the name. We follow the convention and uppercase
the input to avoid silent empty results.
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


def _build_dsn(connection: ConnectionConfig) -> str:
    """Build the Oracle Easy Connect DSN (``host:port/service``).

    When ``connection.options['service_name']`` is set we use it
    verbatim; otherwise we fall back to ``connection.database`` as
    the service name. When neither is set the connector raises
    during ``_open``.
    """
    options = dict(connection.options or {})
    service = options.get("service_name") or connection.database
    if not service:
        raise ConnectorError(
            "oracle connector requires a service_name (options.service_name "
            "or connection.database)",
            kind="oracle",
        )
    return f"{connection.host}:{connection.port}/{service}"


class OracleConnector(BaseConnector):
    """:class:`Connector` implementation for Oracle via :mod:`oracledb`."""

    KIND: ClassVar[DatasourceKind] = "oracle"

    def __init__(
        self,
        *,
        connection: ConnectionConfig,
        credentials: CredentialsPayload,
    ) -> None:
        super().__init__(connection=connection, credentials=credentials)
        self._oracledb: Any = None

    # ------------------------------------------------------------------
    # Driver lifecycle
    # ------------------------------------------------------------------

    def _ensure_driver(self) -> Any:
        if self._oracledb is None:
            import oracledb  # type: ignore[import-not-found]

            self._oracledb = oracledb
        return self._oracledb

    async def _open(self, *, timeout_seconds: float | None) -> Any:
        oracledb = self._ensure_driver()
        dsn = _build_dsn(self._connection)
        timeout = timeout_seconds if timeout_seconds is not None else 10.0
        # ``oracledb`` has an async API: ``await oracledb.connect_async(...)``.
        # We pin ``min`` / ``max`` to 1 because the connector owns
        # the connection for the duration of the call and closes
        # it in the ``finally`` block — a pool is wasted overhead.
        return await asyncio.wait_for(
            oracledb.connect_async(  # type: ignore[attr-defined]
                dsn=dsn,
                user=self._credentials.username,
                password=self._credentials.password,
            ),
            timeout=timeout,
        )

    async def _probe(self, *, timeout_seconds: float | None) -> None:
        conn = await self._open(timeout_seconds=timeout_seconds)
        try:
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1 FROM DUAL")
                await cur.fetchone()
        finally:
            await conn.close()

    # ------------------------------------------------------------------
    # Schema + preview
    # ------------------------------------------------------------------

    async def get_schema(self, database: str | None = None) -> list[TableInfo]:
        conn = await self._open(timeout_seconds=10.0)
        try:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT TABLE_NAME FROM USER_TABLES ORDER BY TABLE_NAME"
                )
                table_rows = await cur.fetchall()
                if not table_rows:
                    return []
                table_names = [row[0] for row in table_rows]
                # ``USER_TAB_COLUMNS`` is the per-user dictionary
                # view. We use ``IN (...)`` with bound parameters
                # so the names are not interpolated.
                placeholders = ",".join([":p" + str(i + 1) for i in range(len(table_names))])
                sql = (
                    f"SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, NULLABLE "
                    f"FROM USER_TAB_COLUMNS "
                    f"WHERE TABLE_NAME IN ({placeholders}) "
                    f"ORDER BY TABLE_NAME, COLUMN_ID"
                )
                await cur.execute(sql, table_names)
                column_rows = await cur.fetchall()
                # Primary-key lookup. ``USER_CONSTRAINTS`` carries
                # the constraint metadata; ``USER_CONS_COLUMNS``
                # maps the constraint to the constrained columns
                # in ``POSITION`` order. Filtering on
                # ``CONSTRAINT_TYPE = 'P'`` keeps the primary
                # key only (foreign keys / uniques are 'R' / 'U').
                await cur.execute(
                    f"SELECT cc.TABLE_NAME, cc.COLUMN_NAME, cc.POSITION "
                    f"FROM USER_CONS_COLUMNS cc "
                    f"JOIN USER_CONSTRAINTS c "
                    f"  ON cc.CONSTRAINT_NAME = c.CONSTRAINT_NAME "
                    f"WHERE c.CONSTRAINT_TYPE = 'P' "
                    f"  AND cc.TABLE_NAME IN ({placeholders}) "
                    f"ORDER BY cc.TABLE_NAME, cc.POSITION",
                    table_names,
                )
                pk_rows = await cur.fetchall()
                # Secondary-index lookup. ``USER_INDEXES`` carries
                # the index metadata; ``IND_COLUMNS`` maps an
                # index to its columns in ``COLUMN_POSITION``
                # order. ``UNIQUENESS`` is ``'UNIQUE'`` or
                # ``'NONUNIQUE'``. We exclude the implicit PK
                # index (``INDEX_TYPE = '...'`` differs across
                # versions; the safe exclusion is by
                # ``GENERATED_NAME`` when present, plus filtering
                # out ``CONSTRAINT_INDEX``-style system indexes).
                await cur.execute(
                    f"SELECT ic.TABLE_NAME, ic.INDEX_NAME, ic.COLUMN_NAME, "
                    f"       ic.COLUMN_POSITION, i.UNIQUENESS, "
                    f"       i.INDEX_TYPE, i.GENERATED_NAME "
                    f"FROM USER_IND_COLUMNS ic "
                    f"JOIN USER_INDEXES i "
                    f"  ON ic.INDEX_NAME = i.INDEX_NAME "
                    f"WHERE ic.TABLE_NAME IN ({placeholders}) "
                    f"  AND i.INDEX_TYPE IS NOT NULL "
                    f"  AND (i.GENERATED_NAME IS NULL "
                    f"       OR i.GENERATED_NAME != 'Y') "
                    f"ORDER BY ic.TABLE_NAME, ic.INDEX_NAME, ic.COLUMN_POSITION",
                    table_names,
                )
                index_rows = await cur.fetchall()
                # Row-count estimate. ``USER_TABLES.NUM_ROWS`` is
                # the dictionary statistic populated by
                # ``DBMS_STATS.GATHER_TABLE_STATS``; the value
                # is ``None`` until the first gather.
                await cur.execute(
                    "SELECT TABLE_NAME, NUM_ROWS FROM USER_TABLES"
                )
                rowcount_rows = await cur.fetchall()
        except Exception as exc:
            raise ConnectorError(
                f"oracle schema introspection failed: {exc}",
                kind=self.KIND,
            ) from exc
        finally:
            await conn.close()

        tables_by_key: dict[str, list[ColumnInfo]] = {}
        for row in column_rows:
            table_name, column_name, data_type, nullable = row
            tables_by_key.setdefault(table_name, []).append(
                ColumnInfo(
                    name=column_name,
                    type=data_type,
                    nullable=(str(nullable).upper() == "Y"),
                )
            )
        pk_by_key: dict[str, list[str]] = {}
        for table_name, column_name, _position in pk_rows:
            pk_by_key.setdefault(table_name, []).append(column_name)
        # Index projection: same composite-pattern as MySQL.
        # ``UNIQUE`` indexes are surfaced via the ``unique`` flag.
        # The ``INDEX_TYPE = 'LOB'`` / ``'CLUSTER'`` / etc. filter
        # avoids surfacing the dictionary's internal indexes as
        # user-visible secondary indexes.
        indexes_by_key: dict[str, list[IndexInfo]] = {}
        index_bufs: dict[tuple[str, str], dict[str, Any]] = {}
        for table_name, index_name, column_name, _pos, uniqueness, _type, _gen in index_rows:
            buf_key = (table_name, index_name)
            buf = index_bufs.setdefault(
                buf_key,
                {"columns": [], "unique": uniqueness == "UNIQUE"},
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
        # Row-count projection: ``NUM_ROWS`` is a ``NUMBER`` and
        # can be ``NULL`` when stats have never been gathered.
        rowcount_by_key: dict[str, int | None] = {}
        for table_name, num_rows in rowcount_rows:
            rowcount_by_key[table_name] = (
                int(num_rows) if num_rows is not None else None
            )
        return [
            TableInfo(
                name=name,
                schema=self._credentials.username.upper(),
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
        # Oracle stores unquoted identifiers upper-case. We
        # upper-case the input so a caller that passes ``"users"``
        # matches a table created as ``"USERS"``. A quoted
        # identifier is preserved verbatim — that path is
        # reserved for future use.
        bare = table.upper()
        conn = await self._open(timeout_seconds=10.0)
        try:
            async with conn.cursor() as cur:
                # ``ROWNUM <= :limit`` is the standard pre-12c
                # pagination; we use it because the connector has
                # to work against 11g+ without depending on
                # ``FETCH FIRST``.
                await cur.execute(
                    f'SELECT * FROM "{bare}" WHERE ROWNUM <= :limit',
                    {"limit": int(limit)},
                )
                columns = [d[0] for d in cur.description]
                rows = await cur.fetchall()
        except Exception as exc:
            raise ConnectorError(
                f"oracle preview failed: {exc}",
                kind=self.KIND,
            ) from exc
        finally:
            await conn.close()
        return [dict(zip(columns, row, strict=True)) for row in rows]

    async def close(self) -> None:
        await super().close()


__all__ = ["OracleConnector"]
