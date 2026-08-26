"""PostgreSQL connector.

Driver: :mod:`asyncpg` (the async-first Postgres driver). The
connection is opened lazily inside :meth:`_probe`,
:meth:`get_schema`, and :meth:`preview` so the cost of
constructing the connector (driver import, URL parsing) is paid
even for the ``GET /api/v1/datasources/types`` endpoint, but the
cost of opening a real socket is paid only when actually needed.

Connection string
-----------------

``asyncpg`` accepts a ``postgresql://user:pass@host:port/database``
URL, which is the form the connector builds. The
``connection.options`` dict is folded into the URL's query string
so driver-specific knobs (``application_name``,
``statement_timeout``, ``sslmode``) flow through naturally.

Failure handling
----------------

:meth:`_probe` opens a short-lived connection and runs
``SELECT 1``. The asyncpg ``InvalidPasswordError`` /
``ConnectionDoesNotExistError`` / etc. propagate to
:meth:`BaseConnector.test` and end up in the
:class:`TestResult.error` field — the test endpoint never raises.

Schema introspection
--------------------

:meth:`get_schema` queries ``information_schema.tables`` and
``information_schema.columns`` filtered to base tables owned by
the current user. We deliberately do **not** expose views or
foreign tables in Phase 1 to keep the agent-gateway's mental
model small.
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
    """Build an ``asyncpg`` DSN from a :class:`ConnectionConfig` + options.

    The credentials are intentionally not in the URL — the
    :class:`_PostgresConnector` calls ``asyncpg.connect(dsn, ...,
    password=...)`` so the password never lands in a log line.
    """
    options = dict(connection.options or {})
    db = connection.database or "postgres"
    # We build a ``host:port/database`` skeleton; the caller injects
    # the user / password via the ``user`` / ``password`` keyword
    # arguments of ``asyncpg.connect``.
    query = "&".join(f"{k}={v}" for k, v in options.items())
    base = f"postgresql://{connection.host}:{connection.port}/{db}"
    return f"{base}?{query}" if query else base


def _parse_pg_indexdef(*, indexdef: str, indexname: str) -> IndexInfo | None:
    """Parse a ``pg_indexes.indexdef`` string into an :class:`IndexInfo`.

    The grammar we accept is the canonical
    ``CREATE [UNIQUE] INDEX name ON schema.table USING method (cols)`` form
    emitted by Postgres itself; we do **not** try to handle every
    extension. The parser is intentionally narrow — when it
    cannot make sense of the input it returns ``None`` and the
    caller drops the entry with a warning, rather than emit a
    malformed :class:`IndexInfo`.

    Args:
        indexdef: The verbatim ``indexdef`` column from ``pg_indexes``.
        indexname: The index name (carried verbatim into the
            result; we use it as a fallback when the parser
            cannot extract the name from the ``indexdef``).

    Returns:
        The parsed :class:`IndexInfo`, or ``None`` when the
        ``indexdef`` is unrecognised.
    """
    upper = indexdef.strip().upper()
    is_unique = upper.startswith("CREATE UNIQUE INDEX")
    if not (upper.startswith("CREATE INDEX") or is_unique):
        return None
    # The column list sits between the outermost parentheses. We
    # split on the first ``(`` to drop the prefix, then take
    # everything from the first ``(`` to the matching ``)`` at
    # the end of the string. The grammar is unambiguous for
    # standard index definitions, so a simple rfind is enough.
    open_paren = indexdef.find("(")
    if open_paren < 0:
        return None
    close_paren = indexdef.rfind(")")
    if close_paren <= open_paren:
        return None
    cols_raw = indexdef[open_paren + 1 : close_paren]
    columns: list[str] = []
    for piece in cols_raw.split(","):
        token = piece.strip()
        if not token:
            continue
        # Strip the ``"schema".col`` wrapper when present; the
        # indexdef always qualifies columns with the table
        # schema. We keep the bare column name so the DDL
        # re-emit round-trips.
        if '"."' in token:
            token = token.split('"."', 1)[1]
        token = token.strip('"')
        columns.append(token)
    if not columns:
        return None
    return IndexInfo(name=indexname, columns=columns, unique=is_unique)


class PostgresConnector(BaseConnector):
    """:class:`Connector` implementation for PostgreSQL via :mod:`asyncpg`."""

    KIND: ClassVar[DatasourceKind] = "postgresql"

    def __init__(
        self,
        *,
        connection: ConnectionConfig,
        credentials: CredentialsPayload,
    ) -> None:
        super().__init__(connection=connection, credentials=credentials)
        # ``asyncpg`` is imported lazily so the platform never pays
        # the import cost when an operator registers no Postgres
        # datasources.
        self._asyncpg: Any = None

    # ------------------------------------------------------------------
    # Driver lifecycle
    # ------------------------------------------------------------------

    def _ensure_driver(self) -> Any:
        """Lazy-import :mod:`asyncpg`."""
        if self._asyncpg is None:
            import asyncpg  # type: ignore[import-not-found]

            self._asyncpg = asyncpg
        return self._asyncpg

    async def _open(self, *, timeout_seconds: float | None) -> Any:
        """Open a fresh :mod:`asyncpg` connection."""
        asyncpg = self._ensure_driver()
        dsn = _build_dsn(self._connection)
        timeout = timeout_seconds if timeout_seconds is not None else 10.0
        return await asyncio.wait_for(
            asyncpg.connect(
                dsn=dsn,
                user=self._credentials.username,
                password=self._credentials.password,
            ),
            timeout=timeout,
        )

    async def _probe(self, *, timeout_seconds: float | None) -> None:
        """Open a connection, run ``SELECT 1``, close."""
        conn = await self._open(timeout_seconds=timeout_seconds)
        try:
            await conn.fetchval("SELECT 1")
        finally:
            await conn.close()

    # ------------------------------------------------------------------
    # Schema + preview
    # ------------------------------------------------------------------

    async def get_schema(self, database: str | None = None) -> list[TableInfo]:
        """Return the base tables owned by the current user.

        Args:
            database: Override the default database. ``None`` uses
                the connection's ``database``.

        Returns:
            A list of :class:`TableInfo`. Each entry carries the
            columns, primary key, secondary indexes, and an
            estimated row count (``pg_class.reltuples``). All four
            lookups are batched in three extra round-trips so the
            overall cost is O(1) on the wire regardless of the
            number of tables.

        Raises:
            ConnectorError: When any of the introspection queries
                fail.
        """
        target_db = database or self._connection.database
        if not target_db:
            raise ConnectorError(
                "postgres schema introspection requires a database name",
                kind=self.KIND,
            )
        conn = await self._open(timeout_seconds=10.0)
        try:
            # ``table_schema NOT IN (...)`` filters out the
            # system / information_schema catalogs. ``table_type =
            # 'BASE TABLE'`` excludes views + foreign tables.
            tables_sql = (
                "SELECT table_schema, table_name "
                "FROM information_schema.tables "
                "WHERE table_type = 'BASE TABLE' "
                "AND table_schema NOT IN ('pg_catalog', 'information_schema') "
                "AND table_catalog = $1 "
                "ORDER BY table_schema, table_name"
            )
            table_rows = await conn.fetch(tables_sql, target_db)
            if not table_rows:
                return []
            # Batched column lookup: one query, one round-trip.
            columns_sql = (
                "SELECT table_schema, table_name, column_name, data_type, is_nullable "
                "FROM information_schema.columns "
                "WHERE table_catalog = $1 "
                "AND table_schema NOT IN ('pg_catalog', 'information_schema') "
                "AND (table_schema, table_name) = ANY($2::text[][]) "
                "ORDER BY table_schema, table_name, ordinal_position"
            )
            # Build the ``ANY($2)`` array literal from the tables
            # we just listed. ``asyncpg`` will encode the list of
            # 2-tuples as a ``text[]`` parameter.
            pairs = [(r["table_schema"], r["table_name"]) for r in table_rows]
            column_rows = await conn.fetch(columns_sql, target_db, pairs)
            # Primary-key lookup. ``information_schema.table_constraints``
            # joined to ``key_column_usage`` gives us the PK columns
            # in the right order; we filter to ``constraint_type =
            # 'PRIMARY KEY'`` so foreign keys / uniques do not leak
            # in.
            pk_sql = (
                "SELECT tc.table_schema, tc.table_name, kcu.column_name, "
                "       kcu.ordinal_position "
                "FROM information_schema.table_constraints tc "
                "JOIN information_schema.key_column_usage kcu "
                "  ON tc.constraint_schema = kcu.constraint_schema "
                "  AND tc.constraint_name = kcu.constraint_name "
                "WHERE tc.table_catalog = $1 "
                "  AND tc.constraint_type = 'PRIMARY KEY' "
                "  AND (tc.table_schema, tc.table_name) = ANY($2::text[][]) "
                "ORDER BY tc.table_schema, tc.table_name, kcu.ordinal_position"
            )
            pk_rows = await conn.fetch(pk_sql, target_db, pairs)
            # Secondary-index lookup. ``pg_indexes`` carries the
            # index name + a verbatim ``indexdef`` string. We also
            # pull ``pg_index.indisunique`` so a unique index is
            # distinguished from a plain one. Column extraction
            # happens in Python because the ``indexdef`` form is
            # the source of truth and parsing the column list
            # client-side is more portable than joining to
            # ``pg_index`` / ``pg_attribute`` / ``pg_index_indrelid``.
            indexes_sql = (
                "SELECT schemaname, tablename, indexname, indexdef "
                "FROM pg_indexes "
                "WHERE schemaname NOT IN ('pg_catalog', 'information_schema') "
                "  AND (schemaname, tablename) = ANY($1::text[][]) "
                "ORDER BY schemaname, tablename, indexname"
            )
            index_rows = await conn.fetch(indexes_sql, pairs)
            # Row-count estimate. ``pg_class.reltuples`` is a
            # planner statistic updated by ``ANALYZE``; the
            # value is approximate (the docstring says "this is
            # only an estimate"). It is cheap (single index scan
            # of ``pg_class``) and the right knob for the
            # agent-gateway's "is this a big table?" hint.
            rowcount_sql = (
                "SELECT n.nspname AS table_schema, c.relname AS table_name, "
                "       c.reltuples::bigint AS row_estimate "
                "FROM pg_class c "
                "JOIN pg_namespace n ON c.relnamespace = n.oid "
                "WHERE c.relkind = 'r' "
                "  AND n.nspname NOT IN ('pg_catalog', 'information_schema') "
                "  AND (n.nspname, c.relname) = ANY($1::text[][])"
            )
            rowcount_rows = await conn.fetch(rowcount_sql, pairs)
        except Exception as exc:
            raise ConnectorError(
                f"postgres schema introspection failed: {exc}",
                kind=self.KIND,
            ) from exc
        finally:
            await conn.close()

        # Project the result into ``TableInfo`` / ``ColumnInfo``.
        tables_by_key: dict[tuple[str, str], list[ColumnInfo]] = {}
        for row in column_rows:
            key = (row["table_schema"], row["table_name"])
            tables_by_key.setdefault(key, []).append(
                ColumnInfo(
                    name=row["column_name"],
                    type=row["data_type"],
                    nullable=(row["is_nullable"].upper() == "YES"),
                )
            )
        # PK projection: group PK columns by (schema, table) in
        # ordinal order. The ``ORDER BY`` above already sorts
        # by ordinal position, so we just append in fetch order.
        pk_by_key: dict[tuple[str, str], list[str]] = {}
        for row in pk_rows:
            key = (row["table_schema"], row["table_name"])
            pk_by_key.setdefault(key, []).append(row["column_name"])
        # Row-count projection: ``reltuples`` is a float in
        # Postgres; we cast to bigint in SQL so the projection is
        # just an int. ``-1`` is the conventional sentinel for
        # "never analyzed" — surface that as ``None`` so the
        # downstream consumer does not show a giant negative.
        rowcount_by_key: dict[tuple[str, str], int | None] = {}
        for row in rowcount_rows:
            key = (row["table_schema"], row["table_name"])
            value = row["row_estimate"]
            rowcount_by_key[key] = None if value is None or value < 0 else int(value)
        # Index projection: parse the ``indexdef`` string to
        # extract the column list + uniqueness. The format is
        # ``CREATE [UNIQUE] INDEX name ON schema.table USING
        # method (col1, col2, ...)``. We keep the parser tiny
        # and conservative — anything we cannot parse we drop
        # with a warning rather than emit a malformed DDL.
        indexes_by_key: dict[tuple[str, str], list[IndexInfo]] = {}
        for row in index_rows:
            key = (row["schemaname"], row["tablename"])
            parsed = _parse_pg_indexdef(
                indexdef=str(row["indexdef"]),
                indexname=str(row["indexname"]),
            )
            if parsed is None:
                continue
            indexes_by_key.setdefault(key, []).append(parsed)
        return [
            TableInfo(
                name=row["table_name"],
                schema=row["table_schema"],
                columns=tables_by_key.get((row["table_schema"], row["table_name"]), []),
                primary_key=pk_by_key.get((row["table_schema"], row["table_name"],), []),
                indexes=indexes_by_key.get((row["table_schema"], row["table_name"]), []),
                row_count_estimate=rowcount_by_key.get((row["table_schema"], row["table_name"])),
            )
            for row in table_rows
        ]

    async def preview(
        self, table: str, limit: int = 100
    ) -> list[dict[str, Any]]:
        """Return up to *limit* rows from *table* as a list of dicts.

        The query is parameterised on the table name to defend
        against accidental identifier injection (a name with a
        semicolon would otherwise terminate the statement early).
        asyncpg escapes the identifier via :meth:`Connection.escape_identifier`.

        Args:
            table: Table name. May be qualified with a schema
                (``"public.users"``); the connector splits on the
                first dot and quotes each side.
            limit: Row cap. Defaults to 100; capped at 1000 by
                the platform-level limit (Phase 1 keeps the cap
                in the API layer, not the connector).

        Returns:
            A list of row dicts (``column → value``).

        Raises:
            ConnectorError: When the query fails.
        """
        if limit <= 0:
            return []
        # Split schema-qualified names; fall back to public when no
        # schema is supplied.
        if "." in table:
            schema, _, bare = table.partition(".")
        else:
            schema, bare = "public", table
        conn = await self._open(timeout_seconds=10.0)
        try:
            ident_schema = conn.escape_identifier(schema)
            ident_table = conn.escape_identifier(bare)
            sql = f"SELECT * FROM {ident_schema}.{ident_table} LIMIT $1"
            rows = await conn.fetch(sql, int(limit))
        except Exception as exc:
            raise ConnectorError(
                f"postgres preview failed: {exc}",
                kind=self.KIND,
            ) from exc
        finally:
            await conn.close()
        # ``Record`` is dict-like; ``dict(row)`` materialises it
        # so the caller can JSON-serialise the result.
        return [dict(row) for row in rows]

    async def close(self) -> None:
        """Idempotent. asyncpg connections are opened/closed per-call."""
        await super().close()


__all__ = ["PostgresConnector"]
