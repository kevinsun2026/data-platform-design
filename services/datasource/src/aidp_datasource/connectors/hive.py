"""Hive connector.

Driver: :mod:`pyhive` + :mod:`thrift` (Thrift-over-HTTP transport
via ``pyhive.hive``. HiveServer2 in HTTP mode). The
``pyhive`` package is *sync* — HiveServer2's Thrift API is not
async — so the connector wraps the driver call in
``asyncio.to_thread`` to keep the async Protocol contract
without blocking the event loop.

Connection parameters
---------------------

``pyhive.hive.connect`` takes ``host`` / ``port`` / ``username`` /
``database`` / ``auth`` kwargs. The ``auth`` knob defaults to
``"CUSTOM"`` (no SASL); Phase 1 does not support Kerberos /
LDAP because the test environment cannot stand up a
``kinit`` server. The auth mode is configurable via
``connection.options['auth']``.

Schema introspection
--------------------

:meth:`get_schema` queries ``SHOW TABLES`` in the current
database. The result is a 2-column ``(tab_name, ...)`` shape;
we project to the first column. Column metadata is fetched via
``DESCRIBE <table>`` per table (Hive does not expose a single
batched ``information_schema.columns`` query, so we issue one
``DESCRIBE`` per table). For Phase 1 the column-metadata cost
is acceptable because most Hive tenants have a small number of
tables; a future task can replace this with a
``DESCRIBE FORMATTED`` batch.

Failure handling
----------------

``pyhive`` raises a :class:`pyhive.exc.OperationalError` /
``ProgrammingError`` / etc. on auth or SQL errors. The connector
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


def _extract_columns(rows: list[tuple[Any, ...]]) -> list[ColumnInfo]:
    """Project the ``DESCRIBE <table>`` output to :class:`ColumnInfo`.

    ``DESCRIBE`` returns ``(col_name, data_type, comment)`` with
    the comment column optional. We use the first two columns and
    fall back to ``"unknown"`` when the type column is empty.
    """
    out: list[ColumnInfo] = []
    for row in rows:
        if not row:
            continue
        name = str(row[0]).strip() if row[0] is not None else ""
        if not name or name.startswith("#"):
            # ``DESCRIBE`` emits comment lines starting with ``#``.
            continue
        type_str = str(row[1]).strip() if len(row) > 1 and row[1] is not None else "unknown"
        # Hive columns are nullable by default; the dictionary
        # does not carry a ``nullable`` flag.
        out.append(ColumnInfo(name=name, type=type_str, nullable=True))
    return out


class HiveConnector(BaseConnector):
    """:class:`Connector` implementation for Hive via :mod:`pyhive`."""

    KIND: ClassVar[DatasourceKind] = "hive"

    def __init__(
        self,
        *,
        connection: ConnectionConfig,
        credentials: CredentialsPayload,
    ) -> None:
        super().__init__(connection=connection, credentials=credentials)
        self._pyhive: Any = None

    # ------------------------------------------------------------------
    # Driver lifecycle
    # ------------------------------------------------------------------

    def _ensure_driver(self) -> Any:
        if self._pyhive is None:
            from pyhive import hive  # type: ignore[import-not-found]

            self._pyhive = hive
        return self._pyhive

    def _build_connect_kwargs(self) -> dict[str, Any]:
        """Build the kwargs for ``pyhive.hive.connect``.

        The ``auth`` knob defaults to ``"CUSTOM"`` (anonymous
        HiveServer2 in HTTP mode). ``password`` is the Hive
        password; ``username`` doubles as the Kerberos principal
        when ``auth="KERBEROS"`` (not exercised in Phase 1).
        """
        opts = dict(self._connection.options or {})
        return {
            "host": self._connection.host,
            "port": self._connection.port,
            "username": self._credentials.username,
            "password": self._credentials.password,
            "database": self._connection.database or "default",
            "auth": opts.get("auth", "CUSTOM"),
        }

    def _open_sync(self) -> Any:
        """Synchronous ``pyhive.hive.connect`` (called via ``to_thread``)."""
        hive = self._ensure_driver()
        return hive.connect(**self._build_connect_kwargs())

    async def _open(self, *, timeout_seconds: float | None) -> Any:
        timeout = timeout_seconds if timeout_seconds is not None else 10.0
        return await asyncio.wait_for(
            asyncio.to_thread(self._open_sync),
            timeout=timeout,
        )

    async def _probe(self, *, timeout_seconds: float | None) -> None:
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
        target_db = database or self._connection.database or "default"
        conn = await self._open(timeout_seconds=10.0)
        try:
            await asyncio.to_thread(self._use_db, conn, target_db)
            tables = await asyncio.to_thread(self._list_tables, conn)
            out: list[TableInfo] = []
            for table_name in tables:
                columns = await asyncio.to_thread(self._describe, conn, table_name)
                # ``DESCRIBE FORMATTED`` is a heavier query that
                # also returns the table's statistics block
                # (``totalNumberOfRows`` + ``rawDataSize``). It is
                # the only cheap way to surface an estimated row
                # count when the table has not been
                # ``ANALYZE``-d. The query may be disabled on
                # some HiveServer2 deployments (ACL on the
                # ``DESCRIBE`` family of commands); we treat a
                # failure as "no estimate" rather than fail the
                # whole sync.
                try:
                    row_estimate = await asyncio.to_thread(
                        self._row_count_estimate, conn, table_name
                    )
                except ConnectorError:
                    row_estimate = None
                out.append(
                    TableInfo(
                        name=table_name,
                        schema=target_db,
                        columns=columns,
                        primary_key=[],
                        indexes=[],
                        row_count_estimate=row_estimate,
                    )
                )
            return out
        except Exception as exc:
            raise ConnectorError(
                f"hive schema introspection failed: {exc}",
                kind=self.KIND,
            ) from exc
        finally:
            await asyncio.to_thread(conn.close)

    async def preview(
        self, table: str, limit: int = 100
    ) -> list[dict[str, Any]]:
        if limit <= 0:
            return []
        conn = await self._open(timeout_seconds=10.0)
        try:
            rows, columns = await asyncio.to_thread(self._select_limit, conn, table, int(limit))
        except Exception as exc:
            raise ConnectorError(
                f"hive preview failed: {exc}",
                kind=self.KIND,
            ) from exc
        finally:
            await asyncio.to_thread(conn.close)
        return [dict(zip(columns, row, strict=True)) for row in rows]

    # ------------------------------------------------------------------
    # Sync helpers (run via ``asyncio.to_thread``)
    # ------------------------------------------------------------------

    @staticmethod
    def _use_db(conn: Any, database: str) -> None:
        cursor = conn.cursor()
        try:
            # ``USE`` has no parameters; we sanitise the database
            # name with a regex-style allow-list to defend
            # against accidental identifier injection.
            if not database.replace("_", "").replace("-", "").isalnum():
                raise ConnectorError(
                    f"hive database name is not safe: {database!r}",
                    kind="hive",
                )
            cursor.execute(f"USE `{database}`")
        finally:
            cursor.close()

    @staticmethod
    def _list_tables(conn: Any) -> list[str]:
        cursor = conn.cursor()
        try:
            cursor.execute("SHOW TABLES")
            out: list[str] = []
            for row in cursor.fetchall():
                if row and row[0]:
                    out.append(str(row[0]).strip())
            return out
        finally:
            cursor.close()

    @staticmethod
    def _describe(conn: Any, table: str) -> list[ColumnInfo]:
        if not table.replace("_", "").isalnum():
            # Defensive: refuse to ``DESCRIBE`` an identifier that
            # contains anything other than letters, digits, and
            # underscores. The PyHive driver does not bind
            # identifiers, so we cannot rely on parameter
            # substitution here.
            raise ConnectorError(
                f"hive table name is not safe: {table!r}",
                kind="hive",
            )
        cursor = conn.cursor()
        try:
            cursor.execute(f"DESCRIBE `{table}`")
            return _extract_columns(list(cursor.fetchall()))
        finally:
            cursor.close()

    @staticmethod
    def _select_limit(conn: Any, table: str, limit: int) -> tuple[list[tuple[Any, ...]], list[str]]:
        if not table.replace("_", "").isalnum():
            raise ConnectorError(
                f"hive table name is not safe: {table!r}",
                kind="hive",
            )
        cursor = conn.cursor()
        try:
            cursor.execute(f"SELECT * FROM `{table}` LIMIT {int(limit)}")
            columns = [d[0] for d in cursor.description]
            rows = list(cursor.fetchall())
            return rows, columns
        finally:
            cursor.close()

    @staticmethod
    def _row_count_estimate(conn: Any, table: str) -> int | None:
        """Best-effort row count from ``DESCRIBE FORMATTED``.

        Hive's ``DESCRIBE FORMATTED <table>`` output is a
        two-column ``(col_name, data_type)`` result followed by
        a ``Detailed Table Information`` block. The block lists
        ``totalNumberOfRows: <int>`` and ``rawDataSize: <int>``
        on consecutive lines when the table has been
        ``ANALYZE``-d; both are ``None`` otherwise.

        The function returns ``None`` (rather than raising) when
        the table has no stats so the caller's caller can carry
        on with the rest of the introspection.

        Args:
            conn: An open ``pyhive.hive`` connection.
            table: The bare table name. Must satisfy the
                ``_describe`` allow-list (alphanumeric +
                underscore).

        Returns:
            The estimated row count, or ``None`` when the value
            is absent from the output.
        """
        if not table.replace("_", "").isalnum():
            raise ConnectorError(
                f"hive table name is not safe: {table!r}",
                kind="hive",
            )
        cursor = conn.cursor()
        try:
            cursor.execute(f"DESCRIBE FORMATTED `{table}`")
            rows = list(cursor.fetchall())
        finally:
            cursor.close()
        in_detailed = False
        for row in rows:
            if not row or not row[0]:
                continue
            key = str(row[0]).strip().lower()
            if "detailed table information" in key:
                in_detailed = True
                continue
            if not in_detailed:
                continue
            if key == "totalnumberofrows" and len(row) > 1 and row[1] is not None:
                try:
                    return int(str(row[1]).strip())
                except (TypeError, ValueError):
                    return None
        return None

    async def close(self) -> None:
        await super().close()


__all__ = ["HiveConnector"]
