"""Schema sync + listing + preview + DDL service.

The Datasource service exposes a small cache of ``information_schema``
snapshots so a tenant browsing the catalog does not have to open
a live connection on every page load. This module is the
business layer for that cache:

- :func:`compute_fingerprint` — derive a stable SHA-256 hex digest
  of a :class:`list[TableInfo]` so the schema service can detect
  upstream drift between two snapshots.
- :class:`SchemaService` — sync (replace the snapshot), list
  (read from the cache), preview (live ``SELECT * LIMIT N``), and
  DDL (render the snapshot back to ``CREATE TABLE`` SQL).

The HTTP layer in :mod:`aidp_datasource.api.schemas` projects
this service's DTOs onto the wire format; the
:mod:`aidp_datasource.jobs.sync_schema` module calls
:meth:`SchemaService.sync_schema` from a FastAPI background task
so the POST endpoint can return ``202 Accepted`` with a job id
while the actual introspection runs in the background.

Why a separate service?
-----------------------

The pre-existing :mod:`aidp_datasource.services.datasource_service`
ships a :meth:`DatasourceService.refresh_schema` method (Task 14).
That method is a *fire-and-forget* helper used by the
``POST /datasources/{id}/test`` happy path. It does not track
fingerprint drift, does not return a job id, and does not
expose the snapshot via the REST surface. Task 15 introduces
the production sync contract: jobs, fingerprint change
detection, preview, and DDL.

Layering
--------

``api → service → connector`` and ``api → job → service``. The
service does not import FastAPI / BackgroundTasks so the same
method can be invoked from a Celery / RQ worker once the brief
graduates the in-process background runner.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol, cast

from aidp_common.errors import NotFoundError
from aidp_db.session import get_session
from sqlalchemy import select

from aidp_datasource.connectors.base import (
    ColumnInfo,
    IndexInfo,
    TableInfo,
    build_connector,
)
from aidp_datasource.models import Datasource, DatasourceSchema
from aidp_datasource.schemas import (
    ConnectionConfig,
    CredentialsPayload,
    DatasourceKind,
)
from aidp_datasource.services.credential_service import (
    CredentialService,
    default_credential_service,
)
from aidp_datasource.services.datasource_service import asyncio_run

_LOG = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Fingerprint
# ---------------------------------------------------------------------------


def _canonical_table_dict(table: TableInfo) -> dict[str, Any]:
    """Project a :class:`TableInfo` to the canonical fingerprint payload.

    The payload is a *normalised* representation: the column
    list is sorted by ordinal position (already true of the
    dataclass), the index list is sorted by ``(name, columns)``
    so two engines that return the same indexes in a different
    order still hash to the same digest, and ``None`` fields
    are stripped so ``None`` and ``""`` do not produce
    different fingerprints.

    We deliberately **exclude** :attr:`TableInfo.row_count_estimate`
    from the fingerprint because row counts change for
    non-schema reasons (an ``INSERT`` / ``DELETE`` / ``ANALYZE``)
    and would force a "schema changed" alert on every
    nightly ETL load. The brief's "fingerprint 检测 schema 变更"
    is the upstream-*schema* drift detector, not the
    row-count change detector.
    """
    return {
        "name": table.name,
        "schema": table.schema or "",
        "columns": [
            {"name": c.name, "type": c.type, "nullable": bool(c.nullable)}
            for c in table.columns
        ],
        "primary_key": list(table.primary_key),
        "indexes": sorted(
            (
                {
                    "name": ix.name,
                    "columns": list(ix.columns),
                    "unique": bool(ix.unique),
                }
                for ix in table.indexes
            ),
            key=lambda d: (d["name"], d["columns"]),
        ),
    }


def compute_fingerprint(tables: list[TableInfo]) -> str:
    """Return a stable SHA-256 hex digest of *tables*.

    The digest is computed over a canonicalised JSON payload so
    two snapshots with semantically identical schemas but
    different column orderings, key orderings, or
    ``information_schema`` row orderings still hash to the
    same value. The format is::

        sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")))

    See :func:`_canonical_table_dict` for the exact field set.

    Args:
        tables: The freshly-introspected table list returned by
            :meth:`Connector.get_schema`.

    Returns:
        The lowercase hex digest (64 chars). Returns the
        SHA-256 of the empty string (``"e3b0c4..."``) for an
        empty schema — that is still a valid fingerprint and
        lets the caller detect "database went empty" the same
        way it detects "database changed".
    """
    payload = sorted(
        (_canonical_table_dict(t) for t in tables),
        key=lambda d: (d["schema"], d["name"]),
    )
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


# ---------------------------------------------------------------------------
# DTOs
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SchemaSyncResult:
    """The outcome of a single sync run.

    Returned by :meth:`SchemaService.sync_schema`. The HTTP
    layer projects this onto the ``SchemaSyncAcceptedResponse``
    body so the agent-gateway can render "schema changed by
    +N tables" without re-fetching the row.

    Attributes:
        job_id: Caller-supplied job id (or a fresh UUID when
            called via the background runner).
        datasource_id: The datasource that was synced.
        status: The terminal status of the run — ``"succeeded"``
            or ``"failed"``. The brief uses
            ``"succeeded" / "failed"`` to stay consistent with
            the connection-test vocabulary.
        fingerprint: The freshly-computed SHA-256 hex digest.
        table_count: Number of tables observed in the live
            introspection. ``0`` when the database is empty.
        changed: ``True`` when the new fingerprint differs from
            the prior snapshot's fingerprint (or when there was
            no prior snapshot). ``False`` when the schema is
            byte-identical to the previous one — the agent can
            then skip downstream "schema changed" notifications.
        prior_fingerprint: The fingerprint of the previous
            snapshot, or ``None`` when no prior snapshot exists.
        error: Truncated error string, or ``None`` on success.
    """

    job_id: str
    datasource_id: str
    status: str
    fingerprint: str
    table_count: int
    changed: bool
    prior_fingerprint: str | None
    error: str | None = None


@dataclass(frozen=True)
class SchemaSyncJob:
    """A durable handle for a sync request.

    The :class:`aidp_datasource.jobs.sync_schema` module keeps
    one of these per in-flight or recently-completed sync. The
    background task updates the ``status`` field as the run
    progresses; the HTTP layer reads the same dict when the
    client polls for completion.

    Attributes:
        job_id: UUID4 string returned to the caller.
        datasource_id: The datasource being synced.
        tenant_id: Tenant the sync belongs to (L1 isolation).
        status: One of ``"pending"`` / ``"running"`` /
            ``"succeeded"`` / ``"failed"``.
        fingerprint: Updated when the run finishes. ``None``
            while the run is still in flight.
        table_count: Updated when the run finishes.
        changed: ``True`` when the new fingerprint differs
            from the prior snapshot. ``False`` when the schema
            is unchanged. ``None`` while the run is in flight.
        error: Truncated error string on failure; ``None``
            otherwise.
        created_at: UTC timestamp the job was created.
        finished_at: UTC timestamp the run reached a terminal
            state. ``None`` while the run is still in flight.
    """

    job_id: str
    datasource_id: str
    tenant_id: str
    status: str
    fingerprint: str | None = None
    table_count: int | None = None
    changed: bool | None = None
    error: str | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    finished_at: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-ready dict for the API response."""
        return {
            "job_id": self.job_id,
            "datasource_id": self.datasource_id,
            "status": self.status,
            "fingerprint": self.fingerprint,
            "table_count": self.table_count,
            "changed": self.changed,
            "error": self.error,
            "created_at": self.created_at.isoformat(),
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
        }


# ---------------------------------------------------------------------------
# DDL rendering
# ---------------------------------------------------------------------------


def _quote_ident(name: str, kind: DatasourceKind) -> str:
    """Quote a SQL identifier per the engine's convention.

    Postgres uses ``"double"`` quotes (the SQL standard),
    MySQL uses backticks, Oracle uses ``"double"`` (with
    the caveat that unquoted names are upper-case on disk),
    Hive uses backticks. The function is intentionally tiny
    — it only handles the case where the identifier needs
    quoting (always, for DDL round-trip). The DDL is
    consumed by the agent-gateway's planner, not a live
    database, so the quote style is purely cosmetic.
    """
    if kind in ("postgresql", "oracle"):
        return f'"{name}"'
    # MySQL + Hive use backticks.
    return f"`{name}`"


def _render_ddl(tables: list[dict[str, Any]], kind: DatasourceKind) -> str:
    """Render ``tables`` to a ``CREATE TABLE`` DDL string.

    The output is one ``CREATE TABLE`` statement per table,
    followed by ``CREATE [UNIQUE] INDEX`` statements for each
    secondary index. The PK is emitted as a column-level
    ``PRIMARY KEY`` clause **and** a table-level
    ``PRIMARY KEY (...)`` constraint so the DDL is self-
    contained (no implicit PK index).

    The output is plain text — the agent-gateway feeds it
    into the LLM prompt verbatim, so we keep the formatting
    stable (two-space indent, trailing newline per statement).

    Args:
        tables: The wire-format table list (output of
            :func:`aidp_datasource.services.datasource_service._table_to_dict`).
        kind: The datasource kind — drives the quote style.

    Returns:
        The multi-line DDL string. Empty string when *tables*
        is empty.
    """
    if not tables:
        return ""
    chunks: list[str] = []
    for tbl in tables:
        name = tbl["name"]
        schema = tbl.get("schema")
        qschema = _quote_ident(schema, kind) if schema else ""
        qname = _quote_ident(name, kind)
        qualified = f"{qschema}.{qname}" if qschema else qname
        column_lines: list[str] = []
        for col in tbl.get("columns") or []:
            line = f"  {_quote_ident(col['name'], kind)} {col['type']}"
            if not col.get("nullable", True):
                line += " NOT NULL"
            column_lines.append(line)
        pk_cols = list(tbl.get("primary_key") or [])
        if pk_cols:
            pk_list = ", ".join(_quote_ident(c, kind) for c in pk_cols)
            column_lines.append(f"  PRIMARY KEY ({pk_list})")
        body = ",\n".join(column_lines)
        chunks.append(f"CREATE TABLE {qualified} (\n{body}\n);")
        for ix in tbl.get("indexes") or []:
            unique = "UNIQUE " if ix.get("unique") else ""
            cols = ", ".join(_quote_ident(c, kind) for c in ix.get("columns") or [])
            qix = _quote_ident(ix["name"], kind)
            chunks.append(
                f"CREATE {unique}INDEX {qix} ON {qualified} ({cols});"
            )
    return "\n".join(chunks) + "\n"


# ---------------------------------------------------------------------------
# Connector factory protocol (for tests)
# ---------------------------------------------------------------------------


class _ConnectorFactory(Protocol):
    """The connector factory signature :class:`SchemaService` consumes.

    Defined as a :class:`Protocol` so the test suite can inject
    a stub without subclassing. The real factory is
    :func:`aidp_datasource.connectors.base.build_connector`.
    """

    def __call__(
        self,
        *,
        kind: DatasourceKind,
        connection: ConnectionConfig,
        credentials: CredentialsPayload,
    ) -> Any: ...


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------


class SchemaService:
    """Business-orchestration layer for the schema cache.

    The class follows the same shape as
    :class:`aidp_datasource.services.datasource_service.DatasourceService`:
    a thin constructor + plain methods, no per-request state.
    The default factory :func:`default_schema_service` wires
    the production dependencies (the process-wide
    :class:`CredentialService` + the real connector factory).
    Tests can substitute their own via
    :func:`set_default_schema_service`.

    The async sync path is provided by
    :meth:`sync_schema`; the HTTP / job layer is responsible
    for turning the synchronous return value into a
    background-task invocation. The service never touches
    FastAPI's ``BackgroundTasks`` directly — that keeps the
    surface reusable from a Celery / RQ worker later.
    """

    def __init__(
        self,
        *,
        credential_service: CredentialService | None = None,
        connector_factory: _ConnectorFactory | None = None,
    ) -> None:
        self._credentials = credential_service or default_credential_service()
        # ``connector_factory`` is held as a private attribute;
        # ``None`` means "look up the real factory via the
        # module attribute at call time" so a
        # ``monkeypatch.setattr`` on
        # ``aidp_datasource.services.schema_service.build_connector``
        # takes effect. The lazy lookup is implemented in
        # :meth:`_call_connector_factory` below.
        self._factory_override = connector_factory

    def _call_connector_factory(
        self,
        *,
        kind: DatasourceKind,
        connection: ConnectionConfig,
        credentials: CredentialsPayload,
    ) -> Any:
        """Resolve the connector factory at call time.

        When the caller supplied a factory via the constructor
        we use it directly. Otherwise we use the
        module-level :data:`build_connector` reference, which
        the test suite's ``monkeypatch.setattr`` rewrites in
        place (storing the bound function at construction would
        freeze the reference and silently defeat
        monkeypatching).
        """
        if self._factory_override is not None:
            return self._factory_override(
                kind=kind, connection=connection, credentials=credentials
            )
        return build_connector(
            kind=kind, connection=connection, credentials=credentials
        )

    # ------------------------------------------------------------------
    # Sync (replaces the cache snapshot)
    # ------------------------------------------------------------------

    def sync_schema(
        self,
        *,
        tenant_id: str,
        actor: str,
        datasource_id: str,
        job_id: str | None = None,
        database: str | None = None,
    ) -> SchemaSyncResult:
        """Run a synchronous schema refresh.

        The method opens the live connection, fetches the
        fresh table list via :meth:`Connector.get_schema`,
        computes the fingerprint, and replaces the cached
        snapshot in a single transaction. The fingerprint is
        compared against the prior snapshot's fingerprint so
        the caller can decide whether to publish a "schema
        changed" downstream event.

        Args:
            tenant_id: Tenant the datasource belongs to.
            actor: User id writing the audit row.
            datasource_id: The datasource to sync.
            job_id: Optional pre-allocated job id. The
                background runner supplies this so the
                :class:`SchemaSyncJob` and the
                :class:`SchemaSyncResult` share an id; the
                direct call (no job) may pass ``None`` and a
                fresh UUID will be allocated.
            database: Optional database override. ``None``
                uses the connection's default database.

        Returns:
            A :class:`SchemaSyncResult` describing the run.

        Raises:
            NotFoundError: When the datasource row is missing
                (or belongs to a different tenant).
            ConnectorError: When the live introspection fails.
        """
        # Local imports to keep the module import graph small
        # for the test suite and to avoid a circular import
        # with :mod:`aidp_datasource.services.datasource_service`
        # at module load time.
        import uuid

        from aidp_datasource.models import DatasourceAudit

        if job_id is None:
            job_id = str(uuid.uuid4())

        with get_session() as session:
            row = session.execute(
                select(Datasource).where(
                    Datasource.id == datasource_id,
                    Datasource.tenant_id == tenant_id,
                )
            ).scalar_one_or_none()
            if row is None:
                raise NotFoundError("datasource", datasource_id)
            connection = ConnectionConfig.model_validate(dict(row.connection_json))
            credentials = self._credentials.decrypt(
                ciphertext=row.credentials_ciphertext,
                nonce=row.credentials_nonce,
                tenant_id=row.tenant_id,
                datasource_id=row.id,
                kind=row.kind,
            )
            # Capture the prior fingerprint (if any) so the
            # result can flag "changed" without a second SELECT.
            prior = session.execute(
                select(DatasourceSchema)
                .where(
                    DatasourceSchema.datasource_id == datasource_id,
                    DatasourceSchema.tenant_id == tenant_id,
                )
                .order_by(DatasourceSchema.refreshed_at.desc().nullslast())
                .limit(1)
            ).scalar_one_or_none()
            prior_fingerprint = prior.fingerprint if prior is not None else None
            kind = row.kind
        # Build + drive the connector *outside* the session
        # so the network round-trip does not hold a DB
        # connection from the pool.
        connector = self._call_connector_factory(
            kind=cast(DatasourceKind, kind),
            connection=connection,
            credentials=credentials,
        )
        try:
            target_db = database or connection.database
            try:
                tables = asyncio_run(connector.get_schema(target_db))
            except Exception as exc:
                # Surface the failure as a structured result
                # (with ``status="failed"``) so the job
                # registry can record it without raising. The
                # caller can still inspect ``error`` to render
                # the failure in the UI.
                _LOG.warning(
                    "schema sync failed at introspection",
                    extra={
                        "tenant_id": tenant_id,
                        "datasource_id": datasource_id,
                        "error": str(exc),
                    },
                )
                return SchemaSyncResult(
                    job_id=job_id,
                    datasource_id=datasource_id,
                    status="failed",
                    fingerprint=prior_fingerprint or "",
                    table_count=0,
                    changed=False,
                    prior_fingerprint=prior_fingerprint,
                    error=_truncate(str(exc)),
                )
        finally:
            with contextlib.suppress(Exception):  # pragma: no cover - best-effort
                asyncio_run(connector.close())

        fingerprint = compute_fingerprint(tables)
        changed = (prior_fingerprint is None) or (fingerprint != prior_fingerprint)
        # Persist the snapshot. Same single-transaction
        # replace pattern as the legacy ``refresh_schema`` —
        # delete prior rows for the datasource, insert the
        # new one. The audit row records the fingerprint
        # delta so an operator can replay "what changed and
        # when".
        table_dicts = [_table_to_dict(t) for t in tables]
        with get_session() as session:
            session.execute(
                DatasourceSchema.__table__.delete().where(  # type: ignore[attr-defined]
                    DatasourceSchema.datasource_id == datasource_id,
                    DatasourceSchema.tenant_id == tenant_id,
                )
            )
            snapshot = DatasourceSchema(
                tenant_id=tenant_id,
                datasource_id=datasource_id,
                table_count=len(tables),
                tables_json=table_dicts,
                fingerprint=fingerprint,
                refreshed_at=datetime.now(UTC),
            )
            session.add(snapshot)
            audit = DatasourceAudit(
                tenant_id=tenant_id,
                datasource_id=datasource_id,
                action="schema_synced",
                actor=actor,
                diff_json={
                    "table_count": len(tables),
                    "fingerprint": fingerprint,
                    "prior_fingerprint": prior_fingerprint,
                    "changed": changed,
                },
            )
            session.add(audit)
            session.flush()
        return SchemaSyncResult(
            job_id=job_id,
            datasource_id=datasource_id,
            status="succeeded",
            fingerprint=fingerprint,
            table_count=len(tables),
            changed=changed,
            prior_fingerprint=prior_fingerprint,
        )

    # ------------------------------------------------------------------
    # Read path
    # ------------------------------------------------------------------

    def list_schemas(
        self,
        *,
        tenant_id: str,
        datasource_id: str,
    ) -> DatasourceSchema:
        """Return the latest cached snapshot for *datasource_id*.

        The method raises :class:`NotFoundError` when the
        datasource does not exist **or** when no snapshot has
        ever been taken (the caller cannot distinguish the two
        by design — both should result in a 404 + a clear
        "no snapshot yet" hint via the API layer that can call
        :meth:`sync_schema`).

        Raises:
            NotFoundError: When the row is missing.
        """
        with get_session() as session:
            ds = session.execute(
                select(Datasource).where(
                    Datasource.id == datasource_id,
                    Datasource.tenant_id == tenant_id,
                )
            ).scalar_one_or_none()
            if ds is None:
                raise NotFoundError("datasource", datasource_id)
            snapshot = session.execute(
                select(DatasourceSchema)
                .where(
                    DatasourceSchema.datasource_id == datasource_id,
                    DatasourceSchema.tenant_id == tenant_id,
                )
                .order_by(DatasourceSchema.refreshed_at.desc().nullslast())
                .limit(1)
            ).scalar_one_or_none()
            if snapshot is None:
                raise NotFoundError("datasource_schema", datasource_id)
            return snapshot

    def preview_table(
        self,
        *,
        tenant_id: str,
        datasource_id: str,
        table: str,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Return up to *limit* rows from *table* via the live connector.

        The method is the read-only ``SELECT * LIMIT N`` proxy
        that backs the operator dashboard's "table preview"
        pane. The connector handles the per-kind identifier
        quoting / schema-splitting.

        Args:
            tenant_id: Tenant the datasource belongs to.
            datasource_id: The datasource to query.
            table: The (possibly schema-qualified) table name.
                The connector's :meth:`Connector.preview` decides
                how to parse it.
            limit: Row cap. Defaults to 100; the API layer
                enforces a hard cap of 1000 to defend against
                runaway responses.

        Returns:
            A list of row dicts (column → value).

        Raises:
            NotFoundError: When the datasource is missing.
        """
        connector = self._open_live_connector(
            tenant_id=tenant_id, datasource_id=datasource_id
        )
        try:
            return cast(
                "list[dict[str, Any]]",
                asyncio_run(connector.preview(table, limit)),
            )
        finally:
            with contextlib.suppress(Exception):  # pragma: no cover - best-effort
                asyncio_run(connector.close())

    def get_table_ddl(
        self,
        *,
        tenant_id: str,
        datasource_id: str,
        table: str,
    ) -> str:
        """Return the rendered DDL for *table* from the cached snapshot.

        The DDL is rendered from the cached snapshot — *not*
        from a live ``SHOW CREATE TABLE`` query — so the
        endpoint is cheap and survives brief upstream
        outages. The snapshot must already include the table
        (the caller's first sync captures every table the
        connector listed at sync time).

        Args:
            tenant_id: Tenant the datasource belongs to.
            datasource_id: The datasource the snapshot covers.
            table: The (possibly schema-qualified) table name.
                The schema is matched on the snapshot's
                ``schema`` + ``name`` pair, with a fallback to
                name-only when the caller did not pass a
                schema.

        Returns:
            The multi-line ``CREATE TABLE`` string. An empty
            string is returned when the cached snapshot has
            no tables at all (the DDL of "nothing" is the
            empty string); a :class:`NotFoundError` is
            raised when the snapshot has tables but the
            requested one is not among them.

        Raises:
            NotFoundError: When the datasource is missing,
                the snapshot is missing, or the table is not
                in the snapshot.
        """
        snapshot = self.list_schemas(
            tenant_id=tenant_id, datasource_id=datasource_id
        )
        # An empty snapshot has no DDL to render. We
        # short-circuit *before* the table lookup so the
        # caller does not get a confusing 404 ("table not
        # found") for a database that genuinely has no
        # tables.
        if not snapshot.tables_json:
            return ""
        # Parse the caller-supplied name into (schema, table)
        # the same way the connector does. We keep the
        # parsing local so the snapshot service does not
        # need to know per-kind quoting rules.
        if "." in table:
            schema, _, bare = table.partition(".")
        else:
            schema, bare = None, table
        # Locate the table in the snapshot. The snapshot's
        # ``schema`` field is set by the connector (the
        # database for MySQL/Hive, the schema for PG, the
        # user for Oracle). When the caller did not pass a
        # schema we match on name only — that is the
        # common case for the operator UI.
        match: dict[str, Any] | None = None
        for entry in snapshot.tables_json:
            entry_name = entry.get("name")
            entry_schema = entry.get("schema")
            if entry_name != bare:
                continue
            if schema is None or schema == entry_schema:
                match = entry
                break
        if match is None:
            raise NotFoundError("table", table)
        with get_session() as session:
            ds = session.execute(
                select(Datasource).where(
                    Datasource.id == datasource_id,
                    Datasource.tenant_id == tenant_id,
                )
            ).scalar_one_or_none()
            if ds is None:
                raise NotFoundError("datasource", datasource_id)
            kind: DatasourceKind = ds.kind  # type: ignore[assignment]
        return _render_ddl([match], kind)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _open_live_connector(
        self,
        *,
        tenant_id: str,
        datasource_id: str,
    ) -> Any:
        """Build a live :class:`Connector` for *datasource_id*.

        Pulls the encrypted credentials out of the database
        and hands them to the factory. The caller is
        responsible for calling :meth:`Connector.close`.
        """
        with get_session() as session:
            row = session.execute(
                select(Datasource).where(
                    Datasource.id == datasource_id,
                    Datasource.tenant_id == tenant_id,
                )
            ).scalar_one_or_none()
            if row is None:
                raise NotFoundError("datasource", datasource_id)
            connection = ConnectionConfig.model_validate(dict(row.connection_json))
            credentials = self._credentials.decrypt(
                ciphertext=row.credentials_ciphertext,
                nonce=row.credentials_nonce,
                tenant_id=row.tenant_id,
                datasource_id=row.id,
                kind=row.kind,
            )
            kind = row.kind
        return self._call_connector_factory(
            kind=cast(DatasourceKind, kind),
            connection=connection,
            credentials=credentials,
        )


# ---------------------------------------------------------------------------
# Helpers (module-private)
# ---------------------------------------------------------------------------


def _truncate(value: str, *, limit: int = 1024) -> str:
    """Cap a string at *limit* characters with a trailing ellipsis marker."""
    if len(value) <= limit:
        return value
    return value[: limit - 3] + "..."


def _table_to_dict(table: TableInfo) -> dict[str, Any]:
    """Local copy of the table-projection helper.

    Defined here (instead of imported from
    :mod:`aidp_datasource.services.datasource_service`) to keep
    the dependency direction ``schema_service → datasource_service``
    one-way; otherwise the two services would form a cycle.
    """
    return {
        "name": table.name,
        "schema": table.schema,
        "columns": [
            {"name": c.name, "type": c.type, "nullable": c.nullable}
            for c in table.columns
        ],
        "primary_key": list(table.primary_key),
        "indexes": [
            {"name": ix.name, "columns": list(ix.columns), "unique": ix.unique}
            for ix in table.indexes
        ],
        "row_count_estimate": table.row_count_estimate,
    }


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------


_DEFAULT: SchemaService | None = None


def default_schema_service() -> SchemaService:
    """Return the process-wide :class:`SchemaService`."""
    global _DEFAULT
    if _DEFAULT is None:
        _DEFAULT = SchemaService()
    return _DEFAULT


def set_default_schema_service(service: SchemaService | None) -> None:
    """Override the process-wide service (used by tests)."""
    global _DEFAULT
    _DEFAULT = service


__all__ = [
    "ColumnInfo",
    "IndexInfo",
    "SchemaService",
    "SchemaSyncJob",
    "SchemaSyncResult",
    "TableInfo",
    "compute_fingerprint",
    "default_schema_service",
    "set_default_schema_service",
]
