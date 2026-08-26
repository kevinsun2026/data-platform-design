"""Business-orchestration layer for the Datasource service.

The HTTP layer in :mod:`aidp_datasource.api` is a thin transport
adapter; every non-trivial operation (CRUD validation, soft
delete, connection test, audit + Kafka events, encrypted
credential lookup for the internal gRPC server) lives here.

The service is the only place that:

- knows the shape of an :class:`aidp_datasource.models.Datasource` row;
- decrypts :class:`aidp_datasource.schemas.CredentialsPayload` via
  :class:`aidp_datasource.services.credential_service.CredentialService`;
- publishes the platform's ``datasource.*.v1`` events;
- writes :class:`aidp_datasource.models.DatasourceAudit` /
  :class:`aidp_datasource.models.ConnectionTest` rows.

Layering
--------

``api → service → model`` — the API layer projects ORM rows onto
the wire format and back; the service layer owns the cross-table
state transitions. Tests exercise the service directly with
``Session`` fixtures, bypassing the FastAPI machinery.

L1 isolation
------------

Every method takes ``tenant_id`` as the first argument and uses
it to scope ORM queries. The L1 listener installed by
:mod:`aidp_db.session` re-asserts the filter; the explicit
``WHERE tenant_id = ...`` clauses are belt-and-suspenders for
the cases the listener can't see (e.g. raw SQL, joins across
tables without ``tenant_id``).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from aidp_common.errors import ConflictError, NotFoundError, ValidationError
from aidp_db.session import get_session
from aidp_events.producer import publish_event
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from aidp_datasource.connectors.base import (
    TableInfo,
    TestResult,
    build_connector,
)
from aidp_datasource.models import (
    ConnectionTest,
    Datasource,
    DatasourceAudit,
    DatasourceSchema,
)
from aidp_datasource.schemas import (
    SUPPORTED_KINDS,
    CredentialsPayload,
    DatasourceCreateRequest,
    DatasourceKind,
    DatasourceUpdateRequest,
)
from aidp_datasource.services.credential_service import (
    CredentialService,
    default_credential_service,
)

_LOG = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public DTOs (returned by service methods)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DatasourceView:
    """The decrypted, in-memory shape of a registered datasource.

    Returned by :func:`get_decrypted_connection` (used by the
    internal gRPC server). REST handlers never see this — they
    get the wire-format :class:`aidp_datasource.schemas.DatasourceResponse`
    instead, which has the credentials stripped.

    Attributes:
        id: The datasource id (UUID4 string).
        tenant_id: The tenant id.
        name: The human-readable name.
        kind: The datasource kind.
        env: The deployment environment label.
        description: Optional description.
        connection: The non-secret connection descriptor.
        credentials: The plaintext credentials.
        tags: Free-form labels.
        enabled: Soft-disable flag.
    """

    id: str
    tenant_id: str
    name: str
    kind: DatasourceKind
    env: str
    description: str
    connection: dict[str, Any]
    credentials: CredentialsPayload
    tags: list[str]
    enabled: bool


@dataclass(frozen=True)
class TestConnectionOutcome:
    """The outcome of a connection-test operation.

    Returned by :func:`test_connection`. The HTTP handler
    projects this onto :class:`aidp_datasource.schemas.ConnectionTestResponse`
    and never returns the underlying :class:`TestResult` directly
    so the wire format stays decoupled from the connector.

    Attributes:
        datasource_id: The datasource id.
        status: One of ``"succeeded"`` / ``"failed"`` / ``"disabled"``.
        latency_ms: Wall-clock latency, or ``None`` on a non-connect
            error.
        error: Truncated error string, or ``None`` on success.
    """

    datasource_id: str
    status: str
    latency_ms: float | None = None
    error: str | None = None


# ---------------------------------------------------------------------------
# Service class
# ---------------------------------------------------------------------------


class DatasourceService:
    """Business-orchestration layer.

    The class is intentionally tiny: the constructor takes the
    :class:`CredentialService` and the Kafka transport, and the
    rest of the surface is plain methods. There is no per-request
    state — a single instance is safe to share across coroutines
    and request handlers.

    The default factory :func:`default_datasource_service` wires
    the production dependencies (the process-wide
    :class:`CredentialService` + the process-wide Kafka
    transport). Tests can substitute their own via
    :func:`set_default_datasource_service`.
    """

    def __init__(
        self,
        *,
        credential_service: CredentialService | None = None,
    ) -> None:
        self._credentials = credential_service or default_credential_service()

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

    def list_datasources(
        self,
        *,
        tenant_id: str,
        env: str | None = None,
        kind: str | None = None,
        tag: str | None = None,
    ) -> list[Datasource]:
        """Return the caller's tenant datasources, optionally filtered.

        Args:
            tenant_id: The caller's tenant id.
            env: Optional env filter (``"dev"`` / ``"staging"`` /
                ``"prod"`` / ``"test"``). Case-insensitive.
            kind: Optional kind filter (``"postgresql"`` /
                ``"mysql"`` / ``"oracle"`` / ``"hive"``).
            tag: Optional tag filter (any row whose ``tags`` list
                contains the value). Match is case-insensitive.

        Returns:
            A list of :class:`Datasource` rows ordered by name.

        Raises:
            ValidationError: When *kind* is not in
                :data:`aidp_datasource.schemas.SUPPORTED_KINDS`.
        """
        with get_session() as session:
            stmt = select(Datasource).where(Datasource.tenant_id == tenant_id)
            if env is not None:
                stmt = stmt.where(Datasource.env == env.strip().lower())
            if kind is not None:
                normalized = kind.strip().lower()
                if normalized not in SUPPORTED_KINDS:
                    raise ValidationError(
                        f"unsupported datasource kind: {kind!r}",
                        details={"kind": kind, "supported": sorted(SUPPORTED_KINDS)},
                    )
                stmt = stmt.where(Datasource.kind == normalized)
            rows = (
                session.execute(stmt.order_by(Datasource.name))
                .scalars()
                .all()
            )
            if tag is not None:
                # ``tags`` is a JSON column; the ``JSON_EXTRACT`` /
                # ``json_each`` style varies by dialect. We do the
                # filter in Python — the list is small and this
                # keeps the test path dialect-agnostic.
                needle = tag.strip().lower()
                rows = [r for r in rows if any(t.lower() == needle for t in r.tags)]
            return list(rows)

    def get_datasource(self, *, tenant_id: str, datasource_id: str) -> Datasource:
        """Return one :class:`Datasource` by id, scoped to the tenant.

        Raises:
            NotFoundError: When no row with *datasource_id* is
                visible to *tenant_id* (the L1 listener returns
                404 for both "no such id" and "wrong tenant", to
                avoid leaking the existence of another tenant's
                data).
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
            return row

    def create_datasource(
        self,
        *,
        tenant_id: str,
        actor: str,
        body: DatasourceCreateRequest,
    ) -> Datasource:
        """Create a new datasource (encrypts credentials before persisting).

        Side-effects:
            - writes one :class:`DatasourceAudit` row with
              ``action="created"``.
            - publishes ``datasource.registered.v1`` to Kafka.

        Raises:
            ConflictError: When a row with the same
                ``(tenant_id, name)`` already exists.
        """
        encrypted = self._credentials.encrypt(
            body.credentials,
            tenant_id=tenant_id,
            datasource_id="placeholder",  # overwritten after insert
            kind=body.kind,
        )
        with get_session() as session:
            existing = session.execute(
                select(Datasource).where(
                    Datasource.tenant_id == tenant_id,
                    Datasource.name == body.name,
                )
            ).scalar_one_or_none()
            if existing is not None:
                raise ConflictError(
                    f"datasource {body.name!r} already exists for this tenant"
                )
            row = Datasource(
                tenant_id=tenant_id,
                name=body.name,
                kind=body.kind,
                env=body.env,
                description=body.description,
                connection_json=dict(body.connection.model_dump(mode="json")),
                credentials_ciphertext=b"",
                credentials_nonce=b"",
                credentials_aad="",
                credentials_key_version=encrypted.key_version,
                tags=list(body.tags),
                enabled=1 if body.enabled else 0,
            )
            session.add(row)
            try:
                session.flush()  # forces the row to be inserted so we have its id
            except IntegrityError as exc:
                raise ConflictError(
                    f"datasource {body.name!r} already exists for this tenant"
                ) from exc
            # Re-encrypt now that we have a real id — the AAD
            # includes the id so a swapped row cannot be
            # decrypted.
            re_encrypted = self._credentials.encrypt(
                body.credentials,
                tenant_id=tenant_id,
                datasource_id=row.id,
                kind=body.kind,
            )
            row.credentials_ciphertext = re_encrypted.ciphertext
            row.credentials_nonce = re_encrypted.nonce
            row.credentials_aad = re_encrypted.aad
            # Audit row
            audit = DatasourceAudit(
                tenant_id=tenant_id,
                datasource_id=row.id,
                action="created",
                actor=actor,
                diff_json={
                    "name": row.name,
                    "kind": row.kind,
                    "env": row.env,
                    "tags": list(row.tags),
                    "enabled": bool(row.enabled),
                },
            )
            session.add(audit)
            session.flush()
            session.refresh(row)
            self._publish_event(
                tenant_id=tenant_id,
                event_type="datasource.registered.v1",
                payload={
                    "datasource_id": row.id,
                    "name": row.name,
                    "kind": row.kind,
                    "env": row.env,
                    "actor": actor,
                },
            )
            return row

    def update_datasource(
        self,
        *,
        tenant_id: str,
        actor: str,
        datasource_id: str,
        body: DatasourceUpdateRequest,
    ) -> Datasource:
        """Apply a partial update to an existing datasource.

        Credentials and ``kind`` are immutable here (changing the
        kind would silently switch the connector; rotating the
        password is a future task). Name uniqueness is re-checked
        when the caller renamed the datasource.

        Side-effects:
            - writes one :class:`DatasourceAudit` row with
              ``action="updated"`` (only when at least one field
              actually changed).
            - publishes ``datasource.updated.v1`` to Kafka.

        Raises:
            NotFoundError: When the row is missing.
            ConflictError: When the new name collides with another
                row of the same tenant.
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
            diff: dict[str, dict[str, Any]] = {}
            if body.name is not None and body.name != row.name:
                existing = session.execute(
                    select(Datasource).where(
                        Datasource.tenant_id == tenant_id,
                        Datasource.name == body.name,
                        Datasource.id != row.id,
                    )
                ).scalar_one_or_none()
                if existing is not None:
                    raise ConflictError(
                        f"datasource {body.name!r} already exists for this tenant"
                    )
                diff["name"] = {"old": row.name, "new": body.name}
                row.name = body.name
            if body.env is not None and body.env != row.env:
                diff["env"] = {"old": row.env, "new": body.env}
                row.env = body.env
            if body.description is not None and body.description != row.description:
                diff["description"] = {"old": row.description, "new": body.description}
                row.description = body.description
            if body.connection is not None:
                new_conn = dict(body.connection.model_dump(mode="json"))
                if new_conn != dict(row.connection_json):
                    diff["connection"] = {"old": dict(row.connection_json), "new": new_conn}
                    row.connection_json = new_conn
            if body.tags is not None and sorted(body.tags) != sorted(row.tags):
                diff["tags"] = {"old": list(row.tags), "new": list(body.tags)}
                row.tags = list(body.tags)
            if body.enabled is not None and bool(body.enabled) != bool(row.enabled):
                diff["enabled"] = {"old": bool(row.enabled), "new": bool(body.enabled)}
                row.enabled = 1 if body.enabled else 0
            if diff:
                audit = DatasourceAudit(
                    tenant_id=tenant_id,
                    datasource_id=row.id,
                    action="updated",
                    actor=actor,
                    diff_json={"changed": diff},
                )
                session.add(audit)
                self._publish_event(
                    tenant_id=tenant_id,
                    event_type="datasource.updated.v1",
                    payload={
                        "datasource_id": row.id,
                        "actor": actor,
                        "changed": list(diff.keys()),
                    },
                )
            session.flush()
            session.refresh(row)
            return row

    def soft_delete_datasource(
        self,
        *,
        tenant_id: str,
        actor: str,
        datasource_id: str,
    ) -> Datasource:
        """Soft-delete a datasource.

        Implementation detail: we set the soft-delete tombstone
        on the :class:`Datasource` row. The brief's
        ``DELETE /api/v1/datasources/{id}`` is the only writer
        for this column. The L1 listener filters on
        ``tenant_id`` so the cross-tenant probe returns 404.

        Side-effects:
            - writes one :class:`DatasourceAudit` row with
              ``action="deleted"``.
            - publishes ``datasource.disabled.v1`` to Kafka (the
              platform contract; the kind is "deletion", but the
              event name is "disabled" — kept stable so existing
              consumers do not have to handle a new event type).
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
            if row.deleted_at is None:
                row.deleted_at = datetime.now(UTC)
                row.enabled = 0
                audit = DatasourceAudit(
                    tenant_id=tenant_id,
                    datasource_id=row.id,
                    action="deleted",
                    actor=actor,
                    diff_json={"name": row.name},
                )
                session.add(audit)
                self._publish_event(
                    tenant_id=tenant_id,
                    event_type="datasource.disabled.v1",
                    payload={
                        "datasource_id": row.id,
                        "actor": actor,
                        "reason": "soft_delete",
                    },
                )
            session.flush()
            session.refresh(row)
            return row

    # ------------------------------------------------------------------
    # Connection test
    # ------------------------------------------------------------------

    def test_connection(
        self,
        *,
        tenant_id: str,
        actor: str,
        datasource_id: str,
        timeout_seconds: float | None = None,
    ) -> TestConnectionOutcome:
        """Probe the registered connection, log the outcome, return it.

        Side-effects:
            - writes one :class:`ConnectionTest` row (one per
              attempt, append-only).
            - writes one :class:`DatasourceAudit` row with
              ``action="tested"`` (with the same status / latency
              / error as the test row).
            - publishes ``datasource.test.succeeded.v1`` or
              ``datasource.test.failed.v1`` to Kafka.

        Returns:
            A :class:`TestConnectionOutcome` describing the probe.
            A disabled datasource short-circuits with
            ``status="disabled"`` without opening a socket.

        Raises:
            NotFoundError: When the row is missing.
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
            if not bool(row.enabled) or row.deleted_at is not None:
                outcome = self._record_test(
                    session=session,
                    tenant_id=tenant_id,
                    datasource=row,
                    result=TestResult(ok=False, error="datasource is disabled"),
                    actor=actor,
                    override_status="disabled",
                )
                return outcome
            # Build a transient connector for the probe. We do
            # *not* persist the decrypted credentials anywhere —
            # the plaintext only lives in the local variable
            # below.
            from aidp_datasource.schemas import ConnectionConfig

            connection_dict = dict(row.connection_json)
            connection = ConnectionConfig.model_validate(connection_dict)
            credentials = self._credentials.decrypt(
                ciphertext=row.credentials_ciphertext,
                nonce=row.credentials_nonce,
                tenant_id=row.tenant_id,
                datasource_id=row.id,
                kind=row.kind,
            )
            connector = build_connector(
                kind=row.kind,  # type: ignore[arg-type]
                connection=connection,
                credentials=credentials,
            )
        # The probe runs *outside* the ``with`` block so the
        # connection is not held during the network round-trip.
        try:
            result = asyncio_run(connector.test(timeout_seconds=timeout_seconds))
        finally:
            asyncio_run(connector.close())
        with get_session() as session:
            outcome = self._record_test(
                session=session,
                tenant_id=tenant_id,
                datasource=row,
                result=result,
                actor=actor,
            )
            return outcome

    def _record_test(
        self,
        *,
        session: Any,
        tenant_id: str,
        datasource: Datasource,
        result: TestResult,
        actor: str,
        override_status: str | None = None,
    ) -> TestConnectionOutcome:
        """Persist a :class:`ConnectionTest` + audit row + Kafka event.

        Helper for :meth:`test_connection` (and the
        "disabled" short-circuit path). Pulled out so the
        short-circuit and the probe path share the same audit +
        publish sequence.
        """
        status = override_status or ("succeeded" if result.ok else "failed")
        latency_ms = (
            int(result.latency_ms)
            if result.latency_ms is not None and status != "disabled"
            else None
        )
        test_row = ConnectionTest(
            tenant_id=tenant_id,
            datasource_id=datasource.id,
            kind=datasource.kind,
            status=status,
            latency_ms=latency_ms,
            error=_truncate(result.error) if result.error else None,
        )
        session.add(test_row)
        audit_row = DatasourceAudit(
            tenant_id=tenant_id,
            datasource_id=datasource.id,
            action="tested",
            actor=actor,
            diff_json={
                "status": status,
                "latency_ms": latency_ms,
                "error": _truncate(result.error) if result.error else None,
            },
        )
        session.add(audit_row)
        event_type = (
            "datasource.test.succeeded.v1"
            if status == "succeeded"
            else "datasource.test.failed.v1"
        )
        self._publish_event(
            tenant_id=tenant_id,
            event_type=event_type,
            payload={
                "datasource_id": datasource.id,
                "status": status,
                "latency_ms": latency_ms,
                "error": _truncate(result.error) if result.error else None,
                "actor": actor,
            },
        )
        session.flush()
        return TestConnectionOutcome(
            datasource_id=datasource.id,
            status=status,
            latency_ms=result.latency_ms,
            error=_truncate(result.error) if result.error else None,
        )

    # ------------------------------------------------------------------
    # Schema cache
    # ------------------------------------------------------------------

    def get_cached_schema(
        self,
        *,
        tenant_id: str,
        datasource_id: str,
    ) -> DatasourceSchema | None:
        """Return the most recent :class:`DatasourceSchema` row, or ``None``."""
        with get_session() as session:
            return (
                session.execute(
                    select(DatasourceSchema)
                    .where(
                        DatasourceSchema.tenant_id == tenant_id,
                        DatasourceSchema.datasource_id == datasource_id,
                    )
                    .order_by(DatasourceSchema.refreshed_at.desc().nullslast())
                    .limit(1)
                )
                .scalars()
                .first()
            )

    def refresh_schema(
        self,
        *,
        tenant_id: str,
        actor: str,
        datasource_id: str,
    ) -> DatasourceSchema:
        """Open the connection, list tables, persist a new snapshot row.

        Side-effects:
            - writes a new :class:`DatasourceSchema` row (replacing
              any prior snapshot via ``DELETE WHERE datasource_id``).
            - writes one :class:`DatasourceAudit` row with
              ``action="schema_refreshed"``.

        Raises:
            NotFoundError: When the datasource row is missing.
            ConnectorError: When the introspection query fails.
        """
        from aidp_datasource.services.schema_service import (
            compute_fingerprint,
        )

        with get_session() as session:
            row = session.execute(
                select(Datasource).where(
                    Datasource.id == datasource_id,
                    Datasource.tenant_id == tenant_id,
                )
            ).scalar_one_or_none()
            if row is None:
                raise NotFoundError("datasource", datasource_id)
            from aidp_datasource.schemas import ConnectionConfig

            connection = ConnectionConfig.model_validate(dict(row.connection_json))
            credentials = self._credentials.decrypt(
                ciphertext=row.credentials_ciphertext,
                nonce=row.credentials_nonce,
                tenant_id=row.tenant_id,
                datasource_id=row.id,
                kind=row.kind,
            )
            connector = build_connector(
                kind=row.kind,  # type: ignore[arg-type]
                connection=connection,
                credentials=credentials,
            )
        try:
            tables = asyncio_run(connector.get_schema(connection.database))
        finally:
            asyncio_run(connector.close())
        # Compute the fingerprint before opening a second
        # session so the value lands on the row in a single
        # commit. ``compute_fingerprint`` is pure (no I/O).
        fingerprint = compute_fingerprint(tables)
        # Persist the snapshot. The implementation deletes the
        # prior row (so a refresh replaces, not appends) and
        # inserts the new one in a single transaction.
        with get_session() as session:
            session.execute(
                DatasourceSchema.__table__.delete().where(
                    DatasourceSchema.datasource_id == datasource_id,
                    DatasourceSchema.tenant_id == tenant_id,
                )
            )
            snapshot = DatasourceSchema(
                tenant_id=tenant_id,
                datasource_id=datasource_id,
                table_count=len(tables),
                tables_json=[_table_to_dict(t) for t in tables],
                fingerprint=fingerprint,
                refreshed_at=datetime.now(UTC),
            )
            session.add(snapshot)
            audit = DatasourceAudit(
                tenant_id=tenant_id,
                datasource_id=datasource_id,
                action="schema_refreshed",
                actor=actor,
                diff_json={"table_count": len(tables), "fingerprint": fingerprint},
            )
            session.add(audit)
            session.flush()
            session.refresh(snapshot)
            return snapshot

    # ------------------------------------------------------------------
    # Internal gRPC: fetch decrypted connection
    # ------------------------------------------------------------------

    def get_decrypted_connection(
        self,
        *,
        tenant_id: str,
        datasource_id: str,
    ) -> DatasourceView:
        """Return the decrypted :class:`DatasourceView` for the gRPC server.

        The internal gRPC server (``DataSourceService.GetConnection``)
        needs the plaintext credentials so it can hand a live
        connection to the agent-gateway. The endpoint requires the
        gRPC server's own auth (a service-to-service token); the
        tenant_id is supplied by the gRPC layer (which already
        trusted the caller).

        Raises:
            NotFoundError: When the row is missing.
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
            credentials = self._credentials.decrypt(
                ciphertext=row.credentials_ciphertext,
                nonce=row.credentials_nonce,
                tenant_id=row.tenant_id,
                datasource_id=row.id,
                kind=row.kind,
            )
            return DatasourceView(
                id=row.id,
                tenant_id=row.tenant_id,
                name=row.name,
                kind=row.kind,  # type: ignore[arg-type]
                env=row.env,
                description=row.description,
                connection=dict(row.connection_json),
                credentials=credentials,
                tags=list(row.tags),
                enabled=bool(row.enabled),
            )

    # ------------------------------------------------------------------
    # Type metadata
    # ------------------------------------------------------------------

    def supported_types(self) -> list[dict[str, Any]]:
        """Return the supported-type metadata for ``GET /api/v1/datasources/types``."""
        return [
            {
                "kind": "postgresql",
                "label": "PostgreSQL",
                "supports_test": True,
                "supports_get_schema": True,
                "supports_preview": True,
            },
            {
                "kind": "mysql",
                "label": "MySQL",
                "supports_test": True,
                "supports_get_schema": True,
                "supports_preview": True,
            },
            {
                "kind": "oracle",
                "label": "Oracle",
                "supports_test": True,
                "supports_get_schema": True,
                "supports_preview": True,
            },
            {
                "kind": "hive",
                "label": "Apache Hive",
                "supports_test": True,
                "supports_get_schema": True,
                "supports_preview": True,
            },
            {
                "kind": "mongodb",
                "label": "MongoDB",
                "supports_test": True,
                "supports_get_schema": True,
                "supports_preview": True,
            },
            {
                "kind": "doris",
                "label": "Apache Doris",
                "supports_test": True,
                "supports_get_schema": True,
                "supports_preview": True,
            },
            {
                "kind": "kafka",
                "label": "Apache Kafka",
                "supports_test": True,
                "supports_get_schema": False,
                "supports_preview": False,
            },
        ]

    # ------------------------------------------------------------------
    # Event publishing
    # ------------------------------------------------------------------

    def _publish_event(
        self,
        *,
        tenant_id: str,
        event_type: str,
        payload: dict[str, Any],
    ) -> None:
        """Publish a Kafka event. Failures are logged, never raised.

        The audit / connection-test row is the durable source of
        truth; the Kafka event is the real-time contract for
        downstream consumers (the agent-gateway, the audit
        service, the data catalog). A Kafka outage must not roll
        back the SQL transaction, so :func:`publish_event` is
        wrapped in a ``try/except`` that swallows + logs.
        """
        import asyncio

        try:
            asyncio.run(
                publish_event(
                    topic="datasource.events.v1",
                    event_type=event_type,
                    payload=payload,
                    tenant_id=tenant_id,
                )
            )
        except Exception as exc:
            _LOG.warning(
                "failed to publish datasource event",
                extra={
                    "event_type": event_type,
                    "tenant_id": tenant_id,
                    "error": str(exc),
                },
            )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _truncate(value: str, *, limit: int = 1024) -> str:
    """Cap a string at *limit* characters with a trailing ellipsis marker."""
    if len(value) <= limit:
        return value
    return value[: limit - 3] + "..."


def _table_to_dict(table: TableInfo) -> dict[str, Any]:
    """Project a :class:`TableInfo` to the JSON-friendly wire shape.

    The shape mirrors the dataclass fields one-to-one; the
    :class:`aidp_datasource.services.schema_service.TableInfo`
    re-projection uses the same key names so the round-trip is
    loss-less. ``row_count_estimate`` is included so the
    agent-gateway can render the "≈ N rows" hint without a
    follow-up query.
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


def asyncio_run(coro: Any) -> Any:
    """Run a coroutine to completion.

    The service methods are called from synchronous FastAPI
    handlers (the brief spec). We use :func:`asyncio.run` for
    each coroutine so a transient ``RuntimeError: this event
    loop is already running`` does not leak between requests.
    The cost of spinning an event loop per request is small
    (microseconds) and keeps the API surface synchronous, which
    matches the platform convention.
    """
    import asyncio

    return asyncio.run(coro)


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------


_DEFAULT: DatasourceService | None = None


def default_datasource_service() -> DatasourceService:
    """Return the process-wide :class:`DatasourceService`."""
    global _DEFAULT
    if _DEFAULT is None:
        _DEFAULT = DatasourceService()
    return _DEFAULT


def set_default_datasource_service(service: DatasourceService | None) -> None:
    """Override the process-wide service (used by tests)."""
    global _DEFAULT
    _DEFAULT = service


__all__ = [
    "DatasourceService",
    "DatasourceView",
    "TestConnectionOutcome",
    "default_datasource_service",
    "set_default_datasource_service",
]
