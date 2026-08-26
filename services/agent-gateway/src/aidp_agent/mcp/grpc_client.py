"""Stub gRPC client for the datasource-service.

The MCP integration in :mod:`aidp_agent.mcp` needs to talk to the
datasource-service (Task 14 of this phase). That service speaks gRPC;
the agent-gateway's MCP tools are the *only* code path that needs
to call it.

This module defines:

- :class:`DatasourceClient` — the :class:`Protocol` the tools depend
  on. The contract is intentionally small (three async methods,
  one per MCP tool) so a future gRPC-backed implementation can be
  dropped in without touching the tools.
- :class:`StubDatasourceClient` — the Phase 1 implementation. It
  returns hard-coded data and carries a ``# TODO`` marker for the
  real gRPC channel. The stub is also the single source of
  truth for the in-process fixture the tests assert against, so
  flipping the implementation to a real channel does not change
  the tool's surface.

Why stub and not "real gRPC against a channel we have not generated
yet"? The datasource-service is not implemented yet (it is Task 14
of this phase). Generating a gRPC stub against protobufs we have
not authored would be wasted churn; the tools + their tests will
stand, and the gRPC call sites are a single file with explicit
``# TODO`` markers. When Task 14 lands, swapping the stub for the
real client is a contained, mechanical change.

Concurrency
-----------

The stub is read-only and stateless after construction — it is
safe to share across coroutines. The eventual gRPC client is
expected to be the same (a single channel per process).
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any, Protocol

# ---------------------------------------------------------------------------
# Public DTOs
# ---------------------------------------------------------------------------


#: The set of datasource kinds the platform recognises. The enum is
#: kept as a plain string-literal type (not a Python ``Enum``) so the
#: gRPC wire shape maps 1-to-1 without explicit conversion on either
#: side. Adding a new kind only requires a new string literal and a
#: corresponding entry in :data:`_STUB_FIXTURES`.
DatasourceKind = str


# Canonical kind values. Anything else is rejected at the client boundary.
KIND_POSTGRES: DatasourceKind = "postgres"
KIND_MYSQL: DatasourceKind = "mysql"
KIND_CLICKHOUSE: DatasourceKind = "clickhouse"
KIND_S3: DatasourceKind = "s3"
SUPPORTED_KINDS: frozenset[DatasourceKind] = frozenset(
    {KIND_POSTGRES, KIND_MYSQL, KIND_CLICKHOUSE, KIND_S3}
)


@dataclass(frozen=True)
class Datasource:
    """Full description of a single datasource.

    The shape mirrors what the eventual datasource-service's
    ``GetDatasource`` RPC will return. Sensitive fields (``password``,
    ``access_key``) are *not* included — the gRPC service must scrub
    them before responding, and the MCP layer never sees them in
    the first place.
    """

    id: str
    name: str
    kind: DatasourceKind
    host: str
    port: int
    database: str
    tenant_id: str
    description: str = ""
    # ``extra`` carries kind-specific knobs (e.g. ClickHouse ``?sslmode=``,
    # S3 region). It is intentionally a free-form dict to keep this
    # client decoupled from any specific datasource kind's config
    # schema.
    extra: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-ready dict of the public fields."""
        return {
            "id": self.id,
            "name": self.name,
            "kind": self.kind,
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "tenant_id": self.tenant_id,
            "description": self.description,
            "extra": dict(self.extra),
        }


@dataclass(frozen=True)
class DatasourceSummary:
    """Lightweight projection used by ``datasource.list``.

    A full :class:`Datasource` carries connection details that a
    list view does not need; the summary keeps the response small
    and avoids leaking any host/port/catalog names to a caller that
    only wants to discover what's registered.
    """

    id: str
    name: str
    kind: DatasourceKind
    tenant_id: str
    description: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-ready dict of the summary fields."""
        return {
            "id": self.id,
            "name": self.name,
            "kind": self.kind,
            "tenant_id": self.tenant_id,
            "description": self.description,
        }

    @classmethod
    def from_full(cls, ds: Datasource) -> DatasourceSummary:
        """Project a :class:`Datasource` to a :class:`DatasourceSummary`."""
        return cls(
            id=ds.id,
            name=ds.name,
            kind=ds.kind,
            tenant_id=ds.tenant_id,
            description=ds.description,
        )


@dataclass(frozen=True)
class TestConnectionOutcome:
    """Outcome of a ``datasource.test_connection`` probe.

    ``latency_ms`` is ``None`` when the probe failed before timing
    out (e.g. an authentication error). ``error`` is ``None`` on
    success.

    The ``__test__ = False`` marker opts this dataclass out of
    pytest's test-class collection. Pytest otherwise picks up
    any module-level class whose name starts with ``Test`` and
    tries to instantiate it, which fails for a frozen dataclass
    with required fields.
    """

    # Opt out of pytest's automatic test-class collection.
    # The attribute is the documented pytest hook (see the
    # pytest collection protocol).
    __test__ = False

    datasource_id: str
    ok: bool
    latency_ms: float | None = None
    error: str | None = None
    # A short, human-readable hint for the operator who reads the
    # MCP client log. Empty on success.
    hint: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-ready dict of the test result."""
        return {
            "datasource_id": self.datasource_id,
            "ok": self.ok,
            "latency_ms": self.latency_ms,
            "error": self.error,
            "hint": self.hint,
        }


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class DatasourceNotFoundError(LookupError):
    """Raised when the requested datasource id is unknown.

    The MCP layer translates this into a structured tool error so
    the caller sees a clean ``isError=true`` result instead of a
    5xx. The eventual gRPC client maps the upstream's ``NOT_FOUND``
    gRPC code to this exception.
    """

    def __init__(self, datasource_id: str) -> None:
        super().__init__(f"datasource not found: {datasource_id}")
        self.datasource_id = datasource_id


class DatasourceUnavailableError(RuntimeError):
    """Raised when the datasource-service itself is unreachable.

    Distinct from :class:`DatasourceNotFoundError`: here the RPC
    failed (network, deadline, 5xx), not the lookup. The MCP layer
    surfaces this as a structured tool error with a hint to retry.
    """


# ---------------------------------------------------------------------------
# Protocol
# ---------------------------------------------------------------------------


class DatasourceClient(Protocol):
    """The minimal contract the MCP tools depend on.

    A real gRPC-backed implementation only needs to satisfy this
    protocol. The Protocol is structural (no inheritance) so the
    eventual implementation can live in another module without
    coupling.
    """

    async def list_datasources(self, *, tenant_id: str | None = None) -> list[DatasourceSummary]:
        """List datasources, optionally scoped to *tenant_id*."""
        ...

    async def get_datasource(
        self, datasource_id: str, *, tenant_id: str | None = None
    ) -> Datasource:
        """Return the full :class:`Datasource` for *datasource_id*.

        Raises:
            DatasourceNotFoundError: when no datasource with the
                given id exists (or is invisible to *tenant_id*).
        """
        ...

    async def test_connection(
        self, datasource_id: str, *, tenant_id: str | None = None
    ) -> TestConnectionOutcome:
        """Open a probe connection to *datasource_id* and report.

        Never raises for connection-level failures (auth, timeout,
        ...): the failures are returned in
        :attr:`TestConnectionOutcome.error`. The only exception
        this method raises is :class:`DatasourceNotFoundError`.
        """
        ...


# ---------------------------------------------------------------------------
# Stub fixture data
# ---------------------------------------------------------------------------


#: In-process fixture for the stub. Three datasources, one per
#: supported kind that the agent will commonly see in production.
#: The ``tenant_id`` values let the tests exercise both the
#: tenant-scoped and tenant-agnostic code paths.
_STUB_FIXTURES: tuple[Datasource, ...] = (
    Datasource(
        id="ds-pg-001",
        name="Primary Postgres",
        kind=KIND_POSTGRES,
        host="pg-primary.internal",
        port=5432,
        database="aidp",
        tenant_id="tenant-a",
        description="Primary OLTP Postgres for tenant-a",
    ),
    Datasource(
        id="ds-ch-001",
        name="Warehouse ClickHouse",
        kind=KIND_CLICKHOUSE,
        host="ch.internal",
        port=9000,
        database="analytics",
        tenant_id="tenant-a",
        description="ClickHouse cluster for analytics workloads",
        extra={"sslmode": "require"},
    ),
    Datasource(
        id="ds-s3-001",
        name="Landing S3",
        kind=KIND_S3,
        host="s3://aidp-landing",
        port=443,
        database="landing",
        tenant_id="tenant-b",
        description="Landing zone bucket for tenant-b",
        extra={"region": "us-east-1"},
    ),
)


# ---------------------------------------------------------------------------
# Stub client
# ---------------------------------------------------------------------------


class StubDatasourceClient:
    """In-process stand-in for the gRPC datasource client.

    The stub:

    - Holds the :data:`_STUB_FIXTURES` set and answers lookups from
      it.
    - Simulates a small ``latency_ms`` on the test-connection probe
      so a caller that wants to display the timing in its UI gets
      a realistic value.
    - Returns :class:`DatasourceNotFoundError` for unknown ids.

    When the real gRPC client lands, this class is replaced by a
    thin wrapper around the generated stub. The class-level
    ``__all__`` is intentionally not changed so the import surface
    stays stable.
    """

    #: Latency in milliseconds reported by the stub for a
    #: successful connection probe. Picked to be small but non-zero
    #: so the field is observably present in the response.
    _STUB_OK_LATENCY_MS: float = 12.5

    def __init__(
        self,
        fixtures: Iterable[Datasource] | None = None,
    ) -> None:
        # ``list(...)`` materialises the iterable; ``_STUB_FIXTURES``
        # is already a tuple so this is just a copy in the default
        # case. The copy matters when a test passes a custom
        # iterable — we do not want to share state with the caller.
        self._by_id: dict[str, Datasource] = {
            ds.id: ds for ds in (fixtures if fixtures is not None else _STUB_FIXTURES)
        }

    # ------------------------------------------------------------------
    # CRUD helpers (test-only — keep small)
    # ------------------------------------------------------------------

    def upsert(self, datasource: Datasource) -> None:
        """Insert or replace a fixture entry. Intended for tests."""
        self._by_id[datasource.id] = datasource

    def remove(self, datasource_id: str) -> None:
        """Drop a fixture entry. Intended for tests."""
        self._by_id.pop(datasource_id, None)

    def all(self) -> list[Datasource]:
        """Return every fixture entry. Intended for tests."""
        return list(self._by_id.values())

    # ------------------------------------------------------------------
    # DatasourceClient protocol
    # ------------------------------------------------------------------

    async def list_datasources(self, *, tenant_id: str | None = None) -> list[DatasourceSummary]:
        # Materialise the values view once; we may iterate it
        # again below when filtering.
        rows: list[Datasource] = list(self._by_id.values())
        if tenant_id is not None:
            rows = [ds for ds in rows if ds.tenant_id == tenant_id]
        return [DatasourceSummary.from_full(ds) for ds in rows]

    async def get_datasource(
        self, datasource_id: str, *, tenant_id: str | None = None
    ) -> Datasource:
        ds = self._by_id.get(datasource_id)
        if ds is None:
            raise DatasourceNotFoundError(datasource_id)
        if tenant_id is not None and ds.tenant_id != tenant_id:
            # Treat cross-tenant lookups as "not found" so we do
            # not leak the existence of another tenant's data.
            raise DatasourceNotFoundError(datasource_id)
        return ds

    async def test_connection(
        self, datasource_id: str, *, tenant_id: str | None = None
    ) -> TestConnectionOutcome:
        # We resolve the datasource so an unknown id still raises
        # ``DatasourceNotFoundError`` (the gRPC client behaviour we
        # are committing to).
        ds = await self.get_datasource(datasource_id, tenant_id=tenant_id)
        # The stub *always* succeeds; the real client will do
        # the actual probe. We keep the latency deterministic so
        # tests can pin it.
        return TestConnectionOutcome(
            datasource_id=ds.id,
            ok=True,
            latency_ms=self._STUB_OK_LATENCY_MS,
            error=None,
            hint="",
        )


# ---------------------------------------------------------------------------
# Build helper
# ---------------------------------------------------------------------------


def build_default_datasource_client() -> DatasourceClient:
    """Return the default :class:`DatasourceClient` for the process.

    This is the single seam the lifespan / tests use to obtain a
    client. Today it returns a :class:`StubDatasourceClient`; when
    the gRPC channel is ready the implementation here is the only
    line that changes.
    """
    return StubDatasourceClient()


__all__ = [
    "KIND_CLICKHOUSE",
    "KIND_MYSQL",
    "KIND_POSTGRES",
    "KIND_S3",
    "SUPPORTED_KINDS",
    "Datasource",
    "DatasourceClient",
    "DatasourceKind",
    "DatasourceNotFoundError",
    "DatasourceSummary",
    "DatasourceUnavailableError",
    "StubDatasourceClient",
    "TestConnectionOutcome",
    "build_default_datasource_client",
]


# ---------------------------------------------------------------------------
# TODO(gRPC): replace ``StubDatasourceClient`` with a real gRPC client
#
# When datasource-service (Task 14) lands, this module will gain a
# ``GrpcDatasourceClient`` class that:
#
#   1. Opens a single ``grpc.aio.insecure_channel`` against
#      ``AIDP_DATASOURCE_GRPC_URL`` (default ``datasource-service:8005``).
#   2. Delegates to the generated ``DatasourceServiceStub`` (proto
#      package ``aidp.datasource.v1``).
#   3. Maps ``grpc.RpcError`` to :class:`DatasourceUnavailableError`
#      (for ``UNAVAILABLE`` / ``DEADLINE_EXCEEDED`` /
#      ``INTERNAL``) and to :class:`DatasourceNotFoundError` (for
#      ``NOT_FOUND``).
#
# The contract above is small enough that the migration should be
# a single-file change; the MCP tools do not need to move.
# ---------------------------------------------------------------------------
