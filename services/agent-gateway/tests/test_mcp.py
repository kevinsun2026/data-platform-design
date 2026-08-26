"""Tests for the Agent Gateway MCP integration.

The MCP module exposes:

- ``GET  /mcp/sse``         — the official MCP SSE transport.
- ``POST /mcp/messages/``   — the MCP message endpoint (client
  posts JSON-RPC requests here).
- ``POST /mcp/tools/call``  — a single-shot JSON-RPC shortcut
  that bypasses the SSE channel.
- ``GET  /mcp/tools``       — a non-MCP list endpoint for
  operator / test discovery.

The tests cover three layers:

1. **gRPC client stub** (:mod:`aidp_agent.mcp.grpc_client`) —
   pin the in-process fixture and the cross-tenant isolation
   contract.
2. **Tool functions** (:mod:`aidp_agent.mcp.tools.datasource`) —
   pin the success envelopes, the ``ToolError`` subclasses, and
   the input validation.
3. **HTTP + SSE integration** — pin the FastAPI routes, the
   JSON-RPC envelope on the ``/mcp/tools/call`` shortcut, and
   the SSE initial-event payload.

The SSE test bypasses :class:`fastapi.testclient.TestClient`
(which has no streaming timeout) and uses a raw TCP socket
against an in-process uvicorn instance. Everything else uses
the standard TestClient pattern from the rest of the agent
gateway suite.
"""

from __future__ import annotations

import asyncio
import os
import socket
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import pytest
from aidp_agent.mcp.grpc_client import (
    KIND_CLICKHOUSE,
    KIND_POSTGRES,
    KIND_S3,
    SUPPORTED_KINDS,
    Datasource,
    DatasourceClient,
    DatasourceNotFoundError,
    DatasourceSummary,
    DatasourceUnavailableError,
    StubDatasourceClient,
    TestConnectionOutcome,
    build_default_datasource_client,
)
from aidp_agent.mcp.server import (
    MCPServerConfig,
    build_default_server,
    build_mcp_router,
    build_sse_starlette_app,
    get_datasource_client,
    get_mcp_server,
    set_datasource_client,
    set_mcp_server,
)
from aidp_agent.mcp.tools.datasource import (
    TOOL_REGISTRY,
    DatasourceUnavailableToolError,
    InvalidArgumentToolError,
    ToolNotFoundError,
    _validate_datasource_id,
    datasource_get,
    datasource_list,
    datasource_test_connection,
)
from fastapi import FastAPI
from fastapi.testclient import TestClient

# ---------------------------------------------------------------------------
# Helper: build a fresh FastAPI app with the MCP surface mounted
# ---------------------------------------------------------------------------


@contextmanager
def make_mcp_app(
    *,
    relax_transport: bool = True,
) -> Iterator[tuple[FastAPI, TestClient]]:
    """Yield a ``(FastAPI, TestClient)`` pair with the MCP surface mounted.

    The factory mirrors :func:`aidp_agent.main.create_app` but
    skips the metering worker, the provider-registry warm-up,
    and the long-lived asyncio state so each test gets a
    pristine app. The ``AIDP_AGENT_MCP_RELAX_TRANSPORT`` env
    var controls whether the SDK's DNS-rebinding protection is
    active; tests set it to a relaxed mode so the
    ``testserver`` host used by ``TestClient`` is allowed.
    """
    if relax_transport:
        os.environ["AIDP_AGENT_MCP_RELAX_TRANSPORT"] = "1"
    else:
        os.environ.pop("AIDP_AGENT_MCP_RELAX_TRANSPORT", None)

    # Reset the MCP singletons so a stale server from a prior
    # test does not leak in.
    set_datasource_client(None)
    set_mcp_server(None)

    # We import ``create_app`` lazily so the env var is in
    # place before the MCP module is loaded.
    from aidp_agent.main import create_app  # local import

    app = create_app()
    client = TestClient(app)
    try:
        yield app, client
    finally:
        # Reset again so a subsequent test does not see our
        # overrides.
        set_datasource_client(None)
        set_mcp_server(None)
        os.environ.pop("AIDP_AGENT_MCP_RELAX_TRANSPORT", None)


# ---------------------------------------------------------------------------
# gRPC client stub tests
# ---------------------------------------------------------------------------


class TestStubDatasourceClient:
    """Pin the in-process stub's behaviour.

    The stub is the single source of truth the tools and HTTP
    endpoints read from in Phase 1. When the gRPC channel
    lands, these tests will run against the real client (with
    a fixture swap), and the tool/HTTP tests above this layer
    do not need to change.
    """

    def test_default_fixture_has_three_datasources(self) -> None:
        """The default fixture ships three datasources across the supported kinds."""
        client = StubDatasourceClient()
        all_rows = client.all()
        kinds = {ds.kind for ds in all_rows}
        assert kinds == set(SUPPORTED_KINDS) - {"mysql"}
        # ``mysql`` is in the supported kinds but not in the
        # default fixture (none of the production instances
        # run MySQL); a deployment that needs MySQL just adds
        # a row via :meth:`upsert`.
        assert any(ds.kind == KIND_POSTGRES for ds in all_rows)
        assert any(ds.kind == KIND_CLICKHOUSE for ds in all_rows)
        assert any(ds.kind == KIND_S3 for ds in all_rows)

    @pytest.mark.asyncio
    async def test_list_datasources_returns_summaries(self) -> None:
        """``list_datasources`` returns summary entries, not full datasource rows."""
        client = StubDatasourceClient()
        items = await client.list_datasources()
        assert all(isinstance(item, DatasourceSummary) for item in items)
        # The summary must not leak connection details.
        for item in items:
            d = item.to_dict()
            assert "host" not in d
            assert "port" not in d
            assert "database" not in d

    @pytest.mark.asyncio
    async def test_list_datasources_tenant_scoped(self) -> None:
        """``tenant_id`` filters out other tenants' datasources."""
        client = StubDatasourceClient()
        tenant_a = await client.list_datasources(tenant_id="tenant-a")
        tenant_b = await client.list_datasources(tenant_id="tenant-b")
        assert all(item.tenant_id == "tenant-a" for item in tenant_a)
        assert all(item.tenant_id == "tenant-b" for item in tenant_b)
        # The two tenants see disjoint rows.
        assert {item.id for item in tenant_a} & {item.id for item in tenant_b} == set()

    @pytest.mark.asyncio
    async def test_list_datasources_unknown_tenant_is_empty(self) -> None:
        """An unknown tenant id returns an empty list (not an error)."""
        client = StubDatasourceClient()
        items = await client.list_datasources(tenant_id="tenant-zzz")
        assert items == []

    @pytest.mark.asyncio
    async def test_get_datasource_returns_full_row(self) -> None:
        """``get_datasource`` returns the full ``Datasource`` with connection details."""
        client = StubDatasourceClient()
        ds = await client.get_datasource("ds-pg-001")
        assert isinstance(ds, Datasource)
        assert ds.id == "ds-pg-001"
        assert ds.host == "pg-primary.internal"
        assert ds.port == 5432
        assert ds.database == "aidp"

    @pytest.mark.asyncio
    async def test_get_datasource_unknown_raises_not_found(self) -> None:
        """An unknown id raises ``DatasourceNotFoundError``."""
        client = StubDatasourceClient()
        with pytest.raises(DatasourceNotFoundError) as excinfo:
            await client.get_datasource("does-not-exist")
        assert excinfo.value.datasource_id == "does-not-exist"

    @pytest.mark.asyncio
    async def test_get_datasource_cross_tenant_raises_not_found(self) -> None:
        """A cross-tenant lookup raises ``DatasourceNotFoundError`` (no existence leak)."""
        client = StubDatasourceClient()
        with pytest.raises(DatasourceNotFoundError):
            await client.get_datasource("ds-pg-001", tenant_id="tenant-b")

    @pytest.mark.asyncio
    async def test_test_connection_returns_ok_for_known_id(self) -> None:
        """``test_connection`` returns a successful ``TestConnectionOutcome``."""
        client = StubDatasourceClient()
        result = await client.test_connection("ds-pg-001")
        assert isinstance(result, TestConnectionOutcome)
        assert result.ok is True
        assert result.error is None
        assert result.latency_ms is not None
        assert result.latency_ms > 0

    @pytest.mark.asyncio
    async def test_test_connection_unknown_raises_not_found(self) -> None:
        """``test_connection`` propagates ``DatasourceNotFoundError`` for an unknown id."""
        client = StubDatasourceClient()
        with pytest.raises(DatasourceNotFoundError):
            await client.test_connection("does-not-exist")

    def test_upsert_and_remove_round_trip(self) -> None:
        """``upsert`` adds a new row; ``remove`` drops it."""
        client = StubDatasourceClient()
        client.upsert(
            Datasource(
                id="ds-test",
                name="Test",
                kind=KIND_POSTGRES,
                host="localhost",
                port=5432,
                database="db",
                tenant_id="tenant-x",
            )
        )
        assert any(ds.id == "ds-test" for ds in client.all())
        client.remove("ds-test")
        assert not any(ds.id == "ds-test" for ds in client.all())

    def test_build_default_datasource_client_returns_stub(self) -> None:
        """The default factory returns a stub today; a future change here is the gRPC seam."""
        client = build_default_datasource_client()
        assert isinstance(client, StubDatasourceClient)
        # ``DatasourceClient`` is a Protocol; the structural check
        # is a runtime callable check.
        assert hasattr(client, "list_datasources")
        assert hasattr(client, "get_datasource")
        assert hasattr(client, "test_connection")

    def test_datasource_summary_from_full_strips_connection_details(self) -> None:
        """``DatasourceSummary.from_full`` projects away connection details."""
        full = Datasource(
            id="x",
            name="x",
            kind=KIND_POSTGRES,
            host="h",
            port=1,
            database="d",
            tenant_id="t",
        )
        summary = DatasourceSummary.from_full(full)
        d = summary.to_dict()
        assert d == {
            "id": "x",
            "name": "x",
            "kind": "postgres",
            "tenant_id": "t",
            "description": "",
        }


# ---------------------------------------------------------------------------
# Tool function tests
# ---------------------------------------------------------------------------


@pytest.fixture
def client() -> StubDatasourceClient:
    """Return a fresh :class:`StubDatasourceClient` per test."""
    return StubDatasourceClient()


class TestDatasourceListTool:
    """Pin the ``datasource.list`` tool function."""

    @pytest.mark.asyncio
    async def test_returns_data_with_count_and_items(self, client: StubDatasourceClient) -> None:
        result = await datasource_list(client)
        assert "datasources" in result
        assert "count" in result
        assert result["count"] == len(client.all())
        assert len(result["datasources"]) == result["count"]

    @pytest.mark.asyncio
    async def test_tenant_filter_narrows_results(self, client: StubDatasourceClient) -> None:
        result = await datasource_list(client, tenant_id="tenant-a")
        assert all(item["tenant_id"] == "tenant-a" for item in result["datasources"])

    @pytest.mark.asyncio
    async def test_unavailable_client_raises_datasource_unavailable(
        self, client: StubDatasourceClient
    ) -> None:
        """An unavailable upstream raises ``DatasourceUnavailableToolError``."""

        class _DownClient(DatasourceClient):
            async def list_datasources(
                self, *, tenant_id: str | None = None
            ) -> list[DatasourceSummary]:
                raise DatasourceUnavailableError("connection refused")

            async def get_datasource(
                self, datasource_id: str, *, tenant_id: str | None = None
            ) -> Datasource:
                raise DatasourceUnavailableError("connection refused")

            async def test_connection(
                self, datasource_id: str, *, tenant_id: str | None = None
            ) -> TestConnectionOutcome:
                raise DatasourceUnavailableError("connection refused")

        with pytest.raises(DatasourceUnavailableToolError) as excinfo:
            await datasource_list(_DownClient())
        assert excinfo.value.code == "DATASOURCE_UNAVAILABLE"


class TestDatasourceGetTool:
    """Pin the ``datasource.get`` tool function."""

    @pytest.mark.asyncio
    async def test_returns_datasource(self, client: StubDatasourceClient) -> None:
        result = await datasource_get(client, "ds-pg-001")
        assert result["datasource"]["id"] == "ds-pg-001"
        assert result["datasource"]["host"] == "pg-primary.internal"

    @pytest.mark.asyncio
    async def test_unknown_id_raises_tool_not_found(self, client: StubDatasourceClient) -> None:
        with pytest.raises(ToolNotFoundError) as excinfo:
            await datasource_get(client, "does-not-exist")
        assert excinfo.value.code == "NOT_FOUND"

    @pytest.mark.asyncio
    async def test_cross_tenant_raises_tool_not_found(self, client: StubDatasourceClient) -> None:
        with pytest.raises(ToolNotFoundError):
            await datasource_get(client, "ds-pg-001", tenant_id="tenant-b")

    @pytest.mark.asyncio
    async def test_empty_id_is_rejected(self, client: StubDatasourceClient) -> None:
        with pytest.raises(InvalidArgumentToolError):
            await datasource_get(client, "")

    @pytest.mark.asyncio
    async def test_whitespace_id_is_rejected(self, client: StubDatasourceClient) -> None:
        with pytest.raises(InvalidArgumentToolError):
            await datasource_get(client, "   ")

    @pytest.mark.asyncio
    async def test_non_string_id_is_rejected(self, client: StubDatasourceClient) -> None:
        with pytest.raises(InvalidArgumentToolError):
            await datasource_get(client, 123)  # type: ignore[arg-type]

    @pytest.mark.asyncio
    async def test_too_long_id_is_rejected(self, client: StubDatasourceClient) -> None:
        with pytest.raises(InvalidArgumentToolError):
            await datasource_get(client, "x" * 200)

    @pytest.mark.asyncio
    async def test_unavailable_client_raises(self, client: StubDatasourceClient) -> None:
        """A down upstream raises ``DatasourceUnavailableToolError``."""

        class _DownClient(DatasourceClient):
            async def list_datasources(
                self, *, tenant_id: str | None = None
            ) -> list[DatasourceSummary]:
                return []

            async def get_datasource(
                self, datasource_id: str, *, tenant_id: str | None = None
            ) -> Datasource:
                raise DatasourceUnavailableError("connection refused")

            async def test_connection(
                self, datasource_id: str, *, tenant_id: str | None = None
            ) -> TestConnectionOutcome:
                raise DatasourceUnavailableError("connection refused")

        with pytest.raises(DatasourceUnavailableToolError):
            await datasource_get(_DownClient(), "ds-pg-001")


class TestDatasourceTestConnectionTool:
    """Pin the ``datasource.test_connection`` tool function."""

    @pytest.mark.asyncio
    async def test_returns_test_result(self, client: StubDatasourceClient) -> None:
        result = await datasource_test_connection(client, "ds-pg-001")
        assert result["ok"] is True
        assert result["datasource_id"] == "ds-pg-001"
        assert result["latency_ms"] is not None

    @pytest.mark.asyncio
    async def test_unknown_id_raises_tool_not_found(self, client: StubDatasourceClient) -> None:
        with pytest.raises(ToolNotFoundError):
            await datasource_test_connection(client, "does-not-exist")

    @pytest.mark.asyncio
    async def test_empty_id_raises_invalid_argument(self, client: StubDatasourceClient) -> None:
        with pytest.raises(InvalidArgumentToolError):
            await datasource_test_connection(client, "")

    @pytest.mark.asyncio
    async def test_unavailable_client_raises(self, client: StubDatasourceClient) -> None:
        """A down upstream raises ``DatasourceUnavailableToolError``."""

        class _DownClient(DatasourceClient):
            async def list_datasources(
                self, *, tenant_id: str | None = None
            ) -> list[DatasourceSummary]:
                return []

            async def get_datasource(
                self, datasource_id: str, *, tenant_id: str | None = None
            ) -> Datasource:
                raise DatasourceUnavailableError("connection refused")

            async def test_connection(
                self, datasource_id: str, *, tenant_id: str | None = None
            ) -> TestConnectionOutcome:
                raise DatasourceUnavailableError("connection refused")

        with pytest.raises(DatasourceUnavailableToolError):
            await datasource_test_connection(_DownClient(), "ds-pg-001")


class TestToolErrorCodes:
    """Pin the ``code`` field of each tool-error subclass."""

    def test_invalid_argument_code(self) -> None:
        assert InvalidArgumentToolError("x").code == "INVALID_ARGUMENT"

    def test_tool_not_found_code(self) -> None:
        assert ToolNotFoundError("x").code == "NOT_FOUND"

    def test_datasource_unavailable_code(self) -> None:
        assert DatasourceUnavailableToolError("x").code == "DATASOURCE_UNAVAILABLE"


class TestValidateDatasourceId:
    """Pin the argument-validation helper."""

    def test_valid_id_round_trips(self) -> None:
        assert _validate_datasource_id("ds-1") == "ds-1"

    def test_strips_whitespace(self) -> None:
        assert _validate_datasource_id("  ds-1  ") == "ds-1"

    @pytest.mark.parametrize("bad", [None, 0, 1.5, [], {}, True])
    def test_non_string_raises(self, bad: object) -> None:
        with pytest.raises(InvalidArgumentToolError):
            _validate_datasource_id(bad)

    @pytest.mark.parametrize("bad", ["", "   ", "\n", "\t"])
    def test_empty_or_whitespace_raises(self, bad: str) -> None:
        with pytest.raises(InvalidArgumentToolError):
            _validate_datasource_id(bad)


class TestToolRegistry:
    """Pin the canonical tool-name -> function mapping."""

    def test_all_three_tools_are_registered(self) -> None:
        assert set(TOOL_REGISTRY) == {
            "datasource.list",
            "datasource.get",
            "datasource.test_connection",
        }

    def test_registry_resolves_to_real_functions(self) -> None:
        assert TOOL_REGISTRY["datasource.list"] is datasource_list
        assert TOOL_REGISTRY["datasource.get"] is datasource_get
        assert TOOL_REGISTRY["datasource.test_connection"] is datasource_test_connection


# ---------------------------------------------------------------------------
# Server factory + singleton tests
# ---------------------------------------------------------------------------


class TestBuildDefaultServer:
    """Pin the server factory's contract."""

    def test_default_server_uses_stub_client(self) -> None:
        server = build_default_server()
        # The default factory injects a stub client; pin the
        # type so a future swap to a gRPC client is a
        # visible change.
        assert server.name == "aidp-agent-mcp"

    def test_custom_client_is_used(self) -> None:
        stub = StubDatasourceClient()
        # Passing a custom client must not raise; the server
        # is built with the three tools registered.
        server = build_default_server(client=stub)
        assert server is not None

    def test_custom_config_overrides_defaults(self) -> None:
        cfg = MCPServerConfig(name="custom", version="9.9.9")
        server = build_default_server(config=cfg)
        assert server.name == "custom"
        assert server.version == "9.9.9"


class TestModuleLevelSingletons:
    """Pin the module-level-singleton-with-overrides pattern."""

    def test_set_datasource_client_round_trips(self) -> None:
        sentinel = StubDatasourceClient()
        set_datasource_client(sentinel)
        try:
            assert get_datasource_client() is sentinel
        finally:
            set_datasource_client(None)

    def test_get_datasource_client_falls_back_to_default(self) -> None:
        set_datasource_client(None)
        client = get_datasource_client()
        # The default is a fresh stub (factory creates new
        # each call).
        assert isinstance(client, StubDatasourceClient)

    def test_set_mcp_server_round_trips(self) -> None:
        server = build_default_server()
        set_mcp_server(server)
        try:
            assert get_mcp_server() is server
        finally:
            set_mcp_server(None)

    def test_get_mcp_server_falls_back_to_default(self) -> None:
        set_mcp_server(None)
        server = get_mcp_server()
        assert server is not None
        assert server.name == "aidp-agent-mcp"


# ---------------------------------------------------------------------------
# HTTP endpoint tests
# ---------------------------------------------------------------------------


class TestMcpToolsListEndpoint:
    """Pin the ``GET /mcp/tools`` operator endpoint."""

    def test_lists_three_tools(self) -> None:
        with make_mcp_app() as (_app, client):
            response = client.get("/mcp/tools")
            assert response.status_code == 200
            body = response.json()
            assert body["count"] == 3
            names = {t["name"] for t in body["tools"]}
            assert names == {
                "datasource.list",
                "datasource.get",
                "datasource.test_connection",
            }
            for entry in body["tools"]:
                assert "name" in entry
                assert "title" in entry
                assert "description" in entry


class TestMcpToolsCallEndpoint:
    """Pin the ``POST /mcp/tools/call`` JSON-RPC shortcut."""

    def test_datasource_list_returns_payload(self) -> None:
        with make_mcp_app() as (_app, client):
            body = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": "datasource.list", "arguments": {}},
            }
            response = client.post("/mcp/tools/call", json=body)
            assert response.status_code == 200
            data = response.json()
            assert data["jsonrpc"] == "2.0"
            assert data["id"] == 1
            result = data["result"]
            assert result["is_error"] is False
            payload = result["payload"]
            assert payload["count"] == 3
            assert len(payload["datasources"]) == 3

    def test_datasource_get_returns_payload(self) -> None:
        with make_mcp_app() as (_app, client):
            body = {
                "method": "tools/call",
                "params": {
                    "name": "datasource.get",
                    "arguments": {"datasource_id": "ds-pg-001"},
                },
            }
            response = client.post("/mcp/tools/call", json=body)
            assert response.status_code == 200
            data = response.json()
            result = data["result"]
            assert result["is_error"] is False
            assert result["payload"]["datasource"]["id"] == "ds-pg-001"
            assert result["payload"]["datasource"]["host"] == "pg-primary.internal"

    def test_datasource_test_connection_returns_payload(self) -> None:
        with make_mcp_app() as (_app, client):
            body = {
                "method": "tools/call",
                "params": {
                    "name": "datasource.test_connection",
                    "arguments": {"datasource_id": "ds-pg-001"},
                },
            }
            response = client.post("/mcp/tools/call", json=body)
            assert response.status_code == 200
            data = response.json()
            result = data["result"]
            assert result["is_error"] is False
            assert result["payload"]["ok"] is True

    def test_unknown_tool_returns_tool_not_found_error(self) -> None:
        with make_mcp_app() as (_app, client):
            body = {
                "method": "tools/call",
                "params": {"name": "datasource.bogus", "arguments": {}},
            }
            response = client.post("/mcp/tools/call", json=body)
            assert response.status_code == 404
            data = response.json()
            assert data["error"]["code"] == "TOOL_NOT_FOUND"
            assert "known_tools" in data["error"]["extra"]
            assert "datasource.list" in data["error"]["extra"]["known_tools"]

    def test_unknown_id_returns_is_error_result(self) -> None:
        """A tool-level ``NOT_FOUND`` is a JSON-RPC ``result`` with ``is_error=true``."""
        with make_mcp_app() as (_app, client):
            body = {
                "method": "tools/call",
                "params": {
                    "name": "datasource.get",
                    "arguments": {"datasource_id": "nope"},
                },
            }
            response = client.post("/mcp/tools/call", json=body)
            assert response.status_code == 200
            data = response.json()
            assert data["result"]["is_error"] is True
            assert "nope" in (data["result"].get("raw_text") or "")

    def test_empty_id_returns_is_error_result(self) -> None:
        """A tool-level ``INVALID_ARGUMENT`` surfaces as ``is_error=true`` result."""
        with make_mcp_app() as (_app, client):
            body = {
                "method": "tools/call",
                "params": {
                    "name": "datasource.get",
                    "arguments": {"datasource_id": ""},
                },
            }
            response = client.post("/mcp/tools/call", json=body)
            assert response.status_code == 200
            data = response.json()
            assert data["result"]["is_error"] is True

    def test_missing_id_returns_is_error_result(self) -> None:
        """The SDK's Pydantic-level validation surfaces as ``is_error=true``."""
        with make_mcp_app() as (_app, client):
            body = {
                "method": "tools/call",
                "params": {"name": "datasource.get", "arguments": {}},
            }
            response = client.post("/mcp/tools/call", json=body)
            assert response.status_code == 200
            data = response.json()
            assert data["result"]["is_error"] is True

    def test_bad_method_returns_method_not_found(self) -> None:
        with make_mcp_app() as (_app, client):
            body = {"method": "foo", "params": {}}
            response = client.post("/mcp/tools/call", json=body)
            assert response.status_code == 400
            data = response.json()
            assert data["error"]["code"] == "METHOD_NOT_FOUND"

    def test_missing_name_returns_invalid_argument(self) -> None:
        with make_mcp_app() as (_app, client):
            body = {"method": "tools/call", "params": {}}
            response = client.post("/mcp/tools/call", json=body)
            assert response.status_code == 400
            data = response.json()
            assert data["error"]["code"] == "INVALID_ARGUMENT"

    def test_non_object_arguments_returns_invalid_argument(self) -> None:
        with make_mcp_app() as (_app, client):
            body = {
                "method": "tools/call",
                "params": {"name": "datasource.list", "arguments": "bad"},
            }
            response = client.post("/mcp/tools/call", json=body)
            assert response.status_code == 400
            data = response.json()
            assert data["error"]["code"] == "INVALID_ARGUMENT"

    def test_json_rpc_id_is_echoed(self) -> None:
        with make_mcp_app() as (_app, client):
            body = {
                "id": "req-42",
                "method": "tools/call",
                "params": {"name": "datasource.list", "arguments": {}},
            }
            response = client.post("/mcp/tools/call", json=body)
            assert response.json()["id"] == "req-42"

    def test_extra_top_level_fields_are_ignored(self) -> None:
        """The request model is permissive on extras (Pydantic ``extra='ignore'``)."""
        with make_mcp_app() as (_app, client):
            body = {
                "method": "tools/call",
                "params": {"name": "datasource.list", "arguments": {}},
                "future_extension": {"x": 1},
            }
            response = client.post("/mcp/tools/call", json=body)
            assert response.status_code == 200


# ---------------------------------------------------------------------------
# SSE transport test (raw socket, no TestClient)
# ---------------------------------------------------------------------------


class TestSseTransport:
    """Pin the SSE transport endpoint's wire shape.

    The MCP SDK's SSE handler keeps the connection open
    indefinitely (waiting for messages to push), so the
    FastAPI :class:`TestClient` (which lacks a streaming
    timeout) cannot drive it cleanly. We spin up a real
    in-process uvicorn instance and read the initial bytes
    over a raw socket.
    """

    def _run_async(self, coro: Any) -> Any:
        """Run a coroutine in a fresh event loop (sync entry-point helper)."""
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    def _spawn_server(self) -> tuple[Any, int]:
        """Start an in-process uvicorn server bound to an ephemeral port.

        Returns ``(server, port)``. The caller must stop the
        server with ``server.should_exit = True`` and await
        the serve task to release the port.
        """
        from uvicorn import Config, Server

        # ``bind`` to port 0 so the OS picks a free port.
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        sock.close()

        from aidp_agent.main import create_app

        os.environ["AIDP_AGENT_MCP_RELAX_TRANSPORT"] = "1"
        set_datasource_client(None)
        set_mcp_server(None)
        app = create_app()

        config = Config(
            app,
            host="127.0.0.1",
            port=port,
            log_level="warning",
            access_log=False,
            lifespan="on",
        )
        server = Server(config)
        return server, port

    def test_sse_endpoint_returns_event_stream_with_endpoint_event(self) -> None:
        server, port = self._spawn_server()
        serve_task: asyncio.Task[None] | None = None

        async def driver() -> None:
            nonlocal serve_task
            serve_task = asyncio.create_task(server.serve())
            # Wait for uvicorn to bind the port.
            for _ in range(50):
                if server.started:
                    break
                await asyncio.sleep(0.05)

            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            # Use HTTP/1.0 so the response is *not* chunked
            # (the body is then read as one contiguous stream,
            # not a series of length-prefixed chunks). The
            # server uses the Connection: close convention to
            # bound the body.
            writer.write(
                b"GET /mcp/sse HTTP/1.0\r\nHost: 127.0.0.1\r\nAccept: text/event-stream\r\n\r\n"
            )
            await writer.drain()

            # Read status line and headers until the blank line.
            status_seen = False
            content_type_seen = False
            for _ in range(30):
                line = await asyncio.wait_for(reader.readline(), timeout=5.0)
                decoded = line.decode("latin-1").rstrip("\r\n")
                if decoded == "":
                    break
                if decoded.startswith("HTTP/"):
                    assert decoded.startswith("HTTP/1."), decoded
                    assert " 200 " in decoded, decoded
                    status_seen = True
                elif decoded.lower().startswith("content-type:"):
                    ct = decoded.split(":", 1)[1].strip().lower()
                    assert ct.startswith("text/event-stream"), ct
                    content_type_seen = True
            assert status_seen
            assert content_type_seen

            # Read the first SSE event. We just look for the
            # ``event: endpoint`` line followed by a ``data:``
            # line that contains the SDK's message URL.
            buf = b""
            while True:
                chunk = await asyncio.wait_for(reader.read(64), timeout=2.0)
                if not chunk:
                    break
                buf += chunk
                if b"event: endpoint" in buf and b"data: " in buf:
                    break
                if len(buf) > 4096:
                    break  # pragma: no cover - defensive

            text = buf.decode("latin-1", errors="replace")
            assert "event: endpoint" in text
            # The data line carries the SDK's message URL with
            # a session_id query parameter.
            for event_line in text.splitlines():
                if event_line.startswith("data: "):
                    data_value = event_line[6:].rstrip("\r\n")
                    assert "/mcp/messages/" in data_value
                    assert "session_id=" in data_value
                    break
            else:  # pragma: no cover - defensive
                raise AssertionError("no data: line in SSE stream")

            writer.close()
            await writer.wait_closed()

        try:
            self._run_async(driver())
        finally:
            server.should_exit = True
            if serve_task is not None:
                self._run_async(_await_task(serve_task))


async def _await_task(task: asyncio.Task[None]) -> None:
    """Await *task* in its own event loop (sync helper)."""
    import contextlib

    with contextlib.suppress(Exception):  # pragma: no cover - shutdown is best-effort
        await task


# ---------------------------------------------------------------------------
# Mount-point tests (verify the wiring in main.py)
# ---------------------------------------------------------------------------


class TestMcpMount:
    """Pin the FastAPI mount structure created by ``_mount_mcp``."""

    def _all_paths(self, app: FastAPI) -> set[str]:
        """Return every routable path on *app* (recursing into mounted sub-apps)."""
        paths: set[str] = set()
        # ``app.routes`` is a list of Route / APIRoute / Mount /
        # _IncludedRouter. The exact attribute layout differs:
        #
        # - ``Route`` / ``APIRoute`` carry ``.path`` (no children).
        # - ``Mount`` carries ``.path`` and ``.app`` (a sub-app
        #   with its own ``.routes``).
        # - ``_IncludedRouter`` exposes ``.path = None`` and
        #   ``.original_router`` (an :class:`APIRouter` whose
        #   ``.routes`` already carry the full URL because the
        #   prefix was baked in at include time).
        #
        # We walk recursively with a stack so the order is
        # deterministic. The walker therefore must *not*
        # re-prepend the parent's prefix for included routers
        # — the children are walked with the *current* prefix
        # unchanged.
        stack: list[tuple[Any, str]] = [(app, "")]
        while stack:
            obj, prefix = stack.pop()
            for route in getattr(obj, "routes", []):
                path = getattr(route, "path", None)
                if isinstance(path, str):
                    paths.add(prefix + path)
                inner_prefix = prefix + (path if isinstance(path, str) else "")
                # FastAPI's :class:`_IncludedRouter` is the
                # wrapper produced by ``app.include_router``.
                # It does not expose a ``.routes`` attribute
                # directly; recurse into the original router
                # (whose children already carry the full URL).
                if hasattr(route, "original_router"):
                    stack.append((route.original_router, prefix))
                # Mount and APIRouter both expose ``.routes``;
                # recurse with the appropriate prefix. The path
                # is already baked in for included routers (we
                # walked them above without adding to the
                # prefix); for Starlette routers nested in a
                # Mount, the inner prefix is the parent's path.
                if hasattr(route, "routes") and not hasattr(route, "original_router"):
                    stack.append((route, inner_prefix))
                # Recurse into a Mount's sub-app.
                sub_app = getattr(route, "app", None)
                if sub_app is not None and hasattr(sub_app, "routes"):
                    stack.append((sub_app, inner_prefix))
        return paths

    def test_routes_are_mounted(self) -> None:
        with make_mcp_app() as (app, _client):
            paths = self._all_paths(app)
            # The HTTP shortcut.
            assert "/mcp/tools/call" in paths
            assert "/mcp/tools" in paths
            # The SDK's SSE and message routes are inside the
            # mounted sub-app, so they appear with the
            # ``/mcp`` prefix in the parent's route list.
            # Starlette may normalise the trailing slash off
            # the message path, so accept either form.
            assert "/mcp/sse" in paths
            assert any(p == "/mcp/messages" or p == "/mcp/messages/" for p in paths)

    def test_app_can_be_disabled_via_env(self) -> None:
        """``AIDP_AGENT_MCP_ENABLED=0`` skips the MCP surface entirely."""
        from aidp_agent.main import create_app

        os.environ["AIDP_AGENT_MCP_ENABLED"] = "0"
        try:
            set_datasource_client(None)
            set_mcp_server(None)
            app = create_app()
            paths = self._all_paths(app)
            assert "/mcp/tools/call" not in paths
            assert "/mcp/tools" not in paths
        finally:
            os.environ.pop("AIDP_AGENT_MCP_ENABLED", None)
            set_datasource_client(None)
            set_mcp_server(None)


# ---------------------------------------------------------------------------
# Build helpers
# ---------------------------------------------------------------------------


def test_build_mcp_router_returns_router() -> None:
    """``build_mcp_router`` returns a router with the expected routes."""
    router = build_mcp_router()
    paths = {route.path for route in router.routes if hasattr(route, "path")}
    assert "/mcp/tools/call" in paths
    assert "/mcp/tools" in paths


def test_dependency_helpers_return_singletons() -> None:
    """The FastAPI dependency helpers return the module-level singletons."""
    from aidp_agent.mcp.server import (
        get_datasource_client_dep,
        get_mcp_server_dep,
    )

    sentinel_client = StubDatasourceClient()
    sentinel_server = build_default_server(client=sentinel_client)
    set_datasource_client(sentinel_client)
    set_mcp_server(sentinel_server)
    try:
        assert get_datasource_client_dep() is sentinel_client
        assert get_mcp_server_dep() is sentinel_server
    finally:
        set_datasource_client(None)
        set_mcp_server(None)


def test_dispatch_tool_handles_non_json_text_content() -> None:
    """When the SDK returns a non-JSON text block, ``_dispatch_tool`` keeps ``raw_text`` and sets ``payload=None``."""
    import asyncio

    from aidp_agent.mcp.server import _dispatch_tool
    from mcp.server.mcpserver import MCPServer

    server = MCPServer(name="x", version="0")

    async def fake_call_tool(name: str, arguments: dict[str, Any]) -> Any:
        # Construct a CallToolResult whose single text block is
        # *not* valid JSON, to exercise the fallback path.
        return _make_result_with_text("not-json-just-a-plain-message", is_error=False)

    # Monkey-patch the server's call_tool to return our fake.
    server.call_tool = fake_call_tool  # type: ignore[method-assign,assignment]

    result = asyncio.run(_dispatch_tool(server, "any", {}))
    assert result["is_error"] is False
    assert result["payload"] is None
    assert result["raw_text"] == "not-json-just-a-plain-message"


def test_dispatch_tool_handles_embedded_dict_payload() -> None:
    """When the SDK embeds a non-string value in ``text``, ``_dispatch_tool`` uses it as the payload directly."""
    import asyncio

    from aidp_agent.mcp.server import _dispatch_tool
    from mcp.server.mcpserver import MCPServer

    server = MCPServer(name="x", version="0")

    class _DictPayload:
        """Duck-typed substitute for ``TextContent`` whose ``text`` is a dict."""

        text: dict[str, Any]
        type: str = "text"

        def __init__(self) -> None:
            self.text = {"already": "a-dict"}

    class _Result:
        is_error: bool = False
        content: list[Any]

        def __init__(self) -> None:
            self.content = []

    async def fake_call_tool(name: str, arguments: dict[str, Any]) -> Any:
        r = _Result()
        r.content = [_DictPayload()]
        return r

    server.call_tool = fake_call_tool  # type: ignore[method-assign,assignment]

    result = asyncio.run(_dispatch_tool(server, "any", {}))
    assert result["is_error"] is False
    assert result["payload"] == {"already": "a-dict"}
    # ``raw_text`` stays ``None`` — the SDK did not produce a
    # string form, the caller has the structured payload
    # directly.
    assert result["raw_text"] is None


def _make_result_with_text(text: str, *, is_error: bool) -> Any:
    """Build a :class:`CallToolResult`-shaped object for tests."""
    from mcp_types import CallToolResult, TextContent

    return CallToolResult(
        content=[TextContent(type="text", text=text)],
        is_error=is_error,
    )


def test_build_sse_starlette_app_returns_starlette() -> None:
    """``build_sse_starlette_app`` returns the SDK's Starlette sub-app."""
    app = build_sse_starlette_app(
        sse_path="/sse",
        message_path="/messages/",
        transport_security=_relaxed_security(),
    )
    # The returned object is a Starlette app; we assert on
    # ``app.router.routes`` so the test does not depend on
    # whether the SDK wraps the routes in a sub-router or
    # mounts them directly. Starlette normalises the trailing
    # slash on the message path, so accept either form.
    routes = getattr(app, "router", app).routes
    paths = {getattr(r, "path", "") for r in routes}
    assert "/sse" in paths
    assert "/messages" in paths or "/messages/" in paths


def _relaxed_security() -> Any:
    """Build a :class:`TransportSecuritySettings` that disables DNS-rebinding protection."""
    from mcp.server.transport_security import TransportSecuritySettings

    return TransportSecuritySettings(enable_dns_rebinding_protection=False)
