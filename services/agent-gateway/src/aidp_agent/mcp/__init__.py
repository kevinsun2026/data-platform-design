"""MCP (Model Context Protocol) integration for the Agent Gateway.

This package exposes the platform's *datasource* operations to external
agents via the standard Model Context Protocol so an MCP-aware agent
(e.g. Claude Desktop, an in-house IDE plugin, or any third-party client
that speaks MCP) can discover and call:

- ``datasource.list`` — list every registered datasource.
- ``datasource.get`` — fetch one datasource by its id.
- ``datasource.test_connection`` — open a probe connection to a
  datasource and report the outcome.

The package layout is intentionally small:

- :mod:`aidp_agent.mcp.grpc_client` — the gRPC client that the tools
  call against :mod:`datasource-service` (Task 14). It is implemented
  as a stub for now and carries a ``TODO`` marker where the real gRPC
  channel will plug in.
- :mod:`aidp_agent.mcp.tools.datasource` — the MCP tool functions
  themselves. They are thin wrappers over the gRPC client: parameter
  validation, MCP-shaped return values, no I/O.
- :mod:`aidp_agent.mcp.server` — the FastAPI integration. It builds an
  :class:`mcp.server.mcpserver.MCPServer` (the official MCP Python
  SDK's v2 entry point), registers the tools, exposes the SSE
  transport at ``GET /mcp/sse``, and adds a JSON-RPC-friendly
  ``POST /mcp/tools/call`` shortcut for callers that prefer a
  single round-trip over the SSE channel.

Endpoints
---------

- ``GET  /mcp/sse``         — official MCP SSE transport. The SDK
  handles protocol negotiation; we just mount its Starlette app at
  ``/mcp`` so the path becomes ``/mcp/sse``.
- ``POST /mcp/tools/call``  — convenience HTTP endpoint that accepts a
  single JSON-RPC 2.0 ``tools/call`` request and returns a single
  JSON-RPC 2.0 response. Useful for callers that do not want to
  maintain an SSE channel just to call one tool.

Why both surfaces? The SSE channel is the standard MCP transport; a
strictly conformant client should be able to discover, initialise, and
call every tool over it. The ``/mcp/tools/call`` endpoint is a
non-standard but pragmatic shortcut: it lets a plain ``curl`` / HTTP
client exercise the same tools without standing up an SSE session,
which dramatically simplifies unit testing and one-shot
operator scripts.
"""

from __future__ import annotations

from aidp_agent.mcp.grpc_client import (
    Datasource,
    DatasourceClient,
    DatasourceKind,
    DatasourceSummary,
    StubDatasourceClient,
    TestConnectionOutcome,
)
from aidp_agent.mcp.server import (
    DatasourceToolClientDep,
    MCPServerConfig,
    build_default_server,
    build_mcp_router,
    build_sse_starlette_app,
    get_datasource_client,
    get_mcp_server,
    set_datasource_client,
    set_mcp_server,
)

__all__ = [
    "Datasource",
    "DatasourceClient",
    "DatasourceKind",
    "DatasourceSummary",
    "DatasourceToolClientDep",
    "MCPServerConfig",
    "StubDatasourceClient",
    "TestConnectionOutcome",
    "build_default_server",
    "build_mcp_router",
    "build_sse_starlette_app",
    "get_datasource_client",
    "get_mcp_server",
    "set_datasource_client",
    "set_mcp_server",
]
