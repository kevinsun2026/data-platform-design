"""MCP server module — wires the SDK + tools into the FastAPI app.

The module owns the three concerns the gateway needs to expose
datasource tools to external agents over the Model Context Protocol:

1. The :class:`mcp.server.mcpserver.MCPServer` singleton (a thin
   wrapper around the official Python SDK's v2 entry point). The
   server holds the tool registry; the SSE transport and the
   ``/mcp/tools/call`` HTTP endpoint both dispatch through it.
2. A :class:`DatasourceClient` singleton. The MCP tools call into
   this client (which is currently the in-process stub from
   :mod:`aidp_agent.mcp.grpc_client` and will become a real gRPC
   channel once :mod:`datasource-service` is implemented).
3. A FastAPI router that mounts the JSON-RPC-friendly
   ``POST /mcp/tools/call`` endpoint. The SSE transport is owned
   by the SDK; we just mount its Starlette app at ``/mcp`` from
   :mod:`aidp_agent.main` so the path is ``/mcp/sse``.

Singleton pattern
-----------------

The module follows the same module-level-singleton-with-overrides
pattern the rest of the gateway uses (:class:`aidp_agent.main.AppState`):

- :func:`set_datasource_client` / :func:`get_datasource_client`
  manage the datasource client.
- :func:`set_mcp_server` / :func:`get_mcp_server` manage the
  :class:`MCPServer` instance.
- :func:`build_default_server` is the factory the lifespan calls
  on startup. It builds a fresh client and a fresh server and
  sets both as the process-wide singletons.

A test that wants a different client (or a different server
config) calls :func:`set_datasource_client` and
:func:`set_mcp_server` *before* the app handles a request. The
:func:`aidp_agent.main.set_state` pattern is already
autouse-reset by the test conftest, so the same teardown
fixtures cover the MCP singletons too.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Annotated, Any

from fastapi import APIRouter, Depends, status
from fastapi.responses import JSONResponse
from mcp.server.mcpserver import MCPServer
from pydantic import BaseModel, ConfigDict, Field

from aidp_agent.mcp.grpc_client import (
    DatasourceClient,
    build_default_datasource_client,
)
from aidp_agent.mcp.tools.datasource import (
    TOOL_REGISTRY,
    datasource_get,
    datasource_list,
    datasource_test_connection,
)

_LOG = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MCPServerConfig:
    """Static configuration for the gateway's MCP server.

    The defaults are picked so a developer running the service
    locally (with no env vars set) gets a working MCP surface
    pointing at the in-process stub client. A production
    deployment overrides the values via the
    :func:`build_default_server` factory and a custom
    :class:`DatasourceClient`.
    """

    name: str = "aidp-agent-mcp"
    version: str = "0.1.0"
    title: str = "AIDP Agent MCP Server"
    description: str = (
        "MCP server for the AIDP Agent Gateway. "
        "Exposes datasource discovery and connectivity probe tools."
    )
    instructions: str = (
        "Use datasource.list to discover available datasources, "
        "datasource.get to fetch a single datasource's details, "
        "and datasource.test_connection to verify connectivity."
    )


# ---------------------------------------------------------------------------
# Tool registration
# ---------------------------------------------------------------------------


def _register_datasource_tools(server: MCPServer, client: DatasourceClient) -> None:
    """Register the three datasource tools on *server*.

    The wrappers below are deliberate: the SDK's tool manager
    expects callables whose signature is the MCP-visible argument
    list, with the *client* dependency closed over. Wrapping each
    tool function in a one-liner keeps the tool functions in
    :mod:`aidp_agent.mcp.tools.datasource` independently testable
    (a test can call them with a fake client directly) while
    still letting the SDK own argument validation via Pydantic.
    """

    async def _list() -> dict[str, Any]:
        return await datasource_list(client)

    async def _get(datasource_id: str) -> dict[str, Any]:
        return await datasource_get(client, datasource_id)

    async def _test(datasource_id: str) -> dict[str, Any]:
        return await datasource_test_connection(client, datasource_id)

    server.add_tool(
        _list,
        name="datasource.list",
        title="List datasources",
        description=(
            "Return every datasource the caller can see, as a list "
            "of summary entries (id, name, kind, tenant_id, description)."
        ),
    )
    server.add_tool(
        _get,
        name="datasource.get",
        title="Get a datasource by id",
        description=(
            "Return the full description of a single datasource, "
            "including host, port, and database name."
        ),
    )
    server.add_tool(
        _test,
        name="datasource.test_connection",
        title="Test connectivity to a datasource",
        description=(
            "Open a probe connection to the named datasource and "
            "report the outcome (ok, latency_ms, error message)."
        ),
    )


# ---------------------------------------------------------------------------
# Server factory
# ---------------------------------------------------------------------------


def build_default_server(
    *,
    client: DatasourceClient | None = None,
    config: MCPServerConfig | None = None,
) -> MCPServer:
    """Build a fresh :class:`MCPServer` with the datasource tools registered.

    Args:
        client: A :class:`DatasourceClient` to back the tools.
            When ``None``, the default (currently the in-process
            stub) is used. Tests pass a fake to keep the suite
            hermetic.
        config: Server-level metadata. When ``None``, the
            :data:`MCPServerConfig` defaults are used.
    """
    cfg = config or MCPServerConfig()
    if client is None:
        client = build_default_datasource_client()
    server = MCPServer(
        name=cfg.name,
        title=cfg.title,
        description=cfg.description,
        instructions=cfg.instructions,
        version=cfg.version,
    )
    _register_datasource_tools(server, client)
    return server


# ---------------------------------------------------------------------------
# Module-level singletons (overridable by tests)
# ---------------------------------------------------------------------------


_CLIENT: DatasourceClient | None = None
_SERVER: MCPServer | None = None


def set_datasource_client(client: DatasourceClient | None) -> None:
    """Set the process-wide :class:`DatasourceClient`.

    Pass ``None`` to clear the override (the next caller falls
    back to :func:`build_default_datasource_client`).
    """
    global _CLIENT
    _CLIENT = client


def get_datasource_client() -> DatasourceClient:
    """Return the process-wide :class:`DatasourceClient`.

    The default is the in-process stub. The lifespan calls
    :func:`set_datasource_client` once at startup; tests call it
    to inject a fake.
    """
    if _CLIENT is None:
        return build_default_datasource_client()
    return _CLIENT


def set_mcp_server(server: MCPServer | None) -> None:
    """Set the process-wide :class:`MCPServer` (intended for tests)."""
    global _SERVER
    _SERVER = server


def get_mcp_server() -> MCPServer:
    """Return the process-wide :class:`MCPServer`.

    The default is built with :func:`build_default_server` and
    the in-process stub client. The lifespan calls
    :func:`set_mcp_server` once at startup; tests call it to
    inject a server with a fake client.
    """
    if _SERVER is None:
        return build_default_server()
    return _SERVER


# ---------------------------------------------------------------------------
# FastAPI integration: dependencies + router
# ---------------------------------------------------------------------------


def get_datasource_client_dep() -> DatasourceClient:
    """Default FastAPI dependency that returns the singleton client."""
    return get_datasource_client()


def get_mcp_server_dep() -> MCPServer:
    """Default FastAPI dependency that returns the singleton server."""
    return get_mcp_server()


#: The dependency alias used by the MCP router. The alias keeps the
#: signature short and the tests don't have to import the
#: ``_dep`` variants directly.
DatasourceToolClientDep = Annotated[DatasourceClient, Depends(get_datasource_client_dep)]


# ---------------------------------------------------------------------------
# /mcp/tools/call wire models
# ---------------------------------------------------------------------------


class ToolCallRequest(BaseModel):
    """Body of ``POST /mcp/tools/call``.

    The shape is a *JSON-RPC 2.0* ``tools/call`` request. We do
    not require the ``jsonrpc`` field — a tool is a real RPC and
    the field is informational for our endpoint — but we accept
    it for protocol-conformant clients.
    """

    model_config = ConfigDict(extra="ignore")

    #: Optional JSON-RPC protocol marker. Accepted but not required.
    jsonrpc: str | None = None
    #: Optional JSON-RPC request id. Echoed back in the response
    #: so an async client can correlate the call.
    id: int | str | None = None
    #: The MCP method. Only ``tools/call`` is accepted on this
    #: endpoint; the SSE transport supports the full set.
    method: str = "tools/call"
    #: Tool call parameters. The ``name`` is the tool to invoke;
    #: ``arguments`` is the argument dict forwarded verbatim.
    params: dict[str, Any] = Field(default_factory=dict)


class ToolCallError(BaseModel):
    """Error block inside a :class:`ToolCallResponse`."""

    code: str
    message: str
    hint: str | None = None
    extra: dict[str, Any] | None = None


class ToolCallResponse(BaseModel):
    """Body returned by ``POST /mcp/tools/call``.

    On success the response is a JSON-RPC 2.0 ``result``
    envelope; on error it is the protocol's ``error`` envelope.
    The body *never* uses both — the ``error`` field is set iff
    the request could not be dispatched (validation, unknown
    tool, transport error). A tool that *ran* successfully but
    reported a domain failure (e.g. ``NOT_FOUND``) still returns
    ``result`` with ``is_error=True`` content.
    """

    model_config = ConfigDict(extra="ignore")

    jsonrpc: str = "2.0"
    id: int | str | None = None
    result: dict[str, Any] | None = None
    error: ToolCallError | None = None


# ---------------------------------------------------------------------------
# /mcp/tools/call route handler
# ---------------------------------------------------------------------------


#: Callable signature for tool functions registered in
#: :data:`aidp_agent.mcp.tools.datasource.TOOL_REGISTRY`. The
#: tool always takes the client first, then the validated
#: arguments from the MCP wire.
ToolFn = Callable[..., Awaitable[dict[str, Any]]]


async def _dispatch_tool(
    server: MCPServer,
    name: str,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    """Dispatch *name* through the SDK's tool manager.

    The SDK's :meth:`MCPServer.call_tool` is the canonical
    entry point: it routes through the same code path the SSE
    transport uses, so the ``/mcp/tools/call`` endpoint and the
    SSE channel share semantics by construction (Pydantic
    argument validation, tool error mapping, ``is_error``
    propagation, etc.).

    Returns:
        A normalised result dict with three fields:

        - ``is_error``: the SDK's :attr:`CallToolResult.is_error`.
        - ``payload``: the parsed ``text`` payload (the tool
          function's return value, when it was a JSON-encodable
          dict). ``None`` when the tool returned a plain
          string or when the SDK produced an error message that
          is not JSON.
        - ``raw_text``: the original ``text`` field of the
          first content block, for clients that want the
          exact wire bytes the SDK produced.

    Note on tool errors: the SDK's *transport-level* tool
    handler (used by the SSE surface) maps :class:`ToolError`
    to a ``CallToolResult(is_error=True)``. The SDK's public
    :meth:`MCPServer.call_tool` method, however, does *not*
    catch :class:`ToolError` — it propagates the exception to
    the caller. Since this dispatcher sits between the HTTP
    endpoint and the public SDK method, it has to do the
    conversion itself, mirroring the SDK's transport-level
    behaviour. The conversion uses the same TextContent shape
    so the SSE and HTTP surfaces stay symmetric.
    """
    from mcp.server.mcpserver.exceptions import ToolError  # local import: v2 SDK path

    try:
        result = await server.call_tool(name, arguments)
    except ToolError as exc:
        # The SDK's tool manager already wraps the underlying
        # error with ``"Error executing tool <name>: <message>"``
        # in the :class:`ToolError` text. The SDK's
        # transport-level handler (``_handle_call_tool``) catches
        # the same exception and forwards ``str(exc)`` to the
        # caller verbatim, so we mirror that here to keep the
        # SSE and HTTP surfaces symmetric.
        return {
            "is_error": True,
            "payload": None,
            "raw_text": str(exc),
            "tool_error_code": getattr(exc, "code", None),
        }
    # ``call_tool`` returns ``CallToolResult | InputRequiredResult``.
    # The tools registered on this server do not declare any
    # ``Resolve(...)`` parameters, so the ``InputRequiredResult``
    # branch is unreachable in practice. We use ``getattr`` to
    # access the result fields so a test that fakes a
    # different return shape still drives the parsing path
    # cleanly.
    content = getattr(result, "content", None) or []
    is_error = bool(getattr(result, "is_error", False))
    # The tool functions in this gateway return a JSON-encodable
    # dict. The SDK's v2 tool manager serialises that dict to a
    # JSON string in a single ``TextContent`` block. We re-parse
    # it so the HTTP caller gets a structured payload rather than
    # a stringified blob. The ``raw_text`` field keeps the
    # original (unparsed) string for clients that want the exact
    # wire bytes.
    raw_text: str | None = None
    payload: Any = None
    if content:
        first = content[0]
        text_attr = getattr(first, "text", None)
        if isinstance(text_attr, str):
            raw_text = text_attr
            try:
                payload = json.loads(text_attr)
            except (TypeError, ValueError):
                # Not JSON (e.g. a plain string the tool returned,
                # or the SDK's "Error executing tool" message on
                # failure). Keep ``raw_text`` so the caller can
                # still see what the SDK produced.
                payload = None
        elif isinstance(text_attr, (dict, list, int, float, bool)):
            # SDK embedded the value directly. Use it as the
            # payload; ``raw_text`` stays ``None`` (the SDK did
            # not produce a string form).
            payload = text_attr
    return {
        "is_error": is_error,
        "payload": payload,
        "raw_text": raw_text,
    }


def build_mcp_router() -> APIRouter:
    """Build the FastAPI router that exposes ``POST /mcp/tools/call``.

    The SSE transport is *not* part of this router: it is mounted
    as a Starlette sub-app in :mod:`aidp_agent.main` so the
    SDK's own ASGI plumbing runs untouched.
    """
    router = APIRouter(prefix="/mcp", tags=["mcp"])

    @router.post(
        "/tools/call",
        response_model=ToolCallResponse,
        status_code=status.HTTP_200_OK,
        summary="Single-shot JSON-RPC tools/call against the MCP server.",
    )
    async def tools_call(
        body: ToolCallRequest,
        server_dep: Annotated[MCPServer, Depends(get_mcp_server_dep)],
    ) -> JSONResponse:
        """Dispatch a single ``tools/call`` to the MCP server.

        The endpoint:

        1. Validates the JSON-RPC envelope (``jsonrpc``,
           ``method``, ``id``).
        2. Resolves the tool name and arguments.
        3. Forwards the call through the SDK's
           :meth:`MCPServer.call_tool` so the SSE and HTTP
           surfaces share semantics.
        4. Returns a :class:`ToolCallResponse`. The HTTP status
           is always 200; per-protocol the call may still
           report an error or a tool-level ``is_error``.

        See :class:`ToolCallRequest` for the request body
        shape and :class:`ToolCallResponse` for the response.
        """
        if body.method != "tools/call":
            return _jsonrpc_error_response(
                status_code=status.HTTP_400_BAD_REQUEST,
                jsonrpc_id=body.id,
                code="METHOD_NOT_FOUND",
                message=f"unsupported method: {body.method!r}",
                hint="only 'tools/call' is accepted on this endpoint",
            )
        params = body.params
        name = params.get("name")
        arguments = params.get("arguments") or {}
        if not isinstance(name, str) or not name:
            return _jsonrpc_error_response(
                status_code=status.HTTP_400_BAD_REQUEST,
                jsonrpc_id=body.id,
                code="INVALID_ARGUMENT",
                message="params.name must be a non-empty string",
            )
        if not isinstance(arguments, dict):
            return _jsonrpc_error_response(
                status_code=status.HTTP_400_BAD_REQUEST,
                jsonrpc_id=body.id,
                code="INVALID_ARGUMENT",
                message="params.arguments must be an object",
            )
        if name not in TOOL_REGISTRY:
            return _jsonrpc_error_response(
                status_code=status.HTTP_404_NOT_FOUND,
                jsonrpc_id=body.id,
                code="TOOL_NOT_FOUND",
                message=f"unknown tool: {name!r}",
                hint="call the SSE endpoint to discover registered tools",
                extra={"known_tools": sorted(TOOL_REGISTRY)},
            )
        try:
            result = await _dispatch_tool(server_dep, name, arguments)
        except Exception as exc:  # pragma: no cover - defensive
            _LOG.exception("mcp tools/call raised unexpectedly", extra={"tool": name})
            return _jsonrpc_error_response(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                jsonrpc_id=body.id,
                code="INTERNAL_ERROR",
                message="mcp tools/call failed",
                hint="see server logs",
                extra={"cause": str(exc)},
            )
        # ``_dispatch_tool`` returns a normalised result dict
        # with ``is_error``/``payload``/``raw_text``. We forward
        # it verbatim into the JSON-RPC ``result`` envelope,
        # promoting ``is_error`` to a top-level field so a HTTP
        # caller can branch on it without parsing the content
        # blocks.
        response_result: dict[str, Any] = {
            "is_error": result.get("is_error", False),
            "payload": result.get("payload"),
        }
        if result.get("raw_text") is not None:
            response_result["raw_text"] = result.get("raw_text")
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=ToolCallResponse(
                id=body.id,
                result=response_result,
            ).model_dump(exclude_none=True),
        )

    @router.get(
        "/tools",
        response_model=None,
        status_code=status.HTTP_200_OK,
        summary="List the tools exposed by the MCP server.",
    )
    async def list_tools(
        server_dep: Annotated[MCPServer, Depends(get_mcp_server_dep)],
    ) -> dict[str, Any]:
        """Return the tool names registered on the MCP server.

        The endpoint is a non-MCP convenience for operators and
        tests that want to discover the available tools without
        standing up an MCP session. The shape is intentionally
        minimal: ``{"tools": [{"name": "...", "title": "...",
        "description": "..."}, ...]}``.
        """
        # The SDK exposes ``list_tools`` as a coroutine that
        # returns ``list[MCPTool]``. ``MCPTool`` is a Pydantic
        # model, so we serialise via ``model_dump`` for
        # forward-compatibility.
        tools = await server_dep.list_tools()
        return {
            "tools": [
                {
                    "name": t.name,
                    "title": t.title,
                    "description": t.description,
                }
                for t in tools
            ],
            "count": len(tools),
        }

    return router


def _jsonrpc_error_response(
    *,
    status_code: int,
    jsonrpc_id: int | str | None,
    code: str,
    message: str,
    hint: str | None = None,
    extra: dict[str, Any] | None = None,
) -> JSONResponse:
    """Build a JSON-RPC 2.0 ``error`` response envelope."""
    return JSONResponse(
        status_code=status_code,
        content=ToolCallResponse(
            id=jsonrpc_id,
            error=ToolCallError(
                code=code,
                message=message,
                hint=hint,
                extra=extra,
            ),
        ).model_dump(exclude_none=True),
    )


# ---------------------------------------------------------------------------
# SSE transport
# ---------------------------------------------------------------------------


def build_sse_starlette_app(
    *,
    sse_path: str = "/sse",
    message_path: str = "/messages/",
    transport_security: Any | None = None,
    host: str = "127.0.0.1",
) -> Any:
    """Return the SDK's Starlette SSE sub-app.

    The returned app exposes two routes:

    - ``GET  {sse_path}``     — the MCP SSE stream.
    - ``POST {message_path}`` — the MCP message endpoint (client
      posts JSON-RPC requests here).

    The gateway mounts this app at ``/mcp`` so the visible
    URLs are ``GET /mcp/sse`` and ``POST /mcp/messages/``,
    matching the brief. The :mod:`aidp_agent.main` module
    owns the mount point so the routing decision stays
    there.

    Args:
        sse_path: The path (relative to the mount point) of the
            SSE stream endpoint.
        message_path: The path of the message endpoint where
            clients post JSON-RPC requests.
        transport_security: Optional override for the SDK's
            DNS-rebinding protection. The default is the
            SDK's localhost-only policy. Tests that need a
            relaxed policy (e.g. so the ``testserver`` host
            the FastAPI test client uses is allowed) pass a
            custom :class:`TransportSecuritySettings` here.
        host: The host the SDK auto-detects to apply the
            default localhost-only policy. Only relevant when
            *transport_security* is ``None``.
    """
    server = get_mcp_server()
    # ``sse_app`` instantiates a fresh Starlette with the
    # routes the SDK needs. Mounting it under ``/mcp`` makes
    # the SSE path ``/mcp/sse`` and the message path
    # ``/mcp/messages/`` (Starlette preserves the prefix
    # exactly when the sub-app's routes are root-relative).
    return server.sse_app(
        sse_path=sse_path,
        message_path=message_path,
        transport_security=transport_security,
        host=host,
    )


# ---------------------------------------------------------------------------
# Re-exports
# ---------------------------------------------------------------------------


__all__ = [
    "DatasourceToolClientDep",
    "MCPServerConfig",
    "ToolCallError",
    "ToolCallRequest",
    "ToolCallResponse",
    "build_default_server",
    "build_mcp_router",
    "build_sse_starlette_app",
    "get_datasource_client",
    "get_datasource_client_dep",
    "get_mcp_server",
    "get_mcp_server_dep",
    "set_datasource_client",
    "set_mcp_server",
]


# ---------------------------------------------------------------------------
# TODO(Phase 2 / Task 14): replace the in-process stub with a real gRPC client
#
# The lifespan in :mod:`aidp_agent.main` will:
#
# 1. Read ``AIDP_DATASOURCE_GRPC_URL`` (default
#    ``datasource-service:8005``).
# 2. Build a single :class:`grpc.aio.insecure_channel` (and the
#    corresponding generated stub) at startup.
# 3. Pass the channel-wrapped :class:`DatasourceClient` to
#    :func:`build_default_server` and store the result via
#    :func:`set_mcp_server`.
# 4. Close the channel on shutdown.
#
# The MCP server, tools, and HTTP endpoints are unaffected by
# the swap — the only line that changes is what gets handed to
# :func:`set_datasource_client` at startup.
# ---------------------------------------------------------------------------
