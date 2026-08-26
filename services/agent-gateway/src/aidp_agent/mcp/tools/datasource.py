"""MCP tools for the datasource surface.

The module exposes three tools, each a thin function over the
:class:`aidp_agent.mcp.grpc_client.DatasourceClient`:

- :func:`datasource_list` — registered as ``datasource.list``.
- :func:`datasource_get` — registered as ``datasource.get``.
- :func:`datasource_test_connection` — registered as
  ``datasource.test_connection``.

Return-value contract
---------------------

Each tool returns a JSON-encodable :class:`dict` on success. The
keys are documented in the per-function docstrings. On failure
the tool raises a subclass of
:class:`mcp.server.mcpserver.exceptions.ToolError`, which the
official MCP SDK translates into a ``CallToolResult`` with
``is_error=true``. The :mod:`aidp_agent.mcp.server` module
forwards the SDK's wire shape verbatim over the SSE transport
and parses the text back into a structured ``payload`` for the
``/mcp/tools/call`` HTTP shortcut.

The error subclasses below carry a *machine-readable* ``code``
(not just a message) so the HTTP caller can branch on it
without parsing prose. The ``code`` round-trips through the
exception's message as a JSON prefix; the SDK's error path
preserves the message verbatim.

Error model
-----------

- :class:`DatasourceNotFoundError` from the client becomes a
  :class:`ToolNotFoundError` (code ``NOT_FOUND``).
- :class:`DatasourceUnavailableError` from the client becomes a
  :class:`DatasourceUnavailableToolError` (code
  ``DATASOURCE_UNAVAILABLE``).
- Argument-shape problems (missing id, etc.) become a
  :class:`InvalidArgumentToolError` (code ``INVALID_ARGUMENT``).
"""

from __future__ import annotations

from typing import Any

from mcp.server.mcpserver.exceptions import ToolError

from aidp_agent.mcp.grpc_client import (
    DatasourceClient,
    DatasourceNotFoundError,
    DatasourceUnavailableError,
)

# ---------------------------------------------------------------------------
# Tool error subclasses
# ---------------------------------------------------------------------------


class InvalidArgumentToolError(ToolError):
    """Raised when a tool's arguments are malformed.

    Carries ``code = "INVALID_ARGUMENT"``.
    """

    code: str = "INVALID_ARGUMENT"


class ToolNotFoundError(ToolError):
    """Raised when the requested datasource id is unknown.

    Carries ``code = "NOT_FOUND"``.
    """

    code: str = "NOT_FOUND"


class DatasourceUnavailableToolError(ToolError):
    """Raised when the datasource-service itself is unreachable.

    Carries ``code = "DATASOURCE_UNAVAILABLE"``. Distinct from
    :class:`ToolNotFoundError`: here the call reached the gateway
    and was dispatched, but the upstream service could not answer.
    """

    code: str = "DATASOURCE_UNAVAILABLE"


# ---------------------------------------------------------------------------
# Argument validation
# ---------------------------------------------------------------------------


#: Maximum length of a ``datasource_id`` parameter. The gRPC
#: client may forward the id to a downstream service that
#: enforces a 64-byte limit; 128 is a generous local cap.
_MAX_ID_LEN: int = 128


def _validate_datasource_id(datasource_id: Any) -> str:
    """Return *datasource_id* if it is a non-empty string of bounded length.

    Raises:
        InvalidArgumentToolError: when the id is missing, not a
            string, empty, or too long.
    """
    if not isinstance(datasource_id, str):
        raise InvalidArgumentToolError("datasource_id must be a string")
    cleaned = datasource_id.strip()
    if not cleaned:
        raise InvalidArgumentToolError("datasource_id must be a non-empty string")
    if len(cleaned) > _MAX_ID_LEN:
        raise InvalidArgumentToolError(f"datasource_id must be <= {_MAX_ID_LEN} characters")
    return cleaned


# ---------------------------------------------------------------------------
# datasource.list
# ---------------------------------------------------------------------------


async def datasource_list(
    client: DatasourceClient,
    *,
    tenant_id: str | None = None,
) -> dict[str, Any]:
    """Return every datasource the caller can see.

    Tool name: ``datasource.list``.

    Args:
        client: The gRPC-backed (or stub) datasource client.
        tenant_id: Optional L1 tenant scope. When ``None``, the
            call is admin-scoped and returns every datasource.
            The MCP layer never injects this — the agent's
            identity comes from outside the MCP call (Phase 2).

    Returns:
        ``{"datasources": [{...}, ...], "count": N}``. The
        summary entries contain only the safe, non-sensitive
        fields (no host, port, or database name).

    Raises:
        DatasourceUnavailableToolError: when the upstream
            datasource-service is unreachable.
    """
    try:
        items = await client.list_datasources(tenant_id=tenant_id)
    except DatasourceUnavailableError as exc:
        raise DatasourceUnavailableToolError(f"datasource-service is unavailable: {exc}") from exc

    return {
        "datasources": [item.to_dict() for item in items],
        "count": len(items),
    }


# ---------------------------------------------------------------------------
# datasource.get
# ---------------------------------------------------------------------------


async def datasource_get(
    client: DatasourceClient,
    datasource_id: str,
    *,
    tenant_id: str | None = None,
) -> dict[str, Any]:
    """Return one datasource by id.

    Tool name: ``datasource.get``.

    Args:
        client: The datasource client.
        datasource_id: Required. The id of the datasource to fetch.
        tenant_id: Optional L1 tenant scope. A cross-tenant lookup
            is reported as ``NOT_FOUND`` to avoid leaking the
            existence of another tenant's data.

    Returns:
        ``{"datasource": {...full Datasource dict...}}``.

    Raises:
        InvalidArgumentToolError: when *datasource_id* is invalid.
        ToolNotFoundError: when no datasource with the given id
            exists (or is invisible to *tenant_id*).
        DatasourceUnavailableToolError: when the upstream service
            is unreachable.
    """
    normalised_id = _validate_datasource_id(datasource_id)
    try:
        ds = await client.get_datasource(normalised_id, tenant_id=tenant_id)
    except DatasourceNotFoundError as exc:
        raise ToolNotFoundError(
            f"datasource not found: {normalised_id} (hint: call datasource.list)"
        ) from exc
    except DatasourceUnavailableError as exc:
        raise DatasourceUnavailableToolError(f"datasource-service is unavailable: {exc}") from exc

    return {"datasource": ds.to_dict()}


# ---------------------------------------------------------------------------
# datasource.test_connection
# ---------------------------------------------------------------------------


async def datasource_test_connection(
    client: DatasourceClient,
    datasource_id: str,
    *,
    tenant_id: str | None = None,
) -> dict[str, Any]:
    """Probe-connect to a datasource and report the outcome.

    Tool name: ``datasource.test_connection``.

    The probe's success or failure is *returned* in the result
    payload (``ok`` field). The function only raises for
    programming errors (bad argument types, unknown id, upstream
    unreachable) — a failed probe is normal output and surfaces
    as ``ok=false`` + a populated ``error`` field.

    Args:
        client: The datasource client.
        datasource_id: Required. The id of the datasource to test.
        tenant_id: Optional L1 tenant scope (see
            :func:`datasource_get`).

    Returns:
        ``{"datasource_id": ..., "ok": bool, "latency_ms": ...,
        "error": "..."}``.

    Raises:
        InvalidArgumentToolError: when *datasource_id* is invalid.
        ToolNotFoundError: when no datasource with the given id
            exists.
        DatasourceUnavailableToolError: when the upstream service
            is unreachable.
    """
    normalised_id = _validate_datasource_id(datasource_id)
    try:
        result = await client.test_connection(normalised_id, tenant_id=tenant_id)
    except DatasourceNotFoundError as exc:
        raise ToolNotFoundError(
            f"datasource not found: {normalised_id} (hint: call datasource.list)"
        ) from exc
    except DatasourceUnavailableError as exc:
        raise DatasourceUnavailableToolError(f"datasource-service is unavailable: {exc}") from exc

    return result.to_dict()


# ---------------------------------------------------------------------------
# Tool registry (used by server.py)
# ---------------------------------------------------------------------------


#: The canonical mapping ``tool name -> callable``. The mapping
#: is a single source of truth so the SSE surface and the
#: ``/mcp/tools/call`` HTTP surface dispatch through the same
#: function pointers. A future task that adds a new tool only
#: touches this dict.
TOOL_REGISTRY: dict[str, Any] = {
    "datasource.list": datasource_list,
    "datasource.get": datasource_get,
    "datasource.test_connection": datasource_test_connection,
}


__all__ = [
    "TOOL_REGISTRY",
    "DatasourceUnavailableToolError",
    "InvalidArgumentToolError",
    "ToolNotFoundError",
    "datasource_get",
    "datasource_list",
    "datasource_test_connection",
]
