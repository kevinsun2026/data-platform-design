"""MCP tool implementations for the Agent Gateway.

Each sub-module owns the tool functions for one resource surface of
the platform:

- :mod:`aidp_agent.mcp.tools.datasource` — datasource discovery +
  connectivity probe.

The tool functions are intentionally *thin*:

- They take a :class:`aidp_agent.mcp.grpc_client.DatasourceClient`
  as their first positional argument, so a test (or a non-default
  deployment) can swap in a fake without monkey-patching.
- They never call the network directly. The gRPC client owns I/O
  and observability; the tool is a parameter validator and a
  return-value shaper.

Each tool returns a plain :class:`dict` shaped after the MCP
``CallToolResult`` content block (``text``-typed). The
:mod:`aidp_agent.mcp.server` module wraps the result into the
correct protocol envelope.
"""

from __future__ import annotations

__all__: list[str] = []
