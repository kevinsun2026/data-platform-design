"""Services package for the Agent Gateway.

The package groups the cross-cutting helpers (currently only
:class:`aidp_agent.metering`) and is the future home of higher-level
orchestrators (token-budget enforcement, rate-limiting per tenant,
...). Keeping the import stable lets the API layer depend on a
sibling module without circular imports.
"""

from __future__ import annotations

__all__: list[str] = []
