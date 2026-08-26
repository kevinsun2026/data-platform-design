"""API package for the Agent Gateway.

Mounts the OpenAI-compat surface (``/v1/chat/completions``,
``/v1/models``) and the BYOK surface (``/api/v1/agent/credentials``)
on a single FastAPI ``APIRouter``.
"""

from __future__ import annotations

from aidp_agent.api.endpoints import router

__all__ = ["router"]
