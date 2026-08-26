"""AIDP IAM service layer.

Service-layer modules (auth, user, role, ...) hold the business
logic; the API layer under :mod:`aidp_iam.api` is a thin transport
adapter. The split mirrors the platform's standard
``api → service → model`` layering.
"""

from __future__ import annotations

__version__ = "0.1.0"
