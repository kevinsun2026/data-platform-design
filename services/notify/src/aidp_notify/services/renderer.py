"""Handlebars-style template renderer for the Notify service.

The Notify service accepts templates like:

.. code-block:: text

    Subject: Welcome, {{user.name}}!
    Body:    Your account {{user.email}} is ready.

The renderer substitutes every ``{{var}}`` placeholder by walking a
dot-path inside the variables dict. Missing paths render as the empty
string (not the literal placeholder), so a template with a missing
field never leaks ``{{...}}`` to the recipient.

Locale selection
----------------

The dispatcher calls :func:`select_template` to pick a
:class:`NotificationTemplate` row for a given (tenant, code, locale)
triple. The algorithm:

1. Exact match — a row whose ``code`` AND ``locale`` match the request.
2. Locale-prefix match — a row whose ``locale`` is the language tag
   the request asked for (``"en"``) and a more specific variant
   (``"en-US"``) was not found. Implemented by trying a series of
   progressively shorter prefixes.
3. ``"default"`` fallback — a row with the matching code and
   ``locale="default"``. Every tenant is required to have at least
   one such row for every template they want to send.
4. ``None`` — the dispatcher raises :class:`NotFoundError`.

The renderer does not parse the template — it only does literal
``{{var}}`` substitution. This keeps the dependency surface tiny (no
``pybars3`` / ``chevron``) and the failure modes predictable (a typo
in a placeholder never crashes; the worst case is an empty string).
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from aidp_common.errors import NotFoundError
from sqlalchemy import select
from sqlalchemy.orm import Session

from aidp_notify.models import NotificationTemplate

#: Pattern that matches a Handlebars-style ``{{var}}`` placeholder. The
#: capture group is the raw variable name (dot-path). We do not support
#: Handlebars helpers, partials, or block statements; the Notify service
#: uses a deliberately tiny subset of the language.
_PLACEHOLDER_RE = re.compile(r"\{\{\s*([A-Za-z_][A-Za-z0-9_.\-]*)\s*\}\}")


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def _resolve_path(vars_: Mapping[str, Any], path: str) -> str:
    """Resolve a dot-path inside *vars_*; return the empty string on miss.

    The path is split on ``.`` and walked one segment at a time. A
    missing intermediate key or a non-mapping segment returns the
    empty string. ``None`` leaf values also render as the empty
    string (``None`` is *not* a valid render output — templates
    cannot produce the literal text ``"None"``).
    """
    cursor: Any = vars_
    for segment in path.split("."):
        if isinstance(cursor, Mapping):
            cursor = cursor.get(segment)
        else:
            # ``cursor`` is not indexable; treat the rest of the path
            # as a miss. Returning early is cheaper than walking into
            # a TypeError on the next iteration.
            return ""
        if cursor is None:
            return ""
    if cursor is None:
        return ""
    if isinstance(cursor, bool):
        # ``True`` / ``False`` are subclasses of ``int`` in Python; we
        # want the literal text, not the integer.
        return "true" if cursor else "false"
    if isinstance(cursor, (str, int, float)):
        return str(cursor)
    # Any other type (list / dict / custom object) is rendered as
    # ``str()``. The behaviour is deliberately permissive so a caller
    # that passes a structured value gets a sensible fallback rather
    # than a crash.
    return str(cursor)


def render(template: str, vars_: Mapping[str, Any]) -> str:
    """Substitute every ``{{var}}`` placeholder in *template*.

    Args:
        template: The raw template body (subject or body).
        vars_: The variable map. May be empty.

    Returns:
        The rendered string. Missing variables render as the empty
        string. The original *template* is returned unchanged when it
        contains no placeholders.
    """
    if not template or "{{" not in template:
        return template

    def _sub(match: re.Match[str]) -> str:
        return _resolve_path(vars_, match.group(1))

    return _PLACEHOLDER_RE.sub(_sub, template)


# ---------------------------------------------------------------------------
# Locale-aware template selection
# ---------------------------------------------------------------------------


def _locale_prefixes(locale: str) -> list[str]:
    """Return the cascade of locale tags to try for *locale*.

    ``"en-US"`` → ``["en-US", "en"]``. ``"zh-Hant-HK"`` →
    ``["zh-Hant-HK", "zh-Hant", "zh"]``. The caller stops at the first
    row that matches (or falls through to the ``"default"`` variant).
    """
    if not locale:
        return []
    parts = locale.replace("_", "-").split("-")
    out: list[str] = []
    for idx in range(len(parts), 0, -1):
        candidate = "-".join(parts[:idx])
        if candidate and candidate not in out:
            out.append(candidate)
    return out


def select_template(
    session: Session,
    *,
    tenant_id: str,
    code: str,
    locale: str,
) -> NotificationTemplate:
    """Pick the best :class:`NotificationTemplate` row for the request.

    Args:
        session: The active :class:`sqlalchemy.orm.Session`. The caller
            is responsible for committing / rolling back the surrounding
            transaction.
        tenant_id: Tenant the template belongs to (L1 isolation key).
        code: Logical template name (e.g. ``"user.welcome"``).
        locale: Locale tag from the request (e.g. ``"zh-CN"``). The
            empty string is treated as ``"default"``.

    Returns:
        The selected :class:`NotificationTemplate` row. The L1 listener
        auto-filters by ``tenant_id`` so cross-tenant probes surface
        as :class:`NotFoundError` regardless of the *code* match.

    Raises:
        NotFoundError: When no template row matches the request.
    """
    # The L1 listener auto-injects ``tenant_id = :tid`` on every
    # query; the explicit filter is added for documentation of intent
    # and to keep the query portable to environments where the
    # listener is disabled (e.g. a one-off script).
    candidates = _locale_prefixes(locale or "default") or ["default"]
    for tag in candidates:
        row = session.execute(
            select(NotificationTemplate).where(
                NotificationTemplate.tenant_id == tenant_id,
                NotificationTemplate.code == code,
                NotificationTemplate.locale == tag,
            )
        ).scalar_one_or_none()
        if row is not None:
            return row
    # Final fallback: the "default" variant. Only useful when the
    # caller did not already pass ``"default"`` as the locale.
    if "default" not in candidates:
        row = session.execute(
            select(NotificationTemplate).where(
                NotificationTemplate.tenant_id == tenant_id,
                NotificationTemplate.code == code,
                NotificationTemplate.locale == "default",
            )
        ).scalar_one_or_none()
        if row is not None:
            return row
    raise NotFoundError("notification template", f"{code}@{locale}")


__all__ = [
    "_PLACEHOLDER_RE",
    "render",
    "select_template",
]
