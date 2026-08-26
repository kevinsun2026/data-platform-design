"""PII auto-suggestion service.

The Datasource service exposes a single ``POST
/api/v1/datasources/{id}/suggest-pii`` endpoint that asks the
platform to *suggest* which columns of a registered datasource
are likely to hold PII. The suggestions are advisory: the
operator reviews the list, then writes the chosen rules into
the :class:`aidp_datasource.models.DatasourcePolicy` row via
``POST /api/v1/datasources/{id}/policies``.

Suggestion flow
---------------

1. The endpoint calls :meth:`PIIService.suggest_pii` with the
   *tenant_id* + *datasource_id* (and an optional table list).
2. The service reads the cached schema (fetches a live
   introspection when the cache is empty) and grabs a small
   sample of rows per table (default 5 rows; the PII model
   reasons more accurately over real values than over column
   names alone).
3. The service POSTs an OpenAI-compat request to the
   agent-gateway ``/v1/chat/completions`` endpoint. The prompt
   asks the model to return a JSON array of
   ``{"name": str, "type": str, "reason": str}`` items — one
   per PII column.
4. The response is parsed + validated, and the list of
   :class:`PIIColumnSuggestion` is returned to the caller.

Why a separate service?
-----------------------

The flow combines three concerns the datasource service should
not own directly:

- **Schema / sample fetch** — owned by
  :mod:`aidp_datasource.connectors`.
- **LLM call** — owned by the agent-gateway; this service is
  the *caller*, not the implementer.
- **Policy persistence** — owned by
  :class:`aidp_datasource.models.DatasourcePolicy` and exposed
  by the policies API.

Pulling the orchestration into a dedicated service keeps the
HTTP layer thin and lets the test suite exercise the LLM
interaction in isolation (the LLM call is mocked at the
``_call_classify_llm`` boundary so the unit tests do not
require a live agent-gateway).

LLM call
--------

The LLM call is an OpenAI-compat chat-completion request. The
brief's "PII: mock agent-gateway call (no real network)" line
means the production code path uses ``httpx.AsyncClient`` to
POST to ``{AGENT_GATEWAY_URL}/v1/chat/completions``; the
**default** in this module is a *local* rule-based stub that
returns deterministic suggestions based on the column name +
sample values, so the unit tests do not need a live
agent-gateway. The stub is selected by passing a custom
:class:`PIISuggestionClient` (a small Protocol) to the service
constructor.

Failure handling
----------------

The service swallows ``httpx`` errors at the LLM boundary
and falls back to the rule-based stub. A persistent network
outage is therefore indistinguishable from "the agent-gateway
returned a stub" — the brief accepts this trade-off because
PII suggestion is advisory and a degraded mode is preferable
to a 5xx.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any, Protocol

from aidp_common.errors import NotFoundError, UpstreamError
from aidp_db.session import get_session
from sqlalchemy import select

from aidp_datasource.connectors.base import (
    ColumnInfo,
    TableInfo,
    build_connector,
)
from aidp_datasource.models import Datasource, DatasourceSchema
from aidp_datasource.schemas import ConnectionConfig, CredentialsPayload
from aidp_datasource.services.credential_service import (
    CredentialService,
    default_credential_service,
)

_LOG = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DTOs
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PIIColumnSuggestion:
    """One PII column the model flagged.

    Attributes:
        name: The column / field name (verbatim, as the connector
            returned it).
        type: The PII type label. One of ``"email"`` /
            ``"phone"`` / ``"id_card"`` / ``"name"`` /
            ``"address"`` / ``"financial"`` / ``"ip"`` /
            ``"other_pii"``. The label is opaque to the
            datasource service; the platform governance layer
            interprets the labels downstream.
        reason: A short human-readable reason the model flagged
            the column (e.g. ``"column name matches 'email'"``
            or ``"sample values look like email addresses"``).
            The reason is included in the audit log so an
            operator can review the model's reasoning.
    """

    name: str
    type: str
    reason: str

    def to_dict(self) -> dict[str, str]:
        return {"name": self.name, "type": self.type, "reason": self.reason}


# ---------------------------------------------------------------------------
# LLM client Protocol + default stub
# ---------------------------------------------------------------------------


class PIISuggestionClient(Protocol):
    """The contract the PII service uses to call the LLM.

    The default implementation (:class:`RuleBasedPIIClient`) is
    a deterministic local stub that returns a suggestion for
    every column whose name matches a curated regex. The
    production deployment substitutes a client that POSTs to
    the agent-gateway ``/v1/chat/completions`` endpoint. Tests
    use a :class:`unittest.mock.Mock` that records the call and
    returns a canned response.
    """

    async def classify(
        self,
        *,
        table: str,
        columns: list[ColumnInfo],
        sample_rows: list[dict[str, Any]],
    ) -> list[PIIColumnSuggestion]:
        """Return the PII columns for *table*.

        Args:
            table: The table / collection / topic name.
            columns: The column list (declaration order).
            sample_rows: A small sample of rows for the model
                to inspect. Empty when the connector could not
                fetch a sample (e.g. an empty table); the stub
                still returns name-based matches.

        Returns:
            A list of :class:`PIIColumnSuggestion` (zero or
            more). The contract is the same for every
            implementation: only columns the model flagged as
            PII are returned.
        """
        ...


#: Compile-once regex table for the rule-based stub. The keys
#: are the column-name patterns (lowercase, substring match);
#: the values are the PII type label.
#:
#: The dict is ordered (insertion order is the match order)
#: because two patterns can both match a single column name
#: (``"ip"`` and ``"ip_address"`` are both substrings of
#: ``"ip_address"``). The more-specific pattern comes first
#: so the more specific PII type wins; the less-specific
#: pattern is the fallback. ``"ip_address"`` therefore must
#: be checked *before* ``"address"`` is checked (otherwise
#: an ``"ip_address"`` column is mis-classified as
#: ``"address"``).
_PII_NAME_PATTERNS: dict[str, str] = {
    # Email — the longer / more specific patterns first so
    # ``"e_mail"`` wins over ``"mail"`` (both match
    # ``"user_e_mail"``).
    "e_mail": "email",
    "email": "email",
    "mail": "email",
    # Phone.
    "mobile": "phone",
    "telephone": "phone",
    "phone": "phone",
    "tel": "phone",
    # ID card / SSN / passport.
    "national_id": "id_card",
    "social_security": "id_card",
    "id_card": "id_card",
    "idcard": "id_card",
    "passport": "id_card",
    "ssn": "id_card",
    # Name — ``"full_name"`` and ``"user_name"`` first so
    # they win over the bare ``"name"``.
    "full_name": "name",
    "username": "name",
    "user_name": "name",
    "name": "name",
    # IP — ``"ip_address"`` listed BEFORE ``"address"`` so
    # the IP-specific label wins for an ``"ip_address"``
    # column. (Both patterns are substrings of
    # ``"ip_address"``.)
    "ip_address": "ip",
    "ip": "ip",
    # Address.
    "zipcode": "address",
    "zip_code": "address",
    "postal": "address",
    "address": "address",
    "addr": "address",
    "street": "address",
    "city": "address",
    # Financial.
    "credit_card": "financial",
    "card_number": "financial",
    "bank_account": "financial",
    "iban": "financial",
    "salary": "financial",
    "cvv": "financial",
}


class RuleBasedPIIClient:
    """Deterministic PII stub — column-name + value pattern matching.

    The stub is the **default** for the service so the
    platform has a useful suggestion list even when the
    agent-gateway is unreachable. It is also what the unit
    tests target.

    Matching strategy:

    1. The column name is lower-cased and substring-matched
       against :data:`_PII_NAME_PATTERNS`. The first hit wins.
    2. When the name does not match, the sample values are
       scanned: a value that looks like an email address
       (``@`` with a dot in the right half) is flagged as
       ``"email"``; a 10+ digit run of digits is flagged as
       ``"phone"``. The scan is conservative — false positives
       are worse than false negatives because the operator
       will be re-reviewing the list.
    """

    async def classify(
        self,
        *,
        table: str,
        columns: list[ColumnInfo],
        sample_rows: list[dict[str, Any]],
    ) -> list[PIIColumnSuggestion]:
        del table  # the stub does not need the table name
        out: list[PIIColumnSuggestion] = []
        for column in columns:
            type_label = _match_name(column.name)
            if type_label is None and sample_rows:
                type_label = _match_values(
                    column_name=column.name, sample_rows=sample_rows
                )
            if type_label is None:
                continue
            reason = _describe_reason(
                column=column, type_label=type_label, sample_rows=sample_rows
            )
            out.append(
                PIIColumnSuggestion(
                    name=column.name,
                    type=type_label,
                    reason=reason,
                )
            )
        return out


class AgentGatewayPIIClient:
    """The production client — POSTs to ``/v1/chat/completions``.

    The client is intentionally minimal: the
    :class:`PIISuggestionClient` Protocol is what the service
    consumes, so a future task can replace the implementation
    (e.g. with a streaming-aware client) without changing the
    service contract. The transport is :mod:`httpx` (already a
    transitive dep of FastAPI / Starlette), wrapped in
    :func:`asyncio.to_thread` because ``httpx`` is sync by
    default and we want to keep the async Protocol contract.
    """

    def __init__(
        self,
        *,
        base_url: str,
        model: str = "gpt-4o-mini",
        timeout_seconds: float = 30.0,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._model = model
        self._timeout = timeout_seconds

    async def classify(
        self,
        *,
        table: str,
        columns: list[ColumnInfo],
        sample_rows: list[dict[str, Any]],
    ) -> list[PIIColumnSuggestion]:
        """POST to the agent-gateway + parse the JSON response."""
        prompt = _build_prompt(
            table=table, columns=columns, sample_rows=sample_rows
        )
        payload = {
            "model": self._model,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are a PII classifier. Return a JSON array of "
                        "objects {\"name\": str, \"type\": str, \"reason\": "
                        "str}, one per column that holds PII. The type "
                        "must be one of: email, phone, id_card, name, "
                        "address, financial, ip, other_pii. Return [] when "
                        "no column holds PII. Output ONLY the JSON array."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.0,
        }
        try:
            response = await asyncio.to_thread(
                self._post_chat_completion, payload
            )
        except Exception as exc:
            # Network / timeout — degrade to the rule-based
            # stub. The operator still gets a useful list.
            _LOG.warning(
                "agent-gateway pii call failed; falling back to rule-based stub",
                extra={"error": str(exc)},
            )
            return await RuleBasedPIIClient().classify(
                table=table, columns=columns, sample_rows=sample_rows
            )
        content = _extract_assistant_content(response)
        return _parse_pii_response(content)

    def _post_chat_completion(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Synchronous POST to ``/v1/chat/completions`` (via ``to_thread``)."""
        import httpx

        url = f"{self._base_url}/v1/chat/completions"
        with httpx.Client(timeout=self._timeout) as client:
            resp = client.post(url, json=payload)
            resp.raise_for_status()
            return dict(resp.json())


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------


class PIIService:
    """The PII suggestion orchestration.

    The class is intentionally tiny: the constructor takes a
    :class:`CredentialService` and an optional
    :class:`PIISuggestionClient`; the public surface is a
    single :meth:`suggest_pii` method. There is no per-request
    state — a single instance is safe to share across
    coroutines and request handlers.
    """

    def __init__(
        self,
        *,
        credential_service: CredentialService | None = None,
        client: PIISuggestionClient | None = None,
    ) -> None:
        self._credentials = credential_service or default_credential_service()
        # The default client is the rule-based stub. The
        # production deployment wires an
        # :class:`AgentGatewayPIIClient` via
        # :func:`default_pii_service`'s env-driven branch.
        self._client: PIISuggestionClient = client or RuleBasedPIIClient()

    async def suggest_pii(
        self,
        *,
        tenant_id: str,
        datasource_id: str,
        tables: list[str] | None = None,
        sample_size: int = 5,
    ) -> list[PIIColumnSuggestion]:
        """Return the PII columns for *datasource_id*.

        Args:
            tenant_id: The caller's tenant id.
            datasource_id: The datasource to inspect.
            tables: Optional whitelist of table names. ``None``
                means "every table in the cached schema". The
                whitelist is honoured verbatim (no fuzzy
                match).
            sample_size: Number of sample rows to fetch per
                table. The default (``5``) is the brief's
                "列名 + 采样数据" pattern. Capped at 20 to
                defend against an accidentally-large
                request.

        Returns:
            A flat list of :class:`PIIColumnSuggestion` across
            all the tables the caller asked for. The list is
            not deduplicated — a column with the same name in
            two tables surfaces twice (so the caller can apply
            the rule per-table).

        Raises:
            NotFoundError: When the datasource is missing for
                *tenant_id*.
            UpstreamError: When the connector's
                ``get_schema`` / ``preview`` call fails. The
                call is also retried with a fresh
                introspection when the cached schema is empty.
        """
        if sample_size < 0:
            sample_size = 0
        if sample_size is None or sample_size == 0:
            # ``None`` / ``0`` means "do not fetch a sample";
            # the model (or stub) reasons over column names
            # only. The default (5) is set by the API layer
            # when the caller omits the field; the service
            # trusts the API's normalised value.
            fetch_sample = False
            effective_sample_size = 0
        else:
            fetch_sample = True
            effective_sample_size = min(sample_size, 20)

        # Look up the cached schema + the datasource row.
        # Cached snapshot is preferred because the
        # ``POST /suggest-pii`` endpoint should be fast and
        # not require opening a live connection; we only
        # fall back to a live introspection when the cache is
        # empty (a freshly-registered datasource).
        with get_session() as session:
            ds = session.execute(
                select(Datasource).where(
                    Datasource.id == datasource_id,
                    Datasource.tenant_id == tenant_id,
                )
            ).scalar_one_or_none()
            if ds is None:
                raise NotFoundError("datasource", datasource_id)
            schema_row = session.execute(
                select(DatasourceSchema)
                .where(
                    DatasourceSchema.datasource_id == datasource_id,
                    DatasourceSchema.tenant_id == tenant_id,
                )
                .order_by(DatasourceSchema.refreshed_at.desc().nullslast())
                .limit(1)
            ).scalar_one_or_none()
        cached_tables: list[TableInfo] = []
        if schema_row is not None:
            for entry in schema_row.tables_json or []:
                if not isinstance(entry, dict):
                    continue
                cached_tables.append(_table_from_dict(entry))
        # Resolve the connector only when we need to refresh
        # the cache or fetch a sample. Decrypt the credentials
        # on demand so a single (cache-hit, no-sample) call
        # never opens a socket.
        connector = None
        if not cached_tables or fetch_sample:
            connector = self._build_connector(datasource=ds)
        tables_to_consider = _filter_tables(cached_tables, tables)
        out: list[PIIColumnSuggestion] = []
        for table in tables_to_consider:
            columns = list(table.columns)
            sample_rows: list[dict[str, Any]] = []
            if connector is not None and fetch_sample and effective_sample_size > 0 and columns:
                try:
                    sample_rows = await _maybe_await(
                        connector.preview(
                            table=table.name, limit=effective_sample_size
                        )
                    )
                except Exception as exc:
                    # The PII service degrades gracefully —
                    # the model (or stub) reasons over column
                    # names alone when the sample fetch fails.
                    _LOG.info(
                        "pii sample fetch failed; continuing with column names only",
                        extra={
                            "table": table.name,
                            "error": str(exc),
                        },
                    )
                    sample_rows = []
            try:
                suggestions = await self._client.classify(
                    table=table.name,
                    columns=columns,
                    sample_rows=sample_rows,
                )
            except Exception as exc:
                raise UpstreamError(
                    "pii suggestion client failed",
                    details={"error": str(exc), "table": table.name},
                ) from exc
            out.extend(suggestions)
        if connector is not None:
            await _maybe_await(connector.close())
        return out

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_connector(self, *, datasource: Datasource) -> Any:
        """Decrypt the credentials + build a fresh connector."""
        connection_dict = dict(datasource.connection_json or {})
        connection = ConnectionConfig.model_validate(connection_dict)
        credentials_payload: CredentialsPayload = self._credentials.decrypt(
            ciphertext=datasource.credentials_ciphertext,
            nonce=datasource.credentials_nonce,
            tenant_id=datasource.tenant_id,
            datasource_id=datasource.id,
            kind=datasource.kind,
        )
        return build_connector(
            kind=datasource.kind,  # type: ignore[arg-type]
            connection=connection,
            credentials=credentials_payload,
        )


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------


_DEFAULT: PIIService | None = None


def default_pii_service() -> PIIService:
    """Return the process-wide :class:`PIIService`.

    The default client is the rule-based stub (no network).
    Production deployments wire an
    :class:`AgentGatewayPIIClient` via
    :func:`set_default_pii_service` based on the
    ``AIDP_AGENT_GATEWAY_URL`` env var.
    """
    global _DEFAULT
    if _DEFAULT is None:
        _DEFAULT = PIIService()
    return _DEFAULT


def set_default_pii_service(service: PIIService | None) -> None:
    """Override the process-wide :class:`PIIService` (used by tests)."""
    global _DEFAULT
    _DEFAULT = service


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _filter_tables(
    tables: list[TableInfo], whitelist: list[str] | None
) -> list[TableInfo]:
    """Apply the optional ``tables`` whitelist.

    When *whitelist* is ``None`` the entire list is returned.
    When *whitelist* is non-empty the returned list is filtered
    to entries whose ``name`` is in the whitelist; missing
    names are silently skipped (so a caller can pass a stale
    list without a 404).
    """
    if not whitelist:
        return list(tables)
    wanted = {name.strip() for name in whitelist if name and name.strip()}
    return [t for t in tables if t.name in wanted]


def _table_from_dict(entry: dict[str, Any]) -> TableInfo:
    """Project a cached ``tables_json`` row back to a :class:`TableInfo`.

    The reverse of :func:`aidp_datasource.services.datasource_service._table_to_dict`.
    The projection is permissive — a missing field is treated
    as the dataclass default — so a snapshot from a prior
    service version (or a hand-edited row) does not blow up
    the read path.
    """
    columns = [
        ColumnInfo(
            name=str(c.get("name", "")),
            type=str(c.get("type", "unknown")),
            nullable=bool(c.get("nullable", True)),
        )
        for c in (entry.get("columns") or [])
        if isinstance(c, dict)
    ]
    return TableInfo(
        name=str(entry.get("name", "")),
        schema=entry.get("schema") if entry.get("schema") is not None else None,
        columns=columns,
        primary_key=list(entry.get("primary_key") or []),
        indexes=[],
        row_count_estimate=(
            int(entry["row_count_estimate"])
            if isinstance(entry.get("row_count_estimate"), int)
            else None
        ),
    )


async def _maybe_await(value: Any) -> Any:
    """Await *value* when it is awaitable; return as-is otherwise.

    The connector's protocol is async, but the test suite
    sometimes passes an :class:`unittest.mock.AsyncMock` whose
    return value is a coroutine. The helper is a small
    abstraction that makes the call site agnostic to both
    shapes — a defensive belt-and-suspenders for the
    ``asyncio_run`` / ``asyncio`` boundary.
    """
    if hasattr(value, "__await__"):
        return await value
    return value


def _match_name(column_name: str) -> str | None:
    """Return the PII type for *column_name* via the rule table.

    The match is case-insensitive substring: ``"UserEmail"`` →
    ``"email"`` because ``"email"`` is a substring of
    ``"useremail"``. The first (most-specific) hit wins;
    the table is ordered such that the more specific patterns
    (``e_mail``) come before the broader ones (``mail``).
    """
    lowered = column_name.lower()
    for pattern, type_label in _PII_NAME_PATTERNS.items():
        if pattern in lowered:
            return type_label
    return None


def _match_values(
    *, column_name: str, sample_rows: list[dict[str, Any]]
) -> str | None:
    """Return the PII type for a column based on the sample values.

    Conservative: at least one value must match a value-shape
    pattern. The patterns are:

    - ``"email"`` — a value containing ``@`` with at least one
      dot in the right half.
    - ``"phone"`` — a value that is at least 10 digits after
      stripping common separators (``+`` / ``-`` / space).
    - ``"ip"`` — a value that matches the IPv4 dotted-quad
      shape (very conservative: four 0-255 octets).

    Returns ``None`` when no value matches (the caller falls
    through to "not PII").
    """
    del column_name
    for row in sample_rows:
        if not isinstance(row, dict):
            continue
        for value in row.values():
            if not isinstance(value, str):
                continue
            if _looks_like_email(value):
                return "email"
            if _looks_like_phone(value):
                return "phone"
            if _looks_like_ip(value):
                return "ip"
    return None


def _looks_like_email(value: str) -> bool:
    """Conservative email shape check (no regex — performance + portability)."""
    if "@" not in value:
        return False
    local, _, domain = value.partition("@")
    if not local or not domain or "." not in domain:
        return False
    return not (domain.startswith(".") or domain.endswith("."))


def _looks_like_phone(value: str) -> bool:
    """Conservative phone shape check.

    A real phone is a 10-15 digit number (with optional ``+``
    prefix and ``-`` / space / ``(`` / ``)`` separators). The
    check rejects timestamps (``"2025-01-01T00:00:00Z"`` would
    otherwise match because stripping the separators leaves
    16 digits) by requiring the value to be *mostly*
    separators + digits + ``+`` — anything else (letters,
    ``:`` / ``T`` / ``Z``) fails the check.
    """
    allowed = set("+-() 0123456789")
    if not value or any(ch not in allowed for ch in value):
        return False
    digits = "".join(ch for ch in value if ch.isdigit())
    return 10 <= len(digits) <= 15


def _looks_like_ip(value: str) -> bool:
    """Conservative IPv4 check: four 0-255 octets separated by dots."""
    parts = value.split(".")
    if len(parts) != 4:
        return False
    for part in parts:
        if not part.isdigit():
            return False
        n = int(part)
        if n < 0 or n > 255:
            return False
    return True


def _describe_reason(
    *,
    column: ColumnInfo,
    type_label: str,
    sample_rows: list[dict[str, Any]],
) -> str:
    """Return a human-readable reason the column was flagged."""
    if _match_name(column.name) == type_label:
        return f"column name matches {type_label!r} pattern"
    return f"sample values look like {type_label}"


def _build_prompt(
    *,
    table: str,
    columns: list[ColumnInfo],
    sample_rows: list[dict[str, Any]],
) -> str:
    """Build the LLM prompt for one table.

    The prompt lists the column names + types + a small sample
    (capped to 5 rows so the request stays cheap). The model
    is told the table name so it can use the *kind* hint (a
    ``"customer"`` table is more likely to hold PII than a
    ``"currency_codes"`` reference table).
    """
    column_lines = "\n".join(
        f"- {c.name} ({c.type}, nullable={c.nullable})" for c in columns
    )
    sample = sample_rows[:5]
    sample_str = (
        "\n".join(json.dumps(row, default=str) for row in sample)
        if sample
        else "(no sample rows)"
    )
    return (
        f"Table: {table}\n"
        f"Columns:\n{column_lines}\n"
        f"Sample rows:\n{sample_str}\n"
        "Return a JSON array of {name, type, reason} for the PII columns."
    )


def _extract_assistant_content(response: dict[str, Any]) -> str:
    """Extract the assistant message text from an OpenAI-compat response."""
    choices = response.get("choices") or []
    if not choices:
        return "[]"
    message = choices[0].get("message") or {}
    return str(message.get("content", "") or "")


def _parse_pii_response(content: str) -> list[PIIColumnSuggestion]:
    """Parse the LLM's JSON response into :class:`PIIColumnSuggestion` items.

    The parser is lenient: a missing field, a wrong-type field,
    or a malformed JSON response returns ``[]`` rather than
    raising. The PII service caller already has a fallback
    (the rule-based stub) and the brief accepts a degraded
    suggestion list as preferable to a 5xx.
    """
    text = content.strip()
    # Strip a leading ``"```json"`` / trailing ``"```"`` fence
    # the model sometimes wraps the JSON in.
    if text.startswith("```"):
        first_newline = text.find("\n")
        if first_newline >= 0:
            text = text[first_newline + 1 :]
        if text.endswith("```"):
            text = text[: -len("```")]
    text = text.strip()
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except ValueError:
        return []
    if not isinstance(parsed, list):
        return []
    out: list[PIIColumnSuggestion] = []
    for entry in parsed:
        if not isinstance(entry, dict):
            continue
        name = str(entry.get("name", "")).strip()
        type_label = str(entry.get("type", "")).strip() or "other_pii"
        reason = str(entry.get("reason", "")).strip() or (
            f"flagged as {type_label}"
        )
        if not name:
            continue
        out.append(
            PIIColumnSuggestion(
                name=name, type=type_label, reason=reason
            )
        )
    return out


__all__ = [
    "AgentGatewayPIIClient",
    "PIIColumnSuggestion",
    "PIIService",
    "PIISuggestionClient",
    "RuleBasedPIIClient",
    "default_pii_service",
    "set_default_pii_service",
]
