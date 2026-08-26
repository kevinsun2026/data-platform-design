"""Tests for the PII suggestion service (Task 16).

The test suite pins the contract that
:mod:`aidp_datasource.services.pii_service` ships in Task 16:

- The :class:`RuleBasedPIIClient` (the default client) matches
  column names against a curated regex table + a conservative
  sample-value shape check, and returns a
  :class:`PIIColumnSuggestion` per PII column.
- The :class:`AgentGatewayPIIClient` POSTs to the
  agent-gateway ``/v1/chat/completions`` endpoint and parses
  the OpenAI-compat response.
- The :class:`PIIService.suggest_pii` orchestration uses the
  cached schema + a small sample per table; the LLM call is
  mocked at the client boundary so the test does not need a
  live agent-gateway.
- The response parser tolerates the common LLM quirks
  (Markdown fences, missing fields, malformed JSON) and
  returns ``[]`` rather than raising.
- L1 isolation: a missing datasource raises
  :class:`NotFoundError`; the tenant filter is honoured.

The connector is mocked at the
:func:`aidp_datasource.services.pii_service.build_connector`
boundary so the test does not need a real database.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from aidp_common.errors import NotFoundError, UpstreamError
from aidp_datasource.connectors.base import (
    ColumnInfo,
    TableInfo,
)
from aidp_datasource.models import Base, Datasource, DatasourceSchema
from aidp_datasource.schemas import (
    ConnectionConfig,
    CredentialsPayload,
    DatasourceCreateRequest,
)
from aidp_datasource.services.credential_service import (
    CredentialService,
    set_default_credential_service,
)
from aidp_datasource.services.datasource_service import DatasourceService
from aidp_datasource.services.pii_service import (
    AgentGatewayPIIClient,
    PIIColumnSuggestion,
    PIIService,
    RuleBasedPIIClient,
    _build_prompt,
    _looks_like_email,
    _looks_like_ip,
    _looks_like_phone,
    _match_name,
    _match_values,
    _parse_pii_response,
)
from aidp_db.session import get_session
from sqlalchemy import Column, String, Table, create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import StaticPool

# ---------------------------------------------------------------------------
# Engine + tenants fixture
# ---------------------------------------------------------------------------


def _build_engine() -> Engine:
    Table(
        "tenants",
        Base.metadata,
        Column("id", String(36), primary_key=True),
        Column("code", String(64), nullable=False, unique=True),
        Column("name", String(255), nullable=False),
        extend_existing=True,
    )
    eng = create_engine(
        "sqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    from sqlalchemy import event as _event

    @_event.listens_for(eng, "connect")
    def _enable_fk(dbapi_conn: Any, _conn_record: Any) -> None:
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA foreign_keys=ON")
        cur.close()

    Base.metadata.create_all(eng)
    return eng


def _insert_tenant(*, eng: Engine, tenant_id: str, code: str) -> None:
    with eng.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO tenants (id, code, name) VALUES (:id, :code, :name)"
            ),
            {"id": tenant_id, "code": code, "name": code},
        )


@pytest.fixture
def wired_engine(monkeypatch: pytest.MonkeyPatch) -> Iterator[Engine]:
    eng = _build_engine()
    import aidp_db.session as db_session

    monkeypatch.setattr(
        db_session, "_engine_cache", {str(eng.url): eng}
    )
    from aidp_common.config import get_settings

    monkeypatch.setattr(get_settings(), "db_url", str(eng.url))
    _insert_tenant(eng=eng, tenant_id="tenant-a", code="acme")
    _insert_tenant(eng=eng, tenant_id="tenant-b", code="globex")
    try:
        yield eng
    finally:
        db_session.reset_engine_cache()
        Base.metadata.drop_all(eng)
        eng.dispose()


@pytest.fixture
def credential_service() -> Iterator[CredentialService]:
    """A fresh credential service backed by a deterministic key."""
    svc = CredentialService(key=b"\x04" * 32)
    set_default_credential_service(svc)
    try:
        yield svc
    finally:
        set_default_credential_service(None)


# ---------------------------------------------------------------------------
# Datasource factory + cache stub
# ---------------------------------------------------------------------------


def _make_datasource(
    *,
    service: DatasourceService,
    name: str = "primary",
    kind: str = "postgresql",
    tenant_id: str = "tenant-a",
) -> Datasource:
    body = DatasourceCreateRequest(
        name=name,
        kind=kind,  # type: ignore[arg-type]
        env="prod",
        description="",
        connection=ConnectionConfig(host="db.example.test", port=5432, database="aidp"),
        credentials=CredentialsPayload(username="u", password="p"),
        tags=[],
        enabled=True,
    )
    return service.create_datasource(
        tenant_id=tenant_id, actor="u-test", body=body
    )


def _write_schema_cache(
    *,
    eng: Engine,
    tenant_id: str,
    datasource_id: str,
    tables: list[TableInfo],
) -> None:
    """Write a synthetic cached schema row (fingerprint + tables_json)."""
    payload = [
        {
            "name": t.name,
            "schema": t.schema or "public",
            "columns": [
                {"name": c.name, "type": c.type, "nullable": bool(c.nullable)}
                for c in t.columns
            ],
            "primary_key": list(t.primary_key),
            "indexes": [],
            "row_count_estimate": t.row_count_estimate,
        }
        for t in tables
    ]
    from datetime import UTC, datetime

    from sqlalchemy.orm import Session

    with Session(eng) as session:
        session.add(
            DatasourceSchema(
                tenant_id=tenant_id,
                datasource_id=datasource_id,
                table_count=len(tables),
                tables_json=payload,
                fingerprint="x" * 64,
                refreshed_at=datetime.now(UTC),
            )
        )
        session.commit()


# ---------------------------------------------------------------------------
# Connector stub
# ---------------------------------------------------------------------------


def _make_fake_connector(
    *,
    preview_rows: list[dict[str, Any]] | None = None,
) -> Any:
    """A minimal connector stub that satisfies ``preview`` + ``close``."""
    fake = MagicMock()
    fake.KIND = "postgresql"
    rows = preview_rows or []
    fake.preview = AsyncMock(return_value=rows)
    fake.close = AsyncMock()
    return fake


# ---------------------------------------------------------------------------
# Unit tests: helpers
# ---------------------------------------------------------------------------


def test_match_name_detects_email_substring() -> None:
    """``UserEmail`` is flagged because ``email`` is a substring."""
    assert _match_name("user_email") == "email"
    assert _match_name("UserEmail") == "email"


def test_match_name_handles_common_patterns() -> None:
    """The full regex table is exercised end-to-end."""
    assert _match_name("phone_number") == "phone"
    assert _match_name("id_card_no") == "id_card"
    assert _match_name("home_address") == "address"
    assert _match_name("credit_card") == "financial"
    assert _match_name("ip_address") == "ip"
    assert _match_name("first_name") == "name"


def test_match_name_returns_none_for_unrelated() -> None:
    """A column name with no PII pattern returns ``None``."""
    assert _match_name("created_at") is None
    assert _match_name("quantity") is None


def test_looks_like_email_validates() -> None:
    assert _looks_like_email("a@b.test")
    assert _looks_like_email("alice@example.com")
    # No ``@`` or missing domain → reject.
    assert not _looks_like_email("not-an-email")
    assert not _looks_like_email("a@b")
    assert not _looks_like_email("@b.test")
    assert not _looks_like_email("a@.test")
    assert not _looks_like_email("a@b.")


def test_looks_like_phone_accepts_ten_plus_digits() -> None:
    assert _looks_like_phone("+1 (415) 555-1234")
    assert _looks_like_phone("13800138000")
    # 9 digits → reject.
    assert not _looks_like_phone("123456789")


def test_looks_like_ip_validates_dotted_quad() -> None:
    assert _looks_like_ip("192.168.1.1")
    assert _looks_like_ip("0.0.0.0")
    # 5 octets or out-of-range octets → reject.
    assert not _looks_like_ip("1.2.3.4.5")
    assert not _looks_like_ip("256.0.0.0")


def test_match_values_detects_email_in_samples() -> None:
    samples = [{"name": "Alice", "email": "a@b.test"}]
    assert _match_values(column_name="email", sample_rows=samples) == "email"


def test_match_values_detects_phone_in_samples() -> None:
    samples = [{"contact": "13800138000"}]
    assert _match_values(column_name="contact", sample_rows=samples) == "phone"


def test_match_values_returns_none_when_no_match() -> None:
    samples = [{"created_at": "2025-01-01T00:00:00Z"}]
    assert _match_values(column_name="created_at", sample_rows=samples) is None


# ---------------------------------------------------------------------------
# Unit tests: RuleBasedPIIClient
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rule_based_client_flags_name_columns() -> None:
    """The stub flags columns whose name matches a PII pattern."""
    client = RuleBasedPIIClient()
    columns = [
        ColumnInfo(name="id", type="integer"),
        ColumnInfo(name="email", type="text"),
        ColumnInfo(name="created_at", type="timestamp"),
    ]
    suggestions = await client.classify(
        table="users", columns=columns, sample_rows=[]
    )
    names = {s.name for s in suggestions}
    assert "email" in names
    assert "id" not in names
    assert "created_at" not in names
    # The flagged column carries the PII type + a reason.
    email_suggestion = next(s for s in suggestions if s.name == "email")
    assert email_suggestion.type == "email"
    assert "email" in email_suggestion.reason


@pytest.mark.asyncio
async def test_rule_based_client_falls_back_to_sample_values() -> None:
    """A column with no PII name pattern is flagged via the sample values."""
    client = RuleBasedPIIClient()
    columns = [
        ColumnInfo(name="notes", type="text"),
    ]
    samples = [{"notes": "Contact me at a@b.test"}]
    suggestions = await client.classify(
        table="events", columns=columns, sample_rows=samples
    )
    assert len(suggestions) == 1
    assert suggestions[0].name == "notes"
    assert suggestions[0].type == "email"


@pytest.mark.asyncio
async def test_rule_based_client_returns_empty_for_no_pii() -> None:
    """A table with no PII columns returns an empty list."""
    client = RuleBasedPIIClient()
    columns = [
        ColumnInfo(name="id", type="integer"),
        ColumnInfo(name="created_at", type="timestamp"),
        ColumnInfo(name="quantity", type="integer"),
    ]
    suggestions = await client.classify(
        table="orders", columns=columns, sample_rows=[]
    )
    assert suggestions == []


# ---------------------------------------------------------------------------
# Unit tests: AgentGatewayPIIClient
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_agent_gateway_client_parses_valid_response() -> None:
    """A well-formed OpenAI-compat response is parsed correctly."""
    client = AgentGatewayPIIClient(base_url="http://agent.test:8004")
    columns = [ColumnInfo(name="email", type="text")]
    response = {
        "choices": [
            {
                "message": {
                    "content": json.dumps([
                        {
                            "name": "email",
                            "type": "email",
                            "reason": "looks like an email",
                        }
                    ])
                }
            }
        ]
    }
    with patch.object(client, "_post_chat_completion", return_value=response):
        suggestions = await client.classify(
            table="users", columns=columns, sample_rows=[]
        )
    assert len(suggestions) == 1
    assert suggestions[0].name == "email"
    assert suggestions[0].type == "email"
    assert "email" in suggestions[0].reason.lower()


@pytest.mark.asyncio
async def test_agent_gateway_client_falls_back_on_network_error() -> None:
    """A network error degrades to the rule-based stub."""
    client = AgentGatewayPIIClient(base_url="http://agent.test:8004")
    columns = [ColumnInfo(name="email", type="text")]
    with patch.object(
        client,
        "_post_chat_completion",
        side_effect=RuntimeError("connection refused"),
    ):
        suggestions = await client.classify(
            table="users", columns=columns, sample_rows=[]
        )
    # The stub flagged ``email`` by name; the fallback ran.
    assert len(suggestions) == 1
    assert suggestions[0].name == "email"


@pytest.mark.asyncio
async def test_agent_gateway_client_tolerates_markdown_fence() -> None:
    r"""A ``\`\`\`json`` fence is stripped before parsing."""
    client = AgentGatewayPIIClient(base_url="http://agent.test:8004")
    response = {
        "choices": [
            {
                "message": {
                    "content": (
                        "```json\n"
                        + json.dumps(
                            [
                                {
                                    "name": "phone",
                                    "type": "phone",
                                    "reason": "matches phone pattern",
                                }
                            ]
                        )
                        + "\n```"
                    )
                }
            }
        ]
    }
    with patch.object(client, "_post_chat_completion", return_value=response):
        suggestions = await client.classify(
            table="users", columns=[ColumnInfo(name="phone", type="text")],
            sample_rows=[],
        )
    assert len(suggestions) == 1
    assert suggestions[0].name == "phone"


# ---------------------------------------------------------------------------
# Unit tests: response parser
# ---------------------------------------------------------------------------


def test_parse_pii_response_handles_valid_array() -> None:
    """A plain JSON array is parsed verbatim."""
    parsed = _parse_pii_response(
        json.dumps(
            [
                {"name": "email", "type": "email", "reason": "looks like email"},
            ]
        )
    )
    assert len(parsed) == 1
    assert parsed[0].name == "email"
    assert parsed[0].type == "email"


def test_parse_pii_response_fills_missing_reason() -> None:
    """A missing ``reason`` field gets a default explanation."""
    parsed = _parse_pii_response(json.dumps([{"name": "phone", "type": "phone"}]))
    assert parsed[0].reason
    assert "phone" in parsed[0].reason


def test_parse_pii_response_fills_missing_type() -> None:
    """A missing ``type`` field defaults to ``"other_pii"``."""
    parsed = _parse_pii_response(json.dumps([{"name": "x", "reason": "y"}]))
    assert parsed[0].type == "other_pii"


def test_parse_pii_response_skips_entries_without_name() -> None:
    """An entry with no ``name`` is silently dropped."""
    parsed = _parse_pii_response(json.dumps([{"type": "email"}, {"name": "x", "type": "email"}]))
    assert len(parsed) == 1
    assert parsed[0].name == "x"


def test_parse_pii_response_returns_empty_for_malformed() -> None:
    """Malformed JSON returns ``[]`` (the caller has a fallback)."""
    assert _parse_pii_response("not json") == []
    assert _parse_pii_response("") == []
    assert _parse_pii_response("[]") == []
    assert _parse_pii_response('"a string"') == []
    assert _parse_pii_response('{"not": "a list"}') == []


def test_build_prompt_includes_table_and_columns() -> None:
    """The prompt carries the table name + column list + a sample row."""
    prompt = _build_prompt(
        table="users",
        columns=[ColumnInfo(name="id", type="integer"), ColumnInfo(name="email", type="text")],
        sample_rows=[{"id": 1, "email": "a@b.test"}],
    )
    assert "users" in prompt
    assert "id" in prompt
    assert "email" in prompt
    assert "a@b.test" in prompt


def test_build_prompt_handles_no_sample() -> None:
    """An empty sample renders as ``(no sample rows)``."""
    prompt = _build_prompt(
        table="users",
        columns=[ColumnInfo(name="id", type="integer")],
        sample_rows=[],
    )
    assert "(no sample rows)" in prompt


# ---------------------------------------------------------------------------
# Integration tests: PIIService.suggest_pii
# ---------------------------------------------------------------------------


@pytest.fixture
def pii_service(
    credential_service: CredentialService,
) -> Iterator[PIIService]:
    """A fresh :class:`PIIService` with a mock client."""
    client = MagicMock()
    client.classify = AsyncMock(
        return_value=[
            PIIColumnSuggestion(name="email", type="email", reason="test reason")
        ]
    )
    svc = PIIService(credential_service=credential_service, client=client)
    try:
        yield svc
    finally:
        set_default_pii_service_for_test(None)


def set_default_pii_service_for_test(svc: PIIService | None) -> None:
    """Local helper that does not depend on the module's private state.

    The PII service module's :func:`set_default_pii_service` is
    a thin wrapper around the module-level ``_DEFAULT``. Tests
    do not need the global because they pass the service
    explicitly; this helper exists so the test fixture can
    ``yield`` without leaking state.
    """
    # The service is yielded to the test and discarded after
    # the fixture tear-down. We do not touch the module's
    # process-wide default so the next test starts clean.
    return


@pytest.mark.asyncio
async def test_suggest_pii_uses_cached_schema(
    wired_engine: Engine,
    credential_service: CredentialService,
    pii_service: PIIService,
    datasource_service: DatasourceService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``suggest_pii`` reads the cached schema + calls the LLM client."""
    _bypass_kafka(monkeypatch)
    ds = _make_datasource(service=datasource_service)
    _write_schema_cache(
        eng=wired_engine,
        tenant_id="tenant-a",
        datasource_id=ds.id,
        tables=[
            TableInfo(
                name="users",
                schema="public",
                columns=[
                    ColumnInfo(name="id", type="integer"),
                    ColumnInfo(name="email", type="text"),
                ],
            )
        ],
    )
    fake_connector = _make_fake_connector(
        preview_rows=[{"id": 1, "email": "a@b.test"}]
    )
    monkeypatch.setattr(
        "aidp_datasource.services.pii_service.build_connector",
        lambda **kwargs: fake_connector,
    )
    suggestions = await pii_service.suggest_pii(
        tenant_id="tenant-a", datasource_id=ds.id
    )
    # The mock client returns one suggestion per call; the
    # orchestration flattens across tables.
    assert len(suggestions) == 1
    assert suggestions[0].name == "email"
    # The connector's preview was called for the sample row.
    fake_connector.preview.assert_awaited_with(table="users", limit=5)


@pytest.mark.asyncio
async def test_suggest_pii_respects_table_whitelist(
    wired_engine: Engine,
    credential_service: CredentialService,
    pii_service: PIIService,
    datasource_service: DatasourceService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The optional ``tables`` whitelist filters the cached schema."""
    _bypass_kafka(monkeypatch)
    ds = _make_datasource(service=datasource_service)
    _write_schema_cache(
        eng=wired_engine,
        tenant_id="tenant-a",
        datasource_id=ds.id,
        tables=[
            TableInfo(
                name="users",
                schema="public",
                columns=[ColumnInfo(name="email", type="text")],
            ),
            TableInfo(
                name="orders",
                schema="public",
                columns=[ColumnInfo(name="email", type="text")],
            ),
        ],
    )
    fake_connector = _make_fake_connector()
    monkeypatch.setattr(
        "aidp_datasource.services.pii_service.build_connector",
        lambda **kwargs: fake_connector,
    )
    await pii_service.suggest_pii(
        tenant_id="tenant-a",
        datasource_id=ds.id,
        tables=["orders"],
    )
    # The mock client was only called for ``orders``; the
    # ``users`` table is skipped.
    pii_service._client.classify.assert_awaited_once()
    call_kwargs = pii_service._client.classify.await_args.kwargs
    assert call_kwargs["table"] == "orders"


@pytest.mark.asyncio
async def test_suggest_pii_raises_not_found(
    wired_engine: Engine,
    pii_service: PIIService,
) -> None:
    """A missing datasource raises :class:`NotFoundError`."""
    with pytest.raises(NotFoundError):
        await pii_service.suggest_pii(
            tenant_id="tenant-a",
            datasource_id="00000000-0000-0000-0000-000000000000",
        )


@pytest.mark.asyncio
async def test_suggest_pii_continues_when_sample_fails(
    wired_engine: Engine,
    credential_service: CredentialService,
    pii_service: PIIService,
    datasource_service: DatasourceService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A sample-fetch failure does not abort the suggestion."""
    _bypass_kafka(monkeypatch)
    ds = _make_datasource(service=datasource_service)
    _write_schema_cache(
        eng=wired_engine,
        tenant_id="tenant-a",
        datasource_id=ds.id,
        tables=[
            TableInfo(
                name="users",
                schema="public",
                columns=[ColumnInfo(name="email", type="text")],
            )
        ],
    )
    fake_connector = MagicMock()
    fake_connector.KIND = "postgresql"
    fake_connector.preview = AsyncMock(
        side_effect=RuntimeError("connection refused")
    )
    fake_connector.close = AsyncMock()
    monkeypatch.setattr(
        "aidp_datasource.services.pii_service.build_connector",
        lambda **kwargs: fake_connector,
    )
    suggestions = await pii_service.suggest_pii(
        tenant_id="tenant-a", datasource_id=ds.id
    )
    # The mock client still returned a suggestion (the
    # service degraded to column-name-only mode).
    assert len(suggestions) == 1


@pytest.mark.asyncio
async def test_suggest_pii_propagates_llm_error(
    wired_engine: Engine,
    credential_service: CredentialService,
    datasource_service: DatasourceService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A persistent LLM failure is wrapped in :class:`UpstreamError`."""
    _bypass_kafka(monkeypatch)
    ds = _make_datasource(service=datasource_service)
    _write_schema_cache(
        eng=wired_engine,
        tenant_id="tenant-a",
        datasource_id=ds.id,
        tables=[
            TableInfo(
                name="users",
                schema="public",
                columns=[ColumnInfo(name="email", type="text")],
            )
        ],
    )
    fake_connector = _make_fake_connector()
    monkeypatch.setattr(
        "aidp_datasource.services.pii_service.build_connector",
        lambda **kwargs: fake_connector,
    )
    failing_client = MagicMock()
    failing_client.classify = AsyncMock(side_effect=RuntimeError("llm down"))
    svc = PIIService(
        credential_service=credential_service, client=failing_client
    )
    with pytest.raises(UpstreamError):
        await svc.suggest_pii(
            tenant_id="tenant-a", datasource_id=ds.id
        )


@pytest.mark.asyncio
async def test_suggest_pii_skips_sample_when_size_is_zero(
    wired_engine: Engine,
    credential_service: CredentialService,
    pii_service: PIIService,
    datasource_service: DatasourceService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``sample_size=0`` skips the connector round-trip entirely."""
    _bypass_kafka(monkeypatch)
    ds = _make_datasource(service=datasource_service)
    _write_schema_cache(
        eng=wired_engine,
        tenant_id="tenant-a",
        datasource_id=ds.id,
        tables=[
            TableInfo(
                name="users",
                schema="public",
                columns=[ColumnInfo(name="email", type="text")],
            )
        ],
    )
    fake_connector = _make_fake_connector()
    monkeypatch.setattr(
        "aidp_datasource.services.pii_service.build_connector",
        lambda **kwargs: fake_connector,
    )
    await pii_service.suggest_pii(
        tenant_id="tenant-a", datasource_id=ds.id, sample_size=0
    )
    # The mock connector was never awaited because the
    # service skipped the sample step.
    fake_connector.preview.assert_not_awaited()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _bypass_kafka(monkeypatch: pytest.MonkeyPatch) -> None:
    """Bypass the Kafka producer so the test does not need a broker."""
    from aidp_datasource.services import datasource_service

    async def _noop(*args: Any, **kwargs: Any) -> Any:
        return None

    monkeypatch.setattr(datasource_service, "publish_event", _noop)


@pytest.fixture
def datasource_service(
    credential_service: CredentialService,
) -> Iterator[DatasourceService]:
    svc = DatasourceService(credential_service=credential_service)
    yield svc
