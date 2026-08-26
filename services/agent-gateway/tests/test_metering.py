"""Tests for the Agent Gateway metering layer.

The metering layer is responsible for two things:

1. **Cost calculation** — given a token-usage block and a provider
   price list, compute the USD cost.
2. **Async sink** — persist every call's usage to ClickHouse (or
   Postgres fallback). The sink is asynchronous and bounded; a
   back-pressure scenario drops rows rather than blocks the request
   path.

These tests pin the cost math, the ClickHouse / Postgres / InMemory
sink behaviour, the dispatcher's queue semantics, and the public
``build_sink_from_env`` factory.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterator

import httpx
import pytest
from aidp_agent.metering import (
    ClickHouseSink,
    InMemorySink,
    MeteringDispatcher,
    PostgresSink,
    UsageRecord,
    build_record,
    build_sink_from_env,
    calculate_cost,
)
from aidp_agent.providers.base import (
    ChatMessage,
    ChatRequest,
    ModelSpec,
    ModelTier,
    ProviderConfig,
    TaskType,
    TokenUsage,
)
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import StaticPool

# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def openai_config() -> ProviderConfig:
    """A representative OpenAI config for cost tests."""
    return ProviderConfig(
        name="openai",
        display_name="OpenAI",
        base_url="https://api.openai.com/v1",
        api_key="sk-test",
        model_specs=(
            ModelSpec("gpt-4o", ModelTier.FLAGSHIP, 0.005, 0.015),
            ModelSpec("gpt-4o-mini", ModelTier.BALANCED, 0.00015, 0.0006),
            ModelSpec("gpt-4.1-nano", ModelTier.ECONOMY, 0.0001, 0.0004),
        ),
    )


@pytest.fixture
def sample_request() -> ChatRequest:
    """A representative :class:`ChatRequest` for record-builder tests."""
    return ChatRequest(
        model="gpt-4o-mini",
        messages=[ChatMessage(role="user", content="hello")],
        model_tier=ModelTier.BALANCED,
        task_type=TaskType.ETL,
        tenant_id="tenant-a",
        user_id="u-1",
        metadata={"trace_id": "0af7651916cd43dd8448eb211c80319c"},
    )


@pytest.fixture
def in_memory_engine() -> Iterator[Engine]:
    """Build a fresh in-memory SQLite engine for Postgres-sink tests.

    ``yield``-based fixtures must be declared as a generator for
    mypy strict mode; the explicit ``Iterator[Engine]`` annotation
    is what makes the fixture type-check.
    """
    eng = create_engine(
        "sqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    try:
        yield eng
    finally:
        eng.dispose()


# ---------------------------------------------------------------------------
# Cost calculation
# ---------------------------------------------------------------------------


class TestCostCalculation:
    """Pin the (prompt_tokens, completion_tokens, prices) → cost math."""

    def test_basic_cost(self, openai_config: ProviderConfig) -> None:
        """1000 input + 500 output on gpt-4o-mini = $0.15 + $0.30 = $0.45."""
        usage = TokenUsage(
            prompt_tokens=1_000_000, completion_tokens=500_000, total_tokens=1_500_000
        )
        # gpt-4o-mini is $0.00015 / $0.0006 per 1K.
        cost = calculate_cost(usage, openai_config, model="gpt-4o-mini")
        assert cost == pytest.approx(1_000_000 / 1000 * 0.00015 + 500_000 / 1000 * 0.0006)

    def test_zero_tokens_zero_cost(self, openai_config: ProviderConfig) -> None:
        """A zero-token usage is reported as $0.00 (no division-by-zero risk)."""
        usage = TokenUsage(prompt_tokens=0, completion_tokens=0, total_tokens=0)
        assert calculate_cost(usage, openai_config, model="gpt-4o") == 0.0

    def test_unknown_model_falls_back_to_zero_price(self, openai_config: ProviderConfig) -> None:
        """An unknown model falls back to $0.00 rather than raising."""
        usage = TokenUsage(prompt_tokens=100, completion_tokens=100, total_tokens=200)
        cost = calculate_cost(usage, openai_config, model="gpt-99-unknown")
        assert cost == 0.0

    def test_no_model_uses_zero_price(self, openai_config: ProviderConfig) -> None:
        """``model=None`` is treated as unknown → $0.00."""
        usage = TokenUsage(prompt_tokens=100, completion_tokens=100, total_tokens=200)
        cost = calculate_cost(usage, openai_config, model=None)
        assert cost == 0.0

    def test_cost_rounded_to_8_decimals(self, openai_config: ProviderConfig) -> None:
        """Cost is rounded to 8 decimal places to keep float drift out of the DB."""
        usage = TokenUsage(prompt_tokens=1, completion_tokens=1, total_tokens=2)
        cost = calculate_cost(usage, openai_config, model="gpt-4o")
        # 0.005 / 1000 + 0.015 / 1000 = 0.00002
        assert cost == round(cost, 8)


# ---------------------------------------------------------------------------
# Usage record builder
# ---------------------------------------------------------------------------


class TestBuildRecord:
    """Pin the request → UsageRecord translation."""

    def test_basic_record(
        self,
        openai_config: ProviderConfig,
        sample_request: ChatRequest,
    ) -> None:
        """The record mirrors the request's tenant/user and the supplied usage."""
        usage = TokenUsage(prompt_tokens=100, completion_tokens=50, total_tokens=150)
        record = build_record(
            request=sample_request,
            provider_name="openai",
            model="gpt-4o-mini",
            usage=usage,
            cost=0.045,
            trace_id="0af7651916cd43dd8448eb211c80319c",
        )
        assert record.tenant_id == "tenant-a"
        assert record.user_id == "u-1"
        assert record.provider_name == "openai"
        assert record.model == "gpt-4o-mini"
        assert record.prompt_tokens == 100
        assert record.completion_tokens == 50
        assert record.total_tokens == 150
        assert record.cost_usd == 0.045
        assert record.model_tier == "balanced"
        assert record.task_type == "etl"
        assert record.trace_id == "0af7651916cd43dd8448eb211c80319c"
        # The ``timestamp`` defaults to ``time.time()`` — just assert
        # it's a positive float within a sane range.
        assert 1.0 <= record.timestamp <= 1.0e12

    def test_record_without_tier_or_task_defaults(
        self,
        openai_config: ProviderConfig,
    ) -> None:
        """A request without explicit tier / task gets the legacy defaults."""
        request = ChatRequest(
            model="gpt-4o",
            messages=[ChatMessage(role="user", content="hi")],
            tenant_id="tenant-b",
        )
        record = build_record(
            request=request,
            provider_name="openai",
            model="gpt-4o",
            usage=TokenUsage(prompt_tokens=10, completion_tokens=10, total_tokens=20),
            cost=0.0001,
        )
        assert record.model_tier == "balanced"
        assert record.task_type == "insight"

    def test_record_to_dict_is_json_serialisable(
        self,
        openai_config: ProviderConfig,
        sample_request: ChatRequest,
    ) -> None:
        """``UsageRecord.to_dict()`` is JSON-ready for the ClickHouse sink."""
        import json

        record = build_record(
            request=sample_request,
            provider_name="openai",
            model="gpt-4o-mini",
            usage=TokenUsage(prompt_tokens=1, completion_tokens=1, total_tokens=2),
            cost=0.0,
        )
        # The dict must round-trip through ``json.dumps``.
        payload = json.dumps(record.to_dict())
        assert "openai" in payload
        assert "gpt-4o-mini" in payload


# ---------------------------------------------------------------------------
# In-memory sink
# ---------------------------------------------------------------------------


class TestInMemorySink:
    """Pin the test-only ``InMemorySink`` semantics."""

    async def test_records_are_appended(self) -> None:
        """``write`` appends each record in order."""
        sink = InMemorySink()
        for i in range(3):
            record = _make_record(model=f"m-{i}")
            await sink.write(record)
        assert [r.model for r in sink.records] == ["m-0", "m-1", "m-2"]

    async def test_clear_empties_records(self) -> None:
        """``clear`` resets the buffer between tests."""
        sink = InMemorySink()
        await sink.write(_make_record(model="x"))
        sink.clear()
        assert sink.records == []


# ---------------------------------------------------------------------------
# ClickHouse sink
# ---------------------------------------------------------------------------


class TestClickHouseSink:
    """Pin the ClickHouse HTTP sink: payload shape + status handling."""

    async def test_write_posts_json_each_row_payload(self) -> None:
        """``write`` POSTs one JSON line to ``/?query=INSERT ... FORMAT JSONEachRow``."""
        recorded: list[tuple[str, str, bytes]] = []

        def _handler(request: httpx.Request) -> httpx.Response:
            recorded.append(
                (str(request.url), request.headers.get("content-type", ""), request.content)
            )
            return httpx.Response(200, text="")

        transport = httpx.MockTransport(_handler)
        client = httpx.AsyncClient(transport=transport)
        sink = ClickHouseSink(url="http://ch.local:8123", client=client)
        try:
            record = _make_record(model="gpt-4o-mini", cost=0.001)
            await sink.write(record)
        finally:
            await sink.aclose()
        assert len(recorded) == 1
        url, _content_type, body = recorded[0]
        assert "/?query=" in url
        assert "JSONEachRow" in url
        assert "gpt-4o-mini" in body.decode("utf-8")
        assert "openai" in body.decode("utf-8")

    async def test_write_swallows_4xx(self) -> None:
        """A 4xx response is logged but does not raise (metering is best-effort)."""
        transport = httpx.MockTransport(lambda req: httpx.Response(400, text="bad request"))
        client = httpx.AsyncClient(transport=transport)
        sink = ClickHouseSink(url="http://ch.local:8123", client=client)
        try:
            await sink.write(_make_record())
        finally:
            await sink.aclose()

    async def test_write_swallows_5xx(self) -> None:
        """A 5xx response is logged but does not raise."""
        transport = httpx.MockTransport(lambda req: httpx.Response(503, text="unavailable"))
        client = httpx.AsyncClient(transport=transport)
        sink = ClickHouseSink(url="http://ch.local:8123", client=client)
        try:
            await sink.write(_make_record())
        finally:
            await sink.aclose()

    async def test_write_swallows_network_error(self) -> None:
        """A network error is logged but does not raise."""
        transport = httpx.MockTransport(
            lambda req: (_ for _ in ()).throw(httpx.ConnectError("boom"))
        )
        client = httpx.AsyncClient(transport=transport)
        sink = ClickHouseSink(url="http://ch.local:8123", client=client)
        try:
            await sink.write(_make_record())
        finally:
            await sink.aclose()


# ---------------------------------------------------------------------------
# Postgres sink
# ---------------------------------------------------------------------------


class TestPostgresSink:
    """Pin the Postgres fallback: idempotent DDL, parameterised INSERT."""

    async def test_table_created_lazily_and_row_written(self, in_memory_engine: Engine) -> None:
        """The first ``write`` creates the table; the row is queryable."""
        sink = PostgresSink(engine=in_memory_engine)
        record = _make_record(model="gpt-4o-mini", cost=0.0042)
        await sink.write(record)
        with in_memory_engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT tenant_id, provider_name, model, prompt_tokens, "
                    "completion_tokens, total_tokens, cost_usd, model_tier, task_type "
                    "FROM agent_llm_usage"
                )
            ).all()
        assert len(rows) == 1
        row = rows[0]
        assert row.tenant_id == "tenant-a"
        assert row.provider_name == "openai"
        assert row.model == "gpt-4o-mini"
        assert row.prompt_tokens == 100
        assert row.completion_tokens == 50
        assert row.total_tokens == 150
        assert float(row.cost_usd) == pytest.approx(0.0042)
        assert row.model_tier == "balanced"
        assert row.task_type == "etl"

    async def test_multiple_writes_accumulate(self, in_memory_engine: Engine) -> None:
        """Each ``write`` appends a row; the table grows monotonically."""
        sink = PostgresSink(engine=in_memory_engine)
        for i in range(5):
            await sink.write(_make_record(model=f"m-{i}"))
        with in_memory_engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM agent_llm_usage")).scalar()
        assert count == 5

    async def test_ddl_is_idempotent(self, in_memory_engine: Engine) -> None:
        """A second PostgresSink instance can re-run the DDL without error."""
        # The first sink creates the table.
        await PostgresSink(engine=in_memory_engine).write(_make_record())
        # The second sink is fine.
        await PostgresSink(engine=in_memory_engine).write(_make_record())
        with in_memory_engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM agent_llm_usage")).scalar()
        assert count == 2


# ---------------------------------------------------------------------------
# MeteringDispatcher
# ---------------------------------------------------------------------------


class TestMeteringDispatcher:
    """Pin the async dispatcher: enqueue, worker, drain, back-pressure."""

    async def test_record_is_written_by_worker(self) -> None:
        """A ``record`` call is consumed by the worker and forwarded to the sink."""
        sink = InMemorySink()
        dispatcher = MeteringDispatcher(sink=sink, queue_size=8)
        await dispatcher.start()
        try:
            dispatcher.record(_make_record(model="m-1"))
            dispatcher.record(_make_record(model="m-2"))
            await dispatcher.drain()
        finally:
            await dispatcher.stop()
        assert [r.model for r in sink.records] == ["m-1", "m-2"]

    async def test_drain_drains_all_enqueued_records(self) -> None:
        """``drain`` consumes every record currently in the queue."""
        sink = InMemorySink()
        dispatcher = MeteringDispatcher(sink=sink, queue_size=16)
        # The worker must be running for ``record`` to enqueue;
        # without it the records bypass the queue via the sync
        # fallback.
        await dispatcher.start()
        try:
            for i in range(10):
                dispatcher.record(_make_record(model=f"m-{i}"))
            await dispatcher.drain()
        finally:
            await dispatcher.stop()
        assert len(sink.records) == 10

    async def test_start_is_idempotent(self) -> None:
        """Calling ``start`` twice does not spawn a second worker task."""
        sink = InMemorySink()
        dispatcher = MeteringDispatcher(sink=sink, queue_size=4)
        await dispatcher.start()
        first_task = dispatcher._worker_task
        await dispatcher.start()
        assert dispatcher._worker_task is first_task
        await dispatcher.stop()

    async def test_full_queue_drops_record(self) -> None:
        """When the queue is full, ``record`` drops the row without blocking."""

        # Block the sink so the queue fills up. We do this by
        # replacing the sink with a slow one.
        class _SlowSink:
            def __init__(self) -> None:
                self.in_flight = asyncio.Event()
                self.allow_release = asyncio.Event()
                self.records: list[UsageRecord] = []

            async def write(self, record: UsageRecord) -> None:
                self.records.append(record)
                self.in_flight.set()
                await self.allow_release.wait()

        slow = _SlowSink()
        dispatcher = MeteringDispatcher(sink=slow, queue_size=2)
        await dispatcher.start()
        try:
            # Fill the queue: the first call drives the worker (which
            # is blocked in ``_SlowSink.write``); the next two fill
            # the queue; the fourth is dropped.
            dispatcher.record(_make_record(model="a"))
            # Wait for the worker to pick up the first record.
            await slow.in_flight.wait()
            dispatcher.record(_make_record(model="b"))
            dispatcher.record(_make_record(model="c"))
            # The fourth record must be dropped (queue is full).
            dispatcher.record(_make_record(model="dropped"))
            # Now release the worker so it can finish.
            slow.allow_release.set()
            await dispatcher.drain()
        finally:
            await dispatcher.stop()
        # The dropped row never made it to the sink.
        assert all(r.model != "dropped" for r in slow.records)

    async def test_stop_drains_then_exits(self) -> None:
        """``stop`` flushes the queue before returning."""
        sink = InMemorySink()
        dispatcher = MeteringDispatcher(sink=sink, queue_size=4)
        await dispatcher.start()
        for i in range(3):
            dispatcher.record(_make_record(model=f"m-{i}"))
        await dispatcher.stop()
        # The worker is gone; the records are persisted.
        assert [r.model for r in sink.records] == ["m-0", "m-1", "m-2"]


# ---------------------------------------------------------------------------
# build_sink_from_env
# ---------------------------------------------------------------------------


class TestBuildSinkFromEnv:
    """Pin the env-driven sink selection."""

    def test_clickhouse_url_yields_clickhouse_sink(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """``AIDP_AGENT_CLICKHOUSE_URL`` set → :class:`ClickHouseSink`."""
        monkeypatch.setenv("AIDP_AGENT_CLICKHOUSE_URL", "http://ch.local:8123")
        sink = build_sink_from_env()
        assert isinstance(sink, ClickHouseSink)

    def test_no_url_with_engine_yields_postgres_sink(self, in_memory_engine: Engine) -> None:
        """No ClickHouse URL but an engine → :class:`PostgresSink`."""
        sink = build_sink_from_env(engine=in_memory_engine, clickhouse_url="")
        assert isinstance(sink, PostgresSink)

    def test_no_url_no_engine_yields_in_memory_sink(self) -> None:
        """No URL, no engine → :class:`InMemorySink` (test / dev fallback)."""
        sink = build_sink_from_env(clickhouse_url="")
        assert isinstance(sink, InMemorySink)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_record(
    *,
    model: str = "gpt-4o",
    cost: float = 0.001,
) -> UsageRecord:
    """Build a :class:`UsageRecord` for the metering tests."""
    return UsageRecord(
        tenant_id="tenant-a",
        user_id="u-1",
        provider_name="openai",
        model=model,
        prompt_tokens=100,
        completion_tokens=50,
        total_tokens=150,
        cost_usd=cost,
        model_tier="balanced",
        task_type="etl",
        timestamp=1700000000.0,
        trace_id="trace",
    )
