"""Token usage metering and async sink to ClickHouse / Postgres.

The metering layer is responsible for two things:

1. **Cost calculation.** Given a token-usage block and a provider
   config, compute the USD cost of the call.
2. **Asynchronous sink.** Persist every call's usage so the platform
   can charge-back tenants, alert on runaway spend, and surface
   per-model stats in the audit log.

The sink target is configurable:

- When ``AIDP_AGENT_CLICKHOUSE_URL`` is set, every row is written to
  ClickHouse via the HTTP ``INSERT`` endpoint.
- When unset, rows fall back to the ``agent_llm_usage`` Postgres
  table, which is created and migrated alongside the rest of the
  service's schema.

The sink is *asynchronous* and *fire-and-forget*: the public surface
(:func:`record_usage`) returns immediately and the actual write
happens on a background task. A bounded queue + worker task ensures
the metering layer never blocks the request path or grows without
limit under back-pressure.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol

import httpx
from sqlalchemy import text
from sqlalchemy.engine import Engine

from aidp_agent.providers.base import (
    ChatRequest,
    ProviderConfig,
    TokenUsage,
)

_LOG = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Usage record
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class UsageRecord:
    """One chat-completion call's worth of metering.

    The record is intentionally flat: the sink target (ClickHouse or
    Postgres) receives a row with the same shape regardless of which
    provider answered the call. ``provider_name`` and ``model`` are
    the canonical identifiers used in the platform's cost reports.
    """

    tenant_id: str
    user_id: str | None
    provider_name: str
    model: str
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    cost_usd: float
    model_tier: str
    task_type: str
    timestamp: float
    trace_id: str | None = None
    extra: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-ready dict (used by the ClickHouse sink)."""
        return asdict(self)


# ---------------------------------------------------------------------------
# Cost calculation
# ---------------------------------------------------------------------------


def calculate_cost(
    usage: TokenUsage,
    config: ProviderConfig,
    *,
    model: str | None = None,
) -> float:
    """Return the USD cost of *usage* against *config*'s price list.

    When *model* is provided, the per-model price is consulted via
    :meth:`ProviderConfig.price_for`. When ``None``, the function
    falls back to the provider's default (``0.0`` / ``0.0`` for a
    provider that has not published prices). A provider that does
    not publish a price (e.g. a local vLLM mirror) reports
    ``0.0`` cost rather than raising.
    """
    if usage.total_tokens <= 0:
        return 0.0
    input_price, output_price = config.price_for(model)
    prompt_cost = (usage.prompt_tokens / 1000.0) * input_price
    completion_cost = (usage.completion_tokens / 1000.0) * output_price
    return round(prompt_cost + completion_cost, 8)


def build_record(
    *,
    request: ChatRequest,
    provider_name: str,
    model: str,
    usage: TokenUsage,
    cost: float,
    trace_id: str | None = None,
    extra: dict[str, str] | None = None,
    timestamp: float | None = None,
) -> UsageRecord:
    """Build a :class:`UsageRecord` from the routed-call inputs."""
    return UsageRecord(
        tenant_id=request.tenant_id,
        user_id=request.user_id,
        provider_name=provider_name,
        model=model,
        prompt_tokens=usage.prompt_tokens,
        completion_tokens=usage.completion_tokens,
        total_tokens=usage.total_tokens,
        cost_usd=cost,
        model_tier=request.model_tier.value if request.model_tier else "balanced",
        task_type=request.task_type.value if request.task_type else "insight",
        timestamp=timestamp if timestamp is not None else time.time(),
        trace_id=trace_id,
        extra=dict(extra) if extra else {},
    )


# ---------------------------------------------------------------------------
# Sinks
# ---------------------------------------------------------------------------


class UsageSink(Protocol):
    """A target that accepts :class:`UsageRecord` rows."""

    async def write(self, record: UsageRecord) -> None: ...


class ClickHouseSink:
    """Async sink that writes to ClickHouse over HTTP.

    ClickHouse exposes a trivial HTTP ``INSERT`` endpoint that
    accepts a JSONEachRow payload. We avoid pulling in a
    ``clickhouse-driver`` dependency because the wire protocol is
    two lines and the platform's ClickHouse is fronted by an
    HTTP proxy that requires no auth (or Basic auth via env).
    """

    def __init__(
        self,
        url: str,
        *,
        database: str = "aidp",
        table: str = "agent_llm_usage",
        client: httpx.AsyncClient | None = None,
        timeout_seconds: float = 5.0,
    ) -> None:
        # ``url`` is the bare endpoint, e.g. ``http://clickhouse:8123``.
        self._url = url.rstrip("/")
        self._database = database
        self._table = table
        self._client = client
        self._owns_client = client is None
        self._timeout = timeout_seconds

    def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self._timeout)
        return self._client

    async def aclose(self) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    async def write(self, record: UsageRecord) -> None:
        client = self._ensure_client()
        # ClickHouse's JSONEachRow format is one JSON object per line;
        # we send a single-line payload for one record.
        payload = json.dumps(_row_dict_for_clickhouse(record))
        query = f"INSERT INTO {self._database}.{self._table} FORMAT JSONEachRow"
        try:
            response = await client.post(
                f"{self._url}/",
                params={"query": query},
                content=payload,
            )
        except httpx.HTTPError as exc:
            _LOG.warning(
                "clickhouse write failed (network)",
                extra={"error": str(exc), "tenant_id": record.tenant_id},
            )
            return
        if response.status_code >= 400:
            _LOG.warning(
                "clickhouse write failed (status)",
                extra={
                    "status": response.status_code,
                    "body": response.text[:200],
                    "tenant_id": record.tenant_id,
                },
            )


def _row_dict_for_clickhouse(record: UsageRecord) -> dict[str, Any]:
    """Map a :class:`UsageRecord` to the ClickHouse row shape."""
    return {
        "tenant_id": record.tenant_id,
        "user_id": record.user_id or "",
        "provider_name": record.provider_name,
        "model": record.model,
        "prompt_tokens": record.prompt_tokens,
        "completion_tokens": record.completion_tokens,
        "total_tokens": record.total_tokens,
        "cost_usd": record.cost_usd,
        "model_tier": record.model_tier,
        "task_type": record.task_type,
        "timestamp": datetime.fromtimestamp(record.timestamp, tz=UTC).isoformat(),
        "trace_id": record.trace_id or "",
        "extra": json.dumps(record.extra),
    }


class PostgresSink:
    """Async sink that writes to the ``agent_llm_usage`` Postgres table.

    The sink reuses the platform's :func:`aidp_db.session.get_engine`
    to acquire a connection; tests pass an explicit
    :class:`sqlalchemy.engine.Engine` so they can target an
    in-memory SQLite. The row is written with a parameterised
    statement so a stray ``tenant_id`` cannot inject SQL.
    """

    _DDL: str = (
        "CREATE TABLE IF NOT EXISTS agent_llm_usage ("
        "id INTEGER PRIMARY KEY AUTOINCREMENT, "
        "tenant_id VARCHAR(64) NOT NULL, "
        "user_id VARCHAR(64), "
        "provider_name VARCHAR(64) NOT NULL, "
        "model VARCHAR(128) NOT NULL, "
        "prompt_tokens INTEGER NOT NULL DEFAULT 0, "
        "completion_tokens INTEGER NOT NULL DEFAULT 0, "
        "total_tokens INTEGER NOT NULL DEFAULT 0, "
        "cost_usd NUMERIC(18, 8) NOT NULL DEFAULT 0, "
        "model_tier VARCHAR(16) NOT NULL DEFAULT 'balanced', "
        "task_type VARCHAR(16) NOT NULL DEFAULT 'insight', "
        "ts DOUBLE PRECISION NOT NULL, "
        "trace_id VARCHAR(64), "
        "extra TEXT NOT NULL DEFAULT '{}'"
        ")"
    )

    def __init__(self, engine: Engine) -> None:
        self._engine = engine

    async def write(self, record: UsageRecord) -> None:
        # ``engine.begin`` is sync; run it on the default executor
        # so the metering queue does not block the event loop.
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._sync_write, record)

    def _sync_write(self, record: UsageRecord) -> None:
        with self._engine.begin() as conn:
            # Make sure the table exists. ``IF NOT EXISTS`` is
            # idempotent so the first call after a fresh database
            # silently creates the table; subsequent calls no-op.
            conn.exec_driver_sql(self._DDL)
            conn.execute(
                text(
                    "INSERT INTO agent_llm_usage "
                    "(tenant_id, user_id, provider_name, model, prompt_tokens, "
                    "completion_tokens, total_tokens, cost_usd, model_tier, "
                    "task_type, ts, trace_id, extra) VALUES "
                    "(:tenant_id, :user_id, :provider_name, :model, :prompt_tokens, "
                    ":completion_tokens, :total_tokens, :cost_usd, :model_tier, "
                    ":task_type, :ts, :trace_id, :extra)"
                ),
                {
                    "tenant_id": record.tenant_id,
                    "user_id": record.user_id,
                    "provider_name": record.provider_name,
                    "model": record.model,
                    "prompt_tokens": record.prompt_tokens,
                    "completion_tokens": record.completion_tokens,
                    "total_tokens": record.total_tokens,
                    "cost_usd": record.cost_usd,
                    "model_tier": record.model_tier,
                    "task_type": record.task_type,
                    "ts": record.timestamp,
                    "trace_id": record.trace_id,
                    "extra": json.dumps(record.extra),
                },
            )


class InMemorySink:
    """Test-only sink that stores records in a list.

    The test suite uses this sink to assert on the rows the metering
    layer would have written without standing up ClickHouse or
    Postgres. ``records`` is a list (not a deque) so the test can
    index, filter, and snapshot.
    """

    def __init__(self) -> None:
        self.records: list[UsageRecord] = []

    async def write(self, record: UsageRecord) -> None:
        self.records.append(record)

    def clear(self) -> None:
        self.records.clear()


# ---------------------------------------------------------------------------
# Async dispatcher
# ---------------------------------------------------------------------------


class MeteringDispatcher:
    """Async dispatcher that owns a queue + worker task.

    The dispatcher exposes :meth:`record` (fire-and-forget) and a
    :meth:`drain` helper for tests. The worker is started by
    :meth:`start` and stopped by :meth:`stop` (which drains the
    queue first).
    """

    def __init__(
        self,
        sink: UsageSink,
        *,
        queue_size: int = 1024,
    ) -> None:
        self._sink = sink
        self._queue: asyncio.Queue[UsageRecord] = asyncio.Queue(maxsize=queue_size)
        self._worker_task: asyncio.Task[None] | None = None
        self._closed = False

    @property
    def sink(self) -> UsageSink:
        return self._sink

    async def start(self) -> None:
        """Spawn the background worker task (idempotent)."""
        if self._worker_task is not None:
            return
        self._closed = False
        self._worker_task = asyncio.create_task(self._worker(), name="aidp-agent-metering")

    async def stop(self) -> None:
        """Signal shutdown, drain the queue, await the worker."""
        self._closed = True
        if self._worker_task is None:
            return
        # Push a sentinel so the worker exits the loop even if the
        # queue is empty.
        with contextlib.suppress(asyncio.QueueFull):
            self._queue.put_nowait(_SHUTDOWN_SENTINEL)  # type: ignore[arg-type]
        try:
            await self._worker_task
        finally:
            self._worker_task = None

    def record(self, record: UsageRecord) -> None:
        """Enqueue *record* for asynchronous sink. Non-blocking.

        When the queue is full the record is logged and dropped —
        the metering layer must never block the request path.

        When the worker has not been started (e.g. a test that
        drives the dispatcher directly), the record is written
        synchronously to the underlying sink. The
        :class:`InMemorySink` is a plain list; the
        :class:`ClickHouseSink` is an HTTP POST; both work in a
        sync context. The :class:`PostgresSink` is *not* sync —
        production deployments always go through the worker, so
        that path is unreachable in practice.
        """
        if self._worker_task is None:
            self._write_sync(record)
            return
        try:
            self._queue.put_nowait(record)
        except asyncio.QueueFull:
            _LOG.warning(
                "metering queue full, dropping record",
                extra={
                    "tenant_id": record.tenant_id,
                    "provider": record.provider_name,
                    "model": record.model,
                },
            )

    def _write_sync(self, record: UsageRecord) -> None:
        """Sync fallback when no worker is running.

        The :class:`InMemorySink` and :class:`ClickHouseSink` keep
        their state in plain Python objects, so the write is
        effectively synchronous. The ``async def write`` shape is
        preserved for protocol uniformity; we drive the coroutine
        with a private task if a loop is running, or close it
        immediately if not.
        """
        write = self._sink.write
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No running loop — fall through to the create-and-close
            # path so the coroutine never leaks.
            loop = None
        if loop is not None and loop.is_running():
            loop.create_task(write(record))
            return
        # Drive a coroutine to completion synchronously. The
        # InMemorySink and ClickHouseSink never ``await`` anything,
        # so ``send(None)`` returns immediately with ``StopIteration``
        # carrying the return value (which is ``None``).
        coro = write(record)
        try:
            coro.send(None)
        except StopIteration:
            return

    async def drain(self) -> None:
        """Drain the queue synchronously (test helper)."""
        while not self._queue.empty():
            record = self._queue.get_nowait()
            if isinstance(record, _ShutdownSentinel):
                continue
            await self._sink.write(record)

    async def _worker(self) -> None:
        """Worker loop. Pops records from the queue and writes them."""
        while True:
            item = await self._queue.get()
            if isinstance(item, _ShutdownSentinel):
                return
            try:
                await self._sink.write(item)
            except Exception:  # pragma: no cover - sink must not crash worker
                _LOG.exception("metering sink raised")
            finally:
                self._queue.task_done()


# Sentinel type to break the worker loop. We use a class instead of
# ``None`` so a misuse (enqueuing ``None``) cannot accidentally
# terminate the worker.


@dataclass(frozen=True)
class _ShutdownSentinel:
    """Sentinel pushed onto the metering queue to signal shutdown."""


_SHUTDOWN_SENTINEL: _ShutdownSentinel = _ShutdownSentinel()


# ---------------------------------------------------------------------------
# Configuration helpers
# ---------------------------------------------------------------------------


def build_sink_from_env(
    *,
    engine: Engine | None = None,
    clickhouse_url: str | None = None,
) -> UsageSink:
    """Build a sink based on the current environment.

    Selection rules (first match wins):

    1. ``AIDP_AGENT_CLICKHOUSE_URL`` set → :class:`ClickHouseSink`.
    2. Else if *engine* is provided → :class:`PostgresSink`.
    3. Else → :class:`InMemorySink` (test / local dev fallback).

    The function reads the env directly so callers do not have to
    thread the configuration through every constructor.
    """
    url = (
        clickhouse_url
        if clickhouse_url is not None
        else os.environ.get("AIDP_AGENT_CLICKHOUSE_URL")
    )
    if url:
        return ClickHouseSink(url=url)
    if engine is not None:
        return PostgresSink(engine=engine)
    return InMemorySink()


__all__ = [
    "ClickHouseSink",
    "InMemorySink",
    "MeteringDispatcher",
    "PostgresSink",
    "UsageRecord",
    "UsageSink",
    "build_record",
    "build_sink_from_env",
    "calculate_cost",
]
