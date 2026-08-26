"""Tests for the Audit service consumer.

The tests pin the contract that :mod:`aidp_audit.consumer` ships
in Task 10:

- :func:`aidp_audit.consumer.flush_batch` persists one
  ``audit_events`` + ``audit_payloads`` pair per envelope. The
  payload is encrypted with AES-256-GCM (round-trippable via
  :func:`aidp_audit.crypto.decrypt_payload`).
- Idempotency: re-flushing the same ``(tenant_id, event_id)`` pair
  is a no-op (no duplicate row, no overwrite).
- The unique constraint on ``(tenant_id, event_id)`` is the
  consumer's at-least-once / DLQ-recovery backstop.
- Security events are promoted to :class:`SecurityEvent` for the
  types in :data:`aidp_audit.consumer.SECURITY_EVENT_TYPES`.
- The batch-flush loop honours the size / time boundaries
  (:data:`AuditBatch` + :func:`_enqueue_loop`).

The tests use an in-memory SQLite engine wired into the same
``aidp_db.session`` engine cache the SUT consults, so the schema
metadata is identical to production. The schema is created with
``Base.metadata.create_all`` rather than running Alembic, because
the migration test is the focus of the production deploy story and
is not what this consumer test is exercising.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pytest
from aidp_audit.consumer import (
    SECURITY_EVENT_TYPES,
    AuditBatch,
    _enqueue_loop,
    _envelope_to_orm,
    flush_batch,
)
from aidp_audit.crypto import decrypt_payload
from aidp_audit.models import AidpAuditEvent, AuditPayload, Base, SecurityEvent
from aidp_db.session import get_session
from aidp_events.envelope import EventEnvelope, new_envelope
from sqlalchemy import create_engine, select
from sqlalchemy.engine import Engine
from sqlalchemy.pool import StaticPool

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def in_memory_engine() -> Iterator[Engine]:
    """Yield a fresh in-memory SQLite engine with the audit schema applied.

    The testcontainers Postgres + Kafka fixtures are not enabled in
    the current sandbox; the SQLite fallback is the default path
    because it round-trips every code path the production Postgres
    path does (dialect-specific ON CONFLICT aside, which is
    exercised by the SQLAlchemy dialect dispatch).

    A minimal ``tenants`` table is synthesised because the audit
    schema declares FKs to ``tenants.id`` (owned by the IAM service);
    the cross-service FK constraint needs the parent table to exist
    in the test engine. The table is registered directly on the
    audit ``Base.metadata`` so the audit tables' ``ForeignKey("tenants.id")``
    references resolve at ``create_all`` time.
    """
    from sqlalchemy import Column, String, Table

    # ``extend_existing=True`` so a re-import of this module (which
    # happens between test files) does not collide with the
    # already-registered ``tenants`` table.
    Table(
        "tenants",
        Base.metadata,
        Column("id", String(36), primary_key=True),
        Column("code", String(64), nullable=False, unique=True),
        Column("name", String(255), nullable=False),
        Column("plan", String(32), nullable=False, server_default="free"),
        Column(
            "isolation_level",
            String(16),
            nullable=False,
            server_default="l1",
        ),
        Column(
            "region",
            String(32),
            nullable=False,
            server_default="us-east-1",
        ),
        Column("status", String(16), nullable=False, server_default="active"),
        extend_existing=True,
    )

    eng: Engine = create_engine(
        "sqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    from sqlalchemy import event as _event

    @_event.listens_for(eng, "connect")
    def _enable_fk(dbapi_conn, _conn_record):  # pragma: no cover - test helper
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA foreign_keys=ON")
        cur.close()

    Base.metadata.create_all(eng)
    try:
        yield eng
    finally:
        # ``drop_all`` honours FK order, so the tenants stub drops
        # last; removing it from the metadata first would leave
        # the audit tables' FK columns unresolved.
        Base.metadata.drop_all(eng)
        eng.dispose()


@pytest.fixture
def wired_engine(monkeypatch: pytest.MonkeyPatch, in_memory_engine: Engine) -> Iterator[Engine]:
    """Pre-seed the engine cache so ``aidp_db.session.get_session()`` returns *in_memory_engine*.

    Also inserts the ``tenant-a`` and ``tenant-b`` rows so the audit
    events' foreign keys pass.
    """
    import aidp_db.session as db_session

    monkeypatch.setattr(db_session, "_engine_cache", {str(in_memory_engine.url): in_memory_engine})
    from aidp_common.config import get_settings

    monkeypatch.setattr(get_settings(), "db_url", str(in_memory_engine.url))

    from sqlalchemy import text as _text

    with in_memory_engine.begin() as conn:
        for tid, code in (("tenant-a", "acme"), ("tenant-b", "globex")):
            conn.execute(
                _text("INSERT INTO tenants (id, code, name) VALUES (:id, :code, :name)"),
                {"id": tid, "code": code, "name": code},
            )
    try:
        yield in_memory_engine
    finally:
        db_session.reset_engine_cache()


def _envelope(
    *,
    tenant_id: str = "tenant-a",
    event_type: str = "iam.user.logged_in",
    payload: dict[str, object] | None = None,
    event_id: str | None = None,
    occurred_at: datetime | None = None,
) -> EventEnvelope:
    """Build a synthetic envelope for the tests."""
    return new_envelope(
        event_type=event_type,
        tenant_id=tenant_id,
        payload=payload or {"user_id": "u-1", "ip": "10.0.0.1"},
        event_id=event_id,
        occurred_at=occurred_at,
    )


# ---------------------------------------------------------------------------
# Field extraction
# ---------------------------------------------------------------------------


def test_envelope_to_orm_extracts_actor_metadata() -> None:
    """The consumer pulls ``actor_user_id`` / ``actor_ip`` from common payload keys."""
    envelope = _envelope(
        event_type="iam.user.logged_in",
        payload={"user_id": "u-42", "ip": "10.0.0.42", "action": "login"},
    )
    event, payload = _envelope_to_orm(topic="iam.audit", envelope=envelope)
    assert event.tenant_id == envelope.tenant_id
    assert event.event_id == envelope.event_id
    assert event.actor_user_id == "u-42"
    assert event.actor_ip == "10.0.0.42"
    assert event.action == "login"
    assert event.outcome == "success"
    # Payload is encrypted; we just verify the row was created.
    assert payload.ciphertext
    assert payload.nonce
    assert payload.aad


def test_envelope_to_orm_falls_back_to_event_type_suffix_for_action() -> None:
    """When the payload has no ``action`` field, the suffix of ``event_type`` is used."""
    envelope = _envelope(
        event_type="datasource.connection.created",
        payload={"connection_id": "c-1"},
    )
    event, _ = _envelope_to_orm(topic="datasource.connections", envelope=envelope)
    assert event.action == "created"


def test_envelope_to_orm_derives_outcome_and_severity() -> None:
    """``outcome`` / ``severity`` are pulled from the payload when present."""
    envelope = _envelope(
        event_type="iam.user.login_failed",
        payload={"outcome": "failure", "severity": "error"},
    )
    event, _ = _envelope_to_orm(topic="iam.audit", envelope=envelope)
    assert event.outcome == "failure"
    assert event.severity == "error"


def test_envelope_to_orm_security_event_severity_warning_by_default() -> None:
    """Security-grade event types default to ``severity="warning"``."""
    envelope = _envelope(event_type="iam.user.login_failed", payload={})
    event, _ = _envelope_to_orm(topic="iam.audit", envelope=envelope)
    assert event.severity == "warning"


# ---------------------------------------------------------------------------
# Crypto round-trip
# ---------------------------------------------------------------------------


def test_envelope_payload_round_trips_through_encryption() -> None:
    """The payload can be recovered verbatim from the AES-GCM ciphertext."""
    envelope = _envelope(
        event_type="iam.user.logged_in",
        payload={"user_id": "u-99", "ip": "10.0.0.99", "extra": [1, 2, 3]},
    )
    event, payload_row = _envelope_to_orm(topic="iam.audit", envelope=envelope)
    plaintext = decrypt_payload(
        ciphertext=payload_row.ciphertext,
        nonce=payload_row.nonce,
        tenant_id=event.tenant_id,
        event_id=event.event_id,
        event_type=event.event_type,
    )
    decoded = json.loads(plaintext.decode("utf-8"))
    assert decoded == envelope.payload


def test_decrypt_fails_with_wrong_event_type() -> None:
    """GCM auth fails when the AAD columns are tampered with."""
    envelope = _envelope(
        event_type="iam.user.logged_in",
        payload={"user_id": "u-1"},
    )
    _, payload_row = _envelope_to_orm(topic="iam.audit", envelope=envelope)
    from aidp_common.errors import UpstreamError

    with pytest.raises(UpstreamError):
        decrypt_payload(
            ciphertext=payload_row.ciphertext,
            nonce=payload_row.nonce,
            tenant_id=envelope.tenant_id,
            event_id=envelope.event_id,
            event_type="iam.user.tampered",
        )


# ---------------------------------------------------------------------------
# flush_batch
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_flush_batch_persists_audit_events_and_payloads(
    wired_engine: Engine,
) -> None:
    """A flush of N envelopes writes N event rows + N payload rows."""
    envelopes = [
        _envelope(
            tenant_id="tenant-a",
            event_type="iam.user.logged_in",
            payload={"user_id": f"u-{i}", "ip": f"10.0.0.{i}"},
            event_id=f"e-{uuid.uuid4().hex}",
            occurred_at=datetime.now(UTC) - timedelta(seconds=i),
        )
        for i in range(5)
    ]
    inserted = await flush_batch([("iam.audit", env) for env in envelopes])
    assert inserted == 5
    with get_session() as session:
        events = session.execute(select(AidpAuditEvent)).scalars().all()
        payloads = session.execute(select(AuditPayload)).scalars().all()
    assert len(events) == 5
    assert len(payloads) == 5
    assert {e.event_id for e in events} == {e.event_id for e in envelopes}


@pytest.mark.asyncio
async def test_flush_batch_is_idempotent(wired_engine: Engine) -> None:
    """Re-flushing the same envelope is a no-op (no duplicate row)."""
    envelope = _envelope(
        tenant_id="tenant-a",
        event_type="iam.user.logged_in",
        payload={"user_id": "u-1"},
        event_id="e-stable",
    )
    first = await flush_batch([("iam.audit", envelope)])
    second = await flush_batch([("iam.audit", envelope)])
    assert first == 1
    assert second == 0
    with get_session() as session:
        count = (
            session.execute(select(AidpAuditEvent).where(AidpAuditEvent.event_id == "e-stable"))
            .scalars()
            .all()
        )
    assert len(count) == 1


@pytest.mark.asyncio
async def test_flush_batch_promotes_security_events(
    wired_engine: Engine,
) -> None:
    """``SECURITY_EVENT_TYPES`` events are also written to ``security_events``."""
    security_envelope = _envelope(
        tenant_id="tenant-a",
        event_type="iam.user.login_failed",
        payload={"user_id": "u-1", "ip": "10.0.0.1", "reason": "bad_password"},
        event_id="e-sec-1",
    )
    benign_envelope = _envelope(
        tenant_id="tenant-a",
        event_type="iam.user.logged_in",
        payload={"user_id": "u-1"},
        event_id="e-ok-1",
    )
    inserted = await flush_batch(
        [
            ("iam.audit", security_envelope),
            ("iam.audit", benign_envelope),
        ]
    )
    assert inserted == 2
    with get_session() as session:
        security = session.execute(
            select(SecurityEvent).where(SecurityEvent.audit_event_id == "e-sec-1")
        ).scalar_one_or_none()
        benign_promoted = session.execute(
            select(SecurityEvent).where(SecurityEvent.audit_event_id == "e-ok-1")
        ).scalar_one_or_none()
    assert security is not None
    assert security.event_type == "iam.user.login_failed"
    assert security.severity == "warning"
    assert security.reason == "bad_password"
    assert benign_promoted is None


@pytest.mark.asyncio
async def test_flush_batch_can_disable_security_promotion(
    wired_engine: Engine,
) -> None:
    """``promote_security=False`` skips the security_events insert."""
    envelope = _envelope(
        tenant_id="tenant-a",
        event_type="iam.user.login_failed",
        payload={"user_id": "u-1", "reason": "bad_password"},
        event_id="e-sec-2",
    )
    inserted = await flush_batch([("iam.audit", envelope)], promote_security=False)
    assert inserted == 1
    with get_session() as session:
        security = session.execute(
            select(SecurityEvent).where(SecurityEvent.audit_event_id == "e-sec-2")
        ).scalar_one_or_none()
    assert security is None


@pytest.mark.asyncio
async def test_flush_batch_handles_empty_input(wired_engine: Engine) -> None:
    """An empty batch is a no-op (returns 0)."""
    assert await flush_batch([]) == 0


@pytest.mark.asyncio
async def test_flush_batch_writes_one_hundred_events(
    wired_engine: Engine,
) -> None:
    """Step 10.1: send 100 events, verify they all land in the database."""
    envelopes = [
        _envelope(
            tenant_id="tenant-a",
            event_type="iam.user.logged_in",
            payload={"user_id": f"u-{i:03d}", "ip": f"10.0.0.{i % 256}"},
            event_id=f"e-{uuid.uuid4().hex}",
            occurred_at=datetime.now(UTC) - timedelta(seconds=i),
        )
        for i in range(100)
    ]
    inserted = await flush_batch([("iam.audit", env) for env in envelopes])
    assert inserted == 100
    with get_session() as session:
        events = session.execute(select(AidpAuditEvent)).scalars().all()
        payloads = session.execute(select(AuditPayload)).scalars().all()
    assert len(events) == 100
    assert len(payloads) == 100


# ---------------------------------------------------------------------------
# Batch loop
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_enqueue_loop_flushes_on_size_boundary(
    monkeypatch: pytest.MonkeyPatch, wired_engine: Engine
) -> None:
    """The batch loop flushes when the buffer reaches ``flush_batch_size``."""
    # Force a tiny batch size so the test is fast.
    monkeypatch.setenv("AIDP_AUDIT_FLUSH_BATCH_SIZE", "3")
    batch = AuditBatch()
    flushed: list[list[tuple[str, EventEnvelope]]] = []

    async def _handler(current: AuditBatch) -> None:
        flushed.append(list(current.envelopes))

    task = asyncio.create_task(
        _enqueue_loop(
            batch,
            _handler,
            flush_batch_size=3,
            flush_interval=60.0,
        )
    )
    try:
        for i in range(3):
            batch.add("iam.audit", _envelope(event_id=f"e-{i}"))
        # Give the loop a tick to wake up and flush.
        for _ in range(20):
            if flushed:
                break
            await asyncio.sleep(0.05)
        assert flushed
        assert len(flushed[0]) == 3
        assert batch.envelopes == []
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_enqueue_loop_flushes_on_time_boundary(
    monkeypatch: pytest.MonkeyPatch, wired_engine: Engine
) -> None:
    """The batch loop flushes when ``flush_interval`` elapses."""
    batch = AuditBatch()
    flushed: list[list[tuple[str, EventEnvelope]]] = []

    async def _handler(current: AuditBatch) -> None:
        flushed.append(list(current.envelopes))

    task = asyncio.create_task(
        _enqueue_loop(
            batch,
            _handler,
            flush_batch_size=1000,  # unreachable in the test window
            flush_interval=0.1,  # fast
        )
    )
    try:
        batch.add("iam.audit", _envelope(event_id="e-late"))
        # Wait a bit longer than the interval for the flush to fire.
        for _ in range(40):
            if flushed:
                break
            await asyncio.sleep(0.05)
        assert flushed
        assert len(flushed[0]) == 1
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_enqueue_loop_drains_on_cancellation(
    monkeypatch: pytest.MonkeyPatch, wired_engine: Engine
) -> None:
    """On cancellation, a final partial batch is flushed."""
    batch = AuditBatch()
    flushed: list[list[tuple[str, EventEnvelope]]] = []

    async def _handler(current: AuditBatch) -> None:
        flushed.append(list(current.envelopes))

    task = asyncio.create_task(
        _enqueue_loop(
            batch,
            _handler,
            flush_batch_size=1000,  # unreachable
            flush_interval=60.0,  # unreachable
        )
    )
    # Let the task start and enter ``await asyncio.sleep(60)`` so a
    # subsequent cancellation is observed at the await point.
    await asyncio.sleep(0)
    batch.add("iam.audit", _envelope(event_id="e-drain"))
    # Cancel the loop. The shutdown path should flush the partial batch.
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert len(flushed) == 1
    assert len(flushed[0]) == 1


# ---------------------------------------------------------------------------
# SECURITY_EVENT_TYPES
# ---------------------------------------------------------------------------


def test_security_event_types_includes_known_signals() -> None:
    """The security-event allow-list is documented and stable."""
    expected = {
        "iam.user.login_failed",
        "iam.user.password_reset",
        "iam.api_key.revoked",
        "iam.user.role_escalation_denied",
        "security.login.failed",
        "security.permission.denied",
    }
    assert expected.issubset(SECURITY_EVENT_TYPES)
