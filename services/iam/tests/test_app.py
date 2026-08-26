"""Smoke tests for the FastAPI app factory in :mod:`aidp_iam.main`.

Task 7 ships only the bootstrap layer (app factory + lifespan +
``/healthz`` / ``/readyz`` endpoints); the real IAM routes (login,
refresh, user CRUD, ...) land in later tasks. These tests pin:

- :func:`aidp_iam.main.create_app` builds a :class:`fastapi.FastAPI`
  with the right title, version, and the standard health endpoints.
- The module-level :data:`aidp_iam.main.app` instance exists (so
  ``uvicorn aidp_iam.main:app`` works without an extra factory
  import).
- ``/healthz`` always returns 200; ``/readyz`` returns 200 when the
  database is reachable.

The ``/readyz`` probe needs a real database, so the test uses the
same in-memory SQLite pattern as :mod:`tests.test_models`.
"""

from __future__ import annotations

from collections.abc import Iterator
from functools import partial

import pytest
from aidp_db.session import get_session
from aidp_iam.main import app, create_app
from aidp_iam.models import Base
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import StaticPool


@pytest.fixture
def in_memory_engine() -> Iterator[Engine]:
    """Yield a fresh in-memory SQLite engine with the IAM schema applied."""
    eng: Engine = create_engine(
        "sqlite:///:memory:",
        future=True,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(eng)
    try:
        yield eng
    finally:
        Base.metadata.drop_all(eng)
        eng.dispose()


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch, in_memory_engine: Engine) -> Iterator[TestClient]:
    """Yield a :class:`TestClient` pointed at a fresh app with the IAM schema.

    The ``/readyz`` endpoint probes ``aidp_db.session.get_engine`` —
    we monkeypatch the module to return the in-memory engine so the
    probe succeeds without a real Postgres.
    """
    # Build a fresh app so the module-level ``app`` is not shared
    # between tests (each test should see its own health state).
    test_app: FastAPI = create_app()
    # Replace the cached engine so /readyz's ``SELECT 1`` probe runs
    # against our schema and not the production URL.
    import aidp_db.session as session_module

    monkeypatch.setattr(session_module, "_engine_cache", {in_memory_engine.url: in_memory_engine})

    # ``get_engine()`` calls ``get_settings().db_url``; route it to
    # the in-memory URL so the cache lookup hits.
    from aidp_common.config import get_settings

    monkeypatch.setattr(get_settings(), "db_url", str(in_memory_engine.url))

    with TestClient(test_app) as c:
        yield c


def test_module_level_app_exists() -> None:
    """The module-level :data:`app` instance is a :class:`FastAPI`."""
    assert isinstance(app, FastAPI)


def test_create_app_returns_fastapi() -> None:
    """``create_app()`` returns a configured :class:`FastAPI`."""
    fastapi_app = create_app()
    assert isinstance(fastapi_app, FastAPI)
    assert fastapi_app.title == "AIDP IAM Service"


def test_healthz_returns_ok(client: TestClient) -> None:
    """``/healthz`` is the liveness probe and always returns 200."""
    resp = client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_readyz_returns_ready_when_db_reachable(
    client: TestClient, in_memory_engine: Engine
) -> None:
    """``/readyz`` returns 200 with the dialect name when the DB is reachable."""
    # Sanity: the in-memory engine actually works.
    with in_memory_engine.connect() as conn:
        conn.execute(text("SELECT 1")).scalar()
    resp = client.get("/readyz")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ready"
    assert body["database"] in {"sqlite", "postgresql"}


def test_get_session_helper_is_callable() -> None:
    """``aidp_db.session.get_session`` is the same helper used by the lifespan."""
    # Smoke check: the import works and the function is callable.
    # (We don't run a real transaction here — that's covered by
    # ``test_models.py``.)
    assert callable(get_session)
    assert callable(partial(get_session))


def test_model_import_path() -> None:
    """The ``aidp_iam.models`` module re-exports the eight table classes."""
    from aidp_iam import models

    expected = {
        "ApiKey",
        "Base",
        "Group",
        "Role",
        "Session",
        "Tenant",
        "User",
        "UserGroupMember",
        "UserRoleBinding",
    }
    for name in expected:
        assert hasattr(models, name), f"aidp_iam.models is missing {name!r}"
