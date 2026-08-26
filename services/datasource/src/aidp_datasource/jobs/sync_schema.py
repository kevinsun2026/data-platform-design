"""In-process runner for ``POST /datasources/{id}/sync-schema``.

The HTTP handler in :mod:`aidp_datasource.api.schemas` returns
``202 Accepted`` with a job id as soon as the request is
accepted; the actual introspection runs in the background via
FastAPI's :class:`fastapi.BackgroundTasks`. The background
function in this module is the bridge between the
:class:`fastapi.BackgroundTasks` contract and the
:class:`aidp_datasource.services.schema_service.SchemaService`
business logic.

Phase 1 keeps the registry in process memory (a thread-safe
``dict``). The brief notes "后续可换 Celery/RQ" — the public
shape of this module is deliberately Celery-compatible:

- :func:`enqueue_sync_schema_job` returns a job id and does
  not block.
- :func:`run_sync_schema_job` is the unit of work (a Celery
  task would call this directly).
- :func:`get_job` returns the current state for the polling
  endpoint.

A future migration replaces the :class:`SchemaSyncJobRegistry`
with a Redis-backed drop-in; the call sites in the API and
test layers do not change.

Concurrency
-----------

The registry uses a :class:`threading.Lock` to serialise
mutations. The in-process :class:`fastapi.BackgroundTasks`
pool runs tasks in a separate thread (the ``anyio`` worker
thread), so the lock is necessary even though Python's GIL
would otherwise protect the dict.

Error handling
--------------

Any exception raised by :meth:`SchemaService.sync_schema`
is caught and recorded as ``status="failed"`` on the job. The
background task *never* re-raises — the HTTP response has
already been sent by the time the task runs, so an unhandled
exception would only land in the server log.
"""

from __future__ import annotations

import logging
import threading
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from aidp_datasource.services.schema_service import (
    SchemaService,
    SchemaSyncJob,
    SchemaSyncResult,
    default_schema_service,
)

_LOG = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


class SchemaSyncJobRegistry:
    """Process-wide store of in-flight and recently-completed sync jobs.

    The class is intentionally tiny — a ``dict`` + a
    :class:`threading.Lock`. The locking discipline is
    "single dict mutation per critical section"; reads are
    lock-free (the lock is only acquired on the dict's
    pointer swap, which is atomic under CPython).

    A future task replaces this with a Redis-backed
    implementation; the public surface
    (:meth:`create`, :meth:`update`, :meth:`get`,
    :meth:`all_jobs`) stays the same.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._jobs: dict[str, SchemaSyncJob] = {}

    def create(
        self,
        *,
        datasource_id: str,
        tenant_id: str,
    ) -> SchemaSyncJob:
        """Allocate a fresh job in ``"pending"`` state.

        Args:
            datasource_id: The datasource the job will sync.
            tenant_id: The tenant the datasource belongs to.

        Returns:
            The newly-created :class:`SchemaSyncJob`. The job
            is also stored in the registry under
            ``job.job_id`` so a subsequent :meth:`get`
            returns it.
        """
        job = SchemaSyncJob(
            job_id=str(uuid.uuid4()),
            datasource_id=datasource_id,
            tenant_id=tenant_id,
            status="pending",
        )
        with self._lock:
            self._jobs[job.job_id] = job
        return job

    def update(
        self,
        *,
        job_id: str,
        **fields: Any,
    ) -> SchemaSyncJob:
        """Replace the registry entry for *job_id* with a mutated copy.

        Any keyword argument that is a :class:`SchemaSyncJob`
        field (``status`` / ``fingerprint`` / ``table_count``
        / ``changed`` / ``error`` / ``finished_at``) is
        applied. Unknown fields raise :class:`TypeError` —
        the registry is the single source of truth for
        which fields are valid, so a typo at the call site
        surfaces as an error instead of a silently-dropped
        update.

        Returns:
            The updated :class:`SchemaSyncJob`.

        Raises:
            KeyError: When *job_id* is not in the registry.
            TypeError: When an unknown field is supplied.
        """
        valid_fields = {
            "status",
            "fingerprint",
            "table_count",
            "changed",
            "error",
            "finished_at",
        }
        unknown = set(fields) - valid_fields
        if unknown:
            raise TypeError(
                f"unknown SchemaSyncJob field(s): {sorted(unknown)}"
            )
        with self._lock:
            current = self._jobs.get(job_id)
            if current is None:
                raise KeyError(job_id)
            replaced = SchemaSyncJob(
                job_id=current.job_id,
                datasource_id=current.datasource_id,
                tenant_id=current.tenant_id,
                created_at=current.created_at,
                **{
                    **{
                        "status": current.status,
                        "fingerprint": current.fingerprint,
                        "table_count": current.table_count,
                        "changed": current.changed,
                        "error": current.error,
                        "finished_at": current.finished_at,
                    },
                    **fields,
                },
            )
            self._jobs[job_id] = replaced
            return replaced

    def get(self, *, job_id: str) -> SchemaSyncJob | None:
        """Return the current state of *job_id* or ``None``."""
        return self._jobs.get(job_id)

    def all_jobs(self) -> list[SchemaSyncJob]:
        """Return a snapshot list of every job in the registry.

        The list is a copy, so callers can mutate it freely.
        Used by the test suite to assert "all jobs finished"
        after a batch of background tasks.
        """
        return list(self._jobs.values())

    def reset(self) -> None:
        """Drop every job. Used by the test suite between cases."""
        with self._lock:
            self._jobs.clear()


#: Process-wide singleton. Tests can replace it via
#: :func:`set_job_registry`.
_DEFAULT_REGISTRY: SchemaSyncJobRegistry = SchemaSyncJobRegistry()


def get_job_registry() -> SchemaSyncJobRegistry:
    """Return the process-wide :class:`SchemaSyncJobRegistry`."""
    return _DEFAULT_REGISTRY


def set_job_registry(registry: SchemaSyncJobRegistry | None) -> None:
    """Override the process-wide registry (used by tests)."""
    global _DEFAULT_REGISTRY
    _DEFAULT_REGISTRY = registry if registry is not None else SchemaSyncJobRegistry()


# ---------------------------------------------------------------------------
# Background runner
# ---------------------------------------------------------------------------


def enqueue_sync_schema_job(
    *,
    tenant_id: str,
    actor: str,
    datasource_id: str,
    database: str | None = None,
    service: SchemaService | None = None,
    registry: SchemaSyncJobRegistry | None = None,
) -> SchemaSyncJob:
    """Allocate a job and return the (still-pending) handle.

    The actual introspection does **not** run here — the
    caller is expected to pass :func:`run_sync_schema_job` to
    FastAPI's ``BackgroundTasks`` (or to a Celery ``delay``
    call in a future task). Returning the pending job keeps
    the API synchronous: the handler can return ``202
    Accepted`` with the job id immediately.

    Args:
        tenant_id: Tenant the datasource belongs to.
        actor: User id to attribute the audit row to.
        datasource_id: The datasource to sync.
        database: Optional database override.
        service: Optional :class:`SchemaService` override
            (used by tests). ``None`` uses the process-wide
            service.
        registry: Optional :class:`SchemaSyncJobRegistry`
            override. ``None`` uses the process-wide registry.

    Returns:
        The freshly-created :class:`SchemaSyncJob` in
        ``status="pending"`` state.
    """
    reg = registry if registry is not None else get_job_registry()
    job = reg.create(datasource_id=datasource_id, tenant_id=tenant_id)
    # Stash the run args on the job so the background task
    # picks them up without needing a side channel. We do
    # this via the registry's mutation API so a test that
    # spies on the registry sees the assignment.
    _PENDING_RUN_ARGS[job.job_id] = {
        "tenant_id": tenant_id,
        "actor": actor,
        "datasource_id": datasource_id,
        "database": database,
        "service": service if service is not None else default_schema_service(),
    }
    return job


# Per-job run arguments. The dict is keyed by job_id; the
# background task pops the entry after the run completes so
# the dict stays bounded by the number of in-flight jobs. The
# short-lived nature of the dict (entries are removed on
# completion) is acceptable for Phase 1; a Celery / RQ
# migration would carry the args on the task message instead.
_PENDING_RUN_ARGS: dict[str, dict[str, Any]] = {}


def _take_run_args(job_id: str) -> dict[str, Any] | None:
    """Pop the run-args dict for *job_id*, or ``None`` if absent.

    A ``None`` return signals "the job was already taken" or
    "the job was created by a different process" — the
    background task treats both as a no-op.
    """
    return _PENDING_RUN_ARGS.pop(job_id, None)


def run_sync_schema_job(
    job_id: str,
    *,
    registry: SchemaSyncJobRegistry | None = None,
    now: Callable[[], datetime] | None = None,
) -> SchemaSyncResult | None:
    """Execute the actual schema sync for *job_id*.

    The function is the unit-of-work the HTTP handler hands
    to FastAPI's :class:`BackgroundTasks`. It is also the
    function a future Celery task would call directly (the
    ``registry`` kwarg makes it dependency-injectable).

    The function:

    1. Marks the job as ``"running"``.
    2. Calls :meth:`SchemaService.sync_schema` with the
       args stashed by :func:`enqueue_sync_schema_job`.
    3. Updates the job with the result.
    4. Returns the :class:`SchemaSyncResult` (or ``None``
       when no run args were stashed — the job was already
       consumed by another worker).

    The function **never raises**. A failure in the
    introspection or in the DB write is captured as
    ``status="failed"`` on the job; the caller (the
    background task scheduler) gets a structured
    :class:`SchemaSyncResult` back.

    Args:
        job_id: The job to run.
        registry: Optional :class:`SchemaSyncJobRegistry`
            override. ``None`` uses the process-wide registry.
        now: Optional clock for tests. ``None`` uses
            :func:`datetime.now`.

    Returns:
        The :class:`SchemaSyncResult`, or ``None`` if the
        job's run args were not found.
    """
    reg = registry if registry is not None else get_job_registry()
    args = _take_run_args(job_id)
    if args is None:
        _LOG.warning(
            "schema sync background job has no run args; skipping",
            extra={"job_id": job_id},
        )
        return None
    clock = now or (lambda: datetime.now(UTC))
    try:
        reg.update(job_id=job_id, status="running")
        result: SchemaSyncResult = args["service"].sync_schema(
            tenant_id=args["tenant_id"],
            actor=args["actor"],
            datasource_id=args["datasource_id"],
            job_id=job_id,
            database=args.get("database"),
        )
        # Update the registry with the terminal state. We
        # use ``status`` from the result so the registry's
        # ``"failed"`` path lands on the wire correctly.
        reg.update(
            job_id=job_id,
            status=result.status,
            fingerprint=result.fingerprint,
            table_count=result.table_count,
            changed=result.changed,
            error=result.error,
            finished_at=clock(),
        )
        return result
    except Exception as exc:  # pragma: no cover - defensive
        _LOG.exception(
            "schema sync background job raised",
            extra={"job_id": job_id},
        )
        reg.update(
            job_id=job_id,
            status="failed",
            error=_truncate(str(exc)),
            finished_at=clock(),
        )
        return None


def _truncate(value: str, *, limit: int = 1024) -> str:
    """Cap a string at *limit* characters with a trailing ellipsis marker."""
    if len(value) <= limit:
        return value
    return value[: limit - 3] + "..."


__all__ = [
    "SchemaSyncJobRegistry",
    "enqueue_sync_schema_job",
    "get_job_registry",
    "run_sync_schema_job",
    "set_job_registry",
]
