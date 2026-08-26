"""Background-job package for the Datasource service.

The package hosts the in-process sync runner used by the
``POST /api/v1/datasources/{id}/sync-schema`` endpoint. The
runner is intentionally simple — a thread-safe job registry
plus a callable that drives
:class:`aidp_datasource.services.schema_service.SchemaService`
and updates the registry as the run progresses.

A future task graduates the runner to Celery / RQ without
changing the public surface: the HTTP layer still enqueues
a job by id, the worker pool picks the job up, the result
lands in the same registry (or a Redis-backed equivalent
that implements the same ``get_job`` / ``set_job`` API).
"""

from __future__ import annotations
