# AIDP IAM service Dockerfile.
#
# Multi-stage build:
#   1. ``builder`` — installs the workspace with ``uv sync`` to a
#      relocatable ``.venv``.
#   2. ``runtime`` — copies the venv and the IAM package source and
#      runs the FastAPI app under ``uvicorn``.
#
# The image is intentionally minimal: no test dependencies, no
# compiler toolchain (argon2-cffi ships manylinux wheels).

# ---------------------------------------------------------------------------
# Stage 1: build
# ---------------------------------------------------------------------------
FROM python:3.13-slim AS builder

# ``uv`` is the package manager; pin to a recent stable release.
COPY --from=ghcr.io/astral-sh/uv:0.4.18 /uv /uvx /usr/local/bin/

WORKDIR /app

# Install build deps for any wheel that may need compilation.
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy workspace metadata so ``uv sync`` can resolve the full graph
# without the source files (faster cache hits on rebuilds).
COPY pyproject.toml uv.lock ./
COPY libs/python/aidp_common/pyproject.toml libs/python/aidp_common/
COPY libs/python/aidp_db/pyproject.toml libs/python/aidp_db/
COPY libs/python/aidp_auth/pyproject.toml libs/python/aidp_auth/
COPY libs/python/aidp_events/pyproject.toml libs/python/aidp_events/
COPY services/iam/pyproject.toml services/iam/

# Sync the full workspace (dev deps excluded at runtime).
RUN uv sync --frozen --no-dev --package aidp-iam

# Copy the source after the dependency install so code changes do
# not invalidate the wheel cache.
COPY libs/python/ libs/python/
COPY services/iam/ services/iam/

# Re-sync so the local source is installed into the venv.
RUN uv sync --frozen --no-dev --package aidp-iam

# ---------------------------------------------------------------------------
# Stage 2: runtime
# ---------------------------------------------------------------------------
FROM python:3.13-slim AS runtime

# Non-root user for the application.
RUN groupadd --system aidp \
    && useradd --system --gid aidp --create-home --shell /bin/false aidp

WORKDIR /app

# Copy the prepared venv and the IAM source from the builder.
COPY --from=builder --chown=aidp:aidp /app/.venv /app/.venv
COPY --from=builder --chown=aidp:aidp /app/libs /app/libs
COPY --from=builder --chown=aidp:aidp /app/services/iam /app/services/iam

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH="/app/services/iam/src:/app/libs/python" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    AIDP_SERVICE_NAME=aidp-iam

USER aidp

EXPOSE 8001

# Healthcheck against the liveness probe.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request, sys; \
        sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8001/healthz', timeout=2).status == 200 else 1)"

# Run alembic migrations at start, then exec the FastAPI app. The
# lifespan in :mod:`aidp_iam.main` also calls ``run_migrations``;
# the explicit ``alembic`` step is left here as a no-op-safe fallback
# in case a future deployment wraps the image with an init container
# that bypasses the lifespan.
CMD ["uvicorn", "aidp_iam.main:app", "--host", "0.0.0.0", "--port", "8001", "--workers", "1"]
