"""Pydantic v2 request / response models for the Datasource service.

The HTTP layer in :mod:`aidp_datasource.api` projects ORM rows onto these
models. The split is the platform-standard
``api → service → model → schema`` layering: the ORM models are mutable
SQLAlchemy rows, while the Pydantic models are the immutable wire
shape that the client sees.

Notes on Pydantic v2
--------------------

- Every model has ``model_config = ConfigDict(extra="forbid", ...)`` so
  a misnamed field from a downstream caller surfaces as a 400 instead
  of being silently dropped.
- ``connection_json`` is the public shape; the service uses
  ``connection_json`` internally because Pydantic / SQLAlchemy naming
  is split (the brief calls this out).
- The credential blob is **never** echoed back to the caller. The
  :class:`DatasourceResponse` model is the wire format the REST
  endpoints return; the credential ciphertext is consumed only by
  :func:`aidp_datasource.services.datasource_service.get_decrypted_connection`
  and the internal gRPC server.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

# ---------------------------------------------------------------------------
# Connection + credential shapes
# ---------------------------------------------------------------------------

#: The set of datasource kinds the service supports. Strings are kept
#: as literals (not an ``Enum``) so Pydantic / SQLAlchemy / gRPC all
#: share the same wire format without explicit conversion.
#:
#: The seven kinds cover the Phase 1 matrix:
#:
#: - ``"postgresql"`` / ``"mysql"`` / ``"oracle"`` — relational
#:   databases (Task 14).
#: - ``"hive"`` — Hive data warehouse (Task 14).
#: - ``"mongodb"`` — document store; introspected via
#:   ``listCollections`` + per-collection sample rows (Task 16).
#: - ``"doris"`` — real-time analytical database; the MySQL protocol
#:   wire format is reused via ``pymysql`` (Task 16).
#: - ``"kafka"`` — message queue; supports ``list_topics`` +
#:   ``get_topic_schema`` (Avro / JSON / Protobuf introspection) and
#:   does **not** support ``get_schema`` / ``preview`` (Task 16).
DatasourceKind = Literal[
    "postgresql",
    "mysql",
    "oracle",
    "hive",
    "mongodb",
    "doris",
    "kafka",
]

#: Canonical kind values — exposed as module constants so the
#: connector factory and the test suite can refer to them by name.
KIND_POSTGRESQL: DatasourceKind = "postgresql"
KIND_MYSQL: DatasourceKind = "mysql"
KIND_ORACLE: DatasourceKind = "oracle"
KIND_HIVE: DatasourceKind = "hive"
KIND_MONGODB: DatasourceKind = "mongodb"
KIND_DORIS: DatasourceKind = "doris"
KIND_KAFKA: DatasourceKind = "kafka"

SUPPORTED_KINDS: frozenset[DatasourceKind] = frozenset(
    {
        KIND_POSTGRESQL,
        KIND_MYSQL,
        KIND_ORACLE,
        KIND_HIVE,
        KIND_MONGODB,
        KIND_DORIS,
        KIND_KAFKA,
    }
)


class ConnectionConfig(BaseModel):
    """Non-secret connection descriptor for one datasource.

    The shape is open (``dict[str, Any]``) so the four connectors
    can each surface their own knobs (e.g. ``sslmode`` for Postgres,
    ``service_name`` for Oracle, ``auth`` for Hive). The schema does
    **not** validate the keys — each connector validates the keys it
    cares about in :meth:`Connector.test`.

    The contract is:

    - ``host`` (``str``) is required for all four kinds.
    - ``port`` (``int``) is required for all four kinds.
    - ``database`` (``str``) is required for PG / MySQL / Oracle.
      Hive uses it as the default database (optional).
    - ``options`` (``dict[str, Any]``) is reserved for driver-specific
      knobs (``sslmode``, ``application_name``, ``connect_timeout``,
      etc.). Stored verbatim.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=False)

    host: str = Field(min_length=1, max_length=255, description="Hostname or IP of the datasource.")
    port: int = Field(ge=1, le=65535, description="TCP port of the datasource.")
    database: str | None = Field(
        default=None,
        max_length=255,
        description="Default database / catalog / schema name. Required for PG/MySQL/Oracle.",
    )
    options: dict[str, Any] = Field(
        default_factory=dict,
        description="Driver-specific knobs (sslmode, application_name, ...).",
    )


class CredentialsPayload(BaseModel):
    """The secret half of a datasource — never echoed back to the client.

    Attributes:
        username: Plaintext username. Encrypted at rest via the
            :class:`aidp_datasource.services.credential_service.CredentialService`.
        password: Plaintext password. Encrypted at rest.
        extra: Driver-specific credential knobs (e.g. ``service_name``
            for Oracle, ``auth`` for Hive Kerberos). Encrypted at rest
            as a single JSON blob alongside username / password.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=False)

    username: str = Field(
        min_length=1, max_length=128, description="Datasource login username."
    )
    password: str = Field(
        min_length=1,
        max_length=4096,
        description="Datasource login password (or Kerberos keytab / token, etc).",
    )
    extra: dict[str, Any] = Field(
        default_factory=dict,
        description="Driver-specific credential knobs (Oracle service_name, Hive auth, ...).",
    )


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------


class DatasourceCreateRequest(BaseModel):
    """Body of ``POST /api/v1/datasources``.

    The full connection + credentials are required; there is no
    "create-empty-then-update" round-trip in Phase 1. A
    :class:`ValidationError` (400) is returned when ``kind`` is not
    one of the four supported values, when ``connection.host`` /
    ``connection.port`` are missing, or when ``credentials.username``
    / ``credentials.password`` are missing.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=False)

    name: str = Field(
        min_length=1,
        max_length=128,
        description="Human-readable label (unique per tenant).",
    )
    kind: DatasourceKind = Field(
        description=(
            "Driver kind. One of: postgresql, mysql, oracle, hive, "
            "mongodb, doris, kafka."
        )
    )
    env: str = Field(
        default="prod",
        min_length=1,
        max_length=16,
        description="Deployment environment label (dev/staging/prod).",
    )
    description: str = Field(
        default="", max_length=512, description="Optional free-form description."
    )
    connection: ConnectionConfig = Field(
        description="Non-secret connection descriptor (host/port/database/options).",
    )
    credentials: CredentialsPayload = Field(
        description="Secret credentials (encrypted at rest; never echoed back).",
    )
    tags: list[str] = Field(
        default_factory=list,
        max_length=32,
        description="Free-form labels for the ``?tag=`` list filter.",
    )
    enabled: bool = Field(
        default=True,
        description="Soft-disable flag; false skips the datasource on test/get_schema.",
    )

    @field_validator("env")
    @classmethod
    def _validate_env(cls, value: str) -> str:
        normalized = value.strip().lower()
        if normalized not in {"dev", "staging", "prod", "test"}:
            raise ValueError("env must be one of: dev, staging, prod, test")
        return normalized

    @field_validator("tags")
    @classmethod
    def _validate_tags(cls, value: list[str]) -> list[str]:
        cleaned: list[str] = []
        for raw in value:
            tag = raw.strip()
            if not tag:
                raise ValueError("tag must be a non-empty string")
            if len(tag) > 64:
                raise ValueError("tag must be at most 64 characters")
            cleaned.append(tag)
        return cleaned


class DatasourceUpdateRequest(BaseModel):
    """Body of ``PUT /api/v1/datasources/{id}``.

    All fields are optional so a caller can patch one knob at a
    time. ``kind`` is **immutable** after creation (changing the
    kind would silently change the connector); callers must
    soft-delete + re-create to switch driver types.
    ``credentials`` is *also* immutable here — rotating the
    password is a separate concern in Phase 1 (deliberately
    not implemented to keep the surface small).
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=False)

    name: str | None = Field(default=None, min_length=1, max_length=128)
    env: str | None = Field(default=None, min_length=1, max_length=16)
    description: str | None = Field(default=None, max_length=512)
    connection: ConnectionConfig | None = Field(default=None)
    tags: list[str] | None = Field(default=None, max_length=32)
    enabled: bool | None = Field(default=None)

    @field_validator("env")
    @classmethod
    def _validate_env(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip().lower()
        if normalized not in {"dev", "staging", "prod", "test"}:
            raise ValueError("env must be one of: dev, staging, prod, test")
        return normalized

    @field_validator("tags")
    @classmethod
    def _validate_tags(cls, value: list[str] | None) -> list[str] | None:
        if value is None:
            return None
        cleaned: list[str] = []
        for raw in value:
            tag = raw.strip()
            if not tag:
                raise ValueError("tag must be a non-empty string")
            if len(tag) > 64:
                raise ValueError("tag must be at most 64 characters")
            cleaned.append(tag)
        return cleaned


class DatasourceResponse(BaseModel):
    """Body of ``GET /api/v1/datasources/{id}`` (and the POST response).

    The credential ciphertext / nonce / aad columns are **never**
    included in the response. Only the non-secret
    ``connection`` block is echoed.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=False)

    id: str = Field(min_length=1, max_length=36)
    tenant_id: str = Field(min_length=1, max_length=36)
    name: str
    kind: DatasourceKind
    env: str
    description: str
    connection: dict[str, Any]
    tags: list[str]
    enabled: bool
    created_at: datetime
    updated_at: datetime


class DatasourceListResponse(BaseModel):
    """Body of ``GET /api/v1/datasources``."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=False)

    items: list[DatasourceResponse]


# ---------------------------------------------------------------------------
# Connection test
# ---------------------------------------------------------------------------


class ConnectionTestRequest(BaseModel):
    """Body of ``POST /api/v1/datasources/{id}/test``.

    No fields today; the connection descriptor is the registered one.
    Kept as a Pydantic model so a future ``{"timeout_ms": ...}`` or
    ``{"statement": "SELECT 1"}`` knob lands without a route
    signature change.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=False)

    timeout_seconds: float | None = Field(
        default=None,
        ge=0.1,
        le=120.0,
        description="Optional per-test timeout (seconds). Falls back to the connector default.",
    )


class ConnectionTestResponse(BaseModel):
    """Body of ``POST /api/v1/datasources/{id}/test``.

    The handler returns 200 on both success and most failures (so a
    caller can render the failure directly). Only a 5xx or a 404
    crosses into the error envelope. ``latency_ms`` is ``None`` when
    the test failed before timing the connect.
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=False)

    datasource_id: str
    status: str = Field(description="One of: succeeded, failed, disabled.")
    latency_ms: float | None = None
    error: str | None = None


# ---------------------------------------------------------------------------
# Supported types
# ---------------------------------------------------------------------------


class DatasourceTypeInfo(BaseModel):
    """One entry in ``GET /api/v1/datasources/types``.

    Carries the canonical kind + a human-readable label. The label
    is intentionally stable (no translation today; future i18n
    can add a layer on top).
    """

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=False)

    kind: DatasourceKind
    label: str
    # Capabilities the UI can advertise. Each flag is intentionally
    # cheap to compute (no probe at request time); the values are
    # driven by the connector's static self-description.
    supports_test: bool = True
    supports_get_schema: bool = True
    supports_preview: bool = True


class DatasourceTypesResponse(BaseModel):
    """Body of ``GET /api/v1/datasources/types``."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=False)

    items: list[DatasourceTypeInfo]


__all__ = [
    "KIND_DORIS",
    "KIND_HIVE",
    "KIND_KAFKA",
    "KIND_MONGODB",
    "KIND_MYSQL",
    "KIND_ORACLE",
    "KIND_POSTGRESQL",
    "SUPPORTED_KINDS",
    "ConnectionConfig",
    "ConnectionTestRequest",
    "ConnectionTestResponse",
    "CredentialsPayload",
    "DatasourceCreateRequest",
    "DatasourceKind",
    "DatasourceListResponse",
    "DatasourceResponse",
    "DatasourceTypeInfo",
    "DatasourceTypesResponse",
    "DatasourceUpdateRequest",
]
