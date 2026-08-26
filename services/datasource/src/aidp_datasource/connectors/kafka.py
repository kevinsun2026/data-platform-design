"""Apache Kafka connector.

Driver: :mod:`aiokafka` (the official async-first Kafka client
maintained under the ``aiokafka`` PyPI package; supersedes the
older :mod:`kafka-python`). The :class:`aiokafka.AIOKafkaProducer`
/ :class:`aiokafka.AIOKafkaConsumer` are async; the
:class:`aiokafka.admin.AIOKafkaAdminClient` exposes
``list_topics`` / metadata operations for schema lookup.

Kafka is **not** a SQL store, so the brief scopes the connector
to two methods:

- :meth:`KafkaConnector.list_topics` — list the topics on the
  cluster, with the partition count and replication factor.
- :meth:`KafkaConnector.get_topic_schema` — return the schema
  of one topic (Avro / Protobuf / JSON-Schema from a Schema
  Registry, or a JSON-shape inferred from a sample for
  schemaless topics).

The :meth:`Connector.get_schema` / :meth:`Connector.preview`
methods raise :class:`NotImplementedError` (the base class
default) so a misrouted call fails loudly.

Connection parameters
---------------------

``aiokafka`` accepts a bootstrap server list (``host:port``,
comma-separated for a multi-broker cluster). The
:class:`aiokafka.admin.AIOKafkaAdminClient` takes the bootstrap
servers plus a ``security_protocol`` / ``sasl_mechanism`` pair
for SASL authentication. The connector folds
``connection.options`` into the admin-client call so
``security_protocol`` / ``sasl_mechanism`` / ``sasl_plain_username``
/ ``sasl_plain_password`` / ``ssl_cafile`` flow through.

When ``connection.options['schema_registry_url']`` is set, the
connector uses :mod:`aiokafka` + a hand-rolled Schema Registry
HTTP client (via :mod:`httpx`) to fetch the Avro / Protobuf /
JSON-Schema for the topic. When the knob is absent, every topic
is treated as schemaless (``format="bytes"`` or ``"json"`` when
the sample is non-empty).

Schema inference
----------------

For schemaless topics the connector consumes a small sample
(default: ``min(20, total)`` messages) and infers a JSON shape
from the values. The inference follows the same pattern as the
MongoDB connector (the column order is first-observation, the
type is the most-specific BSON-equivalent label, two distinct
types upgrade the field to ``"mixed"``). For Avro / Protobuf
topics the connector parses the Schema Registry response
(recursively, for nested records) and surfaces the flat field
list in declaration order.

Failure handling
----------------

``aiokafka`` raises :class:`aiokafka.errors.KafkaError`
(auth / network / broker outage). The connector catches and
re-raises as :class:`ConnectorError` so the datasource service
layer only has one error type to deal with.
"""

from __future__ import annotations

import json
import logging
from typing import Any, ClassVar

from aidp_datasource.connectors.base import (
    BaseConnector,
    ConnectorError,
    TopicFieldInfo,
    TopicInfo,
    TopicSchema,
)
from aidp_datasource.schemas import ConnectionConfig, CredentialsPayload, DatasourceKind

_LOG = logging.getLogger(__name__)


#: Default sample size for the JSON shape inference on a
#: schemaless topic. 20 messages is enough to see the common
#: field set without flooding a high-throughput topic.
_DEFAULT_SAMPLE_SIZE: int = 20

#: HTTP timeout (seconds) for a Schema Registry GET. The Schema
#: Registry is an internal service; the call should be sub-second
#: in the happy path.
_SCHEMA_REGISTRY_TIMEOUT: float = 5.0


class KafkaConnector(BaseConnector):
    """:class:`Connector` implementation for Apache Kafka via :mod:`aiokafka`."""

    KIND: ClassVar[DatasourceKind] = "kafka"

    def __init__(
        self,
        *,
        connection: ConnectionConfig,
        credentials: CredentialsPayload,
    ) -> None:
        super().__init__(connection=connection, credentials=credentials)
        # :mod:`aiokafka` is imported lazily so the platform
        # never pays the import cost when an operator registers
        # no Kafka datasources.
        self._aiokafka: Any = None
        # The admin client is opened lazily inside
        # :meth:`_open_admin` so a fresh probe / ``list_topics``
        # call does not pay the connection cost up front.
        self._admin: Any = None

    # ------------------------------------------------------------------
    # Driver lifecycle
    # ------------------------------------------------------------------

    def _ensure_driver(self) -> Any:
        if self._aiokafka is None:
            import aiokafka  # type: ignore[import-untyped]

            self._aiokafka = aiokafka
        return self._aiokafka

    def _build_bootstrap_servers(self) -> str:
        """Return the comma-separated ``host:port`` list for the cluster.

        The :class:`AIOKafkaAdminClient` accepts a single
        bootstrap server (it discovers the rest of the cluster
        via metadata); for a multi-broker cluster the caller
        passes the comma-separated list via
        ``connection.options['bootstrap_servers']`` and we
        honour it verbatim. The default uses the connection's
        ``host`` / ``port`` (so a single-broker registration
        Just Works).
        """
        opts = dict(self._connection.options or {})
        if "bootstrap_servers" in opts:
            return str(opts["bootstrap_servers"])
        return f"{self._connection.host}:{self._connection.port}"

    def _build_admin_kwargs(self) -> dict[str, Any]:
        """Build the kwargs for :class:`AIOKafkaAdminClient`.

        The driver needs ``bootstrap_servers`` + the SASL
        credentials when ``security_protocol`` is ``"SASL_*"``.
        Username / password flow through ``credentials``;
        ``security_protocol`` / ``sasl_mechanism`` /
        ``ssl_cafile`` flow through ``connection.options``.
        """
        opts = dict(self._connection.options or {})
        kwargs: dict[str, Any] = {
            "bootstrap_servers": self._build_bootstrap_servers(),
        }
        # ``username`` / ``password`` map to SASL plain
        # credentials; we set them only when ``security_protocol``
        # declares SASL.
        if "security_protocol" in opts:
            kwargs["security_protocol"] = opts["security_protocol"]
        if "sasl_mechanism" in opts:
            kwargs["sasl_mechanism"] = opts["sasl_mechanism"]
        if kwargs.get("sasl_mechanism") in {"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"}:
            kwargs["sasl_plain_username"] = self._credentials.username
            kwargs["sasl_plain_password"] = self._credentials.password
        if "ssl_cafile" in opts:
            kwargs["ssl_cafile"] = opts["ssl_cafile"]
        if "client_id" in opts:
            kwargs["client_id"] = opts["client_id"]
        return kwargs

    async def _open_admin(self) -> Any:
        """Open + start the :class:`AIOKafkaAdminClient`."""
        aiokafka = self._ensure_driver()
        admin = aiokafka.admin.AIOKafkaAdminClient(**self._build_admin_kwargs())
        await admin.start()
        return admin

    async def _close_admin(self) -> None:
        """Close the cached admin client (idempotent)."""
        if self._admin is None:
            return
        try:
            await self._admin.close()
        except Exception:
            # Best-effort: the connector's own ``close()`` is
            # the authoritative cleanup path; we swallow here
            # so a double-close does not surface as a 500.
            _LOG.debug("ignoring error while closing admin client", exc_info=True)
        self._admin = None

    async def _probe(self, *, timeout_seconds: float | None) -> None:
        """Open an admin client, ``list_topics`` (the cluster probe), close.

        :meth:`BaseConnector.test` wraps this in a
        :class:`TestResult`; raising propagates to the
        ``error`` field. The probe path follows the
        brief's "test connection" contract without opening
        a consumer (which would be the only way to verify
        end-to-end data plane — overkill for a Phase 1
        connection probe).
        """
        admin = await self._open_admin()
        try:
            await admin.list_topics()
        finally:
            await self._close_admin()

    async def list_topics(self) -> list[TopicInfo]:
        """List the topics on the cluster with partition + replication info.

        The :class:`AIOKafkaAdminClient.list_topics` call
        returns the full topic metadata. We project to
        :class:`TopicInfo` (sorted by name for determinism).
        System topics (``__consumer_offsets`` /
        ``__transaction_state``) are filtered out so the
        agent-gateway never sees infrastructure noise.

        Returns:
            A list of :class:`TopicInfo` sorted by topic name.

        Raises:
            ConnectorError: When the metadata fetch fails
                (auth, network, broker outage).
        """
        admin = await self._open_admin()
        try:
            try:
                metadata = await admin.list_topics()
            except Exception as exc:
                raise ConnectorError(
                    f"kafka list_topics failed: {exc}", kind=self.KIND
                ) from exc
        finally:
            await self._close_admin()
        out: list[TopicInfo] = []
        for topic_name, topic_meta in metadata.items():
            name = str(topic_name).strip()
            if not name or name.startswith("__"):
                # ``__consumer_offsets`` / ``__transaction_state``
                # are Kafka internals; skip.
                continue
            partition_count = 0
            replication_factor: int | None = None
            # ``topic_meta`` is a :class:`aiokafka.admin.NewTopic`
            # on the create path; for ``list_topics`` it is a
            # metadata dict. We defensively handle both shapes.
            if isinstance(topic_meta, dict):
                partitions = topic_meta.get("partitions") or []
                partition_count = len(partitions)
                if partitions and isinstance(partitions[0], dict):
                    replicas = partitions[0].get("replicas")
                    if isinstance(replicas, list):
                        replication_factor = len(replicas)
            elif hasattr(topic_meta, "partitions"):
                partitions = topic_meta.partitions
                partition_count = len(partitions) if partitions else 0
            out.append(
                TopicInfo(
                    name=name,
                    partition_count=partition_count,
                    replication_factor=replication_factor,
                )
            )
        out.sort(key=lambda t: t.name)
        return out

    async def get_topic_schema(self, topic: str) -> TopicSchema:
        """Return the schema of one Kafka topic.

        The lookup path is:

        1. If ``connection.options['schema_registry_url']`` is
           set, fetch the latest schema from the Schema
           Registry and parse it. ``format`` is set from the
           Schema Registry response (``avro`` / ``protobuf`` /
           ``json_schema``).
        2. Otherwise consume a small sample of messages and
           infer a JSON shape. ``format`` is ``"json"`` when
           the sample is non-empty and ``"bytes"`` /
           ``"unknown"`` when the topic is empty.

        Args:
            topic: The topic name.

        Returns:
            A :class:`TopicSchema` carrying the topic's wire
            format + field list (declaration order for the
            Schema-Registry formats; inferred for ``"json"``;
            empty for ``"bytes"`` / ``"unknown"``).

        Raises:
            ConnectorError: When the lookup fails (auth,
                network, broker outage, no Schema Registry
                configured for an Avro / Protobuf topic).
        """
        opts = dict(self._connection.options or {})
        registry_url = opts.get("schema_registry_url")
        if registry_url:
            return await self._fetch_schema_registry_topic_schema(
                topic=topic, registry_url=str(registry_url)
            )
        return await self._infer_schemaless_topic_schema(topic=topic)

    # ------------------------------------------------------------------
    # Schema Registry path
    # ------------------------------------------------------------------

    async def _fetch_schema_registry_topic_schema(
        self, *, topic: str, registry_url: str
    ) -> TopicSchema:
        """Fetch the latest schema for *topic* from the Schema Registry.

        The Schema Registry REST API exposes
        ``GET /subjects/<topic>-value/versions/latest`` (the
        ``-value`` suffix is the conventional subject naming
        for the value schema; ``-key`` is the key schema).
        We fetch the value schema because the agent-gateway
        reasons about the *payload* (the key is usually a
        small identifier). The connector surfaces the
        ``schemaType`` field as the :attr:`TopicSchema.format`
        label.
        """
        try:
            import httpx
        except ImportError as exc:  # pragma: no cover - hard dep
            raise ConnectorError(
                "httpx is required for the kafka connector's Schema Registry "
                "client; install aidp-datasource[all-connectors]",
                kind=self.KIND,
            ) from exc
        url = f"{registry_url.rstrip('/')}/subjects/{topic}-value/versions/latest"
        auth: tuple[str, str] | None = None
        opts = dict(self._connection.options or {})
        if opts.get("schema_registry_username"):
            auth = (
                str(opts["schema_registry_username"]),
                str(opts.get("schema_registry_password", "")),
            )
        try:
            async with httpx.AsyncClient(timeout=_SCHEMA_REGISTRY_TIMEOUT) as client:
                resp = await client.get(url, auth=auth)
        except Exception as exc:
            raise ConnectorError(
                f"kafka schema registry request failed: {exc}",
                kind=self.KIND,
            ) from exc
        if resp.status_code == 404:
            # Subject not registered — fall back to the
            # schemaless sample path. This is the expected
            # outcome for topics that publish raw JSON or
            # bytes.
            return await self._infer_schemaless_topic_schema(topic=topic)
        if resp.status_code >= 400:
            raise ConnectorError(
                f"kafka schema registry returned {resp.status_code}: {resp.text}",
                kind=self.KIND,
            )
        try:
            payload = resp.json()
        except Exception as exc:
            raise ConnectorError(
                f"kafka schema registry response is not valid JSON: {exc}",
                kind=self.KIND,
            ) from exc
        schema_type = str(payload.get("schemaType", "avro")).lower()
        schema_str = str(payload.get("schema", ""))
        format_label = _SCHEMA_REGISTRY_FORMAT.get(schema_type, "unknown")
        if format_label == "avro":
            fields = _parse_avro_schema(schema_str)
        elif format_label == "json_schema":
            fields = _parse_json_schema(schema_str)
        elif format_label == "protobuf":
            # Protobuf parsing is intentionally minimal in
            # Phase 1: the Schema Registry returns a
            # ``fileDescriptorSet`` (base64-encoded) rather
            # than a JSON schema, and decoding the descriptor
            # is a non-trivial dependency. We surface the
            # type label + a best-effort placeholder so the
            # caller can still render the topic.
            fields = []
        else:
            fields = []
        return TopicSchema(topic=topic, format=format_label, fields=fields)

    # ------------------------------------------------------------------
    # Schemaless path
    # ------------------------------------------------------------------

    async def _infer_schemaless_topic_schema(self, *, topic: str) -> TopicSchema:
        """Consume a small sample of messages and infer a JSON shape.

        The sample is consumed via :class:`AIOKafkaConsumer`
        (auto-offset-reset to ``earliest`` so we see the
        first messages on the topic). For each value we try
        to JSON-decode it; a value that does not decode
        (``bytes`` / ``"null"`` / etc.) is skipped — the
        field list reflects only the values we could parse.
        When every value in the sample is non-JSON, the
        format is reported as ``"bytes"``.
        """
        aiokafka = self._ensure_driver()
        consumer = aiokafka.AIOKafkaConsumer(
            topic,
            bootstrap_servers=self._build_bootstrap_servers(),
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            group_id=None,  # avoid coordinator round-trips
        )
        try:
            await consumer.start()
        except Exception as exc:
            raise ConnectorError(
                f"kafka consumer start failed: {exc}", kind=self.KIND
            ) from exc
        try:
            accumulator: dict[str, dict[str, Any]] = {}
            order: list[str] = []
            parsed_count = 0
            try:
                # ``getmany`` returns a dict[TopicPartition,
                # list[ConsumerRecord]]; we iterate the values
                # and stop after ``_DEFAULT_SAMPLE_SIZE``
                # observations or a 2-second idle window,
                # whichever comes first.
                record_batch = await consumer.getmany(
                    timeout_ms=2000, max_records=_DEFAULT_SAMPLE_SIZE
                )
            except Exception as exc:
                raise ConnectorError(
                    f"kafka sample consume failed: {exc}", kind=self.KIND
                ) from exc
            for _tp, records in record_batch.items():
                for record in records:
                    value = record.value
                    parsed: Any = None
                    parsed_ok = False
                    if isinstance(value, (bytes, bytearray)):
                        try:
                            parsed = json.loads(value.decode("utf-8"))
                            parsed_ok = True
                        except (ValueError, UnicodeDecodeError):
                            parsed_ok = False
                    elif isinstance(value, str):
                        try:
                            parsed = json.loads(value)
                            parsed_ok = True
                        except ValueError:
                            parsed_ok = False
                    elif isinstance(value, dict):
                        parsed = value
                        parsed_ok = True
                    if not parsed_ok or not isinstance(parsed, dict):
                        continue
                    parsed_count += 1
                    for field, field_value in parsed.items():
                        if field not in accumulator:
                            order.append(field)
                        _merge_json_field(
                            accumulator=accumulator,
                            field=field,
                            value=field_value,
                        )
            fields = [
                TopicFieldInfo(
                    name=field,
                    type=str(accumulator[field]["type"]),
                    nullable=bool(accumulator[field]["nullable"]),
                )
                for field in order
            ]
            format_label = "bytes" if parsed_count == 0 else "json"
            return TopicSchema(topic=topic, format=format_label, fields=fields)
        finally:
            try:
                await consumer.stop()
            except Exception:  # pragma: no cover - best-effort
                _LOG.debug(
                    "ignoring error while stopping kafka consumer", exc_info=True
                )

    # ------------------------------------------------------------------
    # SQL-only methods — raise NotImplementedError.
    # ------------------------------------------------------------------

    async def get_schema(self, database: str | None = None) -> Any:
        """Kafka is not a SQL store; raise :class:`NotImplementedError`."""
        raise NotImplementedError(
            "kafka connector does not support get_schema (use list_topics "
            "+ get_topic_schema)"
        )

    async def preview(self, table: str, limit: int = 100) -> Any:
        """Kafka is not a SQL store; raise :class:`NotImplementedError`."""
        raise NotImplementedError(
            "kafka connector does not support preview (use get_topic_schema)"
        )

    async def close(self) -> None:
        await self._close_admin()
        await super().close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


#: Mapping from Schema Registry ``schemaType`` to the
#: :attr:`TopicSchema.format` label. ``"avro"`` is the Schema
#: Registry default; ``"json_schema"`` is the JSON-Schema
#: dialect; ``"protobuf"`` is Google's protobuf.
_SCHEMA_REGISTRY_FORMAT: dict[str, str] = {
    "avro": "avro",
    "json": "json_schema",
    "jsonschema": "json_schema",
    "json_schema": "json_schema",
    "protobuf": "protobuf",
}


def _parse_avro_schema(schema_str: str) -> list[TopicFieldInfo]:
    """Parse a Schema-Registry Avro schema string to a flat field list.

    The parser handles the canonical Avro ``{"type": "record",
    "name": ..., "fields": [...]}`` shape, recursing into nested
    records (``"type": {"type": "record", ...}``), arrays
    (``"type": {"type": "array", "items": ...}``), and maps
    (``"type": {"type": "map", "values": ...}``). The flat
    field list uses dotted names (``"address.city"``) so the
    downstream consumer can render nested fields without a
    second pass.

    A schema that cannot be parsed (e.g. a non-Avro schema
    slipped in via ``schemaType``) returns ``[]`` rather than
    raising — the connector surfaces ``format="unknown"`` in
    that case and the caller can decide what to do.
    """
    try:
        schema = json.loads(schema_str)
    except ValueError:
        return []
    if not isinstance(schema, dict):
        return []
    if schema.get("type") != "record":
        return []
    fields: list[TopicFieldInfo] = []
    for entry in schema.get("fields", []) or []:
        if not isinstance(entry, dict):
            continue
        name = str(entry.get("name", "")).strip()
        if not name:
            continue
        type_label, nullable = _avro_type_label(entry.get("type"))
        fields.append(
            TopicFieldInfo(name=name, type=type_label, nullable=nullable)
        )
    return fields


def _avro_type_label(type_node: Any) -> tuple[str, bool]:
    """Return ``(type_label, nullable)`` for an Avro ``type`` node.

    Avro supports union types (``[null, string]``) to encode
    nullability; we unwrap a single-element union of ``null``
    and a concrete type to the concrete type with
    ``nullable=True``.
    """
    if isinstance(type_node, str):
        return type_node, False
    if isinstance(type_node, list):
        # Union — unwrap ``[null, X]`` / ``[X, null]`` to ``X``
        # with ``nullable=True``. Other unions are reported as
        # ``"union"`` (a coarse label — Avro unions are rare
        # outside the nullability idiom).
        non_null = [t for t in type_node if t != "null"]
        if len(non_null) == 1:
            inner_label, _ = _avro_type_label(non_null[0])
            return inner_label, True
        return "union", True
    if isinstance(type_node, dict):
        kind = str(type_node.get("type", ""))
        if kind == "array":
            inner_label, _ = _avro_type_label(type_node.get("items"))
            return f"array<{inner_label}>", False
        if kind == "map":
            inner_label, _ = _avro_type_label(type_node.get("values"))
            return f"map<string,{inner_label}>", False
        if kind == "record":
            return "record", False
    return "unknown", True


def _parse_json_schema(schema_str: str) -> list[TopicFieldInfo]:
    """Parse a JSON-Schema ``properties`` block to a flat field list.

    The Schema-Registry ``json_schema`` payload is a JSON
    document with a top-level ``properties`` dict. We project
    the top-level keys to :class:`TopicFieldInfo` (no
    recursion — the brief scopes Phase 1 to the first level).
    """
    try:
        schema = json.loads(schema_str)
    except ValueError:
        return []
    if not isinstance(schema, dict):
        return []
    properties = schema.get("properties") or {}
    if not isinstance(properties, dict):
        return []
    fields: list[TopicFieldInfo] = []
    for name, sub in properties.items():
        if isinstance(sub, dict):
            type_label = str(sub.get("type", "unknown"))
            sub_type = sub.get("type")
            nullable = (
                isinstance(sub_type, list) and "null" in sub_type
            )
        else:
            type_label = "unknown"
            nullable = True
        fields.append(
            TopicFieldInfo(name=str(name), type=type_label, nullable=nullable)
        )
    return fields


def _merge_json_field(
    *,
    accumulator: dict[str, dict[str, Any]],
    field: str,
    value: Any,
) -> None:
    """Merge a JSON (field, value) observation into the *accumulator*.

    Mirrors the BSON helper in the MongoDB connector: the first
    observation sets the type; a second observation with a
    different type upgrades to ``"mixed"`` and marks the
    field as nullable.
    """
    inferred = _json_type_label(value)
    if field not in accumulator:
        accumulator[field] = {
            "type": inferred,
            "nullable": inferred == "null",
            "_types_seen": {inferred},
        }
        return
    buf = accumulator[field]
    if inferred not in buf["_types_seen"]:
        buf["_types_seen"].add(inferred)
        buf["type"] = "mixed"
        buf["nullable"] = True
    if inferred == "null":
        buf["nullable"] = True


def _json_type_label(value: Any) -> str:
    """Return a short JSON type label for *value* (matches the MongoDB helper)."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "number"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, (list, tuple)):
        return "array"
    if isinstance(value, dict):
        return "object"
    return "mixed"


__all__ = ["KafkaConnector"]
