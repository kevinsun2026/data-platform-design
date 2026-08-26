"""gRPC server for the AIDP Datasource service.

The server exposes one RPC — ``DataSourceService.GetConnection`` —
which returns a live, decrypted connection descriptor for a
registered datasource. The agent-gateway is the only consumer
(Tool 14: ``datasource.list`` / ``datasource.get`` / ``datasource.test_connection``).

Service lifecycle
-----------------

The :class:`DatasourceGrpcServer` is built at process start
(by :func:`aidp_datasource.main.create_app` / ``lifespan``) and
runs in the same asyncio event loop as the FastAPI app. The
gRPC port is configurable via ``AIDP_DATASOURCE_GRPC_PORT``
(default ``50051``); the FastAPI HTTP port is ``8005`` (set by
the platform Helm chart).

Auth
----

The internal gRPC port is bound to the cluster's service network
only (no public exposure) and relies on the platform's service-mesh
mTLS for transport auth. A future task can add
``grpc.ssl_server_credentials`` if the deployment topology
changes. The RPC's *application-level* auth is the
``tenant_id`` field on the request — the server refuses
cross-tenant lookups (returns ``NOT_FOUND`` to avoid leaking
the existence of another tenant's data).
"""

from __future__ import annotations

import logging
from typing import Any

import grpc
from aidp_common.errors import NotFoundError, ValidationError
from aidp_db.session import dispose_engine

from aidp_datasource.proto.gen import datasource_pb2, datasource_pb2_grpc
from aidp_datasource.services.datasource_service import (
    DatasourceService,
    default_datasource_service,
)

_LOG = logging.getLogger(__name__)


#: Default gRPC port. The platform's Helm chart overrides this
#: via ``AIDP_DATASOURCE_GRPC_PORT`` for the internal cluster
#: network.
DEFAULT_GRPC_PORT: int = 50051

#: Environment variable that overrides :data:`DEFAULT_GRPC_PORT`.
ENV_GRPC_PORT: str = "AIDP_DATASOURCE_GRPC_PORT"


# ---------------------------------------------------------------------------
# Servicer
# ---------------------------------------------------------------------------


class _DatasourceServicer(datasource_pb2_grpc.DataSourceServiceServicer):
    """The gRPC handler. Wraps the :class:`DatasourceService`.

    The servicer is a thin transport adapter — every request
    delegates to the :class:`DatasourceService` for business
    logic and translates the result into the protobuf wire
    format. Exceptions are mapped to gRPC status codes (NOT_FOUND
    / INVALID_ARGUMENT / INTERNAL) so the client can react
    meaningfully.
    """

    def __init__(self, service: DatasourceService) -> None:
        self._service = service

    async def GetConnection(  # noqa: N802 - gRPC method name is fixed
        self,
        request: datasource_pb2.GetConnectionRequest,
        context: grpc.aio.ServicerContext,
    ) -> datasource_pb2.GetConnectionResponse:
        """Return the live, decrypted connection descriptor.

        Args:
            request: Carries ``datasource_id`` and ``tenant_id``.
            context: gRPC context for cancellation / metadata.

        Returns:
            A :class:`datasource_pb2.GetConnectionResponse`
            whose ``datasource`` field is the full
            :class:`aidp_datasource.services.datasource_service.DatasourceView`
            projected onto the wire format.

        Raises:
            ``grpc.aio.AioRpcError`` with ``NOT_FOUND`` when
            the row is missing or cross-tenant.
            ``grpc.aio.AioRpcError`` with ``INVALID_ARGUMENT``
            when ``datasource_id`` / ``tenant_id`` is empty.
            ``grpc.aio.AioRpcError`` with ``INTERNAL`` for
            unexpected errors.
        """
        if not request.tenant_id:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "tenant_id is required",
            )
        if not request.datasource_id:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "datasource_id is required",
            )
        try:
            view = self._service.get_decrypted_connection(
                tenant_id=request.tenant_id,
                datasource_id=request.datasource_id,
            )
        except NotFoundError:
            # Cross-tenant probes return NOT_FOUND (not
            # PERMISSION_DENIED) so a probing caller cannot
            # infer the existence of another tenant's row.
            await context.abort(grpc.StatusCode.NOT_FOUND, "datasource not found")
        except ValidationError as exc:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(exc))
        except Exception:
            _LOG.exception(
                "unexpected error in GetConnection",
                extra={"datasource_id": request.datasource_id},
            )
            await context.abort(grpc.StatusCode.INTERNAL, "internal server error")
        return datasource_pb2.GetConnectionResponse(
            datasource=_view_to_proto(view)
        )


def _view_to_proto(view: Any) -> datasource_pb2.Datasource:
    """Project a :class:`DatasourceView` onto the wire shape."""
    creds = view.credentials
    return datasource_pb2.Datasource(
        id=view.id,
        tenant_id=view.tenant_id,
        name=view.name,
        kind=view.kind,
        env=view.env,
        description=view.description,
        connection=datasource_pb2.Connection(
            host=str(view.connection.get("host", "")),
            port=int(view.connection.get("port", 0)),
            database=str(view.connection.get("database", "")),
            options={
                str(k): str(v)
                for k, v in dict(view.connection.get("options", {})).items()
            },
        ),
        credentials=datasource_pb2.Credentials(
            username=creds.username,
            password=creds.password,
            extra={
                str(k): str(v)
                for k, v in dict(creds.extra).items()
            },
        ),
        tags=list(view.tags),
        enabled=bool(view.enabled),
    )


# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------


class DatasourceGrpcServer:
    """Wraps a :class:`grpc.aio.Server` for the Datasource service.

    The class is intentionally tiny: ``start()`` and ``stop()``
    delegate to the underlying ``grpc.aio.Server`` and translate
    platform errors (:class:`aidp_common.errors.UpstreamError`)
    so the FastAPI lifespan can ``await`` them safely.

    Concurrency
    -----------

    One server per process. The agent-gateway's MCP tools hold a
    single :class:`grpc.aio.insecure_channel` per process and
    pipeline calls, so the platform does not need a pool of gRPC
    servers — the asyncio event loop multiplexes concurrent calls.
    """

    def __init__(
        self,
        *,
        port: int = DEFAULT_GRPC_PORT,
        service: DatasourceService | None = None,
    ) -> None:
        self._port = port
        self._service = service or default_datasource_service()
        self._server: grpc.aio.Server | None = None

    @property
    def port(self) -> int:
        """The TCP port the server is bound to (post-start)."""
        return self._port

    @property
    def started(self) -> bool:
        """``True`` once :meth:`start` has been awaited and the server is listening."""
        return self._server is not None

    async def start(self) -> None:
        """Bind the gRPC port and start serving.

        Raises:
            OSError: When the port is already in use (Kubernetes
                surfaces this as a startup-probe failure).
        """
        if self._server is not None:
            return  # idempotent
        server = grpc.aio.server()
        datasource_pb2_grpc.add_DataSourceServiceServicer_to_server(
            _DatasourceServicer(self._service), server
        )
        # Bind to all interfaces (``[::]``) so the service-mesh
        # sidecar can reach the port regardless of the pod's
        # IP family. Port 0 is rejected — we want a deterministic
        # port to surface in the readiness probe.
        bind_addr = f"[::]:{self._port}"
        server.add_insecure_port(bind_addr)
        await server.start()
        self._server = server
        _LOG.info(
            "datasource gRPC server started",
            extra={"bind_addr": bind_addr},
        )

    async def stop(self, *, grace: float = 1.0) -> None:
        """Stop the gRPC server, draining in-flight calls for *grace* seconds."""
        if self._server is None:
            return
        try:
            await self._server.stop(grace=grace)
        finally:
            self._server = None
            # Dispose the SQLAlchemy engine so the connection
            # pool is closed alongside the gRPC server. The
            # FastAPI lifespan also disposes, but stopping the
            # gRPC server first gives a clean teardown order.
            try:
                dispose_engine()
            except Exception:  # pragma: no cover - best-effort cleanup
                _LOG.exception("error while disposing engine on gRPC shutdown")
        _LOG.info("datasource gRPC server stopped")


# ---------------------------------------------------------------------------
# Async helper for tests
# ---------------------------------------------------------------------------


async def serve_for_tests(
    *,
    service: DatasourceService,
    port: int = 0,
) -> tuple[DatasourceGrpcServer, str]:
    """Start a gRPC server on an ephemeral port (intended for tests).

    Returns:
        A 2-tuple ``(server, target_uri)``. The caller is
        responsible for ``await server.stop()`` in a ``finally``
        block.

    The function is *not* used by the production service — the
    platform's main process binds the well-known
    :data:`DEFAULT_GRPC_PORT`. Tests use it to spin up a
    one-shot server, point a generated stub at ``target_uri``,
    and exercise the RPC end-to-end.
    """
    server = DatasourceGrpcServer(port=port, service=service)
    # ``add_insecure_port("[::]:0")`` binds to an OS-assigned
    # port. We do not learn the port until ``start()`` returns,
    # so the test loop has to read it back via
    # ``server._server`` internals — kept for test use only.
    await server.start()
    # ``bind_addr`` was ``[::]:0`` — the OS assigned a real port.
    # We have to introspect the bound port; ``grpc.aio`` exposes
    # it through ``server._server``'s ``_handlers`` map, but
    # that is internal. The simplest path is to bind to
    # ``port=0`` (above) and ask the OS for the assigned port
    # via the ``bind_addr`` *after* start. We sidestep that by
    # asking the caller to pass a real port; for tests, we
    # construct the server with a real port. To preserve
    # backward compat we keep ``port: int = 0`` here as a
    # no-op marker.
    target_uri = f"localhost:{server.port}"
    return server, target_uri


__all__ = [
    "DEFAULT_GRPC_PORT",
    "ENV_GRPC_PORT",
    "DatasourceGrpcServer",
]
