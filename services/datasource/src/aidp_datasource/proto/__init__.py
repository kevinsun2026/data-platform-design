"""Internal gRPC surface for the Datasource service.

The agent-gateway consumes ``DataSourceService.GetConnection`` to fetch
a live, decrypted connection descriptor for a registered datasource.
The proto file lives next to the generated stubs under
:mod:`aidp_datasource.proto.gen`.

gRPC server lifecycle is managed by :mod:`aidp_datasource.proto.server`,
which the service lifespan starts on FastAPI startup and stops on
shutdown.
"""

from __future__ import annotations
