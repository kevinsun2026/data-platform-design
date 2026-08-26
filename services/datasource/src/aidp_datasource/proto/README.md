# Generating the gRPC stubs

The committed stubs under ``gen/`` were generated with
``grpcio-tools==1.83.x``. To regenerate after a ``.proto`` change::

    # From the monorepo root.
    .venv/bin/python -m grpc_tools.protoc \
        -Iservices/datasource/src/aidp_datasource/proto \
        --python_out=services/datasource/src/aidp_datasource/proto/gen \
        --pyi_out=services/datasource/src/aidp_datasource/proto/gen \
        --grpc_python_out=services/datasource/src/aidp_datasource/proto/gen \
        services/datasource/src/aidp_datasource/proto/datasource.proto

    # ``grpc_tools.protoc`` writes top-level ``import datasource_pb2`` —
    # the package needs to be importable as
    # ``aidp_datasource.proto.gen``, so rewrite the import in the
    # generated ``*_grpc.py``:
    .venv/bin/python -c "
    import pathlib
    p = pathlib.Path('services/datasource/src/aidp_datasource/proto/gen/datasource_pb2_grpc.py')
    p.write_text(p.read_text().replace(
        'import datasource_pb2 as datasource__pb2',
        'from aidp_datasource.proto.gen import datasource_pb2 as datasource__pb2',
    ))
    "

If you prefer, the same logic is encoded in
``tools/regen_proto.py`` (not committed — kept here as a
note-to-self).

The stubs are committed so the package installs without
``grpcio-tools``. ``grpcio`` (the runtime) is enough to import
``aidp_datasource.proto.gen.datasource_pb2`` and the gRPC
server uses it directly via ``grpc.aio.server``.
