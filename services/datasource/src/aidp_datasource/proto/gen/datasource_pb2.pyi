from collections.abc import Iterable as _Iterable
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf.internal import containers as _containers

DESCRIPTOR: _descriptor.FileDescriptor

class Connection(_message.Message):
    __slots__ = ("database", "host", "options", "port")
    class OptionsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: str | None = ..., value: str | None = ...) -> None: ...
    OPTIONS_FIELD_NUMBER: _ClassVar[int]
    HOST_FIELD_NUMBER: _ClassVar[int]
    PORT_FIELD_NUMBER: _ClassVar[int]
    DATABASE_FIELD_NUMBER: _ClassVar[int]
    options: _containers.ScalarMap[str, str]
    host: str
    port: int
    database: str
    def __init__(self, options: _Mapping[str, str] | None = ..., host: str | None = ..., port: int | None = ..., database: str | None = ...) -> None: ...

class Credentials(_message.Message):
    __slots__ = ("extra", "password", "username")
    class ExtraEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: str | None = ..., value: str | None = ...) -> None: ...
    USERNAME_FIELD_NUMBER: _ClassVar[int]
    PASSWORD_FIELD_NUMBER: _ClassVar[int]
    EXTRA_FIELD_NUMBER: _ClassVar[int]
    username: str
    password: str
    extra: _containers.ScalarMap[str, str]
    def __init__(self, username: str | None = ..., password: str | None = ..., extra: _Mapping[str, str] | None = ...) -> None: ...

class Datasource(_message.Message):
    __slots__ = ("connection", "credentials", "description", "enabled", "env", "id", "kind", "name", "tags", "tenant_id")
    ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    ENV_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    CONNECTION_FIELD_NUMBER: _ClassVar[int]
    CREDENTIALS_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    ENABLED_FIELD_NUMBER: _ClassVar[int]
    id: str
    tenant_id: str
    name: str
    kind: str
    env: str
    description: str
    connection: Connection
    credentials: Credentials
    tags: _containers.RepeatedScalarFieldContainer[str]
    enabled: bool
    def __init__(self, id: str | None = ..., tenant_id: str | None = ..., name: str | None = ..., kind: str | None = ..., env: str | None = ..., description: str | None = ..., connection: Connection | _Mapping | None = ..., credentials: Credentials | _Mapping | None = ..., tags: _Iterable[str] | None = ..., enabled: bool | None = ...) -> None: ...

class GetConnectionRequest(_message.Message):
    __slots__ = ("datasource_id", "tenant_id")
    DATASOURCE_ID_FIELD_NUMBER: _ClassVar[int]
    TENANT_ID_FIELD_NUMBER: _ClassVar[int]
    datasource_id: str
    tenant_id: str
    def __init__(self, datasource_id: str | None = ..., tenant_id: str | None = ...) -> None: ...

class GetConnectionResponse(_message.Message):
    __slots__ = ("datasource",)
    DATASOURCE_FIELD_NUMBER: _ClassVar[int]
    datasource: Datasource
    def __init__(self, datasource: Datasource | _Mapping | None = ...) -> None: ...
