from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class HandleState(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CREATED: _ClassVar[HandleState]
    INITIALIZED: _ClassVar[HandleState]
    DATA_PROCESSED: _ClassVar[HandleState]
    CLOSED: _ClassVar[HandleState]
CREATED: HandleState
INITIALIZED: HandleState
DATA_PROCESSED: HandleState
CLOSED: HandleState

class StateRequest(_message.Message):
    __slots__ = ("version", "statefulProcessorHandleCall", "listStateCall")
    VERSION_FIELD_NUMBER: _ClassVar[int]
    STATEFULPROCESSORHANDLECALL_FIELD_NUMBER: _ClassVar[int]
    LISTSTATECALL_FIELD_NUMBER: _ClassVar[int]
    version: int
    statefulProcessorHandleCall: StatefulProcessorHandleCall
    listStateCall: ListStateCall
    def __init__(self, version: _Optional[int] = ..., statefulProcessorHandleCall: _Optional[_Union[StatefulProcessorHandleCall, _Mapping]] = ..., listStateCall: _Optional[_Union[ListStateCall, _Mapping]] = ...) -> None: ...

class StateResponse(_message.Message):
    __slots__ = ("statusCode", "errorMessage")
    STATUSCODE_FIELD_NUMBER: _ClassVar[int]
    ERRORMESSAGE_FIELD_NUMBER: _ClassVar[int]
    statusCode: int
    errorMessage: str
    def __init__(self, statusCode: _Optional[int] = ..., errorMessage: _Optional[str] = ...) -> None: ...

class StatefulProcessorHandleCall(_message.Message):
    __slots__ = ("setHandleState", "getListState")
    SETHANDLESTATE_FIELD_NUMBER: _ClassVar[int]
    GETLISTSTATE_FIELD_NUMBER: _ClassVar[int]
    setHandleState: SetHandleState
    getListState: GetListState
    def __init__(self, setHandleState: _Optional[_Union[SetHandleState, _Mapping]] = ..., getListState: _Optional[_Union[GetListState, _Mapping]] = ...) -> None: ...

class GetListState(_message.Message):
    __slots__ = ("stateName", "schema")
    STATENAME_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    stateName: str
    schema: str
    def __init__(self, stateName: _Optional[str] = ..., schema: _Optional[str] = ...) -> None: ...

class ListStateCall(_message.Message):
    __slots__ = ("exists", "get", "update")
    EXISTS_FIELD_NUMBER: _ClassVar[int]
    GET_FIELD_NUMBER: _ClassVar[int]
    UPDATE_FIELD_NUMBER: _ClassVar[int]
    exists: Exists
    get: Get
    update: Update
    def __init__(self, exists: _Optional[_Union[Exists, _Mapping]] = ..., get: _Optional[_Union[Get, _Mapping]] = ..., update: _Optional[_Union[Update, _Mapping]] = ...) -> None: ...

class Exists(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class Get(_message.Message):
    __slots__ = ("stateName",)
    STATENAME_FIELD_NUMBER: _ClassVar[int]
    stateName: str
    def __init__(self, stateName: _Optional[str] = ...) -> None: ...

class Update(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class SetHandleState(_message.Message):
    __slots__ = ("state",)
    STATE_FIELD_NUMBER: _ClassVar[int]
    state: HandleState
    def __init__(self, state: _Optional[_Union[HandleState, str]] = ...) -> None: ...
