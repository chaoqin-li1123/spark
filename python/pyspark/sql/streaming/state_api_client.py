#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from enum import Enum
import os
import socket
import pyarrow as pa
from typing import Any, TYPE_CHECKING, Iterator, Union, cast

import pyspark.sql.streaming.StateMessage_pb2 as stateMessage
from pyspark.serializers import write_int, read_int
from pyspark.sql.types import StructType, _parse_datatype_string, StructField, LongType

if TYPE_CHECKING:
    from pyspark.sql.pandas._typing import DataFrameLike as PandasDataFrameLike

class StatefulProcessorHandleState(Enum):
    CREATED = 1
    INITIALIZED = 2
    DATA_PROCESSED = 3
    CLOSED = 4

class StateApiClient:
    def __init__(
        self,
        state_server_port: int) -> None:
        self._client_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server_address = './uds_socket'
        self._client_socket.connect(server_address)
        self.sockfile = self._client_socket.makefile("rwb", int(os.environ.get("SPARK_BUFFER_SIZE", 65536)))
        print(f"client is ready - connection established")
        self.handle_state = StatefulProcessorHandleState.CREATED
        print(f"handle state is {self.handle_state}")
        print(f"setting handle state to INITIALIZED")
        self.setHandleState(StatefulProcessorHandleState.INITIALIZED)
        print(f"handle state is {self.handle_state}")
        print(f"getting list state")
        schema = StructType([
            StructField("value", LongType(), True),
            StructField("timestamp", LongType(), True)
        ])
        self.getListState("state1", schema)
        print(f"list state is initialized")

        for i in range(6):
            print(f"calling list state get at attempt {i}")
            self.listStateCallGet("state1")
            print(f"reading arrow batches from socket and writing to a buffer")
            reader = pa.ipc.open_stream(self.sockfile)
            for batch in reader:
                print(f"Read arrow batch with attempt {i}: {batch.to_pandas()}")
            print(f"finished calling list state get")
        print(f"setting handle state to CLOSED")
        self.setHandleState(StatefulProcessorHandleState.CLOSED)
        print(f"handle state is {self.handle_state}")

    
    def setHandleState(self, state: StatefulProcessorHandleState) -> None:
        print(f"setting handle state to {state}")
        proto_state = self._get_proto_state(state)
        set_handle_state = stateMessage.SetHandleState(state=proto_state)
        handle_call = stateMessage.StatefulProcessorHandleCall(setHandleState=set_handle_state)
        message = stateMessage.StateRequest(statefulProcessorHandleCall=handle_call)
        
        self._send_proto_message(message)
        status = read_int(self.sockfile)

        if (status == 0):
            self.handle_state = state
        print(f"status= {status}")

    
    def getListState(self, state_name: str, schema: Union[StructType, str]) -> None:
        print("initializing list state")
        if isinstance(schema, str):
            schema = cast(StructType, _parse_datatype_string(schema))
        
        get_list_state = stateMessage.GetListState()
        get_list_state.stateName = state_name
        get_list_state.schema = schema.json()
        call = stateMessage.StatefulProcessorHandleCall(getListState=get_list_state)

        message = stateMessage.StateRequest(statefulProcessorHandleCall=call)
                
        self._send_proto_message(message)
        status = read_int(self.sockfile)
        print(f"status= {status}")

    def listStateCallGet(self, state_name: str) -> Iterator["PandasDataFrameLike"]:
        get_call = stateMessage.Get()
        get_call.stateName = state_name
        call = stateMessage.ListStateCall(get=get_call)

        message = stateMessage.StateRequest(listStateCall=call)

        self._send_proto_message(message)
        status = read_int(self.sockfile)
        print(f"status= {status}")
        return iter([])
        

    def _get_proto_state(self, state: StatefulProcessorHandleState) -> stateMessage.HandleState.ValueType:
        if (state == StatefulProcessorHandleState.CREATED):
            return stateMessage.CREATED
        elif (state == StatefulProcessorHandleState.INITIALIZED):
            return stateMessage.INITIALIZED
        elif (state == StatefulProcessorHandleState.DATA_PROCESSED):
            return stateMessage.DATA_PROCESSED
        else:
            return stateMessage.CLOSED
        
    def _send_proto_message(self, message: stateMessage.StateRequest) -> None:
        serialized_msg = message.SerializeToString()
        print(f"sending message -- len = {len(serialized_msg)} {str(message)} {str(serialized_msg)}")
        write_int(0, self.sockfile)
        write_int(len(serialized_msg), self.sockfile)
        self.sockfile.write(serialized_msg)
        self.sockfile.flush()