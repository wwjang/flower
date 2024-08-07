# Copyright 2024 Flower Labs GmbH. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==============================================================================
"""ClientAppIo API servicer."""


import grpc

from flwr.common import Context, Message, typing
from flwr.common.serde import (
    error_from_proto,
    error_to_proto,
    metadata_from_proto,
    metadata_to_proto,
    recordset_from_proto,
    recordset_to_proto,
    status_to_proto,
    user_config_from_proto,
    user_config_to_proto,
)
from flwr.common.typing import Run

# from flwr.common.typing import Code, Status  # TODO: add Fab type
# pylint: disable=E0611
from flwr.proto import appio_pb2_grpc
from flwr.proto.appio_pb2 import (
    PullClientAppInputsRequest,
    PullClientAppInputsResponse,
    PushClientAppOutputsRequest,
    PushClientAppOutputsResponse,
)
from flwr.proto.run_pb2 import Run as ProtoRun
from flwr.proto.transport_pb2 import Context as ProtoContext
from flwr.proto.transport_pb2 import Message as ProtoMessage


class ClientAppIoServicer(appio_pb2_grpc.ClientAppIoServicer):
    """ClientAppIo API servicer."""

    # def __init__(self) -> None:
    # self.message: Message = None
    # self.context: Context = None
    # # self.fab = None
    # self.run: Run = None
    # self.token: int = None

    def PullClientAppInputs(
        self, request: PullClientAppInputsRequest, context: grpc.ServicerContext
    ) -> PullClientAppInputsResponse:
        assert request.token == self.token
        return PullClientAppInputsResponse(
            message=self.proto_message,
            context=self.proto_context,
            # fab=self.fab,
            run=self.proto_run,
        )

    def PushClientAppOutputs(
        self, request: PushClientAppOutputsRequest, context: grpc.ServicerContext
    ) -> PushClientAppOutputsResponse:
        assert request.token == self.token
        self.proto_message = request.message
        self.proto_context = request.context
        # Update Message and Context
        self._update_object()
        # Set status
        code = typing.Code.OK
        status = typing.Status(code=code, message="Success")
        proto_status = status_to_proto(status=status)
        return PushClientAppOutputsResponse(status=proto_status)

    def set_object(  # pylint: disable=R0913
        self,
        message: Message,
        context: Context,
        run: Run,
        token: int,
    ) -> None:
        """Set client app objects."""
        # Serialize Message, Context, and Run
        self.proto_message = ProtoMessage(
            metadata=metadata_to_proto(message.metadata),
            content=recordset_to_proto(message.content),
            error=error_to_proto(message.error) if message.has_error() else None,
        )
        self.proto_context = ProtoContext(
            node_id=context.node_id,
            node_config=user_config_to_proto(context.node_config),
            state=recordset_to_proto(context.state),
            run_config=user_config_to_proto(context.run_config),
        )
        # self.fab = fab
        self.proto_run = ProtoRun(
            run_id=run.run_id,
            fab_id=run.fab_id,
            fab_version=run.fab_version,
            override_config=user_config_to_proto(run.override_config),
            fab_hash="",
        )
        self.token = token

    def get_object(self) -> tuple[Message, Context]:
        """Get client app objects."""
        return self.message, self.context

    def _update_object(self) -> None:
        """Update client app objects."""
        # Deserialize Message and Context
        self.message = Message(
            metadata=metadata_from_proto(self.proto_message.metadata),
            content=(
                recordset_from_proto(self.proto_message.content)
                if self.proto_message.HasField("content")
                else None
            ),
            error=(
                error_from_proto(self.proto_message.error)
                if self.proto_message.HasField("error")
                else None
            ),
        )
        self.context = Context(
            node_id=self.proto_context.node_id,
            node_config=user_config_from_proto(self.proto_context.node_config),
            state=recordset_from_proto(self.proto_context.state),
            run_config=user_config_from_proto(self.proto_context.run_config),
        )
