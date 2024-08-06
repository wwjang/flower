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

from flwr.common import Context, Message
from flwr.common.typing import Code, Run, Status  # TODO: add Fab type
from flwr.proto import appio_pb2_grpc  # pylint: disable=E0611
from flwr.proto.appio_pb2 import (  # pylint: disable=E0611
    PullClientAppInputsRequest,
    PullClientAppInputsResponse,
    PushClientAppOutputsRequest,
    PushClientAppOutputsResponse,
)


class ClientAppIoServicer(appio_pb2_grpc.ClientAppIoServicer):
    """ClientAppIo API servicer."""

    def __init__(self) -> None:
        self.message: Message = None
        self.context: Context = None
        # self.fab = None
        self.run: Run = None
        self.token: int = None

    def PullClientAppInputs(
        self, request: PullClientAppInputsRequest, context: grpc.ServicerContext
    ) -> PullClientAppInputsResponse:
        assert request.token == self.token
        return PullClientAppInputsResponse(
            message=self.message,
            context=self.context,
            # fab=self.fab,
            run=self.run,
        )

    def PushClientAppOutputs(
        self, request: PushClientAppOutputsRequest, context: grpc.ServicerContext
    ) -> PushClientAppOutputsResponse:
        assert request.token == self.token
        # Update Message and Context
        self.message = request.message
        self.context = request.context

        code = Code.OK
        message = "OK"
        status = Status(code=code, message=message)
        return PushClientAppOutputsResponse(status=status)

    def set_object(  # pylint: disable=R0913
        self, message: Message, context: Context, run: Run, token: int
    ) -> None:
        """Set client app objects."""
        self.message = message
        self.context = context
        # self.fab = fab
        self.run = run
        self.token = token

    def get_object(self) -> tuple[Message, Context]:
        """Get client app objects."""
        return self.message, self.context
