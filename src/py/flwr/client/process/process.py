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
"""Flower background ClientApp."""

from logging import DEBUG, ERROR, INFO

import grpc

from flwr.cli.install import install_from_fab
from flwr.client.client_app import ClientApp
from flwr.client.supernode.app import _get_load_client_app_fn
from flwr.common.grpc import GRPC_MAX_MESSAGE_LENGTH, create_channel
from flwr.common.logger import log
from flwr.proto.appio_pb2 import (  # pylint: disable=E0611
    PullClientAppInputsRequest,
    PushClientAppOutputsRequest,
)
from flwr.proto.appio_pb2_grpc import ClientAppIoStub, add_ClientAppIoServicer_to_server
from flwr.server.superlink.fleet.grpc_bidi.grpc_server import generic_create_grpc_server

from .clientappio_servicer import ClientAppIoServicer


def _run_background_client(
    address: str,
    token: int,
) -> None:
    """Run background Flower ClientApp process."""

    def on_channel_state_change(channel_connectivity: str) -> None:
        """Log channel connectivity."""
        log(DEBUG, channel_connectivity)

    channel = create_channel(
        server_address=address,
        insecure=True,
    )
    channel.subscribe(on_channel_state_change)

    try:
        stub = ClientAppIoStub(channel)

        req = PullClientAppInputsRequest(token=token)
        res = stub.PullClientAppInputs(req)

        fab_file = res.fab  # Seems unnecessary?
        run = res.run

        install_from_fab(
            fab_file, None, True
        )  # Ensures FAB is installed (default is Flower directory)
        load_client_app_fn = _get_load_client_app_fn(
            default_app_ref="",
            project_dir="",
            multi_app=True,
            flwr_dir=None,
        )
        client_app: ClientApp = load_client_app_fn(
            run.fab_id, run.fab_version  # Can be optimized later
        )
        # Execute ClientApp
        reply_message, reply_context = client_app(
            message=res.message, context=res.context
        )

        req = PushClientAppOutputsRequest(
            token=token,
            message=reply_message,
            context=reply_context,
        )
        res = stub.PushClientAppOutputs(req)
    except KeyboardInterrupt:
        log(INFO, "Closing connection")
    except grpc.RpcError as e:
        log(ERROR, "GRPC error occurred: %s", str(e))
    finally:
        channel.close()


def run_clientappio_api_grpc(
    address: str = "0.0.0.0:9094",
) -> tuple[grpc.Server, grpc.Server]:
    """Run ClientAppIo API (gRPC-rere)."""
    clientappio_servicer: grpc.Server = ClientAppIoServicer()
    clientappio_add_servicer_to_server_fn = add_ClientAppIoServicer_to_server
    clientappio_grpc_server = generic_create_grpc_server(
        servicer_and_add_fn=(
            clientappio_servicer,
            clientappio_add_servicer_to_server_fn,
        ),
        server_address=address,
        max_message_length=GRPC_MAX_MESSAGE_LENGTH,
    )
    log(INFO, "Starting Flower ClientAppIo gRPC server on %s", address)
    clientappio_grpc_server.start()
    return clientappio_servicer, clientappio_grpc_server
