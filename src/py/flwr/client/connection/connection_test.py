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
"""Tests for all connection implemenations."""


from __future__ import annotations

import unittest
from abc import abstractmethod
from dataclasses import dataclass
from typing import Any, TypeVar
from unittest.mock import Mock, patch

from google.protobuf.message import Message as GrpcMessage

from flwr.common import Message, serde
from flwr.common.retry_invoker import RetryInvoker, exponential
from flwr.common.serde_test import RecordMaker
from flwr.common.typing import Fab, Run
from flwr.proto.node_pb2 import Node  # pylint: disable=E0611

from . import Connection, GrpcRereConnection

# Tests for GrpcBidiConnections are not included in this file because
# it doesn't support all Fleet APIs.

T = TypeVar("T", bound=GrpcMessage)


@dataclass
class IoPairs:
    """The input and output pairs of Connection methods."""

    create_node: tuple[tuple[Any, ...], Any]
    delete_node: tuple[tuple[Any, ...], Any]
    receive: tuple[tuple[Any, ...], Any]
    send: tuple[tuple[Any, ...], Any]
    get_run: tuple[tuple[Any, ...], Any]
    get_fab: tuple[tuple[Any, ...], Any]


# Base TestCase for all connection tests
class ConnectionTest(unittest.TestCase):
    """Tests for all connection implementations."""

    # This is to True in each child class
    __test__ = False

    @abstractmethod
    def start_stub_patcher(self) -> None:
        """Start to patch the stub."""

    @abstractmethod
    def stop_stub_patcher(self) -> None:
        """Stop the patcher."""

    @property
    @abstractmethod
    def connection_type(self) -> type[Connection]:
        """Get the connection type."""

    def setUp(self) -> None:
        """Prepare before each test."""
        mk = RecordMaker()
        node_id = 6
        run_id = 616
        received_msg = Message(mk.metadata(), mk.recordset(1, 1, 1))
        received_msg.metadata.__dict__["_src_node_id"] = 0
        received_msg.metadata.dst_node_id = node_id  # Target at this node
        sent_msg = received_msg.create_reply(mk.recordset(1, 1, 1))
        run_info = Run(616, "dummy/mock", "v0.0", "#mock hash", {})
        fab = Fab("#mock hash", b"mock fab content")
        self.pairs = IoPairs(
            create_node=((), node_id),
            delete_node=((), None),
            receive=((), received_msg),
            send=((sent_msg,), None),
            get_run=((run_id,), run_info),
            get_fab=(("#mock hash",), fab),
        )
        self.start_stub_patcher()
        self.conn = self.connection_type(
            server_address="123.123.123.123:1234",
            insecure=True,
            retry_invoker=RetryInvoker(
                exponential, Exception, max_tries=1, max_time=None
            ),
        )

    def tearDown(self) -> None:
        """Cleanup."""
        self.stop_stub_patcher()

    def test_create_node(self) -> None:
        """Test create_node method."""
        # Prepare
        expected_node_id = self.pairs.create_node[1]

        # Execute
        node_id = self.conn.create_node()

        # Assert
        self.assertEqual(node_id, expected_node_id)

    def test_delete_node(self) -> None:
        """Test delete_node method."""
        # Execute
        self.conn.create_node()
        self.conn.delete_node()

    def test_receive(self) -> None:
        """Test receive method."""
        # Prepare
        expected_msg: Message = self.pairs.receive[1]

        # Execute
        self.conn.create_node()
        actual_msg = self.conn.receive()

        # Assert
        assert actual_msg is not None
        # Message object doesn't support `==` operator
        self.assertEqual(actual_msg.metadata, expected_msg.metadata)
        self.assertEqual(actual_msg.content, expected_msg.content)

    def test_send(self) -> None:
        """Test send method."""
        # Prepare
        msg: Message = self.pairs.send[0][0]

        # Execute
        self.conn.create_node()
        self.conn.receive()
        self.conn.send(msg)

    def test_get_run(self) -> None:
        """Test get_run method."""
        # Prepare
        (run_id,), run_info = self.pairs.get_run

        # Execute
        self.conn.create_node()
        actual_run_info = self.conn.get_run(run_id)

        # Assert
        self.assertEqual(actual_run_info, run_info)

    def test_get_fab(self) -> None:
        """Test get_fab method."""
        # Prepare
        (fab_hash,), fab = self.pairs.get_fab

        # Execute
        self.conn.create_node()
        actual_fab = self.conn.get_fab(fab_hash)

        # Assert
        self.assertEqual(actual_fab, fab)


class GrpcRereConnectionTest(ConnectionTest):
    """Tests for GrpcRereConnection."""

    __test__ = True

    @property
    def connection_type(self) -> type[Connection]:
        """Get the connection type."""
        return GrpcRereConnection

    def start_stub_patcher(self) -> None:
        """Start to patch the stub."""
        stub = Mock()

        # Mock Ping
        stub.Ping.return_value = Mock(success=True)

        # Mock CreateNode
        _, expected_nid = self.pairs.create_node
        stub.CreateNode.return_value = Mock(node=Node(node_id=expected_nid))

        # Mock DeleteNode
        def delete_node_side_effect(request: Any) -> Any:
            self.assertEqual(request.node.node_id, expected_nid)

        stub.DeleteNode.side_effect = delete_node_side_effect

        # Mock PullTaskIns (for `receive` method)
        _, received_msg = self.pairs.receive
        task_ins = serde.message_to_taskins(received_msg)
        task_ins.task_id = received_msg.metadata.message_id
        stub.PullTaskIns.return_value = Mock(task_ins_list=[task_ins])

        # Mock PushTaskRes (for `send` method)
        (sent_msg,), _ = self.pairs.send

        def send_side_effect(request: Any) -> Any:
            self.assertEqual(request.task_res_list[0], task_res)

        task_res = serde.message_to_taskres(sent_msg)
        stub.PushTaskRes.side_effect = send_side_effect

        # Mock GetRun
        (run_id,), run_info = self.pairs.get_run

        def get_run_side_effect(request: Any) -> Any:
            self.assertEqual(request.run_id, run_id)
            return Mock(run=run_info)

        stub.GetRun.side_effect = get_run_side_effect

        # Mock GetFab
        (fab_hash,), fab = self.pairs.get_fab

        def get_fab_side_effect(request: Any) -> Any:
            self.assertEqual(request.hash_str, fab_hash)
            return Mock(fab=fab)

        stub.GetFab.side_effect = get_fab_side_effect

        # Start patcher
        self.patcher = patch(
            "flwr.client.connection.grpc_rere.grpc_rere_connection.FleetStub",
            return_value=stub,
        )
        self.patcher.start()

    def stop_stub_patcher(self) -> None:
        """Stop the patcher."""
        self.patcher.stop()
