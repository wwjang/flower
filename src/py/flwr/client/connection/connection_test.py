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
from unittest.mock import Mock, patch

from google.protobuf.message import Message as GrpcMessage
from . import Connection, GrpcAdapterConnection, GrpcRereConnection, RestConnection
from flwr.proto.fleet_pb2 import (  # pylint: disable=E0611
    CreateNodeResponse,
    CreateNodeRequest,
    PingResponse,
    PingRequest,
)
from flwr.common.serde_test import RecordMaker
from flwr.common import Message
from flwr.proto.fab_pb2 import GetFabRequest, GetFabResponse  # pylint: disable=E0611
from flwr.proto.run_pb2 import GetRunRequest, GetRunResponse  # pylint: disable=E0611
from flwr.proto.grpcadapter_pb2 import MessageContainer  # pylint: disable=E0611
from flwr.proto.node_pb2 import Node  # pylint: disable=E0611
from abc import abstractmethod
from flwr.common.retry_invoker import RetryInvoker, exponential
from typing import TypeVar, Any
from dataclasses import dataclass
from flwr.common import serde
from flwr.common.typing import Fab, Run


# Tests for GrpcBidiConnections are not included in this file because 
# it doesn't support all Fleet APIs.

T = TypeVar("T", bound=GrpcMessage)


@dataclass
class IoPairs:
    create_node: tuple[tuple[Any, ...], Any]
    delete_node: tuple[tuple[Any, ...], Any]
    receive: tuple[tuple[Any, ...], Any]
    send: tuple[tuple[Any, ...], Any]
    get_run: tuple[tuple[Any, ...], Any]
    get_fab: tuple[tuple[Any, ...], Any]


# Base TestCase for all connection tests
class ConnectionTest(unittest.TestCase):
    """Test all connection implementations."""

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
        received_msg = Message(mk.metadata(), mk.recordset(1, 1, 1))
        sent_msg = received_msg.create_reply(mk.recordset(1, 1, 1))
        run_info = Run(616, "dummy/mock", "v0.0", "#mock hash", {})
        fab = Fab("#mock hash", b"mock fab content")
        self.pairs = IoPairs(
            create_node=((), 6),
            delete_node=((), None),
            receive=((), received_msg),
            send=((sent_msg,), None),
            get_run=((616,), run_info),
            get_fab=(("#mock hash",), fab)
        )
        self.start_stub_patcher()
        self.conn = self.connection_type(
            server_address="123.123.123.123:1234",
            retry_invoker=RetryInvoker(exponential, Exception, max_tries=1, max_time=None),
        )
    
    def tearDown(self) -> None:
        """Cleanup"""
        self.stop_stub_patcher()
    
    def test_create_node(self):
        # Prepare
        expected_node_id = self.pairs.create_node[1]
        
        # Execute
        node_id = self.conn.create_node()
        
        # Assert
        self.assertEqual(node_id, expected_node_id)
        


class GrpcRereConnectionTest(ConnectionTest):
    
    __test__ = True
    
    @property
    def connection_type(self) -> type[Connection]:
        return GrpcRereConnection
    
    def start_stub_patcher(self) -> None:
        stub = Mock()
        
        # Mock Ping
        stub.Ping.return_value = Mock(success=True)
        
        # Mock CreateNode
        _, expected_nid = self.pairs.create_node
        stub.CreateNode.return_value = Mock(node=Mock(node_id=expected_nid))
        
        # Mock DeleteNode
        stub.DeleteNode.side_effect = lambda req: self.assertEqual(req.node.node_id, expected_nid)
        
        # Mock PullTaskIns (for `receive` method)
        _, received_msg = self.pairs.receive
        task_ins = serde.message_to_taskins(received_msg)
        stub.PullTaskIns.return_value = task_ins
        
        # Mock PushTaskRes (for `send` method)
        (sent_msg, ), _ = self.pairs.send
        task_res = serde.message_to_taskres(sent_msg)
        stub.PushTaskRes.side_effect = lambda req: self.assertEqual(req.task_res_list[0], task_res)
        
        # Mock GetRun
        (run_id, ), run_info = self.pairs.get_run
        stub.GetRun.side_effect = lambda req: (
            self.assertEqual(req.run_id, run_id),
            Mock(run=run_info)
        )[1]
        
        # Mock GetFab
        (fab_hash, ), fab = self.pairs.get_fab
        stub.GetFab.side_effect = lambda req: (
            self.assertEqual(req.hash_str, fab_hash),
            Mock(fab=fab)
        )[1]
        
        # Start patcher
        self.patcher = patch(
            "flwr.client.connection.grpc_rere.grpc_rere_connection.FleetStub",
            return_value=stub,
            )
        self.patcher.start()
    
    def stop_stub_patcher(self) -> None:
        self.patcher.stop()
        
        
    