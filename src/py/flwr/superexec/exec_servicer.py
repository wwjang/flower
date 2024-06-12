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
"""SuperExec API servicer."""


from logging import INFO, DEBUG
from subprocess import Popen
from typing import Dict, Generator, Any

import threading
import grpc
import time
import select

from flwr.common.logger import log
from flwr.proto import exec_pb2_grpc  # pylint: disable=E0611
from flwr.proto.exec_pb2 import (  # pylint: disable=E0611
    StartRunRequest,
    StartRunResponse,
    StreamLogsRequest,
    StreamLogsResponse,
)

from .executor import Executor


class ExecServicer(exec_pb2_grpc.ExecServicer):
    """Driver API servicer."""

    def __init__(self, plugin: Executor) -> None:
        self.plugin = plugin
        self.runs: Dict[int, Popen[str]] = {}
        self.logs = []

        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.select_timeout: int = 1

    def StartRun(
        self, request: StartRunRequest, context: grpc.ServicerContext
    ) -> StartRunResponse:
        """Create run ID."""
        log(INFO, "ExecServicer.StartRun")
        run = self.plugin.start_run(request.fab_file)
        self.runs[run.run_id] = run.proc

        # Start log capturing
        # self._capture_logs(run.proc)
        # Start a background thread to capture the log output
        self.capture_thread = threading.Thread(
            target=self._capture_logs,
            args=(run.proc,),
            daemon=True
        )
        self.capture_thread.start()

        return StartRunResponse(run_id=run.run_id)

    def _capture_logs(self, proc):
        while not self.stop_event.is_set():
            ready_to_read, _, _ = select.select(
                [proc.stdout, proc.stderr],
                [],
                [],
                self.select_timeout,
            ) 
            for stream in ready_to_read:
                line = stream.readline().rstrip()
                if line:
                    with self.lock:
                        self.logs.append(f"{line}")

            # Check if the subprocess has finished
            if proc.poll() is not None:
                log(INFO, "Subprocess finished, exiting log capture")
                # Ensure all remaining output is captured
                self._drain_streams(proc=proc)
                proc.stdout.close()
                proc.stderr.close()
                log(INFO, "Regain servicer thread")
                self.stop_event.set()
                self.capture_thread.join()
                break

    def _drain_streams(self, proc):
        log(INFO, "Draining logs")
        while True:
            ready_to_read, _, _ = select.select(
                [proc.stdout, proc.stderr],
                [],
                [],
                self.select_timeout,
            )
            if not ready_to_read:
                log(INFO, "XX")
                break
            for stream in ready_to_read:
                line = stream.readline().strip()
                if line:
                    with self.lock:
                        self.logs.append(f"{line}")

        log(INFO, "XX")

    def StreamLogs(
        self, request: StreamLogsRequest, context: grpc.ServicerContext
    ) -> Generator[StreamLogsResponse, Any, None]:
        """Get logs."""
        log(INFO, "ExecServicer.StreamLogs")

        last_sent_index = 0
        while context.is_active():
            with self.lock:
                if last_sent_index < len(self.logs):
                    for i in range(last_sent_index, len(self.logs)):
                        yield StreamLogsResponse(log_output=self.logs[i])
                    last_sent_index = len(self.logs)
            time.sleep(0.1)  # Sleep briefly to avoid busy waiting
