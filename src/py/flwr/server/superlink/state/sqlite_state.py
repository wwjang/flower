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
"""SQLite based implemenation of server state."""


import json
import re
import sqlite3
import time
from collections.abc import Sequence
from logging import DEBUG, ERROR
from typing import Any, Optional, Union, cast
from uuid import UUID, uuid4

from flwr.common import log, now
from flwr.common.constant import NODE_ID_NUM_BYTES, RUN_ID_NUM_BYTES
from flwr.common.typing import Run, UserConfig
from flwr.proto.node_pb2 import Node  # pylint: disable=E0611
from flwr.proto.recordset_pb2 import RecordSet  # pylint: disable=E0611
from flwr.proto.task_pb2 import Task, TaskIns, TaskRes  # pylint: disable=E0611
from flwr.server.utils.validator import validate_task_ins_or_res

from .state import State
from .utils import (
    convert_sint64_to_uint64,
    convert_sint64_values_in_dict_to_uint64,
    convert_uint64_to_sint64,
    convert_uint64_values_in_dict_to_sint64,
    generate_rand_int_from_bytes,
    make_node_unavailable_taskres,
)

SQL_CREATE_TABLE_NODE = """
CREATE TABLE IF NOT EXISTS node(
    node_id         INTEGER UNIQUE,
    online_until    REAL,
    ping_interval   REAL,
    public_key      BLOB
);
"""

SQL_CREATE_TABLE_CREDENTIAL = """
CREATE TABLE IF NOT EXISTS credential(
    private_key BLOB PRIMARY KEY,
    public_key BLOB
);
"""

SQL_CREATE_TABLE_PUBLIC_KEY = """
CREATE TABLE IF NOT EXISTS public_key(
    public_key BLOB UNIQUE
);
"""

SQL_CREATE_INDEX_ONLINE_UNTIL = """
CREATE INDEX IF NOT EXISTS idx_online_until ON node (online_until);
"""

SQL_CREATE_TABLE_RUN = """
CREATE TABLE IF NOT EXISTS run(
    run_id                INTEGER UNIQUE,
    fab_id                TEXT,
    fab_version           TEXT,
    fab_hash              TEXT,
    override_config       TEXT
);
"""

SQL_CREATE_TABLE_TASK_INS = """
CREATE TABLE IF NOT EXISTS task_ins(
    task_id                 TEXT UNIQUE,
    group_id                TEXT,
    run_id                  INTEGER,
    producer_anonymous      BOOLEAN,
    producer_node_id        INTEGER,
    consumer_anonymous      BOOLEAN,
    consumer_node_id        INTEGER,
    created_at              REAL,
    delivered_at            TEXT,
    pushed_at               REAL,
    ttl                     REAL,
    ancestry                TEXT,
    task_type               TEXT,
    recordset               BLOB,
    FOREIGN KEY(run_id) REFERENCES run(run_id)
);
"""

SQL_CREATE_TABLE_TASK_RES = """
CREATE TABLE IF NOT EXISTS task_res(
    task_id                 TEXT UNIQUE,
    group_id                TEXT,
    run_id                  INTEGER,
    producer_anonymous      BOOLEAN,
    producer_node_id        INTEGER,
    consumer_anonymous      BOOLEAN,
    consumer_node_id        INTEGER,
    created_at              REAL,
    delivered_at            TEXT,
    pushed_at               REAL,
    ttl                     REAL,
    ancestry                TEXT,
    task_type               TEXT,
    recordset               BLOB,
    FOREIGN KEY(run_id) REFERENCES run(run_id)
);
"""

DictOrTuple = Union[tuple[Any, ...], dict[str, Any]]


class SqliteState(State):  # pylint: disable=R0904
    """SQLite-based state implementation."""

    def __init__(
        self,
        database_path: str,
    ) -> None:
        """Initialize an SqliteState.

        Parameters
        ----------
        database : (path-like object)
            The path to the database file to be opened. Pass ":memory:" to open
            a connection to a database that is in RAM, instead of on disk.
        """
        self.database_path = database_path
        self.conn: Optional[sqlite3.Connection] = None

    def initialize(self, log_queries: bool = False) -> list[tuple[str]]:
        """Create tables if they don't exist yet.

        Parameters
        ----------
        log_queries : bool
            Log each query which is executed.
        """
        self.conn = sqlite3.connect(self.database_path)
        self.conn.execute("PRAGMA foreign_keys = ON;")
        self.conn.row_factory = dict_factory
        if log_queries:
            self.conn.set_trace_callback(lambda query: log(DEBUG, query))
        cur = self.conn.cursor()

        # Create each table if not exists queries
        cur.execute(SQL_CREATE_TABLE_RUN)
        cur.execute(SQL_CREATE_TABLE_TASK_INS)
        cur.execute(SQL_CREATE_TABLE_TASK_RES)
        cur.execute(SQL_CREATE_TABLE_NODE)
        cur.execute(SQL_CREATE_TABLE_CREDENTIAL)
        cur.execute(SQL_CREATE_TABLE_PUBLIC_KEY)
        cur.execute(SQL_CREATE_INDEX_ONLINE_UNTIL)
        res = cur.execute("SELECT name FROM sqlite_schema;")

        return res.fetchall()

    def query(
        self,
        query: str,
        data: Optional[Union[Sequence[DictOrTuple], DictOrTuple]] = None,
    ) -> list[dict[str, Any]]:
        """Execute a SQL query."""
        if self.conn is None:
            raise AttributeError("State is not initialized.")

        if data is None:
            data = []

        # Clean up whitespace to make the logs nicer
        query = re.sub(r"\s+", " ", query)

        try:
            with self.conn:
                if (
                    len(data) > 0
                    and isinstance(data, (tuple, list))
                    and isinstance(data[0], (tuple, dict))
                ):
                    rows = self.conn.executemany(query, data)
                else:
                    rows = self.conn.execute(query, data)

                # Extract results before committing to support
                #   INSERT/UPDATE ... RETURNING
                # style queries
                result = rows.fetchall()
        except KeyError as exc:
            log(ERROR, {"query": query, "data": data, "exception": exc})

        return result

    def store_task_ins(self, task_ins: TaskIns) -> Optional[UUID]:
        """Store one TaskIns.

        Usually, the Driver API calls this to schedule instructions.

        Stores the value of the task_ins in the state and, if successful, returns the
        task_id (UUID) of the task_ins. If, for any reason, storing the task_ins fails,
        `None` is returned.

        Constraints
        -----------
        If `task_ins.task.consumer.anonymous` is `True`, then
        `task_ins.task.consumer.node_id` MUST NOT be set (equal 0).

        If `task_ins.task.consumer.anonymous` is `False`, then
        `task_ins.task.consumer.node_id` MUST be set (not 0)
        """
        # Validate task
        errors = validate_task_ins_or_res(task_ins)
        if any(errors):
            log(ERROR, errors)
            return None

        # Create task_id
        task_id = uuid4()

        # Store TaskIns
        task_ins.task_id = str(task_id)
        data = (task_ins_to_dict(task_ins),)

        # Convert values from uint64 to sint64 for SQLite
        convert_uint64_values_in_dict_to_sint64(
            data[0], ["run_id", "producer_node_id", "consumer_node_id"]
        )

        columns = ", ".join([f":{key}" for key in data[0]])
        query = f"INSERT INTO task_ins VALUES({columns});"

        # Only invalid run_id can trigger IntegrityError.
        # This may need to be changed in the future version with more integrity checks.
        try:
            self.query(query, data)
        except sqlite3.IntegrityError:
            log(ERROR, "`run` is invalid")
            return None

        return task_id

    def get_task_ins(
        self, node_id: Optional[int], limit: Optional[int]
    ) -> list[TaskIns]:
        """Get undelivered TaskIns for one node (either anonymous or with ID).

        Usually, the Fleet API calls this for Nodes planning to work on one or more
        TaskIns.

        Constraints
        -----------
        If `node_id` is not `None`, retrieve all TaskIns where

            1. the `task_ins.task.consumer.node_id` equals `node_id` AND
            2. the `task_ins.task.consumer.anonymous` equals `False` AND
            3. the `task_ins.task.delivered_at` equals `""`.

        If `node_id` is `None`, retrieve all TaskIns where the
        `task_ins.task.consumer.node_id` equals `0` and
        `task_ins.task.consumer.anonymous` is set to `True`.

        `delivered_at` MUST BE set (i.e., not `""`) otherwise the TaskIns MUST not be in
        the result.

        If `limit` is not `None`, return, at most, `limit` number of `task_ins`. If
        `limit` is set, it has to be greater than zero.
        """
        if limit is not None and limit < 1:
            raise AssertionError("`limit` must be >= 1")

        if node_id == 0:
            msg = (
                "`node_id` must be >= 1"
                "\n\n For requesting anonymous tasks use `node_id` equal `None`"
            )
            raise AssertionError(msg)

        data: dict[str, Union[str, int]] = {}

        if node_id is None:
            # Retrieve all anonymous Tasks
            query = """
                SELECT task_id
                FROM task_ins
                WHERE consumer_anonymous == 1
                AND   consumer_node_id == 0
                AND   delivered_at = ""
            """
        else:
            # Convert the uint64 value to sint64 for SQLite
            data["node_id"] = convert_uint64_to_sint64(node_id)

            # Retrieve all TaskIns for node_id
            query = """
                SELECT task_id
                FROM task_ins
                WHERE consumer_anonymous == 0
                AND   consumer_node_id == :node_id
                AND   delivered_at = ""
            """

        if limit is not None:
            query += " LIMIT :limit"
            data["limit"] = limit

        query += ";"

        rows = self.query(query, data)

        if rows:
            # Prepare query
            task_ids = [row["task_id"] for row in rows]
            placeholders: str = ",".join([f":id_{i}" for i in range(len(task_ids))])
            query = f"""
                UPDATE task_ins
                SET delivered_at = :delivered_at
                WHERE task_id IN ({placeholders})
                RETURNING *;
            """

            # Prepare data for query
            delivered_at = now().isoformat()
            data = {"delivered_at": delivered_at}
            for index, task_id in enumerate(task_ids):
                data[f"id_{index}"] = str(task_id)

            # Run query
            rows = self.query(query, data)

        for row in rows:
            # Convert values from sint64 to uint64
            convert_sint64_values_in_dict_to_uint64(
                row, ["run_id", "producer_node_id", "consumer_node_id"]
            )

        result = [dict_to_task_ins(row) for row in rows]

        return result

    def store_task_res(self, task_res: TaskRes) -> Optional[UUID]:
        """Store one TaskRes.

        Usually, the Fleet API calls this when Nodes return their results.

        Stores the TaskRes and, if successful, returns the `task_id` (UUID) of
        the `task_res`. If storing the `task_res` fails, `None` is returned.

        Constraints
        -----------
        If `task_res.task.consumer.anonymous` is `True`, then
        `task_res.task.consumer.node_id` MUST NOT be set (equal 0).

        If `task_res.task.consumer.anonymous` is `False`, then
        `task_res.task.consumer.node_id` MUST be set (not 0)
        """
        # Validate task
        errors = validate_task_ins_or_res(task_res)
        if any(errors):
            log(ERROR, errors)
            return None

        # Create task_id
        task_id = uuid4()

        # Store TaskIns
        task_res.task_id = str(task_id)
        data = (task_res_to_dict(task_res),)

        # Convert values from uint64 to sint64 for SQLite
        convert_uint64_values_in_dict_to_sint64(
            data[0], ["run_id", "producer_node_id", "consumer_node_id"]
        )

        columns = ", ".join([f":{key}" for key in data[0]])
        query = f"INSERT INTO task_res VALUES({columns});"

        # Only invalid run_id can trigger IntegrityError.
        # This may need to be changed in the future version with more integrity checks.
        try:
            self.query(query, data)
        except sqlite3.IntegrityError:
            log(ERROR, "`run` is invalid")
            return None

        return task_id

    # pylint: disable-next=R0914
    def get_task_res(self, task_ids: set[UUID], limit: Optional[int]) -> list[TaskRes]:
        """Get TaskRes for task_ids.

        Usually, the Driver API calls this method to get results for instructions it has
        previously scheduled.

        Retrieves all TaskRes for the given `task_ids` and returns and empty list if
        none could be found.

        Constraints
        -----------
        If `limit` is not `None`, return, at most, `limit` number of TaskRes. The limit
        will only take effect if enough task_ids are in the set AND are currently
        available. If `limit` is set, it has to be greater than zero.
        """
        if limit is not None and limit < 1:
            raise AssertionError("`limit` must be >= 1")

        # Retrieve all anonymous Tasks
        if len(task_ids) == 0:
            return []

        placeholders = ",".join([f":id_{i}" for i in range(len(task_ids))])
        query = f"""
            SELECT *
            FROM task_res
            WHERE ancestry IN ({placeholders})
            AND delivered_at = ""
        """

        data: dict[str, Union[str, float, int]] = {}

        if limit is not None:
            query += " LIMIT :limit"
            data["limit"] = limit

        query += ";"

        for index, task_id in enumerate(task_ids):
            data[f"id_{index}"] = str(task_id)

        rows = self.query(query, data)

        if rows:
            # Prepare query
            found_task_ids = [row["task_id"] for row in rows]
            placeholders = ",".join([f":id_{i}" for i in range(len(found_task_ids))])
            query = f"""
                UPDATE task_res
                SET delivered_at = :delivered_at
                WHERE task_id IN ({placeholders})
                RETURNING *;
            """

            # Prepare data for query
            delivered_at = now().isoformat()
            data = {"delivered_at": delivered_at}
            for index, task_id in enumerate(found_task_ids):
                data[f"id_{index}"] = str(task_id)

            # Run query
            rows = self.query(query, data)

            for row in rows:
                # Convert values from sint64 to uint64
                convert_sint64_values_in_dict_to_uint64(
                    row, ["run_id", "producer_node_id", "consumer_node_id"]
                )

        result = [dict_to_task_res(row) for row in rows]

        # 1. Query: Fetch consumer_node_id of remaining task_ids
        # Assume the ancestry field only contains one element
        data.clear()
        replied_task_ids: set[UUID] = {UUID(str(row["ancestry"])) for row in rows}
        remaining_task_ids = task_ids - replied_task_ids
        placeholders = ",".join([f":id_{i}" for i in range(len(remaining_task_ids))])
        query = f"""
            SELECT consumer_node_id
            FROM task_ins
            WHERE task_id IN ({placeholders});
        """
        for index, task_id in enumerate(remaining_task_ids):
            data[f"id_{index}"] = str(task_id)
        node_ids = [int(row["consumer_node_id"]) for row in self.query(query, data)]

        # 2. Query: Select offline nodes
        placeholders = ",".join([f":id_{i}" for i in range(len(node_ids))])
        query = f"""
            SELECT node_id
            FROM node
            WHERE node_id IN ({placeholders})
            AND online_until < :time;
        """
        data = {f"id_{i}": str(node_id) for i, node_id in enumerate(node_ids)}
        data["time"] = time.time()
        offline_node_ids = [int(row["node_id"]) for row in self.query(query, data)]

        # 3. Query: Select TaskIns for offline nodes
        placeholders = ",".join([f":id_{i}" for i in range(len(offline_node_ids))])
        query = f"""
            SELECT *
            FROM task_ins
            WHERE consumer_node_id IN ({placeholders});
        """
        data = {f"id_{i}": str(node_id) for i, node_id in enumerate(offline_node_ids)}
        task_ins_rows = self.query(query, data)

        # Make TaskRes containing node unavailabe error
        for row in task_ins_rows:
            if limit and len(result) == limit:
                break

            for row in rows:
                # Convert values from sint64 to uint64
                convert_sint64_values_in_dict_to_uint64(
                    row, ["run_id", "producer_node_id", "consumer_node_id"]
                )

            task_ins = dict_to_task_ins(row)
            err_taskres = make_node_unavailable_taskres(
                ref_taskins=task_ins,
            )
            result.append(err_taskres)

        return result

    def num_task_ins(self) -> int:
        """Calculate the number of task_ins in store.

        This includes delivered but not yet deleted task_ins.
        """
        query = "SELECT count(*) AS num FROM task_ins;"
        rows = self.query(query)
        result = rows[0]
        num = cast(int, result["num"])
        return num

    def num_task_res(self) -> int:
        """Calculate the number of task_res in store.

        This includes delivered but not yet deleted task_res.
        """
        query = "SELECT count(*) AS num FROM task_res;"
        rows = self.query(query)
        result: dict[str, int] = rows[0]
        return result["num"]

    def delete_tasks(self, task_ids: set[UUID]) -> None:
        """Delete all delivered TaskIns/TaskRes pairs."""
        ids = list(task_ids)
        if len(ids) == 0:
            return None

        placeholders = ",".join([f":id_{index}" for index in range(len(task_ids))])
        data = {f"id_{index}": str(task_id) for index, task_id in enumerate(task_ids)}

        # 1. Query: Delete task_ins which have a delivered task_res
        query_1 = f"""
            DELETE FROM task_ins
            WHERE delivered_at != ''
            AND task_id IN (
                SELECT ancestry
                FROM task_res
                WHERE ancestry IN ({placeholders})
                AND delivered_at != ''
            );
        """

        # 2. Query: Delete delivered task_res to be run after 1. Query
        query_2 = f"""
            DELETE FROM task_res
            WHERE ancestry IN ({placeholders})
            AND delivered_at != '';
        """

        if self.conn is None:
            raise AttributeError("State not intitialized")

        with self.conn:
            self.conn.execute(query_1, data)
            self.conn.execute(query_2, data)

        return None

    def create_node(
        self, ping_interval: float, public_key: Optional[bytes] = None
    ) -> int:
        """Create, store in state, and return `node_id`."""
        # Sample a random uint64 as node_id
        uint64_node_id = generate_rand_int_from_bytes(NODE_ID_NUM_BYTES)

        # Convert the uint64 value to sint64 for SQLite
        sint64_node_id = convert_uint64_to_sint64(uint64_node_id)

        query = "SELECT node_id FROM node WHERE public_key = :public_key;"
        row = self.query(query, {"public_key": public_key})

        if len(row) > 0:
            log(ERROR, "Unexpected node registration failure.")
            return 0

        query = (
            "INSERT INTO node "
            "(node_id, online_until, ping_interval, public_key) "
            "VALUES (?, ?, ?, ?)"
        )

        try:
            self.query(
                query,
                (
                    sint64_node_id,
                    time.time() + ping_interval,
                    ping_interval,
                    public_key,
                ),
            )
        except sqlite3.IntegrityError:
            log(ERROR, "Unexpected node registration failure.")
            return 0

        # Note: we need to return the uint64 value of the node_id
        return uint64_node_id

    def delete_node(self, node_id: int, public_key: Optional[bytes] = None) -> None:
        """Delete a node."""
        # Convert the uint64 value to sint64 for SQLite
        sint64_node_id = convert_uint64_to_sint64(node_id)

        query = "DELETE FROM node WHERE node_id = ?"
        params = (sint64_node_id,)

        if public_key is not None:
            query += " AND public_key = ?"
            params += (public_key,)  # type: ignore

        if self.conn is None:
            raise AttributeError("State is not initialized.")

        try:
            with self.conn:
                rows = self.conn.execute(query, params)
                if rows.rowcount < 1:
                    raise ValueError("Public key or node_id not found")
        except KeyError as exc:
            log(ERROR, {"query": query, "data": params, "exception": exc})

    def get_nodes(self, run_id: int) -> set[int]:
        """Retrieve all currently stored node IDs as a set.

        Constraints
        -----------
        If the provided `run_id` does not exist or has no matching nodes,
        an empty `Set` MUST be returned.
        """
        # Convert the uint64 value to sint64 for SQLite
        sint64_run_id = convert_uint64_to_sint64(run_id)

        # Validate run ID
        query = "SELECT COUNT(*) FROM run WHERE run_id = ?;"
        if self.query(query, (sint64_run_id,))[0]["COUNT(*)"] == 0:
            return set()

        # Get nodes
        query = "SELECT node_id FROM node WHERE online_until > ?;"
        rows = self.query(query, (time.time(),))

        # Convert sint64 node_ids to uint64
        result: set[int] = {convert_sint64_to_uint64(row["node_id"]) for row in rows}
        return result

    def get_node_id(self, node_public_key: bytes) -> Optional[int]:
        """Retrieve stored `node_id` filtered by `node_public_keys`."""
        query = "SELECT node_id FROM node WHERE public_key = :public_key;"
        row = self.query(query, {"public_key": node_public_key})
        if len(row) > 0:
            node_id: int = row[0]["node_id"]

            # Convert the sint64 value to uint64 after reading from SQLite
            uint64_node_id = convert_sint64_to_uint64(node_id)

            return uint64_node_id
        return None

    def create_run(
        self,
        fab_id: Optional[str],
        fab_version: Optional[str],
        fab_hash: Optional[str],
        override_config: UserConfig,
    ) -> int:
        """Create a new run for the specified `fab_id` and `fab_version`."""
        # Sample a random int64 as run_id
        uint64_run_id = generate_rand_int_from_bytes(RUN_ID_NUM_BYTES)

        # Convert the uint64 value to sint64 for SQLite
        sint64_run_id = convert_uint64_to_sint64(uint64_run_id)

        # Check conflicts
        query = "SELECT COUNT(*) FROM run WHERE run_id = ?;"
        # If sint64_run_id does not exist
        if self.query(query, (sint64_run_id,))[0]["COUNT(*)"] == 0:
            query = (
                "INSERT INTO run "
                "(run_id, fab_id, fab_version, fab_hash, override_config)"
                "VALUES (?, ?, ?, ?, ?);"
            )
            if fab_hash:
                self.query(
                    query,
                    (sint64_run_id, "", "", fab_hash, json.dumps(override_config)),
                )
            else:
                self.query(
                    query,
                    (
                        sint64_run_id,
                        fab_id,
                        fab_version,
                        "",
                        json.dumps(override_config),
                    ),
                )
            # Note: we need to return the uint64 value of the run_id
            return uint64_run_id
        log(ERROR, "Unexpected run creation failure.")
        return 0

    def store_server_private_public_key(
        self, private_key: bytes, public_key: bytes
    ) -> None:
        """Store `server_private_key` and `server_public_key` in state."""
        query = "SELECT COUNT(*) FROM credential"
        count = self.query(query)[0]["COUNT(*)"]
        if count < 1:
            query = (
                "INSERT OR REPLACE INTO credential (private_key, public_key) "
                "VALUES (:private_key, :public_key)"
            )
            self.query(query, {"private_key": private_key, "public_key": public_key})
        else:
            raise RuntimeError("Server private and public key already set")

    def get_server_private_key(self) -> Optional[bytes]:
        """Retrieve `server_private_key` in urlsafe bytes."""
        query = "SELECT private_key FROM credential"
        rows = self.query(query)
        try:
            private_key: Optional[bytes] = rows[0]["private_key"]
        except IndexError:
            private_key = None
        return private_key

    def get_server_public_key(self) -> Optional[bytes]:
        """Retrieve `server_public_key` in urlsafe bytes."""
        query = "SELECT public_key FROM credential"
        rows = self.query(query)
        try:
            public_key: Optional[bytes] = rows[0]["public_key"]
        except IndexError:
            public_key = None
        return public_key

    def store_node_public_keys(self, public_keys: set[bytes]) -> None:
        """Store a set of `node_public_keys` in state."""
        query = "INSERT INTO public_key (public_key) VALUES (?)"
        data = [(key,) for key in public_keys]
        self.query(query, data)

    def store_node_public_key(self, public_key: bytes) -> None:
        """Store a `node_public_key` in state."""
        query = "INSERT INTO public_key (public_key) VALUES (:public_key)"
        self.query(query, {"public_key": public_key})

    def get_node_public_keys(self) -> set[bytes]:
        """Retrieve all currently stored `node_public_keys` as a set."""
        query = "SELECT public_key FROM public_key"
        rows = self.query(query)
        result: set[bytes] = {row["public_key"] for row in rows}
        return result

    def get_run(self, run_id: int) -> Optional[Run]:
        """Retrieve information about the run with the specified `run_id`."""
        # Convert the uint64 value to sint64 for SQLite
        sint64_run_id = convert_uint64_to_sint64(run_id)
        query = "SELECT * FROM run WHERE run_id = ?;"
        rows = self.query(query, (sint64_run_id,))
        if rows:
            row = rows[0]
            return Run(
                run_id=convert_sint64_to_uint64(row["run_id"]),
                fab_id=row["fab_id"],
                fab_version=row["fab_version"],
                fab_hash=row["fab_hash"],
                override_config=json.loads(row["override_config"]),
            )
        log(ERROR, "`run_id` does not exist.")
        return None

    def acknowledge_ping(self, node_id: int, ping_interval: float) -> bool:
        """Acknowledge a ping received from a node, serving as a heartbeat."""
        sint64_node_id = convert_uint64_to_sint64(node_id)

        # Update `online_until` and `ping_interval` for the given `node_id`
        query = "UPDATE node SET online_until = ?, ping_interval = ? WHERE node_id = ?;"
        try:
            self.query(
                query, (time.time() + ping_interval, ping_interval, sint64_node_id)
            )
            return True
        except sqlite3.IntegrityError:
            log(ERROR, "`node_id` does not exist.")
            return False


def dict_factory(
    cursor: sqlite3.Cursor,
    row: sqlite3.Row,
) -> dict[str, Any]:
    """Turn SQLite results into dicts.

    Less efficent for retrival of large amounts of data but easier to use.
    """
    fields = [column[0] for column in cursor.description]
    return dict(zip(fields, row))


def task_ins_to_dict(task_msg: TaskIns) -> dict[str, Any]:
    """Transform TaskIns to dict."""
    result = {
        "task_id": task_msg.task_id,
        "group_id": task_msg.group_id,
        "run_id": task_msg.run_id,
        "producer_anonymous": task_msg.task.producer.anonymous,
        "producer_node_id": task_msg.task.producer.node_id,
        "consumer_anonymous": task_msg.task.consumer.anonymous,
        "consumer_node_id": task_msg.task.consumer.node_id,
        "created_at": task_msg.task.created_at,
        "delivered_at": task_msg.task.delivered_at,
        "pushed_at": task_msg.task.pushed_at,
        "ttl": task_msg.task.ttl,
        "ancestry": ",".join(task_msg.task.ancestry),
        "task_type": task_msg.task.task_type,
        "recordset": task_msg.task.recordset.SerializeToString(),
    }
    return result


def task_res_to_dict(task_msg: TaskRes) -> dict[str, Any]:
    """Transform TaskRes to dict."""
    result = {
        "task_id": task_msg.task_id,
        "group_id": task_msg.group_id,
        "run_id": task_msg.run_id,
        "producer_anonymous": task_msg.task.producer.anonymous,
        "producer_node_id": task_msg.task.producer.node_id,
        "consumer_anonymous": task_msg.task.consumer.anonymous,
        "consumer_node_id": task_msg.task.consumer.node_id,
        "created_at": task_msg.task.created_at,
        "delivered_at": task_msg.task.delivered_at,
        "pushed_at": task_msg.task.pushed_at,
        "ttl": task_msg.task.ttl,
        "ancestry": ",".join(task_msg.task.ancestry),
        "task_type": task_msg.task.task_type,
        "recordset": task_msg.task.recordset.SerializeToString(),
    }
    return result


def dict_to_task_ins(task_dict: dict[str, Any]) -> TaskIns:
    """Turn task_dict into protobuf message."""
    recordset = RecordSet()
    recordset.ParseFromString(task_dict["recordset"])

    result = TaskIns(
        task_id=task_dict["task_id"],
        group_id=task_dict["group_id"],
        run_id=task_dict["run_id"],
        task=Task(
            producer=Node(
                node_id=task_dict["producer_node_id"],
                anonymous=task_dict["producer_anonymous"],
            ),
            consumer=Node(
                node_id=task_dict["consumer_node_id"],
                anonymous=task_dict["consumer_anonymous"],
            ),
            created_at=task_dict["created_at"],
            delivered_at=task_dict["delivered_at"],
            pushed_at=task_dict["pushed_at"],
            ttl=task_dict["ttl"],
            ancestry=task_dict["ancestry"].split(","),
            task_type=task_dict["task_type"],
            recordset=recordset,
        ),
    )
    return result


def dict_to_task_res(task_dict: dict[str, Any]) -> TaskRes:
    """Turn task_dict into protobuf message."""
    recordset = RecordSet()
    recordset.ParseFromString(task_dict["recordset"])

    result = TaskRes(
        task_id=task_dict["task_id"],
        group_id=task_dict["group_id"],
        run_id=task_dict["run_id"],
        task=Task(
            producer=Node(
                node_id=task_dict["producer_node_id"],
                anonymous=task_dict["producer_anonymous"],
            ),
            consumer=Node(
                node_id=task_dict["consumer_node_id"],
                anonymous=task_dict["consumer_anonymous"],
            ),
            created_at=task_dict["created_at"],
            delivered_at=task_dict["delivered_at"],
            pushed_at=task_dict["pushed_at"],
            ttl=task_dict["ttl"],
            ancestry=task_dict["ancestry"].split(","),
            task_type=task_dict["task_type"],
            recordset=recordset,
        ),
    )
    return result
