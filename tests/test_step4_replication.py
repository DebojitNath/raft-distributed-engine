"""Unit and Integration tests for Step 4: Log Replication & State Machine (Happy Path).

Verifies:
- Non-leader rejects execute_command
- Single-node instant log commitment
- 3-node cluster log replication across all nodes
- Distributed state machine (kv_store) consensus and parity
- Sequential ordered multi-command execution
- Heartbeat-driven follower catchup and commitment
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from typing import List

# Add project root to sys.path so tests work whether run from root or inside tests/
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from raft.node import RaftNode
from raft.state import NodeRole


def test_non_leader_rejects_execute_command() -> None:
    """Follower nodes reject direct execute_command requests."""

    async def _test() -> None:
        node = RaftNode(node_id="follower1", peers=["node2", "node3"])
        # Node starts as FOLLOWER without starting election
        node._is_running = True
        success = await node.execute_command("SET x=1", timeout=0.1)
        assert success is False
        assert len(node.state.log) == 0
        assert node.state.kv_store == {}

    asyncio.run(_test())


def test_single_node_commits_command() -> None:
    """A standalone leader (0 peers) commits commands immediately."""

    async def _test() -> None:
        node = RaftNode(
            node_id="single",
            peers=[],
            min_election_timeout=0.030,
            max_election_timeout=0.060,
        )
        await node.start()
        try:
            # Wait for election
            start_time = time.time()
            while time.time() - start_time < 1.5:
                if node.role == NodeRole.LEADER:
                    break
                await asyncio.sleep(0.02)
            assert node.role == NodeRole.LEADER

            # Execute command
            success = await node.execute_command({"op": "SET", "key": "k1", "val": "v1"})
            assert success is True
            assert node.state.commit_index == 1
            assert node.state.last_applied == 1
            assert node.state.kv_store == {"k1": "v1"}
            assert len(node.state.log) == 1
        finally:
            await node.stop()

    asyncio.run(_test())


def test_3_node_cluster_log_replication() -> None:
    """Commands written to Leader replicate to all followers and commit to state machines."""

    async def _test() -> None:
        node_ids = ["node1", "node2", "node3"]
        nodes: List[RaftNode] = []

        for nid in node_ids:
            peers = [p for p in node_ids if p != nid]
            n = RaftNode(
                node_id=nid,
                peers=peers,
                min_election_timeout=0.080,
                max_election_timeout=0.180,
                heartbeat_interval=0.025,
            )
            nodes.append(n)

        for n in nodes:
            await n.rpc.start()

        for n in nodes:
            for peer_node in nodes:
                if peer_node.node_id != n.node_id:
                    n.rpc.peer_addresses[peer_node.node_id] = ("127.0.0.1", peer_node.rpc.port)

        for n in nodes:
            n._is_running = True
            n._election_task = asyncio.create_task(n._run_election_timer())

        try:
            # 1. Wait for Leader election
            leader: RaftNode | None = None
            start_time = time.time()
            while time.time() - start_time < 1.5:
                leaders = [n for n in nodes if n.role == NodeRole.LEADER]
                if len(leaders) == 1:
                    leader = leaders[0]
                    break
                await asyncio.sleep(0.05)

            assert leader is not None, "Failed to elect leader"

            # 2. Submit write command to leader
            success = await leader.execute_command("SET balance=100")
            assert success is True

            # 3. Wait a moment for follower heartbeats to advance follower commit_index
            await asyncio.sleep(0.08)

            # 4. Verify all 3 nodes achieved identical log and state machine
            for n in nodes:
                assert n.state.commit_index == 1
                assert n.state.last_applied == 1
                assert len(n.state.log) == 1
                assert n.state.log[0].index == 1
                assert n.state.log[0].command == "SET balance=100"
                assert n.state.kv_store == {"balance": "100"}

        finally:
            for n in nodes:
                await n.stop()

    asyncio.run(_test())


def test_sequential_writes_ordered_execution() -> None:
    """Multiple sequential commands commit deterministically across the cluster."""

    async def _test() -> None:
        node_ids = ["node1", "node2", "node3"]
        nodes: List[RaftNode] = []

        for nid in node_ids:
            peers = [p for p in node_ids if p != nid]
            n = RaftNode(
                node_id=nid,
                peers=peers,
                min_election_timeout=0.080,
                max_election_timeout=0.180,
                heartbeat_interval=0.025,
            )
            nodes.append(n)

        for n in nodes:
            await n.rpc.start()

        for n in nodes:
            for peer_node in nodes:
                if peer_node.node_id != n.node_id:
                    n.rpc.peer_addresses[peer_node.node_id] = ("127.0.0.1", peer_node.rpc.port)

        for n in nodes:
            n._is_running = True
            n._election_task = asyncio.create_task(n._run_election_timer())

        try:
            # 1. Wait for Leader election
            leader: RaftNode | None = None
            start_time = time.time()
            while time.time() - start_time < 1.5:
                leaders = [n for n in nodes if n.role == NodeRole.LEADER]
                if len(leaders) == 1:
                    leader = leaders[0]
                    break
                await asyncio.sleep(0.05)

            assert leader is not None

            # 2. Execute multiple sequential commands
            commands = [
                {"op": "SET", "key": "a", "val": 10},
                {"op": "SET", "key": "b", "val": 20},
                {"op": "SET", "key": "a", "val": 99},  # Overwrite key 'a'
            ]

            for cmd in commands:
                res = await leader.execute_command(cmd)
                assert res is True

            # Allow followers to sync up
            await asyncio.sleep(0.08)

            # 3. Assert full cluster parity
            expected_kv = {"a": 99, "b": 20}
            for n in nodes:
                assert n.state.commit_index == 3
                assert n.state.last_applied == 3
                assert len(n.state.log) == 3
                assert n.state.kv_store == expected_kv

        finally:
            for n in nodes:
                await n.stop()

    asyncio.run(_test())


if __name__ == "__main__":
    tests = [
        test_non_leader_rejects_execute_command,
        test_single_node_commits_command,
        test_3_node_cluster_log_replication,
        test_sequential_writes_ordered_execution,
    ]
    print(f"Running {len(tests)} tests for Step 4:")
    for t in tests:
        t()
        print(f"  [PASS] {t.__name__}")

    print("\nAll Step 4 tests passed successfully!")
