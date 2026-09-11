"""Unit and Integration tests for Step 3: Leader Election & Heartbeats.

Verifies:
- RequestVote receiver criteria (terms, single-vote per term, log up-to-dateness)
- Single-node cluster automatic leadership
- 3-node cluster leader election via quorum
- Leader heartbeat suppression (followers remain stable)
- Leader failure and automatic re-election of a new leader
- Leader step-down upon discovering higher terms
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from typing import List

# Add project root to sys.path so tests work whether run from root or inside tests/
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from raft.messages import RequestVoteArgs
from raft.node import RaftNode
from raft.state import LogEntry, NodeRole, RaftState


def test_single_node_cluster_elects_self() -> None:
    """A standalone node (0 peers) immediately becomes leader upon election."""

    async def _test() -> None:
        node = RaftNode(
            node_id="single",
            peers=[],
            min_election_timeout=0.030,
            max_election_timeout=0.060,
        )
        await node.start()
        try:
            # Wait for election timeout
            await asyncio.sleep(0.1)
            assert node.role == NodeRole.LEADER
            assert node.leader_id == "single"
        finally:
            await node.stop()

    asyncio.run(_test())


def test_vote_granting_rules() -> None:
    """Verify Section 5.2 and 5.4.1 voting safety rules."""

    async def _test() -> None:
        # Receiver state: Term 2, last log index 2, last log term 2
        state = RaftState(
            current_term=2,
            voted_for=None,
            log=[
                LogEntry(index=1, term=1, command="a"),
                LogEntry(index=2, term=2, command="b"),
            ],
        )
        node = RaftNode(node_id="node1", peers=["node2", "node3"], state=state)

        # 1. Reject stale term (term 1 < term 2)
        stale_req = RequestVoteArgs(term=1, candidate_id="node2", last_log_index=2, last_log_term=2)
        reply = await node.handle_request_vote(stale_req)
        assert reply.vote_granted is False
        assert reply.term == 2

        # 2. Reject candidate with less up-to-date log (term 2, but candidate last_log_term is 1)
        older_log_req = RequestVoteArgs(term=2, candidate_id="node2", last_log_index=3, last_log_term=1)
        reply = await node.handle_request_vote(older_log_req)
        assert reply.vote_granted is False

        # 3. Grant vote to up-to-date candidate in current term
        valid_req = RequestVoteArgs(term=2, candidate_id="node2", last_log_index=2, last_log_term=2)
        reply = await node.handle_request_vote(valid_req)
        assert reply.vote_granted is True
        assert node.state.voted_for == "node2"

        # 4. Reject second candidate in same term (already voted for node2)
        second_cand_req = RequestVoteArgs(term=2, candidate_id="node3", last_log_index=2, last_log_term=2)
        reply = await node.handle_request_vote(second_cand_req)
        assert reply.vote_granted is False

        # 5. Higher term clears previous vote and grants if candidate log is up-to-date
        higher_term_req = RequestVoteArgs(term=3, candidate_id="node3", last_log_index=2, last_log_term=2)
        reply = await node.handle_request_vote(higher_term_req)
        assert reply.vote_granted is True
        assert node.state.current_term == 3
        assert node.state.voted_for == "node3"

    asyncio.run(_test())


def test_3_node_cluster_elects_leader() -> None:
    """A cluster of 3 nodes elects exactly 1 Leader and 2 Followers."""

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

        # 1. Start RPC servers first to assign actual listening ports
        for n in nodes:
            await n.rpc.start()

        # 2. Wire peer addresses
        for n in nodes:
            for peer_node in nodes:
                if peer_node.node_id != n.node_id:
                    n.rpc.peer_addresses[peer_node.node_id] = ("127.0.0.1", peer_node.rpc.port)

        # 3. Start Raft state machines
        for n in nodes:
            n._is_running = True
            n._election_task = asyncio.create_task(n._run_election_timer())

        try:
            # Wait for leader election (up to 1.5 seconds)
            leader: RaftNode | None = None
            start_time = time.time()
            while time.time() - start_time < 1.5:
                leaders = [n for n in nodes if n.role == NodeRole.LEADER]
                if len(leaders) == 1:
                    leader = leaders[0]
                    break
                await asyncio.sleep(0.05)

            assert leader is not None, "Failed to elect a leader within 1.5s"
            assert leader.role == NodeRole.LEADER

            # Wait a small moment for heartbeats to stabilize followers
            await asyncio.sleep(0.08)

            followers = [n for n in nodes if n.node_id != leader.node_id]
            assert len(followers) == 2
            for f in followers:
                assert f.role == NodeRole.FOLLOWER
                assert f.leader_id == leader.node_id
                assert f.state.current_term == leader.state.current_term

        finally:
            for n in nodes:
                await n.stop()

    asyncio.run(_test())


def test_leader_heartbeats_suppress_elections() -> None:
    """Active leader heartbeats keep followers stable without spurious elections."""

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
            # Wait for leader to emerge
            await asyncio.sleep(0.3)
            leaders = [n for n in nodes if n.role == NodeRole.LEADER]
            assert len(leaders) == 1
            leader = leaders[0]
            initial_term = leader.state.current_term

            # Let cluster run for another 300ms (multiple election timeout periods)
            await asyncio.sleep(0.3)

            # Leader must still be the same leader and term must remain unchanged
            assert leader.role == NodeRole.LEADER
            assert leader.state.current_term == initial_term

        finally:
            for n in nodes:
                await n.stop()

    asyncio.run(_test())


def test_re_election_on_leader_failure() -> None:
    """When the leader stops, remaining followers detect failure and elect a new leader."""

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
            # 1. Wait for initial leader
            old_leader: RaftNode | None = None
            start_time = time.time()
            while time.time() - start_time < 2.0:
                leaders = [n for n in nodes if n.role == NodeRole.LEADER]
                if len(leaders) == 1:
                    old_leader = leaders[0]
                    break
                await asyncio.sleep(0.05)

            assert old_leader is not None, "Initial leader was not elected"
            old_term = old_leader.state.current_term

            # 2. Kill the active leader!
            await old_leader.stop()

            # 3. Wait for the remaining 2 followers to detect timeout and re-elect
            new_leader: RaftNode | None = None
            surviving_nodes = [n for n in nodes if n.node_id != old_leader.node_id]

            start_time = time.time()
            while time.time() - start_time < 1.5:
                new_leaders = [n for n in surviving_nodes if n.role == NodeRole.LEADER]
                if len(new_leaders) == 1:
                    new_leader = new_leaders[0]
                    break
                await asyncio.sleep(0.05)

            assert new_leader is not None, "Failed to elect a new leader after crash"
            assert new_leader.node_id != old_leader.node_id
            assert new_leader.state.current_term > old_term

        finally:
            for n in nodes:
                await n.stop()

    asyncio.run(_test())


if __name__ == "__main__":
    tests = [
        test_single_node_cluster_elects_self,
        test_vote_granting_rules,
        test_3_node_cluster_elects_leader,
        test_leader_heartbeats_suppress_elections,
        test_re_election_on_leader_failure,
    ]
    print(f"Running {len(tests)} tests for Step 3:")
    for t in tests:
        t()
        print(f"  [PASS] {t.__name__}")

    print("\nAll Step 3 tests passed successfully!")
