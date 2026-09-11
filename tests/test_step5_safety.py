"""Unit and Integration tests for Step 5: Safety Checks & Log Inconsistency Handling.

Verifies:
- Election Restriction (Section 5.4.1): Voters reject candidates with less up-to-date logs.
- Log Inconsistency Resolution (Section 5.3): Leaders backtrack nextIndex, followers truncate
  conflicting uncommitted entries and achieve log parity.
- Committing entries from previous terms (Section 5.4.2 / Figure 8): Leader only commits
  entries from current term by counting replicas, committing previous entries indirectly.
- Network Partition & Healing: Partitioned followers catch up completely upon link restoration.
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from typing import List

# Add project root to sys.path so tests work whether run from root or inside tests/
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from raft.messages import AppendEntriesArgs, RequestVoteArgs
from raft.node import RaftNode
from raft.state import LogEntry, NodeRole, RaftState


def test_election_restriction_stale_log_rejected() -> None:
    """Voter rejects candidates whose logs are less up-to-date (Section 5.4.1)."""

    async def _test() -> None:
        # Receiver node has log: [#1:Term 1, #2:Term 2, #3:Term 3]
        receiver_state = RaftState(
            current_term=3,
            voted_for=None,
            log=[
                LogEntry(index=1, term=1, command="cmd1"),
                LogEntry(index=2, term=2, command="cmd2"),
                LogEntry(index=3, term=3, command="cmd3"),
            ],
        )
        node = RaftNode(node_id="voter", peers=["candA", "candB", "candC", "candD"], state=receiver_state)

        # 1. Candidate A has shorter log with lower last term (term 2 < 3) -> REJECT
        candA_req = RequestVoteArgs(term=4, candidate_id="candA", last_log_index=2, last_log_term=2)
        replyA = await node.handle_request_vote(candA_req)
        assert replyA.vote_granted is False

        # 2. Candidate B has LONGER log, but lower last term (term 2 < 3) -> REJECT
        candB_req = RequestVoteArgs(term=4, candidate_id="candB", last_log_index=5, last_log_term=2)
        replyB = await node.handle_request_vote(candB_req)
        assert replyB.vote_granted is False

        # 3. Candidate C has identical last term (3) and identical index (3) -> GRANT
        candC_req = RequestVoteArgs(term=4, candidate_id="candC", last_log_index=3, last_log_term=3)
        replyC = await node.handle_request_vote(candC_req)
        assert replyC.vote_granted is True

        # Clear vote to test higher term candidate
        node.state.voted_for = None
        node.state.current_term = 4

        # 4. Candidate D has higher last log term (term 5 > 3) even if shorter -> GRANT
        candD_req = RequestVoteArgs(term=5, candidate_id="candD", last_log_index=2, last_log_term=5)
        replyD = await node.handle_request_vote(candD_req)
        assert replyD.vote_granted is True

    asyncio.run(_test())


def test_divergent_log_truncation_and_overwrite() -> None:
    """Follower with conflicting uncommitted entries truncates them and adopts leader log."""

    async def _test() -> None:
        # Follower log: [#1:T1, #2:T1, #3:T2, #4:T2] (conflicting uncommitted entries from old Term 2 leader)
        follower_state = RaftState(
            current_term=3,
            voted_for=None,
            log=[
                LogEntry(index=1, term=1, command="SET x=1"),
                LogEntry(index=2, term=1, command="SET y=1"),
                LogEntry(index=3, term=2, command="SET z=OLD2"),
                LogEntry(index=4, term=2, command="SET w=OLD2"),
            ],
            commit_index=2,
            last_applied=2,
            kv_store={"x": "1", "y": "1"},
        )
        follower = RaftNode(node_id="follower1", peers=["leader"], state=follower_state)
        follower._is_running = True

        # Leader log: [#1:T1, #2:T1, #3:T3, #4:T3, #5:T3]
        # Step 1: Leader sends AppendEntries starting at index 5 (prev_log_index=4, prev_log_term=3)
        args1 = AppendEntriesArgs(
            term=3,
            leader_id="leader",
            prev_log_index=4,
            prev_log_term=3,  # Leader's term at index 4 is 3, but follower has 2!
            entries=[LogEntry(index=5, term=3, command="SET a=LEADER")],
            leader_commit=2,
        )
        reply1 = await follower.handle_append_entries(args1)
        assert reply1.success is False  # Rejected because term at index 4 mismatches!

        # Step 2: Leader backtracks to index 3 (prev_log_index=2, prev_log_term=1)
        # Leader sends entries #3, #4, #5 with Term 3
        args2 = AppendEntriesArgs(
            term=3,
            leader_id="leader",
            prev_log_index=2,
            prev_log_term=1,  # Matches follower's entry #2!
            entries=[
                LogEntry(index=3, term=3, command="SET z=NEW3"),
                LogEntry(index=4, term=3, command="SET w=NEW3"),
                LogEntry(index=5, term=3, command="SET a=LEADER"),
            ],
            leader_commit=5,
        )
        reply2 = await follower.handle_append_entries(args2)
        assert reply2.success is True
        assert reply2.match_index == 5

        # Verify follower truncated old Term 2 entries and adopted Term 3 entries
        assert len(follower.state.log) == 5
        assert follower.state.log[2].command == "SET z=NEW3"
        assert follower.state.log[2].term == 3
        assert follower.state.log[3].command == "SET w=NEW3"
        assert follower.state.log[3].term == 3
        assert follower.state.log[4].command == "SET a=LEADER"
        assert follower.state.log[4].term == 3

        # Verify state machine applied all 5 committed commands
        assert follower.state.commit_index == 5
        assert follower.state.last_applied == 5
        assert follower.state.kv_store == {"x": "1", "y": "1", "z": "NEW3", "w": "NEW3", "a": "LEADER"}

    asyncio.run(_test())


def test_section_5_4_2_commit_rule() -> None:
    """Leader does not commit entries from previous terms until current-term entry achieves quorum."""

    async def _test() -> None:
        # Leader in Term 3 has an uncommitted entry from Term 2 at index 1
        leader_state = RaftState(
            current_term=3,
            voted_for="leader",
            log=[
                LogEntry(index=1, term=2, command="SET old=2"),
            ],
            commit_index=0,
            last_applied=0,
        )
        leader = RaftNode(node_id="leader", peers=["node2", "node3"], state=leader_state)
        leader.become_leader()

        # Simulate that index 1 (Term 2) is replicated on node2 (majority: leader + node2 = 2/3)
        leader.state.match_index["node2"] = 1
        leader.state.match_index["node3"] = 0

        # Section 5.4.2 rule: Even though index 1 is on a majority, leader MUST NOT advance commit_index
        # because log[0].term (2) != leader current_term (3)
        advanced = leader.check_advance_commit_index()
        assert advanced is False
        assert leader.state.commit_index == 0
        assert leader.state.kv_store == {}

        # Now append a NEW entry in the CURRENT term (Term 3)
        leader.state.log.append(LogEntry(index=2, term=3, command="SET cur=3"))

        # Replicate index 2 to node2 as well
        leader.state.match_index["node2"] = 2

        # Now leader can commit index 2 (current term entry achieved quorum)!
        advanced2 = leader.check_advance_commit_index()
        assert advanced2 is True
        assert leader.state.commit_index == 2
        assert leader.state.last_applied == 2

        # Both the old Term 2 entry and new Term 3 entry are now safely committed
        assert leader.state.kv_store == {"old": "2", "cur": "3"}

    asyncio.run(_test())


def test_partition_and_heal_recovers_diverged_follower() -> None:
    """Partitioned follower that missed multiple commits recovers to 100% parity upon healing."""

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
            # 1. Elect initial leader
            leader: RaftNode | None = None
            start_time = time.time()
            while time.time() - start_time < 1.5:
                leaders = [n for n in nodes if n.role == NodeRole.LEADER]
                if len(leaders) == 1:
                    leader = leaders[0]
                    break
                await asyncio.sleep(0.05)

            assert leader is not None

            # 2. Pick a follower and isolate it (simulate cut cable / partition)
            isolated_follower = [n for n in nodes if n.node_id != leader.node_id][0]
            majority_follower = [n for n in nodes if n.node_id != leader.node_id and n.node_id != isolated_follower.node_id][0]

            # Sever connection between leader/majority and the isolated follower
            leader.rpc.add_ignored(isolated_follower.node_id)
            majority_follower.rpc.add_ignored(isolated_follower.node_id)
            isolated_follower.rpc.add_ignored(leader.node_id)
            isolated_follower.rpc.add_ignored(majority_follower.node_id)

            # 3. Write 2 commands to the majority partition (Leader + majority_follower = 2/3 quorum)
            res1 = await leader.execute_command({"op": "SET", "key": "k1", "val": "v1"})
            res2 = await leader.execute_command({"op": "SET", "key": "k2", "val": "v2"})
            assert res1 is True
            assert res2 is True

            # Leader and majority follower committed entries 1 and 2
            assert leader.state.commit_index == 2
            assert leader.state.kv_store == {"k1": "v1", "k2": "v2"}

            # Isolated follower has missed these writes
            assert isolated_follower.state.commit_index == 0
            assert isolated_follower.state.kv_store == {}

            # 4. Heal the partition!
            leader.rpc.clear_ignored()
            majority_follower.rpc.clear_ignored()
            isolated_follower.rpc.clear_ignored()

            # 5. Wait a short period for leader AppendEntries to discover and heal the follower
            start_catchup = time.time()
            while time.time() - start_catchup < 1.5:
                if isolated_follower.state.commit_index == 2:
                    break
                await asyncio.sleep(0.05)

            # Assert full cluster parity after healing
            assert isolated_follower.state.commit_index == 2
            assert isolated_follower.state.last_applied == 2
            assert isolated_follower.state.kv_store == {"k1": "v1", "k2": "v2"}

        finally:
            for n in nodes:
                await n.stop()

    asyncio.run(_test())


if __name__ == "__main__":
    tests = [
        test_election_restriction_stale_log_rejected,
        test_divergent_log_truncation_and_overwrite,
        test_section_5_4_2_commit_rule,
        test_partition_and_heal_recovers_diverged_follower,
    ]
    print(f"Running {len(tests)} tests for Step 5:")
    for t in tests:
        t()
        print(f"  [PASS] {t.__name__}")

    print("\nAll Step 5 tests passed successfully!")
