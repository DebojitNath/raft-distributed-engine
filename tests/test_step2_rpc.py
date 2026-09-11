"""Unit and Integration tests for Step 2: RPC Layer & Network Abstraction.

Verifies:
- Dataclass serialization/deserialization and envelope framing
- Asyncio TCP RPC transmission between nodes over localhost
- Fault injection filters (network partitions / packet drops)
- Partition healing
- Connection timeout handling for offline nodes
"""

from __future__ import annotations

import asyncio
import os
import sys

# Add project root to sys.path so tests work whether run from root or inside tests/
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from raft.messages import (
    AppendEntriesArgs,
    AppendEntriesReply,
    RequestVoteArgs,
    RequestVoteReply,
    deserialize_envelope,
    serialize_envelope,
)
from raft.rpc import RPCManager
from raft.state import LogEntry


def test_request_vote_args_serialization() -> None:
    """RequestVoteArgs serializes and deserializes accurately."""
    args = RequestVoteArgs(term=3, candidate_id="node1", last_log_index=5, last_log_term=2)
    raw = serialize_envelope("node1", args)
    msg_type, sender_id, deserialized = deserialize_envelope(raw)

    assert msg_type == "RequestVoteArgs"
    assert sender_id == "node1"
    assert isinstance(deserialized, RequestVoteArgs)
    assert deserialized.term == 3
    assert deserialized.candidate_id == "node1"
    assert deserialized.last_log_index == 5
    assert deserialized.last_log_term == 2


def test_request_vote_reply_serialization() -> None:
    """RequestVoteReply serializes and deserializes accurately."""
    reply = RequestVoteReply(term=3, vote_granted=True)
    raw = serialize_envelope("node2", reply)
    msg_type, sender_id, deserialized = deserialize_envelope(raw)

    assert msg_type == "RequestVoteReply"
    assert sender_id == "node2"
    assert isinstance(deserialized, RequestVoteReply)
    assert deserialized.term == 3
    assert deserialized.vote_granted is True


def test_append_entries_args_with_entries_serialization() -> None:
    """AppendEntriesArgs serializes nested LogEntries correctly."""
    entries = [
        LogEntry(index=1, term=1, command={"op": "SET", "key": "x", "val": 10}),
        LogEntry(index=2, term=2, command={"op": "SET", "key": "y", "val": 20}),
    ]
    args = AppendEntriesArgs(
        term=2,
        leader_id="leader1",
        prev_log_index=0,
        prev_log_term=0,
        entries=entries,
        leader_commit=1,
    )
    raw = serialize_envelope("leader1", args)
    msg_type, sender_id, deserialized = deserialize_envelope(raw)

    assert msg_type == "AppendEntriesArgs"
    assert sender_id == "leader1"
    assert isinstance(deserialized, AppendEntriesArgs)
    assert deserialized.term == 2
    assert deserialized.leader_id == "leader1"
    assert len(deserialized.entries) == 2
    assert deserialized.entries[0].index == 1
    assert deserialized.entries[0].command == {"op": "SET", "key": "x", "val": 10}
    assert deserialized.entries[1].index == 2
    assert deserialized.leader_commit == 1


def test_append_entries_reply_serialization() -> None:
    """AppendEntriesReply serializes and deserializes accurately."""
    reply = AppendEntriesReply(term=2, success=True, match_index=2)
    raw = serialize_envelope("node2", reply)
    msg_type, sender_id, deserialized = deserialize_envelope(raw)

    assert msg_type == "AppendEntriesReply"
    assert sender_id == "node2"
    assert isinstance(deserialized, AppendEntriesReply)
    assert deserialized.term == 2
    assert deserialized.success is True
    assert deserialized.match_index == 2


def test_request_vote_rpc_exchange() -> None:
    """Two RPCManagers successfully exchange RequestVote over localhost TCP."""

    async def _test() -> None:
        rpc1 = RPCManager(node_id="node1", host="127.0.0.1", port=0)
        rpc2 = RPCManager(node_id="node2", host="127.0.0.1", port=0)

        # Register handler on node2
        async def handle_vote(args: RequestVoteArgs) -> RequestVoteReply:
            assert args.candidate_id == "node1"
            assert args.term == 1
            return RequestVoteReply(term=1, vote_granted=True)

        rpc2.request_vote_handler = handle_vote

        await rpc1.start()
        await rpc2.start()

        # Link peer addresses
        rpc1.peer_addresses["node2"] = ("127.0.0.1", rpc2.port)
        rpc2.peer_addresses["node1"] = ("127.0.0.1", rpc1.port)

        try:
            vote_args = RequestVoteArgs(term=1, candidate_id="node1", last_log_index=0, last_log_term=0)
            reply = await rpc1.send_request_vote("node2", vote_args)

            assert reply is not None
            assert reply.term == 1
            assert reply.vote_granted is True
        finally:
            await rpc1.stop()
            await rpc2.stop()

    asyncio.run(_test())


def test_append_entries_rpc_exchange() -> None:
    """Two RPCManagers exchange AppendEntries with entries over localhost TCP."""

    async def _test() -> None:
        rpc1 = RPCManager(node_id="node1", host="127.0.0.1", port=0)
        rpc2 = RPCManager(node_id="node2", host="127.0.0.1", port=0)

        # Register handler on node2
        async def handle_append(args: AppendEntriesArgs) -> AppendEntriesReply:
            assert args.leader_id == "node1"
            assert len(args.entries) == 1
            assert args.entries[0].command == "SET x=1"
            return AppendEntriesReply(term=args.term, success=True, match_index=1)

        rpc2.append_entries_handler = handle_append

        await rpc1.start()
        await rpc2.start()

        rpc1.peer_addresses["node2"] = ("127.0.0.1", rpc2.port)
        rpc2.peer_addresses["node1"] = ("127.0.0.1", rpc1.port)

        try:
            entries = [LogEntry(index=1, term=1, command="SET x=1")]
            append_args = AppendEntriesArgs(
                term=1,
                leader_id="node1",
                prev_log_index=0,
                prev_log_term=0,
                entries=entries,
                leader_commit=0,
            )
            reply = await rpc1.send_append_entries("node2", append_args)

            assert reply is not None
            assert reply.term == 1
            assert reply.success is True
            assert reply.match_index == 1
        finally:
            await rpc1.stop()
            await rpc2.stop()

    asyncio.run(_test())


def test_fault_injection_partition_and_heal() -> None:
    """Fault injection filter drops packets during partition and recovers upon healing."""

    async def _test() -> None:
        rpc1 = RPCManager(node_id="node1", host="127.0.0.1", port=0)
        rpc2 = RPCManager(node_id="node2", host="127.0.0.1", port=0)

        async def handle_vote(args: RequestVoteArgs) -> RequestVoteReply:
            return RequestVoteReply(term=args.term, vote_granted=True)

        rpc2.request_vote_handler = handle_vote

        await rpc1.start()
        await rpc2.start()

        rpc1.peer_addresses["node2"] = ("127.0.0.1", rpc2.port)
        rpc2.peer_addresses["node1"] = ("127.0.0.1", rpc1.port)

        try:
            vote_args = RequestVoteArgs(term=1, candidate_id="node1", last_log_index=0, last_log_term=0)

            # 1. Normal state: RPC succeeds
            reply = await rpc1.send_request_vote("node2", vote_args)
            assert reply is not None
            assert reply.vote_granted is True

            # 2. Outgoing partition: node1 ignores node2
            rpc1.add_ignored("node2")
            reply_blocked_sender = await rpc1.send_request_vote("node2", vote_args)
            assert reply_blocked_sender is None  # Dropped!

            # 3. Heal node1, but sever on node2 (incoming partition)
            rpc1.clear_ignored()
            rpc2.add_ignored("node1")
            reply_blocked_receiver = await rpc1.send_request_vote("node2", vote_args)
            assert reply_blocked_receiver is None  # Dropped by receiver!

            # 4. Heal all links
            rpc2.clear_ignored()
            healed_reply = await rpc1.send_request_vote("node2", vote_args)
            assert healed_reply is not None
            assert healed_reply.vote_granted is True
        finally:
            await rpc1.stop()
            await rpc2.stop()

    asyncio.run(_test())


def test_offline_peer_timeout() -> None:
    """RPC call to an offline port fails gracefully and returns None without crashing."""

    async def _test() -> None:
        rpc1 = RPCManager(node_id="node1", host="127.0.0.1", port=0)
        await rpc1.start()

        # Assign non-existent port (offline peer)
        rpc1.peer_addresses["node_dead"] = ("127.0.0.1", 59999)

        try:
            vote_args = RequestVoteArgs(term=1, candidate_id="node1", last_log_index=0, last_log_term=0)
            reply = await rpc1.send_request_vote("node_dead", vote_args, timeout=0.1)
            assert reply is None
        finally:
            await rpc1.stop()

    asyncio.run(_test())


if __name__ == "__main__":
    tests = [
        test_request_vote_args_serialization,
        test_request_vote_reply_serialization,
        test_append_entries_args_with_entries_serialization,
        test_append_entries_reply_serialization,
        test_request_vote_rpc_exchange,
        test_append_entries_rpc_exchange,
        test_fault_injection_partition_and_heal,
        test_offline_peer_timeout,
    ]
    print(f"Running {len(tests)} tests for Step 2:")
    for t in tests:
        t()
        print(f"  [PASS] {t.__name__}")

    print("\nAll Step 2 tests passed successfully!")
