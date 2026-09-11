"""Unit tests for Step 1: Core State Machine & Foundations.

Verifies Raft paper Section 5.1, 5.2, and Figure 2 specifications:
- Node initialization defaults
- Candidate transition & term increment
- Leader transition & volatile state initialization
- All-server rule for higher terms
- Log indexing and properties
"""


import os
import sys

# Add project root to sys.path so tests work whether run from root or inside tests/
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from raft.node import RaftNode
from raft.state import LogEntry, NodeRole, RaftState


def test_node_initial_state() -> None:
    """A freshly initialized node must start as a Follower at Term 0 (Figure 2)."""
    peers = ["node2", "node3"]
    node = RaftNode(node_id="node1", peers=peers)

    assert node.node_id == "node1"
    assert node.peers == ["node2", "node3"]
    assert node.role == NodeRole.FOLLOWER

    # Persistent state defaults
    assert node.state.current_term == 0
    assert node.state.voted_for is None
    assert node.state.log == []

    # Volatile state defaults
    assert node.state.commit_index == 0
    assert node.state.last_applied == 0
    assert node.state.next_index == {}
    assert node.state.match_index == {}

    # Empty log properties
    assert node.state.last_log_index == 0
    assert node.state.last_log_term == 0


def test_transition_to_candidate() -> None:
    """Transitioning to Candidate increments term and votes for self (Section 5.2)."""
    node = RaftNode(node_id="node1", peers=["node2", "node3"])

    node.become_candidate()
    assert node.role == NodeRole.CANDIDATE
    assert node.state.current_term == 1
    assert node.state.voted_for == "node1"

    # Simulating a split vote / restart election:
    node.become_candidate()
    assert node.role == NodeRole.CANDIDATE
    assert node.state.current_term == 2
    assert node.state.voted_for == "node1"


def test_transition_to_leader_initializes_volatile_state() -> None:
    """Leader initializes next_index and match_index for all peers (Figure 2)."""
    peers = ["node2", "node3"]
    state = RaftState(
        current_term=1,
        voted_for="node1",
        log=[
            LogEntry(index=1, term=1, command="SET x=1"),
            LogEntry(index=2, term=1, command="SET y=2"),
        ],
    )
    node = RaftNode(node_id="node1", peers=peers, state=state)
    node.become_candidate()
    assert node.state.current_term == 2

    node.become_leader()
    assert node.role == NodeRole.LEADER

    # next_index must be initialized to leader's last_log_index + 1 (2 + 1 = 3)
    assert node.state.next_index == {"node2": 3, "node3": 3}
    # match_index must be initialized to 0
    assert node.state.match_index == {"node2": 0, "node3": 0}


def test_higher_term_converts_candidate_to_follower() -> None:
    """If Candidate sees a higher term, it must become Follower and clear vote (Figure 2)."""
    node = RaftNode(node_id="node1", peers=["node2", "node3"])
    node.become_candidate()  # term = 1, voted_for = "node1"

    # Peer responds with term 3
    updated = node.update_term_if_higher(3)

    assert updated is True
    assert node.role == NodeRole.FOLLOWER
    assert node.state.current_term == 3
    assert node.state.voted_for is None


def test_higher_term_converts_leader_to_follower() -> None:
    """If Leader sees a higher term, it must step down to Follower (Figure 2)."""
    node = RaftNode(node_id="node1", peers=["node2", "node3"])
    node.become_candidate()  # term = 1
    node.become_leader()
    assert node.role == NodeRole.LEADER

    # Discover a peer with term 5
    updated = node.update_term_if_higher(5)

    assert updated is True
    assert node.role == NodeRole.FOLLOWER
    assert node.state.current_term == 5
    assert node.state.voted_for is None


def test_lower_or_equal_term_does_not_update() -> None:
    """Terms <= current_term should not trigger a role step-down or term update."""
    node = RaftNode(node_id="node1", peers=["node2", "node3"])
    node.become_candidate()  # term = 1
    node.become_leader()

    # Equal term
    assert node.update_term_if_higher(1) is False
    assert node.role == NodeRole.LEADER
    assert node.state.current_term == 1

    # Lower term
    assert node.update_term_if_higher(0) is False
    assert node.role == NodeRole.LEADER
    assert node.state.current_term == 1


def test_log_entry_indexing() -> None:
    """LogEntry and RaftState properties return correct last log index and term."""
    state = RaftState()
    assert state.last_log_index == 0
    assert state.last_log_term == 0
    assert state.kv_store == {}

    entry1 = LogEntry(index=1, term=1, command={"op": "set", "k": "a", "v": 10})
    assert entry1.to_dict() == {"index": 1, "term": 1, "command": {"op": "set", "k": "a", "v": 10}}
    state.log.append(entry1)
    assert state.last_log_index == 1
    assert state.last_log_term == 1

    entry2 = LogEntry(index=2, term=3, command={"op": "set", "k": "b", "v": 20})
    state.log.append(entry2)
    assert state.last_log_index == 2
    assert state.last_log_term == 3


def test_telemetry_state_change_hook() -> None:
    """RaftNode dispatches state change events to registered callbacks (ARCHITECTURE.md)."""
    node = RaftNode(node_id="node1", peers=["node2", "node3"])
    events = []
    node.on_state_change_callbacks.append(lambda e: events.append(e))

    node.become_candidate()
    assert len(events) == 1
    assert events[0]["node_id"] == "node1"
    assert events[0]["role"] == "CANDIDATE"
    assert events[0]["term"] == 1
    assert events[0]["log"] == []
    assert events[0]["kv_store"] == {}

    node.become_leader()
    assert len(events) == 2
    assert events[1]["role"] == "LEADER"


def test_telemetry_rpc_event_hook() -> None:
    """RaftNode dispatches in-flight RPC events for visual flight animations."""
    node = RaftNode(node_id="node1", peers=["node2", "node3"])
    rpc_events = []
    node.on_rpc_event_callbacks.append(lambda e: rpc_events.append(e))

    event = node.emit_rpc_event(
        from_id="node1",
        to_id="node2",
        msg_type="HEARTBEAT",
        payload={"term": 1, "leader_id": "node1"},
    )
    assert len(rpc_events) == 1
    assert rpc_events[0]["from"] == "node1"
    assert rpc_events[0]["to"] == "node2"
    assert rpc_events[0]["type"] == "HEARTBEAT"
    assert "timestamp" in rpc_events[0]


if __name__ == "__main__":
    tests = [
        test_node_initial_state,
        test_transition_to_candidate,
        test_transition_to_leader_initializes_volatile_state,
        test_higher_term_converts_candidate_to_follower,
        test_higher_term_converts_leader_to_follower,
        test_lower_or_equal_term_does_not_update,
        test_log_entry_indexing,
        test_telemetry_state_change_hook,
        test_telemetry_rpc_event_hook,
    ]
    print(f"Running {len(tests)} tests for Step 1:")
    for test in tests:
        test()
        print(f"  [PASS] {test.__name__}")
    print("\nAll Step 1 tests passed successfully!")
