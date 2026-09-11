"""Raft State Data Structures and Invariants.

Reference: Raft Paper ("In Search of an Understandable Consensus Algorithm")
Section 5.1 (Raft basics), Section 5.2 (Leader election), Figure 2 (State summary).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class NodeRole(str, Enum):
    """The three operational states a Raft node can be in (Figure 4)."""

    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"


@dataclass
class LogEntry:
    """A single entry in the replicated log (Figure 2: State).

    Attributes:
        index: Position in the log (1-based index as in the Raft paper).
        term: Term when entry was received by leader.
        command: Command for state machine (can be any serializable object).
    """

    index: int
    term: int
    command: Any

    def to_dict(self) -> Dict[str, Any]:
        """Convert entry to dictionary representation for serialization / telemetry."""
        return {
            "index": self.index,
            "term": self.term,
            "command": self.command,
        }


@dataclass
class RaftState:
    """State maintained by each Raft node (Figure 2: State).

    Persistent state on all servers:
        current_term: Latest term server has seen (initialized to 0 on first boot,
            increases monotonically).
        voted_for: CandidateId that received vote in current term (or None if none).
        log: Log entries; each entry contains command for state machine, and term
            when entry was received by leader (first index is 1).

    Volatile state on all servers:
        commit_index: Index of highest log entry known to be committed
            (initialized to 0, increases monotonically).
        last_applied: Index of highest log entry applied to state machine
            (initialized to 0, increases monotonically).
        kv_store: In-memory Key-Value store state machine.

    Volatile state on leaders (Reinitialized after election):
        next_index: For each server, index of the next log entry to send to that server
            (initialized to leader last log index + 1).
        match_index: For each server, index of highest log entry known to be replicated
            on server (initialized to 0, increases monotonically).
    """

    # Persistent state on all servers (Figure 2)
    current_term: int = 0
    voted_for: Optional[str] = None
    log: List[LogEntry] = field(default_factory=list)

    # Volatile state on all servers (Figure 2 & State Machine)
    commit_index: int = 0
    last_applied: int = 0
    kv_store: Dict[str, Any] = field(default_factory=dict)

    # Volatile state on leaders only (Figure 2)
    next_index: Dict[str, int] = field(default_factory=dict)
    match_index: Dict[str, int] = field(default_factory=dict)

    @property
    def last_log_index(self) -> int:
        """Return the index of the last log entry, or 0 if the log is empty."""
        if not self.log:
            return 0
        return self.log[-1].index

    @property
    def last_log_term(self) -> int:
        """Return the term of the last log entry, or 0 if the log is empty."""
        if not self.log:
            return 0
        return self.log[-1].term

    def init_leader_state(self, peers: List[str]) -> None:
        """Reinitialize volatile leader state after winning an election (Figure 2).

        For each peer:
        - next_index is set to leader's last log index + 1
        - match_index is set to 0
        """
        next_idx = self.last_log_index + 1
        self.next_index = {peer: next_idx for peer in peers}
        self.match_index = {peer: 0 for peer in peers}
