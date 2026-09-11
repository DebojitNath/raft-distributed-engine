"""Raft RPC Messages and Serialization.

Reference: Raft Paper ("In Search of an Understandable Consensus Algorithm")
Section 5.1, 5.2, 5.3, and Figure 2 (RPC Summary).
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Type, Union

from raft.state import LogEntry


@dataclass
class RequestVoteArgs:
    """Arguments for RequestVote RPC (Section 5.2 / Figure 2).

    Invoked by candidates to gather votes.
    """

    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls: Type[RequestVoteArgs], data: Dict[str, Any]) -> RequestVoteArgs:
        return cls(
            term=data["term"],
            candidate_id=data["candidate_id"],
            last_log_index=data["last_log_index"],
            last_log_term=data["last_log_term"],
        )


@dataclass
class RequestVoteReply:
    """Results of RequestVote RPC (Section 5.2 / Figure 2)."""

    term: int
    vote_granted: bool

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls: Type[RequestVoteReply], data: Dict[str, Any]) -> RequestVoteReply:
        return cls(
            term=data["term"],
            vote_granted=data["vote_granted"],
        )


@dataclass
class AppendEntriesArgs:
    """Arguments for AppendEntries RPC (Section 5.3 / Figure 2).

    Invoked by leader to replicate log entries and to send periodic heartbeats.
    """

    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    entries: List[LogEntry] = field(default_factory=list)
    leader_commit: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "term": self.term,
            "leader_id": self.leader_id,
            "prev_log_index": self.prev_log_index,
            "prev_log_term": self.prev_log_term,
            "entries": [entry.to_dict() for entry in self.entries],
            "leader_commit": self.leader_commit,
        }

    @classmethod
    def from_dict(cls: Type[AppendEntriesArgs], data: Dict[str, Any]) -> AppendEntriesArgs:
        entries = [
            LogEntry(
                index=e["index"],
                term=e["term"],
                command=e.get("command"),
            )
            for e in data.get("entries", [])
        ]
        return cls(
            term=data["term"],
            leader_id=data["leader_id"],
            prev_log_index=data["prev_log_index"],
            prev_log_term=data["prev_log_term"],
            entries=entries,
            leader_commit=data.get("leader_commit", 0),
        )


@dataclass
class AppendEntriesReply:
    """Results of AppendEntries RPC (Section 5.3 / Figure 2)."""

    term: int
    success: bool
    match_index: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls: Type[AppendEntriesReply], data: Dict[str, Any]) -> AppendEntriesReply:
        return cls(
            term=data["term"],
            success=data["success"],
            match_index=data.get("match_index", 0),
        )


# Type alias for all RPC messages
RPCMessage = Union[RequestVoteArgs, RequestVoteReply, AppendEntriesArgs, AppendEntriesReply]


def serialize_envelope(sender_id: str, message: RPCMessage) -> str:
    """Package an RPC message into a newline-delimited JSON envelope string."""
    msg_type = type(message).__name__
    envelope = {
        "type": msg_type,
        "sender_id": sender_id,
        "payload": message.to_dict(),
    }
    return json.dumps(envelope) + "\n"


def deserialize_envelope(raw_line: str) -> tuple[str, str, RPCMessage]:
    """Parse a raw JSON envelope line and return (msg_type, sender_id, message_instance)."""
    envelope = json.loads(raw_line.strip())
    msg_type = envelope["type"]
    sender_id = envelope["sender_id"]
    payload = envelope["payload"]

    if msg_type == "RequestVoteArgs":
        msg = RequestVoteArgs.from_dict(payload)
    elif msg_type == "RequestVoteReply":
        msg = RequestVoteReply.from_dict(payload)
    elif msg_type == "AppendEntriesArgs":
        msg = AppendEntriesArgs.from_dict(payload)
    elif msg_type == "AppendEntriesReply":
        msg = AppendEntriesReply.from_dict(payload)
    else:
        raise ValueError(f"Unknown RPC message type: {msg_type}")

    return msg_type, sender_id, msg
