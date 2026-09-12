"""Raft Node Implementation.

Reference: Raft Paper ("In Search of an Understandable Consensus Algorithm")
Section 5.1 (Raft basics), Section 5.2 (Leader election), Section 5.3 (Log replication / heartbeats),
Figure 2 (Rules for Servers).
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
import os
from typing import Any, Callable, Dict, List, Optional, Tuple

from raft.messages import (
    AppendEntriesArgs,
    AppendEntriesReply,
    RequestVoteArgs,
    RequestVoteReply,
)
from raft.rpc import RPCManager
from raft.state import LogEntry, NodeRole, RaftState
from raft.wal import WALStorage

logger = logging.getLogger(__name__)


def apply_command_to_kv_store(kv_store: Dict[str, Any], command: Any) -> None:
    """Helper to apply a state machine command to the in-memory KV dictionary."""
    if isinstance(command, dict):
        op = command.get("op", "SET").upper()
        if op == "SET":
            key = command.get("key") or command.get("k")
            val = command.get("val") if "val" in command else command.get("value")
            if key is not None:
                kv_store[str(key)] = val
        elif op in ("DELETE", "DEL"):
            key = command.get("key") or command.get("k")
            if key is not None:
                kv_store.pop(str(key), None)
    elif isinstance(command, str):
        parts = command.strip().split()
        if len(parts) >= 2 and parts[0].upper() in ("SET", "PUT"):
            rest = " ".join(parts[1:])
            if "=" in rest:
                k, v = rest.split("=", 1)
                kv_store[k.strip()] = v.strip()
            elif len(parts) == 3:
                kv_store[parts[1].strip()] = parts[2].strip()
        elif "=" in command:
            k, v = command.split("=", 1)
            kv_store[k.strip()] = v.strip()


class RaftNode:
    """Represents a single Raft cluster member.

    Manages role transitions, RPC handlers, randomized election timers,
    log replication, and state machine commits according to Figure 2.
    """

    def __init__(
        self,
        node_id: str,
        peers: Optional[List[str]] = None,
        state: Optional[RaftState] = None,
        rpc: Optional[RPCManager] = None,
        host: str = "127.0.0.1",
        port: int = 0,
        peer_addresses: Optional[Dict[str, Tuple[str, int]]] = None,
        min_election_timeout: float = 0.150,
        max_election_timeout: float = 0.300,
        heartbeat_interval: float = 0.050,
        wal_dir: Optional[str] = None,
    ) -> None:
        self.node_id = node_id
        self.peers = list(peers) if peers is not None else []
        self.state = state if state is not None else RaftState()
        self.role = NodeRole.FOLLOWER
        self.leader_id: Optional[str] = None

        self.wal: Optional[WALStorage] = None
        if wal_dir:
            wal_path = os.path.join(wal_dir, f"{self.node_id}.wal")
            self.wal = WALStorage(wal_path)
            r_term, r_voted, r_log = self.wal.recover()
            if r_term > 0 or r_voted or r_log:
                self.state.current_term = r_term
                self.state.voted_for = r_voted
                self.state.log = r_log

        # Timing configurations (in seconds)
        self.min_election_timeout = min_election_timeout
        self.max_election_timeout = max_election_timeout
        self.heartbeat_interval = heartbeat_interval

        # RPC & Networking Layer
        if rpc is not None:
            self.rpc = rpc
        else:
            self.rpc = RPCManager(
                node_id=self.node_id,
                host=host,
                port=port,
                peer_addresses=peer_addresses,
            )

        # Register RPC handlers on the network manager
        self.rpc.request_vote_handler = self.handle_request_vote
        self.rpc.append_entries_handler = self.handle_append_entries

        # Background tasks and execution control
        self._is_running = False
        self._election_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._heartbeat_received_event = asyncio.Event()

        # Telemetry & Observer hooks (ARCHITECTURE.md Section 3)
        self.on_state_change_callbacks: List[Callable[[Dict[str, Any]], None]] = []
        self.on_rpc_event_callbacks: List[Callable[[Dict[str, Any]], None]] = []

    # -------------------------------------------------------------------------
    # Telemetry Helpers
    # -------------------------------------------------------------------------

    def emit_state_change(self) -> Dict[str, Any]:
        """Emit a telemetry event representing the current node state."""
        event = {
            "node_id": self.node_id,
            "role": self.role.value,  # FOLLOWER, CANDIDATE, LEADER
            "leader_id": self.leader_id,
            "term": self.state.current_term,
            "commit_index": self.state.commit_index,
            "last_applied": self.state.last_applied,
            "log": [entry.to_dict() for entry in self.state.log],
            "kv_store": dict(self.state.kv_store),
            "is_running": self._is_running,
        }
        for cb in self.on_state_change_callbacks:
            try:
                cb(event)
            except Exception as e:
                logger.warning("Error in state change callback: %s", e)
        return event

    def emit_rpc_event(self, from_id: str, to_id: str, msg_type: str, payload: Any) -> Dict[str, Any]:
        """Emit a telemetry event representing an RPC message in flight or processed."""
        event = {
            "from": from_id,
            "to": to_id,
            "type": msg_type,  # 'HEARTBEAT', 'REQUEST_VOTE', 'APPEND_ENTRIES'
            "payload": payload,
            "timestamp": time.time(),
        }
        for cb in self.on_rpc_event_callbacks:
            try:
                cb(event)
            except Exception as e:
                logger.warning("Error in rpc event callback: %s", e)
        return event

    # -------------------------------------------------------------------------
    # State Machine Application (All Servers Rule 1)
    # -------------------------------------------------------------------------

    def apply_entries_to_state_machine(self) -> None:
        """Apply committed log entries to the local in-memory Key-Value store."""
        while self.state.commit_index > self.state.last_applied:
            self.state.last_applied += 1
            idx = self.state.last_applied
            if idx <= len(self.state.log):
                entry = self.state.log[idx - 1]
                apply_command_to_kv_store(self.state.kv_store, entry.command)
                logger.info(
                    "Node %s applied entry #%d (%s) to kv_store: %s",
                    self.node_id,
                    idx,
                    entry.command,
                    self.state.kv_store,
                )
        self.emit_state_change()

    # -------------------------------------------------------------------------
    # State Transitions & Role Changes
    # -------------------------------------------------------------------------

    def become_follower(self, term: Optional[int] = None) -> None:
        """Transition node to Follower state (Section 5.1 / Figure 2).

        Args:
            term: If provided and greater than current_term, updates current_term
                and clears voted_for.
        """
        if term is not None and term > self.state.current_term:
            self.state.current_term = term
            self.state.voted_for = None
            if self.wal:
                self.wal.append_term_vote(self.state.current_term, self.state.voted_for)

        self.role = NodeRole.FOLLOWER

        # Stop leader heartbeat loop if stepping down from leader
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            self._heartbeat_task = None

        # Ensure election timer task is running
        if self._is_running and (self._election_task is None or self._election_task.done()):
            self._election_task = asyncio.create_task(self._run_election_timer())

        self.reset_election_timer()
        logger.info(
            "Node %s converted to FOLLOWER (Term: %d)",
            self.node_id,
            self.state.current_term,
        )
        self.emit_state_change()

    def become_candidate(self) -> None:
        """Transition node to Candidate state and start election (Section 5.2).

        On conversion to candidate:
        1. Increment current_term
        2. Vote for self
        3. Set role to CANDIDATE
        """
        self.state.current_term += 1
        self.state.voted_for = self.node_id
        if self.wal:
            self.wal.append_term_vote(self.state.current_term, self.state.voted_for)
        self.role = NodeRole.CANDIDATE
        self.leader_id = None
        logger.info(
            "Node %s converted to CANDIDATE (Term: %d)",
            self.node_id,
            self.state.current_term,
        )
        self.emit_state_change()

    def become_leader(self) -> None:
        """Transition node to Leader state upon winning election (Section 5.2).

        On conversion to leader:
        1. Set role to LEADER
        2. Reinitialize volatile leader state (next_index and match_index for all peers)
        """
        self.role = NodeRole.LEADER
        self.leader_id = self.node_id
        self.state.init_leader_state(self.peers)

        # Wake up election timer loop so it exits while LEADER
        self._heartbeat_received_event.set()

        logger.info(
            "Node %s converted to LEADER (Term: %d)",
            self.node_id,
            self.state.current_term,
        )
        self.emit_state_change()

    def update_term_if_higher(self, term: int) -> bool:
        """Rule for all servers (Figure 2):

        If RPC request or response contains term T > currentTerm:
        set currentTerm = T, convert to Follower (and reset voted_for).

        Returns:
            True if term was strictly higher and updated; False otherwise.
        """
        if term > self.state.current_term:
            logger.info(
                "Node %s observed higher term %d (current: %d). Reverting to FOLLOWER.",
                self.node_id,
                term,
                self.state.current_term,
            )
            self.become_follower(term=term)
            return True
        return False

    # -------------------------------------------------------------------------
    # RPC Handlers (Receiver Implementation - Figure 2)
    # -------------------------------------------------------------------------

    async def handle_request_vote(self, args: RequestVoteArgs) -> RequestVoteReply:
        """Receiver implementation for RequestVote RPC (Section 5.2 / 5.4.1 / Figure 2)."""
        self.emit_rpc_event(args.candidate_id, self.node_id, "REQUEST_VOTE", args.to_dict())

        # 1. Rule: If term > currentTerm, update term and become follower
        if args.term > self.state.current_term:
            self.become_follower(term=args.term)

        # 2. Rule: Reply false if term < currentTerm (Figure 2)
        if args.term < self.state.current_term:
            return RequestVoteReply(term=self.state.current_term, vote_granted=False)

        # 3. Rule: Check vote availability (voted_for is None or already voted for this candidate)
        can_vote = self.state.voted_for is None or self.state.voted_for == args.candidate_id

        # 4. Rule: Election Safety restriction (Section 5.4.1)
        # Candidate's log must be at least as up-to-date as receiver's log
        cand_last_term = args.last_log_term
        cand_last_index = args.last_log_index
        my_last_term = self.state.last_log_term
        my_last_index = self.state.last_log_index

        is_log_up_to_date = (cand_last_term > my_last_term) or (
            cand_last_term == my_last_term and cand_last_index >= my_last_index
        )

        if can_vote and is_log_up_to_date:
            self.state.voted_for = args.candidate_id
            if self.wal:
                self.wal.append_term_vote(self.state.current_term, self.state.voted_for)
            logger.info(
                "Node %s granted vote to %s for Term %d",
                self.node_id,
                args.candidate_id,
                self.state.current_term,
            )
            # Granting a vote resets the election timer (Section 5.2)
            self.reset_election_timer()
            self.emit_state_change()
            return RequestVoteReply(term=self.state.current_term, vote_granted=True)

        return RequestVoteReply(term=self.state.current_term, vote_granted=False)

    async def handle_append_entries(self, args: AppendEntriesArgs) -> AppendEntriesReply:
        """Receiver implementation for AppendEntries RPC (Section 5.2 / 5.3 / Figure 2)."""
        is_heartbeat = len(args.entries) == 0
        msg_type = "HEARTBEAT" if is_heartbeat else "APPEND_ENTRIES"
        self.emit_rpc_event(args.leader_id, self.node_id, msg_type, args.to_dict())

        # 1. Rule: If term > currentTerm, update term and become follower
        if args.term > self.state.current_term:
            self.become_follower(term=args.term)

        # 2. Rule: Reply false if term < currentTerm (Figure 2: Receiver Rule 1)
        if args.term < self.state.current_term:
            return AppendEntriesReply(
                term=self.state.current_term,
                success=False,
                match_index=self.state.last_log_index,
            )

        # If candidate sees valid leader for current term, step down to follower
        if args.term == self.state.current_term and self.role != NodeRole.FOLLOWER:
            self.become_follower(term=args.term)

        # Recognize the valid leader
        self.leader_id = args.leader_id

        # Valid heartbeat received -> Reset election timer (Section 5.2)
        self.reset_election_timer()

        # 3. Rule: Reply false if log doesn't contain an entry at prevLogIndex matching prevLogTerm (Rule 2)
        if args.prev_log_index > 0:
            if len(self.state.log) < args.prev_log_index:
                return AppendEntriesReply(
                    term=self.state.current_term,
                    success=False,
                    match_index=self.state.last_log_index,
                )
            if self.state.log[args.prev_log_index - 1].term != args.prev_log_term:
                return AppendEntriesReply(
                    term=self.state.current_term,
                    success=False,
                    match_index=self.state.last_log_index,
                )

        # 4. Rule: If existing entry conflicts with new entry (same index, different term),
        # delete existing entry and all that follow it (Rule 3)
        # 5. Rule: Append any new entries not already in the log (Rule 4)
        for entry in args.entries:
            if entry.index <= len(self.state.log):
                if self.state.log[entry.index - 1].term != entry.term:
                    self.state.log = self.state.log[: entry.index - 1]
                    if self.wal:
                        self.wal.truncate_log(entry.index)
                    self.state.log.append(entry)
                    if self.wal:
                        self.wal.append_entry(entry)
            else:
                self.state.log.append(entry)
                if self.wal:
                    self.wal.append_entry(entry)

        # 6. Rule: If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry) (Rule 5)
        if args.leader_commit > self.state.commit_index:
            self.state.commit_index = min(args.leader_commit, self.state.last_log_index)
            self.apply_entries_to_state_machine()

        self.emit_state_change()
        return AppendEntriesReply(
            term=self.state.current_term,
            success=True,
            match_index=self.state.last_log_index,
        )

    # -------------------------------------------------------------------------
    # Election Timers & Election Orchestration
    # -------------------------------------------------------------------------

    def reset_election_timer(self) -> None:
        """Signal that a valid heartbeat or vote was granted (resets election timeout countdown)."""
        self._heartbeat_received_event.set()

    async def _run_election_timer(self) -> None:
        """Wait for randomized election timeout; initiate election if no heartbeat arrives."""
        try:
            while self._is_running and self.role != NodeRole.LEADER:
                timeout = random.uniform(self.min_election_timeout, self.max_election_timeout)
                self._heartbeat_received_event.clear()

                try:
                    # Sleep until timeout expires OR a heartbeat / vote grant event occurs
                    await asyncio.wait_for(self._heartbeat_received_event.wait(), timeout=timeout)
                    # If event was set, heartbeat arrived -> loop restarts with a new timeout!
                    continue
                except asyncio.TimeoutError:
                    pass

                if self._is_running and self.role != NodeRole.LEADER:
                    logger.info(
                        "Node %s election timeout (%.3fs) expired. Starting election.",
                        self.node_id,
                        timeout,
                    )
                    await self.start_election()
        except asyncio.CancelledError:
            pass

    async def start_election(self) -> None:
        """Initiate leader election: request votes from all peers and tally quorum."""
        if not self._is_running or self.role == NodeRole.LEADER:
            return

        # Convert to candidate, increment term, vote for self
        self.become_candidate()

        election_term = self.state.current_term
        votes_granted = 1  # Voted for self
        total_nodes = len(self.peers) + 1
        quorum = (total_nodes // 2) + 1

        logger.info(
            "Node %s starting election for Term %d (Need %d/%d votes)",
            self.node_id,
            election_term,
            quorum,
            total_nodes,
        )

        # Single-node cluster edge case
        if votes_granted >= quorum:
            self.become_leader()
            self._start_heartbeat_loop()
            return

        # Prepare vote arguments (Section 5.2)
        vote_args = RequestVoteArgs(
            term=election_term,
            candidate_id=self.node_id,
            last_log_index=self.state.last_log_index,
            last_log_term=self.state.last_log_term,
        )

        rpc_timeout = min(0.15, max(0.04, self.max_election_timeout))

        async def request_vote_from_peer(peer_id: str) -> None:
            nonlocal votes_granted
            reply = await self.rpc.send_request_vote(peer_id, vote_args, timeout=rpc_timeout)

            if not self._is_running or self.role != NodeRole.CANDIDATE:
                return
            if self.state.current_term != election_term:
                return
            if reply is None:
                return

            # If peer has higher term, step down to follower immediately
            if reply.term > self.state.current_term:
                self.become_follower(term=reply.term)
                return

            if reply.term == election_term and reply.vote_granted:
                votes_granted += 1
                logger.info(
                    "Node %s received vote from %s (Total: %d/%d)",
                    self.node_id,
                    peer_id,
                    votes_granted,
                    total_nodes,
                )
                if votes_granted >= quorum and self.role == NodeRole.CANDIDATE:
                    self.become_leader()
                    self._start_heartbeat_loop()

        # Send RequestVote RPCs to all peers in parallel
        await asyncio.gather(
            *(request_vote_from_peer(peer) for peer in self.peers),
            return_exceptions=True,
        )

    # -------------------------------------------------------------------------
    # Leader Heartbeat & Log Replication (Section 5.3)
    # -------------------------------------------------------------------------

    def _start_heartbeat_loop(self) -> None:
        """Launch background heartbeat broadcast loop for active leader."""
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()

        self._heartbeat_task = asyncio.create_task(self._run_heartbeats())

    async def _run_heartbeats(self) -> None:
        """Periodically broadcast AppendEntries to all peers while Leader."""
        try:
            while self._is_running and self.role == NodeRole.LEADER:
                await self.broadcast_append_entries()
                await asyncio.sleep(self.heartbeat_interval)
        except asyncio.CancelledError:
            pass

    def check_advance_commit_index(self) -> bool:
        """Check if leader can advance commit_index by counting replicated quorums (Figure 2: Leader Rule 4)."""
        if self.role != NodeRole.LEADER:
            return False

        total_nodes = len(self.peers) + 1
        quorum = (total_nodes // 2) + 1
        advanced = False

        for n in range(self.state.last_log_index, self.state.commit_index, -1):
            # Section 5.4.2: Leader only commits entries from its current term by counting replicas
            if self.state.log[n - 1].term == self.state.current_term:
                match_count = 1  # Leader has the entry
                for peer in self.peers:
                    if self.state.match_index.get(peer, 0) >= n:
                        match_count += 1

                if match_count >= quorum:
                    self.state.commit_index = n
                    self.apply_entries_to_state_machine()
                    advanced = True
                    logger.info("Leader %s advanced commit_index to %d", self.node_id, n)
                    break

        return advanced

    async def broadcast_append_entries(self) -> None:
        """Broadcast AppendEntries RPC to all peers with pending entries (or empty heartbeat)."""
        if not self._is_running or self.role != NodeRole.LEADER:
            return

        current_term = self.state.current_term
        # 150ms timeout gives Windows TCP ample time to connect while failing fast on dead nodes
        rpc_timeout = 0.150

        async def replicate_to_peer(peer_id: str) -> None:
            next_idx = self.state.next_index.get(peer_id, self.state.last_log_index + 1)
            prev_idx = next_idx - 1
            prev_term = (
                self.state.log[prev_idx - 1].term
                if 0 < prev_idx <= len(self.state.log)
                else 0
            )

            entries_to_send = (
                self.state.log[prev_idx:]
                if prev_idx < len(self.state.log)
                else []
            )

            args = AppendEntriesArgs(
                term=current_term,
                leader_id=self.node_id,
                prev_log_index=prev_idx,
                prev_log_term=prev_term,
                entries=entries_to_send,
                leader_commit=self.state.commit_index,
            )

            reply = await self.rpc.send_append_entries(peer_id, args, timeout=rpc_timeout)
            if not self._is_running or self.role != NodeRole.LEADER or self.state.current_term != current_term:
                return
            if reply is None:
                return

            if reply.term > self.state.current_term:
                logger.info(
                    "Leader %s discovered higher term %d from %s. Stepping down.",
                    self.node_id,
                    reply.term,
                    peer_id,
                )
                self.become_follower(term=reply.term)
                return

            if reply.success:
                self.state.match_index[peer_id] = max(
                    self.state.match_index.get(peer_id, 0),
                    reply.match_index,
                )
                self.state.next_index[peer_id] = self.state.match_index[peer_id] + 1
                self.check_advance_commit_index()
            else:
                # Log inconsistency: decrement next_index and retry on next tick (Figure 2)
                self.state.next_index[peer_id] = max(1, self.state.next_index.get(peer_id, 1) - 1)

        await asyncio.gather(
            *(replicate_to_peer(peer) for peer in self.peers),
            return_exceptions=True,
        )

    async def execute_command(self, command: Any, timeout: float = 2.0) -> bool:
        """Submit a client command to the leader for replication and commitment.

        Returns:
            True if the command was successfully committed to the replicated state machine.
        """
        if not self._is_running or self.role != NodeRole.LEADER:
            logger.warning("Node %s is not LEADER; rejecting execute_command", self.node_id)
            return False

        # 1. Append command to leader log
        new_index = self.state.last_log_index + 1
        entry = LogEntry(index=new_index, term=self.state.current_term, command=command)
        self.state.log.append(entry)
        if self.wal:
            self.wal.append_entry(entry)
        self.emit_state_change()

        # Standalone cluster (0 peers)
        if not self.peers:
            self.state.commit_index = new_index
            self.apply_entries_to_state_machine()
            return True

        # 2. Replicate to followers and await commit quorum
        start_time = time.time()
        while time.time() - start_time < timeout:
            if not self._is_running or self.role != NodeRole.LEADER:
                return False

            await self.broadcast_append_entries()

            if self.state.commit_index >= new_index:
                # Immediate follow-up broadcast so followers learn of updated leader_commit without delay
                await self.broadcast_append_entries()
                return True

            await asyncio.sleep(0.02)

        return False

    async def confirm_leadership(self, timeout: float = 2.0) -> bool:
        """Confirm leadership by sending empty AppendEntries to a quorum and waiting for successes.
        This is a lightweight version of broadcast_append_entries designed to confirm we aren't a stale leader.
        """
        if self.role != NodeRole.LEADER:
            return False

        if not self.peers:
            return True

        current_term = self.state.current_term
        success_count = 1  # Self
        quorum = (len(self.peers) + 1) // 2 + 1

        async def ping_peer(peer_id: str) -> bool:
            args = AppendEntriesArgs(
                term=current_term,
                leader_id=self.node_id,
                prev_log_index=self.state.last_log_index,
                prev_log_term=self.state.log[-1].term if self.state.log else 0,
                entries=[],
                leader_commit=self.state.commit_index,
            )
            reply = await self.rpc.send_append_entries(peer_id, args, timeout=0.5)
            if not self._is_running or self.role != NodeRole.LEADER or self.state.current_term != current_term:
                return False
            if reply is None:
                return False
            if reply.term > current_term:
                self.become_follower(term=reply.term)
                return False
            return reply.success

        tasks = [ping_peer(p) for p in self.peers]
        try:
            for f in asyncio.as_completed(tasks, timeout=timeout):
                if await f:
                    success_count += 1
                if success_count >= quorum:
                    return True
        except asyncio.TimeoutError:
            pass
        return False

    async def linearizable_read(self, key: str, timeout: float = 2.0) -> Tuple[bool, Any]:
        """Perform a ReadIndex linearizable read without writing a new log entry.
        Returns (success, value).
        """
        if self.role != NodeRole.LEADER:
            return False, None

        # 1. Save current commit_index (ReadIndex)
        read_index = self.state.commit_index

        # 2. If the leader hasn't committed an entry from its current term yet, it cannot serve reads safely.
        has_committed_current_term = any(
            e.term == self.state.current_term for e in self.state.log[:read_index]
        )
        if not has_committed_current_term:
            # Submit a no-op to force commit
            await self.execute_command({"op": "NOOP"}, timeout=timeout)
            read_index = self.state.commit_index

        # 3. Confirm leadership with a quorum
        is_leader = await self.confirm_leadership(timeout=timeout)
        if not is_leader:
            return False, None

        # 4. Wait for local state machine to apply up to read_index
        start_time = time.time()
        while self.state.last_applied < read_index:
            if time.time() - start_time > timeout:
                return False, None
            await asyncio.sleep(0.01)

        # 5. Return value from local state machine
        return True, self.state.kv_store.get(key)

    def local_read(self, key: str) -> Any:
        """Perform a local, potentially stale read from the state machine."""
        return self.state.kv_store.get(key)

    # -------------------------------------------------------------------------
    # Server Lifecycle Management
    # -------------------------------------------------------------------------

    async def start(self) -> None:
        """Start the Raft node, open TCP server, and start election timer."""
        if self._is_running:
            return

        await self.rpc.start()
        self._is_running = True
        self._election_task = asyncio.create_task(self._run_election_timer())
        logger.info("RaftNode %s started on port %d", self.node_id, self.rpc.port)

    async def stop(self) -> None:
        """Stop the Raft node, cancel background tasks, and close TCP server."""
        if not self._is_running:
            return

        self._is_running = False

        if self._election_task and not self._election_task.done():
            self._election_task.cancel()
            self._election_task = None

        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            self._heartbeat_task = None

        await self.rpc.stop()
        if self.wal:
            self.wal.close()
        logger.info("RaftNode %s stopped", self.node_id)
