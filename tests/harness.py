"""Chaos Engineering Test Harness for Raft Consensus Cluster.

Reference: ROADMAP.md Step 6 & ARCHITECTURE.md Section 4 (God Mode Backdoor).
Provides the RaftCluster orchestrator to spin up multi-node local clusters,
simulate network partitions, inject crashes, reboot nodes, and assert correctness bounds.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
from typing import Any, Dict, Iterable, List, Optional

# Add project root to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from raft.node import RaftNode
from raft.state import NodeRole, RaftState

logger = logging.getLogger(__name__)


class RaftCluster:
    """Orchestrates an in-memory or localhost cluster of Raft nodes for chaos testing."""

    def __init__(
        self,
        num_nodes: int = 3,
        min_election_timeout: float = 0.300,
        max_election_timeout: float = 0.600,
        heartbeat_interval: float = 0.030,
    ) -> None:
        self.num_nodes = num_nodes
        self.min_election_timeout = min_election_timeout
        self.max_election_timeout = max_election_timeout
        self.heartbeat_interval = heartbeat_interval

        self.node_ids = [f"node{i+1}" for i in range(num_nodes)]
        self.nodes: Dict[str, RaftNode] = {}
        # Track persistent state across crashes and revivals
        self.persistent_states: Dict[str, RaftState] = {}
        self._is_running = False

    async def start(self, wait_for_leader: bool = True, timeout: float = 3.0) -> Optional[RaftNode]:
        """Initialize all nodes, start TCP servers, cross-link addresses, and start election timers."""
        if self._is_running:
            return await self.get_leader()

        self._is_running = True

        # 1. Instantiate RaftNode instances
        for nid in self.node_ids:
            peers = [p for p in self.node_ids if p != nid]
            state = self.persistent_states.get(nid, RaftState())
            self.persistent_states[nid] = state

            node = RaftNode(
                node_id=nid,
                peers=peers,
                state=state,
                min_election_timeout=self.min_election_timeout,
                max_election_timeout=self.max_election_timeout,
                heartbeat_interval=self.heartbeat_interval,
            )
            self.nodes[nid] = node

        # 2. Start all RPC servers
        for node in self.nodes.values():
            await node.rpc.start()

        # 3. Cross-link peer addresses
        for node in self.nodes.values():
            for peer_id, peer_node in self.nodes.items():
                if peer_id != node.node_id:
                    node.rpc.peer_addresses[peer_id] = ("127.0.0.1", peer_node.rpc.port)

        # 4. Start Raft state machines & election timers
        for node in self.nodes.values():
            node._is_running = True
            node._election_task = asyncio.create_task(node._run_election_timer())

        if wait_for_leader:
            return await self.get_leader(timeout=timeout)
        return None

    async def stop(self) -> None:
        """Stop all running nodes and release all TCP sockets."""
        self._is_running = False
        for node in list(self.nodes.values()):
            await node.stop()
        self.nodes.clear()

    async def get_leader(self, timeout: float = 2.0) -> Optional[RaftNode]:
        """Poll cluster until a stable Leader emerges or timeout expires."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            alive_nodes = [n for n in self.nodes.values() if n._is_running]
            leaders = [n for n in alive_nodes if n.role == NodeRole.LEADER]
            if len(leaders) == 1:
                return leaders[0]
            await asyncio.sleep(0.05)
        return None

    # -------------------------------------------------------------------------
    # Chaos Operations (Kill, Revive, Partition, Heal)
    # -------------------------------------------------------------------------

    async def kill_node(self, node_id: str) -> None:
        """Forcibly crash an active node (simulates process crash / power loss)."""
        node = self.nodes.get(node_id)
        if node and node._is_running:
            logger.info("Chaos: Killing node %s", node_id)
            # Save persistent state
            self.persistent_states[node_id] = node.state
            await node.stop()

    async def revive_node(self, node_id: str) -> RaftNode:
        """Revive a crashed node, loading its persistent state from storage."""
        logger.info("Chaos: Reviving node %s", node_id)
        peers = [p for p in self.node_ids if p != node_id]
        saved_state = self.persistent_states.get(node_id, RaftState())

        # Create new node instance with preserved state
        revived = RaftNode(
            node_id=node_id,
            peers=peers,
            state=saved_state,
            min_election_timeout=self.min_election_timeout,
            max_election_timeout=self.max_election_timeout,
            heartbeat_interval=self.heartbeat_interval,
        )
        self.nodes[node_id] = revived

        # Start RPC server
        await revived.rpc.start()

        # Update peer addresses across the cluster
        for peer_id, peer_node in self.nodes.items():
            if peer_id != node_id and peer_node._is_running:
                revived.rpc.peer_addresses[peer_id] = ("127.0.0.1", peer_node.rpc.port)
                peer_node.rpc.peer_addresses[node_id] = ("127.0.0.1", revived.rpc.port)

        # Start revived state machine
        revived._is_running = True
        revived._election_task = asyncio.create_task(revived._run_election_timer())
        return revived

    def partition(self, group_a: Iterable[str], group_b: Iterable[str]) -> None:
        """Sever network connection between group_a nodes and group_b nodes (cut cable)."""
        list_a = list(group_a)
        list_b = list(group_b)
        logger.info("Chaos: Partitioning %s <---> %s", list_a, list_b)

        for a_id in list_a:
            node_a = self.nodes.get(a_id)
            if node_a:
                for b_id in list_b:
                    node_a.rpc.add_ignored(b_id)

        for b_id in list_b:
            node_b = self.nodes.get(b_id)
            if node_b:
                for a_id in list_a:
                    node_b.rpc.add_ignored(a_id)

    def heal(self) -> None:
        """Restore all network links across the entire cluster."""
        logger.info("Chaos: Healing all partitions")
        for node in self.nodes.values():
            node.rpc.clear_ignored()

    async def add_node(self) -> RaftNode:
        """Dynamically add a new node to the running cluster."""
        # Find the next available node ID
        existing_nums = [int(nid.replace("node", "")) for nid in self.node_ids]
        next_num = max(existing_nums) + 1 if existing_nums else 1
        new_id = f"node{next_num}"
        
        logger.info("Chaos: Adding new node %s to cluster", new_id)
        self.node_ids.append(new_id)
        
        peers = [p for p in self.node_ids if p != new_id]
        state = RaftState()
        self.persistent_states[new_id] = state
        
        new_node = RaftNode(
            node_id=new_id,
            peers=peers,
            state=state,
            min_election_timeout=self.min_election_timeout,
            max_election_timeout=self.max_election_timeout,
            heartbeat_interval=self.heartbeat_interval,
        )
        self.nodes[new_id] = new_node
        
        # Start RPC server
        await new_node.rpc.start()
        
        # Cross-link and update peers of EXISTING nodes
        for peer_id, peer_node in self.nodes.items():
            if peer_id != new_id:
                # Add new node to peer's peer list so they vote for and replicate to it
                if new_id not in peer_node.peers:
                    peer_node.peers.append(new_id)
                # Cross-link networking
                new_node.rpc.peer_addresses[peer_id] = ("127.0.0.1", peer_node.rpc.port)
                peer_node.rpc.peer_addresses[new_id] = ("127.0.0.1", new_node.rpc.port)
                
        # Start Raft state machine
        new_node._is_running = True
        new_node._election_task = asyncio.create_task(new_node._run_election_timer())
        
        return new_node

    # -------------------------------------------------------------------------
    # High-Level Verification & Command Execution
    # -------------------------------------------------------------------------

    async def execute_command(self, command: Any, timeout: float = 2.0) -> bool:
        """Submit command to the current cluster leader."""
        leader = await self.get_leader(timeout=timeout)
        if not leader:
            return False
        return await leader.execute_command(command, timeout=timeout)

    async def assert_log_parity(
        self,
        nodes_to_check: Optional[List[str]] = None,
        timeout: float = 2.0,
    ) -> None:
        """Assert that all specified (or all alive) nodes reach identical logs and KV stores."""
        target_ids = nodes_to_check if nodes_to_check is not None else [
            nid for nid, n in self.nodes.items() if n._is_running
        ]

        start_time = time.time()
        while time.time() - start_time < timeout:
            active_nodes = [self.nodes[nid] for nid in target_ids if nid in self.nodes and self.nodes[nid]._is_running]
            if len(active_nodes) == len(target_ids):
                # Check if commit_index matches
                first = active_nodes[0]
                all_match = all(
                    n.state.commit_index == first.state.commit_index
                    and n.state.kv_store == first.state.kv_store
                    and len(n.state.log) == len(first.state.log)
                    for n in active_nodes
                )
                if all_match:
                    return
            await asyncio.sleep(0.05)

        # Final assertion if timeout reached
        active_nodes = [self.nodes[nid] for nid in target_ids if nid in self.nodes and self.nodes[nid]._is_running]
        first = active_nodes[0]
        for n in active_nodes[1:]:
            assert n.state.commit_index == first.state.commit_index, (
                f"Node {n.node_id} commit_index {n.state.commit_index} != {first.node_id} {first.state.commit_index}"
            )
            assert n.state.kv_store == first.state.kv_store, (
                f"Node {n.node_id} kv_store {n.state.kv_store} != {first.node_id} {first.state.kv_store}"
            )
            assert len(n.state.log) == len(first.state.log), (
                f"Node {n.node_id} log len {len(n.state.log)} != {first.node_id} {len(first.state.log)}"
            )
