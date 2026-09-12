"""FastAPI Telemetry Monitor & Chaos Engineering Controller (Step 8).

Reference: ARCHITECTURE.md Section 3 & 4.
Provides a real-time WebSocket stream of cluster events (state changes, RPC packets)
and REST endpoints for the God Mode Chaos Backdoor.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, Set

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from raft.node import RaftNode
from raft.state import NodeRole
from tests.harness import RaftCluster

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# WebSocket Connection Manager
# -----------------------------------------------------------------------------

class WebSocketHub:
    """Manages active WebSocket connections and broadcasts cluster events."""

    def __init__(self) -> None:
        self.active_connections: Set[WebSocket] = set()

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self.active_connections.add(websocket)
        logger.info("WebSocket client connected. Total clients: %d", len(self.active_connections))

    def disconnect(self, websocket: WebSocket) -> None:
        self.active_connections.discard(websocket)
        logger.info("WebSocket client disconnected. Total clients: %d", len(self.active_connections))

    async def broadcast(self, message: Dict[str, Any]) -> None:
        if not self.active_connections:
            return

        payload = json.dumps(message)
        dead_connections: List[WebSocket] = []

        for connection in list(self.active_connections):
            try:
                await connection.send_text(payload)
            except Exception:
                dead_connections.append(connection)

        for dead in dead_connections:
            self.disconnect(dead)


# -----------------------------------------------------------------------------
# Global Cluster Controller
# -----------------------------------------------------------------------------

class ClusterMonitorController:
    """Orchestrates the active RaftCluster, attaches telemetry hooks, and routes REST commands."""

    def __init__(self, num_nodes: int = 5) -> None:
        self.num_nodes = num_nodes
        self.cluster: Optional[RaftCluster] = None
        self.ws_hub = WebSocketHub()
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def _setup_node_hooks(self, node: RaftNode) -> None:
        """Attach observer hooks to a node for state transitions and RPC packets."""

        def on_state(event: Dict[str, Any]) -> None:
            msg = {
                "type": "STATE_CHANGE",
                "event": "state_change",
                "node_id": node.node_id,
                "data": event,
            }
            if self._loop and self._loop.is_running():
                asyncio.run_coroutine_threadsafe(self.ws_hub.broadcast(msg), self._loop)

        def on_rpc(event: Dict[str, Any]) -> None:
            msg = {
                "type": "RPC_EVENT",
                "event": "rpc_event",
                "data": event,
            }
            if self._loop and self._loop.is_running():
                asyncio.run_coroutine_threadsafe(self.ws_hub.broadcast(msg), self._loop)

        node.on_state_change_callbacks.append(on_state)
        node.on_rpc_event_callbacks.append(on_rpc)

    async def start_cluster(self) -> None:
        """Boot the Raft cluster and register telemetry hooks on all nodes."""
        self._loop = asyncio.get_running_loop()
        wal_dir = os.path.join(os.path.dirname(__file__), "server_wal")
        os.makedirs(wal_dir, exist_ok=True)
        self.cluster = RaftCluster(
            num_nodes=self.num_nodes,
            min_election_timeout=0.300,
            max_election_timeout=0.600,
            heartbeat_interval=0.040,
            wal_dir=wal_dir,
        )
        await self.cluster.start(wait_for_leader=False)

        for node in self.cluster.nodes.values():
            self._setup_node_hooks(node)

        logger.info("Cluster initialized with %d nodes", self.num_nodes)

    async def stop_cluster(self) -> None:
        """Stop all running nodes in the cluster."""
        if self.cluster:
            await self.cluster.stop()
            self.cluster = None
            logger.info("Cluster stopped")

    def get_snapshot(self) -> Dict[str, Any]:
        """Return full cluster snapshot including all node states, logs, KV stores, and partitions."""
        if not self.cluster:
            return {"nodes": {}, "leader_id": None, "partitions": {}}

        nodes_data: Dict[str, Any] = {}
        partitions: Dict[str, List[str]] = {}
        leader_id: Optional[str] = None

        for nid in self.cluster.node_ids:
            node = self.cluster.nodes.get(nid)
            if node and node._is_running:
                if node.role == NodeRole.LEADER:
                    leader_id = nid
                nodes_data[nid] = {
                    "node_id": nid,
                    "is_running": True,
                    "role": node.role.value,
                    "leader_id": node.leader_id,
                    "term": node.state.current_term,
                    "commit_index": node.state.commit_index,
                    "last_applied": node.state.last_applied,
                    "log": [entry.to_dict() for entry in node.state.log],
                    "kv_store": dict(node.state.kv_store),
                    "port": node.rpc.port,
                }
                partitions[nid] = list(node.rpc.ignore_list)
            else:
                saved_state = self.cluster.persistent_states.get(nid)
                nodes_data[nid] = {
                    "node_id": nid,
                    "is_running": False,
                    "role": "DEAD",
                    "leader_id": None,
                    "term": saved_state.current_term if saved_state else 0,
                    "commit_index": saved_state.commit_index if saved_state else 0,
                    "last_applied": saved_state.last_applied if saved_state else 0,
                    "log": [entry.to_dict() for entry in saved_state.log] if saved_state else [],
                    "kv_store": dict(saved_state.kv_store) if saved_state else {},
                    "port": 0,
                }
                partitions[nid] = []

        return {
            "nodes": nodes_data,
            "leader_id": leader_id,
            "partitions": partitions,
        }

    async def kill_node(self, node_id: str) -> None:
        if not self.cluster or node_id not in self.cluster.node_ids:
            raise HTTPException(status_code=404, detail=f"Node {node_id} not found")
        await self.cluster.kill_node(node_id)
        # Broadcast state change for dead node using the full updated snapshot data
        snapshot = self.get_snapshot()
        node_data = snapshot["nodes"].get(node_id)
        if node_data:
            dead_event = {
                "type": "STATE_CHANGE",
                "event": "state_change",
                "node_id": node_id,
                "data": node_data,
            }
            await self.ws_hub.broadcast(dead_event)

    async def revive_node(self, node_id: str) -> None:
        if not self.cluster or node_id not in self.cluster.node_ids:
            raise HTTPException(status_code=404, detail=f"Node {node_id} not found")
        revived = await self.cluster.revive_node(node_id)
        self._setup_node_hooks(revived)
        revived.emit_state_change()

    def partition(self, group_a: List[str], group_b: List[str]) -> None:
        if not self.cluster:
            raise HTTPException(status_code=500, detail="Cluster not initialized")
        self.cluster.partition(group_a, group_b)

    def heal(self) -> None:
        if not self.cluster:
            raise HTTPException(status_code=500, detail="Cluster not initialized")
        self.cluster.heal()

    async def execute_command(self, command: Any, timeout: float = 3.0) -> bool:
        if not self.cluster:
            raise HTTPException(status_code=500, detail="Cluster not initialized")
        return await self.cluster.execute_command(command, timeout=timeout)


controller = ClusterMonitorController(num_nodes=5)


# -----------------------------------------------------------------------------
# FastAPI Lifespan & Application Definition
# -----------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Start Raft cluster
    await controller.start_cluster()
    yield
    # Shutdown: Stop Raft cluster
    await controller.stop_cluster()


app = FastAPI(
    title="Raft Consensus Visualizer & Chaos Monitor",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS middleware for local frontend development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# -----------------------------------------------------------------------------
# Request Schemas
# -----------------------------------------------------------------------------

class CommandRequest(BaseModel):
    command: Optional[str] = None
    op: Optional[str] = None
    key: Optional[str] = None
    val: Optional[Any] = None


class PartitionRequest(BaseModel):
    group_a: List[str]
    group_b: List[str]


# -----------------------------------------------------------------------------
# REST API Endpoints (God Mode)
# -----------------------------------------------------------------------------

@app.get("/api/cluster")
async def get_cluster_state():
    """Return the current topology, roles, terms, logs, and KV store of all nodes."""
    return controller.get_snapshot()


@app.post("/api/command")
async def submit_command(req: CommandRequest):
    """Execute a state machine write command (e.g. SET key=val) through the current Leader."""
    if req.command:
        cmd: Any = req.command
    elif req.key is not None:
        cmd = {"op": req.op or "SET", "key": req.key, "val": req.val}
    else:
        raise HTTPException(status_code=400, detail="Missing command payload")

    success = await controller.execute_command(cmd, timeout=3.0)
    return {
        "success": success,
        "command": cmd,
        "snapshot": controller.get_snapshot(),
    }


@app.post("/api/nodes/{node_id}/kill")
async def kill_node(node_id: str):
    """Simulate a hard node crash / process termination."""
    await controller.kill_node(node_id)
    return {"status": "ok", "killed": node_id, "snapshot": controller.get_snapshot()}


@app.post("/api/nodes/{node_id}/revive")
async def revive_node(node_id: str):
    """Reboot a crashed node, loading its persisted state."""
    await controller.revive_node(node_id)
    return {"status": "ok", "revived": node_id, "snapshot": controller.get_snapshot()}

@app.post("/api/nodes/add")
async def add_node():
    """Dynamically spin up 2 new nodes and add them to the cluster."""
    if not controller.cluster:
        raise HTTPException(status_code=500, detail="Cluster not initialized")
    
    new_node1 = await controller.cluster.add_node()
    controller._setup_node_hooks(new_node1)
    
    new_node2 = await controller.cluster.add_node()
    controller._setup_node_hooks(new_node2)
    
    # Broadcast full snapshot so UI redraws
    snapshot = controller.get_snapshot()
    await controller.ws_hub.broadcast({
        "type": "CLUSTER_SNAPSHOT",
        "event": "cluster_snapshot",
        "data": snapshot,
    })
    return {"status": "ok", "added": [new_node1.node_id, new_node2.node_id], "snapshot": snapshot}


@app.post("/api/partition")
async def inject_partition(req: PartitionRequest):
    """Sever network communication between group_a and group_b."""
    controller.partition(req.group_a, req.group_b)
    part_dict = req.model_dump() if hasattr(req, "model_dump") else req.dict()
    # Broadcast snapshot update to all clients
    await controller.ws_hub.broadcast({
        "type": "PARTITION_CHANGE",
        "event": "partition_change",
        "data": controller.get_snapshot(),
    })
    return {"status": "ok", "partition": part_dict, "snapshot": controller.get_snapshot()}


@app.post("/api/heal")
async def heal_partitions():
    """Restore all severed network links across the cluster."""
    controller.heal()
    await controller.ws_hub.broadcast({
        "type": "PARTITION_CHANGE",
        "event": "partition_change",
        "data": controller.get_snapshot(),
    })
    return {"status": "ok", "healed": True, "snapshot": controller.get_snapshot()}


@app.post("/api/reset")
async def reset_cluster():
    """Restart a fresh 5-node cluster."""
    await controller.stop_cluster()
    await controller.start_cluster()
    snapshot = controller.get_snapshot()
    await controller.ws_hub.broadcast({
        "type": "CLUSTER_SNAPSHOT",
        "event": "cluster_snapshot",
        "data": snapshot,
    })
    return {"status": "ok", "reset": True, "snapshot": snapshot}


# -----------------------------------------------------------------------------
# WebSocket Endpoint (Live Event Stream)
# -----------------------------------------------------------------------------

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """Stream live state changes and in-flight RPC events to connected visualizer clients."""
    await controller.ws_hub.connect(websocket)
    try:
        # Transmit initial snapshot immediately upon connection
        initial_snapshot = {
            "type": "CLUSTER_SNAPSHOT",
            "event": "cluster_snapshot",
            "data": controller.get_snapshot(),
        }
        await websocket.send_text(json.dumps(initial_snapshot))

        while True:
            # Keep connection open and accept optional client pings / commands
            data = await websocket.receive_text()
            try:
                msg = json.loads(data)
                if msg.get("type") == "PING":
                    await websocket.send_text(json.dumps({"type": "PONG"}))
            except Exception:
                pass
    except WebSocketDisconnect:
        controller.ws_hub.disconnect(websocket)
    except Exception as e:
        logger.warning("WebSocket error: %s", e)
        controller.ws_hub.disconnect(websocket)


# -----------------------------------------------------------------------------
# Static Visualizer Mounting (Frontend)
# -----------------------------------------------------------------------------

frontend_dir = os.path.join(os.path.dirname(__file__), "frontend")
if os.path.isdir(frontend_dir):
    app.mount("/", StaticFiles(directory=frontend_dir, html=True), name="frontend")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("monitor:app", host="0.0.0.0", port=8000, reload=False)
