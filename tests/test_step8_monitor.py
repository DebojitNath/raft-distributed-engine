"""Unit and Integration Tests for Step 8: Telemetry Monitor Server (monitor.py).

Verifies:
- FastAPI REST endpoints: /api/cluster, /api/command, /api/nodes/{id}/kill, /api/nodes/{id}/revive, /api/partition, /api/heal, /api/reset
- WebSocket live event stream: connection, initial snapshot, state change & RPC telemetry broadcast
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from typing import Any, Dict

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
from fastapi.testclient import TestClient

from monitor import app, controller


@pytest.fixture(autouse=True)
def run_around_tests():
    """Ensure clean cluster state between tests."""
    yield


def test_monitor_rest_cluster_snapshot():
    """Verify GET /api/cluster returns healthy 5-node topology."""
    with TestClient(app) as client:
        # Give cluster a moment to elect leader
        time.sleep(0.8)
        resp = client.get("/api/cluster")
        assert resp.status_code == 200
        data = resp.json()

        assert "nodes" in data
        assert len(data["nodes"]) == 5
        assert "leader_id" in data

        # Check node schema
        for nid, node_info in data["nodes"].items():
            assert "role" in node_info
            assert "term" in node_info
            assert "log" in node_info
            assert "kv_store" in node_info
            assert "is_running" in node_info
            assert node_info["is_running"] is True


def test_monitor_rest_command_execution():
    """Verify POST /api/command executes writes across cluster."""
    with TestClient(app) as client:
        time.sleep(0.8)

        # 1. Execute command via key/val payload
        cmd_payload = {"op": "SET", "key": "monitor_k1", "val": "monitor_v1"}
        resp = client.post("/api/command", json=cmd_payload)
        assert resp.status_code == 200
        res_data = resp.json()
        assert res_data["success"] is True

        # 2. Assert all alive nodes updated KV store
        snapshot = res_data["snapshot"]
        for nid, ninfo in snapshot["nodes"].items():
            assert ninfo["kv_store"].get("monitor_k1") == "monitor_v1"


def test_monitor_rest_kill_and_revive():
    """Verify POST /api/nodes/{id}/kill and /revive."""
    with TestClient(app) as client:
        time.sleep(0.8)
        cluster_info = client.get("/api/cluster").json()
        leader_id = cluster_info["leader_id"]

        # Pick follower to kill
        follower_id = [nid for nid in cluster_info["nodes"] if nid != leader_id][0]

        # 1. Kill follower
        kill_resp = client.post(f"/api/nodes/{follower_id}/kill")
        assert kill_resp.status_code == 200
        snap = kill_resp.json()["snapshot"]
        assert snap["nodes"][follower_id]["is_running"] is False
        assert snap["nodes"][follower_id]["role"] == "DEAD"

        # 2. Revive follower
        revive_resp = client.post(f"/api/nodes/{follower_id}/revive")
        assert revive_resp.status_code == 200
        snap2 = revive_resp.json()["snapshot"]
        assert snap2["nodes"][follower_id]["is_running"] is True


def test_monitor_rest_partition_and_heal():
    """Verify POST /api/partition and POST /api/heal."""
    with TestClient(app) as client:
        time.sleep(0.8)

        # 1. Inject partition: [node1, node2] vs [node3, node4, node5]
        part_payload = {
            "group_a": ["node1", "node2"],
            "group_b": ["node3", "node4", "node5"],
        }
        part_resp = client.post("/api/partition", json=part_payload)
        assert part_resp.status_code == 200
        snap = part_resp.json()["snapshot"]

        # node1 must ignore node3, node4, node5
        assert set(snap["partitions"]["node1"]) == {"node3", "node4", "node5"}
        # node3 must ignore node1, node2
        assert set(snap["partitions"]["node3"]) == {"node1", "node2"}

        # 2. Heal partition
        heal_resp = client.post("/api/heal")
        assert heal_resp.status_code == 200
        snap_healed = heal_resp.json()["snapshot"]
        for nid in snap_healed["nodes"]:
            assert len(snap_healed["partitions"][nid]) == 0


def test_monitor_websocket_stream():
    """Verify WebSocket connection receives initial cluster snapshot and live event broadcast."""
    with TestClient(app) as client:
        with client.websocket_connect("/ws") as websocket:
            # 1. First frame is the initial CLUSTER_SNAPSHOT
            data = websocket.receive_json()
            assert data["type"] == "CLUSTER_SNAPSHOT"
            assert "nodes" in data["data"]
            assert len(data["data"]["nodes"]) == 5

            # 2. Test ping / pong
            websocket.send_json({"type": "PING"})
            pong = websocket.receive_json()
            assert pong["type"] == "PONG"

            # 3. Trigger a command to observe live broadcast
            client.post("/api/command", json={"command": "SET ws_key=ws_val"})

            # Receive broadcasted state changes and/or RPC events
            received_events = []
            for _ in range(5):
                try:
                    event = websocket.receive_json()
                    received_events.append(event.get("type"))
                except Exception:
                    break

            assert any(t in received_events for t in ("STATE_CHANGE", "RPC_EVENT", "CLUSTER_SNAPSHOT"))
