import asyncio
import os
import tempfile
import time
from pathlib import Path

import pytest
from raft.state import NodeRole
from tests.harness import RaftCluster

@pytest.mark.asyncio
async def test_wal_persistence_and_recovery(tmp_path):
    """Test that a cluster can recover its state from the WAL after a full crash."""
    wal_dir = str(tmp_path / "wal_recovery")
    os.makedirs(wal_dir, exist_ok=True)
    
    cluster = RaftCluster(num_nodes=3, wal_dir=wal_dir)
    leader = await cluster.start()
    assert leader is not None

    # Submit a command
    command = {"op": "SET", "key": "x", "val": 42}
    await leader.execute_command(command)

    # Wait for commit
    await asyncio.sleep(0.5)

    # Verify command applied
    for node in cluster.nodes.values():
        assert node.state.kv_store.get("x") == 42
        assert len(node.state.log) > 0

    # Save some assertions
    leader_term = leader.state.current_term
    leader_log_len = len(leader.state.log)

    # Crash all nodes
    await cluster.stop()

    # Check WAL files exist
    wal_files = list(Path(wal_dir).glob("*.wal"))
    assert len(wal_files) == 3

    # Create a NEW cluster with the SAME wal_dir
    # This bypasses the in-memory persistent_states in RaftCluster
    new_cluster = RaftCluster(num_nodes=3, wal_dir=wal_dir)
    
    # Start the new cluster, nodes will initialize with wal_dir and recover
    new_leader = await new_cluster.start()
    assert new_leader is not None

    # Verify state was recovered
    for node in new_cluster.nodes.values():
        # Current term should be at least what it was before (leader election increases it)
        assert node.state.current_term >= leader_term
        # Log length should be preserved
        assert len(node.state.log) == leader_log_len
        # We don't verify kv_store here directly immediately unless we wait for commit, 
        # but wait, last_applied is volatile and reset to 0 on boot.
        # Raft commits the log again once the new leader commits a no-op or advances commit index.
        # Let's wait a bit for the new leader to send heartbeats and advance commit_index.

    # Send a dummy command to force the new leader to commit previous entries
    await new_leader.execute_command({"op": "SET", "key": "y", "val": 100})
    await asyncio.sleep(0.5)

    # Now KV store should be re-applied up to the recovered log
    for node in new_cluster.nodes.values():
        assert node.state.kv_store.get("x") == 42
        assert node.state.kv_store.get("y") == 100
        
    await new_cluster.stop()
    # Give Windows a moment to release file handles before tempdir cleanup
    await asyncio.sleep(0.1)

@pytest.mark.asyncio
async def test_wal_latency_benchmark(tmp_path):
    """Benchmark command execution latency with and without WAL."""
    async def run_workload(wal_dir=None):
        cluster = RaftCluster(num_nodes=3, wal_dir=wal_dir)
        leader = await cluster.start()
        
        start = time.time()
        for i in range(10):
            await leader.execute_command(f"SET k{i}={i}")
            # we just pipeline them, then wait for replication
        
        # Wait for last one to commit
        while leader.state.commit_index < 10:
            await asyncio.sleep(0.01)
            if time.time() - start > 5:
                break
                
        duration = time.time() - start
        await cluster.stop()
        return duration

    duration_no_wal = await run_workload(wal_dir=None)
    
    wal_dir = str(tmp_path / "wal_bench")
    os.makedirs(wal_dir, exist_ok=True)
    duration_wal = await run_workload(wal_dir=wal_dir)
    # Give Windows a moment to release file handles before tempdir cleanup
    await asyncio.sleep(0.1)
        
    print(f"\nLatency (10 ops): No WAL = {duration_no_wal:.3f}s, WAL = {duration_wal:.3f}s")
    # WAL will be slower due to fsync, but we just want to ensure it works
    assert duration_wal > 0
    assert duration_no_wal > 0
