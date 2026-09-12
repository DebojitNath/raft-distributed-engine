import asyncio
import time
import pytest

from raft.state import NodeRole
from tests.harness import RaftCluster

@pytest.mark.asyncio
async def test_read_index_linearizability():
    """Test that ReadIndex prevents stale reads when a leader is partitioned."""
    cluster = RaftCluster(num_nodes=5)
    leader1 = await cluster.start()
    assert leader1 is not None

    # 1. Write an initial value and wait for commit
    success = await leader1.execute_command({"op": "SET", "key": "color", "val": "red"})
    assert success is True
    await asyncio.sleep(0.5)

    # Both reads should return "red" initially
    success, val = await leader1.linearizable_read("color")
    assert success is True
    assert val == "red"
    assert leader1.local_read("color") == "red"

    # 2. Create a network partition:
    # Minority: Leader + 1 Follower
    # Majority: 3 Followers (they will elect a new leader)
    followers = [n for n in cluster.nodes.values() if n.node_id != leader1.node_id]
    minority_follower = followers[0]
    majority_followers = followers[1:]
    
    minority_group = [leader1.node_id, minority_follower.node_id]
    majority_group = [f.node_id for f in majority_followers]
    
    cluster.partition(minority_group, majority_group)
    
    # 3. Wait for the majority to elect a new leader
    start = time.time()
    leader2 = None
    while time.time() - start < 3.0:
        leaders = [n for n in cluster.nodes.values() if n.role == NodeRole.LEADER and n.node_id in majority_group]
        if leaders:
            leader2 = leaders[0]
            break
        await asyncio.sleep(0.1)
        
    assert leader2 is not None
    assert leader2.node_id != leader1.node_id

    # 4. Write a new value to the NEW leader
    success = await leader2.execute_command({"op": "SET", "key": "color", "val": "blue"})
    assert success is True
    await asyncio.sleep(0.5)

    # 5. The OLD leader is isolated. It doesn't know about the new term or value.
    # A local read is stale:
    stale_val = leader1.local_read("color")
    assert stale_val == "red", f"Old leader local read should be stale, got {stale_val}"

    # A linearizable read MUST fail (or timeout) because it cannot get a quorum
    success, lin_val = await leader1.linearizable_read("color", timeout=1.0)
    assert success is False, "Linearizable read should fail on isolated leader"
    
    # 6. Heal the partition
    cluster.heal()
    
    # Wait for the old leader to step down and sync up
    await asyncio.sleep(1.0)
    
    # Now it should have the updated value
    assert leader1.role == NodeRole.FOLLOWER
    assert leader1.local_read("color") == "blue"
    
    await cluster.stop()
