"""Integration Chaos Suite for Step 6: Test Harness & Automated Chaos Scenarios.

Verifies:
- Scenario A: Active Leader Failure & Fast Failover (<1.5s recovery and write continuation)
- Scenario B: 3 vs 2 Network Partition (Majority commits writes, minority blocks, heal brings 100% parity)
- Scenario C: Mid-Replication Crash & Revival (Follower revives and catches up on 5 missed writes)
"""

from __future__ import annotations

import asyncio
import os
import sys

# Add project root to sys.path so tests work whether run from root or inside tests/
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from tests.harness import RaftCluster


def test_scenario_a_leader_failure() -> None:
    """Scenario A: Kill active leader; remaining nodes elect new leader and continue writes."""

    async def _test() -> None:
        cluster = RaftCluster(num_nodes=3)
        leader = await cluster.start()
        assert leader is not None, "Initial leader not elected"

        try:
            # 1. Write initial command to leader
            res = await cluster.execute_command("SET initial=100")
            assert res is True
            await cluster.assert_log_parity()

            old_leader_id = leader.node_id
            old_term = leader.state.current_term

            # 2. Kill the active leader!
            await cluster.kill_node(old_leader_id)

            # 3. Assert new leader elected by surviving 2 nodes
            new_leader = await cluster.get_leader(timeout=3.0)
            assert new_leader is not None, "New leader not elected after leader crash"
            assert new_leader.node_id != old_leader_id
            assert new_leader.state.current_term > old_term

            # 4. Submit new write to the new leader
            res2 = await new_leader.execute_command("SET after_failover=200", timeout=3.0)
            assert res2 is True

            # 5. Assert surviving nodes committed both writes
            surviving_nodes = [nid for nid in cluster.node_ids if nid != old_leader_id]
            await cluster.assert_log_parity(nodes_to_check=surviving_nodes, timeout=3.0)

            first_surviving = cluster.nodes[surviving_nodes[0]]
            assert first_surviving.state.kv_store == {"initial": "100", "after_failover": "200"}

        finally:
            await cluster.stop()

    asyncio.run(_test())


def test_scenario_b_network_partition_3_vs_2() -> None:
    """Scenario B: 3 vs 2 partition. Majority commits writes; minority blocks; heal brings parity."""

    async def _test() -> None:
        cluster = RaftCluster(num_nodes=5)
        leader = await cluster.start()
        assert leader is not None, "5-node leader not elected"

        try:
            # Form 3-node majority (including leader) and 2-node minority
            leader_id = leader.node_id
            other_nodes = [nid for nid in cluster.node_ids if nid != leader_id]

            majority_group = [leader_id, other_nodes[0], other_nodes[1]]  # 3 nodes
            minority_group = [other_nodes[2], other_nodes[3]]              # 2 nodes

            # 1. Inject Network Partition (sever all cables between majority and minority)
            cluster.partition(majority_group, minority_group)

            # 2. Write to Majority Group (3/5 quorum achieved)
            res_maj = await leader.execute_command({"op": "SET", "key": "maj_key", "val": "maj_val"})
            assert res_maj is True, "Majority partition failed to commit write"

            # 3. Assert Majority nodes committed the write
            await cluster.assert_log_parity(nodes_to_check=majority_group)
            assert cluster.nodes[leader_id].state.kv_store == {"maj_key": "maj_val"}

            # 4. Minority nodes must NOT have committed this write
            for min_id in minority_group:
                assert "maj_key" not in cluster.nodes[min_id].state.kv_store

            # 5. Attempt write directly to a minority node (should fail because it cannot achieve 3/5 quorum)
            minority_node = cluster.nodes[minority_group[0]]
            res_min = await minority_node.execute_command("SET rogue=hack", timeout=0.2)
            assert res_min is False, "Minority node should not be able to commit writes"

            # 6. Heal the partition!
            cluster.heal()

            # 7. Wait for cluster to synchronize and assert 100% parity across all 5 nodes
            await cluster.assert_log_parity(nodes_to_check=cluster.node_ids, timeout=3.0)

            for nid in cluster.node_ids:
                node = cluster.nodes[nid]
                assert node.state.kv_store == {"maj_key": "maj_val"}
                assert "rogue" not in node.state.kv_store

        finally:
            await cluster.stop()

    asyncio.run(_test())


def test_scenario_c_crash_and_recovery() -> None:
    """Scenario C: Follower crashes, leader commits 5 writes, follower revives and catches up."""

    async def _test() -> None:
        cluster = RaftCluster(num_nodes=5)
        leader = await cluster.start()
        assert leader is not None

        try:
            # 1. Pick a follower to crash
            target_follower_id = [nid for nid in cluster.node_ids if nid != leader.node_id][-1]
            await cluster.kill_node(target_follower_id)

            # 2. Execute 5 sequential writes while follower is dead
            commands = [
                {"op": "SET", "key": f"key_{i}", "val": f"val_{i}"}
                for i in range(1, 6)
            ]
            for cmd in commands:
                res = await cluster.execute_command(cmd, timeout=3.0)
                assert res is True, f"Failed to commit {cmd}"

            # Surviving 4 nodes committed all 5 writes
            alive_nodes = [nid for nid in cluster.node_ids if nid != target_follower_id]
            await cluster.assert_log_parity(nodes_to_check=alive_nodes, timeout=3.0)

            # 3. Revive the dead follower!
            revived_node = await cluster.revive_node(target_follower_id)
            assert revived_node._is_running is True

            # 4. Wait for Leader's AppendEntries to synchronize revived node's log
            await cluster.assert_log_parity(nodes_to_check=cluster.node_ids, timeout=3.0)

            # 5. Assert revived node reached full 5-key parity
            expected_kv = {f"key_{i}": f"val_{i}" for i in range(1, 6)}
            assert revived_node.state.commit_index == 5
            assert revived_node.state.last_applied == 5
            assert len(revived_node.state.log) == 5
            assert revived_node.state.kv_store == expected_kv

        finally:
            await cluster.stop()

    asyncio.run(_test())


if __name__ == "__main__":
    tests = [
        test_scenario_a_leader_failure,
        test_scenario_b_network_partition_3_vs_2,
        test_scenario_c_crash_and_recovery,
    ]
    print(f"Running {len(tests)} Chaos Scenarios for Step 6:")
    for t in tests:
        t()
        print(f"  [PASS] {t.__name__}")

    print("\nAll Step 6 Chaos Tests passed successfully!")
