import asyncio
import sys
import os
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dcache.algorithm.consistent_hash import ConsistentHashRing, NodeInfo
from dcache.persistence.metadata_store import MetadataStore
from dcache.fault_tolerance.manager import FaultToleranceManager
from dcache.monitor.metrics import MetricsCollector
from dcache.node_manager.manager import NodeManager
from dcache.service.resilience import IdempotencyGuard

results = []


def log(msg):
    print(msg)
    results.append(msg)


async def test_migration_read_consistency():
    log("\n=== TEST 1: Migration + Read Consistency ===")
    with tempfile.TemporaryDirectory() as tmpdir:
        ring = ConsistentHashRing(vnodes_per_weight=50, replication_factor=2)
        meta = MetadataStore(db_path=os.path.join(tmpdir, "db"))
        meta.open()
        metrics = MetricsCollector()
        ft = FaultToleranceManager(get_replicas_fn=ring.get_nodes_for_key)

        nm = NodeManager(
            hash_ring=ring, meta_store=meta, fault_manager=ft,
            metrics=metrics, migration_concurrency=4,
            rebalance_interval=3600, skew_threshold=0.5,
        )
        await nm.start()

        await nm.add_node({"node_id": "n1", "host": "h1", "port": 8001, "weight": 3})
        await nm.add_node({"node_id": "n2", "host": "h2", "port": 8002, "weight": 2})

        written_keys = {}
        for i in range(200):
            key = f"key_{i}"
            value = f"value_{i}"
            node = nm.get_node(key)
            await nm.write_key(node, key, value)
            written_keys[key] = value

        log(f"Wrote {len(written_keys)} keys to {ring.get_node_count()} nodes")

        await nm.add_node({"node_id": "n3", "host": "h3", "port": 8003, "weight": 2})

        await asyncio.sleep(0.05)

        misses = 0
        for key, expected in written_keys.items():
            node = nm.get_node(key)
            result = await nm.read_key(node, key)
            if result != expected:
                misses += 1
                if misses <= 5:
                    log(f"  MISS key={key} got={result} expected={expected}")

        log(f"After adding n3: {misses}/{len(written_keys)} reads missed")
        assert misses < 20, f"Too many misses during migration: {misses}"

        await asyncio.sleep(1.0)

        misses_after = 0
        for key, expected in written_keys.items():
            node = nm.get_node(key)
            result = await nm.read_key(node, key)
            if result != expected:
                misses_after += 1

        log(f"After migration settle: {misses_after}/{len(written_keys)} reads missed")
        assert misses_after == 0, f"Misses after settle: {misses_after}"

        await nm.stop()
        meta.close()
        log("TEST 1 PASSED")


async def test_replica_failover():
    log("\n=== TEST 2: Replica Sync + Primary Failover ===")
    with tempfile.TemporaryDirectory() as tmpdir:
        ring = ConsistentHashRing(vnodes_per_weight=50, replication_factor=2)
        meta = MetadataStore(db_path=os.path.join(tmpdir, "db"))
        meta.open()
        metrics = MetricsCollector()
        ft = FaultToleranceManager(get_replicas_fn=ring.get_nodes_for_key)

        nm = NodeManager(
            hash_ring=ring, meta_store=meta, fault_manager=ft,
            metrics=metrics, migration_concurrency=4,
            rebalance_interval=3600, skew_threshold=0.5,
        )
        await nm.start()

        await nm.add_node({"node_id": "n1", "host": "h1", "port": 8001, "weight": 1})
        await nm.add_node({"node_id": "n2", "host": "h2", "port": 8002, "weight": 1})
        await nm.add_node({"node_id": "n3", "host": "h3", "port": 8003, "weight": 1})

        await asyncio.sleep(0.05)

        test_key = "my_test_key"
        test_value = "secret_value_42"
        primary = nm.get_node(test_key)
        log(f"  Test key={test_key} primary={primary} value={test_value}")
        ok = await nm.write_key(primary, test_key, test_value)
        assert ok, "Write failed"

        ring.set_node_offline(primary)
        await ft.handle_node_failure(primary, "simulated outage")

        fault_count = metrics.get_metrics_text().count("dcache_fault_total")
        log(f"  Fault count in metrics present: {fault_count > 0}")
        assert fault_count > 0, "Fault not recorded"

        await asyncio.sleep(0.1)

        new_primary = nm.get_node(test_key)
        read_result = await nm.read_key(new_primary, test_key)
        log(f"  After {primary} down, read via {new_primary} got: {read_result}")
        assert read_result == test_value, (
            f"After primary failover expected={test_value} got={read_result}"
        )

        await nm.stop()
        meta.close()
        log("TEST 2 PASSED")


async def test_node_removal_safe_drain():
    log("\n=== TEST 3: Node Removal Safe Drain ===")
    with tempfile.TemporaryDirectory() as tmpdir:
        ring = ConsistentHashRing(vnodes_per_weight=50, replication_factor=2)
        meta = MetadataStore(db_path=os.path.join(tmpdir, "db"))
        meta.open()
        metrics = MetricsCollector()
        ft = FaultToleranceManager(get_replicas_fn=ring.get_nodes_for_key)

        nm = NodeManager(
            hash_ring=ring, meta_store=meta, fault_manager=ft,
            metrics=metrics, migration_concurrency=4,
            rebalance_interval=3600, skew_threshold=0.5,
        )
        await nm.start()

        await nm.add_node({"node_id": "n1", "host": "h1", "port": 8001, "weight": 2})
        await nm.add_node({"node_id": "n2", "host": "h2", "port": 8002, "weight": 2})
        await nm.add_node({"node_id": "n3", "host": "h3", "port": 8003, "weight": 2})

        await asyncio.sleep(0.05)

        written = {}
        for i in range(100):
            key = f"safe_k_{i}"
            val = f"safe_v_{i}"
            n = nm.get_node(key)
            await nm.write_key(n, key, val)
            written[key] = val

        await nm.remove_node("n3")
        status = nm.get_cluster_status()
        log(f"  After remove n3: draining={status['draining_nodes']} total_nodes={status['total_nodes']}")

        misses = 0
        for key, expected in written.items():
            n = nm.get_node(key)
            got = await nm.read_key(n, key)
            if got != expected:
                misses += 1

        log(f"  Immediate reads after remove_node: {misses}/{len(written)} missed")
        assert misses == 0, f"Data lost after node removal: {misses} misses"

        await asyncio.sleep(0.5)
        status2 = nm.get_cluster_status()
        log(f"  After settle: draining={status2['draining_nodes']} total_nodes={status2['total_nodes']}")

        misses2 = 0
        for key, expected in written.items():
            n = nm.get_node(key)
            got = await nm.read_key(n, key)
            if got != expected:
                misses2 += 1
        log(f"  Settled reads after remove_node: {misses2}/{len(written)} missed")
        assert misses2 == 0

        await nm.stop()
        meta.close()
        log("TEST 3 PASSED")


async def test_hit_miss_metrics():
    log("\n=== TEST 4: Hit/Miss Metrics Accuracy ===")
    with tempfile.TemporaryDirectory() as tmpdir:
        ring = ConsistentHashRing(vnodes_per_weight=50, replication_factor=2)
        meta = MetadataStore(db_path=os.path.join(tmpdir, "db"))
        meta.open()
        metrics = MetricsCollector()
        ft = FaultToleranceManager(get_replicas_fn=ring.get_nodes_for_key)

        nm = NodeManager(
            hash_ring=ring, meta_store=meta, fault_manager=ft,
            metrics=metrics, migration_concurrency=4,
            rebalance_interval=3600, skew_threshold=0.5,
        )
        await nm.start()

        await nm.add_node({"node_id": "n1", "host": "h1", "port": 8001, "weight": 1})
        await nm.add_node({"node_id": "n2", "host": "h2", "port": 8002, "weight": 1})

        await asyncio.sleep(0.05)

        hit_key = "existing_k"
        n = nm.get_node(hit_key)
        await nm.write_key(n, hit_key, "v1")

        await nm.read_key(n, hit_key)
        await nm.read_key(n, hit_key)
        await nm.read_key(n, "nonexistent_a")
        await nm.read_key(n, "nonexistent_b")

        hit_total = metrics._hit_counts.get(n, 0)
        miss_total = metrics._miss_counts.get(n, 0)

        total_for_node = hit_total + miss_total
        expected_total = 4
        log(f"  For node {n}: hits={hit_total} misses={miss_total} total={total_for_node}")
        assert total_for_node == expected_total, (
            f"Expected {expected_total} total count for node, got {total_for_node} "
            "(duplicate counting?)"
        )
        assert hit_total == 2, f"Expected 2 hits, got {hit_total}"
        assert miss_total == 2, f"Expected 2 misses, got {miss_total}"

        metrics_text = metrics.get_metrics_text()
        for line in metrics_text.splitlines():
            if line.startswith("dcache_node_hit_rate{") and n in line:
                log(f"  Hit rate metric: {line}")
                val = float(line.split()[-1])
                assert 0.49 <= val <= 0.51, f"Hit rate should be ~0.5, got {val}"

        await nm.stop()
        meta.close()
        log("TEST 4 PASSED")


async def test_idempotency_concurrent():
    log("\n=== TEST 5: Concurrent PUT Idempotency ===")
    ig = IdempotencyGuard()

    execution_count = 0
    lock = asyncio.Lock()

    async def do_work(request_id, value):
        nonlocal execution_count
        result = await ig.check_and_mark(request_id)
        if result is IdempotencyGuard.READY:
            async with lock:
                execution_count += 1
            await asyncio.sleep(0.05)
            response = {"processed": True, "value": value, "exec_num": execution_count}
            await ig.record_result(request_id, response)
            return response
        elif result is IdempotencyGuard.PENDING:
            response = await ig.wait_for_result(request_id)
            return response
        else:
            return result

    req_id = "req-concurrent-42"
    results_list = []

    async def worker(idx):
        r = await do_work(req_id, f"worker_{idx}")
        results_list.append((idx, r))

    await asyncio.gather(*[worker(i) for i in range(5)])

    log(f"  Executions: {execution_count}")
    assert execution_count == 1, f"Expected 1 execution for same request id, got {execution_count}"

    for idx, r in results_list:
        log(f"  Worker {idx} result: {r}")
        assert r is not None and r.get("processed"), f"Worker {idx} got no result"
        assert r.get("exec_num") == 1, f"Worker {idx} got wrong exec_num"

    log("TEST 5 PASSED")


async def main():
    log("=" * 60)
    log("INTEGRATION TESTS FOR BUG FIXES")
    log("=" * 60)

    tests = [
        test_migration_read_consistency,
        test_replica_failover,
        test_node_removal_safe_drain,
        test_hit_miss_metrics,
        test_idempotency_concurrent,
    ]

    passed = 0
    failed = 0
    for test in tests:
        try:
            await test()
            passed += 1
        except Exception as e:
            log(f"  FAILED: {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    log(f"\n{'=' * 60}")
    log(f"Results: {passed} passed, {failed} failed")
    log("=" * 60)

    result_file = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..", "integration_test_result.txt"
    )
    with open(result_file, "w") as f:
        f.write("\n".join(results))

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    asyncio.run(main())
