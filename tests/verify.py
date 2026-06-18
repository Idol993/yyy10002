import asyncio
import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

output_lines = []

def log(msg):
    print(msg)
    output_lines.append(msg)

def test_algorithm_layer():
    from dcache.algorithm.consistent_hash import ConsistentHashRing, NodeInfo, DuplicateNodeError, NodeNotFoundError

    ring = ConsistentHashRing(vnodes_per_weight=50, replication_factor=2)

    ring.add_node(NodeInfo("n1", "h1", 8001, weight=1))
    ring.add_node(NodeInfo("n2", "h2", 8002, weight=2))
    ring.add_node(NodeInfo("n3", "h3", 8003, weight=3))

    n = ring.get_node("test_key")
    assert n is not None, "get_node returned None"
    log(f"  test_key -> {n}")

    nodes = ring.get_nodes_for_key("test_key")
    assert len(nodes) >= 1, "get_nodes_for_key returned empty"
    log(f"  Replicas: {nodes}")

    dist = ring.compute_load_distribution()
    assert len(dist) == 3, f"Expected 3 nodes in distribution, got {len(dist)}"
    log(f"  Load distribution: {dist}")

    assert ring.get_node_count() == 3
    assert ring.get_vnode_count() > 0

    plans = ring.remove_node("n2")
    assert ring.get_node_count() == 2
    log(f"  After removing n2: {ring.get_node_count()} nodes, {len(plans)} migration plans")

    try:
        ring.add_node(NodeInfo("n1", "h1", 8001, weight=1))
        assert False, "Should have raised DuplicateNodeError"
    except DuplicateNodeError:
        pass

    try:
        ring.remove_node("nonexistent")
        assert False, "Should have raised NodeNotFoundError"
    except NodeNotFoundError:
        pass

    log("  ALGORITHM LAYER OK")


def test_persistence_layer():
    from dcache.persistence.metadata_store import MetadataStore
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test_db")
        store = MetadataStore(db_path=db_path)
        store.open()

        store.save_node_info("node_1", {"node_id": "node_1", "host": "127.0.0.1", "port": 8001, "weight": 2})
        info = store.load_node_info("node_1")
        assert info is not None
        assert info["weight"] == 2

        all_nodes = store.load_all_nodes()
        assert "node_1" in all_nodes

        store.save_topology({"version": 123, "nodes": {"node_1": info}})
        topo = store.load_topology()
        assert topo is not None
        assert topo["version"] == 123

        store.delete_node_info("node_1")
        assert store.load_node_info("node_1") is None

        recovered = store.recover_topology()
        assert recovered["version"] == 123

        store.close()
        log("  PERSISTENCE LAYER OK")


def test_fault_tolerance_layer():
    from dcache.fault_tolerance.manager import FaultToleranceManager, FaultLevel, FaultEvent

    async def _test():
        def get_replicas(key):
            return ["n1", "n2", "n3"]

        events_received = []

        async def alert_cb(event):
            events_received.append(event)

        ft = FaultToleranceManager(
            get_replicas_fn=get_replicas,
            alert_callbacks=[alert_cb],
        )

        await ft.handle_node_failure("n1", "Connection refused")

        assert ft.replica_manager.is_node_failed("n1")

        assert len(events_received) == 1
        assert events_received[0].node_id == "n1"

        await ft.local_cache.put("key1", "value1")
        val = await ft.local_cache.get("key1")
        assert val == "value1"

        async def read_fn(node, key):
            if node == "n1":
                raise ConnectionError("refused")
            if node == "n2":
                return "from_replica"
            return None

        result = await ft.read_with_fallback("key1", "n1", read_fn)
        assert result == "from_replica"

        await ft.handle_node_recovery("n1")
        assert not ft.replica_manager.is_node_failed("n1")

        log("  FAULT TOLERANCE LAYER OK")

    asyncio.run(_test())


def test_monitoring_layer():
    from dcache.monitor.metrics import MetricsCollector, RequestTimer

    collector = MetricsCollector()

    collector.record_hit("n1")
    collector.record_hit("n1")
    collector.record_miss("n1")

    collector.record_migration("n1", "n2", 1.5, True)
    collector.record_fault("n1", "heartbeat_timeout")

    collector.record_request("GET", "/cache", 0.05, "ok")

    collector.update_cluster_metrics(3, 300, 10, 5)

    metrics_text = collector.get_metrics_text()
    assert "dcache_node_hit_total" in metrics_text
    assert "dcache_migration_duration_seconds" in metrics_text
    assert "dcache_fault_total" in metrics_text

    import time as _time
    with RequestTimer(collector, "GET", "/test"):
        _time.sleep(0.01)

    log("  MONITORING LAYER OK")


def test_resilience_layer():
    from dcache.service.resilience import CircuitBreaker, CircuitBreakerConfig, SlidingWindowRateLimiter, RetryExecutor, IdempotencyGuard

    async def _test():
        cb = CircuitBreaker(CircuitBreakerConfig(failure_threshold=3, recovery_timeout=0.1))
        assert await cb.allow_request()
        for _ in range(3):
            await cb.record_failure()
        assert not await cb.allow_request()
        await asyncio.sleep(0.15)
        assert await cb.allow_request()
        await cb.record_success()
        log("  CircuitBreaker OK")

        rl = SlidingWindowRateLimiter()
        for i in range(100):
            assert await rl.allow("test")
        log("  RateLimiter OK")

        call_count = 0

        async def flaky_fn():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise RuntimeError("flaky")
            return "ok"

        re = RetryExecutor()
        result = await re.execute_with_retry(flaky_fn)
        assert result == "ok"
        log("  RetryExecutor OK")

        ig = IdempotencyGuard()

        log("  IdempotencyGuard scenario 1: first request (READY)")
        state = await ig.check_and_mark("req_first")
        assert state is IdempotencyGuard.READY, f"Expected READY, got {state!r}"
        await ig.record_result("req_first", {"data": 42})

        log("  IdempotencyGuard scenario 2: completed duplicate -> cached result")
        cached = await ig.check_and_mark("req_first")
        assert cached == {"data": 42}, f"Expected cached result, got {cached!r}"

        log("  IdempotencyGuard scenario 3: concurrent duplicate (PENDING)")
        state2 = await ig.check_and_mark("req_concurrent")
        assert state2 is IdempotencyGuard.READY

        async def slow_writer():
            await asyncio.sleep(0.05)
            await ig.record_result("req_concurrent", {"data": 7})

        async def waiter():
            state3 = await ig.check_and_mark("req_concurrent")
            if state3 is IdempotencyGuard.PENDING:
                return await ig.wait_for_result("req_concurrent")
            return state3

        writer_task = asyncio.create_task(slow_writer())
        waiter_task = asyncio.create_task(waiter())
        _, waiter_result = await asyncio.gather(writer_task, waiter_task)
        assert waiter_result == {"data": 7}, f"Expected waiter to get {{'data': 7}}, got {waiter_result!r}"

        log("  IdempotencyGuard OK")

        log("  RESILIENCE LAYER OK")

    asyncio.run(_test())


def test_node_manager():
    from dcache.algorithm.consistent_hash import ConsistentHashRing
    from dcache.persistence.metadata_store import MetadataStore
    from dcache.fault_tolerance.manager import FaultToleranceManager
    from dcache.monitor.metrics import MetricsCollector
    from dcache.node_manager.manager import NodeManager
    import tempfile

    async def _test():
        with tempfile.TemporaryDirectory() as tmpdir:
            ring = ConsistentHashRing(vnodes_per_weight=50, replication_factor=2)
            meta = MetadataStore(db_path=os.path.join(tmpdir, "test_db"))
            meta.open()
            metrics = MetricsCollector()
            ft = FaultToleranceManager(get_replicas_fn=ring.get_nodes_for_key)

            nm = NodeManager(
                hash_ring=ring,
                meta_store=meta,
                fault_manager=ft,
                metrics=metrics,
                migration_concurrency=2,
                rebalance_interval=3600,
            )

            await nm.start()

            await nm.add_node({"node_id": "n1", "host": "h1", "port": 8001, "weight": 2})
            await nm.add_node({"node_id": "n2", "host": "h2", "port": 8002, "weight": 1})
            await nm.add_node({"node_id": "n3", "host": "h3", "port": 8003, "weight": 3})

            node = nm.get_node("some_key")
            assert node is not None

            await nm.write_key(node, "some_key", "some_value")
            val = await nm.read_key(node, "some_key")
            assert val == "some_value"

            status = nm.get_cluster_status()
            assert status["total_nodes"] == 3
            log(f"  Cluster status: {status}")

            written = {}
            for i in range(50):
                k = f"drain_k_{i}"
                v = f"drain_v_{i}"
                n = nm.get_node(k)
                await nm.write_key(n, k, v)
                written[k] = v

            await nm.remove_node("n2")
            status_after_remove = nm.get_cluster_status()
            log(f"  After remove_node API returned: draining={status_after_remove['draining_nodes']} total_nodes={status_after_remove['total_nodes']}")
            assert "n2" in status_after_remove["draining_nodes"] or status_after_remove["total_nodes"] <= 3

            miss_immediate = 0
            for k, v in written.items():
                n = nm.get_node(k)
                got = await nm.read_key(n, k)
                if got != v:
                    miss_immediate += 1
            log(f"  Immediate reads after remove_node: {miss_immediate}/{len(written)} misses")
            assert miss_immediate == 0, f"Data lost immediately after remove_node: {miss_immediate} misses"

            await asyncio.sleep(0.5)
            final_status = nm.get_cluster_status()
            log(f"  After drain settled: total_nodes={final_status['total_nodes']} draining={final_status['draining_nodes']}")
            assert final_status["total_nodes"] == 2, f"Expected 2 nodes after full removal, got {final_status['total_nodes']}"

            miss_final = 0
            for k, v in written.items():
                n = nm.get_node(k)
                got = await nm.read_key(n, k)
                if got != v:
                    miss_final += 1
            log(f"  Reads after drain complete: {miss_final}/{len(written)} misses")
            assert miss_final == 0, f"Data lost after drain finished: {miss_final} misses"

            await nm.stop()
            meta.close()
            log("  NODE MANAGER OK")

    asyncio.run(_test())


def main():
    log("=" * 60)
    log("VERIFICATION TESTS")
    log("=" * 60)

    tests = [
        ("Algorithm Layer", test_algorithm_layer),
        ("Persistence Layer", test_persistence_layer),
        ("Fault Tolerance Layer", test_fault_tolerance_layer),
        ("Monitoring Layer", test_monitoring_layer),
        ("Resilience Layer", test_resilience_layer),
        ("Node Manager", test_node_manager),
    ]

    passed = 0
    failed = 0
    for name, fn in tests:
        log(f"\n[TEST] {name}")
        try:
            fn()
            passed += 1
        except Exception as e:
            log(f"  FAILED: {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    log(f"\n{'=' * 60}")
    log(f"Results: {passed} passed, {failed} failed")
    log("=" * 60)

    result_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "test_result.txt")
    with open(result_file, "w") as f:
        f.write("\n".join(output_lines))

    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
