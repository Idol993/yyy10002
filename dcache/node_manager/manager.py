import asyncio
import logging
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable, Coroutine, Dict, List, Optional, Set

from ..algorithm.consistent_hash import (
    ConsistentHashRing,
    MigrationPlan,
    NodeInfo,
    DuplicateNodeError,
    NodeNotFoundError,
)
from ..persistence.metadata_store import MetadataStore
from ..fault_tolerance.manager import FaultToleranceManager
from ..monitor.metrics import MetricsCollector

logger = logging.getLogger(__name__)


_MISS_SENTINEL = object()


class NodeManager:

    def __init__(
        self,
        hash_ring: ConsistentHashRing,
        meta_store: MetadataStore,
        fault_manager: FaultToleranceManager,
        metrics: MetricsCollector,
        migration_concurrency: int = 4,
        rebalance_interval: float = 60.0,
        skew_threshold: float = 0.3,
        migrate_data_fn: Optional[Callable] = None,
    ):
        self._ring = hash_ring
        self._meta = meta_store
        self._fault = fault_manager
        self._metrics = metrics

        self._migration_concurrency = migration_concurrency
        self._rebalance_interval = rebalance_interval
        self._skew_threshold = skew_threshold
        self._migrate_data_fn = migrate_data_fn

        self._migration_semaphore = asyncio.Semaphore(migration_concurrency)
        self._active_migrations: Dict[str, asyncio.Task] = {}
        self._migration_lock = asyncio.Lock()
        self._rebalance_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._node_heartbeats: Dict[str, float] = {}
        self._heartbeat_timeout = 15.0
        self._running = False

        self._shard_data: Dict[str, Dict[str, Any]] = defaultdict(dict)

        self._draining_nodes: Set[str] = set()
        self._fault_recorded: Set[str] = set()
        self._fault_lock = asyncio.Lock()
        self._data_lock = asyncio.Lock()

    async def start(self) -> None:
        self._running = True
        self._rebalance_task = asyncio.create_task(self._rebalance_loop())
        self._heartbeat_task = asyncio.create_task(self._heartbeat_monitor())
        logger.info("NodeManager started with rebalance_interval=%.1fs", self._rebalance_interval)

    async def stop(self) -> None:
        self._running = False
        if self._rebalance_task:
            self._rebalance_task.cancel()
            try:
                await self._rebalance_task
            except asyncio.CancelledError:
                pass
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass

        for mid, task in list(self._active_migrations.items()):
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._active_migrations.clear()
        logger.info("NodeManager stopped")

    async def add_node(self, node_data: Dict[str, Any]) -> NodeInfo:
        node_id = node_data["node_id"]
        host = node_data["host"]
        port = node_data["port"]
        weight = node_data.get("weight", 1)

        node_info = NodeInfo(
            node_id=node_id, host=host, port=port, weight=weight
        )

        try:
            migration_plans = self._ring.add_node(node_info)
        except DuplicateNodeError:
            logger.warning("Attempted to add duplicate node: %s", node_id)
            raise

        self._node_heartbeats[node_id] = time.monotonic()

        self._meta.save_node_info(node_id, {
            "node_id": node_id,
            "host": host,
            "port": port,
            "weight": weight,
            "is_online": True,
            "added_at": time.time(),
        })
        self._persist_topology()

        if migration_plans:
            for plan in migration_plans:
                await self._start_migration(plan)

        self._update_cluster_metrics()
        logger.info(
            "Node %s added (host=%s:%d, weight=%d), %d migrations",
            node_id, host, port, weight, len(migration_plans),
        )
        return node_info

    async def remove_node(self, node_id: str) -> None:
        logger.info("Safe remove of node %s starting", node_id)

        if node_id not in self._ring.get_all_nodes():
            raise NodeNotFoundError(f"Node {node_id} not found")

        self._draining_nodes.add(node_id)
        self._ring.set_node_offline(node_id)

        drain_migration_id = f"drain_{node_id}"
        async with self._migration_lock:
            if drain_migration_id not in self._active_migrations:
                task = asyncio.create_task(self._execute_drain(node_id, drain_migration_id))
                self._active_migrations[drain_migration_id] = task

        self._node_heartbeats.pop(node_id, None)
        self._meta.delete_node_info(node_id)
        self._persist_topology()
        self._update_cluster_metrics()

        logger.info(
            "Node %s drain started, API returning ok (background drain in progress)",
            node_id,
        )

        async def _finalize_removal():
            try:
                await asyncio.sleep(0)
                await self._wait_active_migrations_for_node(node_id, drain_migration_id)
                try:
                    self._ring.remove_node(node_id)
                except NodeNotFoundError:
                    pass
                self._draining_nodes.discard(node_id)
                async with self._data_lock:
                    self._shard_data.pop(node_id, None)
                self._persist_topology()
                logger.info("Node %s fully removed after drain complete", node_id)
            except Exception as e:
                logger.error("Error finalizing removal of %s: %s", node_id, e)

        asyncio.create_task(_finalize_removal())

    async def _execute_drain(self, source_node: str, migration_id: str) -> None:
        async with self._migration_semaphore:
            start_time = time.monotonic()
            success = True

            try:
                logger.info("Drain %s: node %s -> remaining nodes started", migration_id, source_node)

                async with self._data_lock:
                    source_items = list(self._shard_data.get(source_node, {}).items())

                per_target: Dict[str, List[tuple]] = defaultdict(list)
                for key, value in source_items:
                    target = self._ring.get_node(key)
                    if target is None or target == source_node:
                        remaining = [
                            nid for nid, n in self._ring.get_online_nodes().items()
                            if nid != source_node
                        ]
                        if remaining:
                            target = remaining[0]
                        else:
                            continue
                    per_target[target].append((key, value))

                total_moved = 0
                for target, items in per_target.items():
                    batch_size = 100
                    for i in range(0, len(items), batch_size):
                        batch = items[i : i + batch_size]
                        async with self._data_lock:
                            for key, value in batch:
                                self._shard_data[target][key] = value
                        total_moved += len(batch)
                        await asyncio.sleep(0)

                duration = time.monotonic() - start_time
                self._metrics.record_migration(source_node, "multi_target", duration, True)

                self._meta.save_migration_state(migration_id, {
                    "migration_id": migration_id,
                    "source_node": source_node,
                    "target_nodes": list(per_target.keys()),
                    "status": "completed",
                    "completed_at": time.time(),
                    "moved_keys": total_moved,
                })
                logger.info(
                    "Drain %s completed in %.3fs, %d keys across %d targets",
                    migration_id, duration, total_moved, len(per_target),
                )
            except Exception as e:
                success = False
                duration = time.monotonic() - start_time
                self._metrics.record_migration(source_node, "multi_target", duration, False)
                logger.error("Drain %s failed: %s", migration_id, e)
                self._meta.save_migration_state(migration_id, {
                    "migration_id": migration_id,
                    "source_node": source_node,
                    "status": "failed",
                    "error": str(e),
                })
            finally:
                async with self._migration_lock:
                    self._active_migrations.pop(migration_id, None)

    async def _wait_active_migrations_for_node(
        self, node_id: str, primary_migration_id: Optional[str] = None
    ) -> None:
        deadline = time.monotonic() + 120.0
        while time.monotonic() < deadline:
            async with self._migration_lock:
                done = True
                if primary_migration_id and primary_migration_id in self._active_migrations:
                    if not self._active_migrations[primary_migration_id].done():
                        done = False
                for mid, task in list(self._active_migrations.items()):
                    if not task.done():
                        done = False
                        break
            if done:
                return
            await asyncio.sleep(0.1)

    async def graceful_remove_node(self, node_id: str) -> None:
        await self.remove_node(node_id)

    def update_heartbeat(self, node_id: str) -> None:
        self._node_heartbeats[node_id] = time.monotonic()

    def get_node(self, key: str) -> Optional[str]:
        return self._ring.get_node(key)

    def get_nodes_for_key(self, key: str) -> List[str]:
        return self._ring.get_nodes_for_key(key)

    def _all_responsible_nodes(self, key: str) -> List[str]:
        return self.get_nodes_for_key(key)

    async def _record_read_fault_if_needed(self, primary_node: str, took_over: bool) -> None:
        if not took_over:
            return
        async with self._fault_lock:
            if primary_node in self._fault_recorded:
                return
            self._fault_recorded.add(primary_node)
        try:
            await self._fault.handle_node_failure(
                primary_node, "Primary offline during GET, replica took over"
            )
            self._metrics.record_fault(primary_node, "replica_takeover")
            logger.warning(
                "Recorded fault for node %s due to replica takeover during read",
                primary_node,
            )
        except Exception as e:
            logger.error("Error recording read fault for %s: %s", primary_node, e)

    async def read_key(self, node_id: str, key: str) -> Optional[Any]:
        responsible_nodes = self._all_responsible_nodes(key)
        tried = set()
        result: Optional[Any] = None
        hit_recorded = False
        primary_node = responsible_nodes[0] if responsible_nodes else node_id
        primary_was_offline = False

        for candidate in responsible_nodes:
            if candidate in tried:
                continue
            tried.add(candidate)
            node_info = self._ring.get_all_nodes().get(candidate)
            if node_info is None:
                if candidate == primary_node:
                    primary_was_offline = True
                continue
            if not node_info.is_online and candidate not in self._draining_nodes:
                if candidate == primary_node:
                    primary_was_offline = True
                continue

            async with self._data_lock:
                data = self._shard_data.get(candidate, {})
                value = data.get(key, _MISS_SENTINEL)
            if value is not _MISS_SENTINEL:
                if not hit_recorded:
                    self._metrics.record_hit(candidate)
                    hit_recorded = True
                result = value
                break

        if result is None:
            for drain_node in list(self._draining_nodes):
                async with self._data_lock:
                    drain_data = self._shard_data.get(drain_node, {})
                    value = drain_data.get(key, _MISS_SENTINEL)
                if value is not _MISS_SENTINEL:
                    if not hit_recorded:
                        self._metrics.record_hit(drain_node)
                        hit_recorded = True
                    result = value
                    break

        if result is None:
            cached = await self._fault.local_cache.get(key)
            if cached is not None:
                if not hit_recorded:
                    self._metrics.record_hit("local_cache")
                    hit_recorded = True
                return cached

        took_over = (primary_was_offline and result is not None and primary_node not in tried) \
            or (primary_was_offline and result is not None)

        if primary_was_offline and result is not None and not hit_recorded:
            took_over = True
        elif primary_was_offline and result is not None:
            took_over = True

        if primary_was_offline and result is not None:
            await self._record_read_fault_if_needed(primary_node, took_over)

        if result is None and not hit_recorded:
            self._metrics.record_miss(node_id if node_id else "unknown")

        return result

    async def write_key(self, node_id: str, key: str, value: Any) -> bool:
        all_nodes = self._all_responsible_nodes(key)
        success_count = 0

        for target in all_nodes:
            node_info = self._ring.get_all_nodes().get(target)
            if node_info is None:
                continue
            if not node_info.is_online and target not in self._draining_nodes:
                continue
            try:
                async with self._data_lock:
                    self._shard_data[target][key] = value
                success_count += 1
            except Exception as e:
                logger.warning("Write to replica %s for key %s failed: %s", target, key, e)

        await self._fault.local_cache.put(key, value)

        if success_count > 0:
            return True

        try:
            async with self._data_lock:
                if node_id in self._ring.get_all_nodes():
                    self._shard_data[node_id][key] = value
                else:
                    if all_nodes:
                        self._shard_data[all_nodes[0]][key] = value
            return True
        except Exception as e:
            logger.error("All writes failed for key %s: %s", key, e)
            return False

    async def delete_key(self, node_id: str, key: str) -> bool:
        all_nodes = self._all_responsible_nodes(key)
        for target in all_nodes:
            async with self._data_lock:
                data = self._shard_data.get(target, {})
                data.pop(key, None)
        await self._fault.local_cache.delete(key)
        return True

    async def _remote_read(self, node_id: str, key: str) -> Optional[Any]:
        async with self._data_lock:
            return self._shard_data.get(node_id, {}).get(key)

    async def _remote_write(self, node_id: str, key: str, value: Any) -> bool:
        async with self._data_lock:
            self._shard_data[node_id][key] = value
        return True

    async def _start_migration(self, plan: MigrationPlan) -> str:
        migration_id = str(uuid.uuid4())[:8]

        self._meta.save_migration_state(migration_id, {
            "migration_id": migration_id,
            "source_node": plan.source_node,
            "target_node": plan.target_node,
            "status": "pending",
            "started_at": time.time(),
        })

        async with self._migration_lock:
            if migration_id in self._active_migrations:
                return migration_id

            task = asyncio.create_task(
                self._execute_migration(migration_id, plan)
            )
            self._active_migrations[migration_id] = task

        return migration_id

    async def _execute_migration(
        self, migration_id: str, plan: MigrationPlan
    ) -> None:
        async with self._migration_semaphore:
            start_time = time.monotonic()
            success = True

            try:
                logger.info(
                    "Migration %s: %s -> %s started",
                    migration_id, plan.source_node, plan.target_node,
                )

                async with self._data_lock:
                    source_items = list(self._shard_data.get(plan.source_node, {}).items())

                keys_to_move = []
                for key, value in source_items:
                    real_target = self._ring.get_node(key)
                    if real_target == plan.target_node:
                        keys_to_move.append((key, value))

                batch_size = 100
                moved = 0
                for i in range(0, len(keys_to_move), batch_size):
                    batch = keys_to_move[i : i + batch_size]
                    async with self._data_lock:
                        for key, value in batch:
                            self._shard_data[plan.target_node][key] = value
                    moved += len(batch)
                    await asyncio.sleep(0)

                self._meta.save_migration_state(migration_id, {
                    "migration_id": migration_id,
                    "source_node": plan.source_node,
                    "target_node": plan.target_node,
                    "status": "completed",
                    "completed_at": time.time(),
                    "moved_keys": moved,
                })

                duration = time.monotonic() - start_time
                self._metrics.record_migration(
                    plan.source_node, plan.target_node, duration, True
                )
                logger.info(
                    "Migration %s completed in %.3fs, %d/%d keys migrated",
                    migration_id, duration, moved, len(keys_to_move),
                )

            except Exception as e:
                success = False
                duration = time.monotonic() - start_time
                self._metrics.record_migration(
                    plan.source_node, plan.target_node, duration, False
                )
                logger.error("Migration %s failed: %s", migration_id, e)

                self._meta.save_migration_state(migration_id, {
                    "migration_id": migration_id,
                    "source_node": plan.source_node,
                    "target_node": plan.target_node,
                    "status": "failed",
                    "error": str(e),
                })
            finally:
                async with self._migration_lock:
                    self._active_migrations.pop(migration_id, None)

    async def _rebalance_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(self._rebalance_interval)
                await self._run_rebalance()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Rebalance loop error: %s", e)

    async def _run_rebalance(self) -> None:
        self._metrics.record_rebalance()
        plans = self._ring.compute_rebalance_plan(
            skew_threshold=self._skew_threshold
        )
        if plans:
            logger.info("Rebalance: %d migration plans generated", len(plans))
            for plan in plans:
                await self._start_migration(plan)
        self._update_cluster_metrics()

    async def _heartbeat_monitor(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(5.0)
                now = time.monotonic()
                for node_id, last_hb in list(self._node_heartbeats.items()):
                    if now - last_hb > self._heartbeat_timeout:
                        if node_id in self._draining_nodes:
                            continue
                        logger.warning(
                            "Node %s heartbeat timeout (%.1fs), marking offline",
                            node_id, now - last_hb,
                        )
                        self._ring.set_node_offline(node_id)
                        await self._fault.handle_node_failure(
                            node_id, "Heartbeat timeout"
                        )
                        self._metrics.record_fault(node_id, "heartbeat_timeout")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Heartbeat monitor error: %s", e)

    def _persist_topology(self) -> None:
        nodes = {}
        for nid, ninfo in self._ring.get_all_nodes().items():
            nodes[nid] = {
                "node_id": ninfo.node_id,
                "host": ninfo.host,
                "port": ninfo.port,
                "weight": ninfo.weight,
                "is_online": ninfo.is_online,
                "replica_of": ninfo.replica_of,
            }

        topology = {
            "version": int(time.time()),
            "nodes": nodes,
            "replication_factor": self._ring.get_replication_factor(),
            "vnode_count": self._ring.get_vnode_count(),
        }
        self._meta.save_topology(topology)

    def _update_cluster_metrics(self) -> None:
        self._metrics.update_cluster_metrics(
            active_node_count=len(self._ring.get_online_nodes()),
            vnode_count=self._ring.get_vnode_count(),
            shard_count=len(self._shard_data),
            local_cache_size=self._fault.local_cache.size(),
        )

    async def recover_from_persistence(self) -> None:
        logger.info("Starting recovery from persistent store")
        topology = self._meta.recover_topology()
        if not topology:
            logger.info("No topology to recover")
            return

        nodes = topology.get("nodes", {})
        for nid, ndata in nodes.items():
            try:
                node_info = NodeInfo(
                    node_id=ndata["node_id"],
                    host=ndata["host"],
                    port=ndata["port"],
                    weight=ndata.get("weight", 1),
                    is_online=ndata.get("is_online", True),
                    replica_of=ndata.get("replica_of"),
                )
                if node_info.is_online:
                    self._ring.add_node(node_info)
                    self._node_heartbeats[nid] = time.monotonic()
                else:
                    self._ring.add_node(node_info)
                    self._ring.set_node_offline(nid)
                logger.info("Recovered node %s", nid)
            except DuplicateNodeError:
                logger.debug("Node %s already in ring during recovery", nid)
            except Exception as e:
                logger.error("Failed to recover node %s: %s", nid, e)

        pending = self._meta.load_pending_migrations()
        if pending:
            logger.info("Found %d pending migrations to resume", len(pending))

        self._update_cluster_metrics()
        logger.info(
            "Recovery complete: %d nodes, %d vnodes",
            self._ring.get_node_count(),
            self._ring.get_vnode_count(),
        )

    def get_cluster_status(self) -> Dict[str, Any]:
        distribution = self._ring.compute_load_distribution()
        return {
            "total_nodes": self._ring.get_node_count(),
            "online_nodes": len(self._ring.get_online_nodes()),
            "vnode_count": self._ring.get_vnode_count(),
            "replication_factor": self._ring.get_replication_factor(),
            "active_migrations": len(self._active_migrations),
            "draining_nodes": list(self._draining_nodes),
            "load_distribution": distribution,
        }
