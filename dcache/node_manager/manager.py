import asyncio
import logging
import time
import uuid
from collections import defaultdict
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

        for mid, task in self._active_migrations.items():
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
        try:
            migration_plans = self._ring.remove_node(node_id)
        except NodeNotFoundError:
            logger.warning("Attempted to remove unknown node: %s", node_id)
            raise

        self._node_heartbeats.pop(node_id, None)
        self._meta.delete_node_info(node_id)
        self._persist_topology()

        if migration_plans:
            for plan in migration_plans:
                await self._start_migration(plan)

        self._update_cluster_metrics()
        logger.info(
            "Node %s removed, %d migrations", node_id, len(migration_plans)
        )

    async def graceful_remove_node(self, node_id: str) -> None:
        logger.info("Graceful removal of node %s", node_id)
        self._ring.set_node_offline(node_id)

        migration_plans = self._ring.compute_rebalance_plan(
            skew_threshold=0.01
        )

        node_plans = [p for p in migration_plans if p.source_node == node_id]
        for plan in node_plans:
            await self._start_migration(plan)

        await asyncio.sleep(0.1)
        await self.remove_node(node_id)

    def update_heartbeat(self, node_id: str) -> None:
        self._node_heartbeats[node_id] = time.monotonic()

    def get_node(self, key: str) -> Optional[str]:
        return self._ring.get_node(key)

    def get_nodes_for_key(self, key: str) -> List[str]:
        return self._ring.get_nodes_for_key(key)

    async def read_key(self, node_id: str, key: str) -> Optional[Any]:
        node = self._ring.get_all_nodes().get(node_id)
        if node is None or not node.is_online:
            return await self._fault.read_with_fallback(
                key, node_id, self._remote_read
            )

        try:
            data = self._shard_data.get(node_id, {}).get(key)
            if data is not None:
                self._metrics.record_hit(node_id)
                return data
            self._metrics.record_miss(node_id)
            return None
        except Exception as e:
            logger.warning("Read failed on node %s for key %s: %s", node_id, key, e)
            return await self._fault.read_with_fallback(
                key, node_id, self._remote_read
            )

    async def write_key(self, node_id: str, key: str, value: Any) -> bool:
        node = self._ring.get_all_nodes().get(node_id)
        if node is None or not node.is_online:
            return await self._fault.write_with_fallback(
                key, value, node_id, self._remote_write
            )

        try:
            self._shard_data[node_id][key] = value
            return True
        except Exception as e:
            logger.warning("Write failed on node %s for key %s: %s", node_id, key, e)
            return await self._fault.write_with_fallback(
                key, value, node_id, self._remote_write
            )

    async def delete_key(self, node_id: str, key: str) -> bool:
        try:
            data = self._shard_data.get(node_id, {})
            if key in data:
                del data[key]
            return True
        except Exception as e:
            logger.error("Delete failed on node %s for key %s: %s", node_id, key, e)
            return False

    async def _remote_read(self, node_id: str, key: str) -> Optional[Any]:
        data = self._shard_data.get(node_id, {}).get(key)
        return data

    async def _remote_write(self, node_id: str, key: str, value: Any) -> bool:
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

                source_data = self._shard_data.get(plan.source_node, {})
                keys_to_migrate = list(source_data.keys())

                batch_size = 100
                for i in range(0, len(keys_to_migrate), batch_size):
                    batch = keys_to_migrate[i : i + batch_size]
                    for key in batch:
                        assigned_node = self._ring.get_node(key)
                        if assigned_node == plan.target_node:
                            value = source_data.get(key)
                            if value is not None:
                                self._shard_data[plan.target_node][key] = value

                    await asyncio.sleep(0)

                self._meta.save_migration_state(migration_id, {
                    "migration_id": migration_id,
                    "source_node": plan.source_node,
                    "target_node": plan.target_node,
                    "status": "completed",
                    "completed_at": time.time(),
                })

                duration = time.monotonic() - start_time
                self._metrics.record_migration(
                    plan.source_node, plan.target_node, duration, True
                )
                logger.info(
                    "Migration %s completed in %.3fs, %d keys processed",
                    migration_id, duration, len(keys_to_migrate),
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
            "load_distribution": distribution,
        }
