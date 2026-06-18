import asyncio
import logging
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Coroutine, Dict, List, Optional

logger = logging.getLogger(__name__)


class FaultLevel(Enum):
    LOCAL_CACHE_DEGRADE = 1
    REPLICA_TAKEOVER = 2
    ALERT_CALLBACK = 3


@dataclass
class FaultEvent:
    node_id: str
    fault_level: FaultLevel
    timestamp: float
    error_message: str
    recovered: bool = False


class LocalCache:

    def __init__(self, max_size: int = 10000, ttl_seconds: float = 300.0):
        self._max_size = max_size
        self._ttl = ttl_seconds
        self._store: OrderedDict[str, tuple] = OrderedDict()
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Optional[Any]:
        async with self._lock:
            if key in self._store:
                value, ts = self._store[key]
                if time.monotonic() - ts < self._ttl:
                    self._store.move_to_end(key)
                    return value
                else:
                    del self._store[key]
            return None

    async def put(self, key: str, value: Any) -> None:
        async with self._lock:
            if key in self._store:
                self._store.move_to_end(key)
            self._store[key] = (value, time.monotonic())
            while len(self._store) > self._max_size:
                self._store.popitem(last=False)

    async def delete(self, key: str) -> None:
        async with self._lock:
            self._store.pop(key, None)

    async def clear(self) -> None:
        async with self._lock:
            self._store.clear()

    def size(self) -> int:
        return len(self._store)


class ReplicaManager:

    def __init__(self, get_replicas_fn: Callable[[str], List[str]]):
        self._get_replicas_fn = get_replicas_fn
        self._failed_nodes: Dict[str, float] = {}
        self._recovery_timeout = 30.0

    def mark_node_failed(self, node_id: str) -> None:
        self._failed_nodes[node_id] = time.monotonic()
        logger.warning("Node %s marked as failed for replica takeover", node_id)

    def mark_node_recovered(self, node_id: str) -> None:
        self._failed_nodes.pop(node_id, None)
        logger.info("Node %s recovered, removed from failed list", node_id)

    def is_node_failed(self, node_id: str) -> bool:
        failed_at = self._failed_nodes.get(node_id)
        if failed_at is None:
            return False
        if time.monotonic() - failed_at > self._recovery_timeout:
            self._failed_nodes.pop(node_id, None)
            return False
        return True

    def get_available_replica(self, key: str, primary_node: str) -> Optional[str]:
        if not self.is_node_failed(primary_node):
            return primary_node

        replicas = self._get_replicas_fn(key)
        for replica_node in replicas:
            if replica_node != primary_node and not self.is_node_failed(replica_node):
                logger.info(
                    "Replica takeover: key=%s primary=%s -> replica=%s",
                    key, primary_node, replica_node,
                )
                return replica_node

        logger.error(
            "No available replica for key=%s, primary=%s", key, primary_node
        )
        return None

    def get_failed_nodes(self) -> List[str]:
        return list(self._failed_nodes.keys())


AlertCallback = Callable[[FaultEvent], Coroutine[Any, Any, None]]


class FaultToleranceManager:

    def __init__(
        self,
        get_replicas_fn: Callable[[str], List[str]],
        alert_callbacks: Optional[List[AlertCallback]] = None,
        local_cache_size: int = 10000,
        local_cache_ttl: float = 300.0,
    ):
        self._local_cache = LocalCache(max_size=local_cache_size, ttl_seconds=local_cache_ttl)
        self._replica_manager = ReplicaManager(get_replicas_fn)
        self._alert_callbacks: List[AlertCallback] = alert_callbacks or []
        self._fault_history: List[FaultEvent] = []
        self._max_history = 1000

    @property
    def local_cache(self) -> LocalCache:
        return self._local_cache

    @property
    def replica_manager(self) -> ReplicaManager:
        return self._replica_manager

    def add_alert_callback(self, callback: AlertCallback) -> None:
        self._alert_callbacks.append(callback)

    async def handle_node_failure(self, node_id: str, error_message: str) -> None:
        logger.warning("Handling failure for node %s: %s", node_id, error_message)

        self._replica_manager.mark_node_failed(node_id)

        event = FaultEvent(
            node_id=node_id,
            fault_level=FaultLevel.ALERT_CALLBACK,
            timestamp=time.time(),
            error_message=error_message,
        )
        self._record_fault(event)

        for callback in self._alert_callbacks:
            try:
                await callback(event)
            except Exception as e:
                logger.error("Alert callback failed: %s", e)

    async def handle_node_recovery(self, node_id: str) -> None:
        logger.info("Node %s recovered", node_id)
        self._replica_manager.mark_node_recovered(node_id)

    async def read_with_fallback(
        self,
        key: str,
        primary_node: str,
        remote_read_fn: Callable[[str, str], Coroutine[Any, Any, Optional[Any]]],
    ) -> Optional[Any]:
        try:
            result = await remote_read_fn(primary_node, key)
            if result is not None:
                return result
        except Exception as e:
            logger.warning(
                "Primary read failed for key=%s on node=%s: %s", key, primary_node, e
            )
            await self.handle_node_failure(
                primary_node, f"Read error: {e}"
            )

        replica_node = self._replica_manager.get_available_replica(key, primary_node)
        if replica_node is not None:
            try:
                result = await remote_read_fn(replica_node, key)
                if result is not None:
                    return result
            except Exception as e:
                logger.warning(
                    "Replica read failed for key=%s on node=%s: %s",
                    key, replica_node, e,
                )

        cached = await self._local_cache.get(key)
        if cached is not None:
            logger.info("Local cache degradation hit for key=%s", key)
            return cached

        logger.error("All fallbacks exhausted for key=%s", key)
        return None

    async def write_with_fallback(
        self,
        key: str,
        value: Any,
        primary_node: str,
        remote_write_fn: Callable[[str, str, Any], Coroutine[Any, Any, bool]],
    ) -> bool:
        try:
            result = await remote_write_fn(primary_node, key, value)
            if result:
                await self._local_cache.put(key, value)
                return True
        except Exception as e:
            logger.warning(
                "Primary write failed for key=%s on node=%s: %s",
                key, primary_node, e,
            )
            await self.handle_node_failure(
                primary_node, f"Write error: {e}"
            )

        replica_node = self._replica_manager.get_available_replica(key, primary_node)
        if replica_node is not None:
            try:
                result = await remote_write_fn(replica_node, key, value)
                if result:
                    await self._local_cache.put(key, value)
                    return True
            except Exception as e:
                logger.warning(
                    "Replica write failed for key=%s on node=%s: %s",
                    key, replica_node, e,
                )

        await self._local_cache.put(key, value)
        logger.warning(
            "Write degraded to local cache only for key=%s", key
        )
        return False

    def _record_fault(self, event: FaultEvent) -> None:
        self._fault_history.append(event)
        if len(self._fault_history) > self._max_history:
            self._fault_history = self._fault_history[-self._max_history:]

    def get_fault_history(self, limit: int = 100) -> List[FaultEvent]:
        return self._fault_history[-limit:]

    def get_fault_count(self, node_id: Optional[str] = None) -> int:
        if node_id:
            return sum(1 for e in self._fault_history if e.node_id == node_id)
        return len(self._fault_history)
