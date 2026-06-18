import bisect
import hashlib
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class BusinessError(Exception):
    pass


class NodeNotFoundError(BusinessError):
    pass


class DuplicateNodeError(BusinessError):
    pass


@dataclass
class VirtualNode:
    node_id: str
    vnode_hash: int
    weight: int


@dataclass
class NodeInfo:
    node_id: str
    host: str
    port: int
    weight: int = 1
    is_online: bool = True
    replica_of: Optional[str] = None


@dataclass
class MigrationPlan:
    source_node: str
    target_node: str
    key_ranges: List[Tuple[int, int]]
    estimated_keys: int = 0


class ConsistentHashRing:

    def __init__(self, vnodes_per_weight: int = 150, replication_factor: int = 2):
        self._vnodes_per_weight = vnodes_per_weight
        self._replication_factor = replication_factor
        self._ring_keys: List[int] = []
        self._ring_map: Dict[int, VirtualNode] = {}
        self._nodes: Dict[str, NodeInfo] = {}
        self._node_vnodes: Dict[str, List[int]] = {}
        self._hash_func = self._default_hash

    @staticmethod
    def _default_hash(key: str) -> int:
        digest = hashlib.md5(key.encode()).hexdigest()
        return int(digest[:8], 16)

    def _generate_vnode_hashes(self, node_id: str, weight: int) -> List[int]:
        hashes = []
        for i in range(self._vnodes_per_weight * weight):
            vnode_key = f"{node_id}#vn{i}"
            h = self._hash_func(vnode_key)
            hashes.append(h)
        return hashes

    def add_node(self, node_info: NodeInfo) -> List[MigrationPlan]:
        if node_info.node_id in self._nodes:
            raise DuplicateNodeError(f"Node {node_info.node_id} already exists")

        node_info.is_online = True
        self._nodes[node_info.node_id] = node_info

        vnode_hashes = self._generate_vnode_hashes(node_info.node_id, node_info.weight)
        self._node_vnodes[node_info.node_id] = []

        migration_plans = []
        for vh in vnode_hashes:
            if vh in self._ring_map:
                vh = self._find_unique_hash(vh)
            bisect.insort(self._ring_keys, vh)
            self._ring_map[vh] = VirtualNode(
                node_id=node_info.node_id, vnode_hash=vh, weight=node_info.weight
            )
            self._node_vnodes[node_info.node_id].append(vh)

            plan = self._compute_migration_for_insert(vh, node_info.node_id)
            if plan:
                migration_plans.append(plan)

        logger.info(
            "Node %s added with weight=%d, %d vnodes, %d migrations",
            node_info.node_id, node_info.weight, len(vnode_hashes),
            len(migration_plans),
        )
        return migration_plans

    def _find_unique_hash(self, h: int) -> int:
        offset = 1
        while h + offset in self._ring_map:
            offset += 1
        return h + offset

    def _compute_migration_for_insert(
        self, new_hash: int, new_node_id: str
    ) -> Optional[MigrationPlan]:
        if len(self._ring_keys) <= 1:
            return None

        idx = bisect.bisect_left(self._ring_keys, new_hash)
        if idx == 0:
            prev_idx = len(self._ring_keys) - 1
        else:
            prev_idx = idx - 1

        prev_hash = self._ring_keys[prev_idx]
        prev_vnode = self._ring_map.get(prev_hash)
        if prev_vnode is None or prev_vnode.node_id == new_node_id:
            return None

        successor_idx = (idx + 1) % len(self._ring_keys)
        successor_hash = self._ring_keys[successor_idx]

        return MigrationPlan(
            source_node=prev_vnode.node_id,
            target_node=new_node_id,
            key_ranges=[(prev_hash, new_hash)],
            estimated_keys=new_hash - prev_hash if new_hash > prev_hash else (2**32 + new_hash - prev_hash),
        )

    def remove_node(self, node_id: str) -> List[MigrationPlan]:
        if node_id not in self._nodes:
            raise NodeNotFoundError(f"Node {node_id} not found")

        migration_plans = []
        vnode_hashes = self._node_vnodes.get(node_id, [])

        for vh in vnode_hashes:
            idx = bisect.bisect_left(self._ring_keys, vh)
            if idx < len(self._ring_keys) and self._ring_keys[idx] == vh:
                successor_idx = (idx + 1) % len(self._ring_keys)
                successor_hash = self._ring_keys[successor_idx]
                successor_vnode = self._ring_map.get(successor_hash)

                if successor_vnode and successor_vnode.node_id != node_id:
                    migration_plans.append(
                        MigrationPlan(
                            source_node=node_id,
                            target_node=successor_vnode.node_id,
                            key_ranges=[(vh, successor_hash)],
                            estimated_keys=0,
                        )
                    )

                self._ring_keys.pop(idx)
                del self._ring_map[vh]

        del self._nodes[node_id]
        del self._node_vnodes[node_id]

        logger.info(
            "Node %s removed, %d migrations planned", node_id, len(migration_plans)
        )
        return migration_plans

    def get_node(self, key: str) -> Optional[str]:
        if not self._ring_keys:
            return None
        h = self._hash_func(key)
        idx = bisect.bisect_right(self._ring_keys, h)
        if idx == len(self._ring_keys):
            idx = 0
        vnode = self._ring_map[self._ring_keys[idx]]
        return vnode.node_id

    def get_nodes_for_key(self, key: str) -> List[str]:
        if not self._ring_keys:
            return []
        h = self._hash_func(key)
        idx = bisect.bisect_right(self._ring_keys, h)
        if idx == len(self._ring_keys):
            idx = 0

        result = []
        seen = set()
        for i in range(len(self._ring_keys)):
            ridx = (idx + i) % len(self._ring_keys)
            vnode = self._ring_map[self._ring_keys[ridx]]
            if vnode.node_id not in seen:
                seen.add(vnode.node_id)
                result.append(vnode.node_id)
                if len(result) >= self._replication_factor:
                    break
        return result

    def get_all_nodes(self) -> Dict[str, NodeInfo]:
        return dict(self._nodes)

    def get_online_nodes(self) -> Dict[str, NodeInfo]:
        return {nid: n for nid, n in self._nodes.items() if n.is_online}

    def set_node_offline(self, node_id: str) -> None:
        if node_id in self._nodes:
            self._nodes[node_id].is_online = False
            logger.warning("Node %s marked offline", node_id)

    def set_node_online(self, node_id: str) -> None:
        if node_id in self._nodes:
            self._nodes[node_id].is_online = True
            logger.info("Node %s marked online", node_id)

    def compute_load_distribution(self) -> Dict[str, float]:
        if not self._ring_keys or not self._nodes:
            return {}

        total_space = 2**32
        node_ranges: Dict[str, int] = {nid: 0 for nid in self._nodes}

        for i in range(len(self._ring_keys)):
            cur = self._ring_keys[i]
            prev = self._ring_keys[i - 1] if i > 0 else self._ring_keys[-1]
            if i == 0:
                prev = self._ring_keys[-1]
                rng = (2**32 - prev) + cur
            else:
                rng = cur - prev

            vnode = self._ring_map[cur]
            node_ranges[vnode.node_id] = node_ranges.get(vnode.node_id, 0) + rng

        return {nid: rng / total_space for nid, rng in node_ranges.items()}

    def compute_rebalance_plan(
        self, skew_threshold: float = 0.3
    ) -> List[MigrationPlan]:
        distribution = self.compute_load_distribution()
        if not distribution:
            return []

        online_nodes = {nid: n for nid, n in self._nodes.items() if n.is_online}
        if len(online_nodes) < 2:
            return []

        n_online = len(online_nodes)
        total_weight = sum(n.weight for n in online_nodes.values())
        plans = []

        for nid, node in online_nodes.items():
            expected_share = node.weight / total_weight
            actual_share = distribution.get(nid, 0.0)
            if actual_share == 0:
                continue
            deviation = (actual_share - expected_share) / expected_share

            if deviation > skew_threshold:
                overloaded_vnodes = sorted(
                    self._node_vnodes.get(nid, []),
                    key=lambda h: self._ring_map[h].vnode_hash,
                )
                vnodes_to_move = max(1, int(len(overloaded_vnodes) * (deviation - skew_threshold) / (1 + deviation)))

                underloaded = [
                    (n_id, n.weight / total_weight - distribution.get(n_id, 0.0))
                    for n_id, n in online_nodes.items()
                    if (n.weight / total_weight - distribution.get(n_id, 0.0)) > 0.001
                ]
                underloaded.sort(key=lambda x: -x[1])

                moved = 0
                for vh in overloaded_vnodes:
                    if moved >= vnodes_to_move or not underloaded:
                        break
                    target_nid = underloaded[0][0]

                    plan = MigrationPlan(
                        source_node=nid,
                        target_node=target_nid,
                        key_ranges=[],
                        estimated_keys=1,
                    )
                    plans.append(plan)
                    moved += 1

                    underloaded[0] = (underloaded[0][0], underloaded[0][1] - 0.01)
                    if underloaded[0][1] <= 0.001:
                        underloaded.pop(0)

        if plans:
            logger.info("Rebalance plan: %d migrations to fix skew", len(plans))
        return plans

    def get_node_count(self) -> int:
        return len(self._nodes)

    def get_vnode_count(self) -> int:
        return len(self._ring_keys)

    def get_replication_factor(self) -> int:
        return self._replication_factor
