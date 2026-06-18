import json
import logging
import os
import threading
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class PersistenceError(Exception):
    pass


class MetadataStore:

    META_KEY_PREFIX = b"meta:"
    NODE_KEY_PREFIX = b"node:"
    SHARD_KEY_PREFIX = b"shard:"
    TOPOLOGY_KEY = b"topology:current"

    def __init__(self, db_path: str = "./data/leveldb/dcache_meta"):
        self._db_path = db_path
        self._db = None
        self._lock = threading.Lock()
        self._write_batch = []

    def open(self) -> None:
        try:
            import plyvel

            os.makedirs(os.path.dirname(self._db_path), exist_ok=True)
            self._db = plyvel.DB(
                self._db_path,
                create_if_missing=True,
                write_buffer_size=4 * 1024 * 1024,
                max_file_size=8 * 1024 * 1024,
            )
            logger.info("LevelDB opened at %s", self._db_path)
        except ImportError:
            logger.warning(
                "plyvel not available, falling back to file-based persistence"
            )
            self._db = None
            self._ensure_fallback_dir()
        except Exception as e:
            logger.error("Failed to open LevelDB: %s", e)
            self._db = None
            self._ensure_fallback_dir()

    def _ensure_fallback_dir(self) -> None:
        fallback_path = os.path.join(
            os.path.dirname(self._db_path), "fallback_json"
        )
        os.makedirs(fallback_path, exist_ok=True)
        self._fallback_path = fallback_path

    def close(self) -> None:
        if self._db is not None:
            try:
                self._db.close()
                logger.info("LevelDB closed")
            except Exception as e:
                logger.error("Error closing LevelDB: %s", e)

    def save_node_info(self, node_id: str, node_data: Dict[str, Any]) -> None:
        key = self.NODE_KEY_PREFIX + node_id.encode()
        value = json.dumps(node_data).encode()
        self._put(key, value)
        logger.debug("Saved node info: %s", node_id)

    def load_node_info(self, node_id: str) -> Optional[Dict[str, Any]]:
        key = self.NODE_KEY_PREFIX + node_id.encode()
        value = self._get(key)
        if value is not None:
            return json.loads(value.decode())
        return None

    def load_all_nodes(self) -> Dict[str, Dict[str, Any]]:
        result = {}
        prefix = self.NODE_KEY_PREFIX
        if self._db is not None:
            try:
                for key, value in self._db.iterator(prefix=prefix):
                    node_id = key[len(prefix):].decode()
                    result[node_id] = json.loads(value.decode())
            except Exception as e:
                logger.error("Failed to load all nodes from LevelDB: %s", e)
        else:
            result = self._fallback_load_prefix("node_")
        return result

    def delete_node_info(self, node_id: str) -> None:
        key = self.NODE_KEY_PREFIX + node_id.encode()
        self._delete(key)
        logger.debug("Deleted node info: %s", node_id)

    def save_topology(self, topology_data: Dict[str, Any]) -> None:
        value = json.dumps(topology_data).encode()
        self._put(self.TOPOLOGY_KEY, value)
        logger.info("Topology saved with %d nodes", len(topology_data.get("nodes", {})))

    def load_topology(self) -> Optional[Dict[str, Any]]:
        value = self._get(self.TOPOLOGY_KEY)
        if value is not None:
            return json.loads(value.decode())
        return None

    def save_shard_meta(self, shard_id: str, meta: Dict[str, Any]) -> None:
        key = self.SHARD_KEY_PREFIX + shard_id.encode()
        value = json.dumps(meta).encode()
        self._put(key, value)

    def load_shard_meta(self, shard_id: str) -> Optional[Dict[str, Any]]:
        key = self.SHARD_KEY_PREFIX + shard_id.encode()
        value = self._get(key)
        if value is not None:
            return json.loads(value.decode())
        return None

    def load_all_shards(self) -> Dict[str, Dict[str, Any]]:
        result = {}
        prefix = self.SHARD_KEY_PREFIX
        if self._db is not None:
            try:
                for key, value in self._db.iterator(prefix=prefix):
                    shard_id = key[len(prefix):].decode()
                    result[shard_id] = json.loads(value.decode())
            except Exception as e:
                logger.error("Failed to load all shards: %s", e)
        else:
            result = self._fallback_load_prefix("shard_")
        return result

    def save_migration_state(self, migration_id: str, state: Dict[str, Any]) -> None:
        key = self.META_KEY_PREFIX + b"migration:" + migration_id.encode()
        value = json.dumps(state).encode()
        self._put(key, value)

    def load_pending_migrations(self) -> List[Dict[str, Any]]:
        result = []
        prefix = self.META_KEY_PREFIX + b"migration:"
        if self._db is not None:
            try:
                for key, value in self._db.iterator(prefix=prefix):
                    data = json.loads(value.decode())
                    if data.get("status") == "pending":
                        result.append(data)
            except Exception as e:
                logger.error("Failed to load pending migrations: %s", e)
        return result

    def _put(self, key: bytes, value: bytes) -> None:
        with self._lock:
            if self._db is not None:
                try:
                    self._db.put(key, value)
                except Exception as e:
                    logger.error("LevelDB put failed: %s", e)
                    self._fallback_put(key, value)
            else:
                self._fallback_put(key, value)

    def _get(self, key: bytes) -> Optional[bytes]:
        with self._lock:
            if self._db is not None:
                try:
                    return self._db.get(key)
                except Exception as e:
                    logger.error("LevelDB get failed: %s", e)
                    return self._fallback_get(key)
            else:
                return self._fallback_get(key)

    def _delete(self, key: bytes) -> None:
        with self._lock:
            if self._db is not None:
                try:
                    self._db.delete(key)
                except Exception as e:
                    logger.error("LevelDB delete failed: %s", e)
            else:
                self._fallback_delete(key)

    def _fallback_put(self, key: bytes, value: bytes) -> None:
        if not hasattr(self, "_fallback_path"):
            self._ensure_fallback_dir()
        filename = key.decode("utf-8", errors="replace").replace(":", "_").replace("/", "_")
        filepath = os.path.join(self._fallback_path, f"{filename}.json")
        try:
            with open(filepath, "wb") as f:
                f.write(value)
        except Exception as e:
            logger.error("Fallback put failed: %s", e)

    def _fallback_get(self, key: bytes) -> Optional[bytes]:
        if not hasattr(self, "_fallback_path"):
            self._ensure_fallback_dir()
        filename = key.decode("utf-8", errors="replace").replace(":", "_").replace("/", "_")
        filepath = os.path.join(self._fallback_path, f"{filename}.json")
        try:
            if os.path.exists(filepath):
                with open(filepath, "rb") as f:
                    return f.read()
        except Exception as e:
            logger.error("Fallback get failed: %s", e)
        return None

    def _fallback_delete(self, key: bytes) -> None:
        if not hasattr(self, "_fallback_path"):
            self._ensure_fallback_dir()
        filename = key.decode("utf-8", errors="replace").replace(":", "_").replace("/", "_")
        filepath = os.path.join(self._fallback_path, f"{filename}.json")
        try:
            if os.path.exists(filepath):
                os.remove(filepath)
        except Exception as e:
            logger.error("Fallback delete failed: %s", e)

    def _fallback_load_prefix(self, prefix: str) -> Dict[str, Dict[str, Any]]:
        result = {}
        if not hasattr(self, "_fallback_path"):
            self._ensure_fallback_dir()
        try:
            for filename in os.listdir(self._fallback_path):
                if filename.startswith(prefix) and filename.endswith(".json"):
                    key_part = filename[len(prefix):-5]
                    filepath = os.path.join(self._fallback_path, filename)
                    with open(filepath, "r") as f:
                        result[key_part] = json.load(f)
        except Exception as e:
            logger.error("Fallback load prefix failed: %s", e)
        return result

    def recover_topology(self) -> Dict[str, Any]:
        logger.info("Starting topology recovery from persistent store")
        topology = self.load_topology()
        if topology is None:
            logger.warning("No topology found in persistent store")
            return {}

        nodes = topology.get("nodes", {})
        logger.info("Recovered topology with %d nodes", len(nodes))
        return topology
