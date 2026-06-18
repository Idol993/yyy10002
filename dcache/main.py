import asyncio
import logging
import os
import signal
import sys
from typing import Optional

from aiohttp import web

from .algorithm.consistent_hash import ConsistentHashRing
from .persistence.metadata_store import MetadataStore
from .fault_tolerance.manager import FaultToleranceManager
from .monitor.metrics import MetricsCollector
from .service.cache_service import CacheService
from .node_manager.manager import NodeManager

logger = logging.getLogger(__name__)


def setup_logging(level: str = "INFO") -> None:
    log_format = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    log_level = getattr(logging, level.upper(), logging.INFO)

    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=[
            logging.StreamHandler(sys.stdout),
        ],
    )

    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("plyvel").setLevel(logging.WARNING)


class DistributedCacheServer:

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8080,
        db_path: str = "./data/leveldb/dcache_meta",
        vnodes_per_weight: int = 150,
        replication_factor: int = 2,
        rate_limit: int = 1000,
        rebalance_interval: float = 60.0,
        skew_threshold: float = 0.3,
        migration_concurrency: int = 4,
        log_level: str = "INFO",
    ):
        self._host = host
        self._port = port

        setup_logging(log_level)

        self._ring = ConsistentHashRing(
            vnodes_per_weight=vnodes_per_weight,
            replication_factor=replication_factor,
        )
        self._meta = MetadataStore(db_path=db_path)
        self._metrics = MetricsCollector()

        self._fault = FaultToleranceManager(
            get_replicas_fn=self._ring.get_nodes_for_key,
            alert_callbacks=[self._default_alert_callback],
        )

        self._node_mgr = NodeManager(
            hash_ring=self._ring,
            meta_store=self._meta,
            fault_manager=self._fault,
            metrics=self._metrics,
            migration_concurrency=migration_concurrency,
            rebalance_interval=rebalance_interval,
            skew_threshold=skew_threshold,
        )

        self._cache_svc = CacheService(
            get_node_fn=self._node_mgr.get_node,
            get_nodes_fn=self._node_mgr.get_nodes_for_key,
            read_fn=self._node_mgr.read_key,
            write_fn=self._node_mgr.write_key,
            delete_fn=self._node_mgr.delete_key,
            node_add_fn=self._node_mgr.add_node,
            node_remove_fn=self._node_mgr.remove_node,
            metrics=self._metrics,
            rate_limit=rate_limit,
        )

        self._app: Optional[web.Application] = None
        self._runner: Optional[web.AppRunner] = None

    async def _default_alert_callback(self, event) -> None:
        logger.error(
            "ALERT: node=%s level=%s error=%s",
            event.node_id, event.fault_level.value, event.error_message,
        )

    async def start(self) -> None:
        logger.info("Starting DistributedCacheServer on %s:%d", self._host, self._port)

        self._meta.open()

        await self._node_mgr.recover_from_persistence()

        await self._node_mgr.start()

        self._app = self._cache_svc.create_app()
        self._app.router.add_get("/cluster/status", self._handle_cluster_status)
        self._app.router.add_post("/cluster/heartbeat/{node_id}", self._handle_heartbeat)

        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self._host, self._port)
        await site.start()

        self._actual_port = self._port
        if self._port == 0:
            for s in getattr(self._runner, "sites", []) or []:
                try:
                    server = getattr(s, "_server", None) or s
                    if hasattr(server, "sockets") and server.sockets:
                        self._actual_port = server.sockets[0].getsockname()[1]
                        break
                except Exception:
                    pass

        logger.info("Server started on %s:%d", self._host, self._actual_port)

    @property
    def actual_port(self) -> int:
        return getattr(self, "_actual_port", self._port)

    async def stop(self) -> None:
        logger.info("Stopping DistributedCacheServer")
        await self._node_mgr.stop()
        if self._runner:
            await self._runner.cleanup()
        self._meta.close()
        logger.info("Server stopped")

    async def _handle_cluster_status(self, request: web.Request) -> web.Response:
        status = self._node_mgr.get_cluster_status()
        return web.json_response(status)

    async def _handle_heartbeat(self, request: web.Request) -> web.Response:
        node_id = request.match_info.get("node_id", "")
        self._node_mgr.update_heartbeat(node_id)
        return web.json_response({"status": "ok"})


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Distributed Cache Shard Scheduler")
    parser.add_argument("--host", default="0.0.0.0", help="Bind host")
    parser.add_argument("--port", type=int, default=8080, help="Bind port")
    parser.add_argument("--db-path", default="./data/leveldb/dcache_meta", help="LevelDB path")
    parser.add_argument("--vnodes-per-weight", type=int, default=150, help="Virtual nodes per weight unit")
    parser.add_argument("--replication-factor", type=int, default=2, help="Replication factor")
    parser.add_argument("--rate-limit", type=int, default=1000, help="Max requests per second")
    parser.add_argument("--rebalance-interval", type=float, default=60.0, help="Rebalance interval in seconds")
    parser.add_argument("--skew-threshold", type=float, default=0.3, help="Load skew threshold for rebalance")
    parser.add_argument("--migration-concurrency", type=int, default=4, help="Max concurrent migrations")
    parser.add_argument("--log-level", default="INFO", help="Log level")

    args = parser.parse_args()

    server = DistributedCacheServer(
        host=args.host,
        port=args.port,
        db_path=args.db_path,
        vnodes_per_weight=args.vnodes_per_weight,
        replication_factor=args.replication_factor,
        rate_limit=args.rate_limit,
        rebalance_interval=args.rebalance_interval,
        skew_threshold=args.skew_threshold,
        migration_concurrency=args.migration_concurrency,
        log_level=args.log_level,
    )

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def run() -> None:
        await server.start()
        try:
            stop_event = asyncio.Event()
            loop.add_signal_handler(signal.SIGINT, stop_event.set)
            loop.add_signal_handler(signal.SIGTERM, stop_event.set)
            await stop_event.wait()
        except KeyboardInterrupt:
            pass
        finally:
            await server.stop()

    try:
        loop.run_until_complete(run())
    finally:
        loop.close()


if __name__ == "__main__":
    main()
