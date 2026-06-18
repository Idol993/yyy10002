import asyncio
import json
import logging
import random
import string
import time
import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional

try:
    from aiohttp import ClientSession, TCPConnector
except ImportError:
    print("aiohttp is required: pip install aiohttp")
    raise SystemExit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("stress_test")


@dataclass
class TestResult:
    total_requests: int = 0
    successful: int = 0
    failed: int = 0
    total_duration: float = 0.0
    latencies: List[float] = field(default_factory=list)
    errors: Dict[str, int] = field(default_factory=dict)

    def add_latency(self, lat: float) -> None:
        self.latencies.append(lat)

    def add_error(self, err: str) -> None:
        self.errors[err] = self.errors.get(err, 0) + 1

    @property
    def rps(self) -> float:
        if self.total_duration <= 0:
            return 0.0
        return self.total_requests / self.total_duration

    @property
    def avg_latency_ms(self) -> float:
        if not self.latencies:
            return 0.0
        return (sum(self.latencies) / len(self.latencies)) * 1000

    @property
    def p99_latency_ms(self) -> float:
        if not self.latencies:
            return 0.0
        s = sorted(self.latencies)
        idx = int(len(s) * 0.99)
        return s[min(idx, len(s) - 1)] * 1000

    def summary(self) -> str:
        return (
            f"Results: {self.total_requests} req, "
            f"{self.successful} ok, {self.failed} fail, "
            f"RPS={self.rps:.1f}, "
            f"avg={self.avg_latency_ms:.2f}ms, "
            f"p99={self.p99_latency_ms:.2f}ms"
        )


class StressTest:

    def __init__(
        self,
        base_url: str = "http://127.0.0.1:8080",
        num_keys: int = 5000,
        num_concurrent: int = 50,
        write_ratio: float = 0.3,
        duration_seconds: float = 30.0,
    ):
        self._base_url = base_url.rstrip("/")
        self._num_keys = num_keys
        self._num_concurrent = num_concurrent
        self._write_ratio = write_ratio
        self._duration = duration_seconds
        self._keys = [f"key_{i}" for i in range(num_keys)]

    async def _put(self, session: ClientSession, key: str) -> bool:
        value = "".join(random.choices(string.ascii_letters + string.digits, k=32))
        request_id = str(uuid.uuid4())
        start = time.monotonic()
        try:
            async with session.put(
                f"{self._base_url}/cache/{key}",
                json={"value": value},
                headers={"X-Request-Id": request_id},
            ) as resp:
                elapsed = time.monotonic() - start
                if resp.status == 200:
                    return True
                return False
        except Exception as e:
            return False

    async def _get(self, session: ClientSession, key: str) -> bool:
        start = time.monotonic()
        try:
            async with session.get(
                f"{self._base_url}/cache/{key}",
            ) as resp:
                elapsed = time.monotonic() - start
                if resp.status in (200, 404):
                    return True
                return False
        except Exception:
            return False

    async def _delete(self, session: ClientSession, key: str) -> bool:
        try:
            async with session.delete(
                f"{self._base_url}/cache/{key}",
            ) as resp:
                return resp.status == 200
        except Exception:
            return False

    async def _worker(
        self,
        session: ClientSession,
        result: TestResult,
        stop_event: asyncio.Event,
    ) -> None:
        while not stop_event.is_set():
            key = random.choice(self._keys)
            start = time.monotonic()

            if random.random() < self._write_ratio:
                ok = await self._put(session, key)
            else:
                ok = await self._get(session, key)

            elapsed = time.monotonic() - start
            result.total_requests += 1
            result.add_latency(elapsed)

            if ok:
                result.successful += 1
            else:
                result.failed += 1

    async def run_read_write_mix(self) -> TestResult:
        logger.info(
            "Starting read/write mix test: keys=%d concurrent=%d write_ratio=%.1f duration=%ds",
            self._num_keys, self._num_concurrent, self._write_ratio, self._duration,
        )
        result = TestResult()
        stop_event = asyncio.Event()

        connector = TCPConnector(limit=self._num_concurrent + 10)
        async with ClientSession(connector=connector) as session:
            workers = [
                asyncio.create_task(self._worker(session, result, stop_event))
                for _ in range(self._num_concurrent)
            ]

            start = time.monotonic()
            await asyncio.sleep(self._duration)
            stop_event.set()

            await asyncio.gather(*workers, return_exceptions=True)
            result.total_duration = time.monotonic() - start

        logger.info("Read/write mix test: %s", result.summary())
        return result

    async def run_node_churn(self, node_configs: List[Dict]) -> TestResult:
        logger.info("Starting node churn test with %d node configs", len(node_configs))
        result = TestResult()

        connector = TCPConnector(limit=100)
        async with ClientSession(connector=connector) as session:
            phase_results = []
            for i, cfg in enumerate(node_configs):
                try:
                    async with session.post(
                        f"{self._base_url}/cluster/nodes", json=cfg,
                    ) as resp:
                        if resp.status == 200:
                            result.successful += 1
                        else:
                            result.failed += 1
                        result.total_requests += 1
                except Exception as e:
                    result.failed += 1
                    result.add_error(str(e))
                    result.total_requests += 1

                await asyncio.sleep(0.5)

                for _ in range(100):
                    key = random.choice(self._keys)
                    start = time.monotonic()
                    try:
                        if random.random() < 0.5:
                            ok = await self._put(session, key)
                        else:
                            ok = await self._get(session, key)
                        elapsed = time.monotonic() - start
                        result.add_latency(elapsed)
                        result.total_requests += 1
                        if ok:
                            result.successful += 1
                        else:
                            result.failed += 1
                    except Exception as e:
                        result.add_error(str(e))
                        result.total_requests += 1
                        result.failed += 1

            for i, cfg in enumerate(node_configs):
                node_id = cfg["node_id"]
                try:
                    async with session.delete(
                        f"{self._base_url}/cluster/nodes/{node_id}",
                    ) as resp:
                        result.total_requests += 1
                        if resp.status == 200:
                            result.successful += 1
                        else:
                            result.failed += 1
                except Exception:
                    result.failed += 1
                    result.total_requests += 1

                await asyncio.sleep(0.3)

        result.total_duration = 1.0
        logger.info("Node churn test: %s", result.summary())
        return result

    async def run_concurrent_migrations(self) -> TestResult:
        logger.info("Starting concurrent migration test")
        result = TestResult()

        connector = TCPConnector(limit=100)
        async with ClientSession(connector=connector) as session:
            for batch in range(3):
                nodes = []
                for i in range(5):
                    node_id = f"migration_node_{batch}_{i}"
                    cfg = {
                        "node_id": node_id,
                        "host": "127.0.0.1",
                        "port": 9000 + batch * 10 + i,
                        "weight": random.randint(1, 3),
                    }
                    try:
                        async with session.post(
                            f"{self._base_url}/cluster/nodes", json=cfg,
                        ) as resp:
                            result.total_requests += 1
                            if resp.status == 200:
                                result.successful += 1
                                nodes.append(node_id)
                            else:
                                result.failed += 1
                    except Exception:
                        result.failed += 1
                        result.total_requests += 1

                await asyncio.sleep(1.0)

                for _ in range(200):
                    key = f"mgkey_{random.randint(0, 5000)}"
                    start = time.monotonic()
                    try:
                        if random.random() < 0.4:
                            ok = await self._put(session, key)
                        else:
                            ok = await self._get(session, key)
                        elapsed = time.monotonic() - start
                        result.add_latency(elapsed)
                        result.total_requests += 1
                        if ok:
                            result.successful += 1
                        else:
                            result.failed += 1
                    except Exception:
                        result.total_requests += 1
                        result.failed += 1

                for nid in nodes:
                    try:
                        async with session.delete(
                            f"{self._base_url}/cluster/nodes/{nid}",
                        ) as resp:
                            result.total_requests += 1
                            if resp.status == 200:
                                result.successful += 1
                            else:
                                result.failed += 1
                    except Exception:
                        result.failed += 1
                        result.total_requests += 1

                await asyncio.sleep(0.5)

        result.total_duration = 1.0
        logger.info("Concurrent migration test: %s", result.summary())
        return result

    async def run_all(self) -> None:
        logger.info("=" * 60)
        logger.info("STRESS TEST SUITE STARTING")
        logger.info("=" * 60)

        try:
            connector = TCPConnector(limit=10)
            async with ClientSession(connector=connector) as session:
                async with session.get(f"{self._base_url}/health") as resp:
                    if resp.status != 200:
                        logger.error("Server not healthy, aborting")
                        return
        except Exception as e:
            logger.error("Cannot connect to server: %s", e)
            return

        seed_nodes = [
            {"node_id": "seed_0", "host": "127.0.0.1", "port": 9000, "weight": 2},
            {"node_id": "seed_1", "host": "127.0.0.1", "port": 9001, "weight": 1},
            {"node_id": "seed_2", "host": "127.0.0.1", "port": 9002, "weight": 3},
        ]
        connector = TCPConnector(limit=50)
        async with ClientSession(connector=connector) as session:
            for cfg in seed_nodes:
                try:
                    async with session.post(
                        f"{self._base_url}/cluster/nodes", json=cfg,
                    ) as resp:
                        logger.info("Added seed node %s: status=%d", cfg["node_id"], resp.status)
                except Exception as e:
                    logger.warning("Failed to add seed node: %s", e)

        await asyncio.sleep(1.0)

        rw_result = await self.run_read_write_mix()
        logger.info("READ/WRITE MIX: %s", rw_result.summary())

        churn_nodes = [
            {"node_id": f"churn_{i}", "host": "127.0.0.1", "port": 9100 + i, "weight": random.randint(1, 3)}
            for i in range(5)
        ]
        churn_result = await self.run_node_churn(churn_nodes)
        logger.info("NODE CHURN: %s", churn_result.summary())

        mig_result = await self.run_concurrent_migrations()
        logger.info("CONCURRENT MIGRATIONS: %s", mig_result.summary())

        logger.info("=" * 60)
        logger.info("STRESS TEST SUITE COMPLETE")
        logger.info("=" * 60)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Distributed Cache Stress Test")
    parser.add_argument("--url", default="http://127.0.0.1:8080", help="Server URL")
    parser.add_argument("--keys", type=int, default=5000, help="Number of keys")
    parser.add_argument("--concurrent", type=int, default=50, help="Concurrent workers")
    parser.add_argument("--write-ratio", type=float, default=0.3, help="Write ratio")
    parser.add_argument("--duration", type=float, default=30.0, help="Test duration in seconds")

    args = parser.parse_args()

    test = StressTest(
        base_url=args.url,
        num_keys=args.keys,
        num_concurrent=args.concurrent,
        write_ratio=args.write_ratio,
        duration_seconds=args.duration,
    )

    asyncio.run(test.run_all())


if __name__ == "__main__":
    main()
