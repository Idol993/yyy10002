import logging
import time
from typing import Dict, Optional, List, Tuple

try:
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry, generate_latest
    HAS_PROMETHEUS = True
except ImportError:
    HAS_PROMETHEUS = False

    class CollectorRegistry:
        def register(self, *args, **kwargs): pass

    class _MockMetric:
        _mock_marker = True

        def __init__(self, *args, **kwargs):
            self._name = kwargs.get("name", args[0] if args else "mock")
            self._labelnames = kwargs.get("labelnames", [])
            self._values = {}
            self._default = 0

        def labels(self, *args, **kwargs):
            key = tuple(args) if args else tuple(kwargs.get(k, "") for k in self._labelnames)
            return self._Child(self, key)

        def inc(self, amount=1):
            self._default += amount

        def set(self, value):
            self._default = value

        def observe(self, value):
            self._default = value

        class _Child:
            def __init__(self, parent, key):
                self._parent = parent
                self._key = key
                if key not in parent._values:
                    parent._values[key] = 0

            def inc(self, amount=1):
                self._parent._values[self._key] += amount

            def set(self, value):
                self._parent._values[self._key] = value

            def observe(self, value):
                self._parent._values[self._key] = value

    _mock_metric_names = [
        "dcache_migration_duration_seconds", "dcache_migration_total",
        "dcache_node_hit_total", "dcache_node_miss_total",
        "dcache_node_hit_rate", "dcache_fault_total",
        "dcache_request_duration_seconds", "dcache_active_nodes",
        "dcache_total_vnodes", "dcache_shard_count",
        "dcache_local_cache_size", "dcache_rebalance_runs_total",
    ]

    def Counter(*args, **kwargs): return _MockMetric(*args, **kwargs)
    def Gauge(*args, **kwargs): return _MockMetric(*args, **kwargs)
    def Histogram(*args, **kwargs): return _MockMetric(*args, **kwargs)

    def generate_latest(registry=None):
        lines = []
        for name in _mock_metric_names:
            lines.append(f"# HELP {name} Mock metric")
            lines.append(f"# TYPE {name} gauge")
            lines.append(f"{name} 0")
        return ("\n".join(lines) + "\n").encode("utf-8")

logger = logging.getLogger(__name__)


class MetricsCollector:

    def __init__(self, registry: Optional[CollectorRegistry] = None):
        self._registry = registry or CollectorRegistry()

        self.migration_duration = Histogram(
            "dcache_migration_duration_seconds",
            "Time spent on shard migration",
            ["source_node", "target_node"],
            buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
            registry=self._registry,
        )

        self.migration_total = Counter(
            "dcache_migration_total",
            "Total number of shard migrations",
            ["status"],
            registry=self._registry,
        )

        self.node_hit_total = Counter(
            "dcache_node_hit_total",
            "Number of cache hits per node",
            ["node_id"],
            registry=self._registry,
        )

        self.node_miss_total = Counter(
            "dcache_node_miss_total",
            "Number of cache misses per node",
            ["node_id"],
            registry=self._registry,
        )

        self.node_hit_rate = Gauge(
            "dcache_node_hit_rate",
            "Current hit rate per node (0-1)",
            ["node_id"],
            registry=self._registry,
        )

        self.fault_total = Counter(
            "dcache_fault_total",
            "Number of faults",
            ["node_id", "fault_level"],
            registry=self._registry,
        )

        self.request_duration = Histogram(
            "dcache_request_duration_seconds",
            "Request latency distribution",
            ["method", "endpoint"],
            buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
            registry=self._registry,
        )

        self.request_total = Counter(
            "dcache_request_total",
            "Total requests",
            ["method", "endpoint", "status"],
            registry=self._registry,
        )

        self.active_nodes = Gauge(
            "dcache_active_nodes",
            "Number of active nodes",
            registry=self._registry,
        )

        self.total_vnodes = Gauge(
            "dcache_total_vnodes",
            "Total virtual nodes",
            registry=self._registry,
        )

        self.shard_count = Gauge(
            "dcache_shard_count",
            "Number of shards",
            registry=self._registry,
        )

        self.local_cache_size = Gauge(
            "dcache_local_cache_size",
            "Local fallback cache size",
            registry=self._registry,
        )

        self.rebalance_runs = Counter(
            "dcache_rebalance_runs_total",
            "Number of rebalance task runs",
            registry=self._registry,
        )

        self._hit_counts: Dict[str, int] = {}
        self._miss_counts: Dict[str, int] = {}

    def record_migration(
        self, source_node: str, target_node: str, duration: float, success: bool = True
    ) -> None:
        self.migration_duration.labels(
            source_node=source_node, target_node=target_node
        ).observe(duration)
        status = "success" if success else "failure"
        self.migration_total.labels(status=status).inc()
        logger.debug(
            "Migration recorded: %s->%s duration=%.3fs success=%s",
            source_node, target_node, duration, success,
        )

    def record_hit(self, node_id: str) -> None:
        self.node_hit_total.labels(node_id=node_id).inc()
        self._hit_counts[node_id] = self._hit_counts.get(node_id, 0) + 1
        self._update_hit_rate(node_id)

    def record_miss(self, node_id: str) -> None:
        self.node_miss_total.labels(node_id=node_id).inc()
        self._miss_counts[node_id] = self._miss_counts.get(node_id, 0) + 1
        self._update_hit_rate(node_id)

    def _update_hit_rate(self, node_id: str) -> None:
        hits = self._hit_counts.get(node_id, 0)
        misses = self._miss_counts.get(node_id, 0)
        total = hits + misses
        if total > 0:
            self.node_hit_rate.labels(node_id=node_id).set(hits / total)

    def record_fault(self, node_id: str, fault_level: str) -> None:
        self.fault_total.labels(node_id=node_id, fault_level=fault_level).inc()

    def record_request(
        self, method: str, endpoint: str, duration: float, status: str
    ) -> None:
        self.request_duration.labels(method=method, endpoint=endpoint).observe(duration)
        self.request_total.labels(method=method, endpoint=endpoint, status=status).inc()

    def update_cluster_metrics(
        self,
        active_node_count: int,
        vnode_count: int,
        shard_count: int,
        local_cache_size: int,
    ) -> None:
        self.active_nodes.set(active_node_count)
        self.total_vnodes.set(vnode_count)
        self.shard_count.set(shard_count)
        self.local_cache_size.set(local_cache_size)

    def record_rebalance(self) -> None:
        self.rebalance_runs.inc()

    def get_metrics_text(self) -> str:
        if HAS_PROMETHEUS:
            return generate_latest(self._registry).decode("utf-8")
        lines = []
        for attr_name in dir(self):
            if attr_name.startswith("_"):
                continue
            attr = getattr(self, attr_name)
            if hasattr(attr, "_mock_marker") and getattr(attr, "_mock_marker"):
                metric_name = attr._name
                lines.append(f"# HELP {metric_name} Mock metric")
                lines.append(f"# TYPE {metric_name} counter")
                if attr._values:
                    for labels, value in attr._values.items():
                        label_str = ",".join(
                            f'{k}="{v}"' for k, v in zip(attr._labelnames, labels)
                        )
                        lines.append(f"{metric_name}{{{label_str}}} {value}")
                else:
                    lines.append(f"{metric_name} {attr._default}")
        return "\n".join(lines) + "\n"

    @property
    def registry(self) -> CollectorRegistry:
        return self._registry


class RequestTimer:

    def __init__(self, collector: MetricsCollector, method: str, endpoint: str):
        self._collector = collector
        self._method = method
        self._endpoint = endpoint
        self._start = 0.0

    def __enter__(self):
        self._start = time.monotonic()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = time.monotonic() - self._start
        status = "error" if exc_type else "ok"
        self._collector.record_request(
            self._method, self._endpoint, duration, status
        )
        return False

    async def __aenter__(self):
        self._start = time.monotonic()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        duration = time.monotonic() - self._start
        status = "error" if exc_type else "ok"
        self._collector.record_request(
            self._method, self._endpoint, duration, status
        )
        return False
