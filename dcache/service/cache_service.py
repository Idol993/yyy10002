import asyncio
import json
import logging
import time
from typing import Any, Callable, Coroutine, Dict, Optional

from aiohttp import web

from .resilience import CircuitBreaker, IdempotencyGuard, RetryExecutor, SlidingWindowRateLimiter
from ..monitor.metrics import MetricsCollector, RequestTimer

logger = logging.getLogger(__name__)


class CacheService:

    def __init__(
        self,
        get_node_fn: Callable[[str], Optional[str]],
        get_nodes_fn: Callable[[str], list],
        read_fn: Callable[[str, str], Coroutine[Any, Any, Optional[Any]]],
        write_fn: Callable[[str, str, Any], Coroutine[Any, Any, bool]],
        delete_fn: Callable[[str, str], Coroutine[Any, Any, bool]],
        node_add_fn: Callable[[Dict], Coroutine[Any, Any, Any]],
        node_remove_fn: Callable[[str], Coroutine[Any, Any, Any]],
        metrics: MetricsCollector,
        rate_limit: int = 1000,
    ):
        self._get_node_fn = get_node_fn
        self._get_nodes_fn = get_nodes_fn
        self._read_fn = read_fn
        self._write_fn = write_fn
        self._delete_fn = delete_fn
        self._node_add_fn = node_add_fn
        self._node_remove_fn = node_remove_fn
        self._metrics = metrics

        self._rate_limiter = SlidingWindowRateLimiter(
            config=type("Cfg", (), {"max_requests": rate_limit, "window_seconds": 1.0, "bucket_count": 10})()
        )
        self._circuit_breaker = CircuitBreaker()
        self._retry_executor = RetryExecutor()
        self._idempotency = IdempotencyGuard()

    async def handle_get(self, request: web.Request) -> web.Response:
        key = request.match_info.get("key", "")
        if not key:
            return web.json_response({"error": "missing key"}, status=400)

        if not await self._rate_limiter.allow("get"):
            self._metrics.record_request("GET", "/cache", 0, "rate_limited")
            return web.json_response({"error": "rate limited"}, status=429)

        if not await self._circuit_breaker.allow_request():
            self._metrics.record_request("GET", "/cache", 0, "circuit_open")
            return web.json_response({"error": "circuit breaker open"}, status=503)

        async with RequestTimer(self._metrics, "GET", "/cache"):
            try:
                node_id = self._get_node_fn(key)
                if node_id is None:
                    return web.json_response(
                        {"error": "no available node"}, status=503
                    )

                result = await self._retry_executor.execute_with_retry(
                    self._read_fn, node_id, key
                )

                await self._circuit_breaker.record_success()
                self._metrics.record_hit(node_id)

                if result is not None:
                    return web.json_response(
                        {"key": key, "value": result, "node": node_id}
                    )
                else:
                    self._metrics.record_miss(node_id)
                    return web.json_response(
                        {"key": key, "value": None, "node": node_id}, status=404
                    )
            except Exception as e:
                await self._circuit_breaker.record_failure()
                logger.error("GET key=%s failed: %s", key, e)
                return web.json_response({"error": str(e)}, status=500)

    async def handle_put(self, request: web.Request) -> web.Response:
        key = request.match_info.get("key", "")
        if not key:
            return web.json_response({"error": "missing key"}, status=400)

        request_id = request.headers.get("X-Request-Id", "")
        if request_id:
            cached_result = await self._idempotency.check_and_mark(request_id)
            if cached_result is not None:
                return web.json_response(cached_result)

        if not await self._rate_limiter.allow("put"):
            self._metrics.record_request("PUT", "/cache", 0, "rate_limited")
            return web.json_response({"error": "rate limited"}, status=429)

        if not await self._circuit_breaker.allow_request():
            self._metrics.record_request("PUT", "/cache", 0, "circuit_open")
            return web.json_response({"error": "circuit breaker open"}, status=503)

        async with RequestTimer(self._metrics, "PUT", "/cache"):
            try:
                body = await request.json()
                value = body.get("value")

                node_id = self._get_node_fn(key)
                if node_id is None:
                    return web.json_response(
                        {"error": "no available node"}, status=503
                    )

                success = await self._retry_executor.execute_with_retry(
                    self._write_fn, node_id, key, value
                )

                await self._circuit_breaker.record_success()

                response_data = {
                    "key": key,
                    "node": node_id,
                    "success": success,
                }
                if request_id:
                    await self._idempotency.record_result(request_id, response_data)

                return web.json_response(response_data)
            except Exception as e:
                await self._circuit_breaker.record_failure()
                logger.error("PUT key=%s failed: %s", key, e)
                return web.json_response({"error": str(e)}, status=500)

    async def handle_delete(self, request: web.Request) -> web.Response:
        key = request.match_info.get("key", "")
        if not key:
            return web.json_response({"error": "missing key"}, status=400)

        if not await self._rate_limiter.allow("delete"):
            return web.json_response({"error": "rate limited"}, status=429)

        async with RequestTimer(self._metrics, "DELETE", "/cache"):
            try:
                node_id = self._get_node_fn(key)
                if node_id is None:
                    return web.json_response(
                        {"error": "no available node"}, status=503
                    )

                success = await self._retry_executor.execute_with_retry(
                    self._delete_fn, node_id, key
                )
                return web.json_response(
                    {"key": key, "node": node_id, "success": success}
                )
            except Exception as e:
                logger.error("DELETE key=%s failed: %s", key, e)
                return web.json_response({"error": str(e)}, status=500)

    async def handle_add_node(self, request: web.Request) -> web.Response:
        try:
            body = await request.json()
            await self._node_add_fn(body)
            return web.json_response({"status": "ok"})
        except Exception as e:
            logger.error("Add node failed: %s", e)
            return web.json_response({"error": str(e)}, status=400)

    async def handle_remove_node(self, request: web.Request) -> web.Response:
        node_id = request.match_info.get("node_id", "")
        try:
            await self._node_remove_fn(node_id)
            return web.json_response({"status": "ok"})
        except Exception as e:
            logger.error("Remove node failed: %s", e)
            return web.json_response({"error": str(e)}, status=400)

    async def handle_metrics(self, request: web.Request) -> web.Response:
        metrics_text = self._metrics.get_metrics_text()
        return web.Response(
            text=metrics_text,
            content_type="text/plain",
        )

    async def handle_health(self, request: web.Request) -> web.Response:
        return web.json_response({"status": "healthy"})

    def create_app(self) -> web.Application:
        app = web.Application()

        app.router.add_get("/cache/{key}", self.handle_get)
        app.router.add_put("/cache/{key}", self.handle_put)
        app.router.add_delete("/cache/{key}", self.handle_delete)
        app.router.add_post("/cluster/nodes", self.handle_add_node)
        app.router.add_delete("/cluster/nodes/{node_id}", self.handle_remove_node)
        app.router.add_get("/metrics", self.handle_metrics)
        app.router.add_get("/health", self.handle_health)

        return app
