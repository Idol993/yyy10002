import asyncio
import hashlib
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Coroutine, Dict, Optional, Set

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreakerConfig:
    failure_threshold: int = 5
    recovery_timeout: float = 30.0
    half_open_max_calls: int = 3
    success_threshold: int = 2


class CircuitBreaker:

    def __init__(self, config: Optional[CircuitBreakerConfig] = None):
        self._config = config or CircuitBreakerConfig()
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time = 0.0
        self._half_open_calls = 0
        self._lock = asyncio.Lock()

    @property
    def state(self) -> CircuitState:
        return self._state

    async def allow_request(self) -> bool:
        async with self._lock:
            if self._state == CircuitState.CLOSED:
                return True
            if self._state == CircuitState.OPEN:
                if time.monotonic() - self._last_failure_time >= self._config.recovery_timeout:
                    self._state = CircuitState.HALF_OPEN
                    self._half_open_calls = 0
                    self._success_count = 0
                    logger.info("Circuit breaker entering HALF_OPEN state")
                    return True
                return False
            if self._state == CircuitState.HALF_OPEN:
                if self._half_open_calls < self._config.half_open_max_calls:
                    self._half_open_calls += 1
                    return True
                return False
            return False

    async def record_success(self) -> None:
        async with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self._config.success_threshold:
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
                    logger.info("Circuit breaker recovered to CLOSED state")
            elif self._state == CircuitState.CLOSED:
                self._failure_count = 0

    async def record_failure(self) -> None:
        async with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.monotonic()
            if self._state == CircuitState.HALF_OPEN:
                self._state = CircuitState.OPEN
                logger.warning("Circuit breaker back to OPEN from HALF_OPEN")
            elif self._state == CircuitState.CLOSED:
                if self._failure_count >= self._config.failure_threshold:
                    self._state = CircuitState.OPEN
                    logger.warning(
                        "Circuit breaker OPEN after %d failures",
                        self._failure_count,
                    )


@dataclass
class RateLimiterConfig:
    max_requests: int = 1000
    window_seconds: float = 1.0
    bucket_count: int = 10


class SlidingWindowRateLimiter:

    def __init__(self, config: Optional[RateLimiterConfig] = None):
        self._config = config or RateLimiterConfig()
        self._buckets: Dict[str, Dict[int, int]] = defaultdict(
            lambda: defaultdict(int)
        )
        self._lock = asyncio.Lock()

    async def allow(self, key: str) -> bool:
        async with self._lock:
            now = time.monotonic()
            current_bucket = int(now / (self._config.window_seconds / self._config.bucket_count))
            window_start_bucket = current_bucket - self._config.bucket_count + 1

            total = 0
            for b in range(window_start_bucket, current_bucket + 1):
                total += self._buckets[key].get(b, 0)

            if total >= self._config.max_requests:
                return False

            self._buckets[key][current_bucket] += 1

            for b in list(self._buckets[key].keys()):
                if b < window_start_bucket:
                    del self._buckets[key][b]

            return True


@dataclass
class RetryConfig:
    max_retries: int = 3
    base_delay: float = 0.1
    max_delay: float = 5.0
    exponential_base: float = 2.0
    jitter: bool = True


class RetryExecutor:

    def __init__(self, config: Optional[RetryConfig] = None):
        self._config = config or RetryConfig()

    async def execute_with_retry(
        self,
        fn: Callable[..., Coroutine[Any, Any, Any]],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        last_exception = None
        for attempt in range(self._config.max_retries + 1):
            try:
                return await fn(*args, **kwargs)
            except Exception as e:
                last_exception = e
                if attempt < self._config.max_retries:
                    delay = min(
                        self._config.base_delay
                        * (self._config.exponential_base ** attempt),
                        self._config.max_delay,
                    )
                    if self._config.jitter:
                        import random
                        delay *= 0.5 + random.random() * 0.5
                    logger.debug(
                        "Retry attempt %d/%d after %.3fs: %s",
                        attempt + 1, self._config.max_retries, delay, e,
                    )
                    await asyncio.sleep(delay)
        raise last_exception


class IdempotencyGuard:

    PENDING = "__pending_sentinel__"
    READY = "__ready_sentinel__"

    def __init__(self, ttl_seconds: float = 300.0, max_entries: int = 10000):
        self._ttl = ttl_seconds
        self._max_entries = max_entries
        self._store: Dict[str, tuple] = {}
        self._events: Dict[str, asyncio.Event] = {}
        self._lock = asyncio.Lock()

    async def check_and_mark(self, request_id: str):
        async with self._lock:
            if request_id in self._store:
                result, ts = self._store[request_id]
                if time.monotonic() - ts < self._ttl:
                    if result is self.PENDING:
                        return self.PENDING
                    return result
                del self._store[request_id]
                self._events.pop(request_id, None)

            self._store[request_id] = (self.PENDING, time.monotonic())
            self._events[request_id] = asyncio.Event()
            self._cleanup()
            return self.READY

    async def wait_for_result(self, request_id: str, timeout: float = 30.0):
        event = None
        async with self._lock:
            event = self._events.get(request_id)
            if event is None:
                if request_id in self._store:
                    result, _ = self._store[request_id]
                    if result is not self.PENDING:
                        return result
                return None

        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning("Idempotency wait timed out for request %s", request_id)
            return None

        async with self._lock:
            if request_id in self._store:
                result, _ = self._store[request_id]
                if result is not self.PENDING:
                    return result
            return None

    async def record_result(self, request_id: str, result) -> None:
        async with self._lock:
            self._store[request_id] = (result, time.monotonic())
            event = self._events.get(request_id)
            if event is not None:
                event.set()
                del self._events[request_id]

    async def clear(self, request_id: str) -> None:
        async with self._lock:
            self._store.pop(request_id, None)
            event = self._events.pop(request_id, None)
            if event is not None:
                event.set()

    def _cleanup(self) -> None:
        if len(self._store) > self._max_entries:
            cutoff = time.monotonic() - self._ttl
            expired = [k for k, (_, ts) in self._store.items() if ts < cutoff]
            for k in expired:
                del self._store[k]
                self._events.pop(k, None)
            if len(self._store) > self._max_entries:
                sorted_keys = sorted(
                    self._store.keys(), key=lambda k: self._store[k][1]
                )
                for k in sorted_keys[: len(self._store) - self._max_entries]:
                    del self._store[k]
                    self._events.pop(k, None)
