"""Token bucket for venue REST calls, kept at or below 50 % of published limits (NFR-14)."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable


class TokenBucket:
    """``rate`` weight units per second, burst ``capacity``."""

    def __init__(
        self,
        rate: float,
        capacity: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if rate <= 0 or capacity <= 0:
            raise ValueError("rate and capacity must be positive")
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.clock = clock
        self.stamp = clock()
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        now = self.clock()
        self.tokens = min(self.capacity, self.tokens + (now - self.stamp) * self.rate)
        self.stamp = now

    def wait_time(self, weight: float) -> float:
        """Seconds until ``weight`` tokens are available (0 if now)."""
        self._refill()
        missing = weight - self.tokens
        return 0.0 if missing <= 0 else missing / self.rate

    async def acquire(self, weight: float = 1.0) -> None:
        if weight > self.capacity:
            raise ValueError("weight exceeds bucket capacity")
        async with self._lock:
            delay = self.wait_time(weight)
            if delay > 0:
                await asyncio.sleep(delay)
                self._refill()
            self.tokens -= weight
