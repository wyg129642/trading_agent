"""Per-domain async rate limiter."""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict


class RateLimiter:
    """Token-bucket rate limiter with per-domain tracking."""

    def __init__(self, requests_per_second: float = 2.0):
        self._rate = requests_per_second
        self._interval = 1.0 / requests_per_second
        self._last_call: dict[str, float] = defaultdict(float)
        self._locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    async def acquire(self, domain: str = "default") -> None:
        """Wait until we are allowed to make the next request for this domain."""
        async with self._locks[domain]:
            now = time.monotonic()
            elapsed = now - self._last_call[domain]
            if elapsed < self._interval:
                await asyncio.sleep(self._interval - elapsed)
            self._last_call[domain] = time.monotonic()

    def set_rate(self, domain: str, requests_per_second: float) -> None:
        """Override rate limit for a specific domain."""
        self._locks[domain] = asyncio.Lock()
        self._last_call[domain] = 0.0
        # Store per-domain interval — but simple version uses global rate
        # For a production system, extend to per-domain intervals


class DomainRateLimiter:
    """More advanced: per-domain rate limits."""

    def __init__(self, default_rps: float = 2.0):
        self._default_interval = 1.0 / default_rps
        self._domain_intervals: dict[str, float] = {}
        self._last_call: dict[str, float] = defaultdict(float)
        self._locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    def set_domain_rate(self, domain: str, rps: float) -> None:
        self._domain_intervals[domain] = 1.0 / rps

    async def acquire(self, domain: str) -> None:
        async with self._locks[domain]:
            interval = self._domain_intervals.get(domain, self._default_interval)
            now = time.monotonic()
            elapsed = now - self._last_call[domain]
            if elapsed < interval:
                await asyncio.sleep(interval - elapsed)
            self._last_call[domain] = time.monotonic()
