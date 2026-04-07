"""Base monitor class for all source monitors."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime

import aiohttp

from src.models import NewsItem, SourceHealth

logger = logging.getLogger(__name__)


class BaseMonitor(ABC):
    """Abstract base class for all source monitors.

    Subclasses implement `fetch_items()` to return new news items.
    The orchestrator calls `poll()` which handles error tracking and dedup.

    _seen_hashes is pre-populated from the database by the orchestrator
    before the first poll, so restarts do not report old items as new.

    _last_seen_dt is set by the orchestrator to the most recent published_at
    for this source.  Subclasses whose sources provide reliable timestamps
    can use it to skip items older than this date.
    """

    def __init__(self, config: dict, session: aiohttp.ClientSession, browser_manager=None):
        self.name: str = config["name"]
        self.url: str = config.get("url", "")
        self.priority: str = config.get("priority", "p2")
        self.enabled: bool = config.get("enabled", True)
        self.market: str = config.get("market", "china")
        self.config = config
        self.session = session
        self.browser_manager = browser_manager
        self.health = SourceHealth(source_name=self.name)
        self._seen_hashes: set[str] = set()
        self._last_seen_dt: datetime | None = None
        # Management fields (for grouping, tagging, portfolio tracking)
        self.group: str = config.get("group", "")
        self.tags: list[str] = config.get("tags", [])
        self.stock_name: str = config.get("stock_name", "")
        self.stock_ticker: str = config.get("stock_ticker", "")
        self.stock_market: str = config.get("stock_market", "")

    @abstractmethod
    async def fetch_items(self) -> list[NewsItem]:
        """Fetch new items from the source. Implemented by subclasses."""
        ...

    async def poll(self) -> list[NewsItem]:
        """Poll the source and return only new (unseen) items.

        Handles error tracking and deduplication.
        """
        if not self.enabled:
            return []

        try:
            items = await self.fetch_items()
            new_items = []
            for item in items:
                h = item.content_hash
                if h not in self._seen_hashes:
                    self._seen_hashes.add(h)
                    new_items.append(item)

            # Update health
            self.health.last_success = datetime.now()
            self.health.consecutive_failures = 0
            self.health.total_items_fetched += len(new_items)
            self.health.is_healthy = True

            if new_items:
                logger.info("[%s] Found %d new items", self.name, len(new_items))
            elif items:
                logger.debug("[%s] Fetched %d items, all already seen", self.name, len(items))
            else:
                logger.debug("[%s] Fetched 0 items from source", self.name)
            return new_items

        except Exception as e:
            self.health.last_failure = datetime.now()
            self.health.consecutive_failures += 1
            if self.health.consecutive_failures >= 5:
                self.health.is_healthy = False
            logger.warning(
                "[%s] Fetch failed (attempt %d): %s",
                self.name,
                self.health.consecutive_failures,
                str(e),
            )
            return []

    async def _get(self, url: str, **kwargs) -> str:
        """Helper to make an HTTP GET request with standard headers."""
        headers = kwargs.pop("headers", {})
        headers.setdefault("User-Agent", "TradingAgent/1.0 (Financial Research)")
        headers.setdefault("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
        headers.setdefault("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")

        timeout = aiohttp.ClientTimeout(total=30)
        async with self.session.get(url, headers=headers, timeout=timeout, ssl=False, **kwargs) as resp:
            resp.raise_for_status()
            return await resp.text()

    async def _get_html(self, url: str) -> str:
        """Fetch HTML — uses Playwright if config.requires_browser, else plain HTTP."""
        if self.config.get("requires_browser") and self.browser_manager:
            logger.debug("[%s] Using Playwright for %s", self.name, url)
            timeout_ms = self.config.get("browser_timeout_ms", 30000)
            wait_until = self.config.get("browser_wait_until", "domcontentloaded")
            extra_wait_ms = self.config.get("browser_extra_wait_ms", 2000)
            return await self.browser_manager.fetch_page(
                url,
                timeout_ms=timeout_ms,
                wait_until=wait_until,
                extra_wait_ms=extra_wait_ms,
            )
        return await self._get(url)

    async def _post(self, url: str, data=None, json_data=None, **kwargs) -> str:
        """Helper to make an HTTP POST request."""
        headers = kwargs.pop("headers", {})
        headers.setdefault("User-Agent", "TradingAgent/1.0 (Financial Research)")
        headers.setdefault("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")

        timeout = aiohttp.ClientTimeout(total=30)
        async with self.session.post(
            url, data=data, json=json_data, headers=headers, timeout=timeout, ssl=False, **kwargs
        ) as resp:
            resp.raise_for_status()
            return await resp.text()

    def interval_seconds(self, settings: dict) -> int:
        """Get polling interval based on priority."""
        intervals = settings.get("intervals", {})
        mapping = {
            "p0": intervals.get("p0_critical", 60),
            "p1": intervals.get("p1_high", 300),
            "p2": intervals.get("p2_medium", 600),
            "p3": intervals.get("p3_low", 1800),
        }
        return mapping.get(self.priority, 600)
