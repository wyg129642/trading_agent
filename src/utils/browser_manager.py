"""Singleton Playwright browser manager for rendering JS-heavy pages."""

from __future__ import annotations

import asyncio
import glob
import logging
import os

logger = logging.getLogger(__name__)

# Limit concurrent browser navigations to avoid overwhelming the process
_MAX_CONCURRENT_PAGES = 3


def _find_chromium_executable() -> str | None:
    """Auto-detect the Playwright-managed Chromium executable path."""
    cache_dir = os.path.expanduser("~/.cache/ms-playwright")
    # Look for chromium-*/chrome-linux/chrome
    candidates = sorted(
        glob.glob(os.path.join(cache_dir, "chromium-*/chrome-linux/chrome")),
        reverse=True,  # newest version first
    )
    if candidates:
        return candidates[0]
    return None


class BrowserManager:
    """Manages a shared Playwright Chromium instance with lazy initialization.

    Uses a semaphore to limit concurrent page navigations and prevent
    resource exhaustion / TargetClosedError crashes.

    Usage:
        bm = BrowserManager()
        html = await bm.fetch_page("https://openai.com/blog")
        await bm.close()
    """

    def __init__(self, headless: bool = True):
        self._headless = headless
        self._playwright = None
        self._browser = None
        self._lock = asyncio.Lock()
        self._semaphore = asyncio.Semaphore(_MAX_CONCURRENT_PAGES)
        self._closed = False

    async def _ensure_browser(self) -> None:
        """Lazy-init: launch browser on first use."""
        if self._browser is not None:
            return
        async with self._lock:
            if self._browser is not None:
                return
            if self._closed:
                return
            from playwright.async_api import async_playwright
            self._playwright = await async_playwright().start()

            launch_kwargs: dict = {
                "headless": self._headless,
                "args": [
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-gpu",
                ],
            }
            # If the headless shell is missing, point to the full Chromium binary
            chromium_path = _find_chromium_executable()
            if chromium_path:
                launch_kwargs["executable_path"] = chromium_path

            self._browser = await self._playwright.chromium.launch(**launch_kwargs)
            logger.info("Playwright Chromium browser launched (executable=%s)",
                        launch_kwargs.get("executable_path", "default"))

    async def fetch_page(
        self,
        url: str,
        timeout_ms: int = 30000,
        wait_until: str = "domcontentloaded",
        extra_wait_ms: int = 2000,
    ) -> str:
        """Render a page and return its HTML content.

        Creates a fresh BrowserContext per call for isolation.
        Uses a semaphore to limit concurrent navigations.
        Returns empty string on navigation failure or if browser is closed.

        Args:
            url: Page URL to fetch.
            timeout_ms: Navigation timeout in milliseconds.
            wait_until: Playwright wait_until strategy
                        ("domcontentloaded", "load", "networkidle").
            extra_wait_ms: Extra delay after navigation for JS rendering.
        """
        if self._closed:
            return ""

        async with self._semaphore:
            if self._closed:
                return ""

            try:
                await self._ensure_browser()
            except Exception as e:
                logger.error("Browser launch failed: %s", e)
                return ""

            if self._browser is None:
                return ""

            context = None
            try:
                ctx_kwargs: dict = {
                    "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                    "viewport": {"width": 1280, "height": 800},
                }
                # Use proxy from environment for international sites
                proxy_url = os.environ.get("HTTP_PROXY") or os.environ.get("http_proxy")
                if proxy_url:
                    ctx_kwargs["proxy"] = {"server": proxy_url}
                context = await self._browser.new_context(**ctx_kwargs)
                page = await context.new_page()
                try:
                    await page.goto(url, wait_until=wait_until, timeout=timeout_ms)
                except Exception as e:
                    if self._closed:
                        return ""
                    # Some pages may fail wait_until but still have partial HTML
                    logger.warning("Playwright navigation partial for %s: %s", url, type(e).__name__)
                # Wait for JS rendering
                await page.wait_for_timeout(extra_wait_ms)
                html = await page.content()
                return html
            except Exception as e:
                if self._closed:
                    return ""
                logger.error("Playwright fetch failed for %s: %s", url, e)
                return ""
            finally:
                if context:
                    try:
                        await context.close()
                    except Exception:
                        pass

    async def close(self) -> None:
        """Gracefully shutdown browser and playwright."""
        self._closed = True
        if self._browser:
            try:
                await self._browser.close()
            except Exception as e:
                logger.debug("Browser close error: %s", e)
            self._browser = None
        if self._playwright:
            try:
                await self._playwright.stop()
            except Exception as e:
                logger.debug("Playwright stop error: %s", e)
            self._playwright = None
            logger.info("Playwright browser closed")
