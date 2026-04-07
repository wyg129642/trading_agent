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
        bm = BrowserManager(proxy_url="http://127.0.0.1:7890")
        html = await bm.fetch_page("https://openai.com/blog", use_proxy=True)
        await bm.close()
    """

    def __init__(self, headless: bool = True, proxy_url: str | None = None):
        self._headless = headless
        self._proxy_url = proxy_url
        self._playwright = None
        self._browser = None
        self._browser_proxy = None  # separate browser instance for proxy
        self._lock = asyncio.Lock()
        self._semaphore = asyncio.Semaphore(_MAX_CONCURRENT_PAGES)
        self._closed = False

    async def _ensure_browser(self, use_proxy: bool = False) -> None:
        """Lazy-init: launch browser on first use."""
        attr = "_browser_proxy" if use_proxy else "_browser"
        if getattr(self, attr) is not None:
            return
        async with self._lock:
            if getattr(self, attr) is not None:
                return
            if self._closed:
                return
            if self._playwright is None:
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

            if use_proxy and self._proxy_url:
                launch_kwargs["proxy"] = {"server": self._proxy_url}

            browser = await self._playwright.chromium.launch(**launch_kwargs)
            setattr(self, attr, browser)
            logger.info("Playwright Chromium browser launched (proxy=%s, executable=%s)",
                        use_proxy, launch_kwargs.get("executable_path", "default"))

    async def fetch_page(
        self,
        url: str,
        timeout_ms: int = 30000,
        use_proxy: bool = False,
        wait_until: str = "domcontentloaded",
        extra_wait_ms: int = 2000,
    ) -> str:
        """Render a page and return its HTML content.

        Creates a fresh BrowserContext per call for isolation.
        Uses a semaphore to limit concurrent navigations.
        Returns empty string on navigation failure or if browser is closed.

        Args:
            use_proxy: If True, route traffic through the configured proxy.
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
                await self._ensure_browser(use_proxy=use_proxy)
            except Exception as e:
                logger.error("Browser launch failed: %s", e)
                return ""

            browser = self._browser_proxy if use_proxy else self._browser
            if browser is None:
                return ""

            context = None
            try:
                context = await browser.new_context(
                    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                    viewport={"width": 1280, "height": 800},
                )
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
        for attr in ("_browser", "_browser_proxy"):
            browser = getattr(self, attr)
            if browser:
                try:
                    await browser.close()
                except Exception as e:
                    logger.debug("Browser close error (%s): %s", attr, e)
                setattr(self, attr, None)
        if self._playwright:
            try:
                await self._playwright.stop()
            except Exception as e:
                logger.debug("Playwright stop error: %s", e)
            self._playwright = None
            logger.info("Playwright browser closed")
