"""Fetch article pages and extract clean text for pipeline enrichment."""

from __future__ import annotations

import asyncio
import logging
from urllib.parse import urlparse

import aiohttp

from engine.utils.rate_limiter import DomainRateLimiter
from engine.utils.text_extractor import extract_publish_date, extract_text_from_html, truncate_text

logger = logging.getLogger(__name__)

SKIP_EXTENSIONS = frozenset({
    ".pdf", ".doc", ".docx", ".xls", ".xlsx",
    ".ppt", ".pptx", ".zip", ".rar", ".7z",
})

SKIP_CONTENT_TYPES = frozenset({
    "application/pdf", "application/octet-stream",
    "application/zip", "application/msword",
})


class ContentFetcher:
    """Fetches an article URL and extracts readable text via trafilatura.

    Designed to be injected into AnalysisPipeline.  Failures are silent
    (returns None) so the pipeline can fall back to title-only analysis.
    """

    # Domains known to be overseas (need proxy)
    _OVERSEAS_TLDS = frozenset({".com", ".org", ".net", ".io", ".ai", ".dev", ".co"})
    # Domains known to be domestic China (no proxy)
    _CHINA_TLDS = frozenset({".cn", ".com.cn", ".net.cn", ".org.cn"})

    def __init__(
        self,
        session: aiohttp.ClientSession,
        max_content_chars: int = 5000,
        timeout_seconds: int = 15,
        rate_limit_rps: float = 1.0,
        browser_manager=None,
        proxy_url: str | None = None,
    ):
        self.session = session
        self.max_content_chars = max_content_chars
        self.timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self.rate_limiter = DomainRateLimiter(default_rps=rate_limit_rps)
        self.browser_manager = browser_manager
        self._proxy_url = proxy_url
        # Domains that always need a headless browser (learned at runtime)
        self._browser_domains: set[str] = set()

    def _is_overseas(self, domain: str) -> bool:
        """Heuristic: return True if the domain is likely overseas."""
        domain_lower = domain.lower()
        for tld in self._CHINA_TLDS:
            if domain_lower.endswith(tld):
                return False
        return True

    async def fetch(self, url: str) -> tuple[str | None, "datetime | None", str | None]:
        """Fetch and extract article text and publish date.

        Returns (text, published_at, None) on success or (None, None, reason) on skip/failure.
        """
        from datetime import datetime as _dt

        # Pre-flight: check URL shape and extension
        if not url or not url.startswith(("http://", "https://")):
            return None, None, "invalid_url"

        path_lower = urlparse(url).path.lower()
        for ext in SKIP_EXTENSIONS:
            if path_lower.endswith(ext):
                return None, None, f"skip_ext:{ext}"

        domain = urlparse(url).netloc

        try:
            await self.rate_limiter.acquire(domain)

            html, fetch_err = await self._fetch_html(url, domain)
            if fetch_err:
                return None, None, fetch_err

            if not html or len(html) < 100:
                return None, None, "empty_response"

            # Extract publish date from raw HTML
            published_at = extract_publish_date(html, url=url)

            text = extract_text_from_html(html, url=url)
            if not text or len(text.strip()) < 50:
                return None, published_at, "extraction_too_short"

            text = truncate_text(text, self.max_content_chars)
            return text, published_at, None

        except asyncio.TimeoutError:
            return None, None, "timeout"
        except aiohttp.ClientError as e:
            return None, None, f"client_error:{type(e).__name__}"
        except Exception as e:
            logger.debug("Content fetch error for %s: %s", url, e)
            return None, None, f"error:{type(e).__name__}"

    async def _fetch_html(self, url: str, domain: str) -> tuple[str | None, str | None]:
        """Fetch raw HTML. Uses browser for known browser-required domains,
        otherwise tries HTTP first and falls back to browser on 403.

        Returns (html, None) on success or (None, error_reason) on failure.
        """
        # If domain is known to need browser, go directly to Playwright
        if domain in self._browser_domains and self.browser_manager:
            logger.debug("Using Playwright (known domain) for %s", url)
            html = await self.browser_manager.fetch_page(url)
            return (html, None) if html else (None, "browser_empty")

        headers = {
            "User-Agent": "TradingAgent/1.0 (Financial Research)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        }

        proxy = self._proxy_url if self._is_overseas(domain) else None
        async with self.session.get(
            url, headers=headers, timeout=self.timeout,
            ssl=False, allow_redirects=True, max_redirects=5,
            proxy=proxy,
        ) as resp:
            ct = resp.content_type or ""
            if ct in SKIP_CONTENT_TYPES or "pdf" in ct:
                return None, f"skip_ct:{ct}"
            if resp.status == 403 and self.browser_manager:
                # HTTP blocked — retry with headless browser
                logger.info("HTTP 403 for %s — retrying with Playwright", url)
                self._browser_domains.add(domain)
                html = await self.browser_manager.fetch_page(url)
                return (html, None) if html else (None, "browser_403_empty")
            if resp.status != 200:
                return None, f"http_{resp.status}"
            return await resp.text(errors="replace"), None
