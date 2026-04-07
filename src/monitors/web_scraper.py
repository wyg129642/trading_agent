"""Generic web page scraper for government sites, company blogs, etc."""

from __future__ import annotations

import logging
import re

from src.models import NewsItem
from src.monitors.base import BaseMonitor
from src.utils.text_extractor import extract_links_from_html

logger = logging.getLogger(__name__)

# Navigation / generic titles that are NOT real news articles.
# These are link texts commonly found on news listing pages.
_NAVIGATION_JUNK = frozenset({
    "all news releases", "all news", "all press releases", "all releases",
    "all public company", "all multimedia", "all photos", "all videos",
    "multimedia gallery", "photo gallery", "video gallery",
    "press releases", "news releases", "announcements",
    "english-only", "english only", "español", "more news", "read more",
    "view all", "see all", "load more", "show more",
    "next page", "previous page", "back to top",
    "about us", "contact us", "privacy policy", "terms of use",
    "subscribe", "sign up", "log in", "sign in",
    "home", "search", "menu", "navigation",
    "technology", "innovation & ai", "innovation",
    # Category / section navigation commonly seen on PR sites
    "trending topics", "trending", "featured", "popular",
    "auto & transportation", "all automotive & transportation",
    "all technology", "all business", "all healthcare",
    "country guidance", "classify your item",
    "export administration regulations",
    # Single-word section headers
    "product", "products", "policy", "exclusive",
    "english", "中文",
    # Government / regulatory site navigation
    "articles", "briefings & statements", "presidential actions",
    "submit a confidential lead or tip", "consolidated screening list",
    "辖区监管动态",
    # PR site category labels
    "aerospace, defense", "air freight", "airlines & aviation",
    "banking & financial services", "energy & utilities",
    "company", "companies",
})

# Patterns that indicate navigation/index pages rather than articles
_JUNK_PATTERNS = re.compile(
    r"^(all\s+\w+(\s+&\s+\w+)?|view\s+(all|more)|see\s+(all|more)|load\s+more|"
    r"page\s+\d+|next|previous|back|home|search|menu|sign\s+(in|up)|"
    r"log\s+in|subscribe|contact|save\s+\d+%.*|"
    r"all\s+\w+\s+&\s+\w+)$",
    re.IGNORECASE,
)


def _is_navigation_junk(title: str) -> bool:
    """Return True if title looks like a navigation link rather than a news article."""
    t = title.strip().lower()
    if t in _NAVIGATION_JUNK:
        return True
    if _JUNK_PATTERNS.match(t):
        return True
    # Titles that are just a single generic word (not real articles)
    words = t.split()
    if len(words) <= 2 and t in {
        "news", "blog", "press", "media", "resources", "events",
        "insights", "research", "solutions", "services",
        "新闻发布会", "证监会要闻", "政策法规", "最新动态",
    }:
        return True
    return False


class WebScraperMonitor(BaseMonitor):
    """Monitor a web page for new links/announcements via CSS selectors.

    Configuration in sources.yaml:
      css_selector: container selector
      item_selector: individual item selector
      title_selector: title text selector ("self" = the item itself)
      link_selector: link href selector ("self" = the item's href)
    """

    async def fetch_items(self) -> list[NewsItem]:
        html = await self._get_html(self.url)

        extracted = extract_links_from_html(
            html=html,
            base_url=self.url,
            css_selector=self.config.get("css_selector", "main"),
            item_selector=self.config.get("item_selector", "a"),
            title_selector=self.config.get("title_selector", "self"),
            link_selector=self.config.get("link_selector", "self"),
        )

        # Determine language from source config
        language = "zh" if self.market == "china" else "en"

        items = []
        for entry in extracted[:20]:
            title = entry["title"]
            url = entry["url"]

            # Skip very short titles (likely navigation junk)
            if len(title) < 5:
                continue

            # Skip navigation / generic link text
            if _is_navigation_junk(title):
                continue

            metadata = {}
            if self.group:
                metadata["group"] = self.group
            if self.tags:
                metadata["tags"] = self.tags
            if self.stock_ticker:
                metadata["stock_ticker"] = self.stock_ticker
                metadata["stock_name"] = self.stock_name
                metadata["stock_market"] = self.stock_market

            item = NewsItem(
                source_name=self.name,
                title=title,
                url=url,
                language=language,
                market=self.market,
                metadata=metadata,
            )
            items.append(item)

        return items
