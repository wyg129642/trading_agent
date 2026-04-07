"""RSS/Atom feed monitor for SEC EDGAR and other RSS sources."""

from __future__ import annotations

import logging
from datetime import datetime

import feedparser

from engine.models import NewsItem
from engine.monitors.base import BaseMonitor

logger = logging.getLogger(__name__)


class RSSMonitor(BaseMonitor):
    """Monitor RSS/Atom feeds (SEC EDGAR, Federal Register RSS, etc.)."""

    async def fetch_items(self) -> list[NewsItem]:
        raw = await self._get(self.url)
        feed = feedparser.parse(raw)

        items = []
        for entry in feed.entries[:30]:
            title = entry.get("title", "").strip()
            link = entry.get("link", "")
            if not title or not link:
                continue

            # Parse published date
            published = None
            for date_field in ("published_parsed", "updated_parsed"):
                dt_tuple = entry.get(date_field)
                if dt_tuple:
                    try:
                        published = datetime(*dt_tuple[:6])
                    except (TypeError, ValueError):
                        pass
                    break

            # Extract summary/content
            content = ""
            if entry.get("summary"):
                content = entry["summary"]
            elif entry.get("content"):
                content = entry["content"][0].get("value", "")

            # Skip items older than our last-seen timestamp
            if published and self._last_seen_dt and published <= self._last_seen_dt:
                continue

            # Determine language from title
            language = "en"
            if any("\u4e00" <= c <= "\u9fff" for c in title):
                language = "zh"

            metadata = {"feed_id": entry.get("id", link)}
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
                url=link,
                content=content,
                published_at=published,
                language=language,
                market=self.market,
                metadata=metadata,
            )
            items.append(item)

        return items
