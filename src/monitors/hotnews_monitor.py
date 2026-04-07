"""Hot news monitor — fetches trending topics from Chinese news/social platforms.

Uses the newsnow public API to fetch hot lists from:
  - wallstreetcn (华尔街见闻)
  - cls-hot (财联社)
  - xueqiu (雪球)
  - weibo (微博热搜)
  - and other supported sources

Each source is configured as a separate monitor instance via sources.yaml
with api_type: "hotnews" and an api_source_id field.
"""

from __future__ import annotations

import json
import logging

from src.models import NewsItem
from src.monitors.base import BaseMonitor

logger = logging.getLogger(__name__)

# Human-readable names for logging
SOURCE_NAMES = {
    "weibo": "微博热搜",
    "zhihu": "知乎热榜",
    "bilibili-hot-search": "B站热搜",
    "toutiao": "今日头条",
    "douyin": "抖音热榜",
    "github-trending-today": "GitHub趋势",
    "coolapk": "酷安热榜",
    "tieba": "百度贴吧",
    "wallstreetcn": "华尔街见闻",
    "thepaper": "澎湃新闻",
    "cls-hot": "财联社",
    "xueqiu": "雪球热榜",
}


class HotNewsMonitor(BaseMonitor):
    """Monitor Chinese hot news sources via the newsnow public API.

    Config fields:
      - api_source_id: one of the SOURCE_NAMES keys (e.g. "wallstreetcn")
    """

    NEWSNOW_BASE = "https://newsnow.busiyi.world/api/s"

    # Headers required by the newsnow API (Cloudflare protection)
    HEADERS = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Referer": "https://newsnow.busiyi.world",
        "Connection": "keep-alive",
    }

    async def fetch_items(self) -> list[NewsItem]:
        source_id = self.config.get("api_source_id", "")
        if not source_id:
            logger.warning("[%s] No api_source_id configured", self.name)
            return []

        url = f"{self.NEWSNOW_BASE}?id={source_id}&latest"
        raw = await self._get(url, headers=self.HEADERS)
        data = json.loads(raw)

        # API returns "success" for fresh data or "cache" for cached data — both are valid
        status = data.get("status", "")
        if status not in ("success", "cache"):
            logger.warning("[%s] API returned unexpected status: %s", self.name, status)
            return []

        items: list[NewsItem] = []
        for entry in data.get("items", []):
            title = entry.get("title", "").strip()
            item_url = entry.get("url", "")
            if not title:
                continue

            metadata: dict = {
                "newsnow_id": entry.get("id", ""),
                "source_id": source_id,
            }
            if self.group:
                metadata["group"] = self.group
            if self.tags:
                metadata["tags"] = self.tags

            items.append(
                NewsItem(
                    source_name=self.name,
                    title=title,
                    url=item_url,
                    language="zh",
                    market=self.market,
                    metadata=metadata,
                )
            )

        source_label = SOURCE_NAMES.get(source_id, source_id)
        logger.debug("[%s] Fetched %d items from %s", self.name, len(items), source_label)
        return items
