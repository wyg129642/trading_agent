"""Trending detector — alerts when portfolio holdings appear on social hot lists.

This is a lightweight, zero-LLM-cost detector that runs on every new news item
before it enters the 3-phase analysis pipeline. It matches stock names/tickers
from portfolio_sources.yaml against hot news titles from social sources.

When a match is found, a Feishu alert is sent immediately. Each stock+source
combination is alerted at most once per day to avoid noise.
"""

from __future__ import annotations

import logging
from datetime import datetime

from engine.models import NewsItem

logger = logging.getLogger(__name__)

# newsnow source IDs that are social/hot-list in nature
SOCIAL_SOURCE_IDS = frozenset({
    "wallstreetcn",
    "cls-hot",
    "xueqiu",
    "weibo",
    "zhihu",
    "douyin",
    "toutiao",
    "thepaper",
    "bilibili-hot-search",
    "coolapk",
    "tieba",
})


class TrendingDetector:
    """Detect when portfolio holdings appear on social media hot lists."""

    def __init__(self, portfolio_sources: list[dict], companies: dict | None = None):
        # keyword -> {name, ticker, market}
        self.keywords: dict[str, dict] = {}
        self._build_keyword_map(portfolio_sources, companies)
        # "ticker:source_name" -> already alerted today
        self._alerted_today: set[str] = set()
        self._last_reset_date: str = datetime.now().strftime("%Y-%m-%d")
        logger.info(
            "[TrendingDetector] Initialized with %d keywords from portfolio",
            len(self.keywords),
        )

    def _build_keyword_map(
        self, portfolio_sources: list[dict], companies: dict | None = None
    ) -> None:
        """Build lookup from company names/tickers to portfolio info.

        Sources:
          1. portfolio_sources.yaml entries with group="portfolio"
          2. companies section from sources.yaml (us, china, hk, private)
        """
        # From portfolio_sources.yaml
        for src in portfolio_sources:
            if src.get("group") != "portfolio":
                continue
            name = src.get("stock_name", "").strip()
            ticker = src.get("stock_ticker", "").strip()
            info = {
                "name": name,
                "ticker": ticker,
                "market": src.get("stock_market", ""),
            }
            if name and len(name) >= 2:
                self.keywords[name] = info
            if ticker and len(ticker) >= 2:
                self.keywords[ticker] = info

        # From companies section in sources.yaml
        if companies:
            for market, company_list in companies.items():
                if not isinstance(company_list, list):
                    continue
                for company in company_list:
                    cname = company.get("name", "").strip()
                    # Extract Chinese name from format like "寒武纪 (Cambricon)"
                    if "(" in cname:
                        cn_name = cname.split("(")[0].strip()
                        en_name = cname.split("(")[1].rstrip(")").strip()
                    elif "（" in cname:
                        cn_name = cname.split("（")[0].strip()
                        en_name = cname.split("（")[1].rstrip("）").strip()
                    else:
                        cn_name = cname
                        en_name = ""

                    ticker = company.get("ticker", "").strip()
                    ticker_hk = company.get("ticker_hk", "").strip()
                    info = {
                        "name": cn_name or en_name or cname,
                        "ticker": ticker or ticker_hk,
                        "market": market,
                    }
                    if cn_name and len(cn_name) >= 2:
                        self.keywords[cn_name] = info
                    if en_name and len(en_name) >= 2:
                        self.keywords[en_name] = info
                    if ticker and len(ticker) >= 2:
                        self.keywords[ticker] = info

    def check_item(self, item: NewsItem) -> list[dict]:
        """Check if a news item's title mentions any portfolio holding.

        Only checks items from social/hot-list sources (via metadata.source_id).
        Returns a list of matched holding info dicts.
        """
        source_id = item.metadata.get("source_id", "")
        if source_id not in SOCIAL_SOURCE_IDS:
            return []

        matches = []
        seen_tickers: set[str] = set()
        title = item.title

        for keyword, info in self.keywords.items():
            if keyword in title:
                ticker = info.get("ticker", "")
                if ticker and ticker not in seen_tickers:
                    seen_tickers.add(ticker)
                    matches.append(info)
                elif not ticker:
                    matches.append(info)

        return matches

    async def alert_if_trending(self, item: NewsItem, matches: list[dict], alerter) -> None:
        """Send Feishu alert for each matched holding. Dedup per ticker+source per day."""
        self._maybe_reset_daily()

        for match in matches:
            ticker = match.get("ticker", match.get("name", "unknown"))
            key = f"{ticker}:{item.source_name}"
            if key in self._alerted_today:
                continue
            self._alerted_today.add(key)

            name = match.get("name", "")
            market = match.get("market", "")
            msg = (
                f"📈 持仓股上热搜！\n"
                f"股票: {name} ({ticker}) [{market}]\n"
                f"来源: {item.source_name}\n"
                f"标题: {item.title}\n"
                f"链接: {item.url}"
            )
            logger.info("[TrendingDetector] %s trending on %s: %s", name, item.source_name, item.title)
            try:
                await alerter.send_system_alert(msg)
            except Exception as e:
                logger.warning("[TrendingDetector] Failed to send alert: %s", e)

    def _maybe_reset_daily(self) -> None:
        """Reset the daily dedup set at midnight."""
        today = datetime.now().strftime("%Y-%m-%d")
        if today != self._last_reset_date:
            self._alerted_today.clear()
            self._last_reset_date = today
            logger.info("[TrendingDetector] Daily alert dedup reset")
