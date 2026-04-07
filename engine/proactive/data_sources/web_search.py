"""External web search data source — Baidu (CN) + Tavily + Jina (EN).

v3: Time-gated search. Uses Tavily days=3/topic="news" and Baidu recency="week",
then post-filters ALL results by published_at < 24 hours. Calls search APIs
directly (bypasses parallel_search) for finer control over time parameters.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from engine.proactive.data_sources.base import DataSourcePlugin, DataSourceResult
from engine.proactive.models import BreakingNewsItem, PortfolioHolding, StockBaseline

logger = logging.getLogger(__name__)

CST = ZoneInfo("Asia/Shanghai")


class WebSearchPlugin(DataSourcePlugin):
    """External search via Baidu + Tavily + Jina for stock-specific queries.

    v3: Time-gated — only returns items published within breaking_news_window_hours.
    """

    name = "web_search"

    def __init__(
        self,
        settings: dict,
        queries_per_stock: int = 3,
        breaking_news_window_hours: int = 24,
    ):
        self._settings = settings
        self._queries_per_stock = queries_per_stock
        self._window_hours = breaking_news_window_hours

        self._baidu_api_key = settings.get("baidu", {}).get("api_key", "")
        self._tavily_api_key = settings.get("tavily", {}).get("api_key", "")
        self._jina_api_key = settings.get("jina", {}).get("api_key", "")

    def _generate_queries(
        self, holding: PortfolioHolding,
    ) -> tuple[list[str], list[str]]:
        """Generate bilingual search queries tailored to stock characteristics."""
        cn_queries = []
        en_queries = []

        name_cn = holding.name_cn
        name_en = holding.name_en or holding.ticker
        ticker = holding.ticker
        sector_tags = [t for t in holding.tags if t not in ("holding",)]

        # Query 1: Core breaking news (most important)
        cn_queries.append(f"{name_cn} 最新消息 重大变化")
        en_queries.append(f"{ticker} {name_en} latest news today")

        # Query 2: Industry-specific — use tags to make queries precise
        if sector_tags:
            tag_str = " ".join(sector_tags[:2])
            cn_queries.append(f"{name_cn} {tag_str} 突破 订单 客户")
            en_queries.append(f"{ticker} {name_en} {sector_tags[0]} breakthrough order contract")

        # Query 3: Earnings / guidance / analyst (financial catalysts)
        if self._queries_per_stock >= 3:
            cn_queries.append(f"{name_cn} 业绩 指引 评级调整 目标价")
            en_queries.append(f"{ticker} {name_en} earnings guidance rating upgrade downgrade")

        return cn_queries, en_queries

    async def fetch(
        self,
        holding: PortfolioHolding,
        baseline: StockBaseline,
        content_fetcher=None,
        **kwargs,
    ) -> DataSourceResult:
        """Search with time parameters, then post-filter by publication date."""
        import asyncio
        from engine.tools.web_search import baidu_search, tavily_search, jina_search

        cn_queries, en_queries = self._generate_queries(holding)

        # Run all search API calls concurrently with time parameters
        tasks = []

        # Baidu queries with recency="week" (finest granularity available)
        for q in cn_queries:
            tasks.append(self._search_baidu(q))

        # Tavily queries with days=3 and topic="news"
        for q in en_queries:
            tasks.append(self._search_tavily(q))

        # Jina queries (no time filter available, but still useful for content)
        if self._jina_api_key:
            for q in en_queries[:1]:  # Limit Jina to 1 query to save time
                tasks.append(self._search_jina(q))

        try:
            all_results = await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            logger.warning("WebSearch gather failed for %s: %s", holding.ticker, e)
            return DataSourceResult(source_name=self.name)

        # Flatten and dedup by URL
        all_items = []
        seen_urls: set[str] = set()
        for result_or_exc in all_results:
            if isinstance(result_or_exc, Exception):
                logger.debug("Search task error: %s", result_or_exc)
                continue
            for item in (result_or_exc or []):
                url = item.get("url", "")
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    all_items.append(item)

        # Post-filter by publication date (24-hour window)
        now_utc = datetime.now(timezone.utc)
        cutoff = now_utc - timedelta(hours=self._window_hours)
        recent_items, undated_items = self._post_filter_by_recency(all_items, cutoff)

        # Optionally resolve undated items via ContentFetcher
        if undated_items and content_fetcher and len(recent_items) < 3:
            resolved = await self._resolve_undated_items(
                undated_items[:5], content_fetcher, cutoff,
            )
            recent_items.extend(resolved)

        logger.info(
            "[WebSearch:%s] %d total → %d recent (<%dh), %d undated discarded",
            holding.ticker, len(all_items), len(recent_items),
            self._window_hours, len(undated_items),
        )

        result = DataSourceResult(
            source_name=self.name,
            items=recent_items,
            item_count=len(recent_items),
            new_item_count=len(recent_items),
            metadata={"total_before_filter": len(all_items)},
        )
        result.formatted_text = self.format_for_llm(result, holding)
        return result

    # ------------------------------------------------------------------
    # Search API wrappers with time parameters
    # ------------------------------------------------------------------

    async def _search_baidu(self, query: str) -> list[dict]:
        from engine.tools.web_search import baidu_search
        try:
            results = await baidu_search(
                query=query,
                api_key=self._baidu_api_key,
                max_results=10,
                recency="week",  # Finest Baidu granularity
            )
            # Tag source for timezone parsing
            for r in results:
                r["_source_engine"] = "baidu"
            return results
        except Exception as e:
            logger.debug("Baidu search error: %s", e)
            return []

    async def _search_tavily(self, query: str) -> list[dict]:
        from engine.tools.web_search import tavily_search
        try:
            results = await tavily_search(
                query=query,
                api_key=self._tavily_api_key,
                max_results=10,
                days=1,          # Last 24h — tighter window, fresher results
                topic="news",    # News-focused results
            )
            for r in results:
                r["_source_engine"] = "tavily"
            return results
        except Exception as e:
            logger.debug("Tavily search error: %s", e)
            return []

    async def _search_jina(self, query: str) -> list[dict]:
        from engine.tools.web_search import jina_search
        try:
            results = await jina_search(
                query=query,
                api_key=self._jina_api_key,
                max_results=5,
            )
            for r in results:
                r["_source_engine"] = "jina"
            return results
        except Exception as e:
            logger.debug("Jina search error: %s", e)
            return []

    # ------------------------------------------------------------------
    # Time filtering
    # ------------------------------------------------------------------

    def _post_filter_by_recency(
        self, items: list[dict], cutoff: datetime,
    ) -> tuple[list[dict], list[dict]]:
        """Split items into (recent, undated) based on published_at.

        Timezone handling:
        - Tavily: dates are ISO format, assumed UTC
        - Baidu: dates are Chinese locale, assumed CST (UTC+8)
        - Jina: typically no date
        """
        recent = []
        undated = []

        for item in items:
            date_str = (item.get("date") or "").strip()
            if not date_str:
                undated.append(item)
                continue

            parsed_dt = self._parse_search_date(date_str, item.get("_source_engine", ""))
            if parsed_dt is None:
                undated.append(item)
                continue

            item["_published_at_utc"] = parsed_dt
            if parsed_dt >= cutoff:
                recent.append(item)
            # else: too old, silently discard

        return recent, undated

    def _parse_search_date(self, date_str: str, source_engine: str) -> datetime | None:
        """Parse a date string from a search result into a UTC-aware datetime.

        Handles multiple formats with timezone awareness:
        - ISO 8601: "2026-04-02T10:30:00Z" or "2026-04-02"
        - Chinese: "2026年4月2日" or "2026-04-02"
        - Relative: "3小时前", "1天前" (approximate)
        """
        from dateutil import parser as dateutil_parser

        if not date_str:
            return None

        # Handle relative Chinese dates: "X小时前", "X天前"
        if "前" in date_str:
            return self._parse_relative_date(date_str)

        try:
            # Try standard ISO parse first
            dt = dateutil_parser.parse(date_str, fuzzy=True)

            # Timezone assignment
            if dt.tzinfo is None:
                if source_engine == "baidu":
                    # Baidu returns CST dates
                    dt = dt.replace(tzinfo=CST)
                else:
                    # Tavily, Jina, others: assume UTC
                    dt = dt.replace(tzinfo=timezone.utc)

            # Convert to UTC
            return dt.astimezone(timezone.utc)

        except (ValueError, OverflowError):
            return None

    def _parse_relative_date(self, date_str: str) -> datetime | None:
        """Parse Chinese relative dates like '3小时前', '1天前', '30分钟前'."""
        import re
        now_utc = datetime.now(timezone.utc)

        m = re.search(r"(\d+)\s*小时前", date_str)
        if m:
            return now_utc - timedelta(hours=int(m.group(1)))

        m = re.search(r"(\d+)\s*天前", date_str)
        if m:
            return now_utc - timedelta(days=int(m.group(1)))

        m = re.search(r"(\d+)\s*分钟前", date_str)
        if m:
            return now_utc - timedelta(minutes=int(m.group(1)))

        return None

    async def _resolve_undated_items(
        self,
        items: list[dict],
        content_fetcher,
        cutoff: datetime,
    ) -> list[dict]:
        """Fetch pages for undated items to extract published_at."""
        import asyncio

        resolved = []

        async def _try_resolve(item: dict) -> dict | None:
            url = item.get("url", "")
            if not url:
                return None
            try:
                result = await asyncio.wait_for(
                    content_fetcher.fetch(url),
                    timeout=15,
                )
                # ContentFetcher returns (text, published_at, reason)
                if isinstance(result, tuple) and len(result) >= 2:
                    pub_dt = result[1]
                    if pub_dt:
                        if pub_dt.tzinfo is None:
                            pub_dt = pub_dt.replace(tzinfo=timezone.utc)
                        pub_dt = pub_dt.astimezone(timezone.utc)
                        if pub_dt >= cutoff:
                            item["_published_at_utc"] = pub_dt
                            item["_date_verified"] = True
                            return item
            except Exception:
                pass
            return None

        tasks = [_try_resolve(item) for item in items]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, dict):
                resolved.append(r)

        return resolved

    # ------------------------------------------------------------------
    # Formatting
    # ------------------------------------------------------------------

    def format_for_llm(
        self, result: DataSourceResult, holding: PortfolioHolding | None = None,
    ) -> str:
        """Format search results for LLM, showing publication dates."""
        items = result.items
        if not items:
            return ""

        name = holding.name_cn if holding else ""
        ticker = holding.ticker if holding else ""

        lines = [f"【外部搜索结果 — {name} ({ticker})】"]
        lines.append(f"共找到 {len(items)} 条近期结果 (24小时内)\n")

        now_utc = datetime.now(timezone.utc)
        for i, item in enumerate(items[:20], 1):
            title = item.get("title", "")[:100]
            url = item.get("url", "")
            content = item.get("content", "")[:500]
            source = item.get("source", item.get("_source_engine", ""))
            website = item.get("website", "")

            # Format publication time
            pub_dt = item.get("_published_at_utc")
            if pub_dt:
                age = now_utc - pub_dt
                age_hours = age.total_seconds() / 3600
                # Display in CST for Chinese context
                pub_cst = pub_dt.astimezone(CST)
                time_str = f"{pub_cst.strftime('%m-%d %H:%M')} CST ({age_hours:.0f}h前)"
            else:
                date_str = item.get("date", "")
                time_str = date_str if date_str else "时间未知"

            source_tag = f"[{source}]" if source else ""

            lines.append(f"[{i}] {title} — {website}")
            lines.append(f"    发布: {time_str} {source_tag}")
            if content:
                lines.append(f"    {content}")
            lines.append(f"    URL: {url}")
            lines.append("")

        return "\n".join(lines)
