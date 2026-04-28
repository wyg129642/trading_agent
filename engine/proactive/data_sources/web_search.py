"""External web search data source — Baidu (CN) + Tavily + Jina (EN).

v3: Time-gated search. Uses Tavily days=3/topic="news" and Baidu recency="week",
then post-filters ALL results by published_at < 24 hours. Calls search APIs
directly (bypasses parallel_search) for finer control over time parameters.

v3.1 (2026-04-27): Three-layer recall hardening for the case where Tavily +
Baidu are 429-throttled and only Jina (which returns no `date` field) responds.
Without this, every undated item gets discarded by the 24h gate and the
triage stage sees zero recent news → no alerts fire.
  Layer 1: URL-path date heuristic (`/2026/04/27/`, `YYYY-MM-DD` in path).
  Layer 2: Wider content_fetcher fallback (limit 15, always-on when undated>0).
  Layer 3: Rescue mode — if recall is still 0 but we have undated items,
           keep top-3 as `_date_presumed=True` with synthetic published_at so
           triage can decide. Triage prompt sees `时间未知 (推测近期)` and
           usually returns materiality=none for stale items.
"""

from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from urllib.parse import urlparse

from engine.proactive.data_sources.base import DataSourcePlugin, DataSourceResult
from engine.proactive.models import BreakingNewsItem, PortfolioHolding, StockBaseline

logger = logging.getLogger(__name__)

CST = ZoneInfo("Asia/Shanghai")

# Per-process query cache (engine: Tavily, Baidu — Jina excluded since it's free).
# Keyed by (engine, query) → (timestamp, results). The proactive scanner runs
# the same fixed query templates every cycle (e.g. "MU Micron latest news today")
# so caching across two consecutive cycles cuts external-API volume in half
# without losing freshness — Tavily's news index updates slower than 15 min anyway.
_SEARCH_CACHE_TTL_SEC = 900   # 15 min
_SEARCH_CACHE_MAX = 2000
_search_cache: dict[tuple[str, str], tuple[float, list[dict]]] = {}


def _cache_get(engine: str, query: str) -> list[dict] | None:
    entry = _search_cache.get((engine, query))
    if not entry:
        return None
    ts, results = entry
    if (time.monotonic() - ts) > _SEARCH_CACHE_TTL_SEC:
        _search_cache.pop((engine, query), None)
        return None
    # Clone so callers' per-item mutations (e.g. `_source_engine`, `_published_at_utc`)
    # don't leak back into the cache.
    return [dict(r) for r in results]


def _cache_set(engine: str, query: str, results: list[dict]) -> None:
    if len(_search_cache) >= _SEARCH_CACHE_MAX:
        cutoff = time.monotonic() - _SEARCH_CACHE_TTL_SEC
        for k, (ts, _) in list(_search_cache.items()):
            if ts < cutoff:
                _search_cache.pop(k, None)
        if len(_search_cache) >= _SEARCH_CACHE_MAX:
            _search_cache.pop(next(iter(_search_cache)), None)
    _search_cache[(engine, query)] = (time.monotonic(), [dict(r) for r in results])


# Markets whose active hours line up roughly with US trading. Outside these
# windows we skip Tavily and rely on Baidu + Jina to conserve Tavily quota.
_TAVILY_ACTIVE_MARKETS = {"美股", "港股", "主板", "创业板", "科创板"}


def _tavily_in_active_window(market: str | None) -> bool:
    """Whether Tavily should run for this market at the current wall-clock time.

    For US stocks: only during US market hours ±2h (CST 21:30-06:30 next day).
    For HK / A-shares: only during their session ±2h (CST 07:30-17:30).
    Anything else (and weekends): skip Tavily entirely; Baidu+Jina cover the gap.
    """
    if not market:
        return False
    now_cst = datetime.now(CST)
    if now_cst.weekday() >= 5:
        return False
    minutes = now_cst.hour * 60 + now_cst.minute
    if market == "美股":
        # US session ~21:30-04:00 CST + 2h buffer either side
        return minutes >= (21 * 60 + 30 - 120) or minutes <= (4 * 60 + 120)
    if market in _TAVILY_ACTIVE_MARKETS:
        # HK / A-share: 09:30-16:00 CST + 2h buffer
        return (9 * 60 + 30 - 120) <= minutes <= (16 * 60 + 120)
    return False


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
        """Generate bilingual search queries tailored to stock characteristics.

        Two-query layout (queries_per_stock=2, default):
          q1 — combined breaking news + financial catalysts (covers earnings,
               guidance, ratings, target price changes, and headline news in
               one query — the previous separate q3 was redundant since news
               and earnings hits overlap heavily for any stock with a recent
               event).
          q2 — sector-tag specific (only when tags exist).

        At queries_per_stock=3 the legacy split (separate q1 news, q3 earnings)
        is restored for backward compatibility / debugging.
        """
        cn_queries = []
        en_queries = []

        name_cn = holding.name_cn
        name_en = holding.name_en or holding.ticker
        ticker = holding.ticker
        sector_tags = [t for t in holding.tags if t not in ("holding",)]

        if self._queries_per_stock <= 2:
            # Merged headline + earnings/rating query — fewer API calls,
            # equivalent recall in practice (Tavily news topic returns the
            # same top hits for both query templates on most days).
            cn_queries.append(f"{name_cn} 最新消息 业绩 评级")
            en_queries.append(
                f"{ticker} {name_en} latest news earnings rating today"
            )
        else:
            cn_queries.append(f"{name_cn} 最新消息 重大变化")
            en_queries.append(f"{ticker} {name_en} latest news today")

        # Query 2: Industry-specific — use tags to make queries precise
        if sector_tags:
            tag_str = " ".join(sector_tags[:2])
            cn_queries.append(f"{name_cn} {tag_str} 突破 订单 客户")
            en_queries.append(f"{ticker} {name_en} {sector_tags[0]} breakthrough order contract")

        # Query 3: only when caller explicitly opts back into the 3-query layout.
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

        # Run search API calls with intra-stock staggering to avoid
        # self-induced 429 storms on Tavily/Baidu (each query within a stock
        # should not hit the API at the exact same instant; the scanner
        # already parallelises across 6 stocks per batch).
        tasks = []

        # Baidu: stagger 0.4s/query
        for i, q in enumerate(cn_queries):
            tasks.append(self._search_baidu(q, delay=i * 0.4))

        # Tavily: only during the holding's market hours ±2h. Outside that
        # window, news momentum is low and Baidu+Jina cover the gap, so we
        # skip Tavily to conserve plan quota (the proactive scanner historically
        # burned ~25% of calls into 432 quota errors; gating by trading window
        # cuts off the off-hours bulk that drives that).
        tavily_active = _tavily_in_active_window(holding.market)
        if tavily_active:
            for i, q in enumerate(en_queries):
                tasks.append(self._search_tavily(q, delay=i * 0.3))

        # Jina: free of 429s in practice; run 1 query in parallel
        if self._jina_api_key:
            for q in en_queries[:1]:
                tasks.append(self._search_jina(q))

        try:
            all_results = await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            logger.warning("WebSearch gather failed for %s: %s", holding.ticker, e)
            return DataSourceResult(source_name=self.name)

        # Track how many engines actually returned anything — the rescue
        # path below uses this to decide whether the day is "starved".
        successful_engines: set[str] = set()
        rate_limited_engines: set[str] = set()
        for r in all_results:
            if isinstance(r, list) and r:
                eng = r[0].get("_source_engine", "")
                if eng:
                    successful_engines.add(eng)
            elif isinstance(r, dict) and r.get("_rate_limited"):
                rate_limited_engines.add(r.get("_source_engine", ""))

        # Flatten and dedup by URL (skip rate-limit markers and exceptions)
        all_items = []
        seen_urls: set[str] = set()
        for result_or_exc in all_results:
            if isinstance(result_or_exc, Exception):
                logger.debug("Search task error: %s", result_or_exc)
                continue
            if not isinstance(result_or_exc, list):
                continue
            for item in result_or_exc:
                url = item.get("url", "")
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    all_items.append(item)

        # Post-filter by publication date (24-hour window)
        now_utc = datetime.now(timezone.utc)
        cutoff = now_utc - timedelta(hours=self._window_hours)
        recent_items, undated_items = self._post_filter_by_recency(all_items, cutoff)

        url_recovered = sum(1 for r in recent_items if r.get("_date_from_url"))
        fetcher_recovered = 0
        rescued = 0

        # Layer 2: ContentFetcher fallback. Drop the previous `<3 recent`
        # gate — when Tavily/Baidu are 429-throttled, recent_items=0 with
        # 5 undated, the gate fires but only on 5 URLs. Many Jina URLs are
        # JS-heavy and ContentFetcher returns no date. Widen the budget to
        # 15 so we have a real chance of recovering at least one.
        if undated_items and content_fetcher:
            resolved = await self._resolve_undated_items(
                undated_items[:15], content_fetcher, cutoff,
            )
            fetcher_recovered = len(resolved)
            recent_items.extend(resolved)
            # Items resolved via fetch should not be re-considered
            resolved_urls = {r.get("url", "") for r in resolved}
            undated_items = [
                u for u in undated_items if u.get("url", "") not in resolved_urls
            ]

        # Layer 3: Rescue mode. If we still have zero recent items but
        # Jina/etc returned undated content AND at least one engine looked
        # rate-limited (or only Jina succeeded), keep top-3 undated as
        # `_date_presumed=True` with synthetic published_at = now-12h so
        # the time gate downstream lets them pass to triage. The triage
        # LLM sees a `时间未知 (推测近期)` marker and the item content; if
        # the news is actually stale or off-topic it will return
        # materiality=none. Worst case we waste one triage LLM call per
        # stock per cycle; best case we resurrect a real signal that
        # would otherwise be silently discarded.
        starved = (
            len(recent_items) == 0
            and len(undated_items) > 0
            and (
                bool(rate_limited_engines)
                or successful_engines.issubset({"jina"})
            )
        )
        if starved:
            presumed_age = now_utc - timedelta(hours=12)
            for item in undated_items[:3]:
                item["_published_at_utc"] = presumed_age
                item["_date_presumed"] = True
                recent_items.append(item)
            rescued = min(3, len(undated_items))

        logger.info(
            "[WebSearch:%s] %d total → %d recent (<%dh), %d undated"
            " | engines ok=%s rate-limited=%s | recovered url=%d fetch=%d rescued=%d",
            holding.ticker,
            len(all_items),
            len(recent_items),
            self._window_hours,
            len(undated_items),
            ",".join(sorted(successful_engines)) or "none",
            ",".join(sorted(rate_limited_engines)) or "none",
            url_recovered,
            fetcher_recovered,
            rescued,
        )

        result = DataSourceResult(
            source_name=self.name,
            items=recent_items,
            item_count=len(recent_items),
            new_item_count=len(recent_items),
            metadata={
                "total_before_filter": len(all_items),
                "url_recovered": url_recovered,
                "fetcher_recovered": fetcher_recovered,
                "rescued": rescued,
                "engines_ok": sorted(successful_engines),
                "engines_rate_limited": sorted(rate_limited_engines),
            },
        )
        result.formatted_text = self.format_for_llm(result, holding)
        return result

    # ------------------------------------------------------------------
    # Search API wrappers with time parameters
    # ------------------------------------------------------------------

    async def _search_baidu(self, query: str, delay: float = 0.0) -> list[dict] | dict:
        """Returns either a list of results, or a `{_rate_limited: True}` marker
        so the caller can detect 429 storms."""
        import asyncio
        from engine.tools.web_search import baidu_search

        cached = _cache_get("baidu", query)
        if cached is not None:
            for r in cached:
                r["_source_engine"] = "baidu"
            return cached

        if delay > 0:
            await asyncio.sleep(delay)
        try:
            results = await baidu_search(
                query=query,
                api_key=self._baidu_api_key,
                max_results=10,
                recency="week",  # Finest Baidu granularity
            )
            if results is None:
                return {"_rate_limited": True, "_source_engine": "baidu"}
            _cache_set("baidu", query, results)
            for r in results:
                r["_source_engine"] = "baidu"
            return results
        except Exception as e:
            msg = str(e)
            if "429" in msg or "Too Many Requests" in msg:
                return {"_rate_limited": True, "_source_engine": "baidu"}
            logger.debug("Baidu search error: %s", e)
            return []

    async def _search_tavily(self, query: str, delay: float = 0.0) -> list[dict] | dict:
        import asyncio
        from engine.tools.web_search import tavily_search

        cached = _cache_get("tavily", query)
        if cached is not None:
            for r in cached:
                r["_source_engine"] = "tavily"
            return cached

        if delay > 0:
            await asyncio.sleep(delay)
        try:
            results = await tavily_search(
                query=query,
                api_key=self._tavily_api_key,
                max_results=10,
                days=1,          # Last 24h — tighter window, fresher results
                topic="news",    # News-focused results
            )
            if results is None:
                return {"_rate_limited": True, "_source_engine": "tavily"}
            _cache_set("tavily", query, results)
            for r in results:
                r["_source_engine"] = "tavily"
            return results
        except Exception as e:
            msg = str(e)
            if "429" in msg or "Too Many Requests" in msg:
                return {"_rate_limited": True, "_source_engine": "tavily"}
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
        - Jina: typically no date — falls back to URL-path heuristic
        """
        recent = []
        undated = []

        for item in items:
            parsed_dt = None

            date_str = (item.get("date") or "").strip()
            if date_str:
                parsed_dt = self._parse_search_date(
                    date_str, item.get("_source_engine", ""),
                )

            # Layer 1: URL-path date heuristic for items without a
            # parseable `date` field (most Jina results, some Tavily
            # general-topic results). Many news outlets encode the
            # publish date in the URL path.
            if parsed_dt is None:
                url = item.get("url") or ""
                parsed_dt = self._extract_date_from_url(url)
                if parsed_dt is not None:
                    item["_date_from_url"] = True

            if parsed_dt is None:
                undated.append(item)
                continue

            item["_published_at_utc"] = parsed_dt
            if parsed_dt >= cutoff:
                recent.append(item)
            # else: dated and too old, silently discard

        return recent, undated

    # Compiled once. Captures the date components from URL paths like:
    #   /2026/04/27/some-article
    #   /news/2026-04-27/some-article
    #   /2026-04-27_some-article.html
    _URL_DATE_PATTERNS = [
        re.compile(r"/(\d{4})/(\d{1,2})/(\d{1,2})(?:/|[-_]|\b)"),
        re.compile(r"[/_-](\d{4})-(\d{1,2})-(\d{1,2})(?:[/.\-_]|\b)"),
    ]

    def _extract_date_from_url(self, url: str) -> datetime | None:
        """Best-effort: pull a YYYY-MM-DD from the URL path, treat as
        midnight UTC. Cheap, no I/O, and works for many news outlets
        (cnbc, reuters, bloomberg, etc.) where Jina returns no date."""
        if not url:
            return None
        try:
            path = urlparse(url).path
        except (ValueError, AttributeError):
            return None
        if not path:
            return None
        for pat in self._URL_DATE_PATTERNS:
            m = pat.search(path)
            if not m:
                continue
            try:
                year, month, day = (int(m.group(i)) for i in (1, 2, 3))
            except (ValueError, IndexError):
                continue
            # Sanity-bound to plausible publish-date range
            if not (2000 <= year <= 2100):
                continue
            if not (1 <= month <= 12 and 1 <= day <= 31):
                continue
            try:
                # Treat the URL date as 12:00 UTC of that day — a midpoint
                # so a story posted "today" still falls inside a 24h window
                # regardless of which timezone the publisher used.
                return datetime(year, month, day, 12, 0, tzinfo=timezone.utc)
            except ValueError:
                continue
        return None

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
                pub_cst = pub_dt.astimezone(CST)
                if item.get("_date_presumed"):
                    # Rescue-mode item: time unverified, flag clearly so triage
                    # LLM does not treat synthetic timestamp as authoritative.
                    time_str = "时间未知 (推测近期)"
                elif item.get("_date_from_url"):
                    time_str = (
                        f"{pub_cst.strftime('%m-%d')} CST "
                        f"(~{age_hours:.0f}h前, URL推断)"
                    )
                else:
                    time_str = (
                        f"{pub_cst.strftime('%m-%d %H:%M')} CST "
                        f"({age_hours:.0f}h前)"
                    )
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
