"""Internal platform data source — queries all tagged content from the database.

This is our information advantage: AlphaPai (articles, comments, roadshows),
Jiuqian (forum, minutes, wechat), and News Center (already-analyzed news).
All content is tagged with stock tickers via LLM enrichment.

Uses asyncpg (raw SQL) matching the engine's database access pattern.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone

from engine.proactive.data_sources.base import DataSourcePlugin, DataSourceResult
from engine.proactive.models import PortfolioHolding, StockBaseline

logger = logging.getLogger(__name__)


def _escape_like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


class InternalDBPlugin(DataSourcePlugin):
    """Query all 8 internal data tables for content tagged with a stock."""

    name = "internal_db"

    def __init__(self, db, lookback_hours: int = 26):
        """
        Args:
            db: PostgresDatabase instance with db._pool as asyncpg.Pool
            lookback_hours: How far back to search (v3: 26h, slightly wider
                than external 24h to account for platform ingestion delay).
                Uses absolute window from now, not relative to last scan.
        """
        self._db = db
        self._lookback_hours = lookback_hours

    async def fetch(
        self,
        holding: PortfolioHolding,
        baseline: StockBaseline,
        **kwargs,
    ) -> DataSourceResult:
        pool = self._db._pool
        if not pool:
            return DataSourceResult(source_name=self.name)

        # v3: Absolute time window from now (not relative to last scan)
        # This ensures we only see recent items regardless of scan history
        cutoff = datetime.now(timezone.utc) - timedelta(hours=self._lookback_hours)
        if cutoff.tzinfo is None:
            cutoff = cutoff.replace(tzinfo=timezone.utc)

        known_ids = baseline.known_content_ids
        search_terms = holding.search_terms

        all_items: list[dict] = []

        # Build the LIKE conditions for ticker matching in JSONB enrichment
        like_conditions = []
        title_cn_conditions = []  # Chinese name fallback (table-specific title column)
        for term in search_terms:
            safe = _escape_like(term)
            like_conditions.append(f"CAST(enrichment->'tickers' AS TEXT) ILIKE '%{safe}%'")
            if len(term) >= 2 and not term.isascii():
                title_cn_conditions.append(safe)

        if not like_conditions:
            return DataSourceResult(source_name=self.name)

        # Base condition: match enrichment tickers (works for all tables)
        ticker_where = " OR ".join(like_conditions)

        def _with_title(base_where: str, title_col: str) -> str:
            """Add title fallback conditions for Chinese name terms."""
            if not title_cn_conditions:
                return base_where
            extras = [f"{title_col} ILIKE '%{s}%'" for s in title_cn_conditions]
            return f"({base_where} OR {' OR '.join(extras)})"

        # --- 1. AlphaPai Articles ---
        items = await self._query_alphapai_articles(pool, _with_title(ticker_where, "arc_name"), cutoff)
        all_items.extend(items)

        # --- 2. AlphaPai Comments ---
        items = await self._query_alphapai_comments(pool, _with_title(ticker_where, "title"), cutoff)
        all_items.extend(items)

        # --- 3. AlphaPai Roadshows CN ---
        items = await self._query_roadshows_cn(pool, _with_title(ticker_where, "show_title"), cutoff)
        all_items.extend(items)

        # --- 4. AlphaPai Roadshows US ---
        items = await self._query_roadshows_us(pool, _with_title(ticker_where, "show_title"), cutoff)
        all_items.extend(items)

        # --- 5. Jiuqian Forum ---
        items = await self._query_jiuqian_forum(pool, _with_title(ticker_where, "title"))
        all_items.extend(items)

        # --- 6. Jiuqian Minutes ---
        items = await self._query_jiuqian_minutes(pool, _with_title(ticker_where, "title"), cutoff)
        all_items.extend(items)

        # --- 7. Jiuqian WeChat ---
        items = await self._query_jiuqian_wechat(pool, _with_title(ticker_where, "title"), cutoff)
        all_items.extend(items)

        # --- 8. News Center (already analyzed) ---
        items = await self._query_news_center(pool, search_terms, cutoff)
        all_items.extend(items)

        # Compute new vs known
        new_items = [i for i in all_items if i.get("id", "") not in known_ids]
        total = len(all_items)
        new_count = len(new_items)

        # Sentiment distribution
        sentiment_dist: dict[str, int] = {}
        for item in new_items:
            s = item.get("sentiment", "neutral") or "neutral"
            sentiment_dist[s] = sentiment_dist.get(s, 0) + 1

        # Group by source
        source_items: dict[str, list[dict]] = {}
        for item in all_items:
            src = item.get("source", "unknown")
            source_items.setdefault(src, []).append(item)

        result = DataSourceResult(
            source_name=self.name,
            items=all_items,
            item_count=total,
            new_item_count=new_count,
            metadata={
                "sentiment_distribution": sentiment_dist,
                "source_items": source_items,
            },
        )
        result.formatted_text = self.format_for_llm(result, holding, new_items)
        return result

    def format_for_llm(
        self, result: DataSourceResult, holding: PortfolioHolding | None = None,
        new_items: list[dict] | None = None,
    ) -> str:
        """Format internal data as text for LLM consumption."""
        items = new_items if new_items is not None else result.items
        if not items:
            return ""

        name = holding.name_cn if holding else result.source_name
        ticker = holding.ticker if holding else ""

        lines = [f"【内部数据 — {name} ({ticker})】"]
        lines.append(f"共找到 {len(items)} 条新增内容\n")

        # Group by source
        by_source: dict[str, list[dict]] = {}
        for item in items:
            src = item.get("source_label", item.get("source", "unknown"))
            by_source.setdefault(src, []).append(item)

        for src_label, src_items in by_source.items():
            lines.append(f"--- {src_label} ({len(src_items)}条) ---")
            # Sort by time, newest first
            src_items.sort(key=lambda x: x.get("time", "") or "", reverse=True)
            for i, item in enumerate(src_items[:15], 1):  # Cap per source for context
                time_str = item.get("time", "")
                if time_str and len(time_str) > 16:
                    time_str = time_str[:16]
                title = item.get("title", "")[:100]
                summary = item.get("summary", "")[:300]
                sentiment = item.get("sentiment", "")
                extras = []
                if item.get("institution"):
                    extras.append(item["institution"])
                if item.get("analyst"):
                    extras.append(item["analyst"])
                if item.get("company"):
                    extras.append(item["company"])
                extra_str = f" — {', '.join(extras)}" if extras else ""

                lines.append(f"{i}. [{time_str}] {title}{extra_str}")
                if summary:
                    lines.append(f"   摘要: {summary}")
                detail_parts = []
                if sentiment:
                    detail_parts.append(f"情绪: {sentiment}")
                if item.get("impact_magnitude"):
                    detail_parts.append(f"影响: {item['impact_magnitude']}")
                if item.get("surprise_factor") is not None:
                    detail_parts.append(f"惊讶度: {item['surprise_factor']}")
                if detail_parts:
                    lines.append(f"   {' | '.join(detail_parts)}")
            lines.append("")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Individual source queries (asyncpg raw SQL)
    # ------------------------------------------------------------------

    async def _query_alphapai_articles(
        self, pool, ticker_where: str, cutoff: datetime,
    ) -> list[dict]:
        try:
            rows = await pool.fetch(f"""
                SELECT arc_code AS id, arc_name AS title, publish_time AS time,
                       enrichment, author
                FROM alphapai_articles
                WHERE publish_time >= $1
                  AND is_enriched = true
                  AND (enrichment->>'skipped' IS NULL
                       OR enrichment->>'skipped' = 'false')
                  AND ({ticker_where})
                ORDER BY publish_time DESC
                LIMIT 50
            """, cutoff)
        except Exception as e:
            logger.warning("InternalDB: alphapai_articles query failed: %s", e)
            return []

        items = []
        for r in rows:
            enr = r["enrichment"] or {}
            if isinstance(enr, str):
                enr = json.loads(enr)
            items.append({
                "source": "alphapai_wechat",
                "source_label": "AlphaPai公众号",
                "id": r["id"],
                "title": r["title"],
                "time": r["time"].isoformat() if r["time"] else None,
                "summary": enr.get("summary", ""),
                "sentiment": enr.get("sentiment", ""),
                "relevance_score": enr.get("relevance_score", 0),
                "author": r["author"],
            })
        return items

    async def _query_alphapai_comments(
        self, pool, ticker_where: str, cutoff: datetime,
    ) -> list[dict]:
        try:
            rows = await pool.fetch(f"""
                SELECT cmnt_hcode AS id, title, cmnt_date AS time,
                       enrichment, inst_cname, psn_name
                FROM alphapai_comments
                WHERE cmnt_date >= $1
                  AND is_enriched = true
                  AND ({ticker_where})
                ORDER BY cmnt_date DESC
                LIMIT 30
            """, cutoff)
        except Exception as e:
            logger.warning("InternalDB: alphapai_comments query failed: %s", e)
            return []

        items = []
        for r in rows:
            enr = r["enrichment"] or {}
            if isinstance(enr, str):
                enr = json.loads(enr)
            items.append({
                "source": "alphapai_comment",
                "source_label": "AlphaPai券商点评",
                "id": r["id"],
                "title": r["title"],
                "time": r["time"].isoformat() if r["time"] else None,
                "summary": enr.get("summary", ""),
                "sentiment": enr.get("sentiment", ""),
                "relevance_score": enr.get("relevance_score", 0),
                "institution": r["inst_cname"],
                "analyst": r["psn_name"],
            })
        return items

    async def _query_roadshows_cn(
        self, pool, ticker_where: str, cutoff: datetime,
    ) -> list[dict]:
        try:
            rows = await pool.fetch(f"""
                SELECT trans_id AS id, show_title AS title, stime AS time,
                       enrichment, company
                FROM alphapai_roadshows_cn
                WHERE stime >= $1
                  AND is_enriched = true
                  AND ({ticker_where})
                ORDER BY stime DESC
                LIMIT 20
            """, cutoff)
        except Exception as e:
            logger.warning("InternalDB: roadshows_cn query failed: %s", e)
            return []

        items = []
        for r in rows:
            enr = r["enrichment"] or {}
            if isinstance(enr, str):
                enr = json.loads(enr)
            items.append({
                "source": "alphapai_roadshow",
                "source_label": "AlphaPai路演纪要",
                "id": r["id"],
                "title": r["title"],
                "time": r["time"].isoformat() if r["time"] else None,
                "summary": enr.get("summary", ""),
                "sentiment": enr.get("sentiment", ""),
                "relevance_score": enr.get("relevance_score", 0),
                "company": r["company"],
            })
        return items

    async def _query_roadshows_us(
        self, pool, ticker_where: str, cutoff: datetime,
    ) -> list[dict]:
        try:
            rows = await pool.fetch(f"""
                SELECT trans_id AS id, show_title AS title, stime AS time,
                       enrichment, company
                FROM alphapai_roadshows_us
                WHERE stime >= $1
                  AND is_enriched = true
                  AND ({ticker_where})
                ORDER BY stime DESC
                LIMIT 20
            """, cutoff)
        except Exception as e:
            logger.warning("InternalDB: roadshows_us query failed: %s", e)
            return []

        items = []
        for r in rows:
            enr = r["enrichment"] or {}
            if isinstance(enr, str):
                enr = json.loads(enr)
            items.append({
                "source": "alphapai_roadshow_us",
                "source_label": "AlphaPai路演(US)",
                "id": r["id"],
                "title": r["title"],
                "time": r["time"].isoformat() if r["time"] else None,
                "summary": enr.get("summary", ""),
                "sentiment": enr.get("sentiment", ""),
                "relevance_score": enr.get("relevance_score", 0),
                "company": r["company"],
            })
        return items

    async def _query_jiuqian_forum(
        self, pool, ticker_where: str,
    ) -> list[dict]:
        try:
            rows = await pool.fetch(f"""
                SELECT id, title, meeting_time AS time,
                       enrichment, industry, summary
                FROM jiuqian_forum
                WHERE is_enriched = true
                  AND ({ticker_where})
                ORDER BY meeting_time DESC
                LIMIT 20
            """)
        except Exception as e:
            logger.warning("InternalDB: jiuqian_forum query failed: %s", e)
            return []

        items = []
        for r in rows:
            enr = r["enrichment"] or {}
            if isinstance(enr, str):
                enr = json.loads(enr)
            items.append({
                "source": "jiuqian_forum",
                "source_label": "久谦专家访谈",
                "id": str(r["id"]),
                "title": r["title"],
                "time": r["time"].isoformat() if r["time"] else None,
                "summary": enr.get("summary", "") or r["summary"] or "",
                "sentiment": enr.get("sentiment", ""),
                "relevance_score": enr.get("relevance_score", 0),
                "industry": r["industry"],
            })
        return items

    async def _query_jiuqian_minutes(
        self, pool, ticker_where: str, cutoff: datetime,
    ) -> list[dict]:
        try:
            rows = await pool.fetch(f"""
                SELECT id, title, pub_time AS time,
                       enrichment, source, summary
                FROM jiuqian_minutes
                WHERE pub_time >= $1
                  AND is_enriched = true
                  AND ({ticker_where})
                ORDER BY pub_time DESC
                LIMIT 30
            """, cutoff)
        except Exception as e:
            logger.warning("InternalDB: jiuqian_minutes query failed: %s", e)
            return []

        items = []
        for r in rows:
            enr = r["enrichment"] or {}
            if isinstance(enr, str):
                enr = json.loads(enr)
            items.append({
                "source": "jiuqian_minutes",
                "source_label": "久谦研究纪要",
                "id": r["id"],
                "title": r["title"],
                "time": r["time"].isoformat() if r["time"] else None,
                "summary": enr.get("summary", "") or r["summary"] or "",
                "sentiment": enr.get("sentiment", ""),
                "relevance_score": enr.get("relevance_score", 0),
            })
        return items

    async def _query_jiuqian_wechat(
        self, pool, ticker_where: str, cutoff: datetime,
    ) -> list[dict]:
        try:
            rows = await pool.fetch(f"""
                SELECT id, title, pub_time AS time,
                       enrichment, post_url
                FROM jiuqian_wechat
                WHERE pub_time >= $1
                  AND is_enriched = true
                  AND (enrichment->>'skipped' IS NULL
                       OR enrichment->>'skipped' = 'false')
                  AND ({ticker_where})
                ORDER BY pub_time DESC
                LIMIT 30
            """, cutoff)
        except Exception as e:
            logger.warning("InternalDB: jiuqian_wechat query failed: %s", e)
            return []

        items = []
        for r in rows:
            enr = r["enrichment"] or {}
            if isinstance(enr, str):
                enr = json.loads(enr)
            items.append({
                "source": "jiuqian_wechat",
                "source_label": "久谦公众号",
                "id": r["id"],
                "title": r["title"],
                "time": r["time"].isoformat() if r["time"] else None,
                "summary": enr.get("summary", "") or "",
                "sentiment": enr.get("sentiment", ""),
                "relevance_score": enr.get("relevance_score", 0),
                "url": r["post_url"] or "",
            })
        return items

    async def _query_news_center(
        self, pool, search_terms: list[str], cutoff: datetime,
    ) -> list[dict]:
        """Query news_items + analysis_results for already-analyzed content."""
        # Build conditions for affected_tickers (JSONB array in analysis_results)
        conditions = []
        for term in search_terms:
            safe = _escape_like(term)
            conditions.append(
                f"CAST(a.affected_tickers AS TEXT) ILIKE '%{safe}%'"
            )
        # Also check news title
        for term in search_terms:
            if len(term) >= 2 and not term.isascii():
                safe = _escape_like(term)
                conditions.append(f"n.title ILIKE '%{safe}%'")

        if not conditions:
            return []

        where = " OR ".join(conditions)
        try:
            rows = await pool.fetch(f"""
                SELECT n.id, n.title, n.published_at, n.fetched_at,
                       n.source_name, n.url, n.metadata,
                       a.summary, a.sentiment, a.impact_magnitude,
                       a.surprise_factor, a.affected_tickers, a.affected_sectors
                FROM news_items n
                JOIN analysis_results a ON a.news_item_id = n.id
                JOIN filter_results f ON f.news_item_id = n.id
                WHERE n.fetched_at >= $1
                  AND f.is_relevant = true
                  AND a.sentiment IS NOT NULL
                  AND ({where})
                ORDER BY n.fetched_at DESC
                LIMIT 30
            """, cutoff)
        except Exception as e:
            logger.warning("InternalDB: news_center query failed: %s", e)
            return []

        items = []
        for r in rows:
            meta = r["metadata"] or {}
            if isinstance(meta, str):
                meta = json.loads(meta)
            title = meta.get("title_zh") or r["title"]
            time = r["published_at"] or r["fetched_at"]
            items.append({
                "source": "news",
                "source_label": "资讯中心",
                "id": r["id"],
                "title": title,
                "time": time.isoformat() if time else None,
                "summary": r["summary"] or "",
                "sentiment": r["sentiment"] or "",
                "impact_magnitude": r["impact_magnitude"],
                "surprise_factor": r["surprise_factor"],
                "url": r["url"],
                "source_name": r["source_name"],
            })
        return items
