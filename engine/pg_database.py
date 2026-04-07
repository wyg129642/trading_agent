"""PostgreSQL database adapter for the trading engine.

Drop-in replacement for engine.database.Database that writes to the same
PostgreSQL tables used by the FastAPI backend, so all data flows through
a single database and is immediately visible on the web frontend.

Usage in engine/main.py:
    if os.getenv("DATABASE_URL"):
        from engine.pg_database import PostgresDatabase
        db = PostgresDatabase(os.environ["DATABASE_URL"])
    else:
        from engine.database import Database
        db = Database(db_path)
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone

import asyncpg

from engine.models import AnalysisResult, FilterResult, NewsItem, ResearchReport, SourceHealth

logger = logging.getLogger(__name__)


def _ensure_tz(dt: datetime | None) -> datetime | None:
    """Ensure a datetime has timezone info (UTC default) for TIMESTAMPTZ."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _parse_json(val) -> list | dict:
    """Parse a JSON string if needed, or return as-is if already a list/dict."""
    if isinstance(val, (list, dict)):
        return val
    if isinstance(val, str):
        try:
            return json.loads(val)
        except (json.JSONDecodeError, TypeError):
            return []
    return []


class _PgConnectionProxy:
    """Minimal proxy so TokenTracker.persist_to_db / load_today_from_db work.

    TokenTracker accesses db._db.execute() / executemany() / commit()
    with SQLite-style ?-placeholder SQL.  This proxy translates to asyncpg.
    """

    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    @staticmethod
    def _translate_sql(sql: str) -> str:
        """Replace ? placeholders with $1, $2, ... for asyncpg."""
        out, idx = [], 0
        for ch in sql:
            if ch == "?":
                idx += 1
                out.append(f"${idx}")
            else:
                out.append(ch)
        return "".join(out)

    def execute(self, sql: str, params=None):
        """Return an async-context-manager compatible with aiosqlite's pattern:
        ``async with db.execute(sql, params) as cursor:``
        """
        translated = self._translate_sql(sql)
        coerced = self._coerce_params(params) if params else None
        return _CursorProxy(self._pool, translated, coerced)

    @staticmethod
    def _coerce_params(params):
        """Convert ISO-string timestamps and other types for asyncpg."""
        from datetime import datetime, timezone
        out = []
        for v in params:
            if isinstance(v, str) and len(v) >= 19 and v[4:5] == '-' and v[10:11] == 'T':
                try:
                    dt = datetime.fromisoformat(v)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    out.append(dt)
                    continue
                except (ValueError, TypeError):
                    pass
            out.append(v)
        return tuple(out)

    async def executemany(self, sql: str, args_list):
        translated = self._translate_sql(sql)
        args_tuples = [self._coerce_params(a) for a in args_list]
        await self._pool.executemany(translated, args_tuples)

    async def commit(self):
        pass  # asyncpg auto-commits


class _CursorProxy:
    """Async-context-manager proxy compatible with aiosqlite cursor pattern.

    Supports both:
        async with db.execute(sql) as cursor:  # used by load_today_from_db
            rows = await cursor.fetchall()

        await db.execute(sql, params)  # used for DML (result unused)
    """

    def __init__(self, pool: asyncpg.Pool, sql: str, params):
        self._pool = pool
        self._sql = sql
        self._params = params
        self._rows = None

    def __await__(self):
        """Allow ``await db.execute(...)`` for DML statements."""
        return self._execute().__await__()

    async def _execute(self):
        if self._params:
            await self._pool.execute(self._sql, *self._params)
        else:
            await self._pool.execute(self._sql)
        return self

    async def __aenter__(self):
        """Execute query and return self as cursor."""
        if self._params:
            self._rows = await self._pool.fetch(self._sql, *self._params)
        else:
            self._rows = await self._pool.fetch(self._sql)
        return self

    async def __aexit__(self, *exc):
        pass

    async def fetchone(self):
        if self._rows:
            return tuple(self._rows[0].values())
        return None

    async def fetchall(self):
        return [tuple(r.values()) for r in (self._rows or [])]

    @property
    def description(self):
        if self._rows:
            return [(k,) for k in self._rows[0].keys()]
        return []


class PostgresDatabase:
    """Async PostgreSQL database manager — same interface as engine.database.Database."""

    def __init__(self, dsn: str, redis_url: str | None = None):
        self.dsn = dsn
        self._pool: asyncpg.Pool | None = None
        self._redis = None
        self._redis_url = redis_url
        # Expose proxy for token_tracker compatibility
        self._db: _PgConnectionProxy | None = None

    async def initialize(self) -> None:
        """Connect to PostgreSQL.  Tables are managed by Alembic migrations."""
        # Convert SQLAlchemy-style URL to asyncpg-style
        dsn = self.dsn
        if dsn.startswith("postgresql+asyncpg://"):
            dsn = dsn.replace("postgresql+asyncpg://", "postgresql://", 1)

        self._pool = await asyncpg.create_pool(
            dsn, min_size=2, max_size=15, command_timeout=30,
        )
        self._db = _PgConnectionProxy(self._pool)

        # Optional Redis for broadcasting new analysis to WebSocket
        if self._redis_url:
            try:
                import redis.asyncio as aioredis
                self._redis = aioredis.from_url(self._redis_url, decode_responses=True)
                logger.info("Engine Redis connected for event broadcasting")
            except Exception as e:
                logger.warning("Redis connection failed (WebSocket updates disabled): %s", e)

        logger.info("Engine PostgreSQL connected: pool_size=2..15")

    async def close(self) -> None:
        if self._redis:
            await self._redis.close()
        if self._pool:
            await self._pool.close()

    # ─── News Items ──────────────────────────────────────────────────

    async def get_content_hashes_by_source(self, source_name: str) -> set[str]:
        rows = await self._pool.fetch(
            "SELECT content_hash FROM news_items WHERE source_name = $1",
            source_name,
        )
        return {r["content_hash"] for r in rows}

    async def get_newest_item_by_source(self, source_name: str) -> dict | None:
        row = await self._pool.fetchrow(
            """SELECT id, source_name, title, url, content, content_hash,
                      published_at, fetched_at, language, market, metadata
               FROM news_items WHERE source_name = $1
               ORDER BY fetched_at DESC LIMIT 1""",
            source_name,
        )
        if not row:
            return None
        result = dict(row)
        # Convert timestamps back to ISO strings for engine compatibility
        for k in ("published_at", "fetched_at"):
            if result[k] is not None:
                result[k] = result[k].isoformat()
        if isinstance(result.get("metadata"), dict):
            result["metadata"] = json.dumps(result["metadata"], ensure_ascii=False)
        return result

    async def get_latest_published_at(self, source_name: str) -> datetime | None:
        row = await self._pool.fetchrow(
            "SELECT MAX(published_at) as max_ts FROM news_items WHERE source_name = $1 AND published_at IS NOT NULL",
            source_name,
        )
        if row and row["max_ts"]:
            dt = row["max_ts"]
            if dt.tzinfo:
                return dt.replace(tzinfo=None)  # engine expects naive datetime
            return dt
        return None

    async def is_duplicate(self, content_hash: str) -> bool:
        row = await self._pool.fetchrow(
            "SELECT 1 FROM news_items WHERE content_hash = $1", content_hash,
        )
        return row is not None

    async def save_news_item(self, item: NewsItem) -> bool:
        if await self.is_duplicate(item.content_hash):
            return False
        try:
            metadata = item.metadata
            if isinstance(metadata, str):
                try:
                    metadata = json.loads(metadata)
                except (json.JSONDecodeError, TypeError):
                    metadata = {}

            await self._pool.execute(
                """INSERT INTO news_items
                   (id, source_name, title, url, content, content_hash,
                    published_at, fetched_at, language, market, metadata)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                   ON CONFLICT (content_hash) DO NOTHING""",
                item.id,
                item.source_name,
                item.title,
                item.url,
                item.content or "",
                item.content_hash,
                _ensure_tz(item.published_at),
                _ensure_tz(item.fetched_at),
                item.language or "zh",
                item.market or "china",
                json.dumps(metadata, ensure_ascii=False),
            )
            return True
        except asyncpg.UniqueViolationError:
            return False

    async def save_filter_result(self, result: FilterResult) -> None:
        await self._pool.execute(
            """INSERT INTO filter_results
               (news_item_id, is_relevant, relevance_score, reason, filtered_at)
               VALUES ($1, $2, $3, $4, $5)
               ON CONFLICT (news_item_id) DO UPDATE SET
                 is_relevant = EXCLUDED.is_relevant,
                 relevance_score = EXCLUDED.relevance_score,
                 reason = EXCLUDED.reason,
                 filtered_at = EXCLUDED.filtered_at""",
            result.news_item_id,
            bool(result.is_relevant),
            float(result.relevance_score),
            result.reason or "",
            _ensure_tz(datetime.now()),
        )

    async def save_analysis_result(self, result: AnalysisResult) -> None:
        affected_tickers = _parse_json(result.affected_tickers)
        affected_sectors = _parse_json(result.affected_sectors)
        key_facts = _parse_json(result.key_facts)
        research_questions = _parse_json(result.research_questions)
        quantified_evidence = _parse_json(getattr(result, "quantified_evidence", []))
        concept_tags = _parse_json(getattr(result, "concept_tags", []))
        industry_tags = _parse_json(getattr(result, "industry_tags", []))
        ticker_sentiments = getattr(result, "ticker_sentiments", {})
        if isinstance(ticker_sentiments, str):
            try:
                ticker_sentiments = json.loads(ticker_sentiments)
            except (json.JSONDecodeError, TypeError):
                ticker_sentiments = {}
        sector_sentiments = getattr(result, "sector_sentiments", {})
        if isinstance(sector_sentiments, str):
            try:
                sector_sentiments = json.loads(sector_sentiments)
            except (json.JSONDecodeError, TypeError):
                sector_sentiments = {}

        await self._pool.execute(
            """INSERT INTO analysis_results
               (news_item_id, sentiment, impact_magnitude, impact_timeframe,
                affected_tickers, affected_sectors, category, summary, key_facts,
                bull_case, bear_case, requires_deep_research, research_questions,
                analyzed_at, model_used,
                surprise_factor, is_routine, market_expectation, quantified_evidence,
                concept_tags, industry_tags, ticker_sentiments, sector_sentiments)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23)
               ON CONFLICT (news_item_id) DO UPDATE SET
                 sentiment = EXCLUDED.sentiment,
                 impact_magnitude = EXCLUDED.impact_magnitude,
                 impact_timeframe = EXCLUDED.impact_timeframe,
                 affected_tickers = EXCLUDED.affected_tickers,
                 affected_sectors = EXCLUDED.affected_sectors,
                 category = EXCLUDED.category,
                 summary = EXCLUDED.summary,
                 key_facts = EXCLUDED.key_facts,
                 bull_case = EXCLUDED.bull_case,
                 bear_case = EXCLUDED.bear_case,
                 requires_deep_research = EXCLUDED.requires_deep_research,
                 research_questions = EXCLUDED.research_questions,
                 analyzed_at = EXCLUDED.analyzed_at,
                 model_used = EXCLUDED.model_used,
                 surprise_factor = EXCLUDED.surprise_factor,
                 is_routine = EXCLUDED.is_routine,
                 market_expectation = EXCLUDED.market_expectation,
                 quantified_evidence = EXCLUDED.quantified_evidence,
                 concept_tags = EXCLUDED.concept_tags,
                 industry_tags = EXCLUDED.industry_tags,
                 ticker_sentiments = EXCLUDED.ticker_sentiments,
                 sector_sentiments = EXCLUDED.sector_sentiments""",
            result.news_item_id,
            result.sentiment or "neutral",
            result.impact_magnitude or "low",
            result.impact_timeframe or "short_term",
            json.dumps(affected_tickers, ensure_ascii=False),
            json.dumps(affected_sectors, ensure_ascii=False),
            result.category or "other",
            result.summary or "",
            json.dumps(key_facts, ensure_ascii=False),
            result.bull_case or "",
            result.bear_case or "",
            bool(result.requires_deep_research),
            json.dumps(research_questions, ensure_ascii=False),
            _ensure_tz(result.analyzed_at),
            result.model_used or "",
            float(getattr(result, "surprise_factor", 0.5)),
            bool(getattr(result, "is_routine", False)),
            getattr(result, "market_expectation", "") or "",
            json.dumps(quantified_evidence, ensure_ascii=False),
            json.dumps(concept_tags, ensure_ascii=False),
            json.dumps(industry_tags, ensure_ascii=False),
            json.dumps(ticker_sentiments, ensure_ascii=False),
            json.dumps(sector_sentiments, ensure_ascii=False),
        )

        # Broadcast to WebSocket via Redis
        if self._redis:
            try:
                # Build payload matching backend's news list format
                news_row = await self._pool.fetchrow(
                    "SELECT title, source_name, published_at, fetched_at FROM news_items WHERE id = $1",
                    result.news_item_id,
                )
                if news_row:
                    event = json.dumps({
                        "id": result.news_item_id,
                        "title": news_row["title"],
                        "source_name": news_row["source_name"],
                        "published_at": news_row["published_at"].isoformat() if news_row["published_at"] else None,
                        "fetched_at": news_row["fetched_at"].isoformat() if news_row["fetched_at"] else None,
                        "sentiment": result.sentiment,
                        "impact_magnitude": result.impact_magnitude,
                        "surprise_factor": float(getattr(result, "surprise_factor", 0.5)),
                        "summary": result.summary or "",
                        "affected_tickers": affected_tickers,
                        "affected_sectors": affected_sectors,
                        "category": result.category,
                        "concept_tags": concept_tags,
                        "industry_tags": industry_tags,
                        "ticker_sentiments": ticker_sentiments,
                        "sector_sentiments": sector_sentiments,
                        "is_read": False,
                    }, ensure_ascii=False)
                    await self._redis.publish("news:analyzed", event)
            except Exception as e:
                logger.debug("Redis publish failed (non-critical): %s", e)

    async def save_research_report(self, report: ResearchReport) -> None:
        market_snapshot = report.market_data_snapshot
        if isinstance(market_snapshot, str):
            try:
                market_snapshot = json.loads(market_snapshot)
            except (json.JSONDecodeError, TypeError):
                market_snapshot = {}
        elif not isinstance(market_snapshot, dict):
            market_snapshot = {}

        # Extract deep_research_data from full_report (which now contains the JSON)
        deep_research_data = {}
        full_report_str = report.full_report or ""
        if full_report_str:
            try:
                parsed = json.loads(full_report_str)
                if isinstance(parsed, dict) and "citations" in parsed:
                    deep_research_data = parsed
            except (json.JSONDecodeError, TypeError):
                pass

        await self._pool.execute(
            """INSERT INTO research_reports
               (news_item_id, executive_summary, context, affected_securities,
                historical_precedent, bull_scenario, bear_scenario,
                recommended_actions, risk_factors, confidence, full_report,
                market_data_snapshot, deep_research_data, researched_at, model_used)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
               ON CONFLICT (news_item_id) DO UPDATE SET
                 executive_summary = EXCLUDED.executive_summary,
                 context = EXCLUDED.context,
                 affected_securities = EXCLUDED.affected_securities,
                 historical_precedent = EXCLUDED.historical_precedent,
                 bull_scenario = EXCLUDED.bull_scenario,
                 bear_scenario = EXCLUDED.bear_scenario,
                 recommended_actions = EXCLUDED.recommended_actions,
                 risk_factors = EXCLUDED.risk_factors,
                 confidence = EXCLUDED.confidence,
                 full_report = EXCLUDED.full_report,
                 market_data_snapshot = EXCLUDED.market_data_snapshot,
                 deep_research_data = EXCLUDED.deep_research_data,
                 researched_at = EXCLUDED.researched_at,
                 model_used = EXCLUDED.model_used""",
            report.news_item_id,
            report.executive_summary or "",
            report.context or "",
            report.affected_securities or "",
            report.historical_precedent or "",
            report.bull_scenario or "",
            report.bear_scenario or "",
            report.recommended_actions or "",
            report.risk_factors or "",
            float(report.confidence or 0.0),
            full_report_str,
            json.dumps(market_snapshot, ensure_ascii=False),
            json.dumps(deep_research_data, ensure_ascii=False),
            _ensure_tz(report.researched_at),
            report.model_used or "",
        )

    async def update_news_content(
        self, item_id: str, content: str,
        metadata: dict | None = None, published_at: datetime | None = None,
    ) -> None:
        parts = ["content = $1"]
        params: list = [content]
        idx = 2
        if metadata:
            parts.append(f"metadata = ${idx}")
            params.append(json.dumps(metadata, ensure_ascii=False))
            idx += 1
        if published_at:
            parts.append(f"published_at = ${idx}")
            params.append(_ensure_tz(published_at))
            idx += 1
        params.append(item_id)
        sql = f"UPDATE news_items SET {', '.join(parts)} WHERE id = ${idx}"
        await self._pool.execute(sql, *params)

    # ─── Source Health ────────────────────────────────────────────────

    async def update_source_health(self, health: SourceHealth) -> None:
        await self._pool.execute(
            """INSERT INTO source_health
               (source_name, last_success, last_failure, consecutive_failures,
                total_items_fetched, is_healthy)
               VALUES ($1, $2, $3, $4, $5, $6)
               ON CONFLICT (source_name) DO UPDATE SET
                 last_success = EXCLUDED.last_success,
                 last_failure = EXCLUDED.last_failure,
                 consecutive_failures = EXCLUDED.consecutive_failures,
                 total_items_fetched = EXCLUDED.total_items_fetched,
                 is_healthy = EXCLUDED.is_healthy""",
            health.source_name,
            _ensure_tz(health.last_success),
            _ensure_tz(health.last_failure),
            health.consecutive_failures,
            health.total_items_fetched,
            bool(health.is_healthy),
        )

    # ─── Queries ─────────────────────────────────────────────────────

    async def get_recent_news(self, limit: int = 50) -> list[dict]:
        rows = await self._pool.fetch(
            """SELECT n.*, a.sentiment, a.impact_magnitude, a.summary
               FROM news_items n
               LEFT JOIN analysis_results a ON n.id = a.news_item_id
               ORDER BY n.fetched_at DESC LIMIT $1""",
            limit,
        )
        return [dict(r) for r in rows]

    async def search_news(self, query: str, limit: int = 20) -> list[dict]:
        rows = await self._pool.fetch(
            """SELECT n.*, a.sentiment, a.impact_magnitude, a.summary
               FROM news_items n
               LEFT JOIN analysis_results a ON n.id = a.news_item_id
               WHERE n.title ILIKE $1 OR n.content ILIKE $1
               ORDER BY n.fetched_at DESC LIMIT $2""",
            f"%{query}%", limit,
        )
        return [dict(r) for r in rows]

    async def get_recent_news_for_tickers(self, tickers: list[str], hours: int = 72, limit: int = 10) -> list[dict]:
        if not tickers:
            return []
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        # Build OR conditions for JSONB containment and title match
        conditions = []
        params: list = []
        idx = 1
        for t in tickers:
            conditions.append(f"a.affected_tickers::text ILIKE ${idx}")
            params.append(f"%{t}%")
            idx += 1
            conditions.append(f"n.title ILIKE ${idx}")
            params.append(f"%{t}%")
            idx += 1
        where = " OR ".join(conditions)
        params.extend([cutoff, limit])
        query = f"""
            SELECT n.title, n.fetched_at, n.source_name,
                   a.sentiment, a.impact_magnitude, a.category, a.summary
            FROM news_items n
            LEFT JOIN analysis_results a ON n.id = a.news_item_id
            WHERE ({where}) AND n.fetched_at >= ${idx}
            ORDER BY n.fetched_at DESC LIMIT ${idx + 1}
        """
        rows = await self._pool.fetch(query, *params)
        return [dict(r) for r in rows]

    async def get_recent_news_by_category(self, category: str, hours: int = 72, limit: int = 5) -> list[dict]:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        rows = await self._pool.fetch(
            """SELECT n.title, n.fetched_at, a.sentiment, a.impact_magnitude, a.summary
               FROM news_items n
               JOIN analysis_results a ON n.id = a.news_item_id
               WHERE a.category = $1 AND n.fetched_at >= $2
               ORDER BY n.fetched_at DESC LIMIT $3""",
            category, cutoff, limit,
        )
        return [dict(r) for r in rows]

    async def get_stats(self) -> dict:
        stats = {}
        for table in ["news_items", "filter_results", "analysis_results", "research_reports"]:
            row = await self._pool.fetchrow(f"SELECT COUNT(*) as cnt FROM {table}")
            stats[table] = row["cnt"]
        return stats

    async def get_token_stats(self, days: int = 1) -> dict:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        result = {"by_stage": {}, "by_model": {}, "total": {}}

        row = await self._pool.fetchrow(
            """SELECT COUNT(*), COALESCE(SUM(prompt_tokens),0),
                      COALESCE(SUM(completion_tokens),0), COALESCE(SUM(cost_cny),0)
               FROM token_usage WHERE timestamp >= $1""",
            cutoff,
        )
        result["total"] = {
            "calls": row[0], "prompt_tokens": row[1],
            "completion_tokens": row[2], "total_tokens": row[1] + row[2],
            "cost_cny": round(row[3], 4),
        }

        rows = await self._pool.fetch(
            """SELECT stage, COUNT(*), SUM(prompt_tokens),
                      SUM(completion_tokens), SUM(cost_cny)
               FROM token_usage WHERE timestamp >= $1
               GROUP BY stage ORDER BY SUM(cost_cny) DESC""",
            cutoff,
        )
        for r in rows:
            result["by_stage"][r[0]] = {
                "calls": r[1], "prompt_tokens": r[2],
                "completion_tokens": r[3], "cost_cny": round(r[4], 4),
            }

        rows = await self._pool.fetch(
            """SELECT model, COUNT(*), SUM(prompt_tokens),
                      SUM(completion_tokens), SUM(cost_cny)
               FROM token_usage WHERE timestamp >= $1
               GROUP BY model""",
            cutoff,
        )
        for r in rows:
            result["by_model"][r[0]] = {
                "calls": r[1], "prompt_tokens": r[2],
                "completion_tokens": r[3], "cost_cny": round(r[4], 4),
            }

        return result
