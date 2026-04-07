"""SQLite database for persistent storage of news items and analysis results."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

import aiosqlite

from src.models import AnalysisResult, FilterResult, NewsItem, ResearchReport, SourceHealth

logger = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS news_items (
    id TEXT PRIMARY KEY,
    source_name TEXT NOT NULL,
    title TEXT NOT NULL,
    url TEXT NOT NULL,
    content TEXT DEFAULT '',
    content_hash TEXT NOT NULL,
    published_at TEXT,
    fetched_at TEXT NOT NULL,
    language TEXT DEFAULT 'zh',
    market TEXT DEFAULT 'china',
    metadata TEXT DEFAULT '{}',
    UNIQUE(content_hash)
);

CREATE TABLE IF NOT EXISTS filter_results (
    news_item_id TEXT PRIMARY KEY REFERENCES news_items(id),
    is_relevant INTEGER NOT NULL,
    relevance_score REAL NOT NULL,
    reason TEXT DEFAULT '',
    filtered_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS analysis_results (
    news_item_id TEXT PRIMARY KEY REFERENCES news_items(id),
    sentiment TEXT DEFAULT 'neutral',
    impact_magnitude TEXT DEFAULT 'low',
    impact_timeframe TEXT DEFAULT 'short_term',
    affected_tickers TEXT DEFAULT '[]',
    affected_sectors TEXT DEFAULT '[]',
    category TEXT DEFAULT 'other',
    summary TEXT DEFAULT '',
    key_facts TEXT DEFAULT '[]',
    bull_case TEXT DEFAULT '',
    bear_case TEXT DEFAULT '',
    requires_deep_research INTEGER DEFAULT 0,
    research_questions TEXT DEFAULT '[]',
    analyzed_at TEXT NOT NULL,
    model_used TEXT DEFAULT ''
);

CREATE TABLE IF NOT EXISTS research_reports (
    news_item_id TEXT PRIMARY KEY REFERENCES news_items(id),
    executive_summary TEXT DEFAULT '',
    context TEXT DEFAULT '',
    affected_securities TEXT DEFAULT '',
    historical_precedent TEXT DEFAULT '',
    bull_scenario TEXT DEFAULT '',
    bear_scenario TEXT DEFAULT '',
    recommended_actions TEXT DEFAULT '',
    risk_factors TEXT DEFAULT '',
    confidence REAL DEFAULT 0.0,
    full_report TEXT DEFAULT '',
    market_data_snapshot TEXT DEFAULT '{}',
    researched_at TEXT NOT NULL,
    model_used TEXT DEFAULT ''
);

CREATE TABLE IF NOT EXISTS source_health (
    source_name TEXT PRIMARY KEY,
    last_success TEXT,
    last_failure TEXT,
    consecutive_failures INTEGER DEFAULT 0,
    total_items_fetched INTEGER DEFAULT 0,
    is_healthy INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS token_usage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    model TEXT NOT NULL,
    stage TEXT NOT NULL,
    prompt_tokens INTEGER NOT NULL,
    completion_tokens INTEGER NOT NULL,
    estimated_prompt INTEGER DEFAULT 0,
    source_name TEXT DEFAULT '',
    duration_ms INTEGER DEFAULT 0,
    cost_cny REAL DEFAULT 0.0
);

CREATE INDEX IF NOT EXISTS idx_news_fetched_at ON news_items(fetched_at);
CREATE INDEX IF NOT EXISTS idx_news_source ON news_items(source_name);
CREATE INDEX IF NOT EXISTS idx_news_hash ON news_items(content_hash);
CREATE INDEX IF NOT EXISTS idx_analysis_magnitude ON analysis_results(impact_magnitude);
CREATE INDEX IF NOT EXISTS idx_token_timestamp ON token_usage(timestamp);
CREATE INDEX IF NOT EXISTS idx_token_stage ON token_usage(stage);
"""


class Database:
    """Async SQLite database manager."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._db: aiosqlite.Connection | None = None

    async def initialize(self) -> None:
        """Create database and tables."""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(self.db_path)
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA synchronous=NORMAL")
        await self._db.executescript(SCHEMA)
        # Additive migrations (safe to re-run: columns ignored if they exist)
        for col, default in [
            ("surprise_factor", "0.5"),
            ("is_routine", "0"),
            ("market_expectation", "''"),
            ("quantified_evidence", "'[]'"),
        ]:
            try:
                await self._db.execute(
                    f"ALTER TABLE analysis_results ADD COLUMN {col} "
                    f"{'REAL' if col == 'surprise_factor' else 'INTEGER' if col == 'is_routine' else 'TEXT'} "
                    f"DEFAULT {default}"
                )
            except Exception:
                pass  # column already exists
        await self._db.commit()
        logger.info("Database initialized at %s", self.db_path)

    async def close(self) -> None:
        if self._db:
            await self._db.close()

    # --- News Items ---

    async def get_content_hashes_by_source(self, source_name: str) -> set[str]:
        """Load all content hashes for a given source (for warm-starting dedup)."""
        async with self._db.execute(
            "SELECT content_hash FROM news_items WHERE source_name = ?",
            (source_name,),
        ) as cursor:
            rows = await cursor.fetchall()
            return {row[0] for row in rows}

    async def get_newest_item_by_source(self, source_name: str) -> dict | None:
        """Get the most recently fetched news item for a given source."""
        async with self._db.execute(
            """SELECT id, source_name, title, url, content, content_hash,
                      published_at, fetched_at, language, market, metadata
               FROM news_items
               WHERE source_name = ?
               ORDER BY fetched_at DESC LIMIT 1""",
            (source_name,),
        ) as cursor:
            row = await cursor.fetchone()
            if not row:
                return None
            columns = [d[0] for d in cursor.description]
            return dict(zip(columns, row))

    async def get_latest_published_at(self, source_name: str) -> datetime | None:
        """Return the most recent published_at for a source, or None."""
        async with self._db.execute(
            "SELECT MAX(published_at) FROM news_items WHERE source_name = ? AND published_at IS NOT NULL",
            (source_name,),
        ) as cursor:
            row = await cursor.fetchone()
            if row and row[0]:
                try:
                    return datetime.fromisoformat(row[0])
                except (ValueError, TypeError):
                    return None
            return None

    async def is_duplicate(self, content_hash: str) -> bool:
        """Check if we've already stored this news item."""
        async with self._db.execute(
            "SELECT 1 FROM news_items WHERE content_hash = ?", (content_hash,)
        ) as cursor:
            return await cursor.fetchone() is not None

    async def save_news_item(self, item: NewsItem) -> bool:
        """Save a news item. Returns False if duplicate."""
        if await self.is_duplicate(item.content_hash):
            return False
        try:
            await self._db.execute(
                """INSERT INTO news_items (id, source_name, title, url, content,
                   content_hash, published_at, fetched_at, language, market, metadata)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    item.id,
                    item.source_name,
                    item.title,
                    item.url,
                    item.content,
                    item.content_hash,
                    item.published_at.isoformat() if item.published_at else None,
                    item.fetched_at.isoformat(),
                    item.language,
                    item.market,
                    json.dumps(item.metadata, ensure_ascii=False),
                ),
            )
            await self._db.commit()
            return True
        except aiosqlite.IntegrityError:
            return False

    async def update_news_metadata(self, news_id: str, key: str, value: str) -> None:
        """Update a single key in the news item's JSON metadata (SQLite)."""
        import json as _json
        row = await self._db.execute_fetchall(
            "SELECT metadata FROM news_items WHERE id = ?", (news_id,)
        )
        if row:
            meta = _json.loads(row[0][0]) if row[0][0] else {}
            meta[key] = value
            await self._db.execute(
                "UPDATE news_items SET metadata = ? WHERE id = ?",
                (_json.dumps(meta, ensure_ascii=False), news_id),
            )
            await self._db.commit()

    async def save_filter_result(self, result: FilterResult) -> None:
        await self._db.execute(
            """INSERT OR REPLACE INTO filter_results
               (news_item_id, is_relevant, relevance_score, reason, filtered_at)
               VALUES (?, ?, ?, ?, ?)""",
            (
                result.news_item_id,
                int(result.is_relevant),
                result.relevance_score,
                result.reason,
                datetime.now().isoformat(),
            ),
        )
        await self._db.commit()

    async def save_analysis_result(self, result: AnalysisResult) -> None:
        await self._db.execute(
            """INSERT OR REPLACE INTO analysis_results
               (news_item_id, sentiment, impact_magnitude, impact_timeframe,
                affected_tickers, affected_sectors, category, summary, key_facts,
                bull_case, bear_case, requires_deep_research, research_questions,
                analyzed_at, model_used,
                surprise_factor, is_routine, market_expectation, quantified_evidence)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                result.news_item_id,
                result.sentiment,
                result.impact_magnitude,
                result.impact_timeframe,
                json.dumps(result.affected_tickers, ensure_ascii=False),
                json.dumps(result.affected_sectors, ensure_ascii=False),
                result.category,
                result.summary,
                json.dumps(result.key_facts, ensure_ascii=False),
                result.bull_case,
                result.bear_case,
                int(result.requires_deep_research),
                json.dumps(result.research_questions, ensure_ascii=False),
                result.analyzed_at.isoformat(),
                result.model_used,
                result.surprise_factor,
                int(result.is_routine),
                result.market_expectation,
                json.dumps(result.quantified_evidence, ensure_ascii=False),
            ),
        )
        await self._db.commit()

    async def save_research_report(self, report: ResearchReport) -> None:
        await self._db.execute(
            """INSERT OR REPLACE INTO research_reports
               (news_item_id, executive_summary, context, affected_securities,
                historical_precedent, bull_scenario, bear_scenario,
                recommended_actions, risk_factors, confidence, full_report,
                market_data_snapshot, researched_at, model_used)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                report.news_item_id,
                report.executive_summary,
                report.context,
                report.affected_securities,
                report.historical_precedent,
                report.bull_scenario,
                report.bear_scenario,
                report.recommended_actions,
                report.risk_factors,
                report.confidence,
                report.full_report,
                json.dumps(report.market_data_snapshot, ensure_ascii=False),
                report.researched_at.isoformat(),
                report.model_used,
            ),
        )
        await self._db.commit()

    async def update_news_content(
        self, item_id: str, content: str,
        metadata: dict | None = None, published_at: datetime | None = None,
    ) -> None:
        """Update content (and optionally metadata/published_at) after article fetch."""
        parts = ["content = ?"]
        params: list = [content]
        if metadata:
            parts.append("metadata = ?")
            params.append(json.dumps(metadata, ensure_ascii=False))
        if published_at:
            parts.append("published_at = ?")
            params.append(published_at.isoformat())
        params.append(item_id)
        sql = f"UPDATE news_items SET {', '.join(parts)} WHERE id = ?"
        await self._db.execute(sql, params)
        await self._db.commit()

    # --- Source Health ---

    async def update_source_health(self, health: SourceHealth) -> None:
        await self._db.execute(
            """INSERT OR REPLACE INTO source_health
               (source_name, last_success, last_failure, consecutive_failures,
                total_items_fetched, is_healthy)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                health.source_name,
                health.last_success.isoformat() if health.last_success else None,
                health.last_failure.isoformat() if health.last_failure else None,
                health.consecutive_failures,
                health.total_items_fetched,
                int(health.is_healthy),
            ),
        )
        await self._db.commit()

    # --- Queries ---

    async def get_recent_news(self, limit: int = 50) -> list[dict]:
        """Get most recent news items with their analysis."""
        async with self._db.execute(
            """SELECT n.*, a.sentiment, a.impact_magnitude, a.summary
               FROM news_items n
               LEFT JOIN analysis_results a ON n.id = a.news_item_id
               ORDER BY n.fetched_at DESC LIMIT ?""",
            (limit,),
        ) as cursor:
            rows = await cursor.fetchall()
            columns = [d[0] for d in cursor.description]
            return [dict(zip(columns, row)) for row in rows]

    async def search_news(self, query: str, limit: int = 20) -> list[dict]:
        """Search historical news by title/content keywords."""
        async with self._db.execute(
            """SELECT n.*, a.sentiment, a.impact_magnitude, a.summary
               FROM news_items n
               LEFT JOIN analysis_results a ON n.id = a.news_item_id
               WHERE n.title LIKE ? OR n.content LIKE ?
               ORDER BY n.fetched_at DESC LIMIT ?""",
            (f"%{query}%", f"%{query}%", limit),
        ) as cursor:
            rows = await cursor.fetchall()
            columns = [d[0] for d in cursor.description]
            return [dict(zip(columns, row)) for row in rows]

    async def get_recent_news_for_tickers(self, tickers: list[str], hours: int = 72, limit: int = 10) -> list[dict]:
        """Get recent news mentioning any of the given tickers (for novelty detection)."""
        if not tickers:
            return []
        cutoff = (datetime.now() - timedelta(hours=hours)).isoformat()
        # Search in affected_tickers JSON and in title text
        conditions = []
        params = []
        for t in tickers:
            conditions.append("a.affected_tickers LIKE ?")
            params.append(f"%{t}%")
            conditions.append("n.title LIKE ?")
            params.append(f"%{t}%")
        where = " OR ".join(conditions)
        params.extend([cutoff, limit])
        query = f"""
            SELECT n.title, n.fetched_at, n.source_name,
                   a.sentiment, a.impact_magnitude, a.category, a.summary
            FROM news_items n
            LEFT JOIN analysis_results a ON n.id = a.news_item_id
            WHERE ({where}) AND n.fetched_at >= ?
            ORDER BY n.fetched_at DESC LIMIT ?
        """
        async with self._db.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            columns = [d[0] for d in cursor.description]
            return [dict(zip(columns, row)) for row in rows]

    async def get_recent_news_by_category(self, category: str, hours: int = 72, limit: int = 5) -> list[dict]:
        """Get recent news of the same category (for calibration)."""
        cutoff = (datetime.now() - timedelta(hours=hours)).isoformat()
        query = """
            SELECT n.title, n.fetched_at, a.sentiment, a.impact_magnitude, a.summary
            FROM news_items n
            JOIN analysis_results a ON n.id = a.news_item_id
            WHERE a.category = ? AND n.fetched_at >= ?
            ORDER BY n.fetched_at DESC LIMIT ?
        """
        async with self._db.execute(query, (category, cutoff, limit)) as cursor:
            rows = await cursor.fetchall()
            columns = [d[0] for d in cursor.description]
            return [dict(zip(columns, row)) for row in rows]

    async def get_stats(self) -> dict:
        """Get database statistics."""
        stats = {}
        for table in ["news_items", "filter_results", "analysis_results", "research_reports"]:
            async with self._db.execute(f"SELECT COUNT(*) FROM {table}") as cursor:
                row = await cursor.fetchone()
                stats[table] = row[0]
        return stats

    async def get_token_stats(self, days: int = 1) -> dict:
        """Get token usage statistics for the last N days."""
        from datetime import datetime, timedelta
        cutoff = (datetime.now() - timedelta(days=days)).isoformat()
        result = {"by_stage": {}, "by_model": {}, "total": {}}

        # Total
        async with self._db.execute(
            """SELECT COUNT(*), COALESCE(SUM(prompt_tokens),0),
                      COALESCE(SUM(completion_tokens),0), COALESCE(SUM(cost_cny),0)
               FROM token_usage WHERE timestamp >= ?""",
            (cutoff,),
        ) as cursor:
            row = await cursor.fetchone()
            result["total"] = {
                "calls": row[0], "prompt_tokens": row[1],
                "completion_tokens": row[2], "total_tokens": row[1] + row[2],
                "cost_cny": round(row[3], 4),
            }

        # By stage
        async with self._db.execute(
            """SELECT stage, COUNT(*), SUM(prompt_tokens),
                      SUM(completion_tokens), SUM(cost_cny)
               FROM token_usage WHERE timestamp >= ?
               GROUP BY stage ORDER BY SUM(cost_cny) DESC""",
            (cutoff,),
        ) as cursor:
            for row in await cursor.fetchall():
                result["by_stage"][row[0]] = {
                    "calls": row[1], "prompt_tokens": row[2],
                    "completion_tokens": row[3], "cost_cny": round(row[4], 4),
                }

        # By model
        async with self._db.execute(
            """SELECT model, COUNT(*), SUM(prompt_tokens),
                      SUM(completion_tokens), SUM(cost_cny)
               FROM token_usage WHERE timestamp >= ?
               GROUP BY model""",
            (cutoff,),
        ) as cursor:
            for row in await cursor.fetchall():
                result["by_model"][row[0]] = {
                    "calls": row[1], "prompt_tokens": row[2],
                    "completion_tokens": row[3], "cost_cny": round(row[4], 4),
                }

        return result
