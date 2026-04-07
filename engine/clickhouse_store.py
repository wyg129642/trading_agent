"""ClickHouse storage for backtesting — denormalized news + analysis data."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

# DDL statements for table creation
_DDL_NEWS_ANALYSIS = """
CREATE TABLE IF NOT EXISTS {db}.news_analysis (
    news_item_id String,
    source_name LowCardinality(String),
    title String,
    url String,
    content String,
    published_at Nullable(DateTime64(3)),
    fetched_at DateTime64(3),
    language LowCardinality(String),
    market LowCardinality(String),
    is_relevant UInt8,
    relevance_score Float32,
    filter_reason String DEFAULT '',
    sentiment LowCardinality(String) DEFAULT 'neutral',
    impact_magnitude LowCardinality(String) DEFAULT 'low',
    impact_timeframe LowCardinality(String) DEFAULT 'short_term',
    category LowCardinality(String) DEFAULT 'other',
    summary String DEFAULT '',
    affected_tickers Array(String),
    affected_sectors Array(String),
    surprise_factor Float32 DEFAULT 0.5,
    is_routine UInt8 DEFAULT 0,
    market_expectation String DEFAULT '',
    key_facts Array(String),
    bull_case String DEFAULT '',
    bear_case String DEFAULT '',
    signal_score Float32 DEFAULT 0.0,
    signal_tier LowCardinality(String) DEFAULT 'suppress',
    has_research UInt8 DEFAULT 0,
    research_confidence Float32 DEFAULT 0.0,
    analyzed_at DateTime64(3),
    model_used LowCardinality(String) DEFAULT ''
) ENGINE = ReplacingMergeTree(analyzed_at)
ORDER BY (news_item_id)
PARTITION BY toYYYYMM(fetched_at)
"""

_DDL_TICKER_EVENTS = """
CREATE TABLE IF NOT EXISTS {db}.news_ticker_events (
    news_item_id String,
    ticker String,
    market LowCardinality(String),
    event_time DateTime64(3),
    sentiment LowCardinality(String),
    impact_magnitude LowCardinality(String),
    impact_timeframe LowCardinality(String),
    category LowCardinality(String),
    surprise_factor Float32,
    signal_score Float32,
    signal_tier LowCardinality(String),
    title String,
    summary String,
    price_t0 Nullable(Float64),
    price_t1_1d Nullable(Float64),
    price_t2_3d Nullable(Float64),
    price_t3_5d Nullable(Float64),
    return_1d Nullable(Float64),
    return_3d Nullable(Float64),
    return_5d Nullable(Float64),
    prediction_correct Nullable(UInt8),
    outcome_updated_at DateTime64(3) DEFAULT now64()
) ENGINE = ReplacingMergeTree(outcome_updated_at)
ORDER BY (ticker, event_time, news_item_id)
PARTITION BY toYYYYMM(event_time)
"""

_DDL_TOKEN_USAGE = """
CREATE TABLE IF NOT EXISTS {db}.token_usage (
    timestamp DateTime64(3),
    model LowCardinality(String),
    stage LowCardinality(String),
    prompt_tokens UInt32,
    completion_tokens UInt32,
    source_name LowCardinality(String) DEFAULT '',
    duration_ms UInt32 DEFAULT 0,
    cost_cny Float32 DEFAULT 0
) ENGINE = MergeTree()
ORDER BY (timestamp, stage)
PARTITION BY toYYYYMM(timestamp)
"""

_DDL_STOCK_PRICES = """
CREATE TABLE IF NOT EXISTS {db}.stock_prices (
    ticker String,
    market LowCardinality(String),
    trade_date Date,
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume UInt64,
    updated_at DateTime64(3) DEFAULT now64()
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (ticker, market, trade_date)
PARTITION BY toYYYYMM(trade_date)
"""

_DDL_BREAKING_NEWS = """
CREATE TABLE IF NOT EXISTS {db}.portfolio_breaking_news (
    id String,
    ticker String,
    name_cn String,
    name_en String DEFAULT '',
    market LowCardinality(String),
    market_label String DEFAULT '',
    scan_time DateTime64(3),

    -- Stage 1: Breaking news triage
    news_materiality LowCardinality(String),
    news_summary String,
    new_developments Array(String),

    -- Stage 2: Novelty verification
    novelty_status LowCardinality(String) DEFAULT '',
    earliest_report_time Nullable(DateTime64(3)),
    deep_research_performed UInt8 DEFAULT 0,
    research_iterations UInt8 DEFAULT 0,
    key_findings Array(String),
    news_timeline String DEFAULT '',
    referenced_sources String DEFAULT '',

    -- Stage 3: Historical precedents (JSON string)
    historical_precedents String DEFAULT '',

    -- Stage 4: Alert decision
    alert_confidence Float32 DEFAULT 0.0,
    alert_rationale String DEFAULT '',
    sentiment LowCardinality(String) DEFAULT 'neutral',
    impact_magnitude LowCardinality(String) DEFAULT 'low',
    impact_timeframe LowCardinality(String) DEFAULT 'short_term',
    surprise_factor Float32 DEFAULT 0.5,
    bull_case String DEFAULT '',
    bear_case String DEFAULT '',
    recommended_action String DEFAULT '',

    -- Metadata
    tokens_used UInt32 DEFAULT 0,
    cost_cny Float32 DEFAULT 0.0,
    created_at DateTime64(3) DEFAULT now64()
) ENGINE = ReplacingMergeTree(created_at)
ORDER BY (ticker, scan_time, id)
PARTITION BY toYYYYMM(scan_time)
"""

_SCHEMA_V2_MIGRATIONS = [
    "ALTER TABLE {db}.news_ticker_events ADD COLUMN IF NOT EXISTS source_category LowCardinality(String) DEFAULT ''",
    "ALTER TABLE {db}.news_ticker_events ADD COLUMN IF NOT EXISTS sentiment_score_short Nullable(Float64)",
    "ALTER TABLE {db}.news_ticker_events ADD COLUMN IF NOT EXISTS confidence_short Nullable(Float64)",
    "ALTER TABLE {db}.news_ticker_events ADD COLUMN IF NOT EXISTS sentiment_label_short LowCardinality(String) DEFAULT ''",
    "ALTER TABLE {db}.news_ticker_events ADD COLUMN IF NOT EXISTS sentiment_score_medium Nullable(Float64)",
    "ALTER TABLE {db}.news_ticker_events ADD COLUMN IF NOT EXISTS confidence_medium Nullable(Float64)",
    "ALTER TABLE {db}.news_ticker_events ADD COLUMN IF NOT EXISTS sentiment_label_medium LowCardinality(String) DEFAULT ''",
    "ALTER TABLE {db}.news_ticker_events ADD COLUMN IF NOT EXISTS sentiment_score_long Nullable(Float64)",
    "ALTER TABLE {db}.news_ticker_events ADD COLUMN IF NOT EXISTS confidence_long Nullable(Float64)",
    "ALTER TABLE {db}.news_ticker_events ADD COLUMN IF NOT EXISTS sentiment_label_long LowCardinality(String) DEFAULT ''",
    "ALTER TABLE {db}.news_ticker_events ADD COLUMN IF NOT EXISTS return_t1 Nullable(Float64)",
    "ALTER TABLE {db}.news_ticker_events ADD COLUMN IF NOT EXISTS return_t5 Nullable(Float64)",
    "ALTER TABLE {db}.news_ticker_events ADD COLUMN IF NOT EXISTS return_t20 Nullable(Float64)",
    "ALTER TABLE {db}.news_ticker_events ADD COLUMN IF NOT EXISTS correct_t1 Nullable(UInt8)",
    "ALTER TABLE {db}.news_ticker_events ADD COLUMN IF NOT EXISTS correct_t5 Nullable(UInt8)",
    "ALTER TABLE {db}.news_ticker_events ADD COLUMN IF NOT EXISTS correct_t20 Nullable(UInt8)",
    "ALTER TABLE {db}.news_ticker_events ADD COLUMN IF NOT EXISTS evaluated UInt8 DEFAULT 0",
    "ALTER TABLE {db}.news_ticker_events ADD COLUMN IF NOT EXISTS source_name String DEFAULT ''",
]

_DDL_MV_DAILY_ALPHA = """
CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.daily_alpha_factor
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (trade_date, ticker, market)
PARTITION BY toYYYYMM(trade_date)
AS SELECT
    toDate(event_time) AS trade_date,
    ticker,
    market,
    avg(sentiment_score_short) AS alpha_short,
    avg(sentiment_score_medium) AS alpha_medium,
    avg(sentiment_score_long) AS alpha_long,
    avg(confidence_short) AS avg_conf_short,
    avg(confidence_medium) AS avg_conf_medium,
    avg(confidence_long) AS avg_conf_long,
    count() AS signal_count,
    max(event_time) AS updated_at
FROM {db}.news_ticker_events
WHERE sentiment_score_short IS NOT NULL
GROUP BY trade_date, ticker, market
"""

_DDL_MV_SOURCE_WEEKLY = """
CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.source_weekly_stats
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (source_name, week_start)
AS SELECT
    source_name,
    toMonday(toDate(event_time)) AS week_start,
    count() AS signal_count,
    avg(correct_t1) AS accuracy_t1,
    avg(correct_t5) AS accuracy_t5,
    avg(correct_t20) AS accuracy_t20,
    avg(sentiment_score_short) AS avg_score_short,
    avg(return_t1) AS avg_return_t1,
    varPop(sentiment_score_short) AS var_score,
    varPop(return_t1) AS var_return,
    covarPop(sentiment_score_short, return_t1) AS cov_score_return_t1,
    covarPop(sentiment_score_medium, return_t5) AS cov_score_return_t5,
    covarPop(sentiment_score_long, return_t20) AS cov_score_return_t20,
    max(outcome_updated_at) AS updated_at
FROM {db}.news_ticker_events
WHERE evaluated = 1
GROUP BY source_name, week_start
"""


def _to_dt64(val: Any) -> datetime | None:
    """Convert various datetime representations to a datetime for ClickHouse."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    if isinstance(val, str):
        try:
            return datetime.fromisoformat(val)
        except ValueError:
            return None
    return None


class ClickHouseStore:
    """Async ClickHouse client with batch buffering.

    Uses clickhouse-connect (HTTP protocol).
    All blocking client calls are wrapped in asyncio.to_thread.
    """

    def __init__(self, config: dict):
        self._config = config
        self._client = None
        self._db = config.get("database", "db_spider")
        self._batch_size = config.get("batch_size", 50)
        self._flush_interval = config.get("flush_interval_seconds", 30)

        # Batch buffers
        self._news_buf: list[list] = []
        self._ticker_buf: list[list] = []
        self._token_buf: list[list] = []
        self._last_flush = time.monotonic()
        self._lock = asyncio.Lock()

    async def initialize(self) -> None:
        """Connect and create tables."""
        import clickhouse_connect

        try:
            self._client = await asyncio.wait_for(
                asyncio.to_thread(
                    clickhouse_connect.get_client,
                    host=self._config.get("host", "192.168.31.137"),
                    port=self._config.get("port", 38123),
                    database=self._config.get("database", "db_spider"),
                    username=self._config.get("user", "u_spider"),
                    password=self._config.get("password", ""),
                ),
                timeout=30,
            )
        except asyncio.TimeoutError:
            raise ConnectionError("ClickHouse connection timed out after 30s")

        # Create tables
        for ddl in (_DDL_NEWS_ANALYSIS, _DDL_TICKER_EVENTS, _DDL_TOKEN_USAGE,
                     _DDL_STOCK_PRICES, _DDL_BREAKING_NEWS):
            stmt = ddl.format(db=self._db)
            await asyncio.to_thread(self._client.command, stmt)

        # Run schema v2 migrations (idempotent ADD COLUMN IF NOT EXISTS)
        for stmt in _SCHEMA_V2_MIGRATIONS:
            try:
                await asyncio.to_thread(self._client.command, stmt.format(db=self._db))
            except Exception as e:
                logger.debug("Schema v2 migration skipped (may already exist): %s", e)

        # Create materialized views (idempotent)
        for mv_ddl in (_DDL_MV_DAILY_ALPHA, _DDL_MV_SOURCE_WEEKLY):
            try:
                await asyncio.to_thread(self._client.command, mv_ddl.format(db=self._db))
            except Exception as e:
                logger.debug("MV creation skipped (may already exist): %s", e)

        logger.info("ClickHouse initialized: %s:%s/%s",
                     self._config.get("host"), self._config.get("port"), self._db)

    # --- Insert methods (buffer rows) ---

    async def insert_news_analysis(
        self,
        item,       # NewsItem
        filter_res, # FilterResult
        analysis=None,   # AnalysisResult | None
        signal=None,     # SignalScore | None
        research=None,   # ResearchReport | None
    ) -> None:
        """Buffer a denormalized news+filter+analysis row."""
        now = datetime.now(timezone.utc)
        row = [
            item.id,
            item.source_name,
            item.title,
            item.url,
            item.content or "",
            _to_dt64(item.published_at),
            _to_dt64(item.fetched_at) or now,
            item.language,
            item.market,
            # filter
            int(filter_res.is_relevant),
            float(filter_res.relevance_score),
            filter_res.reason or "",
            # analysis (defaults if None)
            analysis.sentiment if analysis else "neutral",
            analysis.impact_magnitude if analysis else "low",
            analysis.impact_timeframe if analysis else "short_term",
            analysis.category if analysis else "other",
            analysis.summary if analysis else "",
            analysis.affected_tickers if analysis else [],
            analysis.affected_sectors if analysis else [],
            analysis.surprise_factor if analysis else 0.5,
            int(analysis.is_routine) if analysis else 0,
            analysis.market_expectation if analysis else "",
            analysis.key_facts if analysis else [],
            analysis.bull_case if analysis else "",
            analysis.bear_case if analysis else "",
            # signal
            signal.composite_score if signal else 0.0,
            signal.tier if signal else "suppress",
            # research
            int(bool(research)),
            research.confidence if research else 0.0,
            # meta
            _to_dt64(analysis.analyzed_at) if analysis else now,
            analysis.model_used if analysis else "",
        ]
        async with self._lock:
            self._news_buf.append(row)
        await self._maybe_flush()

    async def insert_ticker_events(
        self,
        item,       # NewsItem
        analysis,   # AnalysisResult
        signal=None,
    ) -> None:
        """Insert one row per (news, ticker) pair for backtesting."""
        if not analysis or not analysis.affected_tickers:
            return
        now = datetime.now(timezone.utc)
        event_time = _to_dt64(item.published_at) or _to_dt64(item.fetched_at) or now

        # Per-stock sentiment: use ticker-specific sentiment if available
        ticker_sents = getattr(analysis, "ticker_sentiments", {}) or {}

        rows = []
        for ticker in analysis.affected_tickers:
            # Resolve per-stock sentiment with fallback to global
            stock_sentiment = None
            if ticker_sents:
                stock_sentiment = ticker_sents.get(ticker)
                if not stock_sentiment:
                    for ts_key, ts_val in ticker_sents.items():
                        if ticker in ts_key or ts_key in ticker:
                            stock_sentiment = ts_val
                            break
            if not stock_sentiment:
                stock_sentiment = analysis.sentiment

            # Extract multi-horizon data if stock_sentiment is a dict (new format)
            if isinstance(stock_sentiment, dict):
                sentiment_label = stock_sentiment.get("sentiment", stock_sentiment.get("label", analysis.sentiment))
                source_category = stock_sentiment.get("source_category", "")
                # Short horizon
                short_data = stock_sentiment.get("short", {})
                sentiment_score_short = short_data.get("score") if short_data else None
                confidence_short = short_data.get("confidence") if short_data else None
                sentiment_label_short = short_data.get("label", "") if short_data else ""
                # Medium horizon
                medium_data = stock_sentiment.get("medium", {})
                sentiment_score_medium = medium_data.get("score") if medium_data else None
                confidence_medium = medium_data.get("confidence") if medium_data else None
                sentiment_label_medium = medium_data.get("label", "") if medium_data else ""
                # Long horizon
                long_data = stock_sentiment.get("long", {})
                sentiment_score_long = long_data.get("score") if long_data else None
                confidence_long = long_data.get("confidence") if long_data else None
                sentiment_label_long = long_data.get("label", "") if long_data else ""
            else:
                sentiment_label = stock_sentiment if isinstance(stock_sentiment, str) else analysis.sentiment
                source_category = ""
                sentiment_score_short = None
                confidence_short = None
                sentiment_label_short = ""
                sentiment_score_medium = None
                confidence_medium = None
                sentiment_label_medium = ""
                sentiment_score_long = None
                confidence_long = None
                sentiment_label_long = ""

            rows.append([
                item.id,
                ticker,
                item.market,
                event_time,
                sentiment_label,
                analysis.impact_magnitude,
                analysis.impact_timeframe,
                analysis.category,
                analysis.surprise_factor,
                signal.composite_score if signal else 0.0,
                signal.tier if signal else "suppress",
                item.title,
                analysis.summary or "",
                # backtesting columns (filled later by backtest script)
                None, None, None, None, None, None, None, None,
                event_time,  # outcome_updated_at defaults to event_time
                # v2 columns
                source_category,
                sentiment_score_short, confidence_short, sentiment_label_short,
                sentiment_score_medium, confidence_medium, sentiment_label_medium,
                sentiment_score_long, confidence_long, sentiment_label_long,
                # source_name for MV joins
                getattr(item, "source_name", "") or "",
            ])

        async with self._lock:
            self._ticker_buf.extend(rows)
        await self._maybe_flush()

    async def insert_token_usage(
        self,
        timestamp: datetime,
        model: str,
        stage: str,
        prompt_tokens: int,
        completion_tokens: int,
        source_name: str = "",
        duration_ms: int = 0,
        cost_cny: float = 0.0,
    ) -> None:
        """Buffer a token usage row."""
        row = [
            timestamp,
            model,
            stage,
            prompt_tokens,
            completion_tokens,
            source_name,
            duration_ms,
            cost_cny,
        ]
        async with self._lock:
            self._token_buf.append(row)

    async def insert_stock_prices(self, rows: list[list]) -> None:
        """Bulk insert OHLCV price data into stock_prices table."""
        if not rows:
            return
        await self._insert_batch(
            f"{self._db}.stock_prices",
            ["ticker", "market", "trade_date", "open", "high", "low", "close", "volume"],
            rows,
        )
        logger.info("[ClickHouse] Inserted %d stock price rows", len(rows))

    async def insert_breaking_news(self, result) -> None:
        """Insert a proactive scan result as a breaking news record.

        Args:
            result: A ProactiveScanResult from the proactive scanner.
        """
        import json
        import hashlib

        holding = result.holding
        analysis = result.full_analysis or {}

        # Deterministic ID: ticker + scan_time
        id_seed = f"{holding.ticker}:{result.scan_time.isoformat()}"
        row_id = hashlib.sha256(id_seed.encode()).hexdigest()[:24]

        row = [
            row_id,
            holding.ticker,
            holding.name_cn,
            holding.name_en,
            holding.market,
            holding.market_label,
            result.scan_time,
            # Stage 1
            result.news_materiality,
            result.news_summary or "",
            result.new_developments or [],
            # Stage 2
            result.novelty_status or "",
            _to_dt64(result.earliest_report_time),
            int(result.deep_research_performed),
            result.research_iterations,
            result.key_findings or [],
            json.dumps(result.news_timeline or [], ensure_ascii=False, default=str),
            json.dumps(result.referenced_sources or [], ensure_ascii=False, default=str),
            # Stage 3
            json.dumps(result.historical_precedents or [], ensure_ascii=False, default=str),
            # Stage 4
            result.alert_confidence,
            result.alert_rationale or "",
            analysis.get("sentiment", "neutral"),
            analysis.get("impact_magnitude", "low"),
            analysis.get("impact_timeframe", "short_term"),
            analysis.get("surprise_factor", 0.5),
            analysis.get("bull_case", ""),
            analysis.get("bear_case", ""),
            analysis.get("recommended_action", ""),
            # Metadata
            result.tokens_used,
            result.cost_cny,
            result.scan_time,
        ]

        columns = [
            "id", "ticker", "name_cn", "name_en", "market", "market_label", "scan_time",
            "news_materiality", "news_summary", "new_developments",
            "novelty_status", "earliest_report_time", "deep_research_performed",
            "research_iterations", "key_findings", "news_timeline", "referenced_sources",
            "historical_precedents",
            "alert_confidence", "alert_rationale", "sentiment", "impact_magnitude",
            "impact_timeframe", "surprise_factor", "bull_case", "bear_case",
            "recommended_action",
            "tokens_used", "cost_cny", "created_at",
        ]

        try:
            await self._insert_batch(
                f"{self._db}.portfolio_breaking_news", columns, [row],
            )
            logger.info("[ClickHouse] Inserted breaking news for %s", holding.ticker)
        except Exception as e:
            logger.error("[ClickHouse] Breaking news insert failed for %s: %s",
                         holding.ticker, e)

    # --- Flush ---

    async def _maybe_flush(self) -> None:
        """Auto-flush if buffer is large enough or enough time has elapsed."""
        elapsed = time.monotonic() - self._last_flush
        total = len(self._news_buf) + len(self._ticker_buf) + len(self._token_buf)
        if total >= self._batch_size or elapsed >= self._flush_interval:
            await self.flush_all()

    async def flush_all(self) -> None:
        """Force-flush all buffers to ClickHouse."""
        async with self._lock:
            news_rows = self._news_buf[:]
            ticker_rows = self._ticker_buf[:]
            token_rows = self._token_buf[:]
            self._news_buf.clear()
            self._ticker_buf.clear()
            self._token_buf.clear()
            self._last_flush = time.monotonic()

        if news_rows:
            await self._insert_batch(
                f"{self._db}.news_analysis",
                [
                    "news_item_id", "source_name", "title", "url", "content",
                    "published_at", "fetched_at", "language", "market",
                    "is_relevant", "relevance_score", "filter_reason",
                    "sentiment", "impact_magnitude", "impact_timeframe",
                    "category", "summary", "affected_tickers", "affected_sectors",
                    "surprise_factor", "is_routine", "market_expectation",
                    "key_facts", "bull_case", "bear_case",
                    "signal_score", "signal_tier",
                    "has_research", "research_confidence",
                    "analyzed_at", "model_used",
                ],
                news_rows,
            )

        if ticker_rows:
            await self._insert_batch(
                f"{self._db}.news_ticker_events",
                [
                    "news_item_id", "ticker", "market", "event_time",
                    "sentiment", "impact_magnitude", "impact_timeframe",
                    "category", "surprise_factor", "signal_score", "signal_tier",
                    "title", "summary",
                    "price_t0", "price_t1_1d", "price_t2_3d", "price_t3_5d",
                    "return_1d", "return_3d", "return_5d",
                    "prediction_correct", "outcome_updated_at",
                    # v2 columns
                    "source_category",
                    "sentiment_score_short", "confidence_short", "sentiment_label_short",
                    "sentiment_score_medium", "confidence_medium", "sentiment_label_medium",
                    "sentiment_score_long", "confidence_long", "sentiment_label_long",
                    "source_name",
                ],
                ticker_rows,
            )

        if token_rows:
            await self._insert_batch(
                f"{self._db}.token_usage",
                [
                    "timestamp", "model", "stage",
                    "prompt_tokens", "completion_tokens",
                    "source_name", "duration_ms", "cost_cny",
                ],
                token_rows,
            )

        flushed = len(news_rows) + len(ticker_rows) + len(token_rows)
        if flushed:
            logger.debug(
                "[ClickHouse] Flushed %d rows (news=%d, ticker=%d, token=%d)",
                flushed, len(news_rows), len(ticker_rows), len(token_rows),
            )

    async def _insert_batch(self, table: str, columns: list[str], rows: list[list]) -> None:
        """Insert a batch of rows into a ClickHouse table."""
        try:
            await asyncio.to_thread(
                self._client.insert, table, rows, column_names=columns,
            )
        except Exception as e:
            logger.error("[ClickHouse] Insert into %s failed (%d rows): %s", table, len(rows), e)

    async def close(self) -> None:
        """Final flush and close the client."""
        try:
            await self.flush_all()
        except Exception as e:
            logger.error("[ClickHouse] Final flush failed: %s", e)
        if self._client:
            try:
                await asyncio.to_thread(self._client.close)
            except Exception:
                pass
            self._client = None
            logger.info("ClickHouse connection closed")
