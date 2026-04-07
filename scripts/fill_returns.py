#!/usr/bin/env python3
"""Fill return columns in ClickHouse news_ticker_events by joining with stock_prices.

Usage:
    python scripts/fill_returns.py              # Fill all unfilled rows
    python scripts/fill_returns.py --days 30    # Only last 30 days
    python scripts/fill_returns.py --dry-run    # Show counts only
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SETTINGS_PATH = PROJECT_ROOT / "config" / "settings.yaml"

# Market timezone offsets (hours from UTC) — same as source_scorer.py
MARKET_TZ_OFFSETS = {
    "china": 8,
    "us": -5,
    "hk": 8,
    "global": 0,
    "kr": 9,
    "jp": 9,
}


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


def load_clickhouse_config() -> dict:
    """Load ClickHouse connection settings from config/settings.yaml."""
    with open(SETTINGS_PATH, "r") as f:
        settings = yaml.safe_load(f)
    ch_cfg = settings.get("clickhouse", {})
    if not ch_cfg.get("enabled", False):
        logger.warning("ClickHouse is not enabled in settings.yaml — proceeding anyway")
    return ch_cfg


def get_client(cfg: dict):
    """Create a clickhouse-connect client."""
    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=cfg.get("host", "192.168.31.137"),
        port=cfg.get("port", 38123),
        database=cfg.get("database", "db_spider"),
        username=cfg.get("user", "u_spider"),
        password=cfg.get("password", ""),
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def signal_date_from_event_time(event_time: datetime, market: str) -> datetime:
    """Convert UTC event_time to the local trading date for the market."""
    offset_hours = MARKET_TZ_OFFSETS.get(market, 0)
    # If event_time is naive, assume UTC
    if event_time.tzinfo is None:
        local_time = event_time + timedelta(hours=offset_hours)
    else:
        local_time = event_time.astimezone(timezone.utc) + timedelta(hours=offset_hours)
    return local_time.replace(hour=0, minute=0, second=0, microsecond=0)


def compute_correctness(sentiment_score: float | None, ret: float | None) -> int | None:
    """Return 1 if direction matches, 0 if not, None if undetermined.

    sentiment_score > 0 means bullish prediction, < 0 means bearish.
    """
    if sentiment_score is None or ret is None:
        return None
    if sentiment_score == 0:
        return None  # neutral — can't evaluate
    if (sentiment_score > 0 and ret > 0) or (sentiment_score < 0 and ret < 0):
        return 1
    return 0


# ---------------------------------------------------------------------------
# Main fill logic
# ---------------------------------------------------------------------------


async def fill_returns(days: int | None, dry_run: bool) -> None:
    """Main fill routine."""
    cfg = load_clickhouse_config()
    client = await asyncio.to_thread(get_client, cfg)
    db = cfg.get("database", "db_spider")

    logger.info("Connected to ClickHouse %s:%s/%s", cfg.get("host"), cfg.get("port"), db)

    # 1. Query unfilled signals
    where_clauses = ["evaluated = 0"]
    if days is not None:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        where_clauses.append(f"event_time >= '{cutoff.strftime('%Y-%m-%d %H:%M:%S')}'")

    where_sql = " AND ".join(where_clauses)

    count_query = f"SELECT count() FROM {db}.news_ticker_events WHERE {where_sql}"
    count_result = await asyncio.to_thread(client.query, count_query)
    total_unfilled = count_result.result_rows[0][0]

    logger.info("Found %d unfilled signal rows (evaluated=0)", total_unfilled)

    if total_unfilled == 0:
        logger.info("Nothing to fill — all signals are already evaluated")
        await asyncio.to_thread(client.close)
        return

    if dry_run:
        # Show breakdown by market
        breakdown_query = (
            f"SELECT market, count() as cnt "
            f"FROM {db}.news_ticker_events "
            f"WHERE {where_sql} "
            f"GROUP BY market ORDER BY cnt DESC"
        )
        breakdown = await asyncio.to_thread(client.query, breakdown_query)
        logger.info("--- DRY RUN — unfilled signals by market ---")
        for row in breakdown.result_rows:
            logger.info("  %s: %d signals", row[0], row[1])

        # Show breakdown by ticker (top 20)
        ticker_query = (
            f"SELECT ticker, market, count() as cnt "
            f"FROM {db}.news_ticker_events "
            f"WHERE {where_sql} "
            f"GROUP BY ticker, market ORDER BY cnt DESC LIMIT 20"
        )
        ticker_result = await asyncio.to_thread(client.query, ticker_query)
        logger.info("--- Top 20 tickers with unfilled signals ---")
        for row in ticker_result.result_rows:
            logger.info("  %s (%s): %d signals", row[0], row[1], row[2])

        await asyncio.to_thread(client.close)
        return

    # 2. Fetch all unfilled signals
    signals_query = (
        f"SELECT "
        f"  news_item_id, ticker, market, event_time, "
        f"  sentiment, impact_magnitude, impact_timeframe, category, "
        f"  surprise_factor, signal_score, signal_tier, "
        f"  title, summary, "
        f"  sentiment_score_short, confidence_short, sentiment_label_short, "
        f"  sentiment_score_medium, confidence_medium, sentiment_label_medium, "
        f"  sentiment_score_long, confidence_long, sentiment_label_long, "
        f"  source_name "
        f"FROM {db}.news_ticker_events "
        f"WHERE {where_sql} "
        f"ORDER BY ticker, event_time"
    )
    signals_result = await asyncio.to_thread(client.query, signals_query)
    signals = signals_result.result_rows
    col_names = signals_result.column_names

    logger.info("Fetched %d unfilled signals for processing", len(signals))

    # 3. Build a cache of trading dates per (ticker, market) from stock_prices
    # This avoids repeated queries for the same ticker
    price_cache: dict[tuple[str, str], list[tuple]] = {}

    async def get_price_data(ticker: str, market: str) -> list[tuple]:
        """Get sorted price data for a ticker from cache or ClickHouse."""
        cache_key = (ticker, market)
        if cache_key in price_cache:
            return price_cache[cache_key]

        price_query = (
            f"SELECT trade_date, open, close "
            f"FROM {db}.stock_prices "
            f"WHERE ticker = %(ticker)s AND market = %(market)s "
            f"ORDER BY trade_date ASC"
        )
        result = await asyncio.to_thread(
            client.query, price_query,
            parameters={"ticker": ticker, "market": market},
        )
        rows = result.result_rows  # list of (trade_date, open, close)
        price_cache[cache_key] = rows
        return rows

    def find_nth_trading_day(price_rows: list[tuple], signal_date, n: int):
        """Find the Nth trading day on or after signal_date.

        price_rows: sorted list of (trade_date, open, close).
        n=0 means first trading day on or after signal_date.
        Returns (trade_date, open, close) or None.
        """
        # Filter to trading days on or after signal_date
        sig_d = signal_date.date() if hasattr(signal_date, "date") else signal_date
        future_days = [(d, o, c) for d, o, c in price_rows if d >= sig_d]
        if len(future_days) > n:
            return future_days[n]
        return None

    # 4. Process each signal and compute returns
    stats = {"filled": 0, "no_price_data": 0, "errors": 0}
    batch_rows = []
    BATCH_SIZE = 500

    now = datetime.now(timezone.utc)

    for i, signal_row in enumerate(signals):
        news_item_id = signal_row[0]
        ticker = signal_row[1]
        market = signal_row[2]
        event_time = signal_row[3]
        sentiment = signal_row[4]
        impact_magnitude = signal_row[5]
        impact_timeframe = signal_row[6]
        category = signal_row[7]
        surprise_factor = signal_row[8]
        signal_score = signal_row[9]
        signal_tier = signal_row[10]
        title = signal_row[11]
        summary = signal_row[12]
        sentiment_score_short = signal_row[13]
        confidence_short = signal_row[14]
        sentiment_label_short = signal_row[15]
        sentiment_score_medium = signal_row[16]
        confidence_medium = signal_row[17]
        sentiment_label_medium = signal_row[18]
        sentiment_score_long = signal_row[19]
        confidence_long = signal_row[20]
        sentiment_label_long = signal_row[21]
        source_name = signal_row[22] if len(signal_row) > 22 else ""

        try:
            price_rows = await get_price_data(ticker, market)
        except Exception as e:
            logger.warning("Failed to fetch price data for %s (%s): %s", ticker, market, e)
            stats["errors"] += 1
            continue

        if not price_rows:
            stats["no_price_data"] += 1
            # Mark as evaluated=2 (no_data) if signal is old enough (>3 days)
            # to avoid re-querying forever
            if (now - event_time).days > 3:
                no_data_row = [
                    news_item_id, ticker, market, event_time,
                    sentiment, impact_magnitude, impact_timeframe,
                    category, surprise_factor, signal_score, signal_tier,
                    title, summary,
                    None, None, None, None,  # price columns
                    None, None, None,  # return columns
                    None, now,  # prediction_correct, outcome_updated_at
                    sentiment_score_short, confidence_short, sentiment_label_short or "",
                    sentiment_score_medium, confidence_medium, sentiment_label_medium or "",
                    sentiment_score_long, confidence_long, sentiment_label_long or "",
                    None, None, None,  # return_t1/t5/t20
                    None, None, None,  # correct_t1/t5/t20
                    2,  # evaluated = 2 (no_data)
                    source_name or "",
                ]
                batch_rows.append(no_data_row)
            continue

        # Determine signal trading date
        signal_date = signal_date_from_event_time(event_time, market)

        # T+0: first trading day on or after signal_date
        t0 = find_nth_trading_day(price_rows, signal_date, 0)
        # T+1: next trading day
        t1 = find_nth_trading_day(price_rows, signal_date, 1)
        # T+5: 5th trading day after signal
        t5 = find_nth_trading_day(price_rows, signal_date, 5)
        # T+20: 20th trading day after signal
        t20 = find_nth_trading_day(price_rows, signal_date, 20)

        if t0 is None:
            stats["no_price_data"] += 1
            continue

        signal_close = t0[2]  # close on T+0

        # Compute returns
        return_t1 = None
        return_t5 = None
        return_t20 = None

        # Also fill the original columns: price_t0, price_t1_1d, return_1d, etc.
        price_t0 = float(signal_close) if signal_close else None
        price_t1_1d = None
        price_t2_3d = None
        price_t3_5d = None
        return_1d = None
        return_3d = None
        return_5d = None

        if signal_close and signal_close > 0:
            if t1 is not None:
                t1_close = t1[2]
                return_t1 = (t1_close - signal_close) / signal_close
                price_t1_1d = float(t1_close)
                return_1d = return_t1

            # T+3 for the original return_3d column
            t3 = find_nth_trading_day(price_rows, signal_date, 3)
            if t3 is not None:
                t3_close = t3[2]
                price_t2_3d = float(t3_close)
                return_3d = (t3_close - signal_close) / signal_close

            if t5 is not None:
                t5_close = t5[2]
                return_t5 = (t5_close - signal_close) / signal_close
                price_t3_5d = float(t5_close)
                return_5d = return_t5

            if t20 is not None:
                t20_close = t20[2]
                return_t20 = (t20_close - signal_close) / signal_close

        # Compute correctness per horizon
        correct_t1 = compute_correctness(sentiment_score_short, return_t1)
        correct_t5 = compute_correctness(sentiment_score_medium, return_t5)
        correct_t20 = compute_correctness(sentiment_score_long, return_t20)

        # Legacy prediction_correct: based on return_1d and sentiment
        prediction_correct = None
        if return_1d is not None and sentiment in ("bullish", "very_bullish", "bearish", "very_bearish"):
            if sentiment in ("bullish", "very_bullish"):
                prediction_correct = 1 if return_1d > 0 else 0
            else:
                prediction_correct = 1 if return_1d < 0 else 0

        # Build a full replacement row for ReplacingMergeTree (insert with newer outcome_updated_at)
        # Columns: all original + v2 columns
        updated_row = [
            news_item_id,
            ticker,
            market,
            event_time,
            sentiment,
            impact_magnitude,
            impact_timeframe,
            category,
            surprise_factor,
            signal_score,
            signal_tier,
            title,
            summary,
            # Original price/return columns
            price_t0,
            price_t1_1d,
            price_t2_3d,
            price_t3_5d,
            return_1d,
            return_3d,
            return_5d,
            prediction_correct,
            now,  # outcome_updated_at — newer than original → replaces in ReplacingMergeTree
            # V2 columns — sentiment scores pass through unchanged
            sentiment_score_short,
            confidence_short,
            sentiment_label_short or "",
            sentiment_score_medium,
            confidence_medium,
            sentiment_label_medium or "",
            sentiment_score_long,
            confidence_long,
            sentiment_label_long or "",
            # V2 return columns
            return_t1,
            return_t5,
            return_t20,
            correct_t1,
            correct_t5,
            correct_t20,
            1,  # evaluated = 1
            source_name or "",
        ]
        batch_rows.append(updated_row)
        stats["filled"] += 1

        # Flush in batches
        if len(batch_rows) >= BATCH_SIZE:
            await _flush_batch(client, db, batch_rows)
            logger.info("  Flushed batch of %d rows (%d/%d processed)",
                         len(batch_rows), i + 1, len(signals))
            batch_rows = []

    # Flush remaining
    if batch_rows:
        await _flush_batch(client, db, batch_rows)
        logger.info("  Flushed final batch of %d rows", len(batch_rows))

    # 5. Summary
    logger.info("=" * 60)
    logger.info("Fill returns complete!")
    logger.info("  Signals filled:      %d", stats["filled"])
    logger.info("  No price data:       %d", stats["no_price_data"])
    logger.info("  Errors:              %d", stats["errors"])
    logger.info("  Total processed:     %d", len(signals))
    logger.info("=" * 60)

    await asyncio.to_thread(client.close)


async def _flush_batch(client, db: str, rows: list[list]) -> None:
    """Insert a batch of updated rows into news_ticker_events."""
    columns = [
        "news_item_id", "ticker", "market", "event_time",
        "sentiment", "impact_magnitude", "impact_timeframe",
        "category", "surprise_factor", "signal_score", "signal_tier",
        "title", "summary",
        # Original price/return columns
        "price_t0", "price_t1_1d", "price_t2_3d", "price_t3_5d",
        "return_1d", "return_3d", "return_5d",
        "prediction_correct", "outcome_updated_at",
        # V2 sentiment columns
        "sentiment_score_short", "confidence_short", "sentiment_label_short",
        "sentiment_score_medium", "confidence_medium", "sentiment_label_medium",
        "sentiment_score_long", "confidence_long", "sentiment_label_long",
        # V2 return columns
        "return_t1", "return_t5", "return_t20",
        "correct_t1", "correct_t5", "correct_t20",
        "evaluated",
        "source_name",
    ]
    try:
        await asyncio.to_thread(
            client.insert,
            f"{db}.news_ticker_events",
            rows,
            column_names=columns,
        )
    except Exception as e:
        logger.error("Batch insert failed (%d rows): %s", len(rows), e)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="Fill return columns in news_ticker_events")
    parser.add_argument("--days", type=int, default=None,
                        help="Only process signals from the last N days (default: all unfilled)")
    parser.add_argument("--dry-run", action="store_true", help="Show counts only, don't update")
    args = parser.parse_args()

    asyncio.run(fill_returns(days=args.days, dry_run=args.dry_run))


if __name__ == "__main__":
    main()
