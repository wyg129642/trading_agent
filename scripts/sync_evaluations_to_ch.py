#!/usr/bin/env python3
"""Sync signal evaluations from PostgreSQL to ClickHouse.

Reads SignalEvaluation records from PostgreSQL and updates corresponding
news_ticker_events rows in ClickHouse with return/correctness data.

Uses the ReplacingMergeTree insert-to-update pattern: inserts a full
replacement row with a newer outcome_updated_at so ClickHouse deduplicates
and keeps the latest version.

Usage:
    python scripts/sync_evaluations_to_ch.py
    python scripts/sync_evaluations_to_ch.py --days 30
    python scripts/sync_evaluations_to_ch.py --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import asyncpg
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
ENV_PATH = PROJECT_ROOT / ".env"

# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------


def load_clickhouse_config() -> dict:
    """Load ClickHouse connection settings from config/settings.yaml."""
    with open(SETTINGS_PATH, "r") as f:
        settings = yaml.safe_load(f)
    return settings.get("clickhouse", {})


def get_ch_client(cfg: dict):
    """Create a clickhouse-connect client."""
    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=cfg.get("host", "192.168.31.137"),
        port=cfg.get("port", 38123),
        database=cfg.get("database", "db_spider"),
        username=cfg.get("user", "u_spider"),
        password=cfg.get("password", ""),
    )


def load_pg_dsn() -> str:
    """Build a PostgreSQL DSN from .env file or environment variables.

    Reads POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER,
    POSTGRES_PASSWORD from environment or .env file.
    """
    # Try loading from .env if env vars are not set
    if not os.environ.get("POSTGRES_HOST") and ENV_PATH.exists():
        with open(ENV_PATH) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, _, value = line.partition("=")
                    key = key.strip()
                    value = value.strip()
                    if key and key not in os.environ:
                        os.environ[key] = value

    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "trading_agent")
    user = os.environ.get("POSTGRES_USER", "trading_agent")
    password = os.environ.get("POSTGRES_PASSWORD", "")

    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


# ---------------------------------------------------------------------------
# Sync logic
# ---------------------------------------------------------------------------

BATCH_SIZE = 500


async def sync_evaluations(days: int | None, dry_run: bool) -> None:
    """Sync signal_evaluations from PostgreSQL to ClickHouse news_ticker_events."""
    start_time = time.monotonic()

    # 1. Connect to PostgreSQL
    pg_dsn = load_pg_dsn()
    logger.info("Connecting to PostgreSQL...")
    pg_conn = await asyncpg.connect(pg_dsn)
    logger.info("Connected to PostgreSQL")

    # 2. Connect to ClickHouse
    ch_cfg = load_clickhouse_config()
    ch_client = await asyncio.to_thread(get_ch_client, ch_cfg)
    db = ch_cfg.get("database", "db_spider")
    logger.info("Connected to ClickHouse %s:%s/%s", ch_cfg.get("host"), ch_cfg.get("port"), db)

    # 3. Query signal_evaluations from PostgreSQL
    where_clause = ""
    params = []
    if days is not None:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        where_clause = "WHERE evaluated_at >= $1"
        params = [cutoff]

    count_query = f"SELECT count(*) FROM signal_evaluations {where_clause}"
    total = await pg_conn.fetchval(count_query, *params)
    logger.info("Found %d signal evaluations in PostgreSQL%s",
                total, f" (last {days} days)" if days else "")

    if total == 0:
        logger.info("Nothing to sync")
        await pg_conn.close()
        await asyncio.to_thread(ch_client.close)
        return

    if dry_run:
        # Show breakdown
        breakdown_query = (
            f"SELECT source_name, count(*) as cnt "
            f"FROM signal_evaluations {where_clause} "
            f"GROUP BY source_name ORDER BY cnt DESC LIMIT 20"
        )
        rows = await pg_conn.fetch(breakdown_query, *params)
        logger.info("--- DRY RUN --- Evaluations by source:")
        for row in rows:
            logger.info("  %s: %d", row["source_name"], row["cnt"])
        await pg_conn.close()
        await asyncio.to_thread(ch_client.close)
        return

    # 4. Fetch all evaluations
    eval_query = (
        f"SELECT "
        f"  news_item_id, source_name, category, ticker, market, "
        f"  signal_time, predicted_sentiment, price_at_signal, "
        f"  return_t0, return_t1, return_t5, return_t20, "
        f"  correct_t0, correct_t1, correct_t5, correct_t20, "
        f"  evaluated_at, "
        f"  predicted_sentiment_t1, predicted_sentiment_t5, predicted_sentiment_t20, "
        f"  sentiment_score_t1, sentiment_score_t5, sentiment_score_t20, "
        f"  confidence_t1, confidence_t5, confidence_t20 "
        f"FROM signal_evaluations {where_clause} "
        f"ORDER BY evaluated_at ASC"
    )
    evals = await pg_conn.fetch(eval_query, *params)
    logger.info("Fetched %d evaluations for syncing", len(evals))

    # 5. For each evaluation, look up the existing CH row and re-insert with return data
    stats = {"synced": 0, "not_found": 0, "errors": 0}
    batch_rows = []
    now = datetime.now(timezone.utc)

    for i, ev in enumerate(evals):
        news_item_id = ev["news_item_id"]
        ticker = ev["ticker"]
        market = ev["market"]

        # Look up existing row in ClickHouse to get full data
        lookup_query = (
            f"SELECT "
            f"  news_item_id, ticker, market, event_time, "
            f"  sentiment, impact_magnitude, impact_timeframe, category, "
            f"  surprise_factor, signal_score, signal_tier, "
            f"  title, summary, "
            f"  source_category, "
            f"  sentiment_score_short, confidence_short, sentiment_label_short, "
            f"  sentiment_score_medium, confidence_medium, sentiment_label_medium, "
            f"  sentiment_score_long, confidence_long, sentiment_label_long "
            f"FROM {db}.news_ticker_events FINAL "
            f"WHERE news_item_id = %(nid)s AND ticker = %(ticker)s "
            f"LIMIT 1"
        )
        try:
            result = await asyncio.to_thread(
                ch_client.query, lookup_query,
                parameters={"nid": news_item_id, "ticker": ticker},
            )
        except Exception as e:
            logger.warning("CH lookup failed for %s/%s: %s", news_item_id, ticker, e)
            stats["errors"] += 1
            continue

        if not result.result_rows:
            stats["not_found"] += 1
            continue

        ch_row = result.result_rows[0]
        # Unpack existing CH row fields
        (
            _, ch_ticker, ch_market, event_time,
            sentiment, impact_magnitude, impact_timeframe, category,
            surprise_factor, signal_score, signal_tier,
            title, summary,
            source_category,
            ss_short, conf_short, sl_short,
            ss_medium, conf_medium, sl_medium,
            ss_long, conf_long, sl_long,
        ) = ch_row

        # Map PG evaluation data into CH columns
        price_t0 = float(ev["price_at_signal"]) if ev["price_at_signal"] is not None else None
        return_1d = float(ev["return_t1"]) if ev["return_t1"] is not None else None
        return_3d = None  # Not directly in signal_evaluations
        return_5d = float(ev["return_t5"]) if ev["return_t5"] is not None else None

        # price_t1_1d, price_t2_3d, price_t3_5d: compute from return + price_t0
        price_t1_1d = None
        price_t2_3d = None
        price_t3_5d = None
        if price_t0 is not None and price_t0 > 0:
            if return_1d is not None:
                price_t1_1d = price_t0 * (1.0 + return_1d)
            if return_5d is not None:
                price_t3_5d = price_t0 * (1.0 + return_5d)

        # Correctness: PG uses bool, CH uses UInt8
        def bool_to_uint8(val):
            if val is None:
                return None
            return 1 if val else 0

        prediction_correct = bool_to_uint8(ev["correct_t1"])
        correct_t1 = bool_to_uint8(ev["correct_t1"])
        correct_t5 = bool_to_uint8(ev["correct_t5"])
        correct_t20 = bool_to_uint8(ev["correct_t20"])

        return_t1 = float(ev["return_t1"]) if ev["return_t1"] is not None else None
        return_t5 = float(ev["return_t5"]) if ev["return_t5"] is not None else None
        return_t20 = float(ev["return_t20"]) if ev["return_t20"] is not None else None

        # Override sentiment scores from PG if available
        if ev["sentiment_score_t1"] is not None:
            ss_short = float(ev["sentiment_score_t1"])
        if ev["sentiment_score_t5"] is not None:
            ss_medium = float(ev["sentiment_score_t5"])
        if ev["sentiment_score_t20"] is not None:
            ss_long = float(ev["sentiment_score_t20"])
        if ev["confidence_t1"] is not None:
            conf_short = float(ev["confidence_t1"])
        if ev["confidence_t5"] is not None:
            conf_medium = float(ev["confidence_t5"])
        if ev["confidence_t20"] is not None:
            conf_long = float(ev["confidence_t20"])

        # Build full replacement row
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
            # Price/return columns
            price_t0,
            price_t1_1d,
            price_t2_3d,
            price_t3_5d,
            return_1d,
            return_3d,
            return_5d,
            prediction_correct,
            now,  # outcome_updated_at -- newer to trigger ReplacingMergeTree dedup
            # V2 sentiment columns
            source_category,
            ss_short, conf_short, sl_short or "",
            ss_medium, conf_medium, sl_medium or "",
            ss_long, conf_long, sl_long or "",
            # V2 return columns
            return_t1,
            return_t5,
            return_t20,
            correct_t1,
            correct_t5,
            correct_t20,
            1,  # evaluated = 1
            ev["source_name"] or "",  # source_name
        ]
        batch_rows.append(updated_row)
        stats["synced"] += 1

        # Flush in batches
        if len(batch_rows) >= BATCH_SIZE:
            await _flush_batch(ch_client, db, batch_rows)
            logger.info("  Flushed batch of %d rows (%d/%d processed)",
                        len(batch_rows), i + 1, len(evals))
            batch_rows = []

    # Flush remaining
    if batch_rows:
        await _flush_batch(ch_client, db, batch_rows)
        logger.info("  Flushed final batch of %d rows", len(batch_rows))

    elapsed = time.monotonic() - start_time

    # Summary
    logger.info("=" * 60)
    logger.info("SYNC COMPLETE")
    logger.info("  Evaluations synced:  %d", stats["synced"])
    logger.info("  Not found in CH:     %d", stats["not_found"])
    logger.info("  Errors:              %d", stats["errors"])
    logger.info("  Total processed:     %d", len(evals))
    logger.info("  Time elapsed:        %.1f seconds", elapsed)
    logger.info("=" * 60)

    await pg_conn.close()
    await asyncio.to_thread(ch_client.close)


async def _flush_batch(ch_client, db: str, rows: list[list]) -> None:
    """Insert a batch of updated rows into news_ticker_events."""
    columns = [
        "news_item_id", "ticker", "market", "event_time",
        "sentiment", "impact_magnitude", "impact_timeframe",
        "category", "surprise_factor", "signal_score", "signal_tier",
        "title", "summary",
        # Price/return columns
        "price_t0", "price_t1_1d", "price_t2_3d", "price_t3_5d",
        "return_1d", "return_3d", "return_5d",
        "prediction_correct", "outcome_updated_at",
        # V2 sentiment columns
        "source_category",
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
            ch_client.insert,
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
    parser = argparse.ArgumentParser(
        description="Sync signal evaluations from PostgreSQL to ClickHouse"
    )
    parser.add_argument(
        "--days", type=int, default=None,
        help="Only sync evaluations from the last N days (default: all)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show counts only, don't sync",
    )
    args = parser.parse_args()

    asyncio.run(sync_evaluations(days=args.days, dry_run=args.dry_run))


if __name__ == "__main__":
    main()
