#!/usr/bin/env python3
"""Backfill stock prices from akshare into ClickHouse stock_prices table.

Usage:
    python scripts/backfill_prices.py --days 90     # Last 90 days
    python scripts/backfill_prices.py --days 30     # Last 30 days
    python scripts/backfill_prices.py --dry-run     # Show tickers only
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import akshare as ak
import pandas as pd
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
# Ticker normalisation
# ---------------------------------------------------------------------------

# Pattern: "英伟达(NVDA)" → extract "NVDA"
_PAREN_TICKER_RE = re.compile(r"[（(]([A-Za-z0-9.]+)[）)]")


def normalize_ticker(raw_ticker: str, market: str) -> tuple[str, str]:
    """Normalize a ticker string for akshare lookup.

    Returns (clean_ticker, effective_market).
    """
    ticker = raw_ticker.strip()

    # Handle Chinese-named tickers with parenthetical codes: "英伟达(NVDA)" → "NVDA"
    m = _PAREN_TICKER_RE.search(ticker)
    if m:
        ticker = m.group(1)
        # If the extracted code is all-alpha, treat as US
        if ticker.isalpha():
            market = "us"

    # Strip exchange suffixes
    for suffix in (".SZ", ".SS", ".SH", ".HK", ".hk"):
        if ticker.endswith(suffix):
            ticker = ticker[: -len(suffix)]
            break

    # Normalize by market
    if market == "china":
        # Pad to 6 digits for A-shares
        if ticker.isdigit():
            ticker = ticker.zfill(6)
    elif market == "hk":
        if ticker.isdigit():
            ticker = ticker.zfill(5)
    elif market == "us":
        ticker = ticker.upper()

    return ticker, market


# ---------------------------------------------------------------------------
# Price fetching (reuses logic from engine/analysis/source_scorer.py)
# ---------------------------------------------------------------------------


def _unproxy_context():
    """Context manager to temporarily remove HTTP proxy env vars.

    akshare uses requests which picks up HTTP_PROXY. For China-domestic
    endpoints (eastmoney.com), we need direct access without proxy.
    """
    from contextlib import contextmanager

    @contextmanager
    def _ctx():
        saved = {}
        for key in ("HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy"):
            if key in os.environ:
                saved[key] = os.environ.pop(key)
        try:
            yield
        finally:
            os.environ.update(saved)

    return _ctx()


def fetch_ohlcv(
    ticker: str,
    market: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame | None:
    """Fetch daily OHLCV for a ticker via akshare.

    Parameters:
        start_date, end_date: 'YYYYMMDD' format.

    Returns DataFrame with columns [date, open, high, low, close, volume]
    or None on failure.
    """
    try:
        if market == "china":
            with _unproxy_context():
                df = ak.stock_zh_a_hist(
                    symbol=ticker,
                    period="daily",
                    start_date=start_date,
                    end_date=end_date,
                    adjust="qfq",
                )
            if df is None or df.empty:
                return None
            df = df.rename(columns={
                "日期": "date",
                "开盘": "open",
                "最高": "high",
                "最低": "low",
                "收盘": "close",
                "成交量": "volume",
            })
            df["date"] = pd.to_datetime(df["date"])
            return df[["date", "open", "high", "low", "close", "volume"]].sort_values("date").reset_index(drop=True)

        elif market == "us":
            df = ak.stock_us_daily(symbol=ticker, adjust="qfq")
            if df is None or df.empty:
                return None
            df["date"] = pd.to_datetime(df["date"])
            sd = pd.to_datetime(start_date)
            ed = pd.to_datetime(end_date)
            df = df[(df["date"] >= sd) & (df["date"] <= ed)]
            # US daily columns: date, open, high, low, close, volume
            cols = ["date", "open", "high", "low", "close", "volume"]
            # Rename if needed (akshare US usually has English names)
            for col in cols:
                if col not in df.columns:
                    df[col] = 0
            return df[cols].sort_values("date").reset_index(drop=True)

        elif market == "hk":
            df = ak.stock_hk_daily(symbol=ticker, adjust="qfq")
            if df is None or df.empty:
                return None
            df["date"] = pd.to_datetime(df["date"])
            sd = pd.to_datetime(start_date)
            ed = pd.to_datetime(end_date)
            df = df[(df["date"] >= sd) & (df["date"] <= ed)]
            cols = ["date", "open", "high", "low", "close", "volume"]
            for col in cols:
                if col not in df.columns:
                    df[col] = 0
            return df[cols].sort_values("date").reset_index(drop=True)

        else:
            logger.debug("Unsupported market %r for ticker %r", market, ticker)
            return None

    except Exception as e:
        logger.warning("Failed to fetch price for %s (%s): %s", ticker, market, e)
        return None


# ---------------------------------------------------------------------------
# Main backfill logic
# ---------------------------------------------------------------------------

RATE_LIMIT_SLEEP = 0.5  # seconds between akshare API calls


async def backfill(days: int, dry_run: bool) -> None:
    """Main backfill routine."""
    cfg = load_clickhouse_config()
    client = await asyncio.to_thread(get_client, cfg)
    db = cfg.get("database", "db_spider")

    logger.info("Connected to ClickHouse %s:%s/%s", cfg.get("host"), cfg.get("port"), db)

    # 1. Get all unique (ticker, market) pairs from news_ticker_events
    query = f"SELECT DISTINCT ticker, market FROM {db}.news_ticker_events"
    result = await asyncio.to_thread(client.query, query)
    tickers = [(row[0], row[1]) for row in result.result_rows]

    logger.info("Found %d unique (ticker, market) pairs", len(tickers))

    if not tickers:
        logger.info("No tickers found — nothing to backfill")
        await asyncio.to_thread(client.close)
        return

    if dry_run:
        logger.info("--- DRY RUN — would fetch prices for these tickers ---")
        for raw_ticker, market in tickers:
            clean_ticker, eff_market = normalize_ticker(raw_ticker, market)
            logger.info("  %s (%s) → akshare: %s (%s)", raw_ticker, market, clean_ticker, eff_market)
        await asyncio.to_thread(client.close)
        return

    # 2. Date range
    end_dt = datetime.now()
    start_dt = end_dt - timedelta(days=days)
    start_str = start_dt.strftime("%Y%m%d")
    end_str = end_dt.strftime("%Y%m%d")

    logger.info("Backfilling prices from %s to %s (%d days)", start_str, end_str, days)

    # 3. Fetch and insert for each ticker
    stats = {"success": 0, "failed": 0, "rows_inserted": 0, "skipped": 0}

    for i, (raw_ticker, market) in enumerate(tickers, 1):
        clean_ticker, eff_market = normalize_ticker(raw_ticker, market)
        logger.info("[%d/%d] Fetching %s (%s) → akshare: %s (%s)",
                     i, len(tickers), raw_ticker, market, clean_ticker, eff_market)

        try:
            df = await asyncio.to_thread(fetch_ohlcv, clean_ticker, eff_market, start_str, end_str)
        except Exception as e:
            logger.error("  Exception fetching %s: %s", clean_ticker, e)
            stats["failed"] += 1
            time.sleep(RATE_LIMIT_SLEEP)
            continue

        if df is None or df.empty:
            logger.warning("  No data returned for %s (%s)", clean_ticker, eff_market)
            stats["failed"] += 1
            time.sleep(RATE_LIMIT_SLEEP)
            continue

        # Prepare rows for stock_prices table
        # Schema: ticker, market, trade_date, open, high, low, close, volume
        rows = []
        for _, row in df.iterrows():
            trade_date = row["date"].date() if hasattr(row["date"], "date") else row["date"]
            rows.append([
                raw_ticker,          # store original ticker as it appears in news_ticker_events
                market,              # original market
                trade_date,
                float(row["open"]),
                float(row["high"]),
                float(row["low"]),
                float(row["close"]),
                int(row["volume"]) if pd.notna(row["volume"]) else 0,
            ])

        if rows:
            try:
                await asyncio.to_thread(
                    client.insert,
                    f"{db}.stock_prices",
                    rows,
                    column_names=[
                        "ticker", "market", "trade_date",
                        "open", "high", "low", "close", "volume",
                    ],
                )
                stats["success"] += 1
                stats["rows_inserted"] += len(rows)
                logger.info("  Inserted %d price rows for %s", len(rows), raw_ticker)
            except Exception as e:
                logger.error("  Insert failed for %s: %s", raw_ticker, e)
                stats["failed"] += 1
        else:
            stats["skipped"] += 1

        # Rate limit
        time.sleep(RATE_LIMIT_SLEEP)

    # 4. Summary
    logger.info("=" * 60)
    logger.info("Backfill complete!")
    logger.info("  Tickers succeeded: %d", stats["success"])
    logger.info("  Tickers failed:    %d", stats["failed"])
    logger.info("  Tickers skipped:   %d", stats["skipped"])
    logger.info("  Total rows inserted: %d", stats["rows_inserted"])
    logger.info("=" * 60)

    await asyncio.to_thread(client.close)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="Backfill stock prices into ClickHouse")
    parser.add_argument("--days", type=int, default=90, help="Number of days to backfill (default: 90)")
    parser.add_argument("--dry-run", action="store_true", help="Show tickers only, don't fetch prices")
    args = parser.parse_args()

    asyncio.run(backfill(days=args.days, dry_run=args.dry_run))


if __name__ == "__main__":
    main()
