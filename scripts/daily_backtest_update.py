#!/usr/bin/env python3
"""Daily backtesting data update -- run after market close.

Chains:
1. Backfill stock prices (last 5 days to catch gaps)
2. Fill returns for unevaluated signals
3. Print summary

Usage:
    python scripts/daily_backtest_update.py
    python scripts/daily_backtest_update.py --price-days 10  # Custom price lookback
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time
from pathlib import Path

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
sys.path.insert(0, str(PROJECT_ROOT))

# ---------------------------------------------------------------------------
# Import the main functions from existing scripts
# ---------------------------------------------------------------------------

from scripts.backfill_prices import backfill  # noqa: E402
from scripts.fill_returns import fill_returns  # noqa: E402


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def daily_update(price_days: int, return_days: int | None) -> None:
    """Run the full daily backtest update pipeline."""
    overall_start = time.monotonic()

    # Step 1: Backfill prices
    logger.info("=" * 60)
    logger.info("STEP 1/2: Backfilling stock prices (last %d days)", price_days)
    logger.info("=" * 60)
    step1_start = time.monotonic()
    try:
        await backfill(days=price_days, dry_run=False)
    except Exception as e:
        logger.error("Price backfill failed: %s", e)
        logger.info("Continuing to fill_returns despite price backfill failure...")
    step1_elapsed = time.monotonic() - step1_start
    logger.info("Step 1 completed in %.1f seconds", step1_elapsed)

    # Step 2: Fill returns for unevaluated signals
    logger.info("")
    logger.info("=" * 60)
    logger.info("STEP 2/2: Filling returns for unevaluated signals")
    if return_days:
        logger.info("  (limited to last %d days)", return_days)
    logger.info("=" * 60)
    step2_start = time.monotonic()
    try:
        await fill_returns(days=return_days, dry_run=False)
    except Exception as e:
        logger.error("Fill returns failed: %s", e)
    step2_elapsed = time.monotonic() - step2_start
    logger.info("Step 2 completed in %.1f seconds", step2_elapsed)

    # Summary
    overall_elapsed = time.monotonic() - overall_start
    logger.info("")
    logger.info("=" * 60)
    logger.info("DAILY BACKTEST UPDATE COMPLETE")
    logger.info("  Total time: %.1f seconds (%.1f min)", overall_elapsed, overall_elapsed / 60)
    logger.info("  Price backfill: %.1f s", step1_elapsed)
    logger.info("  Fill returns:   %.1f s", step2_elapsed)
    logger.info("=" * 60)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Daily backtesting data update (prices + returns)"
    )
    parser.add_argument(
        "--price-days", type=int, default=5,
        help="Number of days to backfill stock prices (default: 5)",
    )
    parser.add_argument(
        "--return-days", type=int, default=None,
        help="Only fill returns for signals from last N days (default: all unfilled)",
    )
    args = parser.parse_args()

    asyncio.run(daily_update(
        price_days=args.price_days,
        return_days=args.return_days,
    ))


if __name__ == "__main__":
    main()
