#!/usr/bin/env python3
"""Re-check and fix stock tickers in all enriched articles.

Scans all enriched items across alphapai (articles, roadshows, comments),
jiuqian (forum, minutes, wechat), and news (analysis_results) tables.
Verifies each ticker against stock lists and Baidu search,
then updates incorrect tickers in-place.

Usage:
    cd /home/ygwang/trading_agent
    python -m scripts.recheck_stock_tickers [--dry-run] [--limit N] [--channel CHANNEL]

Options:
    --dry-run       Show changes without writing to DB
    --limit N       Process at most N items per table (default: all)
    --channel       Only process: alphapai, jiuqian, news, or all (default: all)
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Load environment
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import httpx
from openai import AsyncOpenAI
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from backend.app.config import get_settings
from backend.app.services.stock_verifier import StockVerifier

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


async def get_db_session(settings):
    engine = create_async_engine(settings.database_url, echo=False)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    return async_session, engine


def create_verifier(settings) -> StockVerifier:
    llm = AsyncOpenAI(
        api_key=settings.minimax_api_key,
        base_url=settings.minimax_base_url,
        timeout=90.0,
        http_client=httpx.AsyncClient(trust_env=False, timeout=90.0),
    )
    verifier = StockVerifier(
        baidu_api_key=settings.baidu_api_key,
        llm_client=llm,
        llm_model=settings.minimax_model,
    )
    verifier.load_stock_lists()
    return verifier



# Table name → primary key column mapping
TABLE_PK = {
    "alphapai_comments": "cmnt_hcode",
    "alphapai_roadshows_cn": "trans_id",
    "alphapai_roadshows_us": "trans_id",
    "alphapai_articles": "arc_code",
    "jiuqian_forum": "id",
    "jiuqian_minutes": "id",
    "jiuqian_wechat": "id",
}


async def recheck_enrichment_table(
    session: AsyncSession,
    verifier: StockVerifier,
    table_name: str,
    dry_run: bool,
    limit: int | None,
) -> dict:
    """Re-check tickers in a table with JSONB enrichment column."""
    stats = {"total": 0, "checked": 0, "fixed": 0, "dropped": 0, "errors": 0}
    pk_col = TABLE_PK.get(table_name, "id")

    # Query enriched items with tickers
    query = f"""
        SELECT {pk_col}, enrichment
        FROM {table_name}
        WHERE is_enriched = true
          AND enrichment IS NOT NULL
          AND enrichment->'tickers' IS NOT NULL
          AND jsonb_array_length(enrichment->'tickers') > 0
        ORDER BY {pk_col} DESC
    """
    if limit:
        query += f" LIMIT {limit}"

    result = await session.execute(text(query))
    rows = result.fetchall()
    stats["total"] = len(rows)
    logger.info("[%s] Found %d enriched items with tickers", table_name, len(rows))

    for row_id, enrichment in rows:
        if not isinstance(enrichment, dict):
            continue
        old_tickers = enrichment.get("tickers", [])
        if not old_tickers:
            continue

        stats["checked"] += 1
        try:
            new_tickers = await verifier.verify_tickers(old_tickers)
        except Exception as e:
            logger.error("[%s] %s=%s verify error: %s", table_name, pk_col, row_id, e)
            stats["errors"] += 1
            continue

        # Compare
        if set(old_tickers) == set(new_tickers):
            continue  # No change needed

        dropped = len(old_tickers) - len(new_tickers)
        if dropped > 0:
            stats["dropped"] += dropped

        if old_tickers != new_tickers:
            stats["fixed"] += 1
            logger.info(
                "[%s] %s=%s: %s → %s",
                table_name, pk_col, row_id,
                old_tickers, new_tickers,
            )
            if not dry_run:
                enrichment["tickers"] = new_tickers
                enrichment["_ticker_rechecked"] = True
                await session.execute(
                    text(f"UPDATE {table_name} SET enrichment = :enr WHERE {pk_col} = :pk"),
                    {"enr": json.dumps(enrichment, ensure_ascii=False), "pk": row_id},
                )

    if not dry_run and stats["fixed"] > 0:
        await session.commit()

    return stats


async def recheck_news_tickers(
    session: AsyncSession,
    verifier: StockVerifier,
    dry_run: bool,
    limit: int | None,
) -> dict:
    """Re-check tickers in analysis_results table (JSONB affected_tickers column)."""
    stats = {"total": 0, "checked": 0, "fixed": 0, "dropped": 0, "errors": 0}

    query = """
        SELECT news_item_id, affected_tickers
        FROM analysis_results
        WHERE affected_tickers IS NOT NULL
          AND jsonb_array_length(affected_tickers) > 0
        ORDER BY news_item_id DESC
    """
    if limit:
        query += f" LIMIT {limit}"

    result = await session.execute(text(query))
    rows = result.fetchall()
    stats["total"] = len(rows)
    logger.info("[analysis_results] Found %d items with tickers", len(rows))

    for news_id, old_tickers in rows:
        if not old_tickers:
            continue
        stats["checked"] += 1
        try:
            new_tickers = await verifier.verify_news_tickers(old_tickers)
        except Exception as e:
            logger.error("[analysis_results] id=%s verify error: %s", news_id, e)
            stats["errors"] += 1
            continue

        if set(old_tickers) == set(new_tickers):
            continue

        dropped = len(old_tickers) - len(new_tickers)
        if dropped > 0:
            stats["dropped"] += dropped

        if old_tickers != new_tickers:
            stats["fixed"] += 1
            logger.info(
                "[analysis_results] id=%s: %s → %s",
                news_id, old_tickers, new_tickers,
            )
            if not dry_run:
                await session.execute(
                    text("UPDATE analysis_results SET affected_tickers = :tickers WHERE news_item_id = :id"),
                    {"tickers": json.dumps(new_tickers, ensure_ascii=False), "id": news_id},
                )

    if not dry_run and stats["fixed"] > 0:
        await session.commit()

    return stats


async def main():
    parser = argparse.ArgumentParser(description="Re-check stock tickers in enriched articles")
    parser.add_argument("--dry-run", action="store_true", help="Show changes without writing")
    parser.add_argument("--limit", type=int, default=None, help="Max items per table")
    parser.add_argument("--channel", default="all", choices=["all", "alphapai", "jiuqian", "news"])
    args = parser.parse_args()

    settings = get_settings()
    verifier = create_verifier(settings)
    async_session_factory, engine = await get_db_session(settings)

    all_stats = {}

    async with async_session_factory() as session:
        # AlphaPai tables
        if args.channel in ("all", "alphapai"):
            for table in ["alphapai_comments", "alphapai_roadshows_cn", "alphapai_roadshows_us", "alphapai_articles"]:
                logger.info("=== Processing %s ===", table)
                stats = await recheck_enrichment_table(session, verifier, table, args.dry_run, args.limit)
                all_stats[table] = stats
                logger.info("[%s] Stats: %s", table, stats)

        # Jiuqian tables
        if args.channel in ("all", "jiuqian"):
            for table in ["jiuqian_forum", "jiuqian_minutes", "jiuqian_wechat"]:
                logger.info("=== Processing %s ===", table)
                stats = await recheck_enrichment_table(session, verifier, table, args.dry_run, args.limit)
                all_stats[table] = stats
                logger.info("[%s] Stats: %s", table, stats)

        # News table
        if args.channel in ("all", "news"):
            logger.info("=== Processing analysis_results ===")
            stats = await recheck_news_tickers(session, verifier, args.dry_run, args.limit)
            all_stats["analysis_results"] = stats
            logger.info("[analysis_results] Stats: %s", stats)

    await engine.dispose()

    # Summary
    print("\n" + "=" * 60)
    print("RECHECK SUMMARY" + (" (DRY RUN)" if args.dry_run else ""))
    print("=" * 60)
    total_fixed = 0
    total_dropped = 0
    for table, stats in all_stats.items():
        print(f"  {table}: checked={stats['checked']}, fixed={stats['fixed']}, dropped={stats['dropped']}, errors={stats['errors']}")
        total_fixed += stats["fixed"]
        total_dropped += stats["dropped"]
    print(f"\n  TOTAL: {total_fixed} items fixed, {total_dropped} invalid tickers dropped")


if __name__ == "__main__":
    asyncio.run(main())
