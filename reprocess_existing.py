#!/usr/bin/env python3
"""Reprocess existing non-neutral news items with the improved deep research pipeline.

Re-runs Phase 2 (Deep Research) + Phase 3 (Final Assessment) for items that
already have analysis results, using the updated prompts, Google+Baidu parallel
search, and improved timeline/citation extraction.

Usage:
    python reprocess_existing.py              # Reprocess all non-neutral items
    python reprocess_existing.py --limit 5    # Reprocess only 5 items
    python reprocess_existing.py --dry-run    # Show what would be reprocessed
"""

import argparse
import asyncio
import json
import logging
import os
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path

# Add project root to path
project_root = Path(__file__).resolve().parent
sys.path.insert(0, str(project_root))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)-25s | %(message)s",
)
logger = logging.getLogger("reprocess")


async def main():
    parser = argparse.ArgumentParser(description="Reprocess existing news with improved deep research")
    parser.add_argument("--limit", type=int, default=0, help="Max items to reprocess (0=all)")
    parser.add_argument("--dry-run", action="store_true", help="Show candidates without processing")
    parser.add_argument("--item-id", type=str, default="", help="Reprocess a specific item by ID")
    parser.add_argument("--days", type=int, default=7, help="Reprocess items from last N days (0=all non-neutral)")
    args = parser.parse_args()

    import yaml
    from engine.analysis.llm_client import LLMClient
    from engine.analysis.pipeline import AnalysisPipeline
    from engine.models import NewsItem, InitialEvaluation
    from engine.utils.content_fetcher import ContentFetcher
    from engine.utils.browser_manager import BrowserManager

    # Load config
    config_base = project_root / "config"
    with open(config_base / "settings.yaml", "r", encoding="utf-8") as f:
        settings = yaml.safe_load(f)

    # Connect to PostgreSQL
    import asyncpg
    conn = await asyncpg.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        database=os.environ.get("POSTGRES_DB", "trading_agent"),
        user=os.environ.get("POSTGRES_USER", "trading_agent"),
        password=os.environ.get("POSTGRES_PASSWORD", "TradingAgent2025Secure"),
    )

    # Find candidates: non-neutral items with analysis
    if args.item_id:
        rows = await conn.fetch("""
            SELECT n.id, n.source_name, n.title, n.url, n.content,
                   n.published_at, n.fetched_at, n.language, n.market, n.metadata,
                   a.sentiment, a.impact_magnitude, a.affected_tickers, a.affected_sectors,
                   a.category
            FROM news_items n
            JOIN analysis_results a ON n.id = a.news_item_id
            WHERE n.id = $1
        """, args.item_id)
    else:
        if args.days > 0:
            rows = await conn.fetch(f"""
                SELECT n.id, n.source_name, n.title, n.url, n.content,
                       n.published_at, n.fetched_at, n.language, n.market, n.metadata,
                       a.sentiment, a.impact_magnitude, a.affected_tickers, a.affected_sectors,
                       a.category
                FROM news_items n
                JOIN analysis_results a ON n.id = a.news_item_id
                WHERE a.sentiment != 'neutral'
                  AND n.fetched_at >= NOW() - INTERVAL '{args.days} days'
                ORDER BY n.fetched_at DESC
            """)
        else:
            rows = await conn.fetch("""
                SELECT n.id, n.source_name, n.title, n.url, n.content,
                       n.published_at, n.fetched_at, n.language, n.market, n.metadata,
                       a.sentiment, a.impact_magnitude, a.affected_tickers, a.affected_sectors,
                       a.category
                FROM news_items n
                JOIN analysis_results a ON n.id = a.news_item_id
                WHERE a.sentiment != 'neutral'
                ORDER BY n.fetched_at DESC
            """)

    if args.limit > 0:
        rows = rows[:args.limit]

    print(f"\n{'='*70}")
    print(f"  Reprocess Deep Research — {len(rows)} candidate items")
    print(f"{'='*70}")

    if args.dry_run:
        for r in rows:
            print(f"  [{r['sentiment']:12s}|{r['impact_magnitude']:8s}] {r['title'][:65]}")
        print(f"\n  (dry run — no changes made)")
        await conn.close()
        return

    # Clear existing signal evaluations for items being reprocessed
    if not args.dry_run and rows:
        item_ids = [r["id"] for r in rows]
        placeholders = ", ".join(f"${i+1}" for i in range(len(item_ids)))
        deleted = await conn.execute(
            f"DELETE FROM signal_evaluations WHERE news_item_id IN ({placeholders})",
            *item_ids,
        )
        print(f"  Cleared {deleted} old signal evaluations for reprocessed items")

    # Initialize pipeline components
    llm = LLMClient(settings)

    # Create PostgresDatabase instance
    from engine.pg_database import PostgresDatabase
    pg_dsn = f"postgresql://trading_agent:TradingAgent2025Secure@localhost:5432/trading_agent"
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    db = PostgresDatabase(pg_dsn, redis_url)
    await db.initialize()

    # Content fetcher for URL retrieval
    browser_mgr = BrowserManager()
    content_fetcher = ContentFetcher(browser_mgr)

    pipeline = AnalysisPipeline(
        llm=llm,
        db=db,
        settings=settings,
        content_fetcher=content_fetcher,
        uqer_token=settings.get("uqer", {}).get("token", "") or os.environ.get("UQER_TOKEN", ""),
    )

    processed = 0
    errors = 0

    for i, r in enumerate(rows):
        item_id = r["id"]
        title = r["title"]
        print(f"\n--- [{i+1}/{len(rows)}] {title[:65]}")
        print(f"    ID: {item_id} | {r['sentiment']} | {r['impact_magnitude']}")

        try:
            # Reconstruct NewsItem
            metadata = r["metadata"]
            if isinstance(metadata, str):
                metadata = json.loads(metadata)
            elif metadata is None:
                metadata = {}

            item = NewsItem(
                source_name=r["source_name"],
                title=r["title"],
                url=r["url"],
                content=r["content"] or "",
                published_at=r["published_at"],
                language=r["language"] or "zh",
                market=r["market"] or "china",
                metadata=metadata,
            )
            item.id = item_id

            # Reconstruct InitialEvaluation from existing analysis
            tickers = r["affected_tickers"]
            if isinstance(tickers, str):
                tickers = json.loads(tickers)
            sectors = r["affected_sectors"]
            if isinstance(sectors, str):
                sectors = json.loads(sectors)

            # Re-run Phase 1 to get fresh search queries with new prompts
            print("    Phase 1: Generating search queries...")
            evaluation, _ = await pipeline._phase1_evaluate(item)

            # If Phase 1 says not relevant, use existing data to force it through
            if not evaluation.may_affect_market:
                evaluation.may_affect_market = True
                evaluation.relevance_score = max(evaluation.relevance_score, 0.5)

            # Ensure we have stocks from existing analysis if Phase 1 missed them
            if not evaluation.related_stocks and tickers:
                evaluation.related_stocks = [{"name": t, "ticker": t} for t in tickers]
            if not evaluation.related_sectors and sectors:
                evaluation.related_sectors = sectors

            print(f"    Phase 1: score={evaluation.relevance_score:.2f}, "
                  f"baidu_queries={sum(len(v) for v in evaluation.search_queries.values())}, "
                  f"google_queries={sum(len(v) for v in evaluation.google_queries.values())}")

            # Run Phase 2: Deep Research with improved prompts + Google
            print("    Phase 2: Running deep research (Baidu + Google parallel)...")
            deep_research, _ = await pipeline._phase2_deep_research(item, evaluation)

            print(f"    Phase 2: {deep_research.total_iterations} iterations, "
                  f"{len(deep_research.all_search_results)} search results, "
                  f"{len(deep_research.all_fetched_pages)} pages fetched, "
                  f"{len(deep_research.news_timeline)} timeline entries, "
                  f"{len(deep_research.referenced_sources)} referenced sources")

            # Run Phase 3: Final Assessment with research context
            print("    Phase 3: Final assessment...")
            assessment, _ = await pipeline._phase3_assess(item, evaluation, deep_research)

            print(f"    Phase 3: sentiment={assessment.sentiment}, "
                  f"impact={assessment.impact_magnitude}, "
                  f"surprise={assessment.surprise_factor:.2f}")

            # Save updated results
            analysis = pipeline._build_analysis_result(item_id, evaluation, assessment)
            await db.save_analysis_result(analysis)

            research_report = pipeline._build_research_report(item_id, assessment, deep_research)
            await db.save_research_report(research_report)

            print(f"    ✅ Saved! citations={len(deep_research.citations)}, "
                  f"timeline={len(deep_research.news_timeline)}")
            processed += 1

        except Exception as e:
            print(f"    ❌ Error: {e}")
            traceback.print_exc()
            errors += 1

    # Cleanup
    await browser_mgr.close()
    await conn.close()

    print(f"\n{'='*70}")
    print(f"  Done: {processed} processed, {errors} errors")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    asyncio.run(main())
