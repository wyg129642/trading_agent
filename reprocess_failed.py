#!/usr/bin/env python3
"""Reprocess news items that failed LLM analysis due to API balance exhaustion.

Targets items fetched since 2026-03-20 10:00 that have:
  - filter_results.relevance_score = 0 (LLM returned error, defaulted to 0)
  - OR no filter_results at all (never processed)

Re-runs them through the full pipeline (Phase 1 → 2 → 3) using the current
LLM provider (MiniMax).

Usage:
    python reprocess_failed.py              # Reprocess all failed items
    python reprocess_failed.py --limit 50   # Reprocess first 50 items
    python reprocess_failed.py --dry-run    # Show candidates without processing
    python reprocess_failed.py --since 2026-03-20  # Custom start date
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
logger = logging.getLogger("reprocess_failed")


async def main():
    parser = argparse.ArgumentParser(description="Reprocess failed LLM analysis items")
    parser.add_argument("--limit", type=int, default=0, help="Max items to reprocess (0=all)")
    parser.add_argument("--dry-run", action="store_true", help="Show candidates without processing")
    parser.add_argument("--since", type=str, default="2026-03-20 10:00:00",
                        help="Start date for items to reprocess (default: 2026-03-20 10:00:00)")
    # Sequential processing — pipeline internally parallelizes search calls
    args = parser.parse_args()

    import yaml
    from src.analysis.llm_client import LLMClient
    from src.analysis.pipeline import AnalysisPipeline
    from src.models import NewsItem
    from src.utils.content_fetcher import ContentFetcher
    from src.utils.browser_manager import BrowserManager

    # Load config
    config_base = project_root / "config"
    with open(config_base / "settings.yaml", "r", encoding="utf-8") as f:
        settings = yaml.safe_load(f)

    print(f"\n  LLM Provider: {settings['llm']['provider']}")
    print(f"  Model: {settings['llm'].get('model_filter', 'N/A')}")

    # Connect to PostgreSQL
    import asyncpg
    conn = await asyncpg.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        database=os.environ.get("POSTGRES_DB", "trading_agent"),
        user=os.environ.get("POSTGRES_USER", "trading_agent"),
        password=os.environ.get("POSTGRES_PASSWORD", "TradingAgent2025Secure"),
    )

    # Find failed items: score=0 (LLM error) OR no filter result at all
    since_dt = datetime.strptime(args.since, "%Y-%m-%d %H:%M:%S")
    rows = await conn.fetch("""
        SELECT n.id, n.source_name, n.title, n.url, n.content,
               n.published_at, n.fetched_at, n.language, n.market, n.metadata
        FROM news_items n
        LEFT JOIN filter_results f ON n.id = f.news_item_id
        WHERE n.fetched_at >= $1
          AND (f.news_item_id IS NULL OR f.relevance_score = 0)
        ORDER BY n.fetched_at ASC
    """, since_dt)

    if args.limit > 0:
        rows = rows[:args.limit]

    print(f"\n{'='*70}")
    print(f"  Reprocess Failed Items — {len(rows)} candidates (since {args.since})")
    print(f"{'='*70}")

    if not rows:
        print("  No items to reprocess.")
        await conn.close()
        return

    if args.dry_run:
        for i, r in enumerate(rows):
            print(f"  [{i+1:4d}] [{r['source_name']:20s}] {r['title'][:60]}")
        print(f"\n  (dry run — no changes made)")
        await conn.close()
        return

    # Initialize pipeline
    llm = LLMClient(settings)

    from src.pg_database import PostgresDatabase
    pg_dsn = "postgresql://trading_agent:TradingAgent2025Secure@localhost:5432/trading_agent"
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    db = PostgresDatabase(pg_dsn, redis_url)
    await db.initialize()

    browser_mgr = BrowserManager()
    content_fetcher = ContentFetcher(browser_mgr)

    pipeline = AnalysisPipeline(
        llm=llm,
        db=db,
        settings=settings,
        content_fetcher=content_fetcher,
        uqer_token=settings.get("uqer", {}).get("token", "") or os.environ.get("UQER_TOKEN", ""),
    )

    # Use a connection pool for delete operations (asyncpg single conn can't handle concurrent ops)
    pool = await asyncpg.create_pool(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        database=os.environ.get("POSTGRES_DB", "trading_agent"),
        user=os.environ.get("POSTGRES_USER", "trading_agent"),
        password=os.environ.get("POSTGRES_PASSWORD", "TradingAgent2025Secure"),
        min_size=1, max_size=5,
    )

    processed = 0
    filtered_out = 0
    fully_analyzed = 0
    errors = 0

    for i, r in enumerate(rows):
        title = r["title"]
        try:
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
            item.id = r["id"]

            # Delete old broken filter result before reprocessing
            async with pool.acquire() as pconn:
                await pconn.execute(
                    "DELETE FROM filter_results WHERE news_item_id = $1", r["id"]
                )

            # Run full pipeline (Phase 1 → 2 → 3)
            result = await pipeline.process(item)

            stage = result.get("stage", 1)
            if stage >= 3:
                assessment = result.get("final_assessment")
                logger.info(
                    "[%d/%d] Full analysis: %s | sentiment=%s impact=%s | %s",
                    i + 1, len(rows), title[:45],
                    assessment.sentiment if assessment else "?",
                    assessment.impact_magnitude if assessment else "?",
                    r["source_name"],
                )
                fully_analyzed += 1
            else:
                score = result["filter"].relevance_score if result.get("filter") else 0
                logger.info(
                    "[%d/%d] Filtered (score=%.2f): %s",
                    i + 1, len(rows), score, title[:50],
                )
                filtered_out += 1

            processed += 1

        except Exception as e:
            logger.error("[%d/%d] Error processing '%s': %s", i + 1, len(rows), title[:40], e)
            traceback.print_exc()
            errors += 1

    # Cleanup
    await browser_mgr.close()
    await pool.close()
    await conn.close()

    print(f"\n{'='*70}")
    print(f"  Done!")
    print(f"  Total processed:    {processed}")
    print(f"  Fully analyzed:     {fully_analyzed}")
    print(f"  Filtered out:       {filtered_out}")
    print(f"  Errors:             {errors}")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    asyncio.run(main())
