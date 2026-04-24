#!/usr/bin/env python3
"""One-time script to trigger LLM enrichment for recent Jiuqian data (March 15-16).

This processes only:
- Forum items with meeting_time >= 2026-03-15
- Minutes items with pub_time >= 2026-03-15
- WeChat items with pub_time >= 2026-03-15

Usage: python scripts/jiuqian_enrich_recent.py
"""
import asyncio
import sys
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from backend.app.config import get_settings
from backend.app.core.database import async_session_factory, engine
from backend.app.models.jiuqian import JiuqianForum, JiuqianMinutes, JiuqianWechat
from sqlalchemy import select, func


async def main():
    settings = get_settings()

    if not settings.llm_enrichment_api_key:
        print("ERROR: llm_enrichment_api_key not configured. Cannot run enrichment.")
        sys.exit(1)

    from backend.app.services.jiuqian_processor import JiuqianProcessor
    processor = JiuqianProcessor(settings)

    # Check what needs processing
    cutoff = datetime(2026, 3, 15, 0, 0, 0, tzinfo=timezone.utc)

    async with async_session_factory() as db:
        forum_count = await db.scalar(
            select(func.count()).select_from(JiuqianForum)
            .where(JiuqianForum.meeting_time >= cutoff)
            .where(JiuqianForum.is_enriched == False)
        ) or 0

        minutes_count = await db.scalar(
            select(func.count()).select_from(JiuqianMinutes)
            .where(JiuqianMinutes.pub_time >= cutoff)
            .where(JiuqianMinutes.is_enriched == False)
        ) or 0

        wechat_count = await db.scalar(
            select(func.count()).select_from(JiuqianWechat)
            .where(JiuqianWechat.pub_time >= cutoff)
            .where(JiuqianWechat.is_enriched == False)
        ) or 0

        print(f"Items to process (pub_time >= 2026-03-15):")
        print(f"  Forum: {forum_count}")
        print(f"  Minutes: {minutes_count}")
        print(f"  WeChat: {wechat_count}")
        print(f"  Total: {forum_count + minutes_count + wechat_count}")

    if forum_count + minutes_count + wechat_count == 0:
        print("Nothing to process!")
        await engine.dispose()
        return

    print("\nStarting enrichment...")

    # Process a few batches
    for cycle in range(5):  # Up to 5 cycles of 60 items each
        print(f"\n--- Cycle {cycle + 1} ---")
        async with async_session_factory() as db:
            remaining = 60
            remaining = await processor._enrich_forum(db, remaining)
            remaining = await processor._enrich_minutes(db, remaining)
            remaining = await processor._enrich_wechat(db, remaining)

        if remaining == 60:
            print("No more items to process within date range.")
            break

        await asyncio.sleep(2)  # Brief pause between cycles

    # Summary
    async with async_session_factory() as db:
        forum_enriched = await db.scalar(
            select(func.count()).select_from(JiuqianForum)
            .where(JiuqianForum.meeting_time >= cutoff)
            .where(JiuqianForum.is_enriched == True)
        ) or 0
        minutes_enriched = await db.scalar(
            select(func.count()).select_from(JiuqianMinutes)
            .where(JiuqianMinutes.pub_time >= cutoff)
            .where(JiuqianMinutes.is_enriched == True)
        ) or 0
        wechat_enriched = await db.scalar(
            select(func.count()).select_from(JiuqianWechat)
            .where(JiuqianWechat.pub_time >= cutoff)
            .where(JiuqianWechat.is_enriched == True)
        ) or 0
        print(f"\n=== Enrichment Summary ===")
        print(f"Forum: {forum_enriched} enriched")
        print(f"Minutes: {minutes_enriched} enriched")
        print(f"WeChat: {wechat_enriched} enriched")

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
