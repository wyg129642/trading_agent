#!/usr/bin/env python3
"""One-time script to sync Jiuqian JSONL data into PostgreSQL and
trigger LLM enrichment for March 15-16 data only.

Usage: python scripts/jiuqian_initial_sync.py
"""
import asyncio
import json
import sys
import os
from datetime import datetime, timezone
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from backend.app.core.database import async_session_factory, engine
from backend.app.models.jiuqian import JiuqianForum, JiuqianMinutes, JiuqianWechat, JiuqianSyncState
from sqlalchemy import select, text


DATA_DIR = Path("/home/ygwang/jiuqian-api-store/data")


def parse_dt(val):
    if not val:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(val, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def parse_date(val):
    if not val:
        return None
    try:
        return datetime.strptime(val[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


async def sync_all():
    async with async_session_factory() as db:
        # 1. Sync Forum
        print("=== Syncing Forum ===")
        filepath = DATA_DIR / "forum" / "forum_all.jsonl"
        existing = set((await db.execute(select(JiuqianForum.id))).scalars().all())
        count = 0
        for line in filepath.read_text(encoding="utf-8").strip().split("\n"):
            if not line.strip():
                continue
            data = json.loads(line)
            item_id = data.get("id")
            if item_id in existing:
                continue
            record = JiuqianForum(
                id=item_id,
                industry=data.get("industry"),
                related_targets=data.get("relatedTargets"),
                title=data.get("title", ""),
                author=data.get("author"),
                expert_information=data.get("expertInformation"),
                topic=data.get("topic"),
                summary=data.get("summary"),
                content=data.get("content", ""),
                insight=data.get("insight"),
                create_time=parse_dt(data.get("createTime")),
                meeting_time=parse_dt(data.get("meetingTime")),
                operation_time=parse_dt(data.get("operationTime")),
            )
            db.add(record)
            existing.add(item_id)
            count += 1
        await db.commit()
        print(f"Forum: {count} new records synced")

        # 2. Sync Minutes
        print("=== Syncing Minutes ===")
        filepath = DATA_DIR / "minutes" / "minutes_all.jsonl"
        existing = set()
        offset = 0
        while True:
            chunk = (await db.execute(select(JiuqianMinutes.id).offset(offset).limit(5000))).scalars().all()
            existing.update(chunk)
            if len(chunk) < 5000:
                break
            offset += 5000

        count = 0
        batch = []
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                data = json.loads(line)
                item_id = str(data.get("id", ""))
                if not item_id or item_id in existing:
                    continue

                content = data.get("content", "")
                if isinstance(content, list):
                    content = "\n\n".join(str(c) for c in content)

                record = JiuqianMinutes(
                    id=item_id,
                    platform=data.get("platform"),
                    source=data.get("source"),
                    pub_time=parse_date(data.get("pubTime")),
                    title=data.get("title", ""),
                    summary=data.get("summary"),
                    content=content,
                    author=data.get("author"),
                    company=data.get("company", []),
                )
                batch.append(record)
                existing.add(item_id)
                count += 1

                if len(batch) >= 500:
                    db.add_all(batch)
                    await db.commit()
                    print(f"  Minutes batch: {count} total")
                    batch = []

        if batch:
            db.add_all(batch)
            await db.commit()
        print(f"Minutes: {count} new records synced")

        # 3. Sync WeChat
        print("=== Syncing WeChat ===")
        filepath = DATA_DIR / "wechat" / "wechat_all.jsonl"
        existing = set()
        offset = 0
        while True:
            chunk = (await db.execute(select(JiuqianWechat.id).offset(offset).limit(5000))).scalars().all()
            existing.update(chunk)
            if len(chunk) < 5000:
                break
            offset += 5000

        count = 0
        batch = []
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                data = json.loads(line)
                item_id = str(data.get("id", ""))
                if not item_id or item_id in existing:
                    continue

                content = data.get("content", "")
                if isinstance(content, list):
                    content = "\n\n".join(str(c) for c in content)

                record = JiuqianWechat(
                    id=item_id,
                    platform=data.get("platform"),
                    source=data.get("source"),
                    district=data.get("district"),
                    pub_time=parse_date(data.get("pubTime")),
                    title=data.get("title", ""),
                    summary=data.get("summary"),
                    content=content,
                    post_url=data.get("postUrl", ""),
                )
                batch.append(record)
                existing.add(item_id)
                count += 1

                if len(batch) >= 500:
                    db.add_all(batch)
                    await db.commit()
                    print(f"  WeChat batch: {count} total")
                    batch = []

        if batch:
            db.add_all(batch)
            await db.commit()
        print(f"WeChat: {count} new records synced")

        # Summary
        forum_total = await db.scalar(select(text("count(*)")).select_from(JiuqianForum.__table__))
        minutes_total = await db.scalar(select(text("count(*)")).select_from(JiuqianMinutes.__table__))
        wechat_total = await db.scalar(select(text("count(*)")).select_from(JiuqianWechat.__table__))
        print(f"\n=== Total in DB ===")
        print(f"Forum: {forum_total}")
        print(f"Minutes: {minutes_total}")
        print(f"WeChat: {wechat_total}")

    await engine.dispose()


if __name__ == "__main__":
    asyncio.run(sync_all())
