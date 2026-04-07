"""Sync service for Jiuqian data — reads JSONL files and inserts into PostgreSQL.

Runs every hour, reads the *_all.jsonl files, deduplicates by ID against what's already
in the database, and inserts new records.
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.config import Settings
from backend.app.core.database import async_session_factory
from backend.app.models.jiuqian import (
    JiuqianForum,
    JiuqianMinutes,
    JiuqianSyncState,
    JiuqianWechat,
)

logger = logging.getLogger(__name__)

# Default path to jiuqian data directory
_DEFAULT_DATA_DIR = Path("/home/ygwang/jiuqian-api-store/data")


class JiuqianSyncService:
    """Background service that syncs JSONL files into the database."""

    def __init__(self, settings: Settings, data_dir: Path | None = None):
        self.settings = settings
        self.data_dir = data_dir or _DEFAULT_DATA_DIR
        self._running = False
        self._task: asyncio.Task | None = None
        self.interval = getattr(settings, "jiuqian_sync_interval_seconds", 3600)

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="jiuqian_sync")
        logger.info("[Jiuqian-Sync] started (dir=%s, interval=%ds)", self.data_dir, self.interval)

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("[Jiuqian-Sync] stopped")

    async def _loop(self) -> None:
        await asyncio.sleep(5)  # Brief delay on startup
        while self._running:
            try:
                await self._sync_all()
            except Exception:
                logger.exception("[Jiuqian-Sync] error in sync cycle")

            for _ in range(self.interval):
                if not self._running:
                    return
                await asyncio.sleep(1)

    async def _sync_all(self) -> None:
        async with async_session_factory() as db:
            await self._sync_forum(db)
            await self._sync_minutes(db)
            await self._sync_wechat(db)

    # ------------------------------------------------------------------ #
    # Forum
    # ------------------------------------------------------------------ #
    async def _sync_forum(self, db: AsyncSession) -> None:
        filepath = self.data_dir / "forum" / "forum_all.jsonl"
        if not filepath.exists():
            return

        existing_ids = set(
            (await db.execute(select(JiuqianForum.id))).scalars().all()
        )

        new_records = []
        for line in filepath.read_text(encoding="utf-8").strip().split("\n"):
            if not line.strip():
                continue
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                continue

            item_id = data.get("id")
            if item_id is None or item_id in existing_ids:
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
                create_time=_parse_dt(data.get("createTime")),
                meeting_time=_parse_dt(data.get("meetingTime")),
                operation_time=_parse_dt(data.get("operationTime")),
            )
            new_records.append(record)
            existing_ids.add(item_id)

        if new_records:
            db.add_all(new_records)
            await db.commit()
            logger.info("[Jiuqian-Sync] forum: +%d new records", len(new_records))
        await self._update_state(db, "forum", len(new_records))

    # ------------------------------------------------------------------ #
    # Minutes
    # ------------------------------------------------------------------ #
    async def _sync_minutes(self, db: AsyncSession) -> None:
        filepath = self.data_dir / "minutes" / "minutes_all.jsonl"
        if not filepath.exists():
            return

        # Load existing IDs (batch check)
        existing_ids = set()
        chunk_size = 5000
        offset = 0
        while True:
            chunk = (await db.execute(
                select(JiuqianMinutes.id).offset(offset).limit(chunk_size)
            )).scalars().all()
            existing_ids.update(chunk)
            if len(chunk) < chunk_size:
                break
            offset += chunk_size

        new_records = []
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    data = json.loads(line)
                except json.JSONDecodeError:
                    continue

                item_id = str(data.get("id", ""))
                if not item_id or item_id in existing_ids:
                    continue

                # Parse content - it may be a JSON array string
                content = data.get("content", "")
                if isinstance(content, list):
                    content = "\n\n".join(str(c) for c in content)

                record = JiuqianMinutes(
                    id=item_id,
                    platform=data.get("platform"),
                    source=data.get("source"),
                    pub_time=_parse_date(data.get("pubTime")),
                    title=data.get("title", ""),
                    summary=data.get("summary"),
                    content=content,
                    author=data.get("author"),
                    company=data.get("company", []),
                )
                new_records.append(record)
                existing_ids.add(item_id)

                # Batch insert every 500 to avoid memory issues
                if len(new_records) >= 500:
                    db.add_all(new_records)
                    await db.commit()
                    logger.info("[Jiuqian-Sync] minutes: batch +%d", len(new_records))
                    new_records = []

        if new_records:
            db.add_all(new_records)
            await db.commit()
            logger.info("[Jiuqian-Sync] minutes: +%d new records", len(new_records))
        await self._update_state(db, "minutes", len(new_records))

    # ------------------------------------------------------------------ #
    # WeChat
    # ------------------------------------------------------------------ #
    async def _sync_wechat(self, db: AsyncSession) -> None:
        filepath = self.data_dir / "wechat" / "wechat_all.jsonl"
        if not filepath.exists():
            return

        existing_ids = set()
        chunk_size = 5000
        offset = 0
        while True:
            chunk = (await db.execute(
                select(JiuqianWechat.id).offset(offset).limit(chunk_size)
            )).scalars().all()
            existing_ids.update(chunk)
            if len(chunk) < chunk_size:
                break
            offset += chunk_size

        new_records = []
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    data = json.loads(line)
                except json.JSONDecodeError:
                    continue

                item_id = str(data.get("id", ""))
                if not item_id or item_id in existing_ids:
                    continue

                content = data.get("content", "")
                if isinstance(content, list):
                    content = "\n\n".join(str(c) for c in content)

                record = JiuqianWechat(
                    id=item_id,
                    platform=data.get("platform"),
                    source=data.get("source"),
                    district=data.get("district"),
                    pub_time=_parse_date(data.get("pubTime")),
                    title=data.get("title", ""),
                    summary=data.get("summary"),
                    content=content,
                    post_url=data.get("postUrl", ""),
                )
                new_records.append(record)
                existing_ids.add(item_id)

                if len(new_records) >= 500:
                    db.add_all(new_records)
                    await db.commit()
                    logger.info("[Jiuqian-Sync] wechat: batch +%d", len(new_records))
                    new_records = []

        if new_records:
            db.add_all(new_records)
            await db.commit()
            logger.info("[Jiuqian-Sync] wechat: +%d new records", len(new_records))
        await self._update_state(db, "wechat", len(new_records))

    # ------------------------------------------------------------------ #
    # State tracking
    # ------------------------------------------------------------------ #
    async def _update_state(self, db: AsyncSession, source: str, new_count: int) -> None:
        state = await db.scalar(
            select(JiuqianSyncState).where(JiuqianSyncState.source_name == source)
        )
        if state is None:
            state = JiuqianSyncState(source_name=source)
            db.add(state)
        state.last_sync_time = datetime.now(timezone.utc)
        state.total_synced = (state.total_synced or 0) + new_count
        state.last_processed_ids = new_count
        state.last_error = None
        await db.commit()


def _parse_dt(val: str | None) -> datetime | None:
    """Parse datetime string like '2026-03-16 10:00:00'."""
    if not val:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(val, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _parse_date(val: str | None) -> datetime | None:
    """Parse date string like '2026-03-15'."""
    if not val:
        return None
    try:
        dt = datetime.strptime(val[:10], "%Y-%m-%d")
        return dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None
