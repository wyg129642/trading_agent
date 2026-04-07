"""Background sync service — polls AlphaPai APIs every N seconds and upserts into PostgreSQL."""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from dateutil import parser as dtparser
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.config import Settings
from backend.app.core.database import async_session_factory
from backend.app.models.alphapai import (
    AlphaPaiArticle,
    AlphaPaiComment,
    AlphaPaiRoadshowCN,
    AlphaPaiRoadshowUS,
    AlphaPaiSyncState,
)
from backend.app.services.alphapai_client import AlphaPaiClient

logger = logging.getLogger(__name__)


def _parse_dt(val: Any) -> datetime | None:
    """Safely parse a datetime string from AlphaPai; return None on failure."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    try:
        return dtparser.parse(str(val))
    except Exception:
        return None


class AlphaPaiSyncService:
    """Background task that periodically fetches new data from AlphaPai."""

    def __init__(self, client: AlphaPaiClient, settings: Settings):
        self.client = client
        self.settings = settings
        self._running = False
        self._task: asyncio.Task | None = None

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #
    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="alphapai_sync")
        logger.info(
            "[AlphaPai-Sync] started (interval=%ds)",
            self.settings.alphapai_sync_interval_seconds,
        )

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        await self.client.close()
        logger.info("[AlphaPai-Sync] stopped")

    # ------------------------------------------------------------------ #
    # Main loop
    # ------------------------------------------------------------------ #
    async def _loop(self) -> None:
        # Small initial delay to let the app finish startup
        await asyncio.sleep(5)
        while self._running:
            try:
                await self._sync_cycle()
            except Exception:
                logger.exception("[AlphaPai-Sync] cycle error")
            # Sleep in 1-s increments for graceful shutdown
            for _ in range(self.settings.alphapai_sync_interval_seconds):
                if not self._running:
                    return
                await asyncio.sleep(1)

    async def _sync_cycle(self) -> None:
        async with async_session_factory() as db:
            await self._sync_wechat(db)
            await self._sync_roadshows_cn(db)
            await self._sync_roadshows_us(db)
            await self._sync_comments(db)

    # ------------------------------------------------------------------ #
    # Sync-state helper
    # ------------------------------------------------------------------ #
    async def _get_state(self, db: AsyncSession, name: str) -> AlphaPaiSyncState:
        state = await db.scalar(
            select(AlphaPaiSyncState).where(AlphaPaiSyncState.api_name == name)
        )
        if state is None:
            state = AlphaPaiSyncState(
                api_name=name,
                last_sync_time=datetime.now(timezone.utc) - timedelta(hours=36),
            )
            try:
                db.add(state)
                await db.commit()
                await db.refresh(state)
            except Exception:
                await db.rollback()
                # Another task may have inserted concurrently — re-fetch
                state = await db.scalar(
                    select(AlphaPaiSyncState).where(AlphaPaiSyncState.api_name == name)
                )
                if state is None:
                    raise
        return state

    async def _update_state(
        self, db: AsyncSession, state: AlphaPaiSyncState, new_count: int, error: str | None = None,
    ) -> None:
        state.last_sync_time = datetime.now(timezone.utc)
        state.last_sync_count = new_count
        state.total_synced += new_count
        state.last_error = error
        state.updated_at = datetime.now(timezone.utc)
        await db.commit()

    # ------------------------------------------------------------------ #
    # WeChat articles
    # ------------------------------------------------------------------ #
    async def _sync_wechat(self, db: AsyncSession) -> None:
        state = await self._get_state(db, "wechat_articles")
        start = (state.last_sync_time - timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")
        end = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        try:
            items = await self.client.fetch_wechat_articles(
                start, end, self.settings.alphapai_batch_size,
            )
        except Exception as exc:
            logger.error("[AlphaPai-Sync] wechat fetch error: %s", exc)
            await self._update_state(db, state, 0, str(exc))
            return

        if not items:
            await self._update_state(db, state, 0)
            return

        # Batch existence check
        incoming_pks = [it["arc_code"] for it in items if it.get("arc_code")]
        existing = set()
        for chunk_start in range(0, len(incoming_pks), 500):
            chunk = incoming_pks[chunk_start : chunk_start + 500]
            rows = await db.execute(
                select(AlphaPaiArticle.arc_code).where(AlphaPaiArticle.arc_code.in_(chunk))
            )
            existing.update(r[0] for r in rows)

        new_objs = []
        for it in items:
            pk = it.get("arc_code")
            if not pk or pk in existing:
                continue
            new_objs.append(AlphaPaiArticle(
                arc_code=pk,
                arc_name=it.get("arc_name") or "",
                author=it.get("author"),
                publish_time=_parse_dt(it.get("publish_time")),
                spider_time=_parse_dt(it.get("spider_time")),
                text_count=it.get("text_count") or 0,
                read_duration=str(it.get("read_duration") or ""),
                is_original=it.get("is_original") or 0,
                url=it.get("url") or "",
                content_html_path=it.get("content_html") or "",
                wxacc_code=it.get("wxacc_code"),
                research_type=it.get("research_type"),
            ))

        if new_objs:
            db.add_all(new_objs)
            await db.commit()

        logger.info("[AlphaPai-Sync] wechat: fetched=%d new=%d", len(items), len(new_objs))
        await self._update_state(db, state, len(new_objs))

    # ------------------------------------------------------------------ #
    # A-share roadshows
    # ------------------------------------------------------------------ #
    async def _sync_roadshows_cn(self, db: AsyncSession) -> None:
        state = await self._get_state(db, "roadshows_cn")
        start = (state.last_sync_time - timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")
        end = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        try:
            items = await self.client.fetch_roadshows_cn(
                start, end, self.settings.alphapai_batch_size,
            )
        except Exception as exc:
            logger.error("[AlphaPai-Sync] roadshows_cn fetch error: %s", exc)
            await self._update_state(db, state, 0, str(exc))
            return

        if not items:
            await self._update_state(db, state, 0)
            return

        incoming_pks = [it["trans_id"] for it in items if it.get("trans_id")]
        existing = set()
        for chunk_start in range(0, len(incoming_pks), 500):
            chunk = incoming_pks[chunk_start : chunk_start + 500]
            rows = await db.execute(
                select(AlphaPaiRoadshowCN.trans_id).where(AlphaPaiRoadshowCN.trans_id.in_(chunk))
            )
            existing.update(r[0] for r in rows)

        new_objs = []
        for it in items:
            pk = it.get("trans_id")
            if not pk or pk in existing:
                continue
            # Parse ind_json
            ind = it.get("ind_json")
            if isinstance(ind, str):
                try:
                    ind = __import__("json").loads(ind)
                except Exception:
                    ind = []
            new_objs.append(AlphaPaiRoadshowCN(
                trans_id=pk,
                roadshow_id=it.get("roadshow_id") or "",
                show_title=it.get("show_title") or "",
                company=it.get("company"),
                guest=it.get("guest"),
                stime=_parse_dt(it.get("stime")),
                word_count=it.get("word_count") or 0,
                est_reading_time=str(it.get("est_reading_time") or ""),
                ind_json=ind or [],
                trans_source=it.get("trans_source") or it.get("recorder") or "MT",
                content_path=it.get("content") or "",
                is_conference=bool(it.get("is_conference")),
                is_investigation=bool(it.get("is_investigation")),
                is_executive=bool(it.get("is_executive")),
                is_buyside=bool(it.get("is_buyside")),
            ))

        if new_objs:
            db.add_all(new_objs)
            await db.commit()

        logger.info("[AlphaPai-Sync] roadshows_cn: fetched=%d new=%d", len(items), len(new_objs))
        await self._update_state(db, state, len(new_objs))

    # ------------------------------------------------------------------ #
    # US roadshows
    # ------------------------------------------------------------------ #
    async def _sync_roadshows_us(self, db: AsyncSession) -> None:
        state = await self._get_state(db, "roadshows_us")
        start = (state.last_sync_time - timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")
        end = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        try:
            items = await self.client.fetch_roadshows_us(
                start, end, self.settings.alphapai_batch_size,
            )
        except Exception as exc:
            logger.error("[AlphaPai-Sync] roadshows_us fetch error: %s", exc)
            await self._update_state(db, state, 0, str(exc))
            return

        if not items:
            await self._update_state(db, state, 0)
            return

        incoming_pks = [it["trans_id"] for it in items if it.get("trans_id")]
        existing = set()
        for chunk_start in range(0, len(incoming_pks), 500):
            chunk = incoming_pks[chunk_start : chunk_start + 500]
            rows = await db.execute(
                select(AlphaPaiRoadshowUS.trans_id).where(AlphaPaiRoadshowUS.trans_id.in_(chunk))
            )
            existing.update(r[0] for r in rows)

        new_objs = []
        for it in items:
            pk = it.get("trans_id")
            if not pk or pk in existing:
                continue
            ind = it.get("ind_json")
            if isinstance(ind, str):
                try:
                    ind = __import__("json").loads(ind)
                except Exception:
                    ind = []
            new_objs.append(AlphaPaiRoadshowUS(
                trans_id=pk,
                roadshow_id=it.get("roadshow_id") or "",
                show_title=it.get("show_title") or "",
                company=it.get("company"),
                guest=it.get("guest"),
                stime=_parse_dt(it.get("stime")),
                word_count=it.get("word_count") or 0,
                est_reading_time=str(it.get("est_reading_time") or ""),
                ind_json=ind or [],
                trans_source=it.get("trans_source") or it.get("recorder") or "AI",
                content_path=it.get("content") or "",
                rec_source=it.get("rec_source"),
                quarter_year=it.get("quarter_year"),
                files_type=it.get("files_type"),
            ))

        if new_objs:
            db.add_all(new_objs)
            await db.commit()

        # Download AI auxiliary JSON for items that have it
        for it in items:
            pk = it.get("trans_id")
            ai_path = it.get("ai_auxiliary_json_s3")
            if pk and ai_path and pk not in existing:
                try:
                    content = await self.client.download_content(ai_path)
                    ai_data = __import__("json").loads(content)
                    row = await db.scalar(
                        select(AlphaPaiRoadshowUS).where(AlphaPaiRoadshowUS.trans_id == pk)
                    )
                    if row:
                        row.ai_auxiliary_json = ai_data
                except Exception as exc:
                    logger.debug("[AlphaPai-Sync] ai_auxiliary download failed for %s: %s", pk, exc)

        await db.commit()
        logger.info("[AlphaPai-Sync] roadshows_us: fetched=%d new=%d", len(items), len(new_objs))
        await self._update_state(db, state, len(new_objs))

    # ------------------------------------------------------------------ #
    # Analyst comments
    # ------------------------------------------------------------------ #
    async def _sync_comments(self, db: AsyncSession) -> None:
        state = await self._get_state(db, "comments")
        start = (state.last_sync_time - timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")
        end = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        try:
            items = await self.client.fetch_comments(
                start, end, self.settings.alphapai_batch_size,
            )
        except Exception as exc:
            logger.error("[AlphaPai-Sync] comments fetch error: %s", exc)
            await self._update_state(db, state, 0, str(exc))
            return

        if not items:
            await self._update_state(db, state, 0)
            return

        incoming_pks = [str(it.get("cmnt_hcode") or it.get("id", "")) for it in items]
        incoming_pks = [pk for pk in incoming_pks if pk]
        existing = set()
        for chunk_start in range(0, len(incoming_pks), 500):
            chunk = incoming_pks[chunk_start : chunk_start + 500]
            rows = await db.execute(
                select(AlphaPaiComment.cmnt_hcode).where(AlphaPaiComment.cmnt_hcode.in_(chunk))
            )
            existing.update(r[0] for r in rows)

        new_objs = []
        for it in items:
            pk = str(it.get("cmnt_hcode") or it.get("id", ""))
            if not pk or pk in existing:
                continue
            new_objs.append(AlphaPaiComment(
                cmnt_hcode=pk,
                title=it.get("title") or "",
                content=it.get("content") or "",
                psn_name=it.get("psn_name"),
                team_cname=it.get("team_cname"),
                inst_cname=it.get("inst_cname"),
                cmnt_date=_parse_dt(it.get("cmnt_date")),
                is_new_fortune=bool(it.get("is_new_fortune")),
                src_type=it.get("src_type"),
                group_id=it.get("group_id"),
            ))

        if new_objs:
            db.add_all(new_objs)
            await db.commit()

        logger.info("[AlphaPai-Sync] comments: fetched=%d new=%d", len(items), len(new_objs))
        await self._update_state(db, state, len(new_objs))

    # ------------------------------------------------------------------ #
    # Manual trigger (called from admin API)
    # ------------------------------------------------------------------ #
    async def trigger_sync(self) -> dict:
        """Run one sync cycle immediately. Returns summary."""
        try:
            await self._sync_cycle()
            return {"status": "ok"}
        except Exception as exc:
            logger.exception("[AlphaPai-Sync] manual trigger error")
            return {"status": "error", "detail": str(exc)}
