"""Daily scheduler: regenerate personalized AI-chat quick-start questions.

Each morning at 08:00 CST we walk every user who has been chat-active in the
last 30 days and call ``recommendation_service.generate_for_user`` on their
behalf. This keeps the first chat-page load latency-free for the typical user
— when they land, the cache is already warm.

Falls back gracefully: if regen fails for a user we log and move on; if the
LLM is down entirely, each user's existing questions remain served from cache.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone, timedelta

from sqlalchemy import select, func

from backend.app.core.database import async_session_factory
from backend.app.models.chat import ChatConversation

logger = logging.getLogger(__name__)


class RecommendationScheduler:
    """Periodic background job for chat-quick-start recommendations."""

    def __init__(self, settings):
        self.settings = settings
        self._task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()
        # Check every 30 minutes — the run-window guard keeps us to once/day.
        self._check_interval = 30 * 60
        self._last_run_date: str | None = None

    async def start(self):
        self._task = asyncio.create_task(self._loop(), name="recommendation-scheduler")
        logger.info(
            "RecommendationScheduler started (check interval: %ds)",
            self._check_interval,
        )

    async def stop(self):
        self._stop_event.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        logger.info("RecommendationScheduler stopped")

    async def _loop(self):
        while not self._stop_event.is_set():
            try:
                await self._check_and_run()
            except Exception:
                logger.exception("RecommendationScheduler error in check cycle")
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self._check_interval
                )
                break
            except asyncio.TimeoutError:
                pass

    async def _check_and_run(self):
        """Run once per day when CST hour is in [8, 9]."""
        now_utc = datetime.now(timezone.utc)
        now_cst = now_utc + timedelta(hours=8)
        today_str = now_cst.strftime("%Y-%m-%d")
        if self._last_run_date == today_str:
            return
        if 8 <= now_cst.hour <= 9:
            logger.info(
                "RecommendationScheduler: running daily refresh (CST hour=%d)",
                now_cst.hour,
            )
            await self._run_refresh()
            self._last_run_date = today_str

    async def _run_refresh(self):
        """Generate recommendations for every user with recent chat activity."""
        from backend.app.services.recommendation_service import generate_for_user

        cutoff = datetime.now(timezone.utc) - timedelta(days=30)
        async with async_session_factory() as session:
            user_ids = (
                await session.execute(
                    select(ChatConversation.user_id)
                    .where(ChatConversation.updated_at >= cutoff)
                    .group_by(ChatConversation.user_id)
                )
            ).scalars().all()

        if not user_ids:
            logger.info("RecommendationScheduler: no active chat users, skipping")
            return

        ok = 0
        fail = 0
        for uid in user_ids:
            async with async_session_factory() as session:
                try:
                    await generate_for_user(session, uid, force=True)
                    ok += 1
                except Exception:
                    logger.exception(
                        "RecommendationScheduler: failed for user=%s", uid
                    )
                    fail += 1
                    try:
                        await session.rollback()
                    except Exception:
                        pass
            # Gentle pacing so we don't spike LLM quota on large tenants.
            await asyncio.sleep(0.5)

        logger.info(
            "RecommendationScheduler: refresh complete (ok=%d, fail=%d, users=%d)",
            ok,
            fail,
            len(user_ids),
        )
