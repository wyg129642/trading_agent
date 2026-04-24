"""Background service: periodic news monitoring against user tracking topics."""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone

import httpx
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.config import Settings

logger = logging.getLogger(__name__)

# Check interval: 5 minutes
CHECK_INTERVAL_S = 300
MATCH_THRESHOLD = 0.3


class TrackingAlertService:
    """Periodically scans new analyzed news against user tracking topics."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self._running = False
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="tracking_alert")
        logger.info("[TrackingAlert] Started (interval=%ds, threshold=%.1f)", CHECK_INTERVAL_S, MATCH_THRESHOLD)

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("[TrackingAlert] Stopped")

    async def _loop(self) -> None:
        await asyncio.sleep(30)  # initial delay
        while self._running:
            try:
                await self._check_all_topics()
            except Exception:
                logger.exception("[TrackingAlert] Error in check cycle")
            # Sleep with responsive shutdown check
            for _ in range(CHECK_INTERVAL_S):
                if not self._running:
                    return
                await asyncio.sleep(1)

    async def _check_all_topics(self) -> None:
        from backend.app.core.database import async_session_factory
        from backend.app.models.chat import ChatTrackingTopic, ChatTrackingAlert
        from backend.app.models.news import NewsItem, AnalysisResult

        async with async_session_factory() as session:
            # Get all active tracking topics
            result = await session.execute(
                select(ChatTrackingTopic).where(ChatTrackingTopic.is_active == True)
            )
            topics = result.scalars().all()

            if not topics:
                return

            now = datetime.now(timezone.utc)

            for topic in topics:
                try:
                    await self._check_topic(session, topic, now)
                except Exception:
                    logger.exception("[TrackingAlert] Error checking topic %s", topic.id)

            await session.commit()

    async def _check_topic(
        self,
        session: AsyncSession,
        topic,
        now: datetime,
    ) -> None:
        from backend.app.models.chat import ChatTrackingAlert
        from backend.app.models.news import NewsItem, AnalysisResult
        from sqlalchemy import func

        # Query new analyzed news since last check
        since = topic.last_checked_at or now.replace(hour=0, minute=0, second=0)

        result = await session.execute(
            select(AnalysisResult, NewsItem)
            .join(NewsItem, AnalysisResult.news_item_id == NewsItem.id)
            .where(AnalysisResult.analyzed_at > since)
            .order_by(AnalysisResult.analyzed_at.desc())
            .limit(100)
        )
        rows = result.all()

        matched_count = 0
        for analysis, news in rows:
            score, reasons = self._compute_match(topic, analysis, news)

            if score >= MATCH_THRESHOLD:
                # Check for duplicate
                existing = await session.execute(
                    select(func.count(ChatTrackingAlert.id))
                    .where(
                        ChatTrackingAlert.topic_id == topic.id,
                        ChatTrackingAlert.news_item_id == news.id,
                    )
                )
                if existing.scalar() > 0:
                    continue

                # Create alert
                alert = ChatTrackingAlert(
                    topic_id=topic.id,
                    news_item_id=news.id,
                    match_score=score,
                    match_reason="; ".join(reasons),
                )
                session.add(alert)
                matched_count += 1

                # Publish to Redis for WebSocket push
                if "browser" in (topic.notify_channels or []):
                    await self._publish_alert(topic, news, score)

                # Feishu notification
                if "feishu" in (topic.notify_channels or []):
                    await self._send_feishu(topic, news, score, reasons)

        if matched_count > 0:
            topic.last_triggered_at = now
            logger.info(
                "[TrackingAlert] Topic '%s' matched %d news items",
                topic.topic[:30], matched_count,
            )

        topic.last_checked_at = now

    def _compute_match(self, topic, analysis, news) -> tuple[float, list[str]]:
        """Compute match score between a tracking topic and a news item."""
        score = 0.0
        reasons = []

        title_content = (news.title or "").lower() + " " + (news.content or "")[:500].lower()

        # Keyword matching
        for kw in (topic.keywords or []):
            if kw.lower() in title_content:
                score += 0.3
                reasons.append(f"关键词匹配: {kw}")

        # Ticker overlap
        news_tickers = set(analysis.affected_tickers or [])
        for ticker in (topic.related_tickers or []):
            if ticker in news_tickers:
                score += 0.4
                reasons.append(f"股票匹配: {ticker}")

        # Sector overlap
        news_sectors = set(analysis.affected_sectors or [])
        for sector in (topic.related_sectors or []):
            if sector in news_sectors:
                score += 0.2
                reasons.append(f"板块匹配: {sector}")

        return min(score, 1.0), reasons

    async def _publish_alert(self, topic, news, score: float) -> None:
        """Publish alert to Redis for WebSocket delivery."""
        try:
            import redis.asyncio as aioredis
            from backend.app.core.events import CHANNEL_ALERT

            redis_conn = aioredis.from_url(self.settings.redis_url, decode_responses=True)
            payload = json.dumps({
                "type": "tracking_alert",
                "user_id": str(topic.user_id),
                "topic_id": str(topic.id),
                "topic": topic.topic,
                "news_title": news.title,
                "match_score": score,
            }, ensure_ascii=False)
            await redis_conn.publish(CHANNEL_ALERT, payload)
            await redis_conn.close()
        except Exception:
            logger.warning("[TrackingAlert] Failed to publish Redis alert")

    async def _send_feishu(self, topic, news, score: float, reasons: list[str]) -> None:
        """Send Feishu webhook notification."""
        webhook_url = getattr(self.settings, "feishu_webhook_url", None)
        if not webhook_url:
            return
        try:
            card = {
                "msg_type": "interactive",
                "card": {
                    "header": {
                        "title": {"tag": "plain_text", "content": f"📡 跟踪提醒: {topic.topic[:40]}"},
                        "template": "orange",
                    },
                    "elements": [
                        {"tag": "div", "text": {"tag": "lark_md", "content": f"**{news.title}**"}},
                        {"tag": "div", "text": {"tag": "lark_md", "content": f"匹配度: {score:.0%} | {'; '.join(reasons[:3])}"}},
                    ],
                },
            }
            async with httpx.AsyncClient(timeout=10.0, trust_env=False) as client:
                await client.post(webhook_url, json=card)
        except Exception:
            logger.warning("[TrackingAlert] Failed to send Feishu alert")
