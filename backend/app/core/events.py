"""Redis pub/sub event bus for real-time news distribution."""

from __future__ import annotations

import json
import logging
from typing import Any

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)

CHANNEL_NEWS = "news:analyzed"
CHANNEL_ALERT = "news:alert"


class EventBus:
    """Redis-based event bus for distributing news events to WebSocket clients."""

    def __init__(self, redis: aioredis.Redis):
        self.redis = redis

    async def publish_news(self, event: dict[str, Any]) -> None:
        """Publish a new analyzed news event."""
        try:
            await self.redis.publish(CHANNEL_NEWS, json.dumps(event, ensure_ascii=False, default=str))
        except Exception as e:
            logger.error("Failed to publish news event: %s", e)

    async def subscribe_news(self):
        """Subscribe to the news channel. Returns an async iterator of messages."""
        pubsub = self.redis.pubsub()
        await pubsub.subscribe(CHANNEL_NEWS)
        return pubsub

    async def publish_alert(self, event: dict[str, Any]) -> None:
        """Publish an alert event."""
        try:
            await self.redis.publish(CHANNEL_ALERT, json.dumps(event, ensure_ascii=False, default=str))
        except Exception as e:
            logger.error("Failed to publish alert event: %s", e)
