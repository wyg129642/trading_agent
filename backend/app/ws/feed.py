"""WebSocket endpoint for live news feed push."""

from __future__ import annotations

import asyncio
import json
import logging

from fastapi import APIRouter, Depends, WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState

from backend.app.config import get_settings
from backend.app.core.events import CHANNEL_NEWS, CHANNEL_ALERT

logger = logging.getLogger(__name__)

router = APIRouter()


class ConnectionManager:
    """Manages active WebSocket connections per user."""

    def __init__(self):
        self.active: dict[str, list[WebSocket]] = {}  # user_id → [ws1, ws2, ...]

    async def connect(self, ws: WebSocket, user_id: str) -> None:
        await ws.accept()
        self.active.setdefault(user_id, []).append(ws)
        logger.info("WebSocket connected: user=%s (total=%d)", user_id, self._total())

    def disconnect(self, ws: WebSocket, user_id: str) -> None:
        conns = self.active.get(user_id, [])
        if ws in conns:
            conns.remove(ws)
        if not conns:
            self.active.pop(user_id, None)
        logger.info("WebSocket disconnected: user=%s (total=%d)", user_id, self._total())

    async def broadcast(self, message: str) -> None:
        """Broadcast to all connected clients."""
        for user_id, connections in list(self.active.items()):
            for ws in list(connections):
                try:
                    if ws.client_state == WebSocketState.CONNECTED:
                        await ws.send_text(message)
                except Exception:
                    self.disconnect(ws, user_id)

    async def send_to_user(self, user_id: str, message: str) -> None:
        """Send to a specific user's connections."""
        for ws in list(self.active.get(user_id, [])):
            try:
                if ws.client_state == WebSocketState.CONNECTED:
                    await ws.send_text(message)
            except Exception:
                self.disconnect(ws, user_id)

    def _total(self) -> int:
        return sum(len(v) for v in self.active.values())


manager = ConnectionManager()


@router.websocket("/ws/feed")
async def ws_feed(ws: WebSocket):
    """Live news feed WebSocket endpoint.

    Client sends JWT token as first message for authentication.
    Then receives real-time news events from Redis pub/sub.
    """
    # Authenticate via query param or first message
    token = ws.query_params.get("token")

    if not token:
        await ws.accept()
        try:
            msg = await asyncio.wait_for(ws.receive_text(), timeout=10.0)
            data = json.loads(msg)
            token = data.get("token")
        except Exception:
            await ws.close(code=4001, reason="Authentication required")
            return

    # Validate JWT
    from backend.app.core.security import decode_access_token
    settings = get_settings()
    payload = decode_access_token(token, settings)
    if not payload:
        if ws.client_state != WebSocketState.CONNECTED:
            await ws.accept()
        await ws.close(code=4001, reason="Invalid token")
        return

    user_id = payload["sub"]

    if ws.client_state != WebSocketState.CONNECTED:
        await manager.connect(ws, user_id)
    else:
        manager.active.setdefault(user_id, []).append(ws)

    # Subscribe to Redis and forward events
    import redis.asyncio as aioredis
    redis_conn = None
    pubsub = None
    try:
        redis_conn = aioredis.from_url(settings.redis_url, decode_responses=True)
        pubsub = redis_conn.pubsub()
        await pubsub.subscribe(CHANNEL_NEWS, CHANNEL_ALERT)

        # Run two tasks: listen to Redis + listen to client pings
        async def _redis_listener():
            async for message in pubsub.listen():
                if message["type"] == "message":
                    try:
                        channel = message.get("channel", "")
                        if channel == CHANNEL_ALERT:
                            # Alert messages are per-user — only send to target
                            data = json.loads(message["data"])
                            if data.get("user_id") == user_id:
                                await ws.send_text(message["data"])
                        else:
                            await ws.send_text(message["data"])
                    except Exception:
                        break

        async def _client_listener():
            try:
                while True:
                    data = await ws.receive_text()
                    # Handle client messages (e.g., ping, filter updates)
                    if data == "ping":
                        await ws.send_text("pong")
            except WebSocketDisconnect:
                pass

        await asyncio.gather(
            _redis_listener(),
            _client_listener(),
            return_exceptions=True,
        )
    finally:
        manager.disconnect(ws, user_id)
        if pubsub:
            try:
                await pubsub.unsubscribe(CHANNEL_NEWS, CHANNEL_ALERT)
                await pubsub.close()
            except Exception:
                logger.debug("Error closing Redis pubsub")
        if redis_conn:
            try:
                await redis_conn.close()
            except Exception:
                logger.debug("Error closing Redis connection")
