"""FastAPI middleware configuration."""
import logging
import time

from starlette.types import ASGIApp, Receive, Scope, Send

logger = logging.getLogger(__name__)


class RequestLoggingMiddleware:
    """Pure ASGI middleware for request logging.

    Unlike BaseHTTPMiddleware, this does NOT buffer response bodies,
    so StreamingResponse (SSE) works correctly.
    """

    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        start = time.perf_counter()
        status_code = 0

        async def send_wrapper(message):
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
            await send(message)

        await self.app(scope, receive, send_wrapper)

        elapsed = (time.perf_counter() - start) * 1000
        method = scope.get("method", "")
        path = scope.get("path", "")
        logger.info("%s %s %d %.1fms", method, path, status_code, elapsed)
