"""FastAPI application factory for the Trading Agent web platform."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from starlette.responses import FileResponse

from backend.app.config import get_settings
from backend.app.core.middleware import RequestLoggingMiddleware

# Configure root logger so application logs appear in uvicorn output
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s:     %(message)s",
)
logger = logging.getLogger(__name__)

# Path to built frontend
_FRONTEND_DIST = Path(__file__).resolve().parent.parent.parent / "frontend" / "dist"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    settings = get_settings()
    logger.info("Trading Agent API starting (env=%s)", settings.app_env)

    # Initialize Redis connection pool
    import redis.asyncio as aioredis
    app.state.redis = aioredis.from_url(settings.redis_url, decode_responses=True)
    logger.info("Redis connected at %s:%d", settings.redis_host, settings.redis_port)

    # Start Engine Manager (auto-starts the trading engine subprocess)
    from backend.app.services.engine_manager import EngineManager
    engine_mgr = EngineManager(settings)
    app.state.engine_manager = engine_mgr
    await engine_mgr.start()
    logger.info("Engine manager initialized (auto-start enabled)")

    # Start AlphaPai sync + enrichment services (if configured)
    alphapai_sync = None
    alphapai_processor = None
    if settings.alphapai_sync_enabled and settings.alphapai_app_agent:
        from backend.app.services.alphapai_client import AlphaPaiClient
        from backend.app.services.alphapai_sync import AlphaPaiSyncService

        client = AlphaPaiClient(settings.alphapai_base_url, settings.alphapai_app_agent)
        alphapai_sync = AlphaPaiSyncService(client, settings)
        await alphapai_sync.start()
        app.state.alphapai_sync = alphapai_sync
        logger.info("AlphaPai sync service started")

        # Start LLM enrichment if MiniMax key is configured
        if settings.minimax_api_key:
            from backend.app.services.alphapai_processor import AlphaPaiProcessor

            alphapai_processor = AlphaPaiProcessor(settings)
            await alphapai_processor.start(client)
            app.state.alphapai_processor = alphapai_processor
            logger.info("AlphaPai LLM enrichment started (model=%s)", settings.minimax_model)

    # Start Jiuqian sync + enrichment services
    jiuqian_sync = None
    jiuqian_processor = None
    try:
        from backend.app.services.jiuqian_sync import JiuqianSyncService

        jiuqian_sync = JiuqianSyncService(settings)
        await jiuqian_sync.start()
        app.state.jiuqian_sync = jiuqian_sync
        logger.info("Jiuqian sync service started")

        if settings.minimax_api_key:
            from backend.app.services.jiuqian_processor import JiuqianProcessor

            jiuqian_processor = JiuqianProcessor(settings)
            await jiuqian_processor.start()
            app.state.jiuqian_processor = jiuqian_processor
            logger.info("Jiuqian LLM enrichment started")
    except Exception:
        logger.exception("Failed to start Jiuqian services")

    # Start hot news LLM filter (classifies hot news titles for market relevance)
    hotnews_filter = None
    if settings.minimax_api_key:
        try:
            from backend.app.services.hotnews_filter import HotNewsFilter

            hotnews_filter = HotNewsFilter(settings)
            await hotnews_filter.start()
            app.state.hotnews_filter = hotnews_filter
            logger.info("Hot news LLM filter started")
        except Exception:
            logger.exception("Failed to start hot news filter")

    # Start topic clustering service (anomaly detection over enriched data)
    topic_cluster_svc = None
    try:
        from backend.app.services.topic_cluster import TopicClusterService

        topic_cluster_svc = TopicClusterService(settings)
        await topic_cluster_svc.start()
        app.state.topic_cluster = topic_cluster_svc
        logger.info("Topic clustering service started")
    except Exception:
        logger.exception("Failed to start topic clustering service")

    # Start daily backtest scheduler
    backtest_scheduler = None
    try:
        from backend.app.services.backtest_scheduler import BacktestScheduler
        backtest_scheduler = BacktestScheduler(settings)
        await backtest_scheduler.start()
        app.state.backtest_scheduler = backtest_scheduler
        logger.info("Backtest scheduler started")
    except Exception:
        logger.exception("Failed to start backtest scheduler")

    # Start tracking alert service (monitors news for user tracking topics)
    tracking_alert_svc = None
    try:
        from backend.app.services.tracking_alert import TrackingAlertService
        tracking_alert_svc = TrackingAlertService(settings)
        await tracking_alert_svc.start()
        app.state.tracking_alert = tracking_alert_svc
        logger.info("Tracking alert service started")
    except Exception:
        logger.exception("Failed to start tracking alert service")

    yield

    # Shutdown
    if tracking_alert_svc:
        await tracking_alert_svc.stop()
    if hotnews_filter:
        await hotnews_filter.stop()
    if topic_cluster_svc:
        await topic_cluster_svc.stop()
    if jiuqian_processor:
        await jiuqian_processor.stop()
    if jiuqian_sync:
        await jiuqian_sync.stop()
    if alphapai_processor:
        await alphapai_processor.stop()
    if alphapai_sync:
        await alphapai_sync.stop()
    if backtest_scheduler:
        await backtest_scheduler.stop()
    await engine_mgr.stop()
    await app.state.redis.close()
    logger.info("Trading Agent API shutdown complete")


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    settings = get_settings()

    app = FastAPI(
        title="Trading Agent API",
        description="Real-time trading intelligence platform for discretionary researchers",
        version="1.0.0",
        docs_url="/docs",
        lifespan=lifespan,
    )

    # CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origin_list,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Request logging
    app.add_middleware(RequestLoggingMiddleware)

    # Register API routers
    from backend.app.api.auth import router as auth_router
    from backend.app.api.news import router as news_router
    from backend.app.api.watchlist import router as watchlist_router
    from backend.app.api.sources import router as sources_router
    from backend.app.api.analytics import router as analytics_router
    from backend.app.api.admin import router as admin_router

    from backend.app.api.alphapai import router as alphapai_router
    from backend.app.api.jiuqian import router as jiuqian_router
    from backend.app.api.engine import router as engine_router
    from backend.app.api.favorites import router as favorites_router
    from backend.app.api.stock_search import router as stock_search_router
    from backend.app.api.topic_radar import router as topic_radar_router
    from backend.app.api.leaderboard import router as leaderboard_router
    from backend.app.api.signals import router as signals_router
    from backend.app.api.analyst_rating import router as analyst_rating_router
    from backend.app.api.chat import router as chat_router
    from backend.app.api.predictions import router as predictions_router
    from backend.app.api.open import router as open_router
    from backend.app.api.portfolio_news import router as portfolio_news_router

    app.include_router(auth_router, prefix="/api/auth", tags=["Auth"])
    app.include_router(news_router, prefix="/api/news", tags=["News"])
    app.include_router(watchlist_router, prefix="/api/watchlists", tags=["Watchlists"])
    app.include_router(sources_router, prefix="/api/sources", tags=["Sources"])
    app.include_router(analytics_router, prefix="/api/analytics", tags=["Analytics"])
    app.include_router(admin_router, prefix="/api/admin", tags=["Admin"])
    app.include_router(alphapai_router, prefix="/api/alphapai", tags=["AlphaPai"])
    app.include_router(jiuqian_router, prefix="/api/jiuqian", tags=["Jiuqian"])
    app.include_router(engine_router, prefix="/api/engine", tags=["Engine"])
    app.include_router(favorites_router, prefix="/api/favorites", tags=["Favorites"])
    app.include_router(stock_search_router, prefix="/api/stock", tags=["Stock Search"])
    app.include_router(topic_radar_router, prefix="/api/topic-radar", tags=["Topic Radar"])
    app.include_router(leaderboard_router, prefix="/api/leaderboard", tags=["Leaderboard"])
    app.include_router(signals_router, prefix="/api/signals", tags=["Signals"])
    app.include_router(analyst_rating_router, prefix="/api/analyst-rating", tags=["Analyst Rating"])
    app.include_router(chat_router, prefix="/api/chat", tags=["AI Chat"])
    app.include_router(predictions_router, prefix="/api/predictions", tags=["Predictions"])
    app.include_router(open_router, prefix="/api/open", tags=["Open API"])
    app.include_router(portfolio_news_router, prefix="/api/portfolio", tags=["Portfolio"])

    # WebSocket
    from backend.app.ws.feed import router as ws_router
    app.include_router(ws_router)

    @app.get("/api/health")
    async def health():
        return {"status": "ok", "version": "1.0.0"}

    # Serve frontend static files (built React app)
    if _FRONTEND_DIST.exists():
        # Mount static assets (JS, CSS, images)
        app.mount("/assets", StaticFiles(directory=_FRONTEND_DIST / "assets"), name="assets")

        # Catch-all: serve index.html for SPA client-side routing
        @app.get("/{full_path:path}")
        async def serve_spa(full_path: str):
            # If a real file exists (favicon, etc.), serve it
            file_path = _FRONTEND_DIST / full_path
            if file_path.is_file():
                return FileResponse(file_path)
            # Otherwise serve index.html for React Router
            return FileResponse(_FRONTEND_DIST / "index.html")

        logger.info("Serving frontend from %s", _FRONTEND_DIST)

    return app


app = create_app()
