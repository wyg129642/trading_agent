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
from backend.app.services.chat_debug import setup_chat_debug_logging

# Configure root logger so application logs appear in uvicorn output
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s:     %(message)s",
)
logger = logging.getLogger(__name__)

# Initialize dedicated chat debug logging (writes to logs/chat_debug.log)
setup_chat_debug_logging()

# Path to built frontend.
# Staging builds produce `frontend/dist-staging/` (see frontend/vite.config.ts +
# start_web.sh build_frontend); prod builds stay in `frontend/dist/`. Both
# worktrees carry both folders after a fresh clone, but each APP_ENV only
# ever serves its own bundle so a prod-bundle rebuild can't accidentally
# replace staging's in-progress UI. If the env-specific bundle is missing,
# fall back to the legacy `dist/` path — covers first-run on a freshly
# cloned staging worktree before `npm run build:staging` has run.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_FRONTEND_DIST_PROD = _PROJECT_ROOT / "frontend" / "dist"
_FRONTEND_DIST_STAGING = _PROJECT_ROOT / "frontend" / "dist-staging"


def _resolve_frontend_dist() -> Path:
    env = (get_settings().app_env or "").lower()
    if env == "staging" and _FRONTEND_DIST_STAGING.exists():
        return _FRONTEND_DIST_STAGING
    return _FRONTEND_DIST_PROD


_FRONTEND_DIST = _resolve_frontend_dist()


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

        # Start LLM enrichment if an enrichment LLM is configured
        if settings.llm_enrichment_api_key:
            from backend.app.services.alphapai_processor import AlphaPaiProcessor

            alphapai_processor = AlphaPaiProcessor(settings)
            await alphapai_processor.start(client)
            app.state.alphapai_processor = alphapai_processor
            logger.info("AlphaPai LLM enrichment started (model=%s)", settings.llm_enrichment_model)

    # Start Jiuqian sync + enrichment services
    jiuqian_sync = None
    jiuqian_processor = None
    try:
        from backend.app.services.jiuqian_sync import JiuqianSyncService

        jiuqian_sync = JiuqianSyncService(settings)
        await jiuqian_sync.start()
        app.state.jiuqian_sync = jiuqian_sync
        logger.info("Jiuqian sync service started")

        if settings.llm_enrichment_api_key:
            from backend.app.services.jiuqian_processor import JiuqianProcessor

            jiuqian_processor = JiuqianProcessor(settings)
            await jiuqian_processor.start()
            app.state.jiuqian_processor = jiuqian_processor
            logger.info("Jiuqian LLM enrichment started")
    except Exception:
        logger.exception("Failed to start Jiuqian services")

    # Start hot news LLM filter (classifies hot news titles for market relevance)
    hotnews_filter = None
    if settings.llm_enrichment_api_key:
        try:
            from backend.app.services.hotnews_filter import HotNewsFilter

            hotnews_filter = HotNewsFilter(settings)
            await hotnews_filter.start()
            app.state.hotnews_filter = hotnews_filter
            logger.info("Hot news LLM filter started")
        except Exception:
            logger.exception("Failed to start hot news filter")

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

    # Personal knowledge base — multi-phase startup:
    #   1. Mongo indexes (lexical side).
    #   2. jieba token backfill on legacy chunks.
    #   3. Milvus collection ensure + dense-vector backfill (fail-open:
    #      if Milvus or the embedder is unreachable, the chat tool's
    #      hybrid search degrades to BM25-only).
    #   4. Re-enqueue any parse jobs that were interrupted by a prior
    #      shutdown.
    # Everything here is best-effort — a failure must not block boot.
    try:
        from backend.app.services import user_kb_service as _user_kb
        from backend.app.services import user_kb_vector as _user_kb_vector

        migrated = await _user_kb.backfill_chunk_tokens()
        await _user_kb.ensure_indexes()
        if migrated:
            logger.info(
                "user_kb: migrated %d legacy chunks with jieba tokens", migrated,
            )

        # Dense-vector side. Non-fatal if Milvus / OpenAI is down.
        try:
            if await _user_kb_vector.ensure_collection():
                embedded = await _user_kb.backfill_embeddings()
                if embedded:
                    logger.info(
                        "user_kb: backfilled %d chunks with dense vectors at startup",
                        embedded,
                    )
            else:
                logger.warning(
                    "user_kb: Milvus unavailable — hybrid search will run BM25-only",
                )
        except Exception:
            logger.exception(
                "user_kb: vector backfill failed (non-fatal, BM25 still works)",
            )

        n = await _user_kb.recover_stuck_parses()
        if n:
            logger.info("user_kb: re-enqueued %d stuck parse jobs at startup", n)

        # Seed (or upgrade) the built-in workspace skills (标准 DCF、三张报表、
        # 敏感性、同业对比、研报纪要). Idempotent: slug-keyed upserts. Fail-open
        # — a skills seeding failure doesn't block chat or uploads.
        try:
            from backend.app.core.database import async_session_factory
            from backend.app.services import kb_skills_service as _skills_svc
            async with async_session_factory() as _db:
                count = await _skills_svc.ensure_system_skills(_db)
                if count:
                    logger.info(
                        "user_kb: seeded/upgraded %d system skill(s)", count,
                    )
        except Exception:
            logger.exception(
                "user_kb: system-skill seeding failed (non-fatal)",
            )

        # Continuous ASR recovery sweep: when the jumpbox tunnel is down
        # (ReadTimeout during upload), _parse_audio defers the doc back to
        # PENDING instead of marking it FAILED. This loop probes the ASR
        # service every 60s and, when it recovers, re-enqueues every
        # pending audio doc — realising the "服务恢复后会自动续转" promise
        # the UI banner makes to the user.
        import asyncio as _aio_kb
        app.state.asr_recovery_task = _aio_kb.create_task(
            _user_kb.asr_recovery_sweep_loop(interval_seconds=60),
            name="user_kb_asr_recovery_sweep",
        )
    except Exception:
        logger.exception("user_kb startup recovery failed (non-fatal)")

    # Connect the research-interaction recorder (MongoDB-backed session log
    # feeding the admin /admin/research-logs visualization page). Best-effort:
    # if the target Mongo refuses auth, the recorder disables itself and the
    # chat pipeline proceeds untouched.
    try:
        from backend.app.services.research_interaction_log import init_recorder
        await init_recorder()
    except Exception:
        logger.exception("Failed to initialize research interaction recorder")

    # Start daily AI-chat recommendation scheduler
    recommendation_scheduler = None
    try:
        from backend.app.services.recommendation_scheduler import RecommendationScheduler
        recommendation_scheduler = RecommendationScheduler(settings)
        await recommendation_scheduler.start()
        app.state.recommendation_scheduler = recommendation_scheduler
        logger.info("Recommendation scheduler started")
    except Exception:
        logger.exception("Failed to start recommendation scheduler")

    # Pre-warm the portfolio-quotes Redis cache so the first dashboard hit is fast.
    # Also re-warms every 60s so the 90s TTL never expires under users.
    async def _quote_warmer():
        import asyncio as _asyncio
        import yaml as _yaml
        from pathlib import Path as _Path
        from backend.app.services.stock_quote import get_quotes
        cfg_path = _Path(__file__).resolve().parent.parent.parent / "config" / "portfolio_sources.yaml"
        await _asyncio.sleep(2)  # let DB / Redis settle
        while True:
            try:
                with open(cfg_path, "r", encoding="utf-8") as f:
                    data = _yaml.safe_load(f) or {}
                pairs: dict[str, str] = {}
                for s in data.get("sources", []):
                    t = s.get("stock_ticker", "")
                    if t and t not in pairs:
                        pairs[t] = s.get("stock_market", "")
                if pairs:
                    await get_quotes(
                        list(pairs.items()),
                        redis=app.state.redis,
                        settings=settings,
                        use_cache=False,
                    )
                    logger.info("Portfolio quote cache warmed (%d tickers)", len(pairs))
            except Exception:
                logger.exception("Quote warmer iteration failed")
            await _asyncio.sleep(60)

    import asyncio as _aio
    app.state.quote_warmer_task = _aio.create_task(_quote_warmer())

    # ── Revenue Modeling: load industry packs + import public recipes ────
    try:
        from industry_packs import pack_registry
        pack_registry.reload()
        packs = pack_registry.list()
        logger.info(
            "Revenue Modeling: %d industry pack(s) loaded: %s",
            len(packs), [p.slug for p in packs],
        )
        # Auto-import public pack recipes into the DB on first boot so researchers
        # see ready-to-run recipes immediately. Idempotent on (slug, version).
        from sqlalchemy import select
        from backend.app.core.database import async_session_factory
        from backend.app.models.recipe import Recipe
        async with async_session_factory() as _db:
            for pack in packs:
                for rname, rdata in pack.recipes.items():
                    pack_ref = f"{pack.slug}:{rname}"
                    existing = (
                        await _db.execute(
                            select(Recipe)
                            .where(Recipe.pack_ref == pack_ref)
                            .order_by(Recipe.version.desc())
                            .limit(1)
                        )
                    ).scalar_one_or_none()
                    if existing:
                        continue
                    _db.add(Recipe(
                        name=rdata.get("name") or rname,
                        slug=rdata.get("slug") or rname,
                        industry=rdata.get("industry") or pack.slug,
                        description=rdata.get("description", ""),
                        graph={"nodes": rdata.get("nodes", []), "edges": rdata.get("edges", [])},
                        version=1,
                        is_public=True,
                        created_by=None,
                        pack_ref=pack_ref,
                        tags=list(rdata.get("tags") or []),
                    ))
            await _db.commit()
        logger.info("Revenue Modeling: seed recipes checked / imported")
    except Exception:
        logger.exception("Revenue Modeling pack init failed (non-fatal)")

    # Weekly feedback consolidator (Fridays 23:00 local).
    async def _feedback_consolidator_cron():
        import asyncio as _asyncio
        from datetime import datetime as _dt
        from backend.app.services.feedback_consolidator import consolidate_feedback
        while True:
            try:
                now = _dt.now()
                # Friday = 4 in Python (Mon=0). Run at 23:00.
                if now.weekday() == 4 and now.hour == 23:
                    res = await consolidate_feedback()
                    logger.info("Weekly feedback consolidator: %s", res)
                    await _asyncio.sleep(3700)  # skip past the hour
                else:
                    await _asyncio.sleep(600)  # check every 10 minutes
            except Exception:
                logger.exception("feedback consolidator loop iteration failed")
                await _asyncio.sleep(600)

    import asyncio as _aio_fc
    app.state.feedback_consolidator_task = _aio_fc.create_task(_feedback_consolidator_cron())

    # Pre-warm Wind 一致预期 (consensus forecast) cache — MySQL queries take 10-15s
    # against the non-indexed wind tables, so users should never hit it cold.
    # Cache TTL is 30min; re-warm every 25min to stay ahead.
    async def _consensus_warmer():
        import asyncio as _asyncio
        import yaml as _yaml
        from pathlib import Path as _Path
        from backend.app.services.consensus_forecast import fetch_consensus
        cfg_path = _Path(__file__).resolve().parent.parent.parent / "config" / "portfolio_sources.yaml"
        await _asyncio.sleep(30)  # let other services finish startup first
        while True:
            try:
                with open(cfg_path, "r", encoding="utf-8") as f:
                    data = _yaml.safe_load(f) or {}
                pairs: list[tuple[str, str]] = []
                seen: set[str] = set()
                for s in data.get("sources", []):
                    t = s.get("stock_ticker", "")
                    if t and t not in seen:
                        seen.add(t)
                        pairs.append((t, s.get("stock_market", "")))
                if pairs:
                    result = await fetch_consensus(
                        pairs,
                        settings=settings,
                        redis=app.state.redis,
                        use_cache=False,
                    )
                    logger.info("Consensus forecast cache warmed (%d A-share tickers)", len(result))
            except Exception:
                logger.exception("Consensus warmer iteration failed")
            await _asyncio.sleep(1500)  # 25 min

    app.state.consensus_warmer_task = _aio.create_task(_consensus_warmer())

    # ── Citation audit daily sampler + weekly review ─────────────────────
    # Daily: pick ~8 ready models, sample 5% of their citations, write audit
    # logs. Weekly (Mon 08:00): aggregate the log + push Feishu alert + auto-
    # pause models exceeding the 15% red line.
    async def _citation_audit_cron():
        import asyncio as _asyncio
        from datetime import datetime as _dt
        from backend.app.services.hallucination_guard import (
            daily_sample_pass,
            weekly_review_and_alert,
        )
        # Let other services finish startup first
        await _asyncio.sleep(120)
        last_weekly_iso = ""  # guard against double-fire within the hour
        while True:
            try:
                now = _dt.now()
                # Daily sample pass at 02:00 local (budget-friendly hour).
                if now.hour == 2 and now.minute < 5:
                    try:
                        result = await daily_sample_pass()
                        logger.info(
                            "Citation audit daily pass: sampled %d models",
                            result.get("sampled_models", 0),
                        )
                    except Exception:
                        logger.exception("citation_audit daily pass failed")
                    await _asyncio.sleep(600)  # skip the window

                # Weekly review every Monday at 08:00
                elif now.weekday() == 0 and now.hour == 8 and now.minute < 5:
                    stamp = now.strftime("%Y-%m-%dT%H")
                    if stamp != last_weekly_iso:
                        last_weekly_iso = stamp
                        try:
                            review = await weekly_review_and_alert()
                            logger.info(
                                "Weekly hallucination review: rate=%.2f%% paused=%d",
                                100.0 * (review.get("summary", {}).get("hallucination_rate") or 0.0),
                                len(review.get("paused_models") or []),
                            )
                        except Exception:
                            logger.exception("weekly_review_and_alert failed")
                    await _asyncio.sleep(600)
                else:
                    await _asyncio.sleep(300)  # check every 5 min
            except Exception:
                logger.exception("citation_audit cron iteration failed")
                await _asyncio.sleep(300)

    app.state.citation_audit_task = _aio.create_task(_citation_audit_cron())

    # Daily A/B-winner distillation at 03:00 local.
    async def _ab_distiller_cron():
        import asyncio as _asyncio
        from datetime import datetime as _dt
        from backend.app.services.ab_winner_distiller import distill_ab_winners
        await _asyncio.sleep(180)
        last_stamp = ""
        while True:
            try:
                now = _dt.now()
                if now.hour == 3 and now.minute < 5:
                    stamp = now.strftime("%Y-%m-%dT%H")
                    if stamp != last_stamp:
                        last_stamp = stamp
                        try:
                            res = await distill_ab_winners(since_days=30)
                            logger.info("A/B winner distiller: %s", res)
                        except Exception:
                            logger.exception("ab_winner distill failed")
                    await _asyncio.sleep(600)
                else:
                    await _asyncio.sleep(300)
            except Exception:
                logger.exception("ab distiller cron iteration failed")
                await _asyncio.sleep(300)

    app.state.ab_distiller_task = _aio.create_task(_ab_distiller_cron())

    # Staging-only: auto-mirror users + user_preferences + user_sources +
    # kb_folders + watchlists + watchlist_items from prod Postgres into
    # the staging DB. Runs once on boot (so employees can log in to a
    # fresh staging instance with their prod credentials immediately),
    # then every 15 min to pick up new signups / preference edits.
    # See backend/app/services/staging_user_sync.py for the full rationale.
    if settings.is_staging:
        from backend.app.services.staging_user_sync import run_staging_user_sync_loop
        app.state.staging_user_sync_task = _aio.create_task(
            run_staging_user_sync_loop(interval_seconds=900)
        )
        logger.info("staging_user_sync: scheduled (interval=900s)")

    yield

    # Shutdown
    task = getattr(app.state, "quote_warmer_task", None)
    if task:
        task.cancel()
    task = getattr(app.state, "consensus_warmer_task", None)
    if task:
        task.cancel()
    task = getattr(app.state, "asr_recovery_task", None)
    if task:
        task.cancel()
    task = getattr(app.state, "feedback_consolidator_task", None)
    if task:
        task.cancel()
    task = getattr(app.state, "citation_audit_task", None)
    if task:
        task.cancel()
    task = getattr(app.state, "ab_distiller_task", None)
    if task:
        task.cancel()
    task = getattr(app.state, "staging_user_sync_task", None)
    if task:
        task.cancel()
    try:
        from backend.app.services.quote_providers import futu_provider
        futu_provider.close_ctx()
    except Exception:
        pass
    if recommendation_scheduler:
        await recommendation_scheduler.stop()
    if tracking_alert_svc:
        await tracking_alert_svc.stop()
    if hotnews_filter:
        await hotnews_filter.stop()
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
    from backend.app.api.alphapai_db import router as alphapai_db_router
    from backend.app.api.jinmen_db import router as jinmen_db_router
    from backend.app.api.meritco_db import router as meritco_db_router
    from backend.app.api.thirdbridge_db import router as thirdbridge_db_router
    from backend.app.api.funda_db import router as funda_db_router
    from backend.app.api.gangtise_db import router as gangtise_db_router
    from backend.app.api.acecamp_db import router as acecamp_db_router
    from backend.app.api.alphaengine_db import router as alphaengine_db_router
    from backend.app.api.semianalysis_db import router as semianalysis_db_router
    from backend.app.api.unified import router as unified_router
    from backend.app.api.stock_hub import router as stock_hub_router
    from backend.app.api.jiuqian import router as jiuqian_router
    from backend.app.api.engine import router as engine_router
    from backend.app.api.favorites import router as favorites_router
    from backend.app.api.stock_search import router as stock_search_router
    from backend.app.api.leaderboard import router as leaderboard_router
    from backend.app.api.signals import router as signals_router
    from backend.app.api.analyst_rating import router as analyst_rating_router
    from backend.app.api.chat import router as chat_router
    from backend.app.api.chat_memory import router as chat_memory_router
    from backend.app.api.predictions import router as predictions_router
    from backend.app.api.open import router as open_router
    from backend.app.api.portfolio_news import router as portfolio_news_router
    from backend.app.api.sentimentrader import router as sentimentrader_router
    from backend.app.api.data_sources import router as data_sources_router
    from backend.app.api.research_logs import router as research_logs_router
    from backend.app.api.user_kb import router as user_kb_router
    from backend.app.api.database_overview import router as database_overview_router
    from backend.app.api.platform_info import router as platform_info_router
    from backend.app.api.revenue_models import router as revenue_models_router
    from backend.app.api.recipes import router as recipes_router
    from backend.app.api.playbook import router as playbook_router
    from backend.app.api.citation_audit import router as citation_audit_router
    from backend.app.api.backtest import router as backtest_router
    from backend.app.api.collaboration import router as collaboration_router
    from backend.app.api.expert_calls import router as expert_calls_router
    from backend.app.api.cost_governance import router as cost_governance_router
    from backend.app.api.kb_doc_viewer import router as kb_doc_viewer_router
    from backend.app.api.revenue_snapshot import router as revenue_snapshot_router

    app.include_router(auth_router, prefix="/api/auth", tags=["Auth"])
    app.include_router(news_router, prefix="/api/news", tags=["News"])
    app.include_router(watchlist_router, prefix="/api/watchlists", tags=["Watchlists"])
    app.include_router(sources_router, prefix="/api/sources", tags=["Sources"])
    app.include_router(analytics_router, prefix="/api/analytics", tags=["Analytics"])
    app.include_router(admin_router, prefix="/api/admin", tags=["Admin"])
    app.include_router(alphapai_router, prefix="/api/alphapai", tags=["AlphaPai"])
    app.include_router(alphapai_db_router, prefix="/api/alphapai-db", tags=["AlphaPai DB"])
    app.include_router(jinmen_db_router, prefix="/api/jinmen-db", tags=["Jinmen DB"])
    app.include_router(meritco_db_router, prefix="/api/meritco-db", tags=["Meritco DB"])
    app.include_router(thirdbridge_db_router, prefix="/api/thirdbridge-db", tags=["Third Bridge DB"])
    app.include_router(funda_db_router, prefix="/api/funda-db", tags=["Funda DB"])
    app.include_router(gangtise_db_router, prefix="/api/gangtise-db", tags=["Gangtise DB"])
    app.include_router(acecamp_db_router, prefix="/api/acecamp-db", tags=["AceCamp DB"])
    app.include_router(alphaengine_db_router, prefix="/api/alphaengine-db", tags=["AlphaEngine DB"])
    app.include_router(semianalysis_db_router, prefix="/api/semianalysis-db", tags=["SemiAnalysis DB"])
    app.include_router(unified_router, prefix="/api/unified", tags=["Unified (cross-platform)"])
    app.include_router(stock_hub_router, prefix="/api/stock-hub", tags=["Stock Hub (per-stock aggregator)"])
    app.include_router(jiuqian_router, prefix="/api/jiuqian", tags=["Jiuqian"])
    app.include_router(engine_router, prefix="/api/engine", tags=["Engine"])
    app.include_router(favorites_router, prefix="/api/favorites", tags=["Favorites"])
    app.include_router(stock_search_router, prefix="/api/stock", tags=["Stock Search"])
    app.include_router(leaderboard_router, prefix="/api/leaderboard", tags=["Leaderboard"])
    app.include_router(signals_router, prefix="/api/signals", tags=["Signals"])
    app.include_router(analyst_rating_router, prefix="/api/analyst-rating", tags=["Analyst Rating"])
    app.include_router(chat_router, prefix="/api/chat", tags=["AI Chat"])
    app.include_router(chat_memory_router, prefix="/api/chat-memory", tags=["AI Chat Memory"])
    app.include_router(predictions_router, prefix="/api/predictions", tags=["Predictions"])
    app.include_router(open_router, prefix="/api/open", tags=["Open API"])
    app.include_router(portfolio_news_router, prefix="/api/portfolio", tags=["Portfolio"])
    app.include_router(sentimentrader_router, prefix="/api/sentimentrader", tags=["SentimenTrader"])
    app.include_router(data_sources_router, prefix="/api/data-sources", tags=["Data Sources"])
    app.include_router(research_logs_router, prefix="/api/research-logs", tags=["Research Logs (Admin)"])
    app.include_router(user_kb_router, prefix="/api/user-kb", tags=["Personal Knowledge Base"])
    app.include_router(database_overview_router, prefix="/api", tags=["System"])
    app.include_router(platform_info_router, prefix="/api/platform-info", tags=["AlphaPai Platform Info"])
    app.include_router(revenue_models_router, prefix="/api/models", tags=["Revenue Modeling"])
    app.include_router(collaboration_router, prefix="/api/models", tags=["Revenue Modeling"])
    app.include_router(citation_audit_router, prefix="/api/citation-audit", tags=["Citation Audit"])
    app.include_router(backtest_router, prefix="/api/backtest", tags=["Backtest & Calibration"])
    app.include_router(expert_calls_router, prefix="/api/expert-calls", tags=["Expert Calls"])
    app.include_router(recipes_router, prefix="/api/recipes", tags=["Recipes"])
    app.include_router(playbook_router, prefix="/api/playbook", tags=["Playbook"])
    app.include_router(cost_governance_router, prefix="/api", tags=["Cost Governance"])
    app.include_router(kb_doc_viewer_router, prefix="/api/kb-viewer", tags=["KB Doc Viewer"])
    app.include_router(revenue_snapshot_router, prefix="/api/snapshot", tags=["Revenue Snapshot"])

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
