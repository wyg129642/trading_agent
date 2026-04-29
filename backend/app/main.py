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

    # Start the chat-audit writer task. Every ChatTrace.log_* call enqueues
    # an event onto its in-memory queue; one consumer drains in batches and
    # writes to chat_audit_run / chat_audit_event. Failure-tolerant: if the
    # queue can't drain, the rotating logs/chat_debug.log file is still the
    # parallel safety net.
    from backend.app.services import chat_audit_writer
    await chat_audit_writer.start_writer()

    # Pre-warm the crawler Mongo connection pool so the first Stock Hub click
    # after a restart doesn't serialize 21 TCP handshakes. All 8 crawler DBs
    # share one URI; firing several concurrent pings opens that many physical
    # connections in parallel (a single ping only opens one).
    try:
        from backend.app.api.stock_hub import _client as _stock_hub_client
        import asyncio as _asyncio_warm
        _sh_mongo = _stock_hub_client(settings.alphapai_mongo_uri)
        await _asyncio_warm.gather(
            *[_sh_mongo.admin.command("ping") for _ in range(8)],
            return_exceptions=True,
        )
        logger.info("Stock Hub crawler Mongo pool warmed (8 conns)")
    except Exception as e:
        logger.warning("Stock Hub crawler Mongo warmup failed: %s", e)

    # Start Engine Manager (auto-starts the trading engine subprocess).
    # Staging must NOT spawn the engine — it shares prod's Postgres pool, and
    # every ungraceful backend restart orphans the subprocess (ppid=1), each
    # holding a fresh connection pool. Accumulated orphans exhausted
    # max_connections once already (2026-04-24 incident). Gate matches the
    # CLAUDE.md contract: staging runs uvicorn only; engine stays prod-only.
    if settings.is_staging:
        app.state.engine_manager = None
        logger.info("Engine manager skipped (APP_ENV=staging)")
    else:
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

        # Realtime LLM enrichment of newly-ingested AlphaPai records is
        # OFF by default — gated on realtime_llm_enrichment_enabled so the
        # DB only carries raw scraped data going forward. Existing enrichment
        # rows are left untouched.
        if settings.llm_enrichment_api_key and settings.realtime_llm_enrichment_enabled:
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

        # Realtime LLM enrichment for Jiuqian — same gate as AlphaPai above.
        if settings.llm_enrichment_api_key and settings.realtime_llm_enrichment_enabled:
            from backend.app.services.jiuqian_processor import JiuqianProcessor

            jiuqian_processor = JiuqianProcessor(settings)
            await jiuqian_processor.start()
            app.state.jiuqian_processor = jiuqian_processor
            logger.info("Jiuqian LLM enrichment started")
    except Exception:
        logger.exception("Failed to start Jiuqian services")

    # Hot-news LLM relevance filter — also gated behind
    # realtime_llm_enrichment_enabled so no LLM runs at ingest time.
    hotnews_filter = None
    if settings.llm_enrichment_api_key and settings.realtime_llm_enrichment_enabled:
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

    # ── Portfolio research/news LLM-translation worker ───────────────────
    # Every 10 min, scan crawler collections for new portfolio-ticker docs
    # in the last 48h and translate any English body/title via qwen-plus.
    # Idempotent (`<field>_zh_src_hash` check), skip-on-cache-hit; stock_hub
    # already prefers native upstream translations (parsed_msg.translatedXxx /
    # list_item.titleCn / etc) over LLM, so this only fills gaps.
    from backend.app.services.portfolio_translation_worker import (
        worker_loop as _portfolio_translation_loop,
    )
    app.state.portfolio_translation_task = _aio.create_task(
        _portfolio_translation_loop(settings=settings, interval_sec=600, window_hours=48),
        name="portfolio_translation_worker",
    )

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

    # KB vector auto-sync — polls crawler Mongo and ingests new/updated docs
    # into Milvus via the jumpbox TEI (Qwen3-Embedding-8B) embedding service,
    # plus a daily delete sweep that reconciles Milvus to Mongo tombstones.
    # Without this loop the vector index drifts forever behind the crawlers.
    # Gated by VECTOR_SYNC_ENABLED + APP_ENV (+ KB_VECTOR_SYNC_ALLOW_PROD
    # override); see backend/app/services/kb_vector_sync.py for the full
    # rationale including ownership between staging and prod.
    kb_vector_sync_svc = None
    try:
        from backend.app.services.kb_vector_sync import (
            KbVectorSyncService,
            _should_start as _kb_vector_sync_should_start,
        )
        start_it, reason = _kb_vector_sync_should_start(settings)
        if start_it:
            kb_vector_sync_svc = KbVectorSyncService(settings)
            await kb_vector_sync_svc.start()
            app.state.kb_vector_sync = kb_vector_sync_svc
            logger.info("kb_vector_sync enabled (%s)", reason)
        else:
            logger.info("kb_vector_sync disabled (%s)", reason)
    except Exception:
        logger.exception("Failed to start kb_vector_sync (non-fatal)")

    # P3a — Title / institution normalization backfill loop. Same staging-only
    # ownership as kb_vector_sync (crawlers live in staging). Populates
    # `_normalized_title` + `_inst_normalized` on each Mongo doc so the
    # search-time mirror fold is a cheap dict lookup.
    try:
        from backend.app.services import kb_normalize_loop as _kb_norm
        await _kb_norm.start_normalize_loop(settings)
    except Exception:
        logger.exception("Failed to start kb_normalize_loop (non-fatal)")

    # Realtime rule-based ticker enricher — replaces the 10-min cron with an
    # in-process 30s loop so freshly crawled docs are tagged before the LLM
    # fallback (below) sees them. Enabled by default; disable via
    # RULE_TAG_REALTIME_ENABLED=false.
    if getattr(settings, "rule_tag_realtime_enabled", True):
        try:
            import asyncio as _aio_ruletag
            from backend.app.services.realtime_rule_tagger import (
                realtime_rule_tagger_loop,
            )
            app.state.realtime_rule_tagger_task = _aio_ruletag.create_task(
                realtime_rule_tagger_loop(settings),
                name="realtime_rule_tagger",
            )
            logger.info("realtime_rule_tagger task scheduled")
        except Exception:
            logger.exception("Failed to start realtime_rule_tagger (non-fatal)")

    # Realtime LLM ticker tagger — fallback NER for docs whose rule path
    # landed empty. Off by default; enable via LLM_TAG_REALTIME_ENABLED=true.
    if getattr(settings, "llm_tag_realtime_enabled", False):
        try:
            import asyncio as _aio_llmtag
            from backend.app.services.realtime_llm_tagger import (
                realtime_llm_tagger_loop,
            )
            app.state.realtime_llm_tagger_task = _aio_llmtag.create_task(
                realtime_llm_tagger_loop(settings, app.state.redis),
                name="realtime_llm_tagger",
            )
            logger.info("realtime_llm_tagger task scheduled")
        except Exception:
            logger.exception("Failed to start realtime_llm_tagger (non-fatal)")

    # PDF text extraction — wraps scripts/extract_pdf_texts.py in a flock'd
    # subprocess loop so newly crawled PDFs get pdf_text_md within minutes.
    # The legacy */30 min cron stays as belt-and-suspenders (same lock file),
    # so concurrent invocations no-op rather than double-spend the JVM.
    if getattr(settings, "pdf_text_extract_enabled", True):
        try:
            import asyncio as _aio_pdfx
            from backend.app.services.pdf_text_extract_loop import (
                pdf_text_extract_loop,
            )
            app.state.pdf_text_extract_task = _aio_pdfx.create_task(
                pdf_text_extract_loop(settings),
                name="pdf_text_extract",
            )
            logger.info("pdf_text_extract task scheduled")
        except Exception:
            logger.exception("Failed to start pdf_text_extract loop (non-fatal)")

    # News EN→ZH translator — fills news_items.metadata_.title_zh/summary_zh
    # so StockHub 突发新闻 cards render in Chinese by default. Mirrors the
    # local_ai_summary daemon (poll → qwen-plus → hash-keyed dedup).
    if getattr(settings, "news_translator_enabled", True):
        try:
            from backend.app.services.news_translator import news_translator_loop
            import asyncio as _aio_news
            app.state.news_translator_task = _aio_news.create_task(
                news_translator_loop(settings),
                name="news_translator",
            )
            logger.info("news_translator task scheduled")
        except Exception:
            logger.exception("Failed to start news_translator loop (non-fatal)")

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
    task = getattr(app.state, "realtime_rule_tagger_task", None)
    if task:
        task.cancel()
    task = getattr(app.state, "realtime_llm_tagger_task", None)
    if task:
        task.cancel()
    task = getattr(app.state, "pdf_text_extract_task", None)
    if task:
        task.cancel()
    task = getattr(app.state, "news_translator_task", None)
    if task:
        task.cancel()
    if kb_vector_sync_svc is not None:
        try:
            await kb_vector_sync_svc.stop()
        except Exception:
            logger.exception("kb_vector_sync shutdown failed")
    try:
        from backend.app.services import kb_normalize_loop as _kb_norm
        await _kb_norm.stop_normalize_loop()
    except Exception:
        logger.exception("kb_normalize_loop shutdown failed")
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
    if app.state.engine_manager is not None:
        await app.state.engine_manager.stop()
    try:
        await chat_audit_writer.stop_writer()
    except Exception:
        logger.exception("chat_audit_writer shutdown failed")
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

    # Surface starlette's MultiPartException reason. By default FastAPI swallows
    # it into a bare 400 with no structured log, so when an upload is rejected
    # (truncated body from a reverse proxy, malformed boundary, missing name
    # field, etc.) we see only ``POST ... 400`` with no clue why. Logging the
    # exception message + request path + content-length turns this into a
    # one-line diagnosis.
    from starlette.exceptions import HTTPException as StarletteHTTPException
    from starlette.requests import Request
    from starlette.responses import JSONResponse

    try:
        from python_multipart.exceptions import MultipartParseError  # type: ignore
    except Exception:
        try:
            from multipart.exceptions import MultipartParseError  # type: ignore
        except Exception:
            MultipartParseError = None  # type: ignore

    from starlette.formparsers import MultiPartException

    @app.exception_handler(MultiPartException)
    async def _multipart_exception_handler(request: Request, exc: MultiPartException):
        logger.warning(
            "multipart parse failed path=%s content_length=%s content_type=%r: %s",
            request.url.path,
            request.headers.get("content-length"),
            request.headers.get("content-type", ""),
            exc.message,
        )
        return JSONResponse(status_code=400, content={"detail": exc.message})

    # Starlette re-wraps MultiPartException into HTTPException(400, ...) in
    # Request._get_form before our handler gets a chance, so the MultiPart
    # handler above won't fire. This one logs *any* 400 on an upload endpoint
    # so we can see the exact detail ("Malformed boundary", "Part exceeded",
    # "Missing boundary", truncated-body ClientDisconnect, etc.).
    @app.exception_handler(StarletteHTTPException)
    async def _http_exception_logger(request: Request, exc: StarletteHTTPException):
        if (
            exc.status_code == 400
            and request.method == "POST"
            and ("/user-kb/documents" in request.url.path or "/upload" in request.url.path)
        ):
            logger.warning(
                "upload 400 path=%s cl=%s ct=%r detail=%r",
                request.url.path,
                request.headers.get("content-length"),
                request.headers.get("content-type", ""),
                exc.detail,
            )
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

    if MultipartParseError is not None:
        @app.exception_handler(MultipartParseError)
        async def _multipart_parse_error_handler(request: Request, exc):
            logger.warning(
                "multipart parse error path=%s content_length=%s: %r",
                request.url.path,
                request.headers.get("content-length"),
                exc,
            )
            return JSONResponse(
                status_code=400, content={"detail": f"multipart parse error: {exc}"},
            )

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
    from backend.app.api.ir_filings_db import router as ir_filings_db_router
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
    from backend.app.api.chat_audit import router as chat_audit_router
    from backend.app.api.chat_memory import router as chat_memory_router
    from backend.app.api.predictions import router as predictions_router
    from backend.app.api.open import router as open_router
    from backend.app.api.portfolio_news import router as portfolio_news_router
    from backend.app.api.sentimentrader import router as sentimentrader_router
    from backend.app.api.data_sources import router as data_sources_router
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
    from backend.app.api.risk_detection import router as risk_detection_router

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
    app.include_router(ir_filings_db_router, prefix="/api/ir-filings-db", tags=["IR Filings DB"])
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
    app.include_router(chat_audit_router, prefix="/api/chat-audit", tags=["AI Chat Audit"])
    app.include_router(chat_memory_router, prefix="/api/chat-memory", tags=["AI Chat Memory"])
    app.include_router(predictions_router, prefix="/api/predictions", tags=["Predictions"])
    app.include_router(risk_detection_router, prefix="/api/risk-detection", tags=["Risk Detection (Admin)"])
    app.include_router(open_router, prefix="/api/open", tags=["Open API"])
    app.include_router(portfolio_news_router, prefix="/api/portfolio", tags=["Portfolio"])
    app.include_router(sentimentrader_router, prefix="/api/sentimentrader", tags=["SentimenTrader"])
    app.include_router(data_sources_router, prefix="/api/data-sources", tags=["Data Sources"])
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
