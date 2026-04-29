"""Background EN→ZH translator for the Postgres ``news_items`` table.

Why: StockHub "突发新闻" cards source from Postgres (not Mongo), so the Mongo
translation backfill in ``scripts/translate_portfolio_research.py`` doesn't
cover them. This worker mirrors the ``crawl/local_ai_summary`` daemon shape
(periodic poll, qwen-plus, hash-keyed dedup) for that gap.

Translations are stored inside ``news_items.metadata_`` (JSONB) so no Alembic
migration is needed. Keys written:

    title_zh            : str
    title_zh_src_hash   : sha1(title)[:16]
    summary_zh          : str (translation of analysis_results.summary or
                               news_items.content[:600] when summary empty)
    summary_zh_src_hash : sha1(src_text)[:16]
    content_zh          : str (translation of full news_items.content body —
                               feeds the detail-drawer 正文 section's
                               markdown_zh; capped at CONTENT_MAX_CHARS so
                               outlier 50K+ HTML dumps don't run away)
    content_zh_src_hash : sha1(content)[:16]
    zh_translated_at    : ISO timestamp
    zh_model            : str (e.g. "qwen-plus")

The list endpoint (``_query_breaking_news``) reads these fields and surfaces
``title_zh``/``preview_zh`` so the existing frontend toggle just works. The
detail endpoint (``stock_hub_newsfeed_doc``) wires ``content_zh`` into the
正文 section's ``markdown_zh``.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import bindparam, desc, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import update as sa_update

from backend.app.core.database import async_session_factory
from backend.app.models.news import AnalysisResult, NewsItem
from backend.app.services.long_translator import (
    LongTranslator,
    TranslatorConfig,
    looks_foreign,
    src_hash,
)

logger = logging.getLogger(__name__)


# Cap so a runaway 100KB scrape (e.g. an entire HTML page accidentally
# stored as content) doesn't burn tokens. 60K covers Semiconductor
# Engineering's longest weekly digest (~13K) plus headroom.
CONTENT_MAX_CHARS = 60_000
# Skip below this — RSS items shorter than this are already covered by
# the summary translation and translating both is wasted spend.
CONTENT_MIN_CHARS = 400


def _summary_source(summary: str | None, content: str | None) -> str:
    """Pick the text we'll translate as the card preview source.

    Mirrors the ``_preview(summary or content)`` precedence in
    ``stock_hub.py::_query_breaking_news``. Falls back to the first 600 chars
    of content (vs the 320-char ``_preview`` slice) so the LLM has slightly
    more context — the API still re-slices to 320 on read.
    """
    s = (summary or "").strip()
    if s:
        return s
    c = (content or "").strip()
    if not c:
        return ""
    return c[:600]


async def _scan_and_translate(
    *,
    translator: LongTranslator,
    lookback_days: int,
    per_cycle_max: int,
    model_name: str,
) -> dict:
    """One translation cycle. Returns a stats dict for logging."""
    since = datetime.now(timezone.utc) - timedelta(days=lookback_days)

    # Pull recent English news_items + their analysis_result summary in one
    # query.
    #
    # Filter by ``fetched_at`` rather than ``published_at``: a large slice of
    # rows (esp. press-release feeds) leave ``published_at`` NULL, but the
    # API's ``_query_breaking_news`` still surfaces them — so translating
    # only the ones with a real publish date would miss user-visible items.
    #
    # We push the "not-yet-translated" filter into Postgres (``NOT (metadata
    # ? 'title_zh')``) so a backlog of older untranslated rows isn't blocked
    # behind newer already-cached rows in the LIMIT. Without this the worker
    # spins on the latest N rows after the first backfill and never catches
    # up to anything older. ``OR NOT (metadata ? 'summary_zh')`` keeps rows
    # where only one half is done coming back for the missing half.
    async with async_session_factory() as db:
        stmt = (
            select(NewsItem, AnalysisResult)
            .outerjoin(AnalysisResult, AnalysisResult.news_item_id == NewsItem.id)
            .where(NewsItem.language == "en")
            .where(NewsItem.fetched_at >= since)
            .where(
                ~NewsItem.metadata_.has_key("title_zh")  # noqa: W601
                | ~NewsItem.metadata_.has_key("summary_zh")  # noqa: W601
                | ~NewsItem.metadata_.has_key("content_zh")  # noqa: W601
            )
            .order_by(desc(NewsItem.fetched_at))
            .limit(per_cycle_max * 4)
        )
        rows = (await db.execute(stmt)).all()

    # (id, title, title_h, summary_src, summary_h, content_src, content_h)
    pending: list[tuple[str, str, str, str, str, str, str]] = []
    seen = 0
    cached = 0
    for n, a in rows:
        seen += 1
        md = n.metadata_ or {}

        title = (n.title or "").strip()
        title_h = src_hash(title) if title else ""
        title_zh_cached = (md.get("title_zh") or "").strip()
        title_hash_cached = md.get("title_zh_src_hash") or ""
        title_needs = bool(
            title
            and looks_foreign(title, min_signal=20)
            and (title_hash_cached != title_h or not title_zh_cached)
        )

        summary_src = _summary_source(
            (a.summary if a else None),
            n.content,
        )
        summary_h = src_hash(summary_src) if summary_src else ""
        summary_zh_cached = (md.get("summary_zh") or "").strip()
        summary_hash_cached = md.get("summary_zh_src_hash") or ""
        # min_signal=30: RSS feeds (CNBC Top News, WSJ, MarketWatch) routinely
        # ship 50–120 char summaries with ~40–90 ASCII letters. The default
        # min_signal=100 silently gated those out — `title_zh` would land but
        # `summary_zh` would not, so cards rendered "中文标题 + English preview".
        # `len(summary_src) >= 40` already filters out tiny snippets.
        summary_needs = bool(
            summary_src
            and len(summary_src) >= 40
            and looks_foreign(summary_src, min_signal=30)
            and (summary_hash_cached != summary_h or not summary_zh_cached)
        )

        # Full-body translation feeds the detail-drawer 正文 markdown_zh.
        # Without this, sources like Semiconductor Engineering's weekly digest
        # (~13K chars) showed Chinese title+summary but English body. Skip
        # short bodies (already covered by summary) and outlier dumps to keep
        # spend bounded.
        content_src = (n.content or "").strip()
        if len(content_src) > CONTENT_MAX_CHARS:
            content_src = content_src[:CONTENT_MAX_CHARS]
        content_h = src_hash(content_src) if content_src else ""
        content_zh_cached = (md.get("content_zh") or "").strip()
        content_hash_cached = md.get("content_zh_src_hash") or ""
        content_needs = bool(
            content_src
            and len(content_src) >= CONTENT_MIN_CHARS
            and looks_foreign(content_src, min_signal=50)
            and (content_hash_cached != content_h or not content_zh_cached)
        )

        if not title_needs and not summary_needs and not content_needs:
            cached += 1
            continue

        pending.append((
            n.id,
            title if title_needs else "",
            title_h if title_needs else "",
            summary_src if summary_needs else "",
            summary_h if summary_needs else "",
            content_src if content_needs else "",
            content_h if content_needs else "",
        ))
        if len(pending) >= per_cycle_max:
            break

    if not pending:
        return {"seen": seen, "cached": cached, "translated": 0}

    # Translate concurrently; the translator's internal semaphore keeps real
    # API concurrency at cfg.max_concurrency.
    async def _do(item):
        _id, t, th, s, sh, c, ch = item
        t_zh = await translator.translate(t) if t else ""
        s_zh = await translator.translate(s) if s else ""
        c_zh = await translator.translate(c) if c else ""
        return _id, t_zh, th, s_zh, sh, c_zh, ch

    translated_count = 0
    BATCH = 24
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    async with async_session_factory() as db:
        for i in range(0, len(pending), BATCH):
            batch = pending[i:i + BATCH]
            results = await asyncio.gather(*(_do(it) for it in batch))
            for nid, title_zh, title_h, summary_zh, summary_h, content_zh, content_h in results:
                updates: dict = {}
                if title_zh:
                    updates["title_zh"] = title_zh
                    updates["title_zh_src_hash"] = title_h
                if summary_zh:
                    updates["summary_zh"] = summary_zh
                    updates["summary_zh_src_hash"] = summary_h
                if content_zh:
                    updates["content_zh"] = content_zh
                    updates["content_zh_src_hash"] = content_h
                if not updates:
                    continue
                updates["zh_translated_at"] = now_iso
                updates["zh_model"] = model_name
                # Merge into existing metadata JSONB without trampling other
                # keys (e.g. feed_id). PG `metadata || $patch::jsonb` does a
                # shallow merge with the patch winning on conflict; binding
                # the patch as JSONB avoids a CAST(? AS jsonb) string-typing
                # mistake.
                await db.execute(
                    sa_update(NewsItem)
                    .where(NewsItem.id == nid)
                    .values(metadata_=NewsItem.metadata_.op("||")(
                        bindparam("patch", value=updates, type_=JSONB)
                    ))
                )
                translated_count += 1
            await db.commit()
            await asyncio.sleep(0.4)

    return {"seen": seen, "cached": cached, "translated": translated_count}


async def news_translator_loop(settings) -> None:
    """Long-running lifespan task. Sleeps `interval_seconds` between cycles
    and survives transient errors with a one-off backoff.
    """
    if not getattr(settings, "news_translator_enabled", True):
        logger.info("news_translator: disabled by settings, exiting")
        return
    if not settings.llm_enrichment_api_key:
        logger.warning("news_translator: LLM_ENRICHMENT_API_KEY not set — skipping")
        return

    interval = int(getattr(settings, "news_translator_interval_seconds", 300))
    per_cycle = int(getattr(settings, "news_translator_per_cycle_max", 50))
    lookback = int(getattr(settings, "news_translator_lookback_days", 14))

    cfg = TranslatorConfig(
        api_key=settings.llm_enrichment_api_key,
        base_url=settings.llm_enrichment_base_url,
        model=settings.llm_enrichment_model,
    )
    translator = LongTranslator(cfg)
    logger.info(
        "news_translator started: model=%s interval=%ss per_cycle=%d lookback=%dd",
        cfg.model, interval, per_cycle, lookback,
    )

    # Stagger startup so we don't pile on top of the other lifespan warmers.
    await asyncio.sleep(45)

    while True:
        try:
            stats = await _scan_and_translate(
                translator=translator,
                lookback_days=lookback,
                per_cycle_max=per_cycle,
                model_name=cfg.model,
            )
            cost_plus = (
                translator.in_tokens / 1000 * 0.0008
                + translator.out_tokens / 1000 * 0.002
            )
            logger.info(
                "news_translator cycle: seen=%d cached=%d translated=%d  "
                "(cumulative calls=%d in=%d out=%d cost≈¥%.2f)",
                stats["seen"], stats["cached"], stats["translated"],
                translator.calls, translator.in_tokens, translator.out_tokens,
                cost_plus,
            )
        except asyncio.CancelledError:
            logger.info("news_translator cancelled")
            raise
        except Exception:
            logger.exception("news_translator cycle failed")
            await asyncio.sleep(min(interval, 60))
            continue
        await asyncio.sleep(interval)
