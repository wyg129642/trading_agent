"""Lifespan worker that translates newly-arrived portfolio research / news /
filings docs in the background.

Mirrors the targets + field shape of ``scripts/translate_portfolio_research.py``
but runs as a periodic asyncio task inside the FastAPI lifespan, so any new
crawler doc that lands in Mongo and matches a portfolio ticker gets a
``<field>_zh`` translation within minutes — no manual CLI re-run needed.

Behaviour:
- Each pass scans the last ``window_hours`` (default 48h) of release_time_ms
  for each (db, collection), filtered to docs whose ``_canonical_tickers``
  intersect the portfolio set. Wider than the polling interval so a doc that
  enriches its tickers a few hours after first ingestion still gets caught.
- Idempotent: docs whose ``<field>_zh_src_hash`` matches the current source
  hash are skipped. Re-running costs zero LLM calls for unchanged content.
- Same translator (``LongTranslator``) as the news lifespan worker and the
  one-shot script — single prompt, single chunker.
- Native upstream translations (``parsed_msg.translatedDescription`` etc) are
  resolved at read time in ``stock_hub.py`` and don't go through this worker;
  this worker only writes the LLM fallback when no native zh exists.
"""
from __future__ import annotations

import asyncio
import datetime
import logging
import time
from pathlib import Path
from typing import Any

import yaml
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne

from backend.app.config import Settings
from backend.app.services.long_translator import (
    LongTranslator,
    TranslatorConfig,
    looks_foreign,
    src_hash,
)

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[3]

# Same shape as ``scripts.translate_portfolio_research.DEFAULT_TARGETS`` —
# kept here independently so the lifespan worker doesn't import the script.
# When you add/remove a field there, mirror the change here.
DEFAULT_TARGETS: list[tuple[str, str, list[str]]] = [
    ("gangtise-full", "researches",           ["title", "content_md", "brief_md"]),
    ("gangtise-full", "summaries",            ["title", "content_md", "brief_md"]),
    ("gangtise-full", "chief_opinions",       ["title", "content_md", "brief_md", "description_md"]),
    ("alphaengine",   "foreign_reports",      ["content_md", "doc_introduce"]),
    ("alphaengine",   "china_reports",        ["content_md", "doc_introduce"]),
    ("funda",         "posts",                ["title", "content_md"]),
    ("funda",         "earnings_reports",     ["title", "content_md"]),
    ("funda",         "earnings_transcripts", ["title", "content_md"]),
    ("funda",         "semianalysis_posts",   ["title", "content_md", "subtitle"]),
    ("ir_filings",    "sec_edgar",            ["title", "pdf_text_md"]),
    ("ir_filings",    "hkex",                 ["title", "pdf_text_md"]),
    ("ir_filings",    "asx",                  ["title", "pdf_text_md"]),
    ("alphapai-full", "reports",              ["title", "content_md"]),
    ("alphapai-full", "roadshows",            ["title", "content_md", "transcript_md"]),
    ("jinmen-full",   "oversea_reports",      ["title", "content_md"]),
    ("jinmen-full",   "reports",              ["title", "content_md"]),
    ("jinmen-full",   "meetings",             ["title", "content_md", "summary_md"]),
    ("jiuqian-full",  "forum",                ["title", "content_md", "summary_md", "expert_content_md"]),
    ("third-bridge",  "interviews",           ["title", "transcript_md", "agenda_md"]),
    ("acecamp",       "articles",             ["title", "content_md", "transcribe_md", "summary_md"]),
]

MARKET_SUFFIX: dict[str, list[str]] = {
    "美股": ["US"], "港股": ["HK"], "韩股": ["KS", "KQ"], "日股": ["T", "JP"],
    "澳股": ["AU"], "主板": ["SH"], "创业板": ["SZ"], "科创板": ["SH"],
}


def _portfolio_canon_tickers() -> set[str]:
    cfg_path = REPO_ROOT / "config" / "portfolio_sources.yaml"
    try:
        with cfg_path.open() as f:
            cfg = yaml.safe_load(f) or {}
    except Exception:
        logger.exception("portfolio_translation_worker: failed to read %s", cfg_path)
        return set()
    out: set[str] = set()
    for s in cfg.get("sources") or []:
        t = (s.get("stock_ticker") or "").strip()
        mk = (s.get("stock_market") or "").strip()
        if not t or not mk:
            continue
        suffixes = MARKET_SUFFIX.get(mk, [])
        if not suffixes:
            continue
        code = t.zfill(5) if mk == "港股" and t.isdigit() else t
        for suf in suffixes:
            out.add(f"{code}.{suf}")
    return out


def _hash_key(dest_field: str) -> str:
    return f"{dest_field}_src_hash"


async def run_one_pass(
    *,
    settings: Settings,
    window_hours: int = 48,
    skip_min_chars: int = 80,
    skip_max_chars: int = 400_000,
    max_docs_per_collection: int | None = 500,
) -> dict[str, Any]:
    """Run a single translation pass over the configured targets.

    Returns a stats dict. ``max_docs_per_collection`` caps per-collection
    work to protect the event loop on the rare case a backfill window
    explodes (e.g. after a long outage); set to ``None`` to disable.
    """
    if not settings.llm_enrichment_api_key:
        logger.warning("portfolio_translation_worker: LLM_ENRICHMENT_API_KEY unset — skipping")
        return {"enabled": False}

    canon = _portfolio_canon_tickers()
    if not canon:
        logger.warning("portfolio_translation_worker: no portfolio tickers — skipping")
        return {"enabled": True, "skipped": "no_tickers"}

    since_ms = int(
        (datetime.datetime.now() - datetime.timedelta(hours=window_hours)).timestamp() * 1000
    )

    uri = (
        getattr(settings, "alphapai_mongo_uri", None)
        or "mongodb://127.0.0.1:27018/"
    )
    client = AsyncIOMotorClient(uri, serverSelectionTimeoutMS=5000)

    cfg = TranslatorConfig(
        api_key=settings.llm_enrichment_api_key,
        base_url=settings.llm_enrichment_base_url,
        model=settings.llm_enrichment_model,
    )
    translator = LongTranslator(cfg)

    grand_translated = 0
    grand_chars = 0
    t0 = time.monotonic()
    per_coll_stats: list[tuple[str, int, int]] = []

    for db_name, coll_name, fields in DEFAULT_TARGETS:
        coll = client[db_name][coll_name]
        q: dict[str, Any] = {
            "release_time_ms": {"$gte": since_ms},
            "_canonical_tickers": {"$in": list(canon)},
        }
        proj = {f: 1 for f in fields}
        for f in fields:
            proj[f"{f}_zh"] = 1
            proj[f"{f}_zh_src_hash"] = 1
        proj["_canonical_tickers"] = 1
        proj["release_time_ms"] = 1
        proj["title"] = 1

        find = coll.find(q, proj)
        if max_docs_per_collection:
            find = find.limit(max_docs_per_collection)

        pending: list[tuple[Any, str, str, str, str]] = []
        n_seen = 0
        async for doc in find:
            n_seen += 1
            for src_field in fields:
                src = (doc.get(src_field) or "").strip()
                if not src:
                    continue
                is_title = src_field == "title"
                if is_title:
                    if not looks_foreign(src, min_signal=20):
                        continue
                    if len(src) > skip_max_chars:
                        continue
                else:
                    if not looks_foreign(src):
                        continue
                    if len(src) < skip_min_chars or len(src) > skip_max_chars:
                        continue
                dest_field = f"{src_field}_zh"
                cur_hash = src_hash(src)
                cached_hash = doc.get(_hash_key(dest_field))
                if cached_hash == cur_hash and (doc.get(dest_field) or "").strip():
                    continue
                pending.append((doc["_id"], src_field, dest_field, src, cur_hash))

        if not pending:
            per_coll_stats.append((f"{db_name}.{coll_name}", n_seen, 0))
            continue

        # Bound batch size so a viral 24h window doesn't open thousands of
        # concurrent gather frames. The translator's semaphore (size=6) is the
        # real concurrency cap; this just gates memory.
        BATCH = 24
        ops: list[UpdateOne] = []
        n_ok = 0
        for i in range(0, len(pending), BATCH):
            batch = pending[i:i + BATCH]

            async def _do(item: tuple[Any, str, str, str, str]):
                _id, _src_field, _dest_field, _src, _cur_hash = item
                t = await translator.translate(_src)
                return _id, _dest_field, _cur_hash, _src, t

            results = await asyncio.gather(*(_do(it) for it in batch), return_exceptions=False)
            grouped: dict[Any, dict[str, Any]] = {}
            for _id, dest_field, cur_hash, src_text, translated in results:
                if not translated:
                    continue
                grouped.setdefault(_id, {})[dest_field] = translated
                grouped[_id][_hash_key(dest_field)] = cur_hash
                n_ok += 1
                grand_chars += len(src_text)
            for _id, updates in grouped.items():
                ops.append(UpdateOne({"_id": _id}, {"$set": updates}))
            if len(ops) >= 50:
                try:
                    await coll.bulk_write(ops, ordered=False)
                except Exception:
                    logger.exception("portfolio_translation_worker: bulk_write failed for %s.%s", db_name, coll_name)
                ops.clear()
        if ops:
            try:
                await coll.bulk_write(ops, ordered=False)
            except Exception:
                logger.exception("portfolio_translation_worker: bulk_write failed for %s.%s", db_name, coll_name)

        per_coll_stats.append((f"{db_name}.{coll_name}", n_seen, n_ok))
        grand_translated += n_ok

    elapsed = time.monotonic() - t0
    cost_plus = (
        translator.in_tokens / 1000 * 0.0008
        + translator.out_tokens / 1000 * 0.002
    )
    if grand_translated:
        # Only log details when something happened — quiet pass-through cycles.
        for name, seen, ok in per_coll_stats:
            if ok:
                logger.info("portfolio_translation_worker: %-32s seen=%d translated=%d", name, seen, ok)
        logger.info(
            "portfolio_translation_worker: pass done — translated=%d chars=%.1fK calls=%d cost≈¥%.2f elapsed=%.0fs",
            grand_translated, grand_chars / 1e3, translator.calls, cost_plus, elapsed,
        )
    else:
        logger.debug("portfolio_translation_worker: no new work in last %dh", window_hours)

    return {
        "enabled": True,
        "translated": grand_translated,
        "calls": translator.calls,
        "in_tokens": translator.in_tokens,
        "out_tokens": translator.out_tokens,
        "cost_plus": cost_plus,
        "elapsed_sec": elapsed,
        "per_collection": per_coll_stats,
    }


async def worker_loop(
    *,
    settings: Settings,
    interval_sec: int = 600,
    window_hours: int = 48,
    initial_delay_sec: int = 90,
) -> None:
    """Periodic poller — runs ``run_one_pass`` every ``interval_sec``.

    First sleep gives the rest of the lifespan startup (vector sync, quote
    warmer, etc) time to settle so we don't compete for the event loop on
    cold start.
    """
    if not settings.llm_enrichment_api_key:
        logger.info("portfolio_translation_worker: disabled (LLM_ENRICHMENT_API_KEY unset)")
        return
    await asyncio.sleep(initial_delay_sec)
    logger.info(
        "portfolio_translation_worker: started (interval=%ds window=%dh model=%s)",
        interval_sec, window_hours, settings.llm_enrichment_model,
    )
    while True:
        try:
            await run_one_pass(settings=settings, window_hours=window_hours)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("portfolio_translation_worker: pass crashed")
        await asyncio.sleep(interval_sec)
