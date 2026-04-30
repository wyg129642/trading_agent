"""Translate foreign-language (non-Chinese) research/commentary/minutes content
attached to portfolio holdings in the past N days. Writes `<field>_zh` plus
`<field>_zh_src_hash` back into the same Mongo doc so subsequent reads are
instant and re-runs only translate genuine deltas.

Usage:
    PYTHONPATH=. python3 scripts/translate_portfolio_research.py [--days 90]
        [--limit N] [--include-ir-pages] [--dry-run] [--source DB.coll]

The script intentionally lives outside the FastAPI lifespan so we can run a
one-off backfill against staging without touching prod (per CLAUDE.md
"Stay in staging worktree" rule). The same translator service is used by the
existing Funda sentiment pipeline (`sentiment_translator.py`) — we just extend
it to handle longer research bodies via paragraph-level chunking.
"""
from __future__ import annotations

import argparse
import asyncio
import datetime
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

import yaml
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.app.config import get_settings  # noqa: E402
from backend.app.services.long_translator import (  # noqa: E402
    LongTranslator,
    TranslatorConfig,
    looks_foreign as _looks_foreign,
    src_hash as _src_hash,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("translate_portfolio")

# (db, collection, [fields-to-translate]) — `title` is included where the
# upstream platform doesn't already supply a translation. Collections that
# already store native zh titles (alphapai, jinmen, alphaengine — Chinese
# headlines on every doc) are skipped on title to avoid noise.
DEFAULT_TARGETS: list[tuple[str, str, list[str]]] = [
    # Gangtise 三集合 (researches / summaries / chief_opinions) 已通过平台原生
    # translatedBrief / translatedFormattedBrief / translatedTitle 提供 100%
    # 中文翻译 (外资研报; 内资本身就是中文). backend/app/api/stock_hub.py 通过
    # native_zh_paths 直接消费这些字段, 不需要 LLM 翻译. (2026-04-29 移除)
    ("alphaengine",   "foreign_reports",      ["content_md", "doc_introduce"]),
    ("alphaengine",   "china_reports",        ["content_md", "doc_introduce"]),
    ("funda",         "posts",                ["title", "content_md"]),
    ("funda",         "earnings_reports",     ["title", "content_md"]),
    ("funda",         "earnings_transcripts", ["title", "content_md"]),
    ("funda",         "semianalysis_posts",   ["title", "content_md", "subtitle"]),
    ("ir_filings",    "sec_edgar",            ["title", "pdf_text_md"]),
    ("ir_filings",    "hkex",                 ["title", "pdf_text_md"]),
    ("ir_filings",    "asx",                  ["title", "pdf_text_md"]),
    # alphapai mirrors foreign-broker reports — title can be English (eg
    # "JPM | APAC THEMATICS"). list_item.titleCn / list_item.contentCn
    # cover most of these natively, so stock_hub prefers them first; the
    # LLM `title_zh` is only the fallback when titleCn is empty.
    # pdf_text_md = full PDF body extracted by extract_pdf_texts.py — for
    # foreign-broker reports it's English even when the platform supplies a
    # Chinese summary, so translate it too.
    ("alphapai-full", "reports",              ["title", "content_md", "pdf_text_md"]),
    ("alphapai-full", "roadshows",            ["title", "content_md", "transcript_md"]),
    # jinmen oversea_reports has English PDF bodies (Goldman/MS broker
    # notes) under pdf_text_md while summary_md is jinmen's Chinese AI
    # digest — both deserve a `_zh` so the drawer renders fully in Chinese.
    ("jinmen-full",   "oversea_reports",      ["title", "content_md", "pdf_text_md"]),
    ("jinmen-full",   "reports",              ["title", "content_md", "pdf_text_md"]),
    ("jinmen-full",   "meetings",             ["title", "content_md", "summary_md"]),
    ("jiuqian-full",  "forum",                ["title", "content_md", "summary_md", "expert_content_md", "pdf_text_md"]),
    ("third-bridge",  "interviews",           ["title", "transcript_md", "agenda_md"]),
    ("acecamp",       "articles",             ["title", "content_md", "transcribe_md", "summary_md"]),
]
# IR pages (`ir_filings.ir_pages`) is opt-in via --include-ir-pages: the per-doc
# content is huge investor-deck PDFs (~130K chars avg) that dominate cost.
IR_PAGES_TARGET: tuple[str, str, list[str]] = (
    "ir_filings", "ir_pages", ["pdf_text_md", "content_md"],
)

MARKET_SUFFIX = {
    "美股": ["US"],
    "港股": ["HK"],
    "韩股": ["KS", "KQ"],
    "日股": ["T", "JP"],
    "澳股": ["AU"],
    "主板": ["SH"],
    "创业板": ["SZ"],
    "科创板": ["SH"],
}


def _hash_key(dest_field: str) -> str:
    return f"{dest_field}_src_hash"


def _portfolio_canon_tickers() -> set[str]:
    cfg_path = REPO_ROOT / "config" / "portfolio_sources.yaml"
    with cfg_path.open() as f:
        cfg = yaml.safe_load(f)
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


# Translator class lives in backend/app/services/long_translator.py — shared
# with the Postgres news_items lifespan worker so both paths use one prompt
# and one chunker.


# ─── Main loop ───────────────────────────────────────────────────────────────


async def _scan_and_translate(
    *,
    days: int,
    limit: int | None,
    sources_filter: set[str] | None,
    include_ir_pages: bool,
    dry_run: bool,
    skip_min_chars: int,
    skip_max_chars: int,
) -> None:
    settings = get_settings()
    if not settings.llm_enrichment_api_key:
        logger.error("LLM_ENRICHMENT_API_KEY not set in .env — aborting")
        return

    canon = _portfolio_canon_tickers()
    logger.info("portfolio canonical tickers: %d", len(canon))

    targets = list(DEFAULT_TARGETS)
    if include_ir_pages:
        targets.append(IR_PAGES_TARGET)
    if sources_filter:
        targets = [t for t in targets if f"{t[0]}.{t[1]}" in sources_filter]

    since_dt = datetime.datetime.now() - datetime.timedelta(days=days)
    since_ms = int(since_dt.timestamp() * 1000)

    # All current crawler DBs share one Mongo. settings.alphapai_mongo_uri is
    # representative; fall back to local default.
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
    logger.info("translator: model=%s base=%s", cfg.model, cfg.base_url)
    translator = LongTranslator(cfg)

    grand_total = 0
    grand_translated = 0
    grand_chars = 0
    t0 = time.monotonic()

    for db_name, coll_name, fields in targets:
        coll = client[db_name][coll_name]
        # Window: doc qualifies if EITHER its release_time_ms OR crawled_at
        # falls within the window. Catches backfilled-historical docs (old
        # release date but recent ingest) that StockHub still surfaces.
        q: dict[str, Any] = {
            "_canonical_tickers": {"$in": list(canon)},
            "$or": [
                {"release_time_ms": {"$gte": since_ms}},
                {"crawled_at": {"$gte": since_dt}},
            ],
        }
        proj = {f: 1 for f in fields}
        # Cache check needs the existing translation + its source-text hash.
        # Without projecting these the cached_hash comparison reads None and
        # we re-translate every doc on every run.
        for f in fields:
            proj[f"{f}_zh"] = 1
            proj[f"{f}_zh_src_hash"] = 1
        proj["_canonical_tickers"] = 1
        proj["release_time_ms"] = 1
        proj["title"] = 1

        cursor = coll.find(q, proj)
        n_seen = 0
        n_translated = 0
        n_skipped_cached = 0
        n_skipped_size = 0
        ops: list[UpdateOne] = []

        # Build a flat task list of (doc_id, src_field, dest_field, src_text,
        # cur_hash) so we can fire translations concurrently across docs. The
        # translator's internal semaphore caps real API concurrency at
        # cfg.max_concurrency; here we just feed it work fast enough.
        pending: list[tuple[Any, str, str, str, str]] = []
        async for doc in cursor:
            n_seen += 1
            for src_field in fields:
                src = (doc.get(src_field) or "").strip()
                if not src:
                    continue
                # Short fields (title) get a looser foreign-signal threshold
                # and exemption from the body-text min length, otherwise a
                # 30–80 char English headline would be skipped by the body
                # heuristic tuned for multi-paragraph content.
                is_title = src_field == "title"
                if is_title:
                    if not _looks_foreign(src, min_signal=20):
                        continue
                    if len(src) > skip_max_chars:
                        n_skipped_size += 1
                        continue
                else:
                    if not _looks_foreign(src):
                        continue
                    if len(src) < skip_min_chars or len(src) > skip_max_chars:
                        n_skipped_size += 1
                        continue
                dest_field = f"{src_field}_zh"
                cached_hash = doc.get(_hash_key(dest_field))
                cur_hash = _src_hash(src)
                if cached_hash == cur_hash and (doc.get(dest_field) or "").strip():
                    n_skipped_cached += 1
                    continue
                if dry_run:
                    n_translated += 1
                    grand_chars += len(src)
                    continue
                pending.append((doc["_id"], src_field, dest_field, src, cur_hash))
                if limit and (grand_translated + n_translated + len(pending)) >= limit:
                    break
            if limit and (grand_translated + n_translated + len(pending)) >= limit:
                break

        # Translate all pending tasks concurrently. Each gather batch is
        # bounded by cfg.max_concurrency × chunk fan-out so memory stays sane.
        BATCH = 24  # ~4× cfg.max_concurrency: keep the semaphore saturated
        for i in range(0, len(pending), BATCH):
            batch = pending[i:i + BATCH]

            async def _do(item):
                _id, _src_field, _dest_field, _src, _cur_hash = item
                t = await translator.translate(_src)
                return _id, _dest_field, _cur_hash, _src, t

            results = await asyncio.gather(*(_do(it) for it in batch))
            grouped: dict[Any, dict[str, Any]] = {}
            for _id, dest_field, cur_hash, src, translated in results:
                if not translated:
                    continue  # transient failure — leave for next run
                grouped.setdefault(_id, {})[dest_field] = translated
                grouped[_id][_hash_key(dest_field)] = cur_hash
                n_translated += 1
                grand_chars += len(src)
            for _id, updates in grouped.items():
                ops.append(UpdateOne({"_id": _id}, {"$set": updates}))
            if len(ops) >= 50:
                await coll.bulk_write(ops, ordered=False)
                ops.clear()

        if ops and not dry_run:
            await coll.bulk_write(ops, ordered=False)

        elapsed = time.monotonic() - t0
        logger.info(
            "[%-32s] seen=%d translated=%d cached=%d size_skip=%d  (calls=%d in=%d out=%d  elapsed=%.0fs)",
            f"{db_name}.{coll_name}",
            n_seen, n_translated, n_skipped_cached, n_skipped_size,
            translator.calls, translator.in_tokens, translator.out_tokens, elapsed,
        )
        grand_total += n_seen
        grand_translated += n_translated
        if limit and grand_translated >= limit:
            logger.info("hit --limit %d, stopping", limit)
            break

    elapsed = time.monotonic() - t0
    # Pricing reference (DashScope, late 2025): qwen-plus = ¥0.0008/¥0.002 per
    # 1K tokens (input/output); qwen-turbo = ¥0.0003/¥0.0006. Numbers below are
    # billed-token sums actually returned by the API, not character estimates.
    cost_plus = (
        translator.in_tokens / 1000 * 0.0008
        + translator.out_tokens / 1000 * 0.002
    )
    cost_turbo = (
        translator.in_tokens / 1000 * 0.0003
        + translator.out_tokens / 1000 * 0.0006
    )
    logger.info("=" * 80)
    logger.info(
        "DONE  total_docs_scanned=%d  translated=%d  chars=%.1fM  elapsed=%.0fs",
        grand_total, grand_translated, grand_chars / 1e6, elapsed,
    )
    logger.info(
        "API:  calls=%d  input_tokens=%d  output_tokens=%d",
        translator.calls, translator.in_tokens, translator.out_tokens,
    )
    logger.info(
        "Cost: qwen-plus ¥%.2f  /  qwen-turbo ¥%.2f",
        cost_plus, cost_turbo,
    )
    if dry_run:
        # Estimate from chars when there's no actual API usage to bill against.
        est_in = grand_chars / 4
        est_out = grand_chars * 0.6 / 1.4
        logger.info(
            "DRY-RUN cost estimate (chars-based): qwen-plus ¥%.2f  /  qwen-turbo ¥%.2f",
            est_in / 1000 * 0.0008 + est_out / 1000 * 0.002,
            est_in / 1000 * 0.0003 + est_out / 1000 * 0.0006,
        )


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--days", type=int, default=90,
                   help="lookback window in days (default 90)")
    p.add_argument("--limit", type=int, default=None,
                   help="stop after translating N documents (across all sources)")
    p.add_argument("--include-ir-pages", action="store_true",
                   help="also translate ir_filings.ir_pages (large, ~125 MB extra)")
    p.add_argument("--source", action="append", default=[],
                   help="restrict to DB.collection (repeatable). e.g. --source gangtise-full.researches")
    p.add_argument("--dry-run", action="store_true",
                   help="count only, no API calls and no Mongo writes")
    p.add_argument("--skip-min-chars", type=int, default=80,
                   help="skip texts shorter than this (default 80)")
    p.add_argument("--skip-max-chars", type=int, default=400_000,
                   help="skip texts longer than this (default 400K to avoid runaway IR-PDF blowups)")
    return p.parse_args()


def main() -> int:
    # Proxy must be cleared so the OpenAI httpx client and motor talk locally.
    for var in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
        os.environ.pop(var, None)
    args = _parse_args()
    sources_filter = set(args.source) if args.source else None
    asyncio.run(_scan_and_translate(
        days=args.days,
        limit=args.limit,
        sources_filter=sources_filter,
        include_ir_pages=args.include_ir_pages,
        dry_run=args.dry_run,
        skip_min_chars=args.skip_min_chars,
        skip_max_chars=args.skip_max_chars,
    ))
    return 0


if __name__ == "__main__":
    sys.exit(main())
