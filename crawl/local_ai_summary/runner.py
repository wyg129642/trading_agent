#!/usr/bin/env python3
"""local_ai_summary watcher — qwen-plus card-preview summarizer for portfolio docs.

Run modes::

    python -m crawl.local_ai_summary.runner --once --since-days 90 --max 200
        # one-shot; backfill last N days, cap K calls

    python -m crawl.local_ai_summary.runner --watch --interval 300
        # daemon; loop every N seconds, incremental forever

Defaults are tuned for the realtime watcher path (10-min interval, 80 docs
per cycle, recent 14d window). Backfill should be invoked separately with
``--once --since-days 90``.

Reads:
  - config/portfolio_sources.yaml → set of canonical tickers
  - .env → LLM_ENRICHMENT_{API_KEY,BASE_URL,MODEL} (qwen-plus on DashScope)
  - Mongo at 127.0.0.1:27018 (ta-mongo-crawl)

Writes:
  - per-doc local_ai_summary field (one Mongo update per doc, idempotent)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import re
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import requests
from pymongo import MongoClient
from pymongo.collection import Collection

# Make local package imports work when invoked via `python -m crawl.local_ai_summary.runner`
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from crawl.local_ai_summary.holdings import (
    PORTFOLIO_YAML, describe_holdings, load_holdings,
)
from crawl.local_ai_summary.prompts import SYSTEM_PROMPT, build_user_prompt
from crawl.local_ai_summary.targets import (
    MAX_INPUT_CHARS, MIN_NATIVE_SUMMARY_LEN, SUMMARY_SCHEMA_VERSION, TARGETS, Target,
)


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("local_ai_summary")


# ─── Env / config ─────────────────────────────────────────────────────────

def _load_env_from_dotenv() -> None:
    """Best-effort .env load. We don't want to depend on python-dotenv —
    just parse KEY=VALUE for the few vars we need."""
    env_file = Path(__file__).resolve().parent.parent.parent / ".env"
    if not env_file.exists():
        return
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        k = k.strip()
        v = v.strip().strip('"').strip("'")
        # Don't overwrite anything already set via shell env.
        os.environ.setdefault(k, v)


_load_env_from_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27018")
LLM_API_KEY = os.getenv("LLM_ENRICHMENT_API_KEY", "")
LLM_BASE_URL = os.getenv("LLM_ENRICHMENT_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1")
LLM_MODEL = os.getenv("LLM_ENRICHMENT_MODEL", "qwen-plus")


# ─── Doc / field helpers ──────────────────────────────────────────────────

def _pick_nested(d: dict, dotted: str) -> Any:
    cur: Any = d
    for k in dotted.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def _has_usable_native_summary(doc: dict, target: Target) -> bool:
    for field in target.native_summary_fields:
        v = _pick_nested(doc, field)
        if isinstance(v, str) and len(v.strip()) >= MIN_NATIVE_SUMMARY_LEN:
            return True
    return False


def _pick_body(doc: dict, target: Target) -> tuple[str, str]:
    """Return (text, source_field). Empty string if no body found."""
    for field in target.body_fields:
        v = _pick_nested(doc, field)
        if isinstance(v, str) and v.strip():
            return v.strip()[:MAX_INPUT_CHARS], field
    return "", ""


# Simple disclaimer / sales-header trim — the LLM is instructed to skip these,
# but pre-trimming saves tokens when the noise is at the very top of the body.
_SALES_NOISE_PATTERNS = [
    r"本报告为销售产品[，,].*?(?=\n\n|\Z)",
    r"本文不属于摩根大通股票研究部的产品.*?(?=\n\n|\Z)",
    r"^【.*?投资案例.*?】",
]
_SALES_RE = re.compile("|".join(_SALES_NOISE_PATTERNS), re.DOTALL | re.MULTILINE)


def _pre_trim(text: str) -> str:
    """Drop very-likely sales/disclaimer prefix blocks. Conservative: only
    matches well-known patterns, never strips actual content."""
    return _SALES_RE.sub("", text).strip()


# ─── LLM call ─────────────────────────────────────────────────────────────

class LLMError(RuntimeError):
    pass


def call_qwen_plus(*, title: str, source_label: str, body: str,
                   timeout: int = 60) -> dict:
    """Sync call to qwen-plus via DashScope OpenAI-compatible endpoint.

    Returns the parsed dict {"tldr": ..., "bullets": [...]} or raises LLMError.
    """
    if not LLM_API_KEY:
        raise LLMError("LLM_ENRICHMENT_API_KEY is empty — set it in .env")
    if not body.strip():
        raise LLMError("empty body")

    user_text = build_user_prompt(
        title=title or "",
        source_label=source_label or "",
        body=_pre_trim(body),
    )

    payload = {
        "model": LLM_MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_text},
        ],
        "temperature": 0.2,
        "max_tokens": 600,
        "response_format": {"type": "json_object"},
    }
    headers = {
        "Authorization": f"Bearer {LLM_API_KEY}",
        "Content-Type": "application/json",
    }
    # Bypass Clash. DashScope is a CN endpoint that doesn't need (and breaks
    # under) Clash's 7890 proxy.
    proxies = {"http": "", "https": ""}

    url = LLM_BASE_URL.rstrip("/") + "/chat/completions"
    r = requests.post(url, json=payload, headers=headers, proxies=proxies,
                      timeout=timeout)
    if r.status_code != 200:
        raise LLMError(f"http_{r.status_code}: {r.text[:200]}")
    try:
        data = r.json()
        content = data["choices"][0]["message"]["content"]
    except (KeyError, ValueError) as e:
        raise LLMError(f"bad response shape: {e}: {r.text[:200]}")

    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        # qwen sometimes wraps the JSON in ```json ... ``` fences
        m = re.search(r"\{.*\}", content, re.DOTALL)
        if not m:
            raise LLMError(f"no JSON in response: {content[:200]}")
        parsed = json.loads(m.group(0))

    tldr = (parsed.get("tldr") or "").strip()
    bullets = parsed.get("bullets") or []
    if not isinstance(bullets, list):
        bullets = []
    bullets = [str(b).strip() for b in bullets if str(b).strip()]
    raw_sent = parsed.get("sentiment")
    sentiment = raw_sent if raw_sent in ("bullish", "bearish", "neutral") else "neutral"
    return {"tldr": tldr, "bullets": bullets[:5], "sentiment": sentiment}


# ─── Mongo passes ─────────────────────────────────────────────────────────

def _build_query(holdings: set[str], target: Target,
                 *, since_ms: int | None,
                 force_resummarize: bool = False) -> dict:
    q: dict = {
        "_canonical_tickers": {"$in": sorted(holdings)},
    }
    if since_ms is not None:
        # Match docs whose release OR crawl time is within window. Catches
        # backfilled-historical docs (e.g. quarterly earnings released long
        # ago but only crawled this week) that StockHub still surfaces in the
        # "近 3 月" feed. Mongo OR over both fields uses each field's index
        # individually (index union).
        since_dt = datetime.fromtimestamp(since_ms / 1000, tz=timezone.utc)
        q["$and"] = [{"$or": [
            {target.time_ms_field: {"$gte": since_ms}},
            {"crawled_at": {"$gte": since_dt}},
        ]}]
    if not force_resummarize:
        # Skip docs that already have a current-version summary. Use $and
        # composition so this doesn't collide with the time-window $or.
        version_or = [
            {"local_ai_summary": {"$exists": False}},
            {"local_ai_summary.v": {"$lt": SUMMARY_SCHEMA_VERSION}},
        ]
        if "$and" in q:
            q["$and"].append({"$or": version_or})
        else:
            q["$or"] = version_or
    return q


def _process_collection(
    col: Collection,
    target: Target,
    holdings: set[str],
    *,
    since_ms: int | None,
    max_calls: int,
    force_resummarize: bool,
    dry_run: bool,
) -> dict:
    """Process one (db,collection). Returns counters."""
    stats = {"scanned": 0, "skipped_native": 0, "skipped_empty": 0,
             "summarized": 0, "errors": 0, "remaining_budget": max_calls}
    if max_calls <= 0:
        return stats

    q = _build_query(holdings, target, since_ms=since_ms,
                     force_resummarize=force_resummarize)

    # Project only what we need — these collections can have huge raw payloads
    # (PDFs in detail_result, list_item.htmlContent, etc.).
    projection: dict[str, int] = {
        "_id": 1, "title": 1,
        "_canonical_tickers": 1,
        target.time_ms_field: 1,
        "local_ai_summary": 1,
    }
    for f in target.native_summary_fields:
        projection[f.split(".")[0]] = 1
    for f in target.body_fields:
        projection[f.split(".")[0]] = 1

    try:
        cur = col.find(q, projection).sort(target.time_ms_field, -1).limit(max_calls * 4)
    except Exception as e:
        logger.warning("[%s.%s] query failed: %s", target.db, target.collection, e)
        stats["errors"] += 1
        return stats

    for doc in cur:
        stats["scanned"] += 1
        if max_calls <= 0:
            break

        if _has_usable_native_summary(doc, target):
            stats["skipped_native"] += 1
            continue

        body, source_field = _pick_body(doc, target)
        if not body or len(body) < target.min_body_chars:
            # Body is short enough that the StockHub card preview (clamped at
            # 320 chars) already shows essentially all of it — re-summarizing
            # would just paraphrase what the user already reads. Skip.
            stats["skipped_empty"] += 1
            continue

        title = (doc.get("title") or "").strip()[:200]

        if dry_run:
            logger.info("[dry] %s.%s _id=%s tk=%s body=%dch field=%s title=%s",
                        target.db, target.collection, str(doc.get("_id"))[:18],
                        doc.get("_canonical_tickers"), len(body), source_field,
                        title[:50])
            stats["summarized"] += 1
            max_calls -= 1
            continue

        try:
            out = call_qwen_plus(
                title=title,
                source_label=target.label,
                body=body,
            )
        except LLMError as e:
            logger.warning("[%s.%s _id=%s] llm err: %s", target.db, target.collection,
                           str(doc.get("_id"))[:18], e)
            stats["errors"] += 1
            # Still consume budget so a stuck endpoint doesn't keep hammering.
            max_calls -= 1
            time.sleep(1.0)
            continue

        summary = {
            "tldr": out.get("tldr") or "",
            "bullets": out.get("bullets") or [],
            "sentiment": out.get("sentiment") or "neutral",
            "generated_at": datetime.now(timezone.utc),
            "model": LLM_MODEL,
            "source_field": source_field,
            "input_chars": len(body),
            "v": SUMMARY_SCHEMA_VERSION,
        }
        try:
            col.update_one(
                {"_id": doc["_id"]},
                {"$set": {"local_ai_summary": summary}},
            )
            stats["summarized"] += 1
            if stats["summarized"] <= 3 or stats["summarized"] % 25 == 0:
                logger.info("[%s.%s] +%d  tldr=%s",
                            target.db, target.collection, stats["summarized"],
                            (summary["tldr"] or "(empty)")[:80])
        except Exception as e:
            logger.warning("[%s.%s] mongo update fail: %s", target.db,
                           target.collection, e)
            stats["errors"] += 1

        max_calls -= 1
        # Pacing — DashScope qwen-plus default QPS is generous; a tight loop
        # is fine. The 0.1-0.2s jitter just smooths bursts.
        time.sleep(0.1 + random.uniform(0, 0.1))

    stats["remaining_budget"] = max_calls
    return stats


# ─── Main loop ────────────────────────────────────────────────────────────

def _ms_now() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _ms_n_days_ago(days: float) -> int:
    return _ms_now() - int(days * 86400 * 1000)


_SHUTDOWN = False


def _install_signal_handlers() -> None:
    def _h(signum, frame):
        global _SHUTDOWN
        logger.info("signal %s — shutting down after current pass", signum)
        _SHUTDOWN = True
    signal.signal(signal.SIGTERM, _h)
    signal.signal(signal.SIGINT, _h)


def run_once(*, since_days: float, max_calls: int, force_resummarize: bool,
             dry_run: bool, only: str | None) -> dict:
    holdings = load_holdings()
    if not holdings:
        logger.warning("no portfolio holdings resolved — aborting cycle")
        return {"holdings": 0}
    logger.info("[holdings] %s", describe_holdings(holdings))

    since_ms = _ms_n_days_ago(since_days) if since_days > 0 else None
    if since_ms is not None:
        logger.info("[window] since=%s ms (%.1f days)",
                    datetime.fromtimestamp(since_ms / 1000, tz=timezone.utc),
                    since_days)

    mc = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    totals = {"scanned": 0, "skipped_native": 0, "skipped_empty": 0,
              "summarized": 0, "errors": 0}

    # Resolve targets that actually have a live collection on this Mongo.
    runnable: list[tuple[Target, Collection]] = []
    for target in TARGETS:
        if only and f"{target.db}.{target.collection}" != only:
            continue
        try:
            db = mc[target.db]
            if target.collection not in db.list_collection_names():
                continue
            runnable.append((target, db[target.collection]))
        except Exception as e:
            logger.warning("[%s.%s] connect/list fail: %s",
                           target.db, target.collection, e)
            continue

    # Round-robin budget so a backlog-heavy target (e.g. alphapai-full.reports
    # at 1k+ pending) cannot starve later targets (funda.earnings_reports) of
    # cycle budget. Per-target cap = max(4, max_calls / n_targets); the second
    # pass below redistributes any unused budget back to hungry targets.
    n_targets = max(1, len(runnable))
    per_target_cap = max(4, max_calls // n_targets)
    remaining_budget = max_calls
    hungry: list[tuple[Target, Collection]] = []

    for target, col in runnable:
        if _SHUTDOWN:
            break
        if remaining_budget <= 0:
            logger.info("[budget] exhausted — stopping at %s", target.label)
            break
        target_budget = min(per_target_cap, remaining_budget)
        if target_budget <= 0:
            break

        s = _process_collection(
            col, target, holdings,
            since_ms=since_ms,
            max_calls=target_budget,
            force_resummarize=force_resummarize,
            dry_run=dry_run,
        )
        spent = target_budget - s["remaining_budget"]
        remaining_budget -= spent
        for k in totals:
            totals[k] += s.get(k, 0)
        logger.info("[%s] scanned=%d native=%d empty=%d ai=%d err=%d "
                    "spent=%d remain=%d",
                    target.label, s["scanned"], s["skipped_native"],
                    s["skipped_empty"], s["summarized"], s["errors"],
                    spent, remaining_budget)
        # Target hit its cap — keep some budget for it on the redistribute pass.
        if s["remaining_budget"] == 0 and spent >= target_budget:
            hungry.append((target, col))

    # Second pass: drain any unused budget into targets that hit their cap.
    # Skipped during dry-run or when nothing was hungry.
    for target, col in hungry:
        if _SHUTDOWN:
            break
        if remaining_budget <= 0:
            break
        s = _process_collection(
            col, target, holdings,
            since_ms=since_ms,
            max_calls=remaining_budget,
            force_resummarize=force_resummarize,
            dry_run=dry_run,
        )
        spent = remaining_budget - s["remaining_budget"]
        remaining_budget = s["remaining_budget"]
        for k in totals:
            totals[k] += s.get(k, 0)
        if spent:
            logger.info("[%s · 2nd] +ai=%d  spent=%d remain=%d",
                        target.label, s["summarized"], spent, remaining_budget)

    logger.info("[cycle done] %s", totals)
    return totals


def run_watch(*, interval: int, since_days: float, per_cycle_max: int,
              dry_run: bool, only: str | None) -> None:
    _install_signal_handlers()
    logger.info("watch mode — interval=%ds per_cycle_max=%d since_days=%.1f",
                interval, per_cycle_max, since_days)
    while not _SHUTDOWN:
        cycle_start = time.time()
        try:
            run_once(
                since_days=since_days,
                max_calls=per_cycle_max,
                force_resummarize=False,
                dry_run=dry_run,
                only=only,
            )
        except Exception:
            logger.exception("cycle failed")
        elapsed = time.time() - cycle_start
        sleep_s = max(5, interval - elapsed)
        logger.info("[sleep] %.0fs until next cycle (cycle took %.1fs)",
                    sleep_s, elapsed)
        # Sleep in 1s ticks so SIGTERM is responsive.
        slept = 0.0
        while slept < sleep_s and not _SHUTDOWN:
            time.sleep(1)
            slept += 1


def main() -> None:
    p = argparse.ArgumentParser(
        description="qwen-plus card-preview summarizer for portfolio docs",
    )
    p.add_argument("--once", action="store_true",
                   help="single pass, then exit")
    p.add_argument("--watch", action="store_true",
                   help="loop forever (default if --once not set)")
    p.add_argument("--interval", type=int, default=600,
                   help="seconds between watch cycles (default 600 = 10min)")
    p.add_argument("--since-days", type=float, default=14.0,
                   help="lookback window in days (default 14; use 90 for backfill)")
    p.add_argument("--max", type=int, default=80,
                   help="max LLM calls per cycle (default 80)")
    p.add_argument("--force", action="store_true",
                   help="re-summarize even docs that already have local_ai_summary")
    p.add_argument("--dry-run", action="store_true",
                   help="log what would be summarized, no LLM calls / no Mongo writes")
    p.add_argument("--only", type=str, default=None,
                   help="restrict to one db.collection (e.g. alphapai-full.reports)")
    args = p.parse_args()

    if args.once:
        run_once(
            since_days=args.since_days,
            max_calls=args.max,
            force_resummarize=args.force,
            dry_run=args.dry_run,
            only=args.only,
        )
    else:
        run_watch(
            interval=args.interval,
            since_days=args.since_days,
            per_cycle_max=args.max,
            dry_run=args.dry_run,
            only=args.only,
        )


if __name__ == "__main__":
    main()
