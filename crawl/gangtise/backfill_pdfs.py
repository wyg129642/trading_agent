#!/usr/bin/env python3
"""Backfill Gangtise research PDFs whose ``file`` field was null at list time.

Why this exists
---------------
The Gangtise list endpoint
``/application/glory/research/v2/queryByCondition`` returns ``file=null`` for
roughly the first few minutes after a research item is published — the
platform fills that field asynchronously once PDF processing completes.
Our scraper stores the record immediately, so ``pdf_rel_path`` lands as an
empty string and the dedup path in ``dump_research`` never re-examines the
entry (it only re-pulls when ``pdf_size_bytes=0`` AND someone happens to hit
the same record via the list endpoint again — but the watcher only tails
page=1, so historical backlog never gets refreshed).

What this does
--------------
1. Finds all ``researches`` records with ``pdf_size_bytes=0`` and a
   ``pdf_download_error`` that is NOT ``"external_url"`` (the WeChat-link
   ones are not platform-hosted so we skip them).
2. Batch-queries the list endpoint with ``rptIds=[...]`` (up to ~50 per
   call) to discover the now-populated ``file`` paths.
3. For each new ``file`` path, downloads the PDF via the existing
   ``download_research_pdf`` helper.
4. Updates Mongo in place with ``pdf_rel_path / pdf_local_path /
   pdf_size_bytes / pdf_download_error``.

Usage::

    PYTHONPATH=/home/ygwang/trading_agent \
        python3 crawl/gangtise/backfill_pdfs.py           # full backfill
    python3 crawl/gangtise/backfill_pdfs.py --max 50     # cap
    python3 crawl/gangtise/backfill_pdfs.py --dry-run    # inspect only
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Strip proxy env so localhost Mongo + Gangtise CDN stay direct.
for _k in list(os.environ):
    if "proxy" in _k.lower():
        os.environ.pop(_k, None)

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))
sys.path.insert(0, str(_HERE.parent))

from pymongo import MongoClient  # noqa: E402
from scraper import (  # noqa: E402
    API_BASE,
    PDF_DIR_DEFAULT,
    _load_token_from_file,
    _ms_to_str,
    _safe_filename,
    api_call,
    create_session,
    download_research_pdf,
)
# Full antibot v2 + backfill v1 stack
from antibot import (  # noqa: E402
    AdaptiveThrottle, DailyCap, SessionDead,
    AccountBudget, SoftCooldown,
    add_antibot_args, throttle_from_args, cap_from_args,
    add_backfill_args, backfill_session_from_args,
    budget_from_args, log_config_stamp,
    BackfillWindow, BackfillLock, BackfillCheckpointBackoff,
)
import scraper as _scraper  # so we can hot-swap _THROTTLE / _BUDGET in scraper module

LIST_PATH = "/application/glory/research/v2/queryByCondition"
PLATFORM = "gangtise"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--mongo-uri", default="mongodb://localhost:27017")
    p.add_argument("--mongo-db", default="gangtise")
    p.add_argument("--collection", default="researches")
    p.add_argument("--pdf-dir", default=PDF_DIR_DEFAULT)
    p.add_argument("--batch-size", type=int, default=50,
                   help="rptIds per list query")
    p.add_argument("--max", type=int, default=0,
                   help="Max PDFs to download per pass (0 = all). 注: --account-budget 兜底防跑飞.")
    p.add_argument("--dry-run", action="store_true",
                   help="Print what would happen; do not download/write")
    p.add_argument("--sleep", type=float, default=0,
                   help="DEPRECATED — 用 --throttle-base 取代. 0=禁用 (走 antibot Throttle)")
    p.add_argument("--loop", action="store_true",
                   help="Run in watcher mode: repeat forever, sleep --interval between passes")
    p.add_argument("--interval", type=int, default=600,
                   help="Seconds to sleep between passes in --loop mode (default 600 = 10min)")
    p.add_argument("--log-file",
                   default=str(_HERE.parent.parent / "logs" / "gangtise_pdf_backfill.log"))
    # Antibot v2: 节流 + 软冷却 + 账号预算 (走 bg 桶, 让位 realtime).
    add_antibot_args(p, default_base=4.0, default_jitter=2.5,
                     default_burst=30, default_cap=300, platform=PLATFORM)
    # Backfill v1: 工时禁跑 + 强制阅读停留 + 单实例锁.
    add_backfill_args(p, platform=PLATFORM)
    return p.parse_args()


def setup_logging(path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler(path, encoding="utf-8"),
                  logging.StreamHandler(sys.stdout)],
    )


def query_files_by_rpt_ids(session, rpt_ids: list[str]) -> dict[str, dict]:
    """Return {rpt_id: list_item_dict} for the subset of rpt_ids the API knows.

    Only items where ``file`` is populated get included in the result.
    """
    if not rpt_ids:
        return {}
    body = {
        "from": 0, "size": len(rpt_ids) + 10,
        "searchType": 1, "kw": "",
        "startDate": None, "endDate": None,
        "rptIds": rpt_ids,
        "industryList": [], "columnList": [],
        "orgList": [], "orgTypeList": [], "honorTypeList": [],
        "authorList": [], "rateList": [], "changeList": [],
        "source": [0, 1], "exactStockList": [],
        "realTime": None, "curated": None, "typeList": [],
    }
    r = api_call(session, "POST", LIST_PATH, json_body=body)
    data = r.get("data")
    records = data if isinstance(data, list) else (data or {}).get("records") or []
    out: dict[str, dict] = {}
    for rec in records:
        if not isinstance(rec, dict):
            continue
        rid = rec.get("rptId")
        if rid and rec.get("file"):
            out[str(rid)] = rec
    return out


def _account_id_from_token(token: str) -> str:
    """Same hash strategy scraper.py uses (G_token is a UUID, no embedded uid)."""
    import hashlib
    return "h_" + hashlib.md5((token or "").encode()).hexdigest()[:12]


def _setup_antibot(args, token: str):
    """Build throttle / bg-budget / session / lock and inject into scraper module
    so that download_research_pdf / api_call (which use scraper._THROTTLE)
    are also paced. Returns (throttle, bg_budget, bf_session, lock_acquired)."""
    throttle = throttle_from_args(args, platform=PLATFORM)
    cap = cap_from_args(args)
    acct_id = _account_id_from_token(token)
    bg_budget = budget_from_args(args, account_id=acct_id, platform=PLATFORM,
                                  role="bg")
    bf_session = backfill_session_from_args(args, platform=PLATFORM)

    # Hot-swap scraper module's _THROTTLE so api_call / download_research_pdf
    # also obey our backfill-tuned pacing (rather than scraper's default 3s).
    if hasattr(_scraper, "_THROTTLE"):
        _scraper._THROTTLE = throttle

    # Apply checkpoint-resume slow-start (前 30 条 ×3 节奏)
    warm_up = getattr(args, "bf_warm_up", 30)
    if warm_up > 0:
        BackfillCheckpointBackoff(throttle, warm_up=warm_up, factor=3.0).arm()

    log_config_stamp(throttle, cap=cap, budget=bg_budget,
                     bf_session=bf_session, bf_window_platform=PLATFORM,
                     extra=f"acct={acct_id} role=PDF_backfill")
    return throttle, cap, bg_budget, bf_session


def main() -> None:
    args = parse_args()
    setup_logging(args.log_file)
    logging.info("=" * 70)
    logging.info("Gangtise research PDF backfill — START (dry_run=%s)", args.dry_run)

    # 1. 单实例锁 — 防 13 个 backfill_pdfs 同时跑
    if not args.dry_run:
        ok = BackfillLock.acquire(PLATFORM, role=args.bf_lock_role,
                                   force=args.bf_force_lock) \
             if getattr(args, "bf_lock", True) else True
        if not ok:
            logging.error("BackfillLock %s:%s 已被占用, 跳过本轮启动. "
                          "用 --bf-force-lock 强制夺锁, --bf-no-lock 禁用锁",
                          PLATFORM, args.bf_lock_role)
            return

    # 2. 强制工时禁跑 — 工作日 22:00-08:00 + 周末以外, 直接 sleep 到允许窗口
    if getattr(args, "backfill_window", True) and not args.dry_run:
        BackfillWindow.wait_until_allowed(PLATFORM)

    client = MongoClient(args.mongo_uri)
    col = client[args.mongo_db][args.collection]

    query = {
        "pdf_size_bytes": {"$lte": 0},
        "pdf_download_error": {"$ne": "external_url"},
    }
    candidates = list(col.find(
        query,
        {"rpt_id": 1, "title": 1, "release_time_ms": 1, "pdf_download_error": 1},
    ).sort("release_time_ms", -1))
    logging.info("candidates (pdf_size_bytes<=0, not external): %d", len(candidates))
    if not candidates:
        logging.info("nothing to backfill. exiting.")
        return

    token = _load_token_from_file()
    if not token:
        logging.error("no token found in credentials.json — aborting")
        return
    session = create_session(token)
    session.trust_env = False
    session.proxies = {}

    # 3. Antibot stack — throttle/bg-budget/session
    throttle, cap, bg_budget, bf_session = _setup_antibot(args, token)

    pdf_dir = Path(args.pdf_dir)
    pdf_dir.mkdir(parents=True, exist_ok=True)

    stats = {
        "candidates": len(candidates), "queried": 0, "resolved": 0,
        "downloaded": 0, "failed": 0, "still_null": 0, "skipped_dry": 0,
        "soft_cooldown_hits": 0, "budget_stops": 0, "window_blocks": 0,
    }

    # Batch query + download
    for start in range(0, len(candidates), args.batch_size):
        if args.max and stats["downloaded"] >= args.max:
            logging.info("hit --max %d, stopping.", args.max)
            break
        if cap.exhausted():
            logging.info("daily-cap %d 到, 本轮停", cap.max_items)
            stats["budget_stops"] += 1
            break
        if bg_budget.exhausted():
            st = bg_budget.status()
            logging.info("bg budget exhausted (%d/%d 用尽 OR rt sibling >= floor %d%%, "
                         "rt_used=%s), 让位 realtime, 本轮停",
                         st["used_24h"], st["limit"], st["floor_pct"],
                         st["rt_sibling_used"])
            stats["budget_stops"] += 1
            break

        # 每批前再次 check 工时窗口 (跑了几小时可能就窗口关了)
        if getattr(args, "backfill_window", True) and not args.dry_run:
            secs = BackfillWindow.seconds_until_allowed(PLATFORM)
            if secs > 0:
                stats["window_blocks"] += 1
                logging.info("backfill window 已关闭, 本轮停 (剩余等待 %.1fh)",
                             secs / 3600)
                break

        # Soft cooldown (任何 watcher 触发软警告就联动)
        rem = SoftCooldown.remaining(PLATFORM)
        if rem > 0:
            logging.warning("SoftCooldown %s 仍剩 %.0fs, 等待...", PLATFORM, rem)
            stats["soft_cooldown_hits"] += 1
            SoftCooldown.wait_if_active(PLATFORM, verbose=False)

        # Backfill lock heartbeat (TTL 30min, 续期防过期)
        if not args.dry_run:
            BackfillLock.heartbeat(PLATFORM, role=args.bf_lock_role)

        batch = candidates[start:start + args.batch_size]
        rpt_ids = [c["rpt_id"] for c in batch if c.get("rpt_id")]
        stats["queried"] += len(rpt_ids)

        try:
            resolved = query_files_by_rpt_ids(session, rpt_ids)
        except SessionDead as exc:
            logging.error("SessionDead — token 失效, abort: %s", exc)
            return
        except Exception as exc:  # noqa: BLE001
            logging.exception("list query failed (batch %d..%d): %s",
                              start, start + len(batch), exc)
            continue

        logging.info("batch %d..%d  queried=%d  resolved_with_file=%d",
                     start, start + len(batch), len(rpt_ids), len(resolved))

        # 一次 list query 算 1 次预算 (而非每个 rpt_id 算 1)
        bg_budget.bump(); cap.bump()

        for doc in batch:
            rid = doc.get("rpt_id")
            rec = resolved.get(str(rid)) if rid else None
            if not rec:
                stats["still_null"] += 1
                continue
            stats["resolved"] += 1
            rel_path = rec.get("file")
            ext = (rec.get("extension") or ".pdf").lower()
            if not rel_path or ext != ".pdf":
                stats["still_null"] += 1
                continue

            title = doc.get("title") or rid
            release_ms = doc.get("release_time_ms") or rec.get("pubTime") or 0
            release_time = _ms_to_str(release_ms) if release_ms else ""
            fname = _safe_filename(Path(rel_path).name or f"{rid}.pdf")
            ym = (release_time or "unknown")[:7] or "unknown"
            dest = pdf_dir / ym / fname

            if args.dry_run:
                logging.info("  DRY %s → %s  (rel=%s, size=%s)",
                             rid, dest, rel_path, rec.get("size"))
                stats["skipped_dry"] += 1
                continue

            if args.max and stats["downloaded"] >= args.max:
                break
            if cap.exhausted() or bg_budget.exhausted():
                break

            try:
                pdf_size, err = download_research_pdf(session, rel_path, token, dest)
            except SessionDead as exc:
                logging.error("SessionDead during PDF download: %s", exc)
                return
            except Exception as exc:  # noqa: BLE001
                logging.warning("  download exc %s: %s", rid, exc)
                stats["failed"] += 1
                throttle.sleep_before_next()
                continue

            if pdf_size and pdf_size > 0:
                col.update_one({"_id": doc["_id"]}, {"$set": {
                    "pdf_rel_path": rel_path,
                    "pdf_local_path": str(dest),
                    "pdf_size_bytes": pdf_size,
                    "pdf_download_error": "",
                    "_pdf_backfilled_at": datetime.now(timezone.utc),
                    "stats.pdf_size": pdf_size,
                }})
                stats["downloaded"] += 1
                logging.info("  ✓ %s  %s  (%d bytes)", rid, fname, pdf_size)
            else:
                stats["failed"] += 1
                # Persist the error so we don't retry in a tight loop.
                col.update_one({"_id": doc["_id"]}, {"$set": {
                    "pdf_download_error": err or "unknown",
                    "_pdf_backfill_attempt_at": datetime.now(timezone.utc),
                }})
                logging.warning("  ✗ %s  %s  err=%s", rid, fname, err)

            cap.bump(); bg_budget.bump()
            bf_session.step()                       # 每 50 条强制 5-15min idle
            throttle.sleep_before_next()            # gauss + tod + soft cd

        bf_session.page_done()                      # 每批之间 30-90s 间隔

    logging.info("=" * 70)
    logging.info("BACKFILL COMPLETE")
    for k, v in stats.items():
        logging.info("  %-22s %d", k, v)


def run_loop() -> None:
    """--loop 模式: 每 --interval 秒跑一次 main(), 无限重复.
    main() 会自己退出每一轮 (不抓到候选时 return early, 否则下完 --max 条后 return).
    Loop 退出时显式释放 BackfillLock 防卡死.
    """
    args = parse_args()
    round_no = 0
    try:
        while True:
            round_no += 1
            logging.info("===== PDF backfill loop round %d =====", round_no)
            try:
                main()
            except KeyboardInterrupt:
                logging.info("interrupted, exiting loop.")
                return
            except Exception as exc:  # noqa: BLE001
                logging.exception("round %d failed: %s", round_no, exc)
            logging.info("sleeping %ds before next round", args.interval)
            time.sleep(args.interval)
    finally:
        # Lock release 让其他 backfill 进程可以接手
        try:
            BackfillLock.release(PLATFORM, role=args.bf_lock_role)
            logging.info("released BackfillLock %s:%s on loop exit",
                         PLATFORM, args.bf_lock_role)
        except Exception:
            pass


if __name__ == "__main__":
    args = parse_args()
    try:
        if args.loop:
            setup_logging(args.log_file)
            run_loop()
        else:
            main()
    finally:
        # 单次模式也释放锁
        if not args.loop:
            try:
                BackfillLock.release(PLATFORM, role=args.bf_lock_role)
            except Exception:
                pass
