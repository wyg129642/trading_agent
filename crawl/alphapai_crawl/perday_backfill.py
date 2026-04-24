#!/usr/bin/env python3
"""Per-day backfill for AlphaPai 研报.

Background:
  - Scraper's default strategy is "paginate the global feed / 10000 cap" and
    stop when top_id is seen. On heavy-volume days (2000+ reports), new
    items arrive faster than one round captures → coverage stays at ~40%.
  - The list/v2 endpoint silently ignores most filter fields, but DOES
    honor startDate+endDate. Per-day queries walk ~2000/day and show only
    the ~60/day permission-capped items. That closes the bulk of the gap.

Strategy:
  For each date in [start_date, today]:
    - POST reading/report/list/v2 with startDate=date, endDate=date
    - Paginate 100/page until list_len < pageSize
    - For each item, dedup-check via title+time hash
    - If new → call dump_one() which fetches detail + PDF
    - Respect existing AdaptiveThrottle

Usage:
  python3 perday_backfill.py --days 7               # last 7 days
  python3 perday_backfill.py --date 2026-04-21      # one day
  python3 perday_backfill.py --start 2026-04-15 --end 2026-04-22
  python3 perday_backfill.py --days 7 --no-pdf      # skip PDF download
  python3 perday_backfill.py --days 30 --dry-run    # count only, no writes
  python3 perday_backfill.py --watch --interval 300 # loop mode: sweep today every 5 min (safety net)
  python3 perday_backfill.py --watch --days-sliding 2 --interval 600  # sweep today+yesterday every 10 min
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Optional

for _k in ('http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY',
           'all_proxy', 'ALL_PROXY'):
    os.environ.pop(_k, None)

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

from pymongo import MongoClient  # noqa: E402
import scraper as _scraper  # noqa: E402
from scraper import (  # noqa: E402
    _load_token_from_file, _account_id_from_token, create_session, api_call,
    CATEGORIES, dump_one, PDF_DIR_DEFAULT, make_dedup_id,
    OK_CODE,
)
from antibot import (  # noqa: E402
    AdaptiveThrottle, SessionDead, SoftCooldown,
    add_antibot_args, throttle_from_args, cap_from_args,
    add_backfill_args, backfill_session_from_args,
    budget_from_args, log_config_stamp,
    BackfillWindow, BackfillLock, BackfillCheckpointBackoff,
    account_id_for_alphapai,
)

PLATFORM = "alphapai"
CST = timezone(timedelta(hours=8))
CAT_KEY = "report"
CFG = CATEGORIES[CAT_KEY]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("perday_backfill")


STOP = False
def _sigint(_sig, _frame):
    global STOP
    STOP = True
    log.warning("interrupted, will stop after current item")
signal.signal(signal.SIGINT, _sigint)
signal.signal(signal.SIGTERM, _sigint)


def fetch_day_paginated(sess, date_str: str, page_size: int = 100,
                         max_pages: int = 60) -> List[dict]:
    """Return all items for a single day by paginating startDate/endDate."""
    items = []
    for page in range(1, max_pages + 1):
        if STOP: break
        body = {
            "pageNum": page, "pageSize": page_size,
            "startDate": date_str, "endDate": date_str,
        }
        resp = api_call(sess, "POST", CFG["list_path"], json_body=body)
        if resp.get("code") != OK_CODE:
            log.warning("  page %d for %s: code=%s msg=%s",
                        page, date_str, resp.get("code"), resp.get("message"))
            break
        data = resp.get("data") or {}
        lst = data.get("list") or []
        if not lst:
            break
        items.extend(lst)
        total = data.get("total")
        if page == 1:
            log.info("  %s: total=%s", date_str, total)
        if len(lst) < page_size:
            break
    return items


def count_existing(db, items: List[dict]) -> tuple[int, List[dict]]:
    """Split items into (already_in_db, to_fetch) based on title+time dedup hash."""
    coll = db[CFG["collection"]]
    to_fetch = []
    skipped = 0
    for it in items:
        dedup = make_dedup_id(CAT_KEY, it, CFG)
        if coll.find_one({"_id": dedup}, {"_id": 1}):
            skipped += 1
        else:
            to_fetch.append(it)
    return skipped, to_fetch


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--days", type=int, default=None,
                    help="Last N days including today (e.g. 7)")
    ap.add_argument("--date", type=str, default=None,
                    help="Single date YYYY-MM-DD")
    ap.add_argument("--start", type=str, default=None,
                    help="Range start YYYY-MM-DD")
    ap.add_argument("--end", type=str, default=None,
                    help="Range end YYYY-MM-DD (inclusive)")
    ap.add_argument("--no-pdf", action="store_true",
                    help="Don't download PDF")
    ap.add_argument("--dry-run", action="store_true",
                    help="Just count: how many new items per day")
    ap.add_argument("--page-size", type=int, default=100)
    ap.add_argument("--max-items-per-day", type=int, default=None,
                    help="Cap new-item fetches per day (avoid quota hits)")
    ap.add_argument("--auth", type=str, default=None,
                    help="Override token")
    ap.add_argument("--mongo-uri", type=str,
                    default="mongodb://localhost:27017")
    ap.add_argument("--pdf-dir", type=str, default=PDF_DIR_DEFAULT)
    ap.add_argument("--watch", action="store_true",
                    help="Loop forever, sweeping today (or --days-sliding) every --interval seconds")
    ap.add_argument("--interval", type=int, default=300,
                    help="Seconds between watch sweeps (default 300)")
    ap.add_argument("--days-sliding", type=int, default=1,
                    help="In --watch mode, sweep last N days each tick (default 1 = today only)")
    add_antibot_args(ap, default_base=4.0, default_jitter=2.5,
                     default_burst=30, default_cap=400, platform=PLATFORM)
    add_backfill_args(ap, platform=PLATFORM)
    args = ap.parse_args()

    def resolve_dates() -> List[str]:
        # In watch mode, recompute each tick to handle date rollover.
        if args.watch:
            n = args.days_sliding
            return [(datetime.now(CST) - timedelta(days=i)).strftime("%Y-%m-%d")
                     for i in range(n)]
        if args.date:
            return [args.date]
        if args.start:
            s = datetime.strptime(args.start, "%Y-%m-%d")
            e = datetime.strptime(args.end or datetime.now(CST).strftime("%Y-%m-%d"),
                                   "%Y-%m-%d")
            out = []
            cur = e
            while cur >= s:
                out.append(cur.strftime("%Y-%m-%d"))
                cur -= timedelta(days=1)
            return out
        n = args.days or 7
        return [(datetime.now(CST) - timedelta(days=i)).strftime("%Y-%m-%d")
                 for i in range(n)]

    if not args.watch:
        dates = resolve_dates()
        log.info("backfill dates: %s", dates)

    token = args.auth or _load_token_from_file() or os.environ.get("JM_AUTH")
    if not token:
        log.error("no token")
        sys.exit(1)

    # Single-instance lock — perday_backfill 是 report 专用回填
    lock_role = "perday_backfill_report"
    if not args.dry_run and getattr(args, "bf_lock", True):
        if not BackfillLock.acquire(PLATFORM, role=lock_role,
                                     force=args.bf_force_lock):
            log.error("[lock] %s:%s 已被占用. 用 --bf-force-lock 强制夺锁.",
                      PLATFORM, lock_role)
            sys.exit(0)

    # Backfill window
    if not args.dry_run and getattr(args, "backfill_window", True):
        BackfillWindow.wait_until_allowed(PLATFORM)

    # Antibot stack
    base_acct = _account_id_from_token(token)
    acct_id = account_id_for_alphapai(base_acct, "report")
    throttle = throttle_from_args(args, platform=PLATFORM)
    cap = cap_from_args(args)
    bg_budget = budget_from_args(args, account_id=acct_id, platform=PLATFORM,
                                  role="bg")
    bf_session = backfill_session_from_args(args, platform=PLATFORM)
    if hasattr(_scraper, "_THROTTLE"):
        _scraper._THROTTLE = throttle
    if hasattr(_scraper, "_BUDGET"):
        _scraper._BUDGET = bg_budget
    warm_up = getattr(args, "bf_warm_up", 30)
    if warm_up > 0:
        BackfillCheckpointBackoff(throttle, warm_up=warm_up, factor=3.0).arm()
    log_config_stamp(throttle, cap=cap, budget=bg_budget,
                     bf_session=bf_session, bf_window_platform=PLATFORM,
                     extra=f"acct={acct_id} role=perday_backfill")

    sess = create_session(token)

    mc = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=5000)
    db = mc["alphapai"]
    # sanity check
    db.command("ping")

    pdf_dir = Path(args.pdf_dir) if not args.no_pdf else None
    if pdf_dir:
        pdf_dir.mkdir(parents=True, exist_ok=True)

    def run_once(dates: List[str]) -> tuple[int, int, int]:
        grand_added = 0
        grand_skipped = 0
        grand_failed = 0
        for d in dates:
            if STOP: break
            log.info("=" * 60)
            log.info("DAY %s", d)
            try:
                items = fetch_day_paginated(sess, d, page_size=args.page_size)
            except Exception as e:
                log.error("  fetch failed: %s", e)
                continue
            log.info("  fetched: %d items from API", len(items))
            skipped, to_fetch = count_existing(db, items)
            log.info("  already in DB: %d ; new to fetch: %d", skipped, len(to_fetch))
            grand_skipped += skipped

            if args.dry_run:
                continue

            # Cap per day
            if args.max_items_per_day and len(to_fetch) > args.max_items_per_day:
                log.info("  capping at %d (skipping %d)",
                         args.max_items_per_day,
                         len(to_fetch) - args.max_items_per_day)
                to_fetch = to_fetch[:args.max_items_per_day]

            # Fetch detail + PDF for new ones
            added = 0
            failed = 0
            t0 = time.time()
            for i, it in enumerate(to_fetch):
                if STOP: break
                if cap.exhausted():
                    log.info("    [antibot] daily-cap %d 到, 停", cap.max_items); break
                if bg_budget.exhausted():
                    st = bg_budget.status()
                    log.info("    [antibot] bg budget exhausted (%d/%d OR rt floor>=%d%%, "
                             "rt_used=%s), 让位 realtime",
                             st['used_24h'], st['limit'], st['floor_pct'],
                             st['rt_sibling_used'])
                    break
                if getattr(args, "backfill_window", True):
                    if BackfillWindow.seconds_until_allowed(PLATFORM) > 0:
                        log.info("    [backfill-window] 窗口已关闭, 停"); break
                SoftCooldown.wait_if_active(PLATFORM, verbose=False)
                BackfillLock.heartbeat(PLATFORM, role=lock_role)

                title = (it.get("title") or "")[:60]
                try:
                    status, meta = dump_one(
                        sess, db, CAT_KEY, CFG, it,
                        force=False, pdf_dir=pdf_dir,
                        download_pdf=not args.no_pdf, token=token,
                    )
                    if status == "added":
                        added += 1
                        content_len = meta.get("content_len", 0)
                        pdf_size = meta.get("pdf_size", 0)
                        if (i + 1) % 10 == 0 or i < 3:
                            log.info("    [%d/%d] +%d %s content=%d pdf=%d",
                                     i + 1, len(to_fetch), added,
                                     title, content_len, pdf_size)
                    elif status == "failed":
                        failed += 1
                except SessionDead as e:
                    log.error("    SessionDead: %s — abort", e)
                    return grand_added, grand_skipped, grand_failed
                except Exception as e:
                    failed += 1
                    log.warning("    item failed: %s  %s", title, e)
                cap.bump(); bg_budget.bump()
                bf_session.step()
                throttle.sleep_before_next()
            bf_session.page_done()              # 每天切换时 30-90s 间隔
            elapsed = time.time() - t0
            log.info("  day %s done: added=%d skipped_existing=%d failed=%d (%.1fs)",
                     d, added, skipped, failed, elapsed)
            grand_added += added
            grand_failed += failed
        return grand_added, grand_skipped, grand_failed

    try:
        if args.watch:
            log.info("WATCH MODE: sweep last %d day(s) every %ds",
                     args.days_sliding, args.interval)
            tick = 0
            while not STOP:
                tick += 1
                dates = resolve_dates()
                log.info("=" * 60)
                log.info(">>> TICK %d  dates=%s", tick, dates)
                a, s, f = run_once(dates)
                log.info("<<< TICK %d DONE: added=%d skipped_existing=%d failed=%d",
                         tick, a, s, f)
                # 每 tick 之间也检查 backfill window (跨天可能落到工时段)
                if getattr(args, "backfill_window", True):
                    BackfillWindow.wait_until_allowed(PLATFORM)
                # sleep with early-exit on STOP
                end = time.time() + args.interval
                while not STOP and time.time() < end:
                    time.sleep(min(2.0, end - time.time()))
        else:
            dates = resolve_dates()
            a, s, f = run_once(dates)
            log.info("=" * 60)
            log.info("TOTAL: added=%d skipped_existing=%d failed=%d", a, s, f)
    finally:
        try:
            BackfillLock.release(PLATFORM, role=lock_role)
        except Exception:
            pass


if __name__ == "__main__":
    main()
