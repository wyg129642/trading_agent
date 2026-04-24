"""Re-fetch every oversea_reports doc using the patched
json_oversea-research_preview endpoint. Writes homeOssPdfUrl PDFs into
`/home/ygwang/crawl_data/jinmen_pdfs/YYYY-MM/mndj_rtime_<N>.pdf`.

Skips the list scan entirely — iterates rids already in MongoDB. Designed to be
idempotent: if a doc already has a fresh pdf_local_path (mtime > patch time),
skip it. Ctrl-C safe (writes per-doc upserts).
"""
from __future__ import annotations
import argparse
import hashlib
import signal
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import scraper as jm  # noqa: E402
from pymongo import MongoClient
from antibot import (  # noqa: E402
    SessionDead, SoftCooldown,
    add_antibot_args, throttle_from_args, cap_from_args,
    add_backfill_args, backfill_session_from_args,
    budget_from_args, log_config_stamp,
    BackfillWindow, BackfillLock, BackfillCheckpointBackoff,
)

PLATFORM = "jinmen"
_should_stop = False


def _sig(*_):
    global _should_stop
    _should_stop = True
    print("\n[signal] stopping after current item…", flush=True)


signal.signal(signal.SIGINT, _sig)
signal.signal(signal.SIGTERM, _sig)


def _account_id_from_auth(auth: dict) -> str:
    uid = (auth or {}).get("uid") or "unknown"
    return f"u_{uid}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max", type=int, default=0,
                    help="max items to process (0 = all). 注: --account-budget 兜底.")
    ap.add_argument("--pdf-dir", default=str(jm.PDF_DIR_DEFAULT))
    ap.add_argument("--sleep", type=float, default=0,
                    help="DEPRECATED — 用 --throttle-base 取代. 0=禁用 (走 antibot).")
    ap.add_argument("--only-missing", action="store_true",
                    help="skip rids that already have pdf_local_path on disk")
    add_antibot_args(ap, default_base=4.0, default_jitter=2.5,
                     default_burst=30, default_cap=300, platform=PLATFORM)
    add_backfill_args(ap, platform=PLATFORM)
    args = ap.parse_args()

    auth = jm.parse_auth(jm.JM_AUTH_INFO)

    # 1. Single-instance lock (role=oversea-pdf 区别于 jinmen scraper.py 的可能锁)
    if getattr(args, "bf_lock", True):
        if not BackfillLock.acquire(PLATFORM, role=args.bf_lock_role,
                                     force=args.bf_force_lock):
            print(f"[lock] {PLATFORM}:{args.bf_lock_role} 已被占用. "
                  f"用 --bf-force-lock 强制夺锁, --bf-no-lock 禁用.")
            return

    # 2. Working-hours window
    if getattr(args, "backfill_window", True):
        BackfillWindow.wait_until_allowed(PLATFORM)

    # 3. Antibot stack
    throttle = throttle_from_args(args, platform=PLATFORM)
    cap = cap_from_args(args)
    acct_id = _account_id_from_auth(auth)
    bg_budget = budget_from_args(args, account_id=acct_id, platform=PLATFORM,
                                  role="bg")
    bf_session = backfill_session_from_args(args, platform=PLATFORM)
    if hasattr(jm, "_THROTTLE"):
        jm._THROTTLE = throttle
    if hasattr(jm, "_BUDGET"):
        jm._BUDGET = bg_budget
    warm_up = getattr(args, "bf_warm_up", 30)
    if warm_up > 0:
        BackfillCheckpointBackoff(throttle, warm_up=warm_up, factor=3.0).arm()
    log_config_stamp(throttle, cap=cap, budget=bg_budget,
                     bf_session=bf_session, bf_window_platform=PLATFORM,
                     extra=f"acct={acct_id} role=oversea_pdf_backfill")

    sess = jm.create_session(auth)

    cli = MongoClient(jm.MONGO_URI_DEFAULT)
    db = cli[jm.MONGO_DB_DEFAULT]
    col = db[jm.COL_OVERSEA_REPORTS]

    pdf_dir = Path(args.pdf_dir)
    pdf_dir.mkdir(parents=True, exist_ok=True)

    q = {}
    if args.only_missing:
        q = {"$or": [{"pdf_local_path": {"$in": [None, ""]}},
                     {"pdf_size_bytes": {"$lte": 0}}]}
    cur = col.find(q, {"_id": 1, "list_item": 1, "title": 1}).sort("release_time_ms", -1)
    total = col.count_documents(q)
    print(f"[backfill] candidates={total}  pdf_dir={pdf_dir}")

    processed = pdf_ok = pdf_fail = err = 0
    t0 = time.time()
    limit = args.max or total
    try:
        for i, doc in enumerate(cur):
            if _should_stop or processed >= limit:
                break
            if cap.exhausted():
                print(f"  [antibot] daily-cap {cap.max_items} 到, 停")
                break
            if bg_budget.exhausted():
                st = bg_budget.status()
                print(f"  [antibot] bg budget exhausted ({st['used_24h']}/{st['limit']} "
                      f"OR rt sibling >= floor {st['floor_pct']}%, "
                      f"rt_used={st['rt_sibling_used']}), 让位 realtime")
                break
            # Working-hours window 中途变化检查
            if getattr(args, "backfill_window", True):
                secs = BackfillWindow.seconds_until_allowed(PLATFORM)
                if secs > 0:
                    print(f"  [backfill-window] 窗口已关闭, 剩 {secs/3600:.1f}h, 停")
                    break
            # Soft cooldown
            SoftCooldown.wait_if_active(PLATFORM, verbose=False)
            # Lock heartbeat
            BackfillLock.heartbeat(PLATFORM, role=args.bf_lock_role)

            rid = doc["_id"]
            item = doc.get("list_item") or {}
            if not item:
                item = {"id": rid}
            try:
                row = jm.dump_oversea_report(sess, item, db, pdf_dir=pdf_dir,
                                             download_pdf=True, force=True)
                status = row.get("状态")
                pdf_bytes = row.get("pdf_大小", 0) or 0
                if pdf_bytes > 0:
                    pdf_ok += 1
                else:
                    pdf_fail += 1
                processed += 1
                elapsed = time.time() - t0
                rate = processed / max(elapsed, 1)
                eta = (limit - processed) / max(rate, 0.01)
                short_title = (doc.get("title") or "")[:50]
                print(f"  [{processed:4d}/{limit}] rid={rid} {short_title}  "
                      f"status={status}  pdf={pdf_bytes:,}B  "
                      f"(rate={rate:.2f}/s, eta={eta:.0f}s)")
            except SessionDead as exc:
                print(f"  SessionDead — token 失效, abort: {exc}")
                return
            except Exception as exc:
                err += 1
                processed += 1
                print(f"  [{processed:4d}/{limit}] rid={rid}  ERR {exc}")

            cap.bump(); bg_budget.bump()
            bf_session.step()                   # 每 50 条强制 5-15min idle
            throttle.sleep_before_next()        # gauss + tod + soft cd

        elapsed = time.time() - t0
        print(f"\n[backfill done] processed={processed}  pdf_ok={pdf_ok}  "
              f"pdf_fail={pdf_fail}  err={err}  elapsed={elapsed:.0f}s")
    finally:
        try:
            BackfillLock.release(PLATFORM, role=args.bf_lock_role)
        except Exception:
            pass


if __name__ == "__main__":
    main()
