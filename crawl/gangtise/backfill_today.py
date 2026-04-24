"""
一次性回填脚本: 精确定位今日缺失的 summary / research / chief 条目,
对每条独立 force 抓取 detail, bypass watcher 的 resume-top-id 盲点.

用法:
    cd crawl/gangtise
    python3 backfill_today.py --type research
    python3 backfill_today.py --type summary --date 2026-04-23
    python3 backfill_today.py --type all --dry-run
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timedelta, timezone

_BJ_TZ = timezone(timedelta(hours=8))
from pathlib import Path

for k in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"):
    os.environ.pop(k, None)
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import scraper as _scraper  # noqa: E402
from scraper import (  # noqa: E402
    CHIEF_VARIANTS, MONGO_DB_DEFAULT, MONGO_URI_DEFAULT, PDF_DIR_DEFAULT,
    SUMMARY_CLASSIFIES, _items_from_list_resp, _item_time_ms,
    connect_mongo, create_session, dedup_id_chief, dedup_id_research,
    dedup_id_summary, dump_chief, dump_research, dump_summary,
    fetch_chief_list, fetch_research_list, fetch_summary_list,
)
from scraper import _load_token_from_file  # noqa: E402
from antibot import (  # noqa: E402
    SessionDead, SoftCooldown,
    add_antibot_args, throttle_from_args, cap_from_args,
    add_backfill_args, backfill_session_from_args,
    budget_from_args, log_config_stamp,
    BackfillWindow, BackfillLock, BackfillCheckpointBackoff,
)
import hashlib  # noqa: E402

PLATFORM = "gangtise"


TYPE_MAP = {
    "summary":  {"col": "summaries",  "dedup": dedup_id_summary,  "dump": dump_summary,  "label": "纪要"},
    "research": {"col": "researches", "dedup": dedup_id_research, "dump": dump_research, "label": "研报"},
    "chief":    {"col": "chief_opinions", "dedup": dedup_id_chief, "dump": dump_chief,   "label": "首席"},
}


def scan_today(session, content_type: str, day_start_ms: int, day_end_ms: int,
               page_size: int = 100, max_pages: int = 50) -> list[dict]:
    """按 content_type 扫今日全部 items (跨 classify)."""
    # classify loop
    if content_type == "summary":
        classifies = [{"name": c["name"], "id": c["id"], "param": c["param"]} for c in SUMMARY_CLASSIFIES]
    elif content_type == "chief":
        classifies = [dict(v) for v in CHIEF_VARIANTS]
    else:
        classifies = [None]

    items_today: list[dict] = []
    seen = set()

    for classify in classifies:
        label = f"{content_type}/{classify['name']}" if classify else content_type
        page = 1
        stop = False
        while not stop and page <= max_pages:
            try:
                if content_type == "summary":
                    resp = fetch_summary_list(session, page, page_size,
                                              classify_param=classify["param"] if classify else None)
                elif content_type == "research":
                    resp = fetch_research_list(session, page, page_size)
                else:
                    resp = fetch_chief_list(session, page, page_size, variant=classify)
            except Exception as e:
                print(f"  [{label} p{page}] ERR: {e}")
                break
            items = _items_from_list_resp(resp, content_type)
            if not items:
                break
            for it in items:
                # classify id 注入, 用于 dump_summary 的 classify_id/classify_name
                if classify and content_type == "summary":
                    it["_classify_id"] = classify.get("id")
                    it["_classify_name"] = classify.get("name")
                if classify and content_type == "chief":
                    it["_variant_key"] = classify.get("key")
                    it["_variant_name"] = classify.get("name")
                ts = _item_time_ms(it, content_type)
                if ts is None:
                    continue
                if ts < day_start_ms:
                    stop = True
                    break
                if ts <= day_end_ms:
                    # 跨 classify dedup
                    key = _TYPE_INFO[content_type]["dedup"](it)
                    if key in seen:
                        continue
                    seen.add(key)
                    items_today.append(it)
            if len(items) < page_size:
                break
            page += 1
            time.sleep(0.5)
        print(f"  [{label}] 累计 今日 {len(items_today)} 条, 扫了 {page} 页")
    return items_today


_TYPE_INFO = TYPE_MAP  # alias


def run_one(session, db, args, content_type: str):
    info = TYPE_MAP[content_type]
    day_start = (datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=_BJ_TZ) if args.date
                 else datetime.now(_BJ_TZ).replace(hour=0, minute=0, second=0, microsecond=0))
    day_end = day_start.replace(hour=23, minute=59, second=59)
    day_start_ms = int(day_start.timestamp() * 1000)
    day_end_ms = int(day_end.timestamp() * 1000)
    print(f"\n════ {info['label']} ({content_type}) · {day_start:%Y-%m-%d} ════")

    items = scan_today(session, content_type, day_start_ms, day_end_ms,
                       page_size=args.page_size, max_pages=args.max_pages)
    print(f"[步骤 1] 扫到 today items: {len(items)}")

    col = db[info["col"]]
    ids = [info["dedup"](it) for it in items]
    in_db = set()
    if ids:
        for d in col.find({"_id": {"$in": ids}}, {"_id": 1}):
            in_db.add(d["_id"])
    missing = [(iid, it) for iid, it in zip(ids, items) if iid not in in_db]
    print(f"[步骤 2] 平台 {len(items)}  入库 {len(in_db)}  缺 {len(missing)}")

    if args.dry_run or not missing:
        for iid, it in missing[:20]:
            print(f"  · {iid[:16]}.. {it.get('title','')[:50]} "
                  f"pub={_item_time_ms(it, content_type)}")
        return

    print(f"[步骤 3] force 抓 {len(missing)} 条...")
    ok = fail = 0
    pdf_dir = Path(args.pdf_dir)
    token = args.auth or _load_token_from_file()
    cap = args._bf_cap
    bg_budget = args._bf_budget
    bf_session = args._bf_session
    throttle = args._bf_throttle
    lock_role = args._bf_lock_role
    for i, (iid, it) in enumerate(missing, 1):
        if cap.exhausted():
            print(f"  [antibot] daily-cap {cap.max_items} 到, 停"); break
        if bg_budget.exhausted():
            st = bg_budget.status()
            print(f"  [antibot] bg budget exhausted ({st['used_24h']}/{st['limit']}), 让位 realtime")
            break
        if getattr(args, "backfill_window", True):
            if BackfillWindow.seconds_until_allowed(PLATFORM) > 0:
                print(f"  [backfill-window] 窗口已关闭, 停"); break
        SoftCooldown.wait_if_active(PLATFORM, verbose=False)
        BackfillLock.heartbeat(PLATFORM, role=lock_role)

        title = (it.get("title") or "")[:60]
        try:
            if content_type == "research":
                status, stats = info["dump"](session, db, it, pdf_dir, token,
                                             download_pdf=not args.skip_pdf, force=True)
            elif content_type == "summary":
                status, stats = info["dump"](session, db, it, force=True, token=token)
            else:
                status, stats = info["dump"](session, db, it, force=True, token=token)
            ok += 1
            print(f"  [{i:3d}/{len(missing)}] {status}  {title}  "
                  f"chars={stats.get('content_chars',0)}")
        except SessionDead as e:
            print(f"  SessionDead — abort: {e}"); return
        except Exception as e:
            fail += 1
            print(f"  [{i:3d}/{len(missing)}] ✗  {title}  ERR: {e}")
        cap.bump(); bg_budget.bump()
        bf_session.step()
        throttle.sleep_before_next()

    bf_session.page_done()
    in_db_after = col.count_documents({"_id": {"$in": ids}}) if ids else 0
    print(f"\n[完成] 新增 {ok}  失败 {fail}  (DB: {len(in_db)} → {in_db_after} / 平台 {len(items)})")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--type", choices=["all", *TYPE_MAP.keys()], default="research")
    ap.add_argument("--date", metavar="YYYY-MM-DD", default=None)
    ap.add_argument("--page-size", type=int, default=100)
    ap.add_argument("--max-pages", type=int, default=50)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--skip-pdf", action="store_true")
    ap.add_argument("--mongo-uri", default=MONGO_URI_DEFAULT)
    ap.add_argument("--mongo-db", default=MONGO_DB_DEFAULT)
    ap.add_argument("--auth", default=None)
    ap.add_argument("--pdf-dir", default=PDF_DIR_DEFAULT)
    ap.add_argument("--throttle", type=float, default=0,
                    help="DEPRECATED — 走 antibot --throttle-base.")
    add_antibot_args(ap, default_base=4.0, default_jitter=2.5,
                     default_burst=30, default_cap=400, platform=PLATFORM)
    add_backfill_args(ap, platform=PLATFORM)
    args = ap.parse_args()

    token = args.auth or _load_token_from_file()

    # Single-instance lock — 区分 type
    lock_role = f"backfill_today_{args.type}"
    if not args.dry_run and getattr(args, "bf_lock", True):
        if not BackfillLock.acquire(PLATFORM, role=lock_role,
                                     force=args.bf_force_lock):
            print(f"[lock] {PLATFORM}:{lock_role} 已被占用. --bf-force-lock 强制夺锁")
            return

    if not args.dry_run and getattr(args, "backfill_window", True):
        BackfillWindow.wait_until_allowed(PLATFORM)

    # Antibot stack
    acct_id = "h_" + hashlib.md5((token or "").encode()).hexdigest()[:12]
    args._bf_throttle = throttle_from_args(args, platform=PLATFORM)
    args._bf_cap = cap_from_args(args)
    args._bf_budget = budget_from_args(args, account_id=acct_id, platform=PLATFORM,
                                        role="bg")
    args._bf_session = backfill_session_from_args(args, platform=PLATFORM)
    args._bf_lock_role = lock_role
    if hasattr(_scraper, "_THROTTLE"):
        _scraper._THROTTLE = args._bf_throttle
    if hasattr(_scraper, "_BUDGET"):
        _scraper._BUDGET = args._bf_budget
    warm_up = getattr(args, "bf_warm_up", 30)
    if warm_up > 0:
        BackfillCheckpointBackoff(args._bf_throttle, warm_up=warm_up,
                                   factor=3.0).arm()
    log_config_stamp(args._bf_throttle, cap=args._bf_cap, budget=args._bf_budget,
                     bf_session=args._bf_session, bf_window_platform=PLATFORM,
                     extra=f"acct={acct_id} role={lock_role}")

    sess = create_session(token)
    sess.trust_env = False
    sess.proxies = {"http": None, "https": None}
    db = connect_mongo(args.mongo_uri, args.mongo_db)

    types = list(TYPE_MAP.keys()) if args.type == "all" else [args.type]
    try:
        for t in types:
            try:
                run_one(sess, db, args, t)
            except Exception as e:
                print(f"[{t}] 失败: {e}")
    finally:
        try:
            BackfillLock.release(PLATFORM, role=lock_role)
        except Exception:
            pass


if __name__ == "__main__":
    main()
