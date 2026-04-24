"""
一次性回填脚本: 当 `--sweep-today` 的 watcher 跟不上时, 精确定位今日缺失
的 report / roadshow / comment / wechat IDs, 对每条独立 force 抓取 detail.

用法:
    cd crawl/alphapai_crawl
    python3 backfill_today_reports.py --category report
    python3 backfill_today_reports.py --category report --date 2026-04-23

逻辑:
    1. 用 list_extra_body={startDate, endDate} 扫今日全部平台 items (report 生效)
    2. 算 dedup_id, 查 mongo 哪些缺
    3. 对缺失的每条 force=True 调用 dump_one → 独立 detail 抓取, bypass watcher
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timedelta, timezone

_BJ_TZ = timezone(timedelta(hours=8))
from pathlib import Path

# 去代理 + sys.path
for k in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"):
    os.environ.pop(k, None)
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import scraper as _scraper  # noqa: E402
from scraper import (  # noqa: E402
    CATEGORIES, MONGO_URI_DEFAULT, MONGO_DB_DEFAULT, OK_CODE, PDF_DIR_DEFAULT,
    _load_token_from_file, _parse_time_to_dt, _extract_time_str,
    _account_id_from_token,
    api_call, connect_mongo, create_session, dump_one, fetch_list_page,
    make_dedup_id,
)
from antibot import (  # noqa: E402
    SessionDead, SoftCooldown,
    add_antibot_args, throttle_from_args, cap_from_args,
    add_backfill_args, backfill_session_from_args,
    budget_from_args, log_config_stamp,
    BackfillWindow, BackfillLock, BackfillCheckpointBackoff,
    account_id_for_alphapai,
)

PLATFORM = "alphapai"


def collect_today_items(session, cfg, day_start, page_size=100, max_pages=50):
    """用 startDate/endDate 扫今日 + 兜底翻页.

    day_start 以 CST (Asia/Shanghai) 为准; platform items 的 time 字段是 "YYYY-MM-DD HH:MM"
    的北京时间裸字符串, _parse_time_to_dt 返回 naive datetime. 比较时都走 naive-CST.
    """
    # strip tz so we can compare against naive platform datetimes (both as CST wall-clock)
    day_start_naive = day_start.replace(tzinfo=None) if day_start.tzinfo else day_start
    items_today = []
    seen = set()
    stop = False
    page = 1
    while not stop and page <= max_pages:
        resp = fetch_list_page(session, cfg, page, page_size)
        if resp.get("code") != OK_CODE:
            print(f"  [page {page}] code={resp.get('code')} msg={resp.get('message','')[:80]}")
            break
        items = (resp.get("data") or {}).get("list") or []
        if not items:
            break
        for it in items:
            iid = it.get("id")
            if iid in seen:
                continue
            seen.add(iid)
            dt = _parse_time_to_dt(_extract_time_str(it, cfg["time_field"]))
            if dt is None:
                continue
            if dt < day_start_naive:
                stop = True
                break
            if dt.date() == day_start_naive.date():
                items_today.append(it)
        if len(items) < page_size:
            break
        page += 1
        time.sleep(0.8)
    return items_today, page


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--category", choices=list(CATEGORIES.keys()), default="report")
    ap.add_argument("--date", metavar="YYYY-MM-DD", default=None)
    ap.add_argument("--page-size", type=int, default=100)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--skip-pdf", action="store_true")
    ap.add_argument("--mongo-uri", default=MONGO_URI_DEFAULT)
    ap.add_argument("--mongo-db", default=MONGO_DB_DEFAULT)
    ap.add_argument("--auth", default=None)
    ap.add_argument("--throttle", type=float, default=0,
                    help="DEPRECATED — 用 --throttle-base 取代. 0=禁用 (走 antibot).")
    add_antibot_args(ap, default_base=4.0, default_jitter=2.5,
                     default_burst=30, default_cap=400, platform=PLATFORM)
    add_backfill_args(ap, platform=PLATFORM)
    args = ap.parse_args()

    token = args.auth or _load_token_from_file()

    # Single-instance lock — alphapai 子模块各自一把锁 (role=backfill_today_<category>)
    lock_role = f"backfill_today_{args.category}"
    if not args.dry_run and getattr(args, "bf_lock", True):
        if not BackfillLock.acquire(PLATFORM, role=lock_role,
                                     force=args.bf_force_lock):
            print(f"[lock] {PLATFORM}:{lock_role} 已被占用. "
                  f"用 --bf-force-lock 强制夺锁.")
            return

    # Backfill window (alphapai = 22:00-08:00 工作日 + 周末)
    if not args.dry_run and getattr(args, "backfill_window", True):
        BackfillWindow.wait_until_allowed(PLATFORM)

    # Antibot stack
    base_acct = _account_id_from_token(token)
    # alphapai 子模块独立账号桶 — backfill 也按 category 分
    acct_id = account_id_for_alphapai(base_acct, args.category)
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
                     extra=f"acct={acct_id} role=backfill_today")

    sess = create_session(token)
    sess.trust_env = False
    sess.proxies = {"http": None, "https": None}

    db = connect_mongo(args.mongo_uri, args.mongo_db)
    cfg = CATEGORIES[args.category]
    col = db[cfg["collection"]]

    # 日期窗
    if args.date:
        day_start = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=_BJ_TZ)
    else:
        day_start = datetime.now(_BJ_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    date_str = day_start.strftime("%Y-%m-%d")
    print(f"[目标] category={args.category} date={date_str}")

    # report 专用: 注入 startDate/endDate 让平台只返回当日
    if args.category == "report":
        cfg["list_extra_body"] = {"startDate": date_str, "endDate": date_str}
        print(f"[sweep] 注入 startDate={date_str} endDate={date_str}")

    print("[步骤 1] 扫平台今日全部 items...")
    items, pages = collect_today_items(sess, cfg, day_start, args.page_size)
    print(f"  扫到 {len(items)} 条 (页数={pages})")

    # 对比 DB
    ids = [make_dedup_id(args.category, it, cfg) for it in items]
    in_db = set()
    if ids:
        for doc in col.find({"_id": {"$in": ids}}, {"_id": 1}):
            in_db.add(doc["_id"])

    missing_items = []
    for it, iid in zip(items, ids):
        if iid not in in_db:
            missing_items.append((iid, it))

    print(f"[步骤 2] 对比 DB: 平台 {len(items)}  已入库 {len(in_db)}  缺 {len(missing_items)}")

    if args.dry_run or not missing_items:
        for i, (iid, it) in enumerate(missing_items[:20], 1):
            print(f"  · [{i:3d}] {it.get('title','')[:60]}  id={iid[:16]} "
                  f"time={_extract_time_str(it, cfg['time_field'])}")
        if args.dry_run:
            print("(dry-run 结束, 未写入)")
            return
        print("(无缺失)")
        return

    print(f"[步骤 3] 强制重抓 {len(missing_items)} 条 detail...")
    pdf_dir = Path(args.__dict__.get("pdf_dir") or PDF_DIR_DEFAULT) if args.category == "report" else None
    ok = fail = 0
    try:
        for i, (iid, it) in enumerate(missing_items, 1):
            if cap.exhausted():
                print(f"  [antibot] daily-cap {cap.max_items} 到, 停"); break
            if bg_budget.exhausted():
                st = bg_budget.status()
                print(f"  [antibot] bg budget exhausted ({st['used_24h']}/{st['limit']} "
                      f"OR rt sibling >= floor {st['floor_pct']}%, "
                      f"rt_used={st['rt_sibling_used']}), 让位 realtime")
                break
            if getattr(args, "backfill_window", True):
                if BackfillWindow.seconds_until_allowed(PLATFORM) > 0:
                    print(f"  [backfill-window] 窗口已关闭, 停"); break
            SoftCooldown.wait_if_active(PLATFORM, verbose=False)
            BackfillLock.heartbeat(PLATFORM, role=lock_role)

            title = (it.get("title") or "")[:60]
            try:
                status, info = dump_one(
                    sess, db, args.category, cfg, it,
                    force=True, pdf_dir=pdf_dir,
                    download_pdf=not args.skip_pdf, token=token,
                )
                ok += 1
                print(f"  [{i:3d}/{len(missing_items)}] {status}  {title}  "
                      f"content={info.get('content_len',0)}字")
            except SessionDead as e:
                print(f"  SessionDead — token 失效, abort: {e}"); return
            except Exception as e:
                fail += 1
                print(f"  [{i:3d}/{len(missing_items)}] ✗  {title}  ERR: {e}")
            cap.bump(); bg_budget.bump()
            bf_session.step()                   # 每 50 条强制 5-15min idle
            throttle.sleep_before_next()        # gauss + tod + soft cd

        print(f"\n[完成] 新增/更新 {ok}  失败 {fail}")
        # 再扫一次确认
        in_db_after = col.count_documents({"_id": {"$in": ids}}) if ids else 0
        print(f"[核对] 平台 {len(items)}  已入库 {in_db_after}  缺 {len(items)-in_db_after}")
    finally:
        try:
            BackfillLock.release(PLATFORM, role=lock_role)
        except Exception:
            pass


if __name__ == "__main__":
    main()
