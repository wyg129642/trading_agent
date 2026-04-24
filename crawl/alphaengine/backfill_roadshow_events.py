#!/usr/bin/env python3
"""
Full roadshow_events walker (112,705 events total on platform).
search_after cursor格式: [timestamp_ms, flag, id], 每页 100 条, 按 publish_time 升序.
起点 seed 可任意 (test: "1776787200000" = 2026-04-22); 默认无 seed = 从平台最早 (2025-03-30).

Walk is FREE OF QUOTA — this is the alphaglobalpage proxy, no REFRESH_LIMIT here.

Usage:
  python3 backfill_roadshow_events.py                    # 从 DB 当前最新日期继续, 到今天
  python3 backfill_roadshow_events.py --from-scratch    # 全量从平台最早
  python3 backfill_roadshow_events.py --watch --interval 600   # 持续盯 (实时补)
"""
from __future__ import annotations
import argparse, sys, time, json, hashlib
from datetime import datetime, timedelta, timezone

_BJ_TZ = timezone(timedelta(hours=8))
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import scraper as _scraper
from scraper import create_session, _load_token_from_file, refresh_with_file_lock, API_BASE
from pymongo import MongoClient, DESCENDING
from antibot import (
    SessionDead, SoftCooldown,
    add_antibot_args, throttle_from_args, cap_from_args,
    add_backfill_args, backfill_session_from_args,
    budget_from_args, log_config_stamp,
    BackfillWindow, BackfillLock, BackfillCheckpointBackoff,
    account_id_for_alphaengine,
)

PLATFORM = "alphaengine"
URL = f"{API_BASE}/alphaglobalpage/kmproadshow/api/v1/roadshow/new/search/list"

def walk(sess, mdb, start_sa=None, stop_after_zero=30,
         throttle=None, cap=None, bg_budget=None, bf_session=None, lock_role=None,
         respect_window=True):
    """Walk search_after cursor forward, upsert events into roadshow_events.

    Returns ``(added, last_sa, session_dead_reason)``. The caller can inspect
    ``session_dead_reason`` (non-empty str) to decide whether to attempt
    ``refresh_with_file_lock`` before the next round — critical in --watch so
    we don't burn the bg bucket with a dead token.
    """
    sa = start_sa
    page = added = consecutive_zero = 0
    t0 = time.time()
    while True:
        page += 1
        if cap and cap.exhausted():
            print(f"  [antibot] daily-cap {cap.max_items} 到, 停"); break
        if bg_budget and bg_budget.exhausted():
            st = bg_budget.status()
            print(f"  [antibot] bg budget exhausted ({st['used_24h']}/{st['limit']})"); break
        if respect_window and BackfillWindow.seconds_until_allowed(PLATFORM) > 0:
            print(f"  [backfill-window] 窗口已关闭, 停"); break
        SoftCooldown.wait_if_active(PLATFORM, verbose=False)
        if lock_role:
            BackfillLock.heartbeat(PLATFORM, role=lock_role)

        body = {"size":100, "page":page}
        if sa: body["search_after"] = sa
        try:
            r = sess.post(URL, json=body, timeout=30)
            # 401/403: token revoked → surface to caller for refresh
            if r.status_code in (401, 403):
                reason = f"HTTP {r.status_code} on roadshow/search: {r.text[:160]}"
                print(f"  SessionDead: {reason}")
                return added, sa, reason
            j = r.json() if r.content else {}
            # Body-level 401 "刷新 token" (HTTP 200 wrapper) — same as scraper.py
            if isinstance(j, dict) and (j.get("code") == 401
                                         or "刷新 token" in str(j.get("msg") or "")):
                reason = f"biz 401: {j.get('msg')}"
                print(f"  SessionDead: {reason}")
                return added, sa, reason
        except SessionDead as e:
            print(f"  SessionDead: {e}"); return added, sa, str(e)
        except Exception as e:
            print(f"  page {page}: network err {e}, sleep 5s")
            time.sleep(5); continue
        data = j.get("data") or {}
        results = data.get("results") or []
        has_next = data.get("has_next_page")
        new_sa = data.get("search_after")
        this_added = 0
        min_date = max_date = None
        for dg in results:
            d = dg.get("date")
            if d:
                min_date = d if not min_date else min(min_date, d)
                max_date = d if not max_date else max(max_date, d)
            for rs in (dg.get("common_list") or []) + (dg.get("top_list") or []):
                rid = str(rs.get("roadshow_id") or "")
                if not rid: continue
                res = mdb.roadshow_events.update_one(
                    {"_id": rid},
                    {"$setOnInsert": {
                        "_id": rid, "date": d, **rs,
                        "crawled_at": datetime.now(timezone.utc),
                    }},
                    upsert=True,
                )
                if res.upserted_id: this_added += 1
        added += this_added
        if this_added == 0:
            consecutive_zero += 1
        else:
            consecutive_zero = 0
        if page % 20 == 0 or this_added == 0:
            elapsed = time.time() - t0
            print(f"  [p{page:4d}] +{this_added:3d} cum={added:5d} days={min_date}~{max_date} "
                  f"sa={new_sa} has_next={has_next} elapsed={elapsed:.0f}s")
        if cap: cap.bump()
        if bg_budget: bg_budget.bump()
        if bf_session:
            bf_session.step()
        if not has_next or not new_sa or sa == new_sa:
            print(f"  [完结] has_next={has_next}"); break
        if consecutive_zero > stop_after_zero:
            print(f"  [stop] {consecutive_zero} 连续 0 新增"); break
        sa = new_sa
        if throttle:
            throttle.sleep_before_next()
        else:
            time.sleep(0.2)
    return added, sa, None


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--from-scratch", action="store_true", help="从平台最早开始(忽略 checkpoint)")
    p.add_argument("--watch", action="store_true", help="常驻 watcher 模式")
    p.add_argument("--interval", type=int, default=600)
    p.add_argument("--from-date", default=None, help="YYYY-MM-DD 起点日期(会转 cursor)")
    add_antibot_args(p, default_base=4.0, default_jitter=2.5,
                     default_burst=30, default_cap=400, platform=PLATFORM)
    add_backfill_args(p, platform=PLATFORM)
    args = p.parse_args()

    token = _load_token_from_file()

    # Single-instance lock
    lock_role = "backfill_roadshow_events"
    if getattr(args, "bf_lock", True):
        if not BackfillLock.acquire(PLATFORM, role=lock_role,
                                     force=args.bf_force_lock):
            print(f"[lock] {PLATFORM}:{lock_role} 已被占用. --bf-force-lock 强制夺锁")
            return

    if getattr(args, "backfill_window", True):
        BackfillWindow.wait_until_allowed(PLATFORM)

    # Antibot stack — account_id 从 JWT 解, 再按 worker category 隔离 24h 预算.
    # roadshow_events 走 alphaglobalpage 代理 (跟 streamSearch REFRESH_LIMIT 是
    # 独立的配额), 所以用独立 suffix `:roadshow_events`, 哪怕主 scraper 的 4 条
    # list watcher 把各自的桶打满, 本 backfill 依然能继续跑.
    try:
        import base64, json as _json
        parts = (token or "").split(".")
        if len(parts) >= 2:
            pad = parts[1] + "=" * (-len(parts[1]) % 4)
            payload = _json.loads(base64.urlsafe_b64decode(pad))
            acct_id_base = f"u_{payload.get('uid') or payload.get('userId') or payload.get('id') or 'unknown'}"
        else:
            acct_id_base = "h_" + hashlib.md5((token or "").encode()).hexdigest()[:12]
    except Exception:
        acct_id_base = "h_" + hashlib.md5((token or "").encode()).hexdigest()[:12]
    acct_id = account_id_for_alphaengine(acct_id_base, "roadshow_events")

    throttle = throttle_from_args(args, platform=PLATFORM)
    cap = cap_from_args(args)
    bg_budget = budget_from_args(args, account_id=acct_id, platform=PLATFORM, role="bg")
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
                     extra=f"acct={acct_id} role={lock_role}")

    sess = create_session(token)
    mdb = MongoClient("mongodb://localhost:27017")["alphaengine"]

    # Ensure indexes
    mdb.roadshow_events.create_index([("date", DESCENDING)])
    mdb.roadshow_events.create_index("roadshow_id")
    mdb.roadshow_events.create_index([("publish_time", DESCENDING)])

    def derive_start_sa():
        if args.from_scratch: return None
        if args.from_date:
            # --from-date YYYY-MM-DD → BJ 壁钟 (publish_time 同样是 BJ)
            ms = int(datetime.strptime(args.from_date, "%Y-%m-%d")
                     .replace(tzinfo=_BJ_TZ).timestamp() * 1000)
            return [str(ms), 1, "0"]
        # Default: use the latest doc in DB as seed (continue from where we left off)
        latest = mdb.roadshow_events.find_one(sort=[("publish_time", DESCENDING)])
        if latest and latest.get("publish_time"):
            try:
                dt = datetime.strptime(latest["publish_time"][:16], "%Y-%m-%d %H:%M") \
                             .replace(tzinfo=_BJ_TZ)
                ms = int(dt.timestamp() * 1000)
                return [str(ms), 1, latest.get("roadshow_id","0")]
            except Exception:
                pass
        return None

    def _try_refresh() -> bool:
        """Coordinate with scraper.py watchers via file-locked refresh.
        Returns True on success (fresh access token loaded into this session)."""
        nonlocal sess
        new_access, did = refresh_with_file_lock(client_flag="pc", min_age_s=120)
        if new_access:
            sess = create_session(new_access)
            print(f"  [refresh] ✓ {'本进程刷新成功' if did else '跟随其他进程'}, session 已换新 token")
            return True
        print("  [refresh] ✗ refresh_token 链失效, 需浏览器重登")
        return False

    try:
        if args.watch:
            round_n = 0
            session_dead_streak = 0
            while True:
                round_n += 1
                print(f"\n{'='*60}\n[轮 {round_n}] {datetime.now()}\n{'='*60}")
                sa = derive_start_sa()
                print(f"  start sa={sa}")
                added, _, dead_reason = walk(
                    sess, mdb, sa, stop_after_zero=15,
                    throttle=throttle, cap=cap, bg_budget=bg_budget,
                    bf_session=bf_session, lock_role=lock_role,
                    respect_window=getattr(args, "backfill_window", True))
                total = mdb.roadshow_events.estimated_document_count()
                print(f"  本轮新增 {added}, 总计 {total}")
                if dead_reason:
                    session_dead_streak += 1
                    print(f"  [SessionDead] {dead_reason} (streak={session_dead_streak})")
                    if session_dead_streak > 3:
                        print("  [SessionDead] 连续 3 轮失效, 退出等浏览器重登.")
                        break
                    if _try_refresh():
                        session_dead_streak = 0
                        # 不 sleep, 立即下一轮吃进新 token 的增量
                        continue
                    # refresh 失败 — 不要原地狂打, sleep 满 interval 后再试
                else:
                    session_dead_streak = 0
                if getattr(args, "backfill_window", True):
                    BackfillWindow.wait_until_allowed(PLATFORM)
                try: time.sleep(args.interval)
                except KeyboardInterrupt: break
        else:
            sa = derive_start_sa()
            print(f"start sa={sa} {'(from-scratch)' if not sa else ''}")
            added, _, dead_reason = walk(
                sess, mdb, sa,
                throttle=throttle, cap=cap, bg_budget=bg_budget,
                bf_session=bf_session, lock_role=lock_role,
                respect_window=getattr(args, "backfill_window", True))
            print(f"\n完成: +{added}, 总 {mdb.roadshow_events.estimated_document_count()}")
            if dead_reason:
                print(f"[SessionDead] 本轮被 token 失效中断: {dead_reason}")
    finally:
        try:
            BackfillLock.release(PLATFORM, role=lock_role)
        except Exception:
            pass

if __name__ == "__main__":
    main()
