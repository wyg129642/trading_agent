#!/usr/bin/env python3
"""funda 精确补洞 (platform-list vs DB diff) — antibot v2 集成版.

为什么不用 scraper.py --force 直接跑?
  1. scraper.py 主循环按 cursor 分页, 遇 DB 已存在就 skip, 没法精确补"平台有 / DB 缺".
  2. 直接 force 重爬全列表 → 1024+3406 = 4400+ 次 detail 命中 → 吃光 24h 预算 + 可能触发风控.

本脚本:
  - 先全量拉平台 list (只读 list 端点, 不打 detail)
  - 按 raw_id 对 DB 做 diff, 精确出缺失条目
  - 对缺失的每条 force dump_one (每条 1 次 list 引用的 item 对象 + 1 次 detail 调用)
  - 全程复用 scraper.py 的 _THROTTLE / _BUDGET / SoftCooldown

funda-specific 微调 (比 historical 档更保守):
  - throttle base=3.5s jitter=2.5s (historical 3.0/2.0) — 刚 401 过, 谨慎
  - burst_size=30 cooldown=40~90s (historical 40/30~60) — 缩小突发窗口
  - account_budget=1500 (默认 2000) — 预留 25% 余量给实时 watcher
  - 两阶段运行: earnings_report 13 条先跑 (smoke test) → 间隔 30-60s → transcripts 199 条
  - 前置 pre-flight: user.getUserProfile 探活 + 两个 list 端点各探 1 页 (earnings_report
    刚刚 401 过, 要确认现在恢复了) + 检 Redis soft_cooldown:funda

Usage:
    cd crawl/funda
    python3 backfill_missing.py                 # 两阶段完整补
    python3 backfill_missing.py --dry-run       # 只报告缺失, 不写 DB
    python3 backfill_missing.py --only earnings_report     # 单阶段
    python3 backfill_missing.py --only earnings_transcript
"""
from __future__ import annotations

import argparse
import os
import random
import sys
import time
from pathlib import Path

# 代理 / 路径
for k in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"):
    os.environ.pop(k, None)
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent.parent))  # 让 antibot 可 import

# 复用 scraper 的全部管线
import scraper  # noqa: E402
from scraper import (  # noqa: E402
    CATEGORIES, MONGO_URI_DEFAULT, MONGO_DB_DEFAULT,
    create_client, load_creds, extract_session_token,
    fetch_list_page, dump_one, trpc_get, connect_mongo,
)
from antibot import (  # noqa: E402
    AdaptiveThrottle, AccountBudget, SoftCooldown, SessionDead,
    cap_from_args, log_config_stamp, DailyCap,
)


# funda 的 antibot 全局(会覆盖 scraper 模块级的, 保证 dump_one 内部调用走这里)
def install_antibot(platform: str, account_id: str,
                    base: float, jitter: float,
                    burst_size: int, burst_cd_min: float, burst_cd_max: float,
                    budget_limit: int,
                    disable_long_tail: bool = False) -> tuple[AdaptiveThrottle, AccountBudget]:
    throttle = AdaptiveThrottle(
        base_delay=base, jitter=jitter,
        burst_size=burst_size,
        burst_cooldown_min=burst_cd_min,
        burst_cooldown_max=burst_cd_max,
        platform=platform,
        enable_time_of_day=True,
        enable_soft_cooldown=True,
        long_tail_prob=0.0 if disable_long_tail else 0.05,
    )
    budget = AccountBudget(platform, account_id=account_id, daily_limit=budget_limit)
    # 打到 scraper 模块级变量 — 这样 dump_one 里的 _THROTTLE.sleep_before_next() 走我们的配置
    scraper._THROTTLE = throttle
    scraper._BUDGET = budget
    return throttle, budget


def preflight(client, platform: str) -> list[str]:
    """返回 reason 列表, 空 = 全部通过."""
    issues = []
    # 1. soft cooldown active?
    rem = SoftCooldown.remaining(platform)
    if rem > 0:
        issues.append(f"SoftCooldown active {rem/60:.1f}min — abort.")
    # 2. getUserProfile
    try:
        trpc_get(client, "user.getUserProfile",
                 {"0": {"json": None, "meta": {"values": ["undefined"], "v": 1}}},
                 what="preflight/profile")
    except SessionDead as e:
        issues.append(f"user.getUserProfile SessionDead: {e}")
    except Exception as e:
        issues.append(f"user.getUserProfile err: {type(e).__name__} {e}")
    # 3. 两个 list 端点都探一下 (earnings_report 昨晚 401 过, 必须确认恢复)
    for key in ("earnings_report", "earnings_transcript"):
        try:
            r = fetch_list_page(client, CATEGORIES[key], limit=1)
            if not (r or {}).get("items"):
                issues.append(f"{key} list 返回空 items")
        except SessionDead as e:
            issues.append(f"{key} list SessionDead: {e} — 需重登 funda.ai")
        except Exception as e:
            issues.append(f"{key} list err: {type(e).__name__} {e}")
    return issues


def diff_platform(client, coll, key: str, page_size: int = 100) -> tuple[list[dict], list[dict]]:
    """返回 (all_platform_items, missing_items) — 全平台翻到头, raw_id 对 DB diff."""
    cfg = CATEGORIES[key]
    cursor = None
    items: list[dict] = []
    page = 0
    while True:
        page += 1
        r = fetch_list_page(client, cfg, limit=page_size, cursor=cursor)
        ii = (r or {}).get("items") or []
        nxt = (r or {}).get("nextCursor")
        items.extend(ii)
        sys.stdout.write(f"\r  [list page {page}] +{len(ii)} (total {len(items)})")
        sys.stdout.flush()
        # scraper._THROTTLE 会在 trpc_get 内部负责间隔 — list 翻页不需要再 sleep
        if not ii or not nxt:
            break
        cursor = nxt
    print()
    db_ids = {str(d.get("_id")) for d in coll.find({}, {"_id": 1})}
    missing = [it for it in items if it.get("id") not in db_ids]
    return items, missing


def backfill_category(client, db, key: str, missing: list[dict],
                      dry_run: bool, cap: DailyCap, budget: AccountBudget) -> dict:
    """逐条 force dump_one. 返回 stats dict."""
    cfg = CATEGORIES[key]
    stats = {"added": 0, "updated": 0, "skipped": 0, "failed": 0, "halted": False}
    if dry_run:
        print(f"  [dry-run] 跳过 {len(missing)} 条 force fetch")
        return stats
    for i, it in enumerate(missing, 1):
        # 破闸检查
        if SoftCooldown.remaining("funda") > 0:
            print(f"\n  [HALT] SoftCooldown 触发, 停在第 {i-1}/{len(missing)}")
            stats["halted"] = True
            break
        if budget.exhausted():
            print(f"\n  [HALT] 账号 24h 预算耗尽 (usage={budget.count_24h()}/{budget.daily_limit})")
            stats["halted"] = True
            break
        if cap.exhausted():
            print(f"\n  [HALT] daily-cap 耗尽")
            stats["halted"] = True
            break
        try:
            res = dump_one(client, db, key, cfg, it, force=True)
            stats[res] = stats.get(res, 0) + 1
            cap.bump(); budget.bump()
            d = str(it.get("date") or it.get("publishedAt") or "")[:10]
            ttl = str(it.get("title") or it.get("ticker", ""))[:55]
            print(f"  [{i:3d}/{len(missing)}] {res:7s} {d}  {ttl}")
        except SessionDead as e:
            print(f"\n  [HALT] SessionDead: {e}")
            stats["failed"] += 1
            stats["halted"] = True
            break
        except Exception as e:
            stats["failed"] += 1
            print(f"  [{i:3d}/{len(missing)}] FAIL {type(e).__name__}: {str(e)[:80]}")
        # scraper._THROTTLE.sleep_before_next() 已经由 fetch_detail/dump_one 内部调用
    return stats


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--only", choices=["earnings_report", "earnings_transcript"],
                   default=None,
                   help="只跑一个 category (默认两个都跑)")
    p.add_argument("--dry-run", action="store_true",
                   help="只 diff, 不 force fetch")
    p.add_argument("--page-size", type=int, default=100)
    # funda-specific 保守参数
    p.add_argument("--throttle-base", type=float, default=3.5)
    p.add_argument("--throttle-jitter", type=float, default=2.5)
    p.add_argument("--burst-size", type=int, default=30)
    p.add_argument("--burst-cooldown-min", type=float, default=40.0)
    p.add_argument("--burst-cooldown-max", type=float, default=90.0)
    p.add_argument("--daily-cap", type=int, default=0, help="0=不设 (本次 ~260 条)")
    p.add_argument("--account-budget", type=int, default=1500)
    p.add_argument("--mongo-uri", default=MONGO_URI_DEFAULT)
    p.add_argument("--mongo-db", default=MONGO_DB_DEFAULT)
    args = p.parse_args()

    cookie, ua = load_creds()
    sess_token = extract_session_token(cookie)
    import hashlib as _hl
    account_id = "h_" + _hl.md5((sess_token or cookie or "").encode()).hexdigest()[:12]

    throttle, budget = install_antibot(
        platform="funda", account_id=account_id,
        base=args.throttle_base, jitter=args.throttle_jitter,
        burst_size=args.burst_size,
        burst_cd_min=args.burst_cooldown_min,
        burst_cd_max=args.burst_cooldown_max,
        budget_limit=args.account_budget,
    )
    cap = DailyCap(max_items=args.daily_cap if args.daily_cap else 0)
    log_config_stamp(throttle, cap=cap, budget=budget,
                     extra=f"acct={account_id} mode=backfill_missing")

    client = create_client(cookie, ua)
    db = connect_mongo(args.mongo_uri, args.mongo_db)

    # ───── Pre-flight ─────
    print("\n━━━ Pre-flight ━━━")
    issues = preflight(client, "funda")
    if issues:
        print("  ✗ 失败:")
        for i in issues: print(f"    - {i}")
        print("\n  → 中止. 请重登 funda.ai 刷新 cookie, 或清除 SoftCooldown 再试.")
        sys.exit(2)
    print("  ✓ 会话 + 两个 list 端点 + SoftCooldown 均正常")

    # ───── 选阶段 ─────
    plan = []
    if args.only in (None, "earnings_report"):
        plan.append(("earnings_report", "earnings_reports"))
    if args.only in (None, "earnings_transcript"):
        plan.append(("earnings_transcript", "earnings_transcripts"))

    global_stats = {}
    for idx, (key, coll_name) in enumerate(plan):
        coll = db[coll_name]
        print(f"\n━━━━━━━━━━ Phase {idx+1}/{len(plan)}: {key} ━━━━━━━━━━")
        print(f"  先全量拉平台 list (page_size={args.page_size})...")
        all_items, missing = diff_platform(client, coll, key, page_size=args.page_size)
        print(f"  平台={len(all_items)}  DB={coll.estimated_document_count()}  "
              f"缺={len(missing)}  budget_used={budget.count_24h()}/{budget.daily_limit}")
        if not missing:
            print("  ✓ 该分类已全量, 跳过")
            global_stats[key] = {"planned": 0}
            continue
        stats = backfill_category(client, db, key, missing,
                                  dry_run=args.dry_run, cap=cap, budget=budget)
        # 补完再做一次 post-check diff
        if not args.dry_run and not stats.get("halted"):
            ids_after = {str(d.get("_id")) for d in coll.find({}, {"_id": 1})}
            still = [it.get("id") for it in all_items if it.get("id") not in ids_after]
            stats["still_missing"] = len(still)
            print(f"  post-check: DB 新总数={len(ids_after)}  仍缺={len(still)}")
        global_stats[key] = {"planned": len(missing), "stats": stats}
        # 阶段间 30-60s 冷却 (只有下一阶段还要跑才睡)
        if idx + 1 < len(plan) and not stats.get("halted"):
            pause = random.uniform(30, 60)
            print(f"\n  [phase-pause] 休 {pause:.1f}s 再进入下阶段...")
            time.sleep(pause)

    # ───── 汇总 ─────
    print("\n══════ 补洞汇总 ══════")
    for k, v in global_stats.items():
        print(f"  {k}: {v}")
    print(f"  最终 budget 用量: {budget.count_24h()}/{budget.daily_limit}")
    print(f"  SoftCooldown funda 剩余: {SoftCooldown.remaining('funda'):.0f}s")


if __name__ == "__main__":
    main()
