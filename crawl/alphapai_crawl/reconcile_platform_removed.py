#!/usr/bin/env python3
"""
AlphaPai 平台对账脚本 — 标记 / 取消标记 _platform_removed

AlphaPai 会删帖或下架一些条目. 我们的归档策略是 **不删数据**, 但这导致
"今日新增" 仪表盘 (来自 publish_time 聚合) 比平台当前可见数大. 这个脚本
拉取平台 *当前* 的 list (today 窗口), 对库里同日 doc 做 set-diff:

  - DB 有, 平台有 → _platform_removed 清掉 (如果之前被误标)
  - DB 有, 平台无 → _platform_removed=True (平台删帖, 我们归档保留)
  - DB 无, 平台有 → 不处理 (正常的增量 watcher 会下次抓进来)

前端 stats 查询加了 `_platform_removed: {$ne: True}` 过滤, 跑完这个脚本后
今日计数会和平台一致.

用法:
    PYTHONPATH=. python3 crawl/alphapai_crawl/reconcile_platform_removed.py
    # 只对账路演 (默认)
    python3 reconcile_platform_removed.py --category roadshow
    # 只对账今日 (默认), 也可以指定日期
    python3 reconcile_platform_removed.py --date 2026-04-23
    # dry-run 看看会动多少条
    python3 reconcile_platform_removed.py --dry-run

这个脚本 *复用* scraper.py 的 api_call + CATEGORIES 配置, 保证口径一致.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scraper import (  # noqa: E402
    CATEGORIES, SUBTYPES, CATEGORY_ORDER,
    api_call, create_session, fetch_list_page, make_dedup_id,
    connect_mongo, _load_token_from_file, USER_AUTH_TOKEN,
    MONGO_URI_DEFAULT, MONGO_DB_DEFAULT, OK_CODE,
    _parse_time_to_dt, _extract_time_str,
)

_BJ_TZ = timezone(timedelta(hours=8))


def collect_platform_ids_for_day(session, cfg: dict, category_key: str,
                                  target_date: str, max_pages: int = 30) -> set[str]:
    """拉今日在平台仍可见的全部 dedup_id (遍历所有 subtype 覆盖平台各 tab).

    roadshow 平台的默认视图是 union(subtype), 但不同 subtype 翻页顺序不同,
    保险起见遍历所有 subtype 一遍, 把它们各自的 dedup_id 并集起来.
    """
    seen: set[str] = set()

    def scan_one(market_type: str | None, label: str) -> None:
        page = 1
        consecutive_old = 0
        while page <= max_pages:
            resp = fetch_list_page(session, cfg, page, 50,
                                   market_type=market_type,
                                   category_key=category_key)
            if resp.get("code") != OK_CODE:
                print(f"  [{label}] page {page} 拉取失败 code={resp.get('code')}", flush=True)
                break
            items = (resp.get("data") or {}).get("list") or []
            if not items:
                break
            any_today = False
            for it in items:
                t_str = _extract_time_str(it, cfg["time_field"])
                dt = _parse_time_to_dt(t_str)
                if dt is None:
                    continue
                d = dt.strftime("%Y-%m-%d")
                if d == target_date:
                    seen.add(make_dedup_id(category_key, it, cfg))
                    any_today = True
                elif d < target_date:
                    # 条目早于目标日, 认为本页已离开今日窗口
                    consecutive_old += 1
                # d > target_date: 未来日期 (AlphaPai "明日预告"), 跳过
            if not any_today and consecutive_old >= 20:
                break
            if len(items) < 50:
                break
            page += 1

    subtypes = SUBTYPES.get(category_key) or {}
    if subtypes:
        # 平台默认视图 + 所有子类, 取并集
        scan_one(None, "default")
        for mt, meta in subtypes.items():
            scan_one(mt, meta.get("label") or mt)
    else:
        scan_one(None, "default")

    return seen


def reconcile(db, session, category_key: str, target_date: str,
              dry_run: bool = False) -> dict:
    cfg = CATEGORIES[category_key]
    coll = db[cfg["collection"]]

    # 拉平台当前可见 id 集合
    print(f"\n[{category_key}] 拉取平台 {target_date} 仍可见条目 ...", flush=True)
    platform_ids = collect_platform_ids_for_day(session, cfg, category_key, target_date)
    print(f"  平台侧 {target_date} 可见 {len(platform_ids)} 条", flush=True)

    # DB 侧今日 doc
    db_docs = list(coll.find(
        {"publish_time": {"$regex": f"^{target_date}"}},
        {"_id": 1, "_platform_removed": 1},
    ))
    db_ids = {d["_id"] for d in db_docs}
    db_currently_removed = {d["_id"] for d in db_docs if d.get("_platform_removed") is True}
    print(f"  DB 侧 {target_date} 存档 {len(db_ids)} 条 "
          f"(其中 _platform_removed=True: {len(db_currently_removed)})", flush=True)

    # diff
    removed_now = db_ids - platform_ids  # DB 有, 平台无 → 要标 _platform_removed=True
    restored_now = (db_ids & platform_ids) & db_currently_removed  # 之前被标, 现在又出现 → 取消标记

    to_mark = removed_now - db_currently_removed
    print(f"  → 新增 _platform_removed=True 标记: {len(to_mark)}", flush=True)
    print(f"  → 取消 _platform_removed 标记 (平台恢复显示): {len(restored_now)}", flush=True)

    if dry_run:
        for _id in list(to_mark)[:10]:
            print(f"    [dry-run] 将标记: {_id}")
        for _id in list(restored_now)[:10]:
            print(f"    [dry-run] 将恢复: {_id}")
        return {"marked": 0, "restored": 0, "dry_run": True}

    now = datetime.now(timezone.utc)
    marked = 0
    restored = 0
    if to_mark:
        r = coll.update_many(
            {"_id": {"$in": list(to_mark)}},
            {"$set": {
                "_platform_removed": True,
                "_platform_removed_at": now,
            }},
        )
        marked = r.modified_count
    if restored_now:
        r = coll.update_many(
            {"_id": {"$in": list(restored_now)}},
            {"$set": {"_platform_restored_at": now},
             "$unset": {"_platform_removed": "", "_platform_removed_at": ""}},
        )
        restored = r.modified_count
    print(f"  实际写入: marked={marked} restored={restored}", flush=True)
    return {"marked": marked, "restored": restored, "dry_run": False}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--category", default="roadshow",
                   help=f"对账的分类 (默认 roadshow, 可选: {'/'.join(CATEGORY_ORDER)} 或 all)")
    p.add_argument("--date", default=None,
                   help="目标日期 YYYY-MM-DD (默认今日 BJ)")
    p.add_argument("--auth", default="",
                   help="USER_AUTH_TOKEN (默认读 credentials.json)")
    p.add_argument("--mongo-uri", default=MONGO_URI_DEFAULT)
    p.add_argument("--mongo-db", default=MONGO_DB_DEFAULT)
    p.add_argument("--dry-run", action="store_true", help="只看 diff, 不写 DB")
    args = p.parse_args()

    target_date = args.date or datetime.now(_BJ_TZ).strftime("%Y-%m-%d")
    token = (args.auth or _load_token_from_file() or USER_AUTH_TOKEN).strip()
    if not token:
        print("错误: 没有 token (--auth / credentials.json / USER_AUTH_TOKEN 都空)", flush=True)
        sys.exit(1)

    session = create_session(token)
    db = connect_mongo(args.mongo_uri, args.mongo_db)

    cats = CATEGORY_ORDER if args.category == "all" else [args.category]
    summary: dict[str, dict] = {}
    for c in cats:
        if c not in CATEGORIES:
            print(f"跳过未知分类 {c}", flush=True)
            continue
        try:
            summary[c] = reconcile(db, session, c, target_date, dry_run=args.dry_run)
        except Exception as e:
            print(f"[{c}] 对账异常: {e}", flush=True)
            summary[c] = {"error": str(e)}

    print("\n" + "=" * 60)
    print(f"对账完成 target_date={target_date} dry_run={args.dry_run}")
    for c, s in summary.items():
        print(f"  {c}: {s}")
    print("=" * 60)


if __name__ == "__main__":
    main()
