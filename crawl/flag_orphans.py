#!/usr/bin/env python3
"""
crawl/flag_orphans.py — 标记"DB 今日, 平台却看不到"的孤儿条目.

起因 (2026-04-23):
  Gangtise 发现 3 条 summaries 落在 DB 今日 (release_time_ms 是早上 8:00 整点),
  但平台 7 个 classify UI tab 都找不到它们. 原因是爬虫抓那一轮时平台临时把它们挂在
  某个 columnIdList 里, 随后平台重打 tag / columnIds 置 None, UI 不再展示它们.
  DB 仍永久保留 —— 造成"入库 > 平台"的虚高.

对策: 把每日扫到的"在 DB 但不在平台 today 列表里"的条目打 `_orphan=True` +
  `_orphan_marked_at` + `_orphan_reason`. 后续查询都按 `_orphan: {$ne: True}` 过滤,
  保证"今日入库数"和平台 UI 一致.

支持平台:
  - gangtise (summaries / researches / chief_opinions) ✓ 优先
  - alphapai / jinmen / meritco / funda / thirdbridge / acecamp / alphaengine
    → 未实现. 因平台 API 差异大, 先按需添加.

用法:
  python3 crawl/flag_orphans.py --platform gangtise                # dry-run
  python3 crawl/flag_orphans.py --platform gangtise --apply        # 真的打 flag
  python3 crawl/flag_orphans.py --platform gangtise --date 2026-04-22 --apply
  python3 crawl/flag_orphans.py --platform all --apply              # 全部已实现平台

稳定性保护:
  孤儿候选需要 N=2 次连续确认才真正落 `_orphan=True` 字段.
  先次用 `_orphan_candidate_count` 计数 —— 避免平台临时抽风误杀.
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Callable

# 强制不走代理 (infra_proxy 记忆)
for _k in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"):
    os.environ.pop(_k, None)

from pymongo import MongoClient

CRAWL_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(CRAWL_ROOT))

CST = timezone(timedelta(hours=8))


def _cst_day_ms(date_str: str | None):
    if date_str:
        day = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=CST)
    else:
        day = datetime.now(CST).replace(hour=0, minute=0, second=0, microsecond=0)
    start = day.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1) - timedelta(milliseconds=1)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000), start.strftime("%Y-%m-%d")


# ==================== gangtise ====================

def gangtise_platform_ids(start_ms: int, end_ms: int) -> dict[str, set[str]]:
    """Return {type: set(raw_id)} of today's platform IDs per 3 types."""
    sys.path.insert(0, str(CRAWL_ROOT / "gangtise"))
    from scraper import (  # noqa
        _load_token_from_file, create_session,
        fetch_summary_list, fetch_research_list, fetch_chief_list,
        _items_from_list_resp, _item_time_ms,
        SUMMARY_CLASSIFIES, CHIEF_VARIANTS,
    )
    tok = _load_token_from_file()
    if not tok:
        raise RuntimeError("gangtise: credentials.json 缺 token")
    sess = create_session(tok)

    out: dict[str, set[str]] = {"summary": set(), "research": set(), "chief": set()}

    # --- summary: 7 classify ---
    for cl in SUMMARY_CLASSIFIES:
        page = 1
        while page <= 20:
            r = fetch_summary_list(sess, page, 100, classify_param=cl["param"])
            items = _items_from_list_resp(r, "summary")
            if not items:
                break
            stop = False
            for it in items:
                ts = _item_time_ms(it, "summary")
                if ts is None:
                    continue
                if ts < start_ms:
                    stop = True
                    break
                if ts > end_ms:
                    continue
                if it.get("id") is not None:
                    out["summary"].add(str(it["id"]))
            if stop or len(items) < 100:
                break
            page += 1

    # --- research: single feed ---
    page = 1
    while page <= 30:
        r = fetch_research_list(sess, page, 500)
        items = _items_from_list_resp(r, "research")
        if not items:
            break
        stop = False
        for it in items:
            ts = _item_time_ms(it, "research")
            if ts is None:
                continue
            if ts < start_ms:
                stop = True
                break
            if ts > end_ms:
                continue
            rid = it.get("rptId") or it.get("id")
            if rid is not None:
                out["research"].add(str(rid))
        if stop or len(items) < 500:
            break
        page += 1

    # --- chief: 4 variants ---
    for v in CHIEF_VARIANTS:
        page = 1
        while page <= 20:
            r = fetch_chief_list(sess, page, 500, variant=v)
            items = _items_from_list_resp(r, "chief")
            if not items:
                break
            stop = False
            for it in items:
                ts = _item_time_ms(it, "chief")
                if ts is None:
                    continue
                if ts < start_ms:
                    stop = True
                    break
                if ts > end_ms:
                    continue
                cid = it.get("id") or it.get("msgId")
                if cid is not None:
                    out["chief"].add(str(cid))
            if stop or len(items) < 500:
                break
            page += 1

    return out


def gangtise_apply(db, start_ms: int, end_ms: int, apply: bool) -> dict:
    # Collection + platform-id accessor per type.
    # 平台侧 id:
    #   summary / chief: item["id"] (int)    → DB `_id` = "s<id>" / "c<id>"
    #   research       : item["rptId"] (str) → DB `_id` = rptId
    cols = {
        "summary":  ("summaries",       lambda d: str(d["_id"])[1:]),
        "research": ("researches",      lambda d: str(d["_id"])),
        "chief":    ("chief_opinions",  lambda d: str(d["_id"])[1:]),
    }
    plat = gangtise_platform_ids(start_ms, end_ms)
    return _compare_and_mark(db, cols, plat, start_ms, end_ms, apply)


# ==================== alphapai ====================

def alphapai_platform_ids(start_ms: int, end_ms: int) -> dict[str, set[str]]:
    sys.path.insert(0, str(CRAWL_ROOT / "alphapai_crawl"))
    from scraper import (  # noqa
        _load_token_from_file, create_session,
        fetch_list_page, make_dedup_id, _extract_time_str, _parse_time_to_dt,
        CATEGORIES, OK_CODE,
    )
    tok = _load_token_from_file()
    if not tok:
        raise RuntimeError("alphapai: credentials.json 缺 token")
    sess = create_session(tok)

    day_start_dt = datetime.fromtimestamp(start_ms / 1000)   # naive, matches scraper's local-time cmp
    day_end_dt   = datetime.fromtimestamp(end_ms / 1000)

    out = {"roadshow": set(), "comment": set(), "report": set(), "wechat": set()}
    for cat_key, cfg in CATEGORIES.items():
        page = 1
        while page <= 20:
            resp = fetch_list_page(sess, cfg, page, 50)
            if resp.get("code") != OK_CODE:
                break
            items = (resp.get("data") or {}).get("list") or []
            if not items:
                break
            stop = False
            for it in items:
                dt = _parse_time_to_dt(_extract_time_str(it, cfg["time_field"]))
                if dt is None:
                    continue
                if dt < day_start_dt:
                    stop = True
                    break
                if dt > day_end_dt:
                    continue
                out[cat_key].add(make_dedup_id(cat_key, it, cfg))
            if stop or len(items) < 50:
                break
            page += 1
    return out


def alphapai_apply(db, start_ms: int, end_ms: int, apply: bool) -> dict:
    cols = {
        "roadshow": ("roadshows",        lambda d: str(d["_id"])),
        "comment":  ("comments",         lambda d: str(d["_id"])),
        "report":   ("reports",          lambda d: str(d["_id"])),
        "wechat":   ("wechat_articles",  lambda d: str(d["_id"])),
    }
    plat = alphapai_platform_ids(start_ms, end_ms)
    return _compare_and_mark(db, cols, plat, start_ms, end_ms, apply)


# ==================== jinmen ====================

def jinmen_platform_ids(start_ms: int, end_ms: int) -> dict[str, set[str]]:
    sys.path.insert(0, str(CRAWL_ROOT / "jinmen"))
    from scraper import (  # noqa
        fetch_list, fetch_report_list, fetch_oversea_report_list,
        create_session, parse_auth, JM_AUTH_INFO,
    )
    import json as _json
    creds_path = CRAWL_ROOT / "jinmen" / "credentials.json"
    auth_b64 = JM_AUTH_INFO
    if creds_path.exists():
        try:
            auth_b64 = (_json.loads(creds_path.read_text(encoding="utf-8")).get("token") or auth_b64).strip()
        except Exception:
            pass
    auth = parse_auth(auth_b64)
    sess = create_session(auth)

    day_start_dt = datetime.fromtimestamp(start_ms / 1000)
    day_end_dt   = datetime.fromtimestamp(end_ms / 1000)

    def _page_ids(fetcher, id_key: str, time_keys=("stime", "releaseTime", "releaseDate")) -> set[str]:
        ids: set[str] = set()
        page = 1
        while page <= 30:
            try:
                ld = fetcher(sess, page=page, size=40)
            except Exception:
                break
            items = ld.get("rows") or ld.get("data") or []
            if not items:
                break
            stop = False
            for it in items:
                raw = None
                for k in time_keys:
                    if it.get(k):
                        raw = it[k]
                        break
                try:
                    t = int(raw) / 1000 if raw else None
                except Exception:
                    t = None
                if t is None:
                    continue
                d = datetime.fromtimestamp(t)
                if d < day_start_dt:
                    stop = True
                    break
                if d > day_end_dt:
                    continue
                rid = it.get(id_key)
                if rid is not None:
                    ids.add(str(rid))
            extra = ld.get("extra") or {}
            if stop or (("hasMore" in extra) and not extra.get("hasMore")):
                break
            if len(items) < 40:
                break
            page += 1
        return ids

    return {
        "meetings":        _page_ids(fetch_list,                 "roadshowId"),
        "reports":         _page_ids(fetch_report_list,          "id"),
        "oversea_reports": _page_ids(fetch_oversea_report_list,  "id"),
    }


def jinmen_apply(db, start_ms: int, end_ms: int, apply: bool) -> dict:
    cols = {
        "meetings":        ("meetings",         lambda d: str(d.get("_id"))),
        "reports":         ("reports",          lambda d: str(d.get("_id"))),
        "oversea_reports": ("oversea_reports",  lambda d: str(d.get("_id"))),
    }
    plat = jinmen_platform_ids(start_ms, end_ms)
    return _compare_and_mark(db, cols, plat, start_ms, end_ms, apply)


# ==================== 通用比较 / 打标 helper ====================

def _compare_and_mark(db, cols: dict, platform_ids_per_kind: dict,
                       start_ms: int, end_ms: int, apply: bool) -> dict:
    from pymongo import UpdateOne
    report: dict = {}
    for kind, (col, rid_fn) in cols.items():
        platform_ids = platform_ids_per_kind.get(kind, set())
        db_today = list(db[col].find(
            {"release_time_ms": {"$gte": start_ms, "$lte": end_ms}},
            {"_id": 1, "raw_id": 1, "title": 1, "release_time": 1,
             "_orphan": 1, "_orphan_candidate_count": 1},
        ))
        orphans = [d for d in db_today if rid_fn(d) not in platform_ids]
        reappeared = [d for d in db_today if rid_fn(d) in platform_ids
                      and (d.get("_orphan") or d.get("_orphan_candidate_count"))]
        new_flagged, bumped_cand, reset_cand = 0, 0, 0
        if apply:
            ops = []
            now = datetime.now(timezone.utc)
            for d in orphans:
                cur = int(d.get("_orphan_candidate_count") or 0) + 1
                if cur >= 2 and not d.get("_orphan"):
                    ops.append(UpdateOne(
                        {"_id": d["_id"]},
                        {"$set": {"_orphan": True,
                                  "_orphan_marked_at": now,
                                  "_orphan_reason": f"not_in_platform_today_{kind}"},
                         "$inc": {"_orphan_candidate_count": 1}},
                    ))
                    new_flagged += 1
                else:
                    ops.append(UpdateOne(
                        {"_id": d["_id"]},
                        {"$inc": {"_orphan_candidate_count": 1},
                         "$set": {"_orphan_last_seen_at": now}},
                    ))
                    bumped_cand += 1
            # 自愈: 在平台列表中重新出现的条目 → 清零 _orphan 所有标记
            for d in reappeared:
                ops.append(UpdateOne(
                    {"_id": d["_id"]},
                    {"$set": {"_orphan_candidate_count": 0},
                     "$unset": {"_orphan": "", "_orphan_marked_at": "",
                                "_orphan_reason": ""}},
                ))
                reset_cand += 1
            if ops:
                db[col].bulk_write(ops)
        report[kind] = {
            "collection": col,
            "platform_today": len(platform_ids),
            "db_today":       len(db_today),
            "db_today_in_platform": sum(1 for d in db_today if rid_fn(d) in platform_ids),
            "orphan_count":   len(orphans),
            "new_flagged":    new_flagged,
            "bumped_candidate": bumped_cand,
            "reset_reappeared": reset_cand,
            "orphan_samples": [{
                "_id": d["_id"], "raw_id": d.get("raw_id"),
                "title": (d.get("title") or "")[:60],
                "release_time": d.get("release_time"),
            } for d in orphans[:5]],
        }
    return report


# ==================== 统一调度 ====================

IMPLEMENTED: dict[str, Callable] = {
    "gangtise": gangtise_apply,
    "alphapai": alphapai_apply,
    "jinmen":   jinmen_apply,
    # 其他平台: 加在这里.
}

MONGO_DB_MAP = {
    "gangtise":   "gangtise",
    "alphapai":   "alphapai",
    "jinmen":     "jinmen",
    "meritco":    "meritco",
    "thirdbridge": "thirdbridge",
    "funda":      "funda",
    "acecamp":    "acecamp",
    "alphaengine": "alphaengine",
}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--platform", required=True,
                    choices=list(IMPLEMENTED.keys()) + ["all"])
    ap.add_argument("--date", default=None, help="YYYY-MM-DD, default=今日 CST")
    ap.add_argument("--apply", action="store_true",
                    help="真的写 _orphan 字段 (默认只 dry-run)")
    ap.add_argument("--mongo-uri", default=os.environ.get("MONGO_URI", "mongodb://localhost:27017"))
    args = ap.parse_args()

    start_ms, end_ms, date_str = _cst_day_ms(args.date)
    plats = list(IMPLEMENTED.keys()) if args.platform == "all" else [args.platform]
    mc = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=5000)

    for plat in plats:
        db = mc[MONGO_DB_MAP[plat]]
        print(f"\n=== {plat}  ({date_str})  apply={args.apply} ===")
        try:
            report = IMPLEMENTED[plat](db, start_ms, end_ms, apply=args.apply)
        except Exception as e:
            print(f"  ✗ error: {type(e).__name__}: {e}")
            continue
        for kind, r in report.items():
            print(f"  · {kind:8s}/{r['collection']:20s}"
                  f"  platform={r['platform_today']:>5d}"
                  f"  db={r['db_today']:>5d}"
                  f"  orphan={r['orphan_count']}"
                  f"  newly_flagged={r['new_flagged']}"
                  f"  candidates++={r['bumped_candidate']}")
            for s in r["orphan_samples"]:
                print(f"      orphan _id={s['_id']!s:20s} @{s['release_time']}  {s['title']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
