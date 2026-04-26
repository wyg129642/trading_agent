#!/usr/bin/env python3
"""open.gangtise.com 主页聚合信息爬虫 (MongoDB 存储)

抓主页 8 个模块, 写入 gangtise.homepage collection (_id = module_key).

每轮覆盖上次, 不做增量 —— 主页就是实时快照, 不存历史.

模块:
  hot_stocks        机构热议个股        投研 hotRank (topN, 宁德时代/牧原股份等)
  hot_concepts      A 股热门题材        概念热度 (商业航天/光模块等)
  hot_topics        热点话题            每日 AI 晨报式话题
  hot_meetings      机构热议纪要        近期会议 / 路演
  research_sched    近期研究行程        券商近期调研安排
  quick_entries     快速入口            用户菜单 (AI 助手 / 纪要 / ...)
  market_index      大盘指数            上证 / 深证 / 创业板 / 科创 / 恒生 / 纳指 ...
  banners           运营 banner         banner list

复用 scraper.py 里的 session/token/api_call, 只是换 endpoint.

使用:
  python3 scraper_home.py                # 跑一轮, 落库
  python3 scraper_home.py --watch --interval 600
  python3 scraper_home.py --show          # 只看当前 mongo 里啥
  python3 scraper_home.py --module hot_stocks
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timedelta, timezone

_BJ_TZ = timezone(timedelta(hours=8))
from pathlib import Path

# 共享 scraper.py: 复用 create_session / api_call / token 加载
SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))
sys.path.insert(0, str(SCRIPT_DIR.parent))

from scraper import (  # noqa: E402
    _load_token_from_file,
    GANGTISE_TOKEN,
    MONGO_URI_DEFAULT,
    MONGO_DB_DEFAULT,
    create_session,
    api_call,
    _is_ok,
)
from antibot import SessionDead  # noqa: E402
from pymongo import MongoClient  # noqa: E402


COL_HOMEPAGE = "homepage"


# ==================== 每个模块的 endpoint 定义 ====================
#
# 统一用 MODULES 描述 (key, label, method, path, body, extract_items).
# body 可以是 callable(now) -> dict 支持日期参数.

def _today_yyyymmdd() -> int:
    # 岗底斯平台是 Asia/Shanghai, 今天的日期必须按 BJ TZ 取,不然 UTC 服务器会早 8 小时.
    return int(datetime.now(_BJ_TZ).strftime("%Y%m%d"))


MODULES: list[dict] = [
    {
        "key": "hot_stocks",
        "label": "机构热议个股",
        "method": "POST",
        "path": "/application/investReport/api/hotRank",
        "body": lambda: {
            "dataType": 1,
            "dataPeriod": 100300101,
            "topN": 20,
            "statDate": _today_yyyymmdd(),
            "statType": 3,   # 3=纪要热度, 2=研报, 4=... (UI 里会切 tab)
        },
    },
    {
        "key": "hot_concepts",
        "label": "A 股热门题材",
        "method": "GET",
        "path": "/application/cnfrMgr/cncpt/heat/hotList",
        "body": None,
    },
    {
        "key": "hot_topics",
        "label": "每日热点话题",
        "method": "POST",
        "path": "/application/investReport/invest/oppty/queryHotTopic",
        "body": lambda: {"pageSize": 50, "pageNum": 1},
    },
    {
        "key": "hot_meetings",
        "label": "机构热议纪要",
        "method": "POST",
        "path": "/application/cnfrMgr/cnfr/heat/hotList",
        "body": lambda: {
            "pageNum": 1, "pageSize": 20,
            "collected": False, "historical": False, "status": 17,
        },
    },
    {
        "key": "research_sched",
        "label": "近期研究行程",
        "method": "POST",
        "path": "/application/datacall/f8/report",
        "body": lambda: {
            "report_id": "sysStockPool",
            "sql_id": "queryCompanyResearch",
        },
    },
    {
        "key": "quick_entries",
        "label": "快速入口 (用户菜单)",
        "method": "POST",
        "path": "/application/auth/authority/getUserMenuTree/v2",
        # body 需要 access token, 运行时填
        "body": "_inject_token",
    },
    {
        "key": "market_index",
        "label": "大盘指数",
        "method": "POST",
        "path": "/application/quote/v3/getstockquotation",
        "body": lambda: {
            "ktype": "snap",
            "code": [
                "000001.SH", "399001.SZ", "399006.SZ", "000688.SH",
                "HSI.HI", "IXIC.O", "SPX.SPI", "399106.SZ",
            ],
            "select": ("tradetime,timezone,tradeStatus,securityType,securitySubType,"
                       "blockType,blockLevel,date,time,ask,prev_close,name,last,"
                       "change,chg_rate,amount,upNumInBlock,zeroNumInBlock,"
                       "downNumInBlock"),
        },
    },
    {
        "key": "banners",
        "label": "运营 Banner",
        "method": "POST",
        "path": "/application/userCenter/userCenter/api/banner/list",
        "body": lambda: {"platform": 201},
    },
]


# ==================== 抓取 / 规范化 ====================

def _resolve_body(body, token: str) -> dict | None:
    if body is None:
        return None
    if body == "_inject_token":
        return {"accessToken": token}
    if callable(body):
        return body()
    return body


def _extract_list(resp: dict) -> list:
    """尽量拉出 list 形态. gangtise 嵌套不统一:
      data.list / result / data / obj / result.list / result.data / data.datas / batch
    直接在这里兜底, 下游用 raw 字段兜底解析.
    """
    if not isinstance(resp, dict):
        return []
    # quote/v3 系列直接返 {batch: [...]}
    if isinstance(resp.get("batch"), list):
        return resp["batch"]
    candidates = []
    data = resp.get("data")
    result = resp.get("result")
    obj = resp.get("obj")
    for v in (data, result, obj):
        if isinstance(v, list):
            candidates.append(v)
        if isinstance(v, dict):
            for k in ("list", "data", "datas", "systemMenus"):
                if isinstance(v.get(k), list):
                    candidates.append(v[k])
    return candidates[0] if candidates else []


def fetch_module(session, mod: dict, token: str) -> dict:
    """拉一个模块, 返回要存的 doc (不含 _id)."""
    started = time.time()
    body = _resolve_body(mod["body"], token)
    try:
        if mod["method"] == "GET":
            resp = api_call(session, "GET", mod["path"], params=None)
        else:
            resp = api_call(session, "POST", mod["path"], json_body=body)
        ok = _is_ok(resp)
        # quote/v3 (market_index) 没有标准 code 字段, batch 是 list 就算 ok
        if not ok and isinstance(resp, dict) and isinstance(resp.get("batch"), list):
            ok = True
        items = _extract_list(resp) if ok else []
        return {
            "label": mod["label"],
            "method": mod["method"],
            "path": mod["path"],
            "body": body,
            "ok": ok,
            "status_code": resp.get("code") if isinstance(resp, dict) else None,
            "status_msg": resp.get("msg") if isinstance(resp, dict) else None,
            "item_count": len(items) if isinstance(items, list) else 0,
            "items": items,
            "raw": resp,
            "fetched_at": datetime.now(timezone.utc),
            "latency_ms": int((time.time() - started) * 1000),
        }
    except SessionDead as e:
        return {
            "label": mod["label"], "method": mod["method"], "path": mod["path"],
            "ok": False, "items": [], "item_count": 0, "raw": None,
            "status_msg": f"SessionDead: {e}",
            "fetched_at": datetime.now(timezone.utc),
            "latency_ms": int((time.time() - started) * 1000),
        }
    except Exception as e:
        return {
            "label": mod["label"], "method": mod["method"], "path": mod["path"],
            "ok": False, "items": [], "item_count": 0, "raw": None,
            "status_msg": f"{type(e).__name__}: {e}",
            "fetched_at": datetime.now(timezone.utc),
            "latency_ms": int((time.time() - started) * 1000),
        }


def run_once(db, session, token: str, only: str | None = None,
             verbose: bool = True) -> dict:
    """跑一轮: 拉所有模块, upsert 到 gangtise.homepage, 返回 summary."""
    col = db[COL_HOMEPAGE]
    summary = {"ok": 0, "fail": 0, "modules": {}}
    for mod in MODULES:
        if only and mod["key"] != only:
            continue
        doc = fetch_module(session, mod, token)
        doc["_id"] = mod["key"]
        col.replace_one({"_id": mod["key"]}, doc, upsert=True)
        if doc["ok"]:
            summary["ok"] += 1
        else:
            summary["fail"] += 1
        summary["modules"][mod["key"]] = {
            "ok": doc["ok"], "count": doc["item_count"],
            "latency_ms": doc["latency_ms"], "msg": doc.get("status_msg"),
        }
        if verbose:
            flag = "✓" if doc["ok"] else "✗"
            print(f"  {flag} {mod['key']:16s} {doc['label']:20s} "
                  f"items={doc['item_count']:4d} "
                  f"{doc['latency_ms']}ms "
                  f"{doc.get('status_msg') or ''}")
    return summary


def show_state(db):
    col = db[COL_HOMEPAGE]
    docs = list(col.find({}, {"label": 1, "item_count": 1, "ok": 1,
                                "fetched_at": 1, "latency_ms": 1,
                                "status_msg": 1}))
    if not docs:
        print("(空 — 还没跑过)")
        return
    print(f"gangtise.homepage 共 {len(docs)} 个模块:")
    for d in docs:
        age_s = (datetime.now(timezone.utc) -
                 d.get("fetched_at", datetime.now(timezone.utc))).total_seconds()
        flag = "✓" if d.get("ok") else "✗"
        print(f"  {flag} {str(d.get('_id')):16s} {d.get('label'):20s} "
              f"items={d.get('item_count',0):4d}  age={int(age_s)}s  "
              f"{d.get('status_msg') or ''}")


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mongo-uri", default=MONGO_URI_DEFAULT)
    ap.add_argument("--mongo-db", default=MONGO_DB_DEFAULT)
    ap.add_argument("--auth", default="", help="覆盖 token")
    ap.add_argument("--module", default=None,
                    help="只抓指定 key (hot_stocks / hot_concepts / ...)")
    ap.add_argument("--watch", action="store_true",
                    help="循环运行")
    ap.add_argument("--interval", type=int, default=600,
                    help="循环间隔秒 (默认 600 = 10min)")
    ap.add_argument("--show", action="store_true", help="只看当前库里状态")
    args = ap.parse_args()

    token = args.auth or _load_token_from_file() or GANGTISE_TOKEN
    if not token:
        print("ERR: no token", file=sys.stderr)
        return 1

    client = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=5000)
    db = client[args.mongo_db]

    if args.show:
        show_state(db)
        return 0

    session = create_session(token)

    if args.watch:
        print(f"[home] watch 模式, interval={args.interval}s")
        round_no = 0
        while True:
            round_no += 1
            print(f"\n== 轮 {round_no} @ {datetime.now().isoformat(timespec='seconds')} ==",
                  flush=True)
            try:
                summary = run_once(db, session, token, only=args.module)
                print(f"  => {summary['ok']} OK / {summary['fail']} FAIL", flush=True)
            except SessionDead as e:
                print(f"  ✗ Session dead: {e}", file=sys.stderr, flush=True)
                return 2
            except Exception as e:
                print(f"  ! loop err: {type(e).__name__}: {e}",
                      file=sys.stderr, flush=True)
            time.sleep(args.interval)
    else:
        summary = run_once(db, session, token, only=args.module)
        print(f"\nTotal: {summary['ok']} OK / {summary['fail']} FAIL")

    return 0


if __name__ == "__main__":
    sys.exit(main())
