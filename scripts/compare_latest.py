#!/usr/bin/env python3
"""对每个平台做 '源站 latest 5 vs 本地 DB latest 5' 一致性核对.

输出 markdown 表格 + 不一致原因的初步猜测.
"""
from __future__ import annotations
import os, sys, json, subprocess, time, importlib.util
from datetime import datetime, timezone
from pathlib import Path
from pymongo import MongoClient

ROOT = Path("/home/ygwang/trading_agent/crawl")

# 2026-04-23 migration → remote 192.168.31.176:35002 (u_spider).
# 2026-04-26 → migrated back to local `ta-mongo-crawl` :27018. The
# `-full` DB-name suffixes carried over from the remote era.
MONGO_URI = os.environ.get(
    "MONGO_URI",
    "mongodb://127.0.0.1:27018/",
)

# DB name translations matching the migration.
_REMOTE_DB_ALIASES = {
    "alphapai":    "alphapai-full",
    "jinmen":      "jinmen-full",
    "meritco":     "jiuqian-full",
    "thirdbridge": "third-bridge",
    "gangtise":    "gangtise-full",
}

# ---------------------------------------------------------------------------
# Platform configs: (key, dir, collection, subfilter, source_invoker_snippet)
# ---------------------------------------------------------------------------
PLATS = [
    dict(
        key="acecamp_articles",
        dir="AceCamp",
        db="acecamp",
        coll="articles",
        subfilter={},
        invoker="""
from scraper import create_session, fetch_articles_list, _load_cookie_from_file, _sec_to_str
sess = create_session(_load_cookie_from_file())
resp = fetch_articles_list(sess, 1, 5)
out = []
for it in (resp.get('data') or [])[:5]:
    out.append({'id': it.get('id'), 'time': _sec_to_str(it.get('release_time')),
                'title': (it.get('title') or '')[:80]})
print(__import__('json').dumps(out, ensure_ascii=False))
""",
    ),
    dict(
        key="acecamp_events",
        dir="AceCamp",
        db="acecamp",
        coll="events",
        subfilter={},
        invoker="""
from scraper import create_session, fetch_events_list, _load_cookie_from_file, _sec_to_str
sess = create_session(_load_cookie_from_file())
resp = fetch_events_list(sess, 1, 5)
out = []
for it in (resp.get('data') or [])[:5]:
    out.append({'id': it.get('id'), 'time': _sec_to_str(it.get('release_time') or it.get('shown_time')),
                'title': (it.get('name') or '')[:80]})
print(__import__('json').dumps(out, ensure_ascii=False))
""",
    ),
    dict(
        key="gangtise_summary",
        dir="gangtise",
        db="gangtise",
        coll="summaries",
        subfilter={},
        invoker="""
from scraper import create_session, fetch_summary_list, _load_token_from_file, _ms_to_str, GANGTISE_TOKEN
import os
token = _load_token_from_file() or os.environ.get('GANGTISE_AUTH') or GANGTISE_TOKEN
sess = create_session(token)
resp = fetch_summary_list(sess, 1, 5)
data = resp.get('data') or {}
items = data.get('summList') or [] if isinstance(data, dict) else []
out = []
for it in items[:5]:
    out.append({'id': it.get('id'), 'time': _ms_to_str(it.get('msgTime') or it.get('summTime')),
                'title': (it.get('title') or '')[:80]})
print(__import__('json').dumps(out, ensure_ascii=False))
""",
    ),
    dict(
        key="gangtise_research",
        dir="gangtise",
        db="gangtise",
        coll="researches",
        subfilter={},
        invoker="""
from scraper import create_session, fetch_research_list, _load_token_from_file, _ms_to_str, GANGTISE_TOKEN
import os
token = _load_token_from_file() or os.environ.get('GANGTISE_AUTH') or GANGTISE_TOKEN
sess = create_session(token)
resp = fetch_research_list(sess, 1, 5)
data = resp.get('data')
items = data if isinstance(data, list) else (data.get('list') or data.get('records') or []) if isinstance(data, dict) else []
out = []
for it in items[:5]:
    out.append({'id': it.get('rptId') or it.get('id'), 'time': _ms_to_str(it.get('pubTime')),
                'title': (it.get('title') or '')[:80]})
print(__import__('json').dumps(out, ensure_ascii=False))
""",
    ),
    dict(
        key="gangtise_chief",
        dir="gangtise",
        db="gangtise",
        coll="chief_opinions",
        subfilter={},
        invoker="""
from scraper import create_session, fetch_chief_list, _load_token_from_file, _ms_to_str, GANGTISE_TOKEN
import os, json
token = _load_token_from_file() or os.environ.get('GANGTISE_AUTH') or GANGTISE_TOKEN
sess = create_session(token)
resp = fetch_chief_list(sess, 1, 5, chief_type=1)
data = resp.get('data')
items = data if isinstance(data, list) else (data.get('list') or data.get('records') or []) if isinstance(data, dict) else []
out = []
for it in items[:5]:
    title = it.get('title') or ''
    mt = it.get('msgText')
    if not title and isinstance(mt, str) and mt.startswith('{'):
        try: title = (json.loads(mt).get('title') or '')
        except Exception: pass
    out.append({'id': it.get('id'), 'time': _ms_to_str(it.get('msgTime')), 'title': title[:80]})
print(json.dumps(out, ensure_ascii=False))
""",
    ),
]

def run_source(plat):
    """Run source fetcher as subprocess, return list of {id,time,title}."""
    cwd = ROOT / plat["dir"]
    code = plat["invoker"]
    env = os.environ.copy()
    # 禁代理
    for k in ("http_proxy","https_proxy","HTTP_PROXY","HTTPS_PROXY","all_proxy","ALL_PROXY"):
        env.pop(k, None)
    try:
        r = subprocess.run(
            ["python3", "-c", code],
            cwd=str(cwd), env=env, capture_output=True, timeout=30, text=True,
        )
        if r.returncode != 0:
            return {"error": f"exit={r.returncode} stderr={r.stderr[-300:]}"}
        lines = [l for l in r.stdout.splitlines() if l.startswith("[") or l.startswith("{")]
        if not lines:
            return {"error": f"no json in stdout: {r.stdout[-200:]}"}
        return {"items": json.loads(lines[-1])}
    except subprocess.TimeoutExpired:
        return {"error": "timeout 30s"}
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


def run_mongo(plat):
    mc = MongoClient(MONGO_URI)
    db_name = _REMOTE_DB_ALIASES.get(plat["db"], plat["db"])
    coll = mc[db_name][plat["coll"]]
    proj = {"title": 1, "name": 1, "release_time": 1, "release_time_ms": 1, "raw_id": 1}
    cursor = coll.find(plat["subfilter"], proj).sort("release_time_ms", -1).limit(5)
    out = []
    for d in cursor:
        out.append({
            "id": d.get("raw_id") or str(d.get("_id")),
            "time": d.get("release_time") or "",
            "title": (d.get("title") or d.get("name") or "")[:80],
        })
    return out


def compare(src_items, db_items):
    """Return 'ok' if titles/times match, else list of differences."""
    if not src_items or not db_items:
        return "no data"
    diffs = []
    for i in range(min(len(src_items), len(db_items))):
        s = src_items[i]; d = db_items[i]
        if s["title"] != d["title"] or s["time"] != d["time"]:
            diffs.append((i, s, d))
    if len(src_items) != len(db_items):
        diffs.append(("length", len(src_items), len(db_items)))
    return diffs or "ok"


if __name__ == "__main__":
    only = sys.argv[1] if len(sys.argv) > 1 else None
    results = []
    for p in PLATS:
        if only and only not in p["key"]:
            continue
        print(f"\n─── {p['key']} ───", flush=True)
        print(f"  source fetching...", flush=True)
        src = run_source(p)
        if "error" in src:
            print(f"  SOURCE ERROR: {src['error']}")
            results.append((p["key"], None, None, src["error"]))
            continue
        src_items = src["items"]
        print(f"  source top {len(src_items)}:")
        for i, it in enumerate(src_items, 1):
            print(f"    {i}. [{it['time']}] {it['title'][:55]}")
        db_items = run_mongo(p)
        print(f"  db top {len(db_items)}:")
        for i, it in enumerate(db_items, 1):
            print(f"    {i}. [{it['time']}] {it['title'][:55]}")
        diff = compare(src_items, db_items)
        print(f"  DIFF: {diff if isinstance(diff, str) else f'{len(diff)} mismatches'}")
        results.append((p["key"], src_items, db_items, diff))
        time.sleep(2)

    print("\n" + "=" * 70)
    print(f"{'platform':<22} {'src_cnt':>7} {'db_cnt':>6} {'status':>28}")
    for key, s, d, diff in results:
        sc = len(s) if s else 0
        dc = len(d) if d else 0
        if isinstance(diff, str):
            st = diff
        else:
            st = f"{len(diff)} mismatch"
        print(f"  {key:<20} {sc:>6}  {dc:>5}  {st:>28}")
