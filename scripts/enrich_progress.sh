#!/bin/bash
# 查看 enrich_tickers --only-empty 的实时进度。
# 基于 `_canonical_tickers_at` 时间戳 + `_canonical_extract_source: *_title` 计数。

HOURS=${1:-1}

NO_PROXY="192.168.31.176,localhost,127.0.0.1" python3 <<PYEOF
import os
os.environ["NO_PROXY"] = "192.168.31.176,localhost,127.0.0.1"
from pymongo import MongoClient
from datetime import datetime, timezone, timedelta

c = MongoClient("mongodb://127.0.0.1:27018/")
cutoff = datetime.now(timezone.utc) - timedelta(hours=${HOURS})

ROUTE = [
    ("alphapai-full",  ["comments", "roadshows", "wechat_articles", "reports"]),
    ("jinmen-full",    ["meetings", "reports", "oversea_reports"]),
    ("jiuqian-full",   ["forum", "research"]),
    ("third-bridge",   ["interviews"]),
    ("funda",          ["posts", "earnings_reports", "earnings_transcripts", "sentiments"]),
    ("gangtise-full",  ["summaries", "researches", "chief_opinions"]),
    ("acecamp",        ["articles"]),
    ("alphaengine",    ["summaries", "china_reports", "foreign_reports", "news_items"]),
]

print(f"=== 最近 ${HOURS}h 内被 enrich 更新的文档 ===")
print(f"{'集合':<36} {'本次处理':>9} {'新增打标':>9} {'title兜底':>10} {'当前空':>8}")
print("-" * 78)
tot_scanned = tot_new = tot_title = tot_empty = 0
for dbn, colls in ROUTE:
    for coll in colls:
        q_updated = {"_canonical_tickers_at": {"\$gte": cutoff}}
        n_scanned = c[dbn][coll].count_documents(q_updated)
        n_new = c[dbn][coll].count_documents({**q_updated, "_canonical_tickers": {"\$ne": []}})
        n_title = c[dbn][coll].count_documents({**q_updated, "_canonical_extract_source": {"\$regex": "_title\$"}})
        n_empty_now = c[dbn][coll].count_documents({"_canonical_tickers": []})
        if n_scanned > 0 or n_empty_now > 0:
            print(f"{dbn}.{coll:<20} {n_scanned:>9,} {n_new:>9,} {n_title:>10,} {n_empty_now:>8,}")
        tot_scanned += n_scanned; tot_new += n_new; tot_title += n_title; tot_empty += n_empty_now

print("-" * 78)
print(f"{'TOTAL':<36} {tot_scanned:>9,} {tot_new:>9,} {tot_title:>10,} {tot_empty:>8,}")
PYEOF
