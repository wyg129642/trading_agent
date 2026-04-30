"""一次性把 detail._err.code=500020 的历史 stuck doc 软删除.

不打 detail RPC, 不耗 alphapai 日额度. 仅做 DB 标记:
  deleted=True
  _deleted_reason="rate_limit_stub"
  _deleted_at=now

效果:
  - kb_search Phase A/B: SPECS 默认 deleted: {$ne: True} 过滤, 自动忽略
  - Milvus delete-sweep: 24h 内清掉旧 chunk
  - StockHub /research / /roadshow: 默认 deleted_filter, 不再展示
  - dump_one 的 dedup 早退分支已扩展接受 _deleted_reason="rate_limit_stub",
    下次同 dedup_id 进 list 时 fall-through, 不重 detail (除非 list 重新出现).

历史规模 (2026-04-30): roadshows 42876, comments 145.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone

from pymongo import MongoClient


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--collection", choices=("roadshows", "comments", "all"),
                    default="all")
    ap.add_argument("--mongo-uri", default="mongodb://127.0.0.1:27018")
    ap.add_argument("--db", default="alphapai-full")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    c = MongoClient(args.mongo_uri)
    db = c[args.db]

    collections = (["roadshows", "comments"]
                   if args.collection == "all" else [args.collection])

    for coll_name in collections:
        coll = db[coll_name]
        q = {"detail._err.code": 500020,
             "$or": [{"deleted": {"$ne": True}},
                     {"deleted": {"$exists": False}}]}
        n = coll.count_documents(q)
        print(f"[cleanup] {coll_name}: 待软删 stuck doc = {n} "
              f"(dry_run={args.dry_run})")
        if n == 0 or args.dry_run:
            continue
        res = coll.update_many(q, {"$set": {
            "deleted": True,
            "_deleted_reason": "rate_limit_stub",
            "_deleted_at": datetime.now(timezone.utc),
        }})
        print(f"  matched={res.matched_count} modified={res.modified_count}")


if __name__ == "__main__":
    main()
