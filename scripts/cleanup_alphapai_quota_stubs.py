"""One-shot cleanup for alphapai-full content_truncated stubs (no PDF body).

Background: when AlphaPai's per-account daily detail quota was exhausted the
old scraper still upserted the doc, marking ``content_truncated=True`` and
keeping only the ~136-220 char list-card preview as the body. The 2026-04-29
gate in `dump_one` now skips the upsert outright (per user request "如果到今日
限额了, 就别爬取这样的空内容了"). This script soft-deletes the residual stubs
so the StockHub / admin browser stops surfacing them.

Targets — only docs the new gate WOULD have refused to write:
    roadshows  — content_truncated=True (no PDF in this collection)
    comments   — content_truncated=True (no PDF in this collection)
    reports    — content_truncated=True AND no pdf_text_md / no pdf_local_path
                 (truncated reports with PDF still have a body, keep them)

Soft-delete contract — same shape as cleanup_alphapai_thin_clips.py:
    deleted: True
    _deleted_at: <utc datetime>
    _deleted_reason: "quota_stub"

Picked up automatically by `sweep_deleted_docs` (Milvus delete-sweep).

Usage:
    PYTHONPATH=. python3 scripts/cleanup_alphapai_quota_stubs.py            # dry-run
    PYTHONPATH=. python3 scripts/cleanup_alphapai_quota_stubs.py --apply
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone

from pymongo import MongoClient


MONGO_URI_DEFAULT = os.environ.get("MONGO_URI", "mongodb://127.0.0.1:27018/")
MONGO_DB_DEFAULT = os.environ.get("MONGO_DB", "alphapai-full")


def filter_for(category: str) -> dict:
    base = {"content_truncated": True, "deleted": {"$ne": True}}
    if category == "reports":
        # truncated reports with PDF text/file are NOT pure stubs — body is
        # the PDF; keep them visible.
        base["$and"] = [
            {"$or": [{"pdf_text_md": {"$in": [None, ""]}},
                     {"pdf_text_md": {"$exists": False}}]},
            {"$or": [{"pdf_local_path": {"$in": [None, ""]}},
                     {"pdf_local_path": {"$exists": False}}]},
        ]
    return base


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mongo-uri", default=MONGO_URI_DEFAULT)
    parser.add_argument("--mongo-db", default=MONGO_DB_DEFAULT)
    parser.add_argument("--apply", action="store_true",
                        help="Actually soft-delete. Default is dry-run.")
    parser.add_argument("--collections", nargs="+",
                        default=["roadshows", "comments", "reports"],
                        help="Subset of collections to clean.")
    args = parser.parse_args()

    client = MongoClient(args.mongo_uri)
    db = client[args.mongo_db]

    grand_total = 0
    for coll_name in args.collections:
        coll = db[coll_name]
        flt = filter_for(coll_name)
        n = coll.count_documents(flt)
        print(f"  {coll_name}: {n} stubs match")
        grand_total += n

        # Sample a few titles for visibility
        for d in coll.find(flt, {"title": 1, "content": 1}).limit(3):
            preview = (d.get("content") or "")[:80].replace("\n", " ")
            print(f"     [{d['_id']}] {d.get('title')!r} body={preview!r}…")

    print(f"\n[total] {grand_total} stubs across {len(args.collections)} collections")

    if not args.apply:
        print(f"\n[dry-run] no changes written. Re-run with --apply to soft-delete.")
        return

    now = datetime.now(timezone.utc)
    update = {
        "$set": {
            "deleted": True,
            "_deleted_at": now,
            "_deleted_reason": "quota_stub",
        },
    }
    for coll_name in args.collections:
        coll = db[coll_name]
        flt = filter_for(coll_name)
        res = coll.update_many(flt, update)
        print(f"  {coll_name}: soft-deleted {res.modified_count}")

    print(f"\n[apply] Milvus chunks for these docs will be removed by the next "
          f"sweep_deleted_docs run (daily at 03:00, or trigger via "
          f"`PYTHONPATH=. python3 -m scripts.kb_vector.sweep --coll alphapai/<collection> --yes`).")


if __name__ == "__main__":
    main()
