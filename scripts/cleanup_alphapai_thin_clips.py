"""One-shot cleanup for alphapai-full.reports thin third-party clips.

AlphaPai republishes ~10k items from external "clip" feeds (Seeking Alpha
articles, JPM TMT-Breakout newsletters, etc.) as 200–600 char teaser blurbs
with no PDF and no htmlContent. They render in StockHub /research as a card
with a tiny snippet and no PDF link — pure noise. The source genuinely has
no full body for them (the original sits behind an external paywall).

Cleanup contract — same shape as cleanup_gangtise_chief.py:
    deleted: True
    _deleted_at: <utc datetime>
    _deleted_reason: "thin_clip"
    _clip_source: <detail.source — kept for forensics>

Soft-delete is consumed by:
  - kb_service._build_filter / fetch_document (Phase A search + fetch)
  - api/stock_hub research feed
  - kb_vector_ingest.sweep_deleted_docs (next 03:00 cron eats Milvus chunks)

Usage:
    PYTHONPATH=. python3 scripts/cleanup_alphapai_thin_clips.py            # dry-run
    PYTHONPATH=. python3 scripts/cleanup_alphapai_thin_clips.py --apply
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone

from pymongo import MongoClient


MONGO_URI_DEFAULT = os.environ.get("MONGO_URI", "mongodb://127.0.0.1:27018/")
MONGO_DB_DEFAULT = os.environ.get("MONGO_DB", "alphapai-full")

# Mirror crawl/alphapai_crawl/scraper.py::_THIN_CLIP_SOURCES — kept inline so
# the script doesn't depend on the scraper module's import path.
THIN_CLIP_SOURCES = [
    "seekingalpha", "tb_jpm", "tmt_breakout", "ttg_html", "html",
]


def build_filter() -> dict:
    """Match docs that the new scraper gate (`_is_thin_clip_item`) would skip.

    Same three-prong test as the live gate:
      detail.source ∈ THIN_CLIP_SOURCES
      detail.pdfFlag is not True
      no usable PDF on disk
      detail.htmlContent is empty
    Plus an exclusion: don't re-flag rows already soft-deleted (rerunnable).
    """
    return {
        "detail.source": {"$in": THIN_CLIP_SOURCES},
        "$and": [
            {"$or": [{"detail.pdfFlag": False}, {"detail.pdfFlag": None}]},
            {"$or": [{"pdf_local_path": {"$in": [None, ""]}},
                     {"pdf_local_path": {"$exists": False}}]},
            {"$or": [{"detail.htmlContent": {"$in": [None, ""]}},
                     {"detail.htmlContent": {"$exists": False}}]},
            {"deleted": {"$ne": True}},
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mongo-uri", default=MONGO_URI_DEFAULT)
    parser.add_argument("--mongo-db", default=MONGO_DB_DEFAULT)
    parser.add_argument("--apply", action="store_true",
                        help="Actually soft-delete. Default is dry-run.")
    parser.add_argument("--limit", type=int, default=0,
                        help="Cap docs for testing (0 = no cap).")
    args = parser.parse_args()

    client = MongoClient(args.mongo_uri)
    coll = client[args.mongo_db]["reports"]

    flt = build_filter()
    total = coll.count_documents(flt)
    print(f"[cleanup] match: {total} docs in {args.mongo_db}.reports")
    if total == 0:
        return

    # Source / institution breakdown for visibility
    by_source: dict[str, int] = {}
    by_institution: dict[str, int] = {}
    sample: list[dict] = []
    cursor = coll.find(
        flt,
        {"detail.source": 1, "detail.institution.name": 1, "title": 1},
    ).limit(args.limit or total)
    for d in cursor:
        det = d.get("detail") or {}
        src = det.get("source") or "(none)"
        by_source[src] = by_source.get(src, 0) + 1
        inst_list = det.get("institution") or []
        if inst_list:
            inst_name = inst_list[0].get("name") if isinstance(inst_list[0], dict) else str(inst_list[0])
            by_institution[inst_name] = by_institution.get(inst_name, 0) + 1
        if len(sample) < 5:
            sample.append({"_id": d["_id"], "title": d.get("title")})

    print(f"\n  by source:")
    for k, v in sorted(by_source.items(), key=lambda x: -x[1]):
        print(f"    {k}: {v}")
    print(f"  by institution (top 10):")
    for k, v in sorted(by_institution.items(), key=lambda x: -x[1])[:10]:
        print(f"    {k}: {v}")
    print(f"\n  sample titles:")
    for s in sample:
        print(f"    [{s['_id']}] {s['title']!r}")

    if not args.apply:
        print(f"\n[dry-run] no changes written. Re-run with --apply to soft-delete.")
        return

    now = datetime.now(timezone.utc)
    update = {
        "$set": {
            "deleted": True,
            "_deleted_at": now,
            "_deleted_reason": "thin_clip",
        },
    }
    res = coll.update_many(flt, update)
    print(f"\n[apply] soft-deleted {res.modified_count} docs.")

    # Promote detail.source → top-level _clip_source for forensics. Can't
    # be done in the same update_many because Mongo doesn't let you read
    # one path and write another in a single op. Cheap follow-up loop.
    promoted = 0
    for d in coll.find(
        {"deleted": True, "_deleted_reason": "thin_clip",
         "_clip_source": {"$exists": False}},
        {"detail.source": 1},
    ):
        det = d.get("detail") or {}
        if det.get("source"):
            coll.update_one(
                {"_id": d["_id"]},
                {"$set": {"_clip_source": det.get("source")}},
            )
            promoted += 1
    print(f"[apply] promoted detail.source → _clip_source on {promoted} docs.")

    print(f"[apply] Milvus chunks for these docs will be removed by the next "
          f"sweep_deleted_docs run (daily at 03:00, or trigger via "
          f"`PYTHONPATH=. python3 -m scripts.kb_vector.sweep --collection alphapai/reports --apply`).")


if __name__ == "__main__":
    main()
