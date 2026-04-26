#!/usr/bin/env python3
"""Backfill Gangtise summary content_md: HTML → Markdown.

Since 2026-04-21 the gangtise summary/download endpoint started returning
HTML for newer summaries. The scraper now converts these on ingest
(crawl/gangtise/scraper.py::_summary_text_to_md), but ~390 docs already
in Mongo were stored as raw HTML.

This script re-runs the same conversion on existing docs.

Usage:
  PYTHONPATH=. python3 scripts/backfill_gangtise_summary_md.py --dry-run
  PYTHONPATH=. python3 scripts/backfill_gangtise_summary_md.py
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "crawl"))
sys.path.insert(0, str(ROOT / "crawl" / "gangtise"))

from scraper import _BLOCK_HTML_RE, _summary_text_to_md  # noqa: E402
from pymongo import MongoClient  # noqa: E402


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--mongo-uri", default=os.environ.get(
        "MONGO_URI",
        "mongodb://127.0.0.1:27018/",
    ))
    p.add_argument("--db", default="gangtise-full")
    p.add_argument("--collection", default="summaries")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--limit", type=int, default=0, help="0 = no limit")
    args = p.parse_args()

    cli = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=10000,
                      socketTimeoutMS=60000)
    col = cli[args.db][args.collection]

    # Two-phase: aggregation pulls just _ids (server-side regex on indexed
    # release_time subset, tiny payload), then we re-fetch the full docs by
    # _id batch. A single find() with regex + content_md projection over WAN
    # was stalling the cursor read on 51K-doc collections.
    pattern = "<\\s*(h[1-6]|p|ul|ol|li|table|div|span)\\b"
    print(f"phase 1: enumerating _ids matching {pattern}")
    id_docs = list(col.aggregate([
        {"$match": {
            "release_time": {"$gte": "2026-04-20"},
            "content_md": {"$regex": pattern, "$options": "i"},
        }},
        {"$project": {"_id": 1}},
    ]))
    ids = [d["_id"] for d in id_docs]
    if args.limit:
        ids = ids[: args.limit]
    print(f"  matched {len(ids)} _ids")

    # Sequential find_one is empirically much faster than $in batches on this
    # remote (5 docs via $in: 2.8s; 5 docs via 5x find_one: 0.7s).
    print(f"phase 2: fetching {len(ids)} full docs by _id")
    docs = []
    for n, did in enumerate(ids, start=1):
        d = col.find_one({"_id": did}, {"_id": 1, "content_md": 1, "stats": 1})
        if d is not None:
            docs.append(d)
        if n % 50 == 0 or n == len(ids):
            print(f"  ...fetched {n}/{len(ids)}", flush=True)
    print(f"  loaded {len(docs)} candidate docs total")

    from pymongo import UpdateOne

    n_seen = n_changed = n_unchanged = 0
    sample_before = sample_after = ""
    pending: list = []
    BATCH = 50

    def flush() -> None:
        nonlocal pending
        if not pending or args.dry_run:
            pending = []
            return
        col.bulk_write(pending, ordered=False)
        pending = []

    for doc in docs:
        n_seen += 1
        original = doc.get("content_md") or ""
        if not _BLOCK_HTML_RE.search(original):
            n_unchanged += 1
            continue
        cleaned = _summary_text_to_md(original)
        if cleaned == original:
            n_unchanged += 1
            continue
        if not sample_before:
            sample_before = original[:200]
            sample_after = cleaned[:200]
        n_changed += 1
        stats = dict(doc.get("stats") or {})
        stats["content_chars"] = len(cleaned)
        pending.append(
            UpdateOne(
                {"_id": doc["_id"]},
                {"$set": {"content_md": cleaned, "stats": stats}},
            )
        )
        if len(pending) >= BATCH:
            flush()
            print(f"  ...committed {n_changed}/{len(docs)}")
    flush()

    print(f"seen={n_seen} changed={n_changed} unchanged={n_unchanged}")
    if sample_before:
        print("--- BEFORE (first doc) ---")
        print(sample_before)
        print("--- AFTER ---")
        print(sample_after)
    if args.dry_run:
        print("(dry-run, no writes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
