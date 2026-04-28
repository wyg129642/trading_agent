#!/usr/bin/env python3
"""Re-derive dirty *_md fields from existing Mongo `detail_result` blobs.

Two known data-layer bugs as of 2026-04-28:

  1. AceCamp `articles.transcribe_md` — historical scraper did
     `str(detail.get('transcribe') or '').strip()`. The `transcribe` field is
     actually a dict with an `asr[]` array; `str()` produced a 100+ KB Python
     repr (`{'id': ..., 'asr': [{'context': '...'}, ...]}`) that StockHub
     renders as raw text.

  2. Funda `earnings_reports.content_md` (and any post with HTML in
     `detail_result.content`) — old `html_to_text` was a single
     `re.sub(r'<[^>]+>', ' ', html)`. SEC 8-K iXBRL filings store hidden
     XBRL facts in `<ix:hidden>` blocks, so the strip dumps `true true ...
     NASDAQ NASDAQ ...` plus `&#160;` / `&#8220;` entities into content_md.

Both scrapers are now fixed (commits today). This script re-runs the new
extractors over the legacy rows so historical content displays cleanly.

Idempotent: only rewrites when the new output differs. `--dry-run` prints a
sample diff without writing.

Usage:
  PYTHONPATH=. python3 scripts/backfill_clean_md.py --dry-run
  PYTHONPATH=. python3 scripts/backfill_clean_md.py --target acecamp
  PYTHONPATH=. python3 scripts/backfill_clean_md.py --target funda
  PYTHONPATH=. python3 scripts/backfill_clean_md.py             # both
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "crawl"))

from pymongo import MongoClient, UpdateOne  # noqa: E402

# Import the same extractors used by the scrapers, by wiring sys.path per
# platform. We reload between platforms because both packages name their
# top-level module `scraper`.
def _load_acecamp_helpers():
    sys.path.insert(0, str(ROOT / "crawl" / "AceCamp"))
    if "scraper" in sys.modules:
        del sys.modules["scraper"]
    import scraper as ac
    return ac._transcribe_to_md, ac._strip_html


def _load_funda_helpers():
    # Drop AceCamp's scraper from cache so funda's loads cleanly
    if "scraper" in sys.modules:
        del sys.modules["scraper"]
    sys.path.insert(0, str(ROOT / "crawl" / "funda"))
    import scraper as fd
    return fd.html_to_md


def backfill_acecamp(col, dry_run: bool, limit: int) -> dict:
    transcribe_to_md, _ = _load_acecamp_helpers()

    # Match legacy dirt: transcribe_md begins with `{'` (Python dict repr)
    query = {"transcribe_md": {"$regex": r"^\{['\"]"}}
    print(f"[acecamp] scanning {col.name} for dict-repr transcribe_md ...")
    cursor = col.find(query, {"_id": 1, "detail_result": 1, "transcribe_md": 1})
    if limit:
        cursor = cursor.limit(limit)

    n_total = n_fixed = n_skipped = n_no_source = 0
    ops: list[UpdateOne] = []
    sample_shown = False
    for doc in cursor:
        n_total += 1
        det = doc.get("detail_result")
        new_val = ""
        if isinstance(det, dict):
            new_val = transcribe_to_md(det.get("transcribe"))
        if not new_val:
            n_no_source += 1
            continue
        old = doc.get("transcribe_md") or ""
        if new_val == old:
            n_skipped += 1
            continue
        n_fixed += 1
        if not sample_shown and dry_run:
            print(f"  --- sample _id={doc['_id']} ---")
            print(f"  OLD ({len(old)} chars): {old[:200]!r}")
            print(f"  NEW ({len(new_val)} chars): {new_val[:200]!r}")
            print()
            sample_shown = True
        if not dry_run:
            ops.append(UpdateOne(
                {"_id": doc["_id"]},
                {"$set": {"transcribe_md": new_val}},
            ))
        if len(ops) >= 200:
            col.bulk_write(ops, ordered=False)
            ops = []
    if ops:
        col.bulk_write(ops, ordered=False)

    return {"total": n_total, "fixed": n_fixed,
            "already_clean": n_skipped, "no_source": n_no_source}


def backfill_funda(col, dry_run: bool, limit: int) -> dict:
    html_to_md = _load_funda_helpers()

    # Match: content_md contains either `&#` HTML entities OR ix:hidden leak
    # (`true true ... NASDAQ`) OR begins with `<` (raw HTML in content_md).
    # The cheapest pre-filter Mongo can index on is the entity pattern; we
    # post-filter in Python for the rest.
    query = {"$or": [
        {"content_md": {"$regex": r"&#\d+;"}},
        {"content_md": {"$regex": r"^\s*<"}},
    ]}
    print(f"[funda] scanning {col.name} for entity-laden / raw-html content_md ...")
    cursor = col.find(query, {"_id": 1, "detail_result": 1,
                                "content_md": 1, "type": 1})
    if limit:
        cursor = cursor.limit(limit)

    n_total = n_fixed = n_skipped = n_no_source = 0
    ops: list[UpdateOne] = []
    sample_shown = False
    for doc in cursor:
        n_total += 1
        det = doc.get("detail_result") or {}
        c = det.get("content")
        if not isinstance(c, str) or not c:
            n_no_source += 1
            continue
        is_html = (det.get("type") == "EIGHT_K") or c.lstrip().startswith("<")
        if not is_html:
            # plain text content — skip; the scraper now html.unescape()s
            # this on ingest, but on backfill we leave plain text alone to
            # avoid changing legitimate punctuation.
            n_skipped += 1
            continue
        new_val = html_to_md(c, max_len=200_000)
        if not new_val:
            n_no_source += 1
            continue
        old = doc.get("content_md") or ""
        if new_val == old:
            n_skipped += 1
            continue
        n_fixed += 1
        if not sample_shown and dry_run:
            print(f"  --- sample _id={doc['_id']} ---")
            print(f"  OLD ({len(old)} chars): {old[:240]!r}")
            print(f"  NEW ({len(new_val)} chars): {new_val[:240]!r}")
            print()
            sample_shown = True
        if not dry_run:
            ops.append(UpdateOne(
                {"_id": doc["_id"]},
                {"$set": {"content_md": new_val}},
            ))
        if len(ops) >= 200:
            col.bulk_write(ops, ordered=False)
            ops = []
    if ops:
        col.bulk_write(ops, ordered=False)

    return {"total": n_total, "fixed": n_fixed,
            "already_clean": n_skipped, "no_source": n_no_source}


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--mongo-uri", default=os.environ.get(
        "MONGO_URI", "mongodb://127.0.0.1:27018/"))
    p.add_argument("--target", choices=["acecamp", "funda", "all"],
                   default="all")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--limit", type=int, default=0,
                   help="0 = no limit (per platform)")
    args = p.parse_args()

    cli = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=10000,
                      socketTimeoutMS=120000)

    if args.target in ("acecamp", "all"):
        col = cli["acecamp"]["articles"]
        stats = backfill_acecamp(col, args.dry_run, args.limit)
        print(f"[acecamp] {stats}")

    if args.target in ("funda", "all"):
        for coll_name in ("earnings_reports", "earnings_transcripts", "posts"):
            col = cli["funda"][coll_name]
            stats = backfill_funda(col, args.dry_run, args.limit)
            print(f"[funda/{coll_name}] {stats}")

    if args.dry_run:
        print("\n(dry-run: no writes performed)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
