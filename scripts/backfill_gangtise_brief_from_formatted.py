"""Backfill gangtise.researches.brief_md / content_md from formattedBrief.

Bug discovered 2026-04-29: gangtise platform returns both ``brief`` (plain
text) and ``formattedBrief`` (HTML formatted). For some foreign-broker
research notes the platform's plain ``brief`` is mid-sentence-truncated
while ``formattedBrief`` carries the full body (e.g. Deutsche Bank's
"Agents Taking Cloud to Next Level" — brief 2690 chars ending "from: a) its",
formattedBrief 3198 chars including b)/c)).

Scrapes existing docs:
  - prefer formattedBrief (after strip HTML)
  - if formattedBrief stripped > existing brief_md, write back content_md +
    brief_md (chief_opinions: keep brief_md as 500-char prefix per dump_chief
    convention).

Usage:
    PYTHONPATH=. python3 scripts/backfill_gangtise_brief_from_formatted.py
    PYTHONPATH=. python3 scripts/backfill_gangtise_brief_from_formatted.py --apply
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import datetime, timezone

from pymongo import MongoClient
from pymongo.errors import PyMongoError

MONGO_URI_DEFAULT = os.environ.get("MONGO_URI", "mongodb://127.0.0.1:27018/")
MONGO_DB_DEFAULT = os.environ.get("MONGO_DB", "gangtise-full")

_HTML_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")


def _strip_html(s: str) -> str:
    if not s:
        return ""
    s = _HTML_TAG_RE.sub(" ", s)
    s = _WS_RE.sub(" ", s).strip()
    return s


def _pick(doc: dict, *paths: str) -> str:
    for p in paths:
        cur = doc
        for k in p.split("."):
            if not isinstance(cur, dict):
                cur = None
                break
            cur = cur.get(k)
        if isinstance(cur, str) and cur.strip():
            return cur
    return ""


def backfill_researches(db, *, apply: bool) -> dict:
    coll = db["researches"]
    cursor = coll.find(
        {},
        projection={
            "_id": 1, "brief_md": 1, "content_md": 1,
            "list_item.brief": 1, "list_item.formattedBrief": 1,
            "detail_result.brief": 1, "detail_result.formattedBrief": 1,
        },
        no_cursor_timeout=True,
    )
    n_scanned = n_extended = 0
    delta_total = 0
    samples: list[tuple[str, int, int]] = []
    try:
        for d in cursor:
            n_scanned += 1
            existing = (d.get("brief_md") or "")
            fmt_raw = _pick(d, "list_item.formattedBrief", "detail_result.formattedBrief")
            plain_raw = _pick(d, "list_item.brief", "detail_result.brief")
            fmt_text = _strip_html(fmt_raw) if fmt_raw else ""
            plain_text = _strip_html(plain_raw)
            best = fmt_text if len(fmt_text) > len(plain_text) else plain_text
            if len(best) <= len(existing):
                continue
            n_extended += 1
            delta = len(best) - len(existing)
            delta_total += delta
            if len(samples) < 8:
                samples.append((d["_id"], len(existing), len(best)))
            if apply:
                coll.update_one(
                    {"_id": d["_id"]},
                    {"$set": {
                        "brief_md": best,
                        "content_md": best,  # research: content_md == brief by convention
                        "_brief_backfilled_at": datetime.now(timezone.utc),
                    }},
                )
    finally:
        cursor.close()

    print(f"\n=== researches backfill ({'APPLY' if apply else 'DRY-RUN'}) ===")
    print(f"  scanned:  {n_scanned}")
    print(f"  extended: {n_extended}  (+{delta_total} chars total)")
    if samples:
        print("  Samples (existing → new):")
        for _id, old, new in samples:
            print(f"    [{_id:<24}] {old} → {new}  (+{new-old})")
    return {"scanned": n_scanned, "extended": n_extended}


def backfill_chief_opinions(db, *, apply: bool) -> dict:
    """Chief opinions store the upstream chief.list payload — only ones whose
    msgText carries an embedded research (rptId) have a parsed.formattedBrief
    or similar. Most chief items don't have formattedBrief; this is a best-
    effort sweep on those that do (parsed.translatedFormattedBrief is also
    inspected for translated foreign reports).
    """
    coll = db["chief_opinions"]
    cursor = coll.find(
        {"deleted": {"$ne": True}, "is_attachment": {"$ne": True}},
        projection={
            "_id": 1, "content_md": 1, "brief_md": 1,
            "parsed_msg.formattedBrief": 1,
            "parsed_msg.translatedFormattedBrief": 1,
            "parsed_msg.brief": 1,
        },
        no_cursor_timeout=True,
    )
    n_scanned = n_extended = 0
    delta_total = 0
    samples: list[tuple[str, int, int]] = []
    try:
        for d in cursor:
            n_scanned += 1
            pm = d.get("parsed_msg") or {}
            existing = (d.get("content_md") or "")
            cand = ""
            for key in ("formattedBrief", "translatedFormattedBrief", "brief"):
                v = pm.get(key)
                if isinstance(v, str):
                    s = _strip_html(v) if "<" in v else v
                    s = (s or "").strip()
                    if len(s) > len(cand):
                        cand = s
            if len(cand) <= len(existing):
                continue
            n_extended += 1
            delta = len(cand) - len(existing)
            delta_total += delta
            if len(samples) < 8:
                samples.append((d["_id"], len(existing), len(cand)))
            if apply:
                coll.update_one(
                    {"_id": d["_id"]},
                    {"$set": {
                        "content_md": cand,
                        "brief_md": cand[:500],
                        "_brief_backfilled_at": datetime.now(timezone.utc),
                    }},
                )
    finally:
        cursor.close()

    print(f"\n=== chief_opinions backfill ({'APPLY' if apply else 'DRY-RUN'}) ===")
    print(f"  scanned:  {n_scanned}")
    print(f"  extended: {n_extended}  (+{delta_total} chars total)")
    if samples:
        print("  Samples (existing → new):")
        for _id, old, new in samples:
            print(f"    [{_id:<24}] {old} → {new}  (+{new-old})")
    return {"scanned": n_scanned, "extended": n_extended}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--mongo-uri", default=MONGO_URI_DEFAULT)
    ap.add_argument("--mongo-db", default=MONGO_DB_DEFAULT)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    cli = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=5000)
    try:
        cli.admin.command("ping")
    except PyMongoError as e:
        print(f"错误: 无法连接 MongoDB ({args.mongo_uri}): {e}")
        return 2
    db = cli[args.mongo_db]
    print(f"[Mongo] {args.mongo_uri} -> db: {args.mongo_db}")
    print(f"[Mode] {'APPLY' if args.apply else 'DRY-RUN'}")

    backfill_researches(db, apply=args.apply)
    backfill_chief_opinions(db, apply=args.apply)
    print("\nDone.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
