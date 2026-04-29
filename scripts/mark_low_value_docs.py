"""Mark short-content Mongo docs as `_low_value=True` so StockHub can hide them.

Idempotent: re-running picks up new stubs AND unmarks docs whose PDFs were
parsed since the last run (PDF text now ≥ threshold).

Cleanup contract (mirrors scripts/cleanup_alphapai_thin_clips.py):
    Marking:
        _low_value: True
        _low_value_at: <utc datetime>
        _low_value_chars: <sum body fields + pdf_text len, computed via $expr>
        _low_value_threshold: <int>

    Self-heal (unmark): UNSET the same four fields.

Excluded from the marking pass entirely:
  - ir_filings/* — PDF-heavy, scoring meaningless until extract pipeline catches up
  - PDF-pending docs (pdf_local_path exists AND pdf_text_md < threshold) —
    these will heal once extract_pdf_texts.py covers them; marking them now
    would wrongly hide ~100k oversea_reports waiting on PDF parse.
  - already-soft-deleted docs (deleted=True) — separate semantics

Read-side honoring:
  - backend/app/api/stock_hub.py::_query_spec adds `_low_value: {$ne: True}`
    to base_match. Detail endpoint raises 410 on `_low_value=True`.
  - kb_service / Milvus NOT modified in phase 1 (LLM searches keep full corpus).

Usage:
    PYTHONPATH=. python3 scripts/mark_low_value_docs.py                         # dry-run
    PYTHONPATH=. python3 scripts/mark_low_value_docs.py --apply
    PYTHONPATH=. python3 scripts/mark_low_value_docs.py --threshold 100 --apply
    PYTHONPATH=. python3 scripts/mark_low_value_docs.py --apply \
        --only jinmen-full/oversea_reports
    PYTHONPATH=. python3 scripts/mark_low_value_docs.py --apply \
        --exclude jinmen-full/oversea_reports
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from typing import Any

from pymongo import MongoClient


MONGO_URI_DEFAULT = os.environ.get("MONGO_URI", "mongodb://127.0.0.1:27018/")

# (db, collection, body_fields). Mirrors stock_hub.py::SOURCES.body_sections.
# Same list as scripts/analyze_content_lengths.py — keep in sync.
SPECS: list[tuple[str, str, list[str]]] = [
    ("alphapai-full", "reports",          ["content", "list_item.content", "list_item.contentCn"]),
    ("alphapai-full", "comments",         ["content", "list_item.content"]),
    ("alphapai-full", "roadshows",        ["content", "list_item.content"]),
    ("alphapai-full", "wechat_articles",  ["content", "list_item.content"]),
    ("jinmen-full",   "reports",          ["summary_point_md", "summary_md"]),
    ("jinmen-full",   "oversea_reports",  ["summary_point_md", "summary_md"]),
    ("jinmen-full",   "meetings",         ["points_md", "chapter_summary_md", "indicators_md", "transcript_md"]),
    ("gangtise-full", "researches",       ["brief_md", "content_md"]),
    ("gangtise-full", "summaries",        ["content_md", "brief_md"]),
    ("gangtise-full", "chief_opinions",   ["description_md", "content_md", "brief_md"]),
    ("funda",         "posts",            ["content_md"]),
    ("funda",         "earnings_reports", ["content_md"]),
    ("funda",         "earnings_transcripts", ["content_md"]),
    ("funda",         "semianalysis_posts",   ["subtitle", "content_md"]),
    ("alphaengine",   "china_reports",    ["doc_introduce", "content_md"]),
    ("alphaengine",   "foreign_reports",  ["doc_introduce", "content_md"]),
    ("alphaengine",   "summaries",        ["doc_introduce", "content_md"]),
    ("alphaengine",   "news_items",       ["doc_introduce", "content_md"]),
    ("acecamp",       "articles",         ["summary_md", "content_md", "transcribe_md", "brief_md"]),
    ("jiuqian-full",  "forum",            ["insight_md", "summary_md", "expert_content_md", "background_md", "topic_md", "content_md"]),
    ("third-bridge",  "interviews",       ["agenda_md", "specialists_md", "introduction_md", "transcript_md", "commentary_md"]),
]


def _total_expr(fields: list[str]) -> dict[str, Any]:
    """`$expr` fragment summing $strLenCP across body fields + pdf_text_md."""
    terms = [{"$strLenCP": {"$ifNull": [f"${f}", ""]}} for f in fields]
    terms.append({"$strLenCP": {"$ifNull": ["$pdf_text_md", ""]}})
    return {"$add": terms}


def _has_pdf_local_expr() -> dict[str, Any]:
    return {"$gt": [{"$strLenCP": {"$ifNull": ["$pdf_local_path", ""]}}, 0]}


def _pdf_text_len_expr() -> dict[str, Any]:
    return {"$strLenCP": {"$ifNull": ["$pdf_text_md", ""]}}


def build_mark_filter(fields: list[str], threshold: int) -> dict[str, Any]:
    """Match docs that should be NEWLY marked _low_value=True.

    Conditions:
      - not already soft-deleted
      - not already marked _low_value
      - total content < threshold
      - NOT pending-PDF (pdf_local_path missing OR pdf_text_md >= threshold)
    """
    return {
        "deleted": {"$ne": True},
        "_low_value": {"$ne": True},
        "$expr": {
            "$and": [
                {"$lt": [_total_expr(fields), threshold]},
                # exclude pending PDFs
                {"$or": [
                    {"$not": _has_pdf_local_expr()},
                    {"$gte": [_pdf_text_len_expr(), threshold]},
                ]},
            ]
        },
    }


def build_unmark_filter(fields: list[str], threshold: int) -> dict[str, Any]:
    """Match docs CURRENTLY marked _low_value=True whose content has since grown
    above the threshold (typically because the PDF was parsed). Self-heal."""
    return {
        "_low_value": True,
        "$expr": {"$gte": [_total_expr(fields), threshold]},
    }


def specs_filtered(only: list[str], exclude: list[str]) -> list[tuple[str, str, list[str]]]:
    def key(db: str, coll: str) -> str:
        return f"{db}/{coll}"

    out = []
    for db, coll, fields in SPECS:
        k = key(db, coll)
        if only and k not in only:
            continue
        if k in exclude:
            continue
        out.append((db, coll, fields))
    return out


def run_one(client: MongoClient, db: str, coll_name: str, fields: list[str],
            threshold: int, apply: bool) -> dict[str, int]:
    coll = client[db][coll_name]
    if coll_name not in client[db].list_collection_names():
        return {"missing": 1}

    mark_flt = build_mark_filter(fields, threshold)
    unmark_flt = build_unmark_filter(fields, threshold)

    n_to_mark = coll.count_documents(mark_flt)
    n_to_unmark = coll.count_documents(unmark_flt)

    label = f"{db}/{coll_name}"
    print(f"  [{label}] mark={n_to_mark}  unmark(self-heal)={n_to_unmark}")

    # Sample 3 titles being marked, for visibility
    if n_to_mark:
        sample = list(coll.find(mark_flt, {"title": 1}).limit(3))
        for s in sample:
            t = (s.get("title") or "").strip()[:80]
            print(f"      sample-mark: [{s['_id']}] {t!r}")

    if not apply:
        return {"would_mark": n_to_mark, "would_unmark": n_to_unmark}

    now = datetime.now(timezone.utc)
    marked = unmarked = 0

    if n_to_mark:
        # Pipeline-form $set: lets us read $-fields and write computed values.
        # Requires MongoDB 4.2+.
        update_pipeline = [
            {"$set": {
                "_low_value": True,
                "_low_value_at": now,
                "_low_value_chars": _total_expr(fields),
                "_low_value_threshold": threshold,
            }}
        ]
        res = coll.update_many(mark_flt, update_pipeline)
        marked = res.modified_count

    if n_to_unmark:
        res = coll.update_many(
            unmark_flt,
            {"$unset": {
                "_low_value": "",
                "_low_value_at": "",
                "_low_value_chars": "",
                "_low_value_threshold": "",
            }},
        )
        unmarked = res.modified_count

    print(f"      [apply] marked={marked} unmarked={unmarked}")
    return {"marked": marked, "unmarked": unmarked}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mongo-uri", default=MONGO_URI_DEFAULT)
    parser.add_argument("--threshold", type=int, default=100,
                        help="Mark docs with total chars below this (default 100).")
    parser.add_argument("--apply", action="store_true",
                        help="Actually write to Mongo. Default is dry-run.")
    parser.add_argument("--only", action="append", default=[],
                        help="Restrict to specific db/collection (e.g. jinmen-full/oversea_reports). "
                             "Repeatable. Default = all SPECS.")
    parser.add_argument("--exclude", action="append", default=[],
                        help="Skip specific db/collection. Repeatable.")
    args = parser.parse_args()

    os.environ.setdefault("NO_PROXY", "127.0.0.1,localhost")
    os.environ.setdefault("no_proxy", "127.0.0.1,localhost")

    client = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=5_000)

    specs = specs_filtered(args.only, args.exclude)
    if not specs:
        print("[error] no collections matched --only/--exclude")
        sys.exit(2)

    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"[mark_low_value_docs] mode={mode} threshold={args.threshold} collections={len(specs)}")

    grand_marked = grand_unmarked = grand_would_mark = grand_would_unmark = 0
    for db, coll, fields in specs:
        try:
            res = run_one(client, db, coll, fields, args.threshold, args.apply)
        except Exception as e:
            print(f"  [error] {db}/{coll}: {e}")
            continue
        grand_marked += res.get("marked", 0)
        grand_unmarked += res.get("unmarked", 0)
        grand_would_mark += res.get("would_mark", 0)
        grand_would_unmark += res.get("would_unmark", 0)

    print()
    if args.apply:
        print(f"[summary] marked={grand_marked}  unmarked={grand_unmarked}")
    else:
        print(f"[summary] would-mark={grand_would_mark}  would-unmark={grand_would_unmark}")
        print(f"[hint] re-run with --apply to commit.")


if __name__ == "__main__":
    main()
