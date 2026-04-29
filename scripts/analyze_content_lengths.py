"""Read-only character-length distribution across StockHub-visible MongoDB
collections.

Mirrors the body_sections + pdf_text_md mapping declared in
backend/app/api/stock_hub.py::SOURCES (lines 159-498). For each collection we
sum $strLenCP across all body_sections fields *plus* pdf_text_md, then bucket
the result. This is the same "renderable content size" measure the marker
script uses, so the two stay in lockstep.

What's excluded from the histogram:
  - already-soft-deleted docs (deleted=True)
  - PDF-pending docs (pdf_local_path exists AND pdf_text_md still <100 chars).
    These will heal once extract_pdf_texts.py covers them; counting them as
    "low-value" right now would wrongly hide ~98k jinmen/oversea_reports +
    1.4k SEC EDGAR filings whose PDFs are queued for parse.
  - ir_filings/* (per the plan: data structures here lean heavily on PDF, and
    are excluded from the marking pass too — listed only as dashes here).

Usage:
    PYTHONPATH=. python3 scripts/analyze_content_lengths.py
    PYTHONPATH=. python3 scripts/analyze_content_lengths.py --json out.json
    PYTHONPATH=. python3 scripts/analyze_content_lengths.py --threshold 100
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

from pymongo import MongoClient


MONGO_URI_DEFAULT = os.environ.get("MONGO_URI", "mongodb://127.0.0.1:27018/")

# (db, collection, body_fields). Mirrors stock_hub.py::SOURCES.body_sections.
# Keep this in sync when a new source lands.
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

# ir_filings/* skipped from histogram (per plan: heavy PDF dependence makes
# raw body-length stats misleading until extract pipeline catches up).
IR_FILINGS_SPECS: list[tuple[str, str, list[str]]] = [
    ("ir_filings", "sec_edgar", ["content_md"]),
    ("ir_filings", "hkex",      ["content_md"]),
    ("ir_filings", "asx",       ["content_md"]),
    ("ir_filings", "edinet",    ["content_md"]),
    ("ir_filings", "tdnet",     ["content_md"]),
    ("ir_filings", "dart",      ["content_md"]),
    ("ir_filings", "ir_pages",  ["content_md"]),
]

BUCKET_BOUNDARIES = [0, 1, 50, 100, 200, 500, 1_000, 5_000, 10_000, 50_000, 1_000_000]
BUCKET_LABELS = ["=0", "1-50", "50-100", "100-200", "200-500", "500-1k", "1k-5k", "5k-10k", "10k-50k", "50k+"]


def build_pipeline(fields: list[str], pending_pdf_threshold: int) -> list[dict[str, Any]]:
    """Aggregation: project total content length, exclude pending PDFs and soft-deleted, then bucket.

    pending_pdf_threshold: if pdf_local_path exists AND pdf_text_md length is
    below this, the doc is "PDF awaiting parse" and excluded from the histogram.
    """
    body_terms = [{"$strLenCP": {"$ifNull": [f"${f}", ""]}} for f in fields]
    body_terms.append({"$strLenCP": {"$ifNull": ["$pdf_text_md", ""]}})

    return [
        {"$match": {"deleted": {"$ne": True}}},
        {"$project": {
            "total":        {"$add": body_terms},
            "pdf_text_len": {"$strLenCP": {"$ifNull": ["$pdf_text_md", ""]}},
            "has_pdf":      {"$cond": [{"$gt": [{"$strLenCP": {"$ifNull": ["$pdf_local_path", ""]}}, 0]}, 1, 0]},
        }},
        # Drop pending-PDF docs from the histogram (per user: "PDF 没解析的可以先不用算入统计")
        {"$match": {
            "$nor": [
                {"has_pdf": 1, "pdf_text_len": {"$lt": pending_pdf_threshold}},
            ]
        }},
        {"$facet": {
            "hist": [
                {"$bucket": {
                    "groupBy":    "$total",
                    "boundaries": BUCKET_BOUNDARIES,
                    "default":    "huge",
                    "output":     {"n": {"$sum": 1}},
                }}
            ],
            "tot":   [{"$count": "n"}],
            "lt50":  [{"$match": {"total": {"$lt": 50}}},  {"$count": "n"}],
            "lt100": [{"$match": {"total": {"$lt": 100}}}, {"$count": "n"}],
            "lt200": [{"$match": {"total": {"$lt": 200}}}, {"$count": "n"}],
            "lt500": [{"$match": {"total": {"$lt": 500}}}, {"$count": "n"}],
        }},
    ]


def count_pending(coll, fields: list[str], pending_threshold: int) -> int:
    """Count pending-PDF docs (excluded from histogram). Reported separately."""
    return coll.count_documents({
        "deleted": {"$ne": True},
        "pdf_local_path": {"$exists": True, "$nin": [None, ""]},
        "$expr": {
            "$lt": [
                {"$strLenCP": {"$ifNull": ["$pdf_text_md", ""]}},
                pending_threshold,
            ]
        },
    })


def run_one(client: MongoClient, db: str, coll_name: str, fields: list[str],
            pending_threshold: int) -> dict[str, Any]:
    coll = client[db][coll_name]
    if coll_name not in client[db].list_collection_names():
        return {"db": db, "coll": coll_name, "missing": True}
    pipeline = build_pipeline(fields, pending_threshold)
    res = list(coll.aggregate(pipeline, allowDiskUse=True, maxTimeMS=300_000))
    if not res:
        return {"db": db, "coll": coll_name, "total": 0}
    r = res[0]
    total = r["tot"][0]["n"] if r["tot"] else 0
    bm = {h["_id"]: h["n"] for h in r["hist"]}
    pending = count_pending(coll, fields, pending_threshold)
    return {
        "db": db, "coll": coll_name, "fields": fields,
        "total_after_filter": total,
        "pending_pdf_excluded": pending,
        "buckets": {
            "=0":      bm.get(0, 0),
            "1-50":    bm.get(1, 0),
            "50-100":  bm.get(50, 0),
            "100-200": bm.get(100, 0),
            "200-500": bm.get(200, 0),
            "500-1k":  bm.get(500, 0),
            "1k-5k":   bm.get(1_000, 0),
            "5k-10k":  bm.get(5_000, 0),
            "10k-50k": bm.get(10_000, 0),
            "50k+":    bm.get(50_000, 0) + bm.get("huge", 0),
        },
        "lt50":  r["lt50"][0]["n"]  if r["lt50"]  else 0,
        "lt100": r["lt100"][0]["n"] if r["lt100"] else 0,
        "lt200": r["lt200"][0]["n"] if r["lt200"] else 0,
        "lt500": r["lt500"][0]["n"] if r["lt500"] else 0,
    }


def fmt_table(rows: list[dict[str, Any]]) -> str:
    """Print a fixed-width table to stdout."""
    out = []
    headers = ["db/coll", "total", *BUCKET_LABELS, "<50", "<100", "<200", "<500", "pending"]
    fmt = "{:<40s} | " + " | ".join(["{:>7s}"] + ["{:>7s}"] * len(BUCKET_LABELS) + ["{:>6s}", "{:>6s}", "{:>6s}", "{:>6s}", "{:>8s}"])
    out.append(fmt.format(*headers))
    out.append("-" * 200)
    fmt_row = "{:<40s} | " + " | ".join(["{:>7d}"] + ["{:>7d}"] * len(BUCKET_LABELS) + ["{:>6d}", "{:>6d}", "{:>6d}", "{:>6d}", "{:>8d}"])
    for row in rows:
        if row.get("missing"):
            out.append(f"{row['db']}/{row['coll']:<40s} (collection missing)")
            continue
        if row.get("total_after_filter", 0) == 0 and row.get("pending_pdf_excluded", 0) == 0:
            out.append(f"{(row['db']+'/'+row['coll']):<40s} | (empty)")
            continue
        bk = row["buckets"]
        out.append(fmt_row.format(
            f"{row['db']}/{row['coll']}",
            row["total_after_filter"],
            *(bk[lbl] for lbl in BUCKET_LABELS),
            row["lt50"], row["lt100"], row["lt200"], row["lt500"],
            row["pending_pdf_excluded"],
        ))
    return "\n".join(out)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mongo-uri", default=MONGO_URI_DEFAULT)
    parser.add_argument("--threshold", type=int, default=100,
                        help="Highlight count of docs below this length (default 100).")
    parser.add_argument("--pending-pdf-threshold", type=int, default=100,
                        help="If pdf_local_path exists AND pdf_text_md < this, treat as pending and exclude.")
    parser.add_argument("--include-ir-filings", action="store_true",
                        help="Also analyse ir_filings/* (skipped by default — heavy PDF reliance).")
    parser.add_argument("--json", dest="json_out",
                        help="Optional path to write JSON results (in addition to stdout).")
    args = parser.parse_args()

    # Pin no_proxy for local mongo (Clash on 7890 swallows otherwise)
    os.environ.setdefault("NO_PROXY", "127.0.0.1,localhost")
    os.environ.setdefault("no_proxy", "127.0.0.1,localhost")

    client = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=5_000)

    specs = list(SPECS)
    if args.include_ir_filings:
        specs.extend(IR_FILINGS_SPECS)

    rows: list[dict[str, Any]] = []
    for db, coll, fields in specs:
        print(f"  [scanning] {db}/{coll} ...", file=sys.stderr)
        try:
            rows.append(run_one(client, db, coll, fields, args.pending_pdf_threshold))
        except Exception as e:
            print(f"  [error] {db}/{coll}: {e}", file=sys.stderr)
            rows.append({"db": db, "coll": coll, "error": str(e)})

    print(fmt_table(rows))
    print()
    print(f"Legend: total = sum(body_sections fields) + len(pdf_text_md). PDF-pending excluded "
          f"(pdf_local_path AND pdf_text_md < {args.pending_pdf_threshold}).")
    print(f"<{args.threshold} = candidate low-value count after pending-PDF exclusion.")

    grand_low = sum(r.get("lt100", 0) for r in rows if "error" not in r)
    grand_total = sum(r.get("total_after_filter", 0) for r in rows if "error" not in r)
    grand_pending = sum(r.get("pending_pdf_excluded", 0) for r in rows if "error" not in r)
    print(f"\nGrand totals (excluding ir_filings unless --include-ir-filings):")
    print(f"  scannable docs:           {grand_total:>10d}")
    print(f"  pending-PDF (excluded):   {grand_pending:>10d}")
    print(f"  low-value (<{args.threshold} chars):     {grand_low:>10d}  ({100*grand_low/max(grand_total,1):.2f}%)")

    if args.json_out:
        with open(args.json_out, "w", encoding="utf-8") as f:
            json.dump({
                "threshold": args.threshold,
                "pending_pdf_threshold": args.pending_pdf_threshold,
                "rows": rows,
            }, f, ensure_ascii=False, indent=2, default=str)
        print(f"\n[wrote] {args.json_out}")


if __name__ == "__main__":
    main()
