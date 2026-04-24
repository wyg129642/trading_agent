#!/usr/bin/env python3
"""Import Meritco research data from local files into MongoDB.

The secondary scraper `meritco_crawl/research_crawler.py` (not the primary
forum scraper) writes its output to local JSON + PDF files at
`/home/ygwang/trading_agent/meritco_crawl/data/research/` instead of MongoDB.
This script walks that tree once and upserts each item into a new
`meritco.research` collection so it shows up alongside `meritco.forum`.

Source layout:
    data/research/
      lists/<category>/page_XXXX.json    — list responses (summary fields)
      details/<articleId>.json           — full detail responses
      pdfs/<articleId>.pdf               — downloaded PDFs (subset)

The script is idempotent; re-running upserts by articleId.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from pymongo import MongoClient, ASCENDING, DESCENDING

RESEARCH_ROOT = Path("/home/ygwang/trading_agent/meritco_crawl/data/research")
MONGO_URI_DEFAULT = "mongodb://localhost:27017"
MONGO_DB_DEFAULT = "meritco"
COLLECTION = "research"


def _find_items(obj, depth: int = 0):
    """Find the first list-of-dicts-with-articleId inside a nested response."""
    if depth > 6:
        return []
    if isinstance(obj, list) and obj and isinstance(obj[0], dict) and (
        "articleId" in obj[0] or "id" in obj[0]
    ):
        return obj
    if isinstance(obj, dict):
        for v in obj.values():
            r = _find_items(v, depth + 1)
            if r:
                return r
    return []


def build_category_map(root: Path) -> dict[str, str]:
    """Return {articleId -> category_key} for every list item on disk."""
    out: dict[str, str] = {}
    lists_dir = root / "lists"
    if not lists_dir.is_dir():
        return out
    for cat_dir in sorted(lists_dir.iterdir()):
        if not cat_dir.is_dir():
            continue
        for page_file in sorted(cat_dir.glob("*.json")):
            try:
                data = json.load(open(page_file))
            except (OSError, ValueError) as e:
                print(f"  skip {page_file.name}: {e}", file=sys.stderr)
                continue
            for item in _find_items(data):
                aid = item.get("articleId") or item.get("id")
                if aid:
                    out[str(aid)] = cat_dir.name
    return out


def build_list_item_map(root: Path) -> dict[str, dict]:
    """Return {articleId -> list item dict}. Needed when detail is missing."""
    out: dict[str, dict] = {}
    lists_dir = root / "lists"
    if not lists_dir.is_dir():
        return out
    for cat_dir in sorted(lists_dir.iterdir()):
        if not cat_dir.is_dir():
            continue
        for page_file in sorted(cat_dir.glob("*.json")):
            try:
                data = json.load(open(page_file))
            except (OSError, ValueError):
                continue
            for item in _find_items(data):
                aid = item.get("articleId") or item.get("id")
                if aid:
                    out[str(aid)] = item
    return out


def parse_date(d: str | None):
    """articleDate is 'YYYY-MM-DD'. Convert to release_time/release_time_ms."""
    if not d:
        return None, None
    s = str(d).strip()
    if len(s) < 10:
        return s, None
    try:
        dt = datetime.strptime(s[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return s[:16] if len(s) >= 16 else s[:10], int(dt.timestamp() * 1000)
    except ValueError:
        return s, None


def build_doc(
    article_id: str,
    category: str,
    detail_result: dict | None,
    list_item: dict | None,
    pdf_path: Path | None,
) -> dict:
    """Construct the MongoDB document. Fields mirror meritco.forum conventions
    so the frontend/backend can reuse the same patterns."""
    source = detail_result or list_item or {}
    release_time, release_time_ms = parse_date(source.get("articleDate"))

    summary_array = source.get("summaryArray") or []
    paragraph_array = source.get("paragraphArray") or []
    paragraph_titles = source.get("paragraphTitleArray") or []
    points_array = source.get("pointsArray") or []
    points_titles = source.get("pointsTitleArray") or []

    # Build markdown views by combining titled sections
    def to_md(titles: list, bodies: list) -> str:
        if not bodies:
            return ""
        out: list[str] = []
        for i, body in enumerate(bodies):
            title = titles[i] if i < len(titles) else ""
            if title:
                out.append(f"### {title}\n")
            out.append(str(body))
        return "\n\n".join(out).strip()

    summary_md = "\n\n".join(str(x) for x in summary_array).strip()
    paragraph_md = to_md(paragraph_titles, paragraph_array)
    points_md = to_md(points_titles, points_array)

    # Combined "content" for drawer 正文 tab
    if paragraph_md and points_md:
        content_md = f"{points_md}\n\n---\n\n{paragraph_md}"
    else:
        content_md = paragraph_md or points_md

    doc = {
        "_id": str(article_id),
        "id": str(article_id),
        "article_id": str(article_id),
        "category": category,
        "title": source.get("title") or "",
        "release_time": release_time,
        "release_time_ms": release_time_ms,
        "article_date": source.get("articleDate"),
        "platform": source.get("platform") or "",
        "source": source.get("source") or "",
        "type": source.get("type") or "",
        "tag1": source.get("tag1") or "",
        "tag2": source.get("tag2") or "",
        "keyword_arr": source.get("keywordArr") or [],
        "company_kg": source.get("companyKgList") or [],
        "industry_kg": source.get("industryKgList") or [],

        # Text views
        "summary": source.get("summary") or "",
        "summary_md": summary_md,
        "content_md": content_md,
        "paragraph_md": paragraph_md,
        "points_md": points_md,

        # PDF (from secondary scraper's earlier run)
        "pdf_local_path": str(pdf_path) if pdf_path and pdf_path.is_file() else None,
        "pdf_size_bytes": pdf_path.stat().st_size if pdf_path and pdf_path.is_file() else 0,
        "has_pdf": bool(pdf_path and pdf_path.is_file()),
        "download_link": source.get("downloadLink") or "",

        # Raw responses preserved for later reprocessing
        "list_item": list_item or {},
        "detail_result": detail_result or {},

        "stats": {
            "摘要字数": sum(len(str(x)) for x in summary_array),
            "正文字数": sum(len(str(x)) for x in paragraph_array) + sum(len(str(x)) for x in points_array),
            "关键词数": len(source.get("keywordArr") or []),
            "段落数": len(paragraph_array),
        },
        "imported_at": datetime.now(timezone.utc),
        "crawled_at": datetime.now(timezone.utc),
    }
    return doc


def main():
    p = argparse.ArgumentParser(description="Import Meritco research into MongoDB")
    p.add_argument("--root", default=str(RESEARCH_ROOT),
                   help="Local research data root")
    p.add_argument("--mongo-uri", default=MONGO_URI_DEFAULT)
    p.add_argument("--mongo-db", default=MONGO_DB_DEFAULT)
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    root = Path(args.root)
    if not root.is_dir():
        print(f"ERROR: root {root} not found", file=sys.stderr)
        sys.exit(1)

    print(f"[import] scanning {root}")
    cat_map = build_category_map(root)
    list_map = build_list_item_map(root)
    print(f"  lists: {len(cat_map)} unique articleIds across "
          f"{len(set(cat_map.values()))} categories")

    detail_dir = root / "details"
    detail_files = sorted(detail_dir.glob("*.json"))
    print(f"  details: {len(detail_files)} files")

    pdf_dir = root / "pdfs"
    pdf_files = {p.stem: p for p in pdf_dir.glob("*.pdf")} if pdf_dir.is_dir() else {}
    print(f"  pdfs: {len(pdf_files)} files")

    # Compute all articleIds we have (union of list + detail)
    all_ids: set[str] = set(cat_map.keys())
    for f in detail_files:
        all_ids.add(f.stem)
    print(f"[import] total articleIds to upsert: {len(all_ids)}")

    client = MongoClient(args.mongo_uri)
    db = client[args.mongo_db]
    coll = db[COLLECTION]

    if not args.dry_run:
        coll.create_index("release_time")
        coll.create_index("article_date")
        coll.create_index("category")
        coll.create_index("platform")

    added = updated = skipped = 0
    no_detail_count = 0
    with_pdf_count = 0

    for aid in sorted(all_ids):
        detail_file = detail_dir / f"{aid}.json"
        detail_result = None
        if detail_file.is_file():
            try:
                resp = json.load(open(detail_file))
                # Unwrap: real content is under "result" for API responses,
                # but some files may already be unwrapped
                detail_result = resp.get("result") if isinstance(resp, dict) and "result" in resp else resp
            except (OSError, ValueError) as e:
                print(f"  skip detail {aid}: {e}", file=sys.stderr)
        else:
            no_detail_count += 1

        list_item = list_map.get(aid)
        category = cat_map.get(aid, "unknown")
        pdf_path = pdf_files.get(aid)
        if pdf_path:
            with_pdf_count += 1

        doc = build_doc(aid, category, detail_result, list_item, pdf_path)
        if args.dry_run:
            print(f"  [dry] {aid} cat={category} title={doc['title'][:50]!r} "
                  f"pdf={'yes' if pdf_path else 'no'} "
                  f"detail={'yes' if detail_result else 'no'}")
            continue

        existing = coll.find_one({"_id": aid}, {"_id": 1})
        if existing:
            coll.replace_one({"_id": aid}, doc)
            updated += 1
        else:
            coll.insert_one(doc)
            added += 1

    print("")
    print(f"[import] complete: added={added} updated={updated} skipped={skipped}")
    print(f"  items without detail JSON: {no_detail_count}")
    print(f"  items with PDF: {with_pdf_count}")
    if not args.dry_run:
        print(f"  collection `{args.mongo_db}.{COLLECTION}` total: "
              f"{coll.estimated_document_count()}")

    client.close()


if __name__ == "__main__":
    main()
