"""Ingest SemiAnalysis / Funda AI PDFs into MongoDB.

Layout matches the existing crawl-platform convention (CLAUDE.md):
- PDFs live on disk at /home/ygwang/crawl_data/semianalysis_pdfs/
- One MongoDB document per PDF referencing the file by path, plus extracted text
- DB: semianalysis,  Collection: semianalysis

Idempotent: sha256 of the file is the _id, so re-runs upsert in place.
"""
from __future__ import annotations

import hashlib
import re
import sys
import os
from datetime import datetime, timezone
from pathlib import Path

from pymongo import MongoClient, ASCENDING
from pypdf import PdfReader

PDF_ROOT = Path("/home/ygwang/crawl_data/semianalysis_pdfs")
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://127.0.0.1:27018/")
DB_NAME = "foreign-website"
COLL_NAME = "semianalysis_posts"


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def clean_title(filename: str) -> str:
    stem = Path(filename).stem
    stem = re.sub(r"^\[Funda AI\]\s*", "", stem)
    stem = stem.replace("_", ":").replace("–", "-")
    return re.sub(r"\s+", " ", stem).strip()


def infer_source(filename: str) -> str:
    if filename.startswith("[Funda AI]"):
        return "Funda AI"
    return "SemiAnalysis"


def extract_text(path: Path) -> tuple[str, int, str | None]:
    """Return (text, num_pages, error)."""
    try:
        reader = PdfReader(str(path))
        pages = reader.pages
        chunks: list[str] = []
        for page in pages:
            try:
                chunks.append(page.extract_text() or "")
            except Exception as e:  # per-page failure should not kill the file
                chunks.append(f"[PAGE_EXTRACT_ERROR: {e}]")
        return "\n\n".join(chunks), len(pages), None
    except Exception as e:
        return "", 0, f"{type(e).__name__}: {e}"


def build_doc(path: Path) -> dict:
    size = path.stat().st_size
    sha = sha256_of(path)
    text, n_pages, err = extract_text(path)
    now = datetime.now(timezone.utc)
    return {
        "_id": sha,
        "title": clean_title(path.name),
        "filename": path.name,
        "source": infer_source(path.name),
        "pdf_local_path": str(path.resolve()),
        "pdf_rel_path": f"semianalysis_pdfs/{path.name}",
        "pdf_size_bytes": size,
        "sha256": sha,
        "num_pages": n_pages,
        "content_text": text,
        "content_chars": len(text),
        "extraction_error": err,
        "imported_at": now,
        "release_time": None,  # not carried in filename; leave for later enrichment
        "release_time_ms": None,
    }


def main() -> int:
    if not PDF_ROOT.is_dir():
        print(f"PDF root missing: {PDF_ROOT}", file=sys.stderr)
        return 2

    pdfs = sorted([p for p in PDF_ROOT.iterdir()
                   if p.is_file() and p.suffix.lower() == ".pdf"])
    if not pdfs:
        print("No PDFs found.", file=sys.stderr)
        return 2

    client = MongoClient(MONGO_URI)
    coll = client[DB_NAME][COLL_NAME]
    coll.create_index([("sha256", ASCENDING)], unique=True, name="uniq_sha256")
    coll.create_index([("source", ASCENDING), ("title", ASCENDING)],
                      name="source_title")
    coll.create_index([("filename", ASCENDING)], name="filename")

    inserted = updated = errored = 0
    for i, path in enumerate(pdfs, 1):
        doc = build_doc(path)
        result = coll.replace_one({"_id": doc["_id"]}, doc, upsert=True)
        if result.upserted_id is not None:
            inserted += 1
            action = "INSERT"
        else:
            updated += 1
            action = "UPDATE"
        if doc["extraction_error"]:
            errored += 1
        print(f"[{i:>2}/{len(pdfs)}] {action:<6} "
              f"pages={doc['num_pages']:>4} "
              f"chars={doc['content_chars']:>7} "
              f"size={doc['pdf_size_bytes']/1024/1024:>6.2f}MB  "
              f"{doc['filename']}")

    print("---")
    print(f"Total PDFs: {len(pdfs)}  inserted={inserted}  updated={updated}  "
          f"extraction_errors={errored}")
    print(f"Collection: {DB_NAME}.{COLL_NAME}  count="
          f"{coll.count_documents({})}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
