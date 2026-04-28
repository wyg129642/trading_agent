"""Shared helpers for the 5 IR-filings scrapers.

Mongo target
------------
Single dedicated DB ``ir_filings`` on local ``ta-mongo-crawl`` (127.0.0.1:27018,
no auth). Collections per source:

  sec_edgar              # one doc per accession
  sec_xbrl_facts         # denormalized companyfacts entries (segment-revenue join key)
  hkex                   # one doc per NEWS_ID
  edinet                 # one doc per docID
  tdnet                  # one doc per Yanoshin Tdnet.id
  dart                   # one doc per rcept_no
  dart_fnltt             # structured fnltt line items
  account                # last-seen + auth health per source
  _state                 # checkpoints: {_id: "crawler_<source>_<bucket>", ...}

PDF / artifact storage
----------------------
``/home/ygwang/crawl_data/ir_pdfs/<source>/...`` — disk only, NO GridFS (per
2026-04-27 cutover). ``pdf_local_path`` holds absolute path; the cron
``scripts/extract_pdf_texts.py`` discovers new files and writes ``pdf_text_md``
back onto each Mongo doc.

Schema
------
Every filing doc includes the standard fields below. Source-specific extras
(XBRL refs, bilingual variants, etc) get added on top.

  _id, source, category, category_name, title, title_local
  release_time (ISO), release_time_ms (int)
  period_start, period_end, fiscal_year, fiscal_period
  organization, ticker_local, ticker_canonical, lang
  doc_introduce, content_md
  pdf_rel_path, pdf_local_path, pdf_size_bytes, pdf_download_error, pdf_unavailable
  attachments[], xbrl_data_path, xbrl_summary{}
  list_item{} (raw upstream payload)
  web_url, stats{}, crawled_at
  _canonical_tickers[]   # stamped via crawl/ticker_tag.py

The unified shape is what makes ``kb_vector_ingest`` Just Work — it already
reads ``content_md`` / ``pdf_text_md`` / ``_canonical_tickers`` / ``release_time``
across collections regardless of platform.
"""
from __future__ import annotations

import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

# Repo root → backend imports
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

# Crawl helpers
_CRAWL_ROOT = Path(__file__).resolve().parent.parent
if str(_CRAWL_ROOT) not in sys.path:
    sys.path.insert(0, str(_CRAWL_ROOT))

# Clash on :7890 silently intercepts local-LAN TCP when NO_PROXY doesn't cover
# the host (cf. infra_proxy memory). 127.0.0.1 + LAN must be in NO_PROXY before
# we touch pymongo, requests-to-jumpbox, or any local socket.
os.environ["NO_PROXY"] = (
    os.environ.get("NO_PROXY", "")
    + ",127.0.0.1,localhost,192.168.31.0/24,192.168.31.176,192.168.31.224"
)
os.environ["no_proxy"] = os.environ["NO_PROXY"]

from pymongo import ASCENDING, DESCENDING, MongoClient  # noqa: E402
from pymongo.collection import Collection  # noqa: E402
from pymongo.database import Database  # noqa: E402

from ticker_tag import stamp as _stamp_ticker  # noqa: E402

# ============================================================
# Constants
# ============================================================

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://127.0.0.1:27018")
DB_NAME = os.environ.get("IR_FILINGS_DB", "ir_filings")

PDF_ROOT = Path(os.environ.get("IR_PDF_ROOT", "/home/ygwang/crawl_data/ir_pdfs"))

# Source → collection name (filings)
COLLECTION_FOR_SOURCE = {
    "sec_edgar": "sec_edgar",
    "hkex":      "hkex",
    "edinet":    "edinet",
    "tdnet":     "tdnet",
    "dart":      "dart",
    "asx":       "asx",
    "ir_pages":  "ir_pages",
}

# Auxiliary collections (structured XBRL / fnltt line items)
AUX_COLLECTIONS = {
    "sec_xbrl_facts",
    "dart_fnltt",
    "account",
    "_state",
}

# A single connection lasts the whole scraper process (pymongo manages its own pool)
_MONGO_CLIENT: Optional[MongoClient] = None


def get_db() -> Database:
    global _MONGO_CLIENT
    if _MONGO_CLIENT is None:
        _MONGO_CLIENT = MongoClient(
            MONGO_URI,
            serverSelectionTimeoutMS=10_000,
            connectTimeoutMS=10_000,
            socketTimeoutMS=120_000,
            tz_aware=True,
        )
        # Probe so failures fail fast at startup, not on first write
        _MONGO_CLIENT.admin.command("ping")
    return _MONGO_CLIENT[DB_NAME]


def get_collection(source: str) -> Collection:
    coll_name = COLLECTION_FOR_SOURCE.get(source)
    if not coll_name:
        raise ValueError(f"unknown ir_filings source: {source!r}")
    return get_db()[coll_name]


# ============================================================
# Indexes
# ============================================================

def ensure_indexes(source: str) -> None:
    """Idempotent — pymongo no-ops if the index exists. Called once at scraper
    startup. Aligned across sources so backend/frontend filters all behave the
    same."""
    db = get_db()
    coll = db[COLLECTION_FOR_SOURCE[source]]
    # release_time_ms desc — primary sort for "recent filings" listings
    coll.create_index([("release_time_ms", DESCENDING)], background=True)
    # ticker_canonical — per-ticker per-hub aggregator
    coll.create_index([("ticker_canonical", ASCENDING),
                       ("release_time_ms", DESCENDING)], background=True)
    # _canonical_tickers (multi-key, set by ticker_tag.stamp) — kb_search filter
    coll.create_index([("_canonical_tickers", ASCENDING),
                       ("release_time_ms", DESCENDING)], background=True)
    # category (form / type code) — UI filter
    coll.create_index([("category", ASCENDING),
                       ("release_time_ms", DESCENDING)], background=True)
    # crawled_at — ops queries (which docs got pulled today?)
    coll.create_index([("crawled_at", DESCENDING)], background=True)


# ============================================================
# State / checkpoint
# ============================================================

def load_state(source: str, bucket: str = "default") -> dict:
    """Read crawler checkpoint. ``bucket`` distinguishes per-form / per-ticker
    state (e.g. SEC stores `crawler_sec_edgar_INTC` for per-ticker last-seen
    accession)."""
    state_id = f"crawler_{source}_{bucket}"
    return get_db()["_state"].find_one({"_id": state_id}) or {}


def save_state(source: str, bucket: str = "default", **kwargs) -> None:
    state_id = f"crawler_{source}_{bucket}"
    kwargs["updated_at"] = datetime.now(timezone.utc)
    get_db()["_state"].update_one(
        {"_id": state_id}, {"$set": kwargs}, upsert=True,
    )


def record_daily_stat(source: str, bucket: str, *,
                      added: int = 0, skipped: int = 0,
                      errors: int = 0, pdfs: int = 0) -> None:
    """Per-day counters keyed by (source, bucket, YYYY-MM-DD). Mirrors how the
    8 existing crawlers record daily stats — feeds crawler_monitor health."""
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    state_id = f"daily_{source}_{bucket}_{day}"
    get_db()["_state"].update_one(
        {"_id": state_id},
        {"$inc": {"added": added, "skipped": skipped,
                  "errors": errors, "pdfs": pdfs},
         "$set": {"date": day, "source": source, "bucket": bucket,
                  "updated_at": datetime.now(timezone.utc)}},
        upsert=True,
    )


# ============================================================
# PDF storage paths
# ============================================================

def pdf_dir(source: str) -> Path:
    """``/home/ygwang/crawl_data/ir_pdfs/<source>/``. Created on first call."""
    p = PDF_ROOT / source
    p.mkdir(parents=True, exist_ok=True)
    return p


def safe_filename(s: str, max_len: int = 120) -> str:
    """Turn an arbitrary title into a filesystem-safe name. Mirrors what
    alphaengine/scraper.py uses, kept self-contained so this module has no
    inbound deps from other crawlers."""
    s = re.sub(r"[\\/:*?\"<>|]+", "_", s or "")
    s = re.sub(r"\s+", "_", s).strip("_")
    return s[:max_len] or "untitled"


# ============================================================
# Schema normalizer
# ============================================================

# These keys MUST exist on every doc that lands in any of the 5 collections so
# that downstream consumers (kb_vector_ingest, extract_pdf_texts, ticker_tag
# enrich cron, the `/api/<source>-db/...` mirror endpoints) can run unchanged.
_REQUIRED_FIELDS = {
    "_id", "source", "category", "category_name",
    "title", "release_time", "release_time_ms",
    "organization", "ticker_local", "ticker_canonical",
    "lang", "doc_introduce", "content_md",
    "pdf_rel_path", "pdf_local_path", "pdf_size_bytes",
    "pdf_download_error", "pdf_unavailable",
    "list_item", "web_url", "stats", "crawled_at",
}


def make_filing_doc(
    *,
    doc_id: str,
    source: str,
    category: str,
    category_name: str,
    title: str,
    release_time_ms: int,
    organization: str,
    ticker_local: str,
    ticker_canonical: str,
    list_item: dict,
    title_local: str = "",
    period_start: Optional[str] = None,
    period_end: Optional[str] = None,
    fiscal_year: Optional[int] = None,
    fiscal_period: Optional[str] = None,
    lang: str = "en",
    doc_introduce: str = "",
    content_md: str = "",
    pdf_rel_path: str = "",
    pdf_local_path: str = "",
    pdf_size_bytes: int = 0,
    pdf_download_error: str = "",
    pdf_unavailable: bool = False,
    attachments: Optional[list] = None,
    xbrl_data_path: str = "",
    xbrl_summary: Optional[dict] = None,
    web_url: str = "",
    extra: Optional[dict] = None,
) -> dict:
    """Build a unified-schema filing doc. Caller is expected to ``replace_one``
    using the returned dict with ``_id`` as the key.

    Why a builder instead of letting each scraper hand-roll dicts: the schema is
    load-bearing for every downstream tool; a forgotten field would silently
    break ``kb_vector_ingest`` selection or the per-ticker hub aggregator."""
    release_time_iso = datetime.fromtimestamp(release_time_ms / 1000, tz=timezone.utc).isoformat()

    doc = {
        "_id": doc_id,
        "source": source,
        "category": category,
        "category_name": category_name,
        "title": title,
        "title_local": title_local,
        "release_time": release_time_iso,
        "release_time_ms": int(release_time_ms),
        "period_start": period_start,
        "period_end": period_end,
        "fiscal_year": fiscal_year,
        "fiscal_period": fiscal_period,
        "organization": organization,
        "ticker_local": ticker_local,
        "ticker_canonical": ticker_canonical,
        "lang": lang,
        "doc_introduce": doc_introduce,
        "content_md": content_md or "",
        "pdf_rel_path": pdf_rel_path,
        "pdf_local_path": pdf_local_path,
        "pdf_size_bytes": int(pdf_size_bytes or 0),
        "pdf_download_error": pdf_download_error or "",
        "pdf_unavailable": bool(pdf_unavailable),
        "attachments": attachments or [],
        "xbrl_data_path": xbrl_data_path,
        "xbrl_summary": xbrl_summary or {},
        "list_item": list_item,
        "web_url": web_url,
        "stats": {
            "content_chars": len(content_md or ""),
            "pdf_size": int(pdf_size_bytes or 0),
            "attachment_count": len(attachments or []),
        },
        "crawled_at": datetime.now(timezone.utc),
    }
    if extra:
        doc.update(extra)
    # Sanity: every required key present
    missing = _REQUIRED_FIELDS - set(doc.keys())
    if missing:
        raise RuntimeError(f"make_filing_doc missing fields: {missing}")
    return doc


def upsert_filing(coll: Collection, doc: dict) -> None:
    """Standard upsert + ticker stamp.

    For IR filings we *already know* the canonical ticker (one doc = one
    issuer), so we set ``_canonical_tickers`` directly from
    ``ticker_canonical`` rather than relying on ``ticker_tag.stamp`` text
    extraction (which has no per-source extractor registered for the new IR
    sources and would fail-open to empty list).

    We still call stamp() afterwards so any *additional* tickers mentioned in
    the body (e.g. an INTC 8-K mentioning AAPL as a customer) get picked up
    by the standard alias / extractor pipeline once the body lands in
    ``content_md`` / ``pdf_text_md``.
    """
    primary = doc.get("ticker_canonical") or ""
    if primary:
        doc["_canonical_tickers"] = [primary]
        doc["_canonical_tickers_at"] = datetime.now(timezone.utc)
        doc["_canonical_extract_source"] = "ir_filings_known_issuer"
    try:
        _stamp_ticker(doc, doc["source"], coll)
    except Exception:
        pass                              # fail-open; primary already set
    coll.replace_one({"_id": doc["_id"]}, doc, upsert=True)


# ============================================================
# Logging
# ============================================================

def setup_logging(source: str, level: int = logging.INFO) -> logging.Logger:
    """File + stdout logger at ``crawl/<source>/logs/scraper.log``. Mirrors
    the existing 8 crawlers — keeps per-platform log files independently
    rotatable / tailable."""
    log_dir = _CRAWL_ROOT / source / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "scraper.log"

    logger = logging.getLogger(f"ir_filings.{source}")
    logger.setLevel(level)
    if logger.handlers:
        return logger
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S",
    )
    fh = logging.handlers.RotatingFileHandler(
        log_path, maxBytes=20_000_000, backupCount=10, encoding="utf-8",
    ) if hasattr(logging, "handlers") else logging.FileHandler(log_path)
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    return logger


# late import to avoid circular at module load
import logging.handlers  # noqa: E402


__all__ = [
    "MONGO_URI", "DB_NAME", "PDF_ROOT", "COLLECTION_FOR_SOURCE",
    "get_db", "get_collection", "ensure_indexes",
    "load_state", "save_state", "record_daily_stat",
    "pdf_dir", "safe_filename",
    "make_filing_doc", "upsert_filing",
    "setup_logging",
]
