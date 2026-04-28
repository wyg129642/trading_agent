"""REST API exposing the `ir_filings` MongoDB — exchange disclosure crawlers
(SEC EDGAR / HKEXnews / EDINET / TDnet / DART / ASX / IR pages).

Single mirror endpoint covering all 7 sub-collections because the schema is
unified (see ``crawl/ir_filings/common.py::make_filing_doc``). Saves writing
seven near-identical `_db.py` files.

Routes (all `/api/ir-filings-db/*`):

  GET /sources                      — list registered sources + counts
  GET /stats                         — dashboard summary (total/today/per-source/recent)
  GET /sources/{source}              — list filings (paginated, filterable by ticker / category / date / price-sensitive)
  GET /sources/{source}/{id}         — full filing details (incl. attachments, list_item, segment_info_text)
  GET /sources/{source}/{id}/pdf     — stream the canonical PDF (or HTML for DART)
  GET /xbrl/{ticker}                 — SEC companyfacts: per-ticker XBRL fact rows for revenue modeling
  GET /fnltt/{ticker}                — DART fnltt: per-ticker structured FS line items

Why one router covers all sources: the unified schema means `filings_summary`
+ `filings_full` projections are identical across sources; only the source
slug is variable. Adding a new source = `SOURCES` map entry + scraper.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pydantic import BaseModel

from backend.app.config import get_settings
from backend.app.deps import get_current_user
from backend.app.models.user import User

logger = logging.getLogger(__name__)
router = APIRouter()


# Source slug → (collection name, label, pdf_root_attr)
SOURCES: dict[str, dict[str, str]] = {
    "sec_edgar": {"collection": "sec_edgar", "label": "SEC EDGAR (US)",       "pdf_root_attr": "sec_edgar_pdf_dir"},
    "hkex":      {"collection": "hkex",      "label": "HKEXnews (HK)",        "pdf_root_attr": "hkex_pdf_dir"},
    "edinet":    {"collection": "edinet",    "label": "EDINET (JP statutory)", "pdf_root_attr": "edinet_pdf_dir"},
    "tdnet":     {"collection": "tdnet",     "label": "TDnet (JP timely)",     "pdf_root_attr": "tdnet_pdf_dir"},
    "dart":      {"collection": "dart",      "label": "DART (KR)",             "pdf_root_attr": "dart_pdf_dir"},
    "asx":       {"collection": "asx",       "label": "ASX (AU)",              "pdf_root_attr": "asx_pdf_dir"},
    "ir_pages":  {"collection": "ir_pages",  "label": "Company IR pages",      "pdf_root_attr": "ir_pages_pdf_dir"},
}

# DART fnltt + SEC XBRL live in side-collections; expose them separately.
AUX_COLLECTIONS = {
    "sec_xbrl_facts",
    "asx_key_statistics",
    "dart_fnltt",
}


# ---------- Mongo ----------

@lru_cache(maxsize=1)
def _client() -> AsyncIOMotorClient:
    return AsyncIOMotorClient(get_settings().ir_filings_mongo_uri, tz_aware=True)


def _db() -> AsyncIOMotorDatabase:
    return _client()[get_settings().ir_filings_mongo_db]


def _collection_for(source: str):
    spec = SOURCES.get(source)
    if not spec:
        raise HTTPException(400, f"Unknown source '{source}'. "
                                  f"Known: {sorted(SOURCES.keys())}")
    return _db()[spec["collection"]]


def _pdf_root_for(source: str) -> str:
    spec = SOURCES.get(source)
    if not spec:
        raise HTTPException(400, "Unknown source")
    settings = get_settings()
    attr = spec.get("pdf_root_attr") or ""
    return getattr(settings, attr, "") or ""


# ---------- Pydantic ----------

class FilingSummary(BaseModel):
    id: str
    source: str
    category: str
    category_name: str
    title: str
    title_local: str = ""
    release_time: str
    release_time_ms: int
    organization: str
    ticker_local: str
    ticker_canonical: str
    lang: str
    period_start: Optional[str] = None
    period_end: Optional[str] = None
    fiscal_year: Optional[int] = None
    fiscal_period: Optional[str] = None
    pdf_size_bytes: int = 0
    pdf_unavailable: bool = False
    web_url: str = ""
    has_xbrl_summary: bool = False
    is_price_sensitive: Optional[bool] = None    # ASX-specific


class FilingFull(FilingSummary):
    doc_introduce: str = ""
    content_md: str = ""
    pdf_text_md: str = ""
    attachments: list[dict] = []
    xbrl_summary: dict = {}
    list_item: dict = {}
    extra: dict = {}


class SourceInfo(BaseModel):
    slug: str
    label: str
    count: int
    latest_release_time: Optional[str] = None


class StatsResponse(BaseModel):
    total: int
    per_source: dict[str, int]
    today: dict[str, int]
    last_7_days: list[dict]
    sources: list[SourceInfo]
    aux_counts: dict[str, int]


# ---------- Normalization ----------

def _summary(doc: dict) -> FilingSummary:
    return FilingSummary(
        id=str(doc.get("_id") or ""),
        source=doc.get("source") or "",
        category=str(doc.get("category") or ""),
        category_name=str(doc.get("category_name") or ""),
        title=doc.get("title") or "",
        title_local=doc.get("title_local") or "",
        release_time=doc.get("release_time") or "",
        release_time_ms=int(doc.get("release_time_ms") or 0),
        organization=doc.get("organization") or "",
        ticker_local=str(doc.get("ticker_local") or ""),
        ticker_canonical=str(doc.get("ticker_canonical") or ""),
        lang=doc.get("lang") or "",
        period_start=doc.get("period_start"),
        period_end=doc.get("period_end"),
        fiscal_year=doc.get("fiscal_year"),
        fiscal_period=doc.get("fiscal_period"),
        pdf_size_bytes=int(doc.get("pdf_size_bytes") or 0),
        pdf_unavailable=bool(doc.get("pdf_unavailable")),
        web_url=doc.get("web_url") or "",
        has_xbrl_summary=bool(doc.get("xbrl_summary")),
        is_price_sensitive=doc.get("is_price_sensitive"),
    )


def _full(doc: dict) -> FilingFull:
    base = _summary(doc).model_dump()
    # Extra source-specific fields land in `extra` so the standard schema stays clean
    standard_keys = set(base.keys()) | {
        "doc_introduce", "content_md", "pdf_text_md", "attachments",
        "xbrl_summary", "list_item", "_id", "_canonical_tickers",
        "_canonical_tickers_at", "_canonical_extract_source",
        "pdf_local_path", "pdf_rel_path", "pdf_download_error",
        "pdf_size_bytes", "pdf_unavailable", "stats", "crawled_at",
        "ticker_canonical", "ticker_local", "release_time",
        "release_time_ms", "title", "title_local", "category",
        "category_name", "organization", "lang", "period_start", "period_end",
        "fiscal_year", "fiscal_period", "source", "web_url",
        "xbrl_data_path",
    }
    extra = {k: v for k, v in doc.items() if k not in standard_keys}
    return FilingFull(
        **base,
        doc_introduce=doc.get("doc_introduce") or "",
        content_md=doc.get("content_md") or "",
        pdf_text_md=doc.get("pdf_text_md") or "",
        attachments=doc.get("attachments") or [],
        xbrl_summary=doc.get("xbrl_summary") or {},
        list_item=doc.get("list_item") or {},
        extra=extra,
    )


# ---------- Routes ----------

@router.get("/sources", response_model=list[SourceInfo])
async def list_sources(user: User = Depends(get_current_user)):
    db = _db()
    out: list[SourceInfo] = []
    for slug, spec in SOURCES.items():
        coll = db[spec["collection"]]
        n = await coll.count_documents({})
        latest_doc = await coll.find_one({}, sort=[("release_time_ms", -1)],
                                          projection={"release_time": 1})
        out.append(SourceInfo(
            slug=slug,
            label=spec["label"],
            count=n,
            latest_release_time=(latest_doc or {}).get("release_time"),
        ))
    return out


@router.get("/stats", response_model=StatsResponse)
async def get_stats(user: User = Depends(get_current_user)):
    db = _db()
    from zoneinfo import ZoneInfo
    local_tz = ZoneInfo("Asia/Shanghai")
    midnight_local = datetime.now(local_tz).replace(hour=0, minute=0, second=0, microsecond=0)
    midnight_ms = int(midnight_local.timestamp() * 1000)

    per_source: dict[str, int] = {}
    today: dict[str, int] = {}
    sources_info: list[SourceInfo] = []
    for slug, spec in SOURCES.items():
        coll = db[spec["collection"]]
        n = await coll.count_documents({})
        per_source[slug] = n
        today[slug] = await coll.count_documents({"release_time_ms": {"$gte": midnight_ms}})
        latest = await coll.find_one({}, sort=[("release_time_ms", -1)],
                                      projection={"release_time": 1})
        sources_info.append(SourceInfo(
            slug=slug, label=spec["label"], count=n,
            latest_release_time=(latest or {}).get("release_time"),
        ))

    # 7-day stacked counts (across all sources, by date)
    last_7: dict[str, dict[str, int]] = {}
    for slug, spec in SOURCES.items():
        pipeline = [
            {"$match": {"release_time": {"$type": "string"}}},
            {"$group": {
                "_id": {"$substrBytes": ["$release_time", 0, 10]},
                "n": {"$sum": 1},
            }},
            {"$sort": {"_id": -1}},
            {"$limit": 7},
        ]
        async for d in db[spec["collection"]].aggregate(pipeline):
            day = d["_id"]
            last_7.setdefault(day, {s: 0 for s in SOURCES})
            last_7[day][slug] = d["n"]
    last_7_sorted = sorted(last_7.items())[-7:]
    last_7_list = [{"date": d, **counts} for d, counts in last_7_sorted]

    aux_counts = {
        name: await db[name].count_documents({})
        for name in sorted(AUX_COLLECTIONS)
    }

    return StatsResponse(
        total=sum(per_source.values()),
        per_source=per_source,
        today=today,
        last_7_days=last_7_list,
        sources=sources_info,
        aux_counts=aux_counts,
    )


@router.get("/sources/{source}", response_model=list[FilingSummary])
async def list_filings(
    source: str,
    ticker: Optional[str] = Query(None, description="Canonical ticker (INTC.US / 01347.HK / SGQ.AU)"),
    category: Optional[str] = Query(None, description="Source-specific category code (10-K, 13300, A001...)"),
    fiscal_year: Optional[int] = Query(None),
    fiscal_period: Optional[str] = Query(None),
    only_price_sensitive: bool = Query(False, description="ASX only — filter to is_price_sensitive=True"),
    date_from: Optional[str] = Query(None, description="YYYY-MM-DD lower bound on release_time"),
    date_to: Optional[str] = Query(None, description="YYYY-MM-DD upper bound on release_time"),
    limit: int = Query(50, ge=1, le=500),
    skip: int = Query(0, ge=0, le=10_000),
    user: User = Depends(get_current_user),
):
    coll = _collection_for(source)
    query: dict[str, Any] = {}
    if ticker:
        query["$or"] = [
            {"ticker_canonical": ticker},
            {"_canonical_tickers": ticker},
        ]
    if category:
        query["category"] = category
    if fiscal_year is not None:
        query["fiscal_year"] = fiscal_year
    if fiscal_period:
        query["fiscal_period"] = fiscal_period
    if only_price_sensitive:
        query["is_price_sensitive"] = True

    if date_from or date_to:
        ms_filter: dict[str, int] = {}
        if date_from:
            try:
                ms_filter["$gte"] = int(datetime.strptime(date_from, "%Y-%m-%d")
                                         .replace(tzinfo=timezone.utc).timestamp() * 1000)
            except ValueError:
                raise HTTPException(400, "date_from must be YYYY-MM-DD")
        if date_to:
            try:
                ms_filter["$lt"] = int((datetime.strptime(date_to, "%Y-%m-%d")
                                          .replace(tzinfo=timezone.utc).timestamp() + 86400) * 1000)
            except ValueError:
                raise HTTPException(400, "date_to must be YYYY-MM-DD")
        query["release_time_ms"] = ms_filter

    cursor = coll.find(query).sort("release_time_ms", -1).skip(skip).limit(limit)
    return [_summary(d) async for d in cursor]


@router.get("/sources/{source}/{item_id}", response_model=FilingFull)
async def get_filing(source: str, item_id: str,
                     user: User = Depends(get_current_user)):
    coll = _collection_for(source)
    doc = await coll.find_one({"_id": item_id})
    if not doc:
        raise HTTPException(404, f"Filing {source}/{item_id} not found")
    return _full(doc)


@router.get("/sources/{source}/{item_id}/pdf")
async def get_filing_pdf(
    source: str, item_id: str,
    download: int = Query(0, ge=0, le=1),
    user: User = Depends(get_current_user),
):
    coll = _collection_for(source)
    doc = await coll.find_one(
        {"_id": item_id},
        projection={"pdf_local_path": 1, "pdf_rel_path": 1,
                    "pdf_size_bytes": 1, "title": 1, "web_url": 1},
    )
    if not doc:
        raise HTTPException(404, "Filing not found")
    rel = doc.get("pdf_rel_path") or doc.get("pdf_local_path")
    if not rel or (doc.get("pdf_size_bytes") or 0) <= 0:
        # No local PDF → bounce to source viewer if known
        if doc.get("web_url"):
            raise HTTPException(307, detail={"redirect": doc["web_url"],
                                              "reason": "no local PDF; follow web_url"})
        raise HTTPException(404, "PDF not available")
    pdf_root = _pdf_root_for(source)
    if not pdf_root:
        raise HTTPException(500, f"no pdf_root configured for source '{source}'")
    title = (doc.get("title") or f"{source}-{item_id[:12]}")[:120]
    from ..services.pdf_storage import stream_pdf_or_file
    return await stream_pdf_or_file(
        db=coll.database,
        pdf_rel_path=rel,
        pdf_root=pdf_root,
        download_filename=title,
        download=bool(download),
    )


# ---------- XBRL / fnltt (structured financials) ----------

@router.get("/xbrl/{ticker}")
async def get_sec_xbrl(
    ticker: str,
    tag: Optional[str] = Query(None, description="us-gaap concept tag (e.g. RevenueFromContractWithCustomerExcludingAssessedTax)"),
    taxonomy: Optional[str] = Query(None, description="us-gaap | ifrs-full | dei"),
    limit: int = Query(200, ge=1, le=2000),
    user: User = Depends(get_current_user),
):
    """SEC XBRL companyfacts rows for one ticker. Returns the time series of
    the requested concept tag, sorted by `end` ascending. Filter by `tag` /
    `taxonomy` to narrow."""
    coll = _db()["sec_xbrl_facts"]
    q: dict[str, Any] = {"ticker_canonical": ticker}
    if tag:
        q["tag"] = tag
    if taxonomy:
        q["taxonomy"] = taxonomy
    rows = []
    cursor = coll.find(q).sort("end", 1).limit(limit)
    async for r in cursor:
        r.pop("_id", None)
        if "ingested_at" in r and isinstance(r["ingested_at"], datetime):
            r["ingested_at"] = r["ingested_at"].isoformat()
        rows.append(r)
    return {"ticker": ticker, "count": len(rows), "rows": rows}


@router.get("/fnltt/{ticker}")
async def get_dart_fnltt(
    ticker: str,
    bsns_year: Optional[int] = Query(None),
    reprt_code: Optional[str] = Query(None,
                                       description="11011=annual, 11012=H1, 11013=Q1, 11014=Q3"),
    sj_div: Optional[str] = Query(None, description="BS / IS / CIS / CF / SCE"),
    user: User = Depends(get_current_user),
):
    """DART fnltt structured FS line items for one Korean ticker."""
    coll = _db()["dart_fnltt"]
    q: dict[str, Any] = {"ticker_canonical": ticker}
    if bsns_year:
        q["bsns_year"] = bsns_year
    if reprt_code:
        q["reprt_code"] = reprt_code
    if sj_div:
        q["sj_div"] = sj_div
    rows = []
    cursor = coll.find(q).sort([("bsns_year", -1), ("reprt_code", 1)]).limit(2000)
    async for r in cursor:
        r.pop("_id", None)
        if "ingested_at" in r and isinstance(r["ingested_at"], datetime):
            r["ingested_at"] = r["ingested_at"].isoformat()
        rows.append(r)
    return {"ticker": ticker, "count": len(rows), "rows": rows}


@router.get("/key-statistics/{ticker}")
async def get_asx_key_stats(ticker: str, user: User = Depends(get_current_user)):
    """ASX key-statistics panel — 3-year revenue + ratios."""
    coll = _db()["asx_key_statistics"]
    doc = await coll.find_one({"ticker_canonical": ticker})
    if not doc:
        raise HTTPException(404, "no key_statistics for that ticker")
    doc.pop("_id", None)
    if "ingested_at" in doc and isinstance(doc["ingested_at"], datetime):
        doc["ingested_at"] = doc["ingested_at"].isoformat()
    return doc
