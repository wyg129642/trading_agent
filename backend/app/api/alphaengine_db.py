"""REST API exposing MongoDB-backed AlphaEngine (www.alphaengine.top) crawl data.

Reads directly from the `alphaengine` MongoDB database populated by
`crawl/alphaengine/scraper.py`. Four collections:
  - summaries        纪要     (AI 会议纪要 / 业绩会 / 调研 / 专家会)
  - china_reports    国内研报  (内资券商 + 期货研究, 含 PDF)
  - foreign_reports  海外研报  (Citi / JPM / GS 等外资, 含 PDF)
  - news_items       资讯     (TMTB / 海内外媒体等资讯 EOD Wrap)

UI-facing categories are 1:1 with scraper categories:
  - summary       → summaries
  - chinaReport   → china_reports
  - foreignReport → foreign_reports
  - news          → news_items

Shape convention (common to all):
  _id, category, title, release_time (str "YYYY-MM-DD HH:MM"),
  release_time_ms, organization, doc_introduce / content_md,
  institution_names, industry_names, company_codes, company_names,
  document_type_name, type_full_name, pdf_rel_path / pdf_local_path (报告),
  list_item (raw), stats, crawled_at.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.parse import quote as urlquote

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pydantic import BaseModel

from backend.app.config import get_settings
from backend.app.deps import get_current_user
from backend.app.models.user import User

logger = logging.getLogger(__name__)
router = APIRouter()


# UI-facing category → (mongo collection, extra filter)
CATEGORY_SPEC: dict[str, tuple[str, dict[str, Any]]] = {
    "summary":       ("summaries",       {}),
    "chinaReport":   ("china_reports",   {}),
    "foreignReport": ("foreign_reports", {}),
    "news":          ("news_items",      {}),
}

CATEGORY_LABEL_CN = {
    "summary": "纪要",
    "chinaReport": "国内研报",
    "foreignReport": "海外研报",
    "news": "资讯",
}

# Categories that actually host a PDF on the scraper side.
_PDF_CATEGORIES = {"chinaReport", "foreignReport"}


@lru_cache(maxsize=1)
def _mongo_client() -> AsyncIOMotorClient:
    return AsyncIOMotorClient(get_settings().alphaengine_mongo_uri, tz_aware=True)


def _mongo_db() -> AsyncIOMotorDatabase:
    return _mongo_client()[get_settings().alphaengine_mongo_db]


def _collection_for(category: str):
    if category not in CATEGORY_SPEC:
        raise HTTPException(400, f"Unknown category '{category}'")
    cname, _ = CATEGORY_SPEC[category]
    return _mongo_db()[cname]


def _base_filter(category: str) -> dict[str, Any]:
    _, f = CATEGORY_SPEC[category]
    return dict(f)


# --------------------------------------------------------------------------- #
# Normalizers
# --------------------------------------------------------------------------- #
def _brief(doc: dict) -> dict:
    content = doc.get("content_md") or doc.get("doc_introduce") or ""
    preview = content[:360] + ("…" if len(content) > 360 else "")
    category = doc.get("category") or ""
    return {
        "id": str(doc.get("_id")),
        "doc_id": doc.get("doc_id"),
        "summary_id": doc.get("summary_id"),
        "category": category,
        "category_label": CATEGORY_LABEL_CN.get(category, category),
        "title": doc.get("title") or "",
        "title_cn": doc.get("title_cn") or "",
        "release_time": doc.get("release_time"),
        "release_time_ms": doc.get("release_time_ms"),
        "publish_time": doc.get("publish_time"),
        "publish_time_ms": doc.get("publish_time_ms"),
        "rank_date": doc.get("rank_date"),
        "rank_date_ms": doc.get("rank_date_ms"),
        "organization": doc.get("organization") or "",
        "institution_names": doc.get("institution_names") or [],
        "authors": doc.get("authors") or [],
        "document_type_name": doc.get("document_type_name"),
        "type_full_name": doc.get("type_full_name"),
        "first_type_name": doc.get("first_type_name"),
        "type_show_name": doc.get("type_show_name"),
        "industry_names": doc.get("industry_names") or [],
        "company_codes": doc.get("company_codes") or [],
        "company_names": doc.get("company_names") or [],
        "doc_icon": doc.get("doc_icon"),
        "page_num": int(doc.get("page_num") or 0),
        "depth_flag": doc.get("depth_flag"),
        "web_url": doc.get("web_url"),
        "has_pdf": bool(doc.get("pdf_local_path")) and (doc.get("pdf_size_bytes") or 0) > 0,
        "pdf_size_bytes": doc.get("pdf_size_bytes") or 0,
        "pdf_unavailable": bool(doc.get("pdf_unavailable")),
        "preview": preview,
        "content_length": len(content),
        "crawled_at": doc.get("crawled_at"),
        "_canonical_tickers": doc.get("_canonical_tickers") or [],
    }


class ItemListResponse(BaseModel):
    items: list[dict]
    total: int
    page: int
    page_size: int
    has_next: bool


class StatsResponse(BaseModel):
    total: int
    per_category: dict[str, int]
    today: dict[str, int]
    last_7_days: list[dict]
    crawler_state: list[dict]
    daily_platform_stats: dict | None
    top_organizations: dict[str, list[dict]]
    latest_per_category: dict[str, str | None]


# --------------------------------------------------------------------------- #
# List + detail
# --------------------------------------------------------------------------- #
@router.get("/items", response_model=ItemListResponse)
async def list_items(
    category: str = Query("summary",
                          pattern="^(summary|chinaReport|foreignReport|news)$"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    q: str | None = Query(None, description="Full-text on title / intro / content"),
    organization: str | None = None,
    ticker: str | None = Query(None, description="Canonical ticker or company name fragment"),
    industry: str | None = None,
    sort: str = Query(
        "rank_date",
        pattern="^(rank_date|publish_time|crawled_at)$",
        description=(
            "rank_date = 平台重新索引时间 (默认, 与原站列表排序一致); "
            "publish_time = 报告原始发布日期; "
            "crawled_at = 我们抓取入库时间"
        ),
    ),
    user: User = Depends(get_current_user),
):
    coll = _collection_for(category)
    match: dict[str, Any] = _base_filter(category)
    ors: list[dict] = []
    if q:
        ors += [
            {"title": {"$regex": q, "$options": "i"}},
            {"title_cn": {"$regex": q, "$options": "i"}},
            {"doc_introduce": {"$regex": q, "$options": "i"}},
            {"content_md": {"$regex": q, "$options": "i"}},
            {"search_title": {"$regex": q, "$options": "i"}},
        ]
    if organization:
        match["$or"] = [
            {"organization": {"$regex": organization, "$options": "i"}},
            {"institution_names": {"$regex": organization, "$options": "i"}},
        ]
    if ticker:
        ors += [
            {"company_codes": {"$regex": ticker, "$options": "i"}},
            {"company_names": {"$regex": ticker, "$options": "i"}},
            {"_canonical_tickers": ticker.upper()},
        ]
    if industry:
        match["industry_names"] = {"$regex": industry, "$options": "i"}
    if ors:
        # Keep existing $or if set (organization above), combine with $and
        if "$or" in match:
            match = {"$and": [{"$or": match["$or"]}, {"$or": ors}]}
            match = dict(_base_filter(category), **match)
        else:
            match["$or"] = ors

    # Sort dispatch. Default `rank_date` matches the original alphaengine.top
    # list ordering (server's streamSearch returns items sorted by rank_date desc).
    # `rank_date_ms` is kept in sync by the scraper; fall back to legacy
    # `release_time_ms` so older docs (pre-2026-04-22 schema) still order correctly.
    if sort == "publish_time":
        sort_fields: list[tuple[str, int]] = [
            ("publish_time_ms", -1),
            ("release_time_ms", -1),
            ("_id", -1),
        ]
    elif sort == "crawled_at":
        sort_fields = [("crawled_at", -1), ("_id", -1)]
    else:
        sort_fields = [
            ("rank_date_ms", -1),
            ("release_time_ms", -1),
            ("_id", -1),
        ]

    total = await coll.count_documents(match)
    cursor = (
        coll.find(match, projection={
            "list_item": 0,
            "content_md": 0,
        })
        .sort(sort_fields)
        .skip((page - 1) * page_size)
        .limit(page_size)
    )
    items = [_brief(d) async for d in cursor]
    return ItemListResponse(
        items=items,
        total=total,
        page=page,
        page_size=page_size,
        has_next=(page * page_size) < total,
    )


@router.get("/items/{category}/{item_id}")
async def get_item(
    category: str,
    item_id: str,
    user: User = Depends(get_current_user),
):
    if category not in CATEGORY_SPEC:
        raise HTTPException(400, "Unknown category")
    coll = _collection_for(category)
    doc = await coll.find_one({"_id": item_id})
    if not doc:
        raise HTTPException(404, "Item not found")
    return {
        **_brief(doc),
        "content_md": doc.get("content_md") or "",
        "doc_introduce": doc.get("doc_introduce") or "",
        "type_full_id": doc.get("type_full_id"),
        "document_type_id": doc.get("document_type_id"),
        "company_multi_map": doc.get("company_multi_map") or {},
        "first_industry_name": doc.get("first_industry_name") or [],
        "company_show_name": doc.get("company_show_name") or [],
        "share_link": doc.get("share_link"),
        "sensitive": doc.get("sensitive"),
        "sensitive_permission": doc.get("sensitive_permission"),
        "pdf_rel_path": doc.get("pdf_rel_path"),
        "pdf_size_bytes": doc.get("pdf_size_bytes") or 0,
        "pdf_download_error": doc.get("pdf_download_error") or "",
    }


@router.get("/items/{category}/{item_id}/pdf")
async def get_item_pdf(
    category: str,
    item_id: str,
    download: int = Query(0, ge=0, le=1,
                          description="1=强制下载; 0=浏览器内联预览"),
    user: User = Depends(get_current_user),
):
    """流式返回本地 PDF (仅 chinaReport / foreignReport 有 PDF)."""
    if category not in _PDF_CATEGORIES:
        raise HTTPException(400, f"Category '{category}' has no PDF support")
    coll = _collection_for(category)
    doc = await coll.find_one(
        {"_id": item_id},
        projection={"pdf_local_path": 1, "pdf_size_bytes": 1, "title": 1,
                    "pdf_download_error": 1},
    )
    if not doc:
        raise HTTPException(404, "Item not found")

    rel = doc.get("pdf_local_path")
    if not rel or (doc.get("pdf_size_bytes") or 0) <= 0:
        err = doc.get("pdf_download_error") or "PDF not yet downloaded"
        raise HTTPException(404, f"PDF not available: {err}")

    settings = get_settings()
    title = (doc.get("title") or f"alphaengine-{item_id[:12]}")[:120]
    from ..services.pdf_storage import stream_pdf_or_file
    return await stream_pdf_or_file(
        db=_mongo_db(),
        pdf_rel_path=rel,
        pdf_root=settings.alphaengine_pdf_dir,
        download_filename=title,
        download=bool(download),
    )


# --------------------------------------------------------------------------- #
# Stats — for dashboard card + 4-category breakdown
# --------------------------------------------------------------------------- #
@router.get("/stats", response_model=StatsResponse)
async def get_stats(user: User = Depends(get_current_user)):
    db = _mongo_db()
    from zoneinfo import ZoneInfo
    local_tz = ZoneInfo("Asia/Shanghai")
    local_midnight = datetime.now(local_tz).replace(hour=0, minute=0, second=0, microsecond=0)
    midnight_utc = local_midnight.astimezone(timezone.utc)
    midnight_ms = int(local_midnight.timestamp() * 1000)
    today_str = datetime.now(local_tz).strftime("%Y-%m-%d")

    per_category: dict[str, int] = {}
    today: dict[str, int] = {}
    latest_per_category: dict[str, str | None] = {}
    top_orgs: dict[str, list[dict]] = {}

    for cat, (cname, subfilter) in CATEGORY_SPEC.items():
        coll = db[cname]
        per_category[cat] = await coll.count_documents(subfilter)
        # 今日新增 = 今天平台发布的, 按 release_time_ms (不用 crawled_at, 因为
        # 回填/重抓会刷新 crawled_at 让历史 doc 虚报为 "今日").
        today_filter = {**subfilter, "release_time_ms": {"$gte": midnight_ms}}
        today[cat] = await coll.count_documents(today_filter)
        latest_doc = await coll.find_one(
            subfilter, sort=[("release_time_ms", -1)],
            projection={"release_time": 1},
        )
        latest_per_category[cat] = latest_doc.get("release_time") if latest_doc else None

        pipeline = [
            {"$match": {**subfilter, "organization": {"$nin": [None, ""]}}},
            {"$group": {"_id": "$organization", "n": {"$sum": 1}}},
            {"$sort": {"n": -1}},
            {"$limit": 8},
        ]
        top_orgs[cat] = [
            {"name": d["_id"], "count": d["n"]}
            async for d in coll.aggregate(pipeline)
            if d.get("_id")
        ]

    last_7_days: dict[str, dict[str, int]] = {}
    for cat, (cname, subfilter) in CATEGORY_SPEC.items():
        pipeline = [
            {"$match": {**subfilter, "release_time": {"$type": "string"}}},
            {"$group": {
                "_id": {"$substrBytes": ["$release_time", 0, 10]},
                "n": {"$sum": 1},
            }},
            {"$sort": {"_id": -1}},
            {"$limit": 7},
        ]
        async for d in db[cname].aggregate(pipeline):
            date = d["_id"]
            if date not in last_7_days:
                last_7_days[date] = {c: 0 for c in CATEGORY_SPEC}
            last_7_days[date][cat] = d["n"]
    last_7_sorted = sorted(last_7_days.items())[-7:]
    last_7_list = [{"date": d, **counts} for d, counts in last_7_sorted]

    state_coll = db["_state"]
    crawler_state: list[dict] = []
    async for s in state_coll.find({"_id": {"$regex": "^crawler_"}}):
        crawler_state.append({
            "category": s["_id"].replace("crawler_", ""),
            "last_processed_at": s.get("last_processed_at"),
            "last_run_end_at": s.get("last_run_end_at"),
            "last_run_stats": s.get("last_run_stats") or {},
            "in_progress": bool(s.get("in_progress")),
            "top_dedup_id": s.get("top_dedup_id"),
        })

    daily = await state_coll.find_one({"_id": f"daily_{today_str}"})
    daily_platform_stats = None
    if daily:
        daily_platform_stats = {}
        for cat in CATEGORY_SPEC:
            sub = daily.get(cat) or {}
            daily_platform_stats[cat] = {
                "platform_count": sub.get("platform_count", 0),
                "in_db": sub.get("in_db", 0),
                "missing": sub.get("missing", 0),
            }

    return StatsResponse(
        total=sum(per_category.values()),
        per_category=per_category,
        today=today,
        last_7_days=last_7_list,
        crawler_state=crawler_state,
        daily_platform_stats=daily_platform_stats,
        top_organizations=top_orgs,
        latest_per_category=latest_per_category,
    )
