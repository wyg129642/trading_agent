"""REST API exposing MongoDB-backed Gangtise (open.gangtise.com) crawl data.

Reads directly from the `gangtise` MongoDB database populated by
`crawl/gangtise/scraper.py`. Three collections:
  - summaries       纪要 (会议 / 投资者关系 / 公司公告 AI 纪要)
  - researches      研报 (券商研究报告, 含 PDF)
  - chief_opinions  首席观点

Shape convention:
  _id, category, title, release_time (str "YYYY-MM-DD HH:MM"),
  release_time_ms, organization, content_md, brief_md, stocks, industries,
  list_item (raw), stats, crawled_at.

Research docs additionally carry: pdf_rel_path / pdf_local_path / pdf_size_bytes.
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
from backend.app.services.ticker_tags_builder import build_ticker_tags

logger = logging.getLogger(__name__)
router = APIRouter()


CATEGORY_COLLECTION = {
    "summary": "summaries",
    "research": "researches",
    "chief": "chief_opinions",
}

CATEGORY_LABEL_CN = {
    "summary": "纪要",
    "research": "研报",
    "chief": "首席观点",
}


@lru_cache(maxsize=1)
def _mongo_client() -> AsyncIOMotorClient:
    return AsyncIOMotorClient(get_settings().gangtise_mongo_uri, tz_aware=True)


def _mongo_db() -> AsyncIOMotorDatabase:
    return _mongo_client()[get_settings().gangtise_mongo_db]


# --------------------------------------------------------------------------- #
# Normalizers
# --------------------------------------------------------------------------- #
def _stocks(doc: dict) -> list[dict]:
    raw = doc.get("stocks") or []
    out: list[dict] = []
    if isinstance(raw, list):
        for s in raw:
            if isinstance(s, dict) and (s.get("code") or s.get("name")):
                out.append({
                    "code": s.get("code") or "",
                    "name": s.get("name") or "",
                    "rating": s.get("rating"),
                    "rating_change": s.get("rating_change"),
                })
    return out


def _analysts(doc: dict) -> list[str]:
    """Chief has `analyst` scalar; research has `authors[]`."""
    names: list[str] = []
    a = doc.get("analyst")
    if isinstance(a, str) and a:
        names.append(a)
    for au in doc.get("authors") or []:
        if isinstance(au, dict) and au.get("name"):
            names.append(au["name"])
    # dedup preserving order
    seen: set[str] = set()
    out: list[str] = []
    for n in names:
        if n and n not in seen:
            seen.add(n)
            out.append(n)
    return out


def _brief(doc: dict) -> dict:
    """Uniform list-view item."""
    # `content_md` is excluded from the list cursor projection (it can be tens
    # of KB per doc); the server-side aggregation injects `_content_length`
    # via $strLenCP so we get the count without paying to ship the body.
    content_length = int(doc.get("_content_length") or 0)
    brief = doc.get("brief_md") or doc.get("description_md") or ""
    # Preview: brief_md is always present and is a tight summary; use it.
    preview = brief[:360] + ("…" if len(brief) > 360 else "")
    stats = doc.get("stats") or {}
    cat = doc.get("category") or ""
    return {
        "id": str(doc.get("_id")),
        "category": cat,
        "category_label": CATEGORY_LABEL_CN.get(cat, cat),
        "title": doc.get("title") or "",
        "release_time": doc.get("release_time"),
        "release_time_ms": doc.get("release_time_ms"),
        "organization": doc.get("organization") or "",
        "analysts": _analysts(doc),
        "stocks": _stocks(doc),
        "industries": doc.get("industries") or [],
        "column_names": doc.get("column_names") or [],
        "rpt_type_name": doc.get("rpt_type_name") or "",
        "pages": doc.get("pages") or 0,
        "head_party": bool(doc.get("head_party")),
        "foreign_party": bool(doc.get("foreign_party")),
        "first_coverage": bool(doc.get("first_coverage")),
        "has_audio": bool(doc.get("has_audio")),
        "web_url": doc.get("web_url"),
        "preview": preview,
        "content_length": content_length,
        "brief_length": len(brief),
        "has_pdf": bool(doc.get("pdf_local_path") and doc.get("pdf_size_bytes", 0) > 0),
        "pdf_size_bytes": int(doc.get("pdf_size_bytes") or 0),
        "research_directions": doc.get("research_directions") or [],
        "guest": doc.get("guest") or "",
        # 首席观点分区 (chief): 内资机构观点 / 外资机构观点 / 外资独立观点 / 大V观点
        "chief_variant": doc.get("chief_variant"),
        "chief_variant_name": doc.get("chief_variant_name"),
        "crawled_at": doc.get("crawled_at"),
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
    category: str = Query("summary", pattern="^(summary|research|chief)$"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    q: str | None = Query(None, description="Full-text on title/brief/content"),
    organization: str | None = None,
    ticker: str | None = Query(None, description="Stock code/name fragment"),
    industry: str | None = None,
    chief_variant: str | None = Query(
        None,
        description="chief only: filter by 分区 (domestic_institution / foreign_institution / foreign_independent / kol)",
    ),
    research_origin: str | None = Query(
        None,
        pattern="^(domestic|foreign)$",
        description="research only: domestic=内资 / foreign=外资 (按 foreign_party 标志)",
    ),
    user: User = Depends(get_current_user),
):
    coll = _mongo_db()[CATEGORY_COLLECTION[category]]

    match: dict[str, Any] = {}
    ors: list[dict] = []
    if q:
        ors += [
            {"title": {"$regex": q, "$options": "i"}},
            {"brief_md": {"$regex": q, "$options": "i"}},
            {"content_md": {"$regex": q, "$options": "i"}},
        ]
    if organization:
        match["organization"] = {"$regex": organization, "$options": "i"}
    if ticker:
        ors += [
            {"stocks.code": {"$regex": ticker, "$options": "i"}},
            {"stocks.name": {"$regex": ticker, "$options": "i"}},
        ]
    if industry:
        match["industries"] = {"$regex": industry, "$options": "i"}
    if chief_variant and category == "chief":
        # 匹配主 variant 或 chief_variants 数组里的任意一个
        # (一条记录如果在多个 tab 下都出现, 会有 chief_variants 列表)
        match["$and"] = match.get("$and", []) + [{
            "$or": [
                {"chief_variant": chief_variant},
                {"chief_variants": chief_variant},
            ],
        }]
    if research_origin and category == "research":
        match["foreign_party"] = (research_origin == "foreign")
    if ors:
        match["$or"] = ors

    total = await coll.count_documents(match)
    pipeline = [
        {"$match": match},
        {"$sort": {"release_time_ms": -1}},
        {"$skip": (page - 1) * page_size},
        {"$limit": page_size},
        # Compute content length server-side so the heavy `content_md` body
        # never has to come back over the wire — fixes the "0 字" display.
        {"$addFields": {
            "_content_length": {"$strLenCP": {"$ifNull": ["$content_md", ""]}},
        }},
        {"$project": {
            "list_item": 0, "detail_result": 0, "parsed_msg": 0,
            "msg_text": 0, "content_md": 0,
        }},
    ]
    cursor = coll.aggregate(pipeline)
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
    if category not in CATEGORY_COLLECTION:
        raise HTTPException(400, "Unknown category")
    doc = await _mongo_db()[CATEGORY_COLLECTION[category]].find_one({"_id": item_id})
    if not doc:
        raise HTTPException(404, "Item not found")
    return {
        **_brief(doc),
        "content_md": doc.get("content_md") or "",
        "brief_md": doc.get("brief_md") or "",
        "description_md": doc.get("description_md") or "",
        "pdf_text_md": doc.get("pdf_text_md") or "",
        "msg_text": doc.get("msg_text") or [],
        "pdf_rel_path": doc.get("pdf_rel_path"),
        "source_name": doc.get("source_name") or "",
        "location": doc.get("location") or "",
        "researcher": doc.get("researcher") or "",
        "ticker_tags": build_ticker_tags(doc, "gangtise", CATEGORY_COLLECTION[category]),
    }


@router.get("/items/research/{item_id}/pdf")
async def get_research_pdf(
    item_id: str,
    download: int = Query(0, ge=0, le=1,
                          description="1=强制下载; 0=浏览器内联预览"),
    user: User = Depends(get_current_user),
):
    """流式返回研报 PDF. 由 scraper 落盘到 gangtise_pdf_dir, 读 pdf_local_path 后目录穿越防御."""
    doc = await _mongo_db()["researches"].find_one(
        {"_id": item_id},
        projection={"pdf_local_path": 1, "pdf_size_bytes": 1,
                    "pdf_download_error": 1, "title": 1},
    )
    if not doc:
        raise HTTPException(404, "Research not found")
    rel = doc.get("pdf_local_path")
    if not rel or doc.get("pdf_size_bytes", 0) <= 0:
        err = doc.get("pdf_download_error") or "PDF 未下载"
        raise HTTPException(404, f"PDF not available: {err}")

    settings = get_settings()
    title = (doc.get("title") or f"research-{item_id[:12]}")[:120]
    from ..services.pdf_storage import stream_pdf_or_file
    return await stream_pdf_or_file(
        db=_mongo_db(),
        pdf_rel_path=rel,
        pdf_root=settings.gangtise_pdf_dir,
        download_filename=title,
        download=bool(download),
    )


# --------------------------------------------------------------------------- #
# Stats — for dashboard
# --------------------------------------------------------------------------- #
@router.get("/stats", response_model=StatsResponse)
async def get_stats(user: User = Depends(get_current_user)):
    db = _mongo_db()
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    per_category: dict[str, int] = {}
    today: dict[str, int] = {}
    latest_per_category: dict[str, str | None] = {}
    top_orgs: dict[str, list[dict]] = {}

    for cat, cname in CATEGORY_COLLECTION.items():
        coll = db[cname]
        per_category[cat] = await coll.count_documents({})
        today[cat] = await coll.count_documents(
            {"release_time": {"$regex": f"^{today_str}"}}
        )
        latest_doc = await coll.find_one(
            {}, sort=[("release_time_ms", -1)],
            projection={"release_time": 1},
        )
        latest_per_category[cat] = latest_doc.get("release_time") if latest_doc else None

        pipeline = [
            {"$match": {"organization": {"$nin": [None, ""]}}},
            {"$group": {"_id": "$organization", "n": {"$sum": 1}}},
            {"$sort": {"n": -1}},
            {"$limit": 8},
        ]
        top_orgs[cat] = [
            {"name": d["_id"], "count": d["n"]}
            async for d in coll.aggregate(pipeline)
            if d.get("_id")
        ]

    # last 7 days per-category
    last_7_days: dict[str, dict[str, int]] = {}
    for cat, cname in CATEGORY_COLLECTION.items():
        pipeline = [
            {"$match": {"release_time": {"$type": "string"}}},
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
                last_7_days[date] = {c: 0 for c in CATEGORY_COLLECTION}
            last_7_days[date][cat] = d["n"]
    last_7_sorted = sorted(last_7_days.items())[-7:]
    last_7_list = [{"date": d, **counts} for d, counts in last_7_sorted]

    # crawler checkpoints
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

    # daily platform vs local
    daily = await state_coll.find_one({"_id": f"daily_{today_str}"})
    daily_platform_stats = None
    if daily:
        daily_platform_stats = {
            cat: {
                "platform_count": (daily.get(cat) or {}).get("platform_count", 0),
                "in_db": (daily.get(cat) or {}).get("in_db", 0),
                "missing": (daily.get(cat) or {}).get("missing", 0),
            }
            for cat in CATEGORY_COLLECTION
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
