"""REST API exposing MongoDB-backed AceCamp (api.acecamptech.com) crawl data.

Reads directly from the `acecamp` MongoDB database populated by
`crawl/AceCamp/scraper.py`. Two collections (events 集合 + 路演 UI 已于
2026-04-23 完整移除):
  - articles   文章 + 纪要 + 调研 (按 subtype 三分)
  - opinions   观点 (用户短评, expected_trend bullish/bearish)

Articles `subtype` 映射到平台原始字段:
  - subtype=minutes   ←  type=minute  (业绩会 / 公司纪要)
  - subtype=research  ←  type=minute + 标题含"调研/访谈/专家会议"
  - subtype=article   ←  type=original (原创文章 / 白皮书)

UI 类别 (4 个):
  - minutes   → articles.find({subtype: "minutes"})    纪要
  - research  → articles.find({subtype: "research"})   调研
  - article   → articles.find({subtype: "article"})    文章
  - opinion   → opinions                              观点

Shape convention (common to all):
  _id, category, title, release_time (str "YYYY-MM-DD HH:MM"),
  release_time_ms, organization, content_md, brief_md, corporations,
  hashtags, industry_ids, list_item (raw), stats, crawled_at.
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


# UI-facing category → (mongo collection, subtype filter or None)
# 2026-04-23: 路演 (event) 已彻底移除 — MongoDB events collection 已 drop,
# scraper 不再抓 events, 此 spec 不再含 event slug.
CATEGORY_SPEC: dict[str, tuple[str, dict[str, Any]]] = {
    "minutes":  ("articles", {"subtype": "minutes"}),
    "research": ("articles", {"subtype": "research"}),
    "article":  ("articles", {"subtype": "article"}),
    "opinion":  ("opinions", {}),
}

CATEGORY_LABEL_CN = {
    "minutes":  "纪要",
    "research": "调研",
    "article":  "文章",
    "opinion":  "观点",
}

# 合法 category 的正则, 用于 Query(pattern=...)
_CATEGORY_PATTERN = "^(minutes|research|article|opinion)$"


@lru_cache(maxsize=1)
def _mongo_client() -> AsyncIOMotorClient:
    return AsyncIOMotorClient(get_settings().acecamp_mongo_uri, tz_aware=True)


def _mongo_db() -> AsyncIOMotorDatabase:
    return _mongo_client()[get_settings().acecamp_mongo_db]


def _collection_for(category: str):
    if category not in CATEGORY_SPEC:
        raise HTTPException(400, f"Unknown category '{category}'")
    cname, _ = CATEGORY_SPEC[category]
    return _mongo_db()[cname]


def _base_filter(category: str) -> dict[str, Any]:
    """Return the static Mongo filter for a UI-facing category (subtype etc.)."""
    _, f = CATEGORY_SPEC[category]
    # Shallow copy so callers can mutate without affecting module state
    return dict(f)


# --------------------------------------------------------------------------- #
# Normalizers
# --------------------------------------------------------------------------- #
def _corporations(doc: dict) -> list[dict]:
    raw = doc.get("corporations") or []
    out: list[dict] = []
    if isinstance(raw, list):
        for c in raw:
            if not isinstance(c, dict):
                continue
            out.append({
                "id": c.get("id"),
                "name": c.get("name") or "",
                "code": c.get("code") or "",
                "exchange": c.get("exchange") or "",
            })
    return out


def _hashtags(doc: dict) -> list[str]:
    raw = doc.get("hashtags") or []
    out: list[str] = []
    if isinstance(raw, list):
        for h in raw:
            if isinstance(h, str) and h:
                out.append(h)
            elif isinstance(h, dict):
                n = h.get("name") or h.get("title")
                if n:
                    out.append(n)
    return out


def _brief(doc: dict) -> dict:
    """Uniform list-view item."""
    content = doc.get("content_md") or ""
    brief = doc.get("brief_md") or doc.get("summary_md") or doc.get("description_md") or ""
    transcribe = doc.get("transcribe_md") or ""
    preview_src = content if len(content) > 40 else (brief or transcribe)
    preview = preview_src[:360] + ("…" if len(preview_src) > 360 else "")
    cat = doc.get("category") or ""
    subtype = doc.get("subtype") or ""
    # Map internal (category + subtype) → UI category key
    if cat == "article" and subtype in ("minutes", "research", "article"):
        ui_cat = subtype
    elif cat == "opinion":
        ui_cat = "opinion"
    else:
        # 旧数据兜底: subtype=viewpoint (未迁移) 归 article
        ui_cat = "article" if subtype == "viewpoint" else (subtype or cat)
    return {
        "id": str(doc.get("_id")),
        "raw_id": doc.get("raw_id"),
        "category": ui_cat,
        "category_label": CATEGORY_LABEL_CN.get(ui_cat, ui_cat),
        "subtype": subtype,
        "title": doc.get("title") or doc.get("name") or "",
        "original_title": doc.get("original_title") or "",
        "release_time": doc.get("release_time"),
        "release_time_ms": doc.get("release_time_ms"),
        "organization": doc.get("organization") or "",
        "organization_id": doc.get("organization_id"),
        "corporations": _corporations(doc),
        "hashtags": _hashtags(doc),
        "industry_ids": doc.get("industry_ids") or [],
        "views": int(doc.get("views") or 0),
        "likes": int(doc.get("likes") or 0),
        "favorites": int(doc.get("favorites") or 0),
        "comment_count": int(doc.get("comment_count") or 0),
        "has_vip": bool(doc.get("has_vip")),
        "free": bool(doc.get("free")),
        "need_to_pay": bool(doc.get("need_to_pay")),
        "has_paid": bool(doc.get("has_paid")),
        "can_download": bool(doc.get("can_download")),
        "living": bool(doc.get("living")),
        "playback": bool(doc.get("playback")),
        "state": doc.get("state"),
        "expected_trend": doc.get("expected_trend"),
        "identity": doc.get("identity"),
        "cover_image": doc.get("cover_image"),
        "web_url": doc.get("web_url"),
        "preview": preview,
        "content_length": len(content),
        "brief_length": len(brief),
        "transcribe_length": len(transcribe),
        "has_pdf": bool(doc.get("download_url")),
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
    category: str = Query("minutes", pattern=_CATEGORY_PATTERN),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    q: str | None = Query(None, description="Full-text on title/summary/content"),
    organization: str | None = None,
    ticker: str | None = Query(None, description="Corporation code / name fragment"),
    hashtag: str | None = None,
    expected_trend: str | None = Query(
        None, pattern="^(bullish|bearish|neutral)$",
        description="仅用于 category=opinion: 按观点方向过滤",
    ),
    user: User = Depends(get_current_user),
):
    coll = _collection_for(category)
    match: dict[str, Any] = _base_filter(category)
    if category == "opinion" and expected_trend:
        match["expected_trend"] = expected_trend
    ors: list[dict] = []
    if q:
        ors += [
            {"title": {"$regex": q, "$options": "i"}},
            {"summary_md": {"$regex": q, "$options": "i"}},
            {"content_md": {"$regex": q, "$options": "i"}},
            {"transcribe_md": {"$regex": q, "$options": "i"}},
        ]
    if organization:
        match["organization"] = {"$regex": organization, "$options": "i"}
    if ticker:
        ors += [
            {"corporations.code": {"$regex": ticker, "$options": "i"}},
            {"corporations.name": {"$regex": ticker, "$options": "i"}},
        ]
    if hashtag:
        match["hashtags"] = {"$regex": hashtag, "$options": "i"}
    if ors:
        match["$or"] = ors

    total = await coll.count_documents(match)
    cursor = (
        coll.find(match, projection={
            "list_item": 0, "detail_result": 0, "organization_raw": 0,
            # keep brief_md / summary_md for preview, drop heavy full content/transcribe
            "content_md": 0, "transcribe_md": 0,
        })
        .sort("release_time_ms", -1)
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
        "summary_md": doc.get("summary_md") or "",
        "transcribe_md": doc.get("transcribe_md") or "",
        "brief_md": doc.get("brief_md") or "",
        "description_md": doc.get("description_md") or "",
        "source_url": doc.get("source_url"),
        "download_url": doc.get("download_url"),
        "addresses": doc.get("addresses") or [],
        "co_host_organizations": doc.get("co_host_organizations") or [],
        "expert_public_resume": doc.get("expert_public_resume"),
        "events": doc.get("events") or [],
        "event_ids": doc.get("event_ids") or [],
        "meeting_ids": doc.get("meeting_ids") or [],
        "data_level_rule": doc.get("data_level_rule"),
        "ticker_tags": build_ticker_tags(doc, "acecamp", CATEGORY_SPEC[category][0]),
    }


@router.get("/items/{category}/{item_id}/pdf")
async def get_item_pdf(
    category: str,
    item_id: str,
    download: int = Query(0, ge=0, le=1,
                          description="1=强制下载; 0=浏览器内联预览"),
    user: User = Depends(get_current_user),
):
    """流式返回 AceCamp PDF (仅 articles 的少数 can_download=True 文章有).

    scraper 目前只存下载链接不下载文件; 如需本地化, scraper 可扩展为下载到
    acecamp_pdf_dir. 目前 PDF 大多由 S3 直链返回, 此端点兼容未来本地化.
    """
    if category not in CATEGORY_SPEC:
        raise HTTPException(400, "Unknown category")
    coll = _collection_for(category)
    doc = await coll.find_one(
        {"_id": item_id},
        projection={"pdf_local_path": 1, "pdf_size_bytes": 1, "download_url": 1,
                    "title": 1},
    )
    if not doc:
        raise HTTPException(404, "Item not found")

    rel = doc.get("pdf_local_path")
    if not rel or (doc.get("pdf_size_bytes") or 0) <= 0:
        remote = doc.get("download_url")
        if remote:
            # 提示前端直接跳转远程 URL; API 不代理抓远程 PDF
            raise HTTPException(
                307,
                detail={"redirect": remote,
                        "reason": "PDF stored remotely (S3); follow download_url"},
            )
        raise HTTPException(404, "PDF not available")

    settings = get_settings()
    title = (doc.get("title") or f"acecamp-{item_id[:12]}")[:120]
    from ..services.pdf_storage import stream_pdf_or_file
    return await stream_pdf_or_file(
        db=coll.database,
        pdf_rel_path=rel,
        pdf_root=settings.acecamp_pdf_dir,
        download_filename=title,
        download=bool(download),
    )


# --------------------------------------------------------------------------- #
# Stats — for dashboard
# --------------------------------------------------------------------------- #
@router.get("/stats", response_model=StatsResponse)
async def get_stats(user: User = Depends(get_current_user)):
    db = _mongo_db()
    # 今日新增 = 今日"平台发布"条数, 按 release_time_ms >= 本地午夜.
    # 2026-04-23 修正: 原来用 crawled_at, 但回填 (backfill) 把历史 doc 的
    # crawled_at 设成 now, 让数字虚高 (观点曾显示 456 条 "今日", 实际平台当天 0 条).
    # 现在按 release_time_ms 算, 反映平台真实发布节奏, 回填不会污染.
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
        # 今日新增 = 今天平台发布的, 按 release_time_ms (毫秒戳)
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

    # last 7 days per-UI-category
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

    # crawler checkpoints (scraper saves under articles/events keys)
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

    # daily platform vs local (scraper --today fills this)
    daily = await state_coll.find_one({"_id": f"daily_{today_str}"})
    daily_platform_stats = None
    if daily:
        daily_platform_stats = {}
        # scraper saves under raw type (articles/opinions), map to UI categories
        for cat, (cname, _subfilter) in CATEGORY_SPEC.items():
            raw_key = cname  # articles | opinions
            sub = daily.get(raw_key) or {}
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
