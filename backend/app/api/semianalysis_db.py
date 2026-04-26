"""REST API exposing MongoDB-backed SemiAnalysis (newsletter.semianalysis.com)
crawl data.

Backend for the `/semianalysis` frontend page. Reads `semianalysis_posts`
collection (in the `foreign-website` DB as of 2026-04-24 — previously co-hosted
in `funda`; checkpoint lives in `_state_semianalysis` alongside it).

Shape returned:
  _id (stringified "s<post_id>"), title, release_time, release_time_ms,
  audience ('everyone' | 'only_paid' | 'founding'), is_paid, organization,
  authors, subtitle, canonical_url, content_md (detail only), cover_image,
  stats, _canonical_tickers, content_truncated, crawled_at.

Mirrors funda_db.py / acecamp_db.py conventions: Motor `tz_aware=True`,
Asia/Shanghai bucket for today counts, last-7-days rollup, top authors.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, HTTPException, Query
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pydantic import BaseModel

from backend.app.config import get_settings
from backend.app.deps import get_current_user
from backend.app.models.user import User

logger = logging.getLogger(__name__)
router = APIRouter()


@lru_cache(maxsize=1)
def _mongo_client() -> AsyncIOMotorClient:
    return AsyncIOMotorClient(get_settings().semianalysis_mongo_uri, tz_aware=True)


def _mongo_db() -> AsyncIOMotorDatabase:
    return _mongo_client()[get_settings().semianalysis_mongo_db]


def _posts_coll():
    return _mongo_db()[get_settings().semianalysis_collection]


def _state_coll():
    return _mongo_db()[get_settings().semianalysis_state_collection]


# --------------------------------------------------------------------------- #
# Normalizers
# --------------------------------------------------------------------------- #
def _brief(doc: dict) -> dict:
    """Uniform list-view item — no heavy content_md / content_html / list_item."""
    content = doc.get("content_md") or ""
    preview_src = doc.get("subtitle") or doc.get("description") or \
                  doc.get("truncated_body_text") or content
    preview = preview_src[:360] + ("…" if len(preview_src) > 360 else "")
    return {
        "id": str(doc.get("_id") or ""),
        "post_id": int(doc.get("post_id") or 0),
        "slug": doc.get("slug") or "",
        "title": doc.get("title") or "",
        "subtitle": doc.get("subtitle") or "",
        "release_time": doc.get("release_time"),
        "release_time_ms": doc.get("release_time_ms"),
        "post_date": doc.get("post_date"),
        "audience": doc.get("audience"),
        "is_paid": bool(doc.get("is_paid")),
        "content_truncated": bool(doc.get("content_truncated")),
        "section_name": doc.get("section_name"),
        "canonical_url": doc.get("canonical_url"),
        "cover_image": doc.get("cover_image") or "",
        "podcast_url": doc.get("podcast_url") or "",
        "organization": doc.get("organization") or "SemiAnalysis",
        "authors": doc.get("authors") or [],
        "preview": preview,
        "content_length": len(content),
        "word_count": int((doc.get("stats") or {}).get("wordcount") or 0),
        "reaction_count": int((doc.get("stats") or {}).get("reaction_count") or 0),
        "canonical_tickers": doc.get("_canonical_tickers") or [],
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
    today: int
    paid_count: int
    free_count: int
    last_7_days: list[dict]
    top_authors: list[dict]
    latest_release_time: str | None
    crawler_state: dict | None
    daily_platform_stats: dict | None


# --------------------------------------------------------------------------- #
# List + detail
# --------------------------------------------------------------------------- #
@router.get("/posts", response_model=ItemListResponse)
async def list_posts(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    q: str | None = Query(None, description="Full-text on title / subtitle / content"),
    author: str | None = Query(None, description="Author name substring"),
    ticker: str | None = Query(None, description="Canonical ticker substring"),
    audience: str | None = Query(
        None, pattern="^(everyone|only_paid|founding)$",
        description="Filter by audience (everyone / only_paid / founding)",
    ),
    since_days: int | None = Query(None, ge=1, le=365),
    user: User = Depends(get_current_user),
):
    coll = _posts_coll()
    match: dict[str, Any] = {}
    ors: list[dict] = []
    if q:
        ors += [
            {"title": {"$regex": q, "$options": "i"}},
            {"subtitle": {"$regex": q, "$options": "i"}},
            {"content_md": {"$regex": q, "$options": "i"}},
            {"authors": {"$regex": q, "$options": "i"}},
        ]
    if author:
        match["authors"] = {"$regex": author, "$options": "i"}
    if ticker:
        # Canonical tickers are stored as exact "NVDA.US" strings; substring
        # match makes it usable from a search box without needing suffixes.
        match["_canonical_tickers"] = {"$regex": ticker, "$options": "i"}
    if audience:
        match["audience"] = audience
    if since_days:
        from datetime import timedelta
        cutoff_ms = int((datetime.now(timezone.utc)
                          - timedelta(days=since_days)).timestamp() * 1000)
        match["release_time_ms"] = {"$gte": cutoff_ms}
    if ors:
        match["$or"] = ors

    total = await coll.count_documents(match)
    cursor = (
        coll.find(match, projection={
            "list_item": 0, "detail_result": 0,
            "content_md": 0, "content_html": 0,
            "truncated_body_text": 0,
        })
        .sort([("release_time_ms", -1), ("post_id", -1)])
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


@router.get("/posts/{item_id}")
async def get_post(item_id: str, user: User = Depends(get_current_user)):
    coll = _posts_coll()
    # Accept both "s<id>" and bare "<id>"
    query_id = item_id if item_id.startswith("s") else f"s{item_id}"
    doc = await coll.find_one({"_id": query_id})
    if not doc:
        raise HTTPException(404, "Post not found")
    return {
        **_brief(doc),
        "content_md": doc.get("content_md") or "",
        "content_html": doc.get("content_html") or "",
        "truncated_body_text": doc.get("truncated_body_text") or "",
        "description": doc.get("description") or "",
        "detail_result": doc.get("detail_result") or {},
    }


# --------------------------------------------------------------------------- #
# Stats — dashboard
# --------------------------------------------------------------------------- #
@router.get("/stats", response_model=StatsResponse)
async def get_stats(user: User = Depends(get_current_user)):
    coll = _posts_coll()
    state_coll = _state_coll()

    local_tz = ZoneInfo("Asia/Shanghai")
    local_midnight = datetime.now(local_tz).replace(
        hour=0, minute=0, second=0, microsecond=0)
    midnight_ms = int(local_midnight.timestamp() * 1000)
    today_str = datetime.now(local_tz).strftime("%Y-%m-%d")

    total = await coll.count_documents({})
    today = await coll.count_documents({"release_time_ms": {"$gte": midnight_ms}})
    paid = await coll.count_documents({"audience": {"$ne": "everyone"}})
    free = await coll.count_documents({"audience": "everyone"})

    latest_doc = await coll.find_one(
        {}, sort=[("release_time_ms", -1)],
        projection={"release_time": 1},
    )
    latest_release = latest_doc.get("release_time") if latest_doc else None

    # Last 7 days rollup
    pipeline = [
        {"$match": {"release_time": {"$type": "string"}}},
        {"$group": {
            "_id": {"$substrBytes": ["$release_time", 0, 10]},
            "n": {"$sum": 1},
            "paid": {"$sum": {"$cond": [
                {"$eq": ["$audience", "everyone"]}, 0, 1]}},
        }},
        {"$sort": {"_id": -1}},
        {"$limit": 7},
    ]
    last_7 = [
        {"date": d["_id"], "total": d["n"],
         "paid": d.get("paid", 0), "free": d["n"] - d.get("paid", 0)}
        async for d in coll.aggregate(pipeline)
    ]
    last_7.reverse()

    # Top authors (unwind `authors` array)
    pipeline = [
        {"$match": {"authors": {"$ne": []}}},
        {"$unwind": "$authors"},
        {"$group": {"_id": "$authors", "n": {"$sum": 1}}},
        {"$sort": {"n": -1}},
        {"$limit": 10},
    ]
    top_authors = [
        {"name": d["_id"], "count": d["n"]}
        async for d in coll.aggregate(pipeline)
        if d.get("_id")
    ]

    # Crawler checkpoint
    cp = await state_coll.find_one({"_id": "crawler_semianalysis"})
    crawler_state = None
    if cp:
        crawler_state = {
            "top_id": cp.get("top_id"),
            "in_progress": bool(cp.get("in_progress")),
            "last_run_end_at": cp.get("last_run_end_at"),
            "last_run_stats": cp.get("last_run_stats") or {},
            "updated_at": cp.get("updated_at"),
        }

    # Daily platform vs db (scraper --today fills this)
    daily = await state_coll.find_one({"_id": f"daily_semianalysis_{today_str}"})
    daily_platform_stats = None
    if daily:
        daily_platform_stats = {
            "platform_count": daily.get("total_on_platform", 0),
            "in_db": daily.get("in_db", 0),
            "missing": daily.get("not_in_db", 0),
            "scanned_at": daily.get("scanned_at"),
        }

    return StatsResponse(
        total=total,
        today=today,
        paid_count=paid,
        free_count=free,
        last_7_days=last_7,
        top_authors=top_authors,
        latest_release_time=latest_release,
        crawler_state=crawler_state,
        daily_platform_stats=daily_platform_stats,
    )
