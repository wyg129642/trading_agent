"""REST API exposing MongoDB-backed Third Bridge (高临咨询 / forum.thirdbridge.com) data.

Reads directly from the `thirdbridge` MongoDB database populated by
`crawl/third_bridge/scraper.py`. Collection `interviews` holds expert-interview
records with transcripts, agenda, specialists, target companies, etc.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pydantic import BaseModel

from backend.app.config import get_settings
from backend.app.deps import get_current_user
from backend.app.models.user import User
from backend.app.services.ticker_tags_builder import build_ticker_tags

logger = logging.getLogger(__name__)
router = APIRouter()


@lru_cache(maxsize=1)
def _mongo_client() -> AsyncIOMotorClient:
    return AsyncIOMotorClient(get_settings().thirdbridge_mongo_uri, tz_aware=True)


def _db() -> AsyncIOMotorDatabase:
    return _mongo_client()[get_settings().thirdbridge_mongo_db]


def _specialist_brief(s: Any) -> dict | None:
    if not isinstance(s, dict):
        return None
    first = (s.get("firstName") or "").strip()
    last = (s.get("lastName") or "").strip()
    name = f"{first} {last}".strip()
    stypes = s.get("specialistType") or []
    return {
        "name": name,
        "title": s.get("title") or "",
        "types": stypes if isinstance(stypes, list) else [],
    }


def _company_brief(c: Any) -> dict | None:
    if not isinstance(c, dict):
        return None
    return {
        "label": c.get("label") or "",
        "ticker": c.get("ticker") or "",
        "country": c.get("country") or "",
        "sector": c.get("sector") or "",
        "public": bool(c.get("public")),
    }


def _brief(doc: dict) -> dict:
    agenda_md = (doc.get("agenda_md") or "").strip()
    transcript_md = doc.get("transcript_md") or ""
    preview = agenda_md[:360] + "…" if len(agenda_md) > 360 else agenda_md

    stats = doc.get("stats") or {}
    targets = [x for x in (_company_brief(c) for c in (doc.get("target_companies") or [])) if x]
    relevants = [
        x for x in (_company_brief(c) for c in (doc.get("relevant_companies") or [])) if x
    ]
    specialists = [
        x for x in (_specialist_brief(s) for s in (doc.get("specialists") or [])) if x
    ]

    return {
        "id": str(doc.get("_id")),
        "uuid": doc.get("uuid") or str(doc.get("_id")),
        "title": doc.get("title"),
        "release_time": doc.get("release_time"),
        "status": doc.get("status"),
        "language": doc.get("language_label") or doc.get("language_id") or "",
        "content_type": doc.get("content_type_label") or "",
        "researcher_email": doc.get("researcher_email") or "",
        "target_companies": targets,
        "relevant_companies": relevants,
        "specialists": specialists,
        "moderators": doc.get("moderators") or [],
        "themes": doc.get("themes") or [],
        "sectors": doc.get("sectors") or [],
        "geographies": doc.get("geographies") or [],
        "transcripts_available": doc.get("transcripts_available") or [],
        "pdf_available": doc.get("pdf_available") or [],
        "audio": bool(doc.get("audio")),
        "has_commentary": bool(doc.get("has_commentary")),
        "preview": preview,
        "stats": {
            "transcript_segments": int(stats.get("转录段数") or 0),
            "transcript_chars": int(stats.get("转录字数") or 0),
            "agenda_items": int(stats.get("议程条数") or 0),
            "specialists": int(stats.get("专家数") or 0),
            "target_companies": int(stats.get("目标公司") or 0),
            "relevant_companies": int(stats.get("相关公司") or 0),
            "commentary_items": int(stats.get("点评条数") or 0),
            "commentary_chars": int(stats.get("点评字数") or 0),
        },
        "has_transcript": int(stats.get("转录字数") or 0) > 0,
        "has_commentary_content": int(stats.get("点评字数") or 0) > 0,
        "web_url": doc.get("web_url") or (
            f"https://forum.thirdbridge.com/zh/interview/{doc.get('uuid') or doc.get('_id')}"
        ),
        "crawled_at": doc.get("crawled_at"),
    }


class InterviewListResponse(BaseModel):
    items: list[dict]
    total: int
    page: int
    page_size: int
    has_next: bool


class StatsResponse(BaseModel):
    total: int
    today: int
    with_transcript: int
    latest_release_time: str | None
    crawler_state: dict | None
    daily_platform_stats: dict | None


@router.get("/interviews", response_model=InterviewListResponse)
async def list_interviews(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    q: str | None = Query(None, description="Full-text filter on title/agenda/transcript"),
    company: str | None = Query(None, description="Filter by target/relevant company label or ticker"),
    only_with_transcript: bool = Query(False, description="Only show interviews with transcript content"),
    user: User = Depends(get_current_user),
):
    coll = _db()["interviews"]
    match: dict[str, Any] = {}
    if q:
        match["$or"] = [
            {"title": {"$regex": q, "$options": "i"}},
            {"agenda_md": {"$regex": q, "$options": "i"}},
            {"transcript_md": {"$regex": q, "$options": "i"}},
            {"commentary_md": {"$regex": q, "$options": "i"}},
        ]
    if company:
        # Match either ticker or label on target/relevant
        company_or = [
            {"target_companies.label": {"$regex": company, "$options": "i"}},
            {"target_companies.ticker": {"$regex": company, "$options": "i"}},
            {"relevant_companies.label": {"$regex": company, "$options": "i"}},
            {"relevant_companies.ticker": {"$regex": company, "$options": "i"}},
        ]
        if "$or" in match:
            # Combine with an $and so both q and company are respected
            match = {"$and": [{"$or": match["$or"]}, {"$or": company_or}]}
        else:
            match["$or"] = company_or
    if only_with_transcript:
        tx_match = {"stats.转录字数": {"$gt": 0}}
        match = {"$and": [match, tx_match]} if match else tx_match

    total = await coll.count_documents(match)
    cursor = (
        coll.find(
            match,
            projection={
                # Keep list lean
                "list_item": 0,
                "detail_result": 0,
                "entitlements": 0,
                "rules": 0,
                "transcript_items": 0,
                "introduction_items": 0,
                "transcript_md": 0,
                "introduction_md": 0,
                "commentary_items": 0,
                "commentary_md": 0,
            },
        )
        .sort("release_time_ms", -1)
        .skip((page - 1) * page_size)
        .limit(page_size)
    )
    items = [_brief(d) async for d in cursor]
    return InterviewListResponse(
        items=items,
        total=total,
        page=page,
        page_size=page_size,
        has_next=(page * page_size) < total,
    )


@router.get("/interviews/{interview_id}")
async def get_interview(interview_id: str, user: User = Depends(get_current_user)):
    coll = _db()["interviews"]
    doc = await coll.find_one({"_id": interview_id})
    if not doc:
        doc = await coll.find_one({"uuid": interview_id})
    if not doc:
        raise HTTPException(404, "Interview not found")

    brief = _brief(doc)
    return {
        **brief,
        "agenda_md": doc.get("agenda_md") or "",
        "specialists_md": doc.get("specialists_md") or "",
        "introduction_md": doc.get("introduction_md") or "",
        "transcript_md": doc.get("transcript_md") or "",
        "commentary_md": doc.get("commentary_md") or "",
        "ticker_tags": build_ticker_tags(doc, "thirdbridge", "interviews"),
    }


@router.get("/stats", response_model=StatsResponse)
async def get_stats(user: User = Depends(get_current_user)):
    coll = _db()["interviews"]
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    total = await coll.count_documents({})
    today = await coll.count_documents({"release_time": {"$regex": f"^{today_str}"}})
    with_transcript = await coll.count_documents({"stats.转录字数": {"$gt": 0}})

    latest_doc = await coll.find_one(
        {}, sort=[("release_time_ms", -1)], projection={"release_time": 1}
    )
    latest = latest_doc.get("release_time") if latest_doc else None

    state_coll = _db()["_state"]
    crawler_doc = await state_coll.find_one({"_id": "crawler_interviews"})
    crawler_state = None
    if crawler_doc:
        crawler_state = {
            "in_progress": bool(crawler_doc.get("in_progress")),
            "last_processed_at": crawler_doc.get("last_processed_at"),
            "last_run_end_at": crawler_doc.get("last_run_end_at"),
            "last_run_stats": crawler_doc.get("last_run_stats") or {},
            "top_uuid": crawler_doc.get("top_uuid"),
        }

    daily = await state_coll.find_one({"_id": f"daily_{today_str}"})
    daily_platform_stats = None
    if daily:
        daily_platform_stats = {
            "total_on_platform": daily.get("total_on_platform", 0),
            "in_db": daily.get("in_db", 0),
            "not_in_db": daily.get("not_in_db", 0),
            "by_content_type": daily.get("by_content_type") or {},
            "by_sector_top10": daily.get("by_sector_top10") or [],
        }

    return StatsResponse(
        total=total,
        today=today,
        with_transcript=with_transcript,
        latest_release_time=latest,
        crawler_state=crawler_state,
        daily_platform_stats=daily_platform_stats,
    )
