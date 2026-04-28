"""REST API exposing MongoDB-backed Meritco (research.meritco-group.com / 久谦中台) forum data.

Reads directly from the `meritco` MongoDB database populated by
`crawl/meritco_crawl/scraper.py`. The `forum` collection holds three
forum_types: 1 = 活动/活动预告, 2 = 专业内容 (纪要+研报+其他报告), 3 = 久谦自研.
"""
from __future__ import annotations

import json
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


FORUM_TYPE_LABELS = {
    1: "活动",
    2: "专业内容",
    3: "久谦自研",
}


@lru_cache(maxsize=1)
def _mongo_client() -> AsyncIOMotorClient:
    return AsyncIOMotorClient(get_settings().meritco_mongo_uri, tz_aware=True)


def _db() -> AsyncIOMotorDatabase:
    return _mongo_client()[get_settings().meritco_mongo_db]


def _author_names(authors: Any) -> list[str]:
    if not isinstance(authors, list):
        return []
    out: list[str] = []
    for a in authors:
        if isinstance(a, dict):
            name = a.get("name") or a.get("nickname")
            if name:
                out.append(str(name))
        elif a:
            out.append(str(a))
    return out


def _clean_url(v: Any) -> str:
    """pdf_url / meeting_link may be stored as the literal string "[]" (empty list
    serialization artifact from the scraper). Strip that out."""
    if not isinstance(v, str):
        return ""
    s = v.strip()
    if not s or s in ("[]", "null", "None"):
        return ""
    return s


def _build_pdf_preview_url(forum_id: Any, encrypted_url: str, name: str,
                            meeting_time: str) -> str:
    """构造 meritco 前端的 /forumPDF?query=<json> URL.

    Meritco 的 SPA 走这个路由, 服务端用用户 session 解密 URL 再回流 PDF.
    用户只要在浏览器里登过 research.meritco-group.com 就能直接预览,
    不需要我们自己破解加密.
    """
    if not encrypted_url:
        return ""
    payload = json.dumps(
        {"url": encrypted_url, "name": name,
         "time": meeting_time or "", "id": forum_id},
        ensure_ascii=False, separators=(",", ":"),
    )
    return f"https://research.meritco-group.com/forumPDF?query={urlquote(payload, safe='')}"


def _parse_pdf_files(v: Any, doc: dict | None = None) -> list[dict]:
    """Scraper stores pdf_url as JSON-serialized string like
    '[{"uid":..,"name":"xxx.pdf","size":12345,"type":"application/pdf","url":"<encrypted>"}]'.

    Two layers of PDF access:
      1. ``preview_url`` — meritco SPA /forumPDF 页面. 依赖用户在
         research.meritco-group.com 有活跃 session (降级方案).
      2. ``local_pdf_url`` — 我们自己的 /api/meritco-db/forum/<id>/pdf?i=<idx>
         (2026-04-21 起, scraper 已下载 PDF 到 meritco_pdf_dir). 走 FastAPI
         后端, 用户无需额外登录 meritco 就能下载/预览.
    """
    if not isinstance(v, str):
        return []
    s = v.strip()
    if not s or s in ("[]", "null", "None"):
        return []
    try:
        arr = json.loads(s)
    except (ValueError, TypeError):
        return []
    if not isinstance(arr, list):
        return []

    forum_id = ""
    meeting_time = ""
    attachments_meta: list[dict] = []
    if doc:
        forum_id = doc.get("_id") or doc.get("forum_id") or doc.get("id") or ""
        meeting_time = doc.get("meeting_time") or ""
        raw_atts = doc.get("pdf_attachments")
        if isinstance(raw_atts, list):
            attachments_meta = [a for a in raw_atts if isinstance(a, dict)]

    def _match_meta(idx: int, uid: Any) -> dict | None:
        # 先按 uid 匹配 (最稳), 退化按 index
        if uid is not None:
            for m in attachments_meta:
                if m.get("uid") == uid:
                    return m
        if 0 <= idx < len(attachments_meta):
            return attachments_meta[idx]
        return None

    out: list[dict] = []
    for idx, item in enumerate(arr):
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        if not name:
            continue
        size = item.get("size") or 0
        try:
            size = int(size)
        except (TypeError, ValueError):
            size = 0
        if size >= 1024 * 1024:
            size_display = f"{size / (1024 * 1024):.1f} MB"
        elif size >= 1024:
            size_display = f"{size / 1024:.0f} KB"
        else:
            size_display = f"{size} B" if size else ""
        enc_url = str(item.get("url") or "")

        meta = _match_meta(idx, item.get("uid"))
        has_local = bool(
            meta
            and int(meta.get("pdf_size_bytes") or 0) > 0
            and not meta.get("pdf_download_error")
        )
        local_url = ""
        if has_local and forum_id not in ("", None):
            local_url = f"/api/meritco-db/forum/{forum_id}/pdf?i={idx}"

        out.append({
            "name": name,
            "size_bytes": size,
            "size_display": size_display,
            "preview_url": _build_pdf_preview_url(forum_id, enc_url, name, meeting_time),
            "has_local_pdf": has_local,
            "local_pdf_url": local_url,
            "local_pdf_size": int(meta.get("pdf_size_bytes") or 0) if meta else 0,
        })
    return out


def _brief(doc: dict) -> dict:
    # 周报 items ship only content_md (summary_md / insight_md empty),
    # so fall through content_md as last resort to avoid blank list rows.
    raw = (
        doc.get("summary_md")
        or doc.get("insight_md")
        or doc.get("content_md")
        or ""
    ).strip()
    # Strip markdown heading markers/bold for cleaner list preview
    cleaned = raw.replace("**", "").lstrip("#").strip()
    preview = cleaned[:360] + "…" if len(cleaned) > 360 else cleaned

    title_str = doc.get("title") or ""
    is_weekly = "周报" in title_str

    related = doc.get("related_targets") or []
    stats = doc.get("stats") or {}
    return {
        "id": str(doc.get("_id")),
        "forum_id": str(doc.get("id") or doc.get("_id")),
        "forum_type": doc.get("forum_type"),
        "forum_type_label": FORUM_TYPE_LABELS.get(doc.get("forum_type"), "未知"),
        "title": doc.get("title"),
        "release_time": doc.get("release_time"),
        "meeting_time": doc.get("meeting_time"),
        "industry": doc.get("industry") or "",
        "author": doc.get("author") or "",
        "authors": _author_names(doc.get("authors")),
        "experts": _author_names(doc.get("experts")),
        "expert_type_name": doc.get("expert_type_name") or "",
        "report_type_name": doc.get("report_type_name") or "",
        "related_targets": related if isinstance(related, list) else [],
        "keyword_arr": doc.get("keyword_arr") or [],
        "hot_flag": bool(doc.get("hot_flag")),
        "is_top": int(doc.get("is_top") or 0),
        "language": doc.get("language"),
        "pdf_files": _parse_pdf_files(doc.get("pdf_url"), doc),
        "meeting_link": _clean_url(doc.get("meeting_link")),
        "web_url": doc.get("web_url"),
        "preview": preview,
        "stats": {
            "content_chars": int(stats.get("正文字数") or 0),
            "insight_chars": int(stats.get("速览字数") or 0),
            "summary_chars": int(stats.get("摘要字数") or 0),
            "experts": int(stats.get("专家数") or 0),
            "related_targets": int(stats.get("关联标的") or 0),
        },
        "has_summary": bool(doc.get("summary_md")),
        "has_insight": bool(doc.get("insight_md")),
        "has_content": bool(doc.get("content_md")),
        "has_expert_content": bool(doc.get("expert_content_md")),
        "is_weekly_report": is_weekly,
        "crawled_at": doc.get("crawled_at"),
    }


class ForumListResponse(BaseModel):
    items: list[dict]
    total: int
    page: int
    page_size: int
    has_next: bool


class StatsResponse(BaseModel):
    total: int
    today: int
    last_7_days: list[dict]
    per_forum_type: list[dict]
    top_authors: list[dict]
    top_industries: list[dict]
    top_targets: list[dict]
    top_keywords: list[dict]
    latest_release_time: str | None
    crawler_state: list[dict]
    daily_platform_stats: dict | None
    content_coverage: dict


# ------------------------------------------------------------------ #
# Forum list + detail
# ------------------------------------------------------------------ #
@router.get("/forum", response_model=ForumListResponse)
async def list_forum(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    forum_type: int | None = Query(None, ge=1, le=3),
    q: str | None = Query(None, description="Full-text filter on title/summary"),
    industry: str | None = None,
    author: str | None = None,
    target: str | None = Query(None, description="Filter by related target ticker/name"),
    user: User = Depends(get_current_user),
):
    coll = _db()["forum"]
    match: dict[str, Any] = {}
    if forum_type is not None:
        match["forum_type"] = forum_type
    if q:
        match["$or"] = [
            {"title": {"$regex": q, "$options": "i"}},
            {"summary_md": {"$regex": q, "$options": "i"}},
            {"insight_md": {"$regex": q, "$options": "i"}},
        ]
    if industry:
        match["industry"] = {"$regex": industry, "$options": "i"}
    if author:
        match["author"] = {"$regex": author, "$options": "i"}
    if target:
        match["related_targets"] = {"$regex": target, "$options": "i"}

    total = await coll.count_documents(match)
    cursor = (
        coll.find(
            match,
            projection={
                # Exclude heavy raw fields from the list response
                "list_item": 0,
                "detail_result": 0,
                "content_md": 0,
                "topic_md": 0,
                "background_md": 0,
                "expert_content_md": 0,
            },
        )
        .sort("release_time", -1)
        .skip((page - 1) * page_size)
        .limit(page_size)
    )
    items = [_brief(d) async for d in cursor]
    return ForumListResponse(
        items=items,
        total=total,
        page=page,
        page_size=page_size,
        has_next=(page * page_size) < total,
    )


@router.get("/forum/{forum_id}")
async def get_forum_item(forum_id: str, user: User = Depends(get_current_user)):
    coll = _db()["forum"]
    doc: dict | None = None
    # _id may be string or int
    try:
        as_int = int(forum_id)
        doc = await coll.find_one({"_id": as_int})
    except ValueError:
        pass
    if not doc:
        doc = await coll.find_one({"_id": forum_id})
    if not doc:
        doc = await coll.find_one({"id": forum_id})
    if not doc:
        raise HTTPException(404, "Forum item not found")

    brief = _brief(doc)
    return {
        **brief,
        "summary_md": doc.get("summary_md") or "",
        "insight_md": doc.get("insight_md") or "",
        "content_md": doc.get("content_md") or "",
        "topic_md": doc.get("topic_md") or "",
        "background_md": doc.get("background_md") or "",
        "expert_content_md": doc.get("expert_content_md") or "",
        "pdf_text_md": doc.get("pdf_text_md") or "",
        "ticker_tags": build_ticker_tags(doc, "meritco", "forum"),
    }


@router.get("/forum/{forum_id}/pdf")
async def get_forum_pdf(
    forum_id: str,
    i: int = Query(0, ge=0, le=9,
                   description="附件索引 (多附件时用; 默认 0=第一个)"),
    download: int = Query(0, ge=0, le=1,
                          description="1=强制下载; 0=浏览器内联预览"),
    user: User = Depends(get_current_user),
):
    """流式返回 Meritco 论坛文档的 PDF 附件.

    Scraper 把 scraper.py 的 pdf_attachments[i].pdf_local_path 落到
    settings.meritco_pdf_dir 下, 这里做目录穿越防御后 FileResponse 返回.
    """
    coll = _db()["forum"]
    doc: dict | None = None
    try:
        as_int = int(forum_id)
        doc = await coll.find_one({"_id": as_int},
                                  projection={"pdf_attachments": 1, "title": 1})
    except ValueError:
        pass
    if not doc:
        doc = await coll.find_one({"_id": forum_id},
                                  projection={"pdf_attachments": 1, "title": 1})
    if not doc:
        doc = await coll.find_one({"id": forum_id},
                                  projection={"pdf_attachments": 1, "title": 1})
    if not doc:
        raise HTTPException(404, "Forum item not found")

    attachments = doc.get("pdf_attachments") or []
    if not isinstance(attachments, list) or not attachments:
        raise HTTPException(404, "No PDF attachments for this item")
    if i >= len(attachments):
        raise HTTPException(404, f"Attachment index {i} out of range (0..{len(attachments)-1})")

    att = attachments[i] or {}
    rel = att.get("pdf_local_path")
    if not rel or int(att.get("pdf_size_bytes") or 0) <= 0:
        err = att.get("pdf_download_error") or "PDF 未下载"
        raise HTTPException(404, f"PDF not available: {err}")

    settings = get_settings()
    title = (att.get("name") or doc.get("title") or f"meritco-{forum_id}-{i}")[:120]
    from ..services.pdf_storage import stream_pdf_or_file
    return await stream_pdf_or_file(
        db=_db(),
        pdf_rel_path=rel,
        pdf_root=settings.meritco_pdf_dir,
        download_filename=title,
        download=bool(download),
    )


# ------------------------------------------------------------------ #
# Stats
# ------------------------------------------------------------------ #
@router.get("/stats", response_model=StatsResponse)
async def get_stats(user: User = Depends(get_current_user)):
    coll = _db()["forum"]
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    total = await coll.count_documents({})
    today = await coll.count_documents({"release_time": {"$regex": f"^{today_str}"}})

    latest_doc = await coll.find_one(
        {}, sort=[("release_time", -1)], projection={"release_time": 1}
    )
    latest = latest_doc.get("release_time") if latest_doc else None

    # Per forum_type counts
    per_type_raw = [
        d async for d in coll.aggregate([{"$group": {"_id": "$forum_type", "n": {"$sum": 1}}}])
    ]
    per_type = [
        {
            "forum_type": d["_id"],
            "label": FORUM_TYPE_LABELS.get(d["_id"], "未知"),
            "count": d["n"],
        }
        for d in sorted(per_type_raw, key=lambda x: x.get("_id") or 0)
    ]

    # Last 7 distinct days by release_time prefix
    pipeline_7d = [
        {"$match": {"release_time": {"$type": "string"}}},
        {
            "$group": {
                "_id": {"$substrBytes": ["$release_time", 0, 10]},
                "n": {"$sum": 1},
            }
        },
        {"$sort": {"_id": -1}},
        {"$limit": 7},
    ]
    last_7_raw = [d async for d in coll.aggregate(pipeline_7d)]
    last_7 = sorted(
        [{"date": d["_id"], "count": d["n"]} for d in last_7_raw],
        key=lambda x: x["date"],
    )

    # Top authors
    pipeline_authors = [
        {"$match": {"author": {"$nin": [None, ""]}}},
        {"$group": {"_id": "$author", "n": {"$sum": 1}}},
        {"$sort": {"n": -1}},
        {"$limit": 10},
    ]
    top_authors = [
        {"name": d["_id"], "count": d["n"]} async for d in coll.aggregate(pipeline_authors)
    ]

    # Top industries (industry is a scalar string here)
    pipeline_inds = [
        {"$match": {"industry": {"$nin": [None, ""]}}},
        {"$group": {"_id": "$industry", "n": {"$sum": 1}}},
        {"$sort": {"n": -1}},
        {"$limit": 10},
    ]
    top_inds = [
        {"name": d["_id"], "count": d["n"]} async for d in coll.aggregate(pipeline_inds)
    ]

    # Top related targets (array of strings, unwind + group)
    pipeline_targets = [
        {"$unwind": "$related_targets"},
        {"$match": {"related_targets": {"$nin": [None, ""]}}},
        {"$group": {"_id": "$related_targets", "n": {"$sum": 1}}},
        {"$sort": {"n": -1}},
        {"$limit": 15},
    ]
    top_targets = [
        {"name": d["_id"], "count": d["n"]} async for d in coll.aggregate(pipeline_targets)
    ]

    # Top keywords
    pipeline_kw = [
        {"$unwind": "$keyword_arr"},
        {"$match": {"keyword_arr": {"$nin": [None, ""]}}},
        {"$group": {"_id": "$keyword_arr", "n": {"$sum": 1}}},
        {"$sort": {"n": -1}},
        {"$limit": 15},
    ]
    top_keywords = [
        {"name": d["_id"], "count": d["n"]} async for d in coll.aggregate(pipeline_kw)
    ]

    # Content coverage (pdf_url may be stored as the literal "[]"; exclude those)
    coverage = {
        "summary": await coll.count_documents({"summary_md": {"$ne": ""}}),
        "insight": await coll.count_documents({"insight_md": {"$ne": ""}}),
        "content": await coll.count_documents({"content_md": {"$ne": ""}}),
        "expert_content": await coll.count_documents({"expert_content_md": {"$ne": ""}}),
        "pdf": await coll.count_documents(
            {"pdf_url": {"$nin": ["", "[]", "null", "None", None]}}
        ),
    }

    # Crawler checkpoints (one per forum_type)
    state_coll = _db()["_state"]
    crawler_state: list[dict] = []
    async for doc in state_coll.find({"_id": {"$regex": "^crawler_type"}}):
        ft = None
        try:
            ft = int(str(doc["_id"]).replace("crawler_type", ""))
        except (ValueError, TypeError):
            pass
        crawler_state.append(
            {
                "forum_type": ft,
                "label": FORUM_TYPE_LABELS.get(ft, "未知"),
                "in_progress": bool(doc.get("in_progress")),
                "last_processed_at": doc.get("last_processed_at"),
                "last_run_end_at": doc.get("last_run_end_at"),
                "last_run_stats": doc.get("last_run_stats") or {},
                "top_id": doc.get("top_id"),
            }
        )
    crawler_state.sort(key=lambda x: x.get("forum_type") or 0)

    daily_platform_stats = None
    # Try the type=2 daily first (the common default), fall back to any
    for key in (f"daily_type2_{today_str}", f"daily_type1_{today_str}", f"daily_type3_{today_str}"):
        daily = await state_coll.find_one({"_id": key})
        if daily:
            daily_platform_stats = {
                "for_type": int(str(daily["_id"]).split("_type")[1].split("_")[0]),
                "total_on_platform": daily.get("total_on_platform", 0),
                "in_db": daily.get("in_db", 0),
                "not_in_db": daily.get("not_in_db", 0),
                "by_author_top10": daily.get("by_author_top10") or [],
                "by_industry_top10": daily.get("by_industry_top10") or [],
            }
            break

    return StatsResponse(
        total=total,
        today=today,
        last_7_days=last_7,
        per_forum_type=per_type,
        top_authors=top_authors,
        top_industries=top_inds,
        top_targets=top_targets,
        top_keywords=top_keywords,
        latest_release_time=latest,
        crawler_state=crawler_state,
        daily_platform_stats=daily_platform_stats,
        content_coverage=coverage,
    )
