"""REST API exposing MongoDB-backed 微信公众号 (mp.weixin.qq.com) crawl data.

Reads from the `wechat-mp` MongoDB database populated by
``crawl/wechat_mp/scraper.py``. Single primary collection ``articles``;
``account`` holds fakeid metadata; ``_state`` is internal checkpoint.

Routes:
  GET /api/wechat-mp-db/stats                          card metrics
  GET /api/wechat-mp-db/articles                       list (filter+paginate)
  GET /api/wechat-mp-db/articles/{id}                  full doc
  GET /api/wechat-mp-db/articles/{id}/image/{idx}      stream local image
  GET /api/wechat-mp-db/accounts                       cached account meta

Image streaming serves files from
``settings.wechat_mp_image_root`` so the front-end can render them without
hitting mmbiz.qpic.cn (防盗链 + 平台风控).
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from backend.app.config import get_settings
from backend.app.deps import get_current_user
from backend.app.models.user import User

router = APIRouter()


@lru_cache(maxsize=1)
def _client() -> AsyncIOMotorClient:
    return AsyncIOMotorClient(get_settings().wechat_mp_mongo_uri, tz_aware=True)


def _db() -> AsyncIOMotorDatabase:
    return _client()[get_settings().wechat_mp_mongo_db]


def _articles():
    return _db()["articles"]


def _accounts():
    return _db()["account"]


def _image_root() -> Path:
    return Path(get_settings().wechat_mp_image_root)


def _brief(doc: dict) -> dict:
    """List-card brief — strip heavy fields."""
    md_len = len(doc.get("content_md") or "")
    return {
        "id": str(doc.get("_id")),
        "url": doc.get("url"),
        "biz": doc.get("biz"),
        "appmsgid": doc.get("appmsgid"),
        "itemidx": doc.get("itemidx"),
        "sn": doc.get("sn"),
        "account_name": doc.get("account_name"),
        "title": doc.get("title") or "",
        "author": doc.get("author") or "",
        "digest": (doc.get("digest") or "")[:300],
        "cover": doc.get("cover") or "",
        "release_time": doc.get("release_time"),
        "release_time_ms": doc.get("release_time_ms"),
        "content_length": md_len,
        "image_count": len(doc.get("images") or []),
        "fetch_error": doc.get("fetch_error"),
        "_canonical_tickers": doc.get("_canonical_tickers") or [],
    }


@router.get("/stats")
async def stats(_user: User = Depends(get_current_user)) -> dict[str, Any]:
    coll = _articles()
    total = await coll.estimated_document_count()
    accounts = await _accounts().count_documents({})
    latest = await coll.find_one(
        {}, sort=[("release_time_ms", -1)],
        projection={"title": 1, "release_time": 1, "release_time_ms": 1, "account_name": 1},
    )
    today = await coll.aggregate([
        {"$match": {"release_time_ms": {"$exists": True}}},
        {"$group": {"_id": None,
                    "max_ms": {"$max": "$release_time_ms"},
                    "min_ms": {"$min": "$release_time_ms"}}},
    ]).to_list(length=1)
    by_account = await coll.aggregate([
        {"$group": {"_id": "$account_name", "n": {"$sum": 1}}},
        {"$sort": {"n": -1}},
    ]).to_list(length=20)
    return {
        "total_articles": total,
        "total_accounts": accounts,
        "latest": latest,
        "release_time_range_ms": today[0] if today else None,
        "by_account": [{"account_name": x["_id"], "count": x["n"]} for x in by_account],
    }


@router.get("/articles")
async def list_articles(
    account: str | None = Query(None, description="按公众号名过滤"),
    q: str | None = Query(None, description="标题模糊搜索 (case-insensitive)"),
    ticker: str | None = Query(None, description="按 _canonical_tickers 过滤"),
    limit: int = Query(20, ge=1, le=100),
    skip: int = Query(0, ge=0),
    _user: User = Depends(get_current_user),
) -> dict[str, Any]:
    flt: dict[str, Any] = {}
    if account:
        flt["account_name"] = account
    if ticker:
        flt["_canonical_tickers"] = ticker
    if q:
        flt["title"] = {"$regex": q, "$options": "i"}

    coll = _articles()
    total = await coll.count_documents(flt)
    cursor = coll.find(
        flt,
        projection={"html_raw": 0, "list_item": 0},
    ).sort("release_time_ms", -1).skip(skip).limit(limit)
    items = [_brief(d) async for d in cursor]
    return {"total": total, "items": items, "skip": skip, "limit": limit}


@router.get("/articles/{doc_id:path}")
async def get_article(doc_id: str,
                      _user: User = Depends(get_current_user)) -> dict[str, Any]:
    doc = await _articles().find_one({"_id": doc_id})
    if not doc:
        raise HTTPException(404, f"article not found: {doc_id}")
    doc["id"] = str(doc.pop("_id"))
    return doc


@router.get("/articles/{doc_id:path}/image/{idx}")
async def get_article_image(doc_id: str, idx: int,
                            _user: User = Depends(get_current_user)):
    doc = await _articles().find_one({"_id": doc_id}, {"images": 1})
    if not doc:
        raise HTTPException(404, f"article not found: {doc_id}")
    images = doc.get("images") or []
    if idx < 0 or idx >= len(images):
        raise HTTPException(404, f"image idx {idx} out of range (total={len(images)})")
    info = images[idx] or {}
    rel = info.get("local_path") or ""
    if not rel:
        raise HTTPException(404, f"image idx {idx} has no local copy "
                                  f"(download_error={info.get('download_error')!r})")
    abs_path = (_image_root() / rel).resolve()
    root_resolved = _image_root().resolve()
    # 防越界
    try:
        abs_path.relative_to(root_resolved)
    except ValueError:
        raise HTTPException(400, "image path escapes root")
    if not abs_path.exists():
        raise HTTPException(404, f"image file missing on disk: {rel}")
    media_type = "image/jpeg"
    suffix = abs_path.suffix.lower().lstrip(".")
    if suffix in {"png", "gif", "webp", "svg", "bmp"}:
        media_type = f"image/{'svg+xml' if suffix == 'svg' else suffix}"
    return FileResponse(str(abs_path), media_type=media_type)


@router.get("/accounts")
async def list_accounts(_user: User = Depends(get_current_user)) -> list[dict[str, Any]]:
    cursor = _accounts().find({}, sort=[("name", 1)])
    out = []
    async for d in cursor:
        out.append({
            "name": d.get("name") or d.get("_id"),
            "fakeid": d.get("fakeid"),
            "meta": d.get("meta") or {},
            "updated_at": d.get("updated_at"),
        })
    return out
