"""REST API exposing the MongoDB-backed AlphaPai crawl data.

Reads directly from the `alphapai` MongoDB database populated by
`crawl/alphapai_crawl/scraper.py`. Four collections: roadshows, reports,
comments, wechat_articles. Each document has a normalized top-level shape:
  _id, category, title, publish_time (str "YYYY-MM-DD HH:MM"),
  web_url, content, crawled_at, + category-specific extraction fields.
"""
from __future__ import annotations

import logging
import re
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


CATEGORY_COLLECTION = {
    "roadshow": "roadshows",
    "report": "reports",
    "comment": "comments",
    "wechat": "wechat_articles",
}

CATEGORY_LABEL_CN = {
    "roadshow": "会议路演",
    "report": "券商研报",
    "comment": "券商点评",
    "wechat": "社媒公众号",
}


@lru_cache(maxsize=1)
def _mongo_client() -> AsyncIOMotorClient:
    settings = get_settings()
    return AsyncIOMotorClient(settings.alphapai_mongo_uri, tz_aware=True)


def _mongo_db() -> AsyncIOMotorDatabase:
    settings = get_settings()
    return _mongo_client()[settings.alphapai_mongo_db]


def _extract_institution(doc: dict) -> str | None:
    """Normalize publisher name across category shapes."""
    for key in ("publishInstitution", "accountName"):
        v = doc.get(key)
        if isinstance(v, str) and v:
            return v
    inst = doc.get("institution")
    if isinstance(inst, list) and inst:
        first = inst[0]
        if isinstance(first, dict):
            return first.get("name")
        return str(first) if first else None
    if isinstance(inst, str):
        return inst
    return None


def _extract_stocks(doc: dict) -> list[dict]:
    stocks = doc.get("stock") or []
    if not isinstance(stocks, list):
        return []
    return [
        {"code": s.get("code"), "name": s.get("name")}
        for s in stocks
        if isinstance(s, dict)
    ]


def _extract_industries(doc: dict) -> list[str]:
    ind = doc.get("industry")
    if isinstance(ind, list):
        return [i.get("name") for i in ind if isinstance(i, dict) and i.get("name")]
    if isinstance(ind, str):
        return [ind]
    return []


def _extract_analysts(doc: dict) -> list[str]:
    """Return distinct analyst names."""
    names: list[str] = []
    raw = doc.get("analysts")
    if isinstance(raw, list):
        for a in raw:
            if isinstance(a, dict) and a.get("name"):
                names.append(a["name"])
            elif isinstance(a, str):
                names.append(a)
    single = doc.get("analyst")
    if isinstance(single, str) and single:
        names.append(single)
    elif isinstance(single, list):
        for a in single:
            if isinstance(a, dict) and a.get("name"):
                names.append(a["name"])
            elif isinstance(a, str):
                names.append(a)
    seen: set[str] = set()
    out: list[str] = []
    for n in names:
        if n and n not in seen:
            seen.add(n)
            out.append(n)
    return out


# --------------------------------------------------------------------------- #
# Core viewpoint extraction (研报核心观点)
# --------------------------------------------------------------------------- #
# Keywords that typically appear in the "view / recommendation" part of a
# Chinese research report. Higher-weight ones are more decisive.
_VIEWPOINT_KEYWORDS_HIGH = [
    "投资评级", "盈利预测", "投资建议",
    "维持“买入”", "维持买入", "维持“增持”", "维持增持",
    "上调至", "下调至", "首次覆盖",
    "给予“买入”", "给予买入", "给予“增持”", "给予增持",
    "目标价",
]
_VIEWPOINT_KEYWORDS_LOW = [
    "维持", "上调", "下调", "看好", "看空",
    "建议关注", "建议", "推荐", "评级", "我们认为", "我们预计", "预计",
]
_VIEWPOINT_STOP = ["风险提示", "免责声明", "评级说明", "重要声明"]


def _split_bullets(text: str) -> list[str]:
    """Split content into candidate bullets/paragraphs."""
    if not text:
        return []
    # Prefer explicit bullet markers (◼ / ● / ■)
    if any(m in text for m in ("◼", "●", "■")):
        parts = re.split(r"[\n]*\s*[◼●■]\s*", text)
    else:
        parts = re.split(r"\n{1,}", text)
    return [p.strip() for p in parts if p and p.strip()]


def _extract_core_viewpoint(content: str, max_chars: int = 600) -> str:
    """Pull the "核心观点" bullets out of raw research-report content.

    Algorithm:
      1. Break content into bullets.
      2. Keep the FIRST bullet (usually 事件 / 标题陈述 — sets context).
      3. Add any bullet containing a high-weight viewpoint keyword
         (投资评级 / 盈利预测 / 维持买入 / 目标价 / …).
      4. Drop "风险提示" / "免责声明" tails.
      5. Truncate to ``max_chars`` to keep the card compact.

    Returns a newline-separated markdown-friendly string, or empty if content
    is too short / purely factual.
    """
    if not content or len(content) < 40:
        return ""
    bullets = _split_bullets(content)
    if not bullets:
        return content[:max_chars].strip()

    kept: list[str] = []

    def _ok(bullet: str) -> bool:
        return not any(s in bullet[:16] for s in _VIEWPOINT_STOP)

    # 1) first meaningful bullet
    for b in bullets[:3]:
        if _ok(b):
            kept.append(b)
            break

    # 2) any bullet with a high-weight keyword
    for b in bullets:
        if b in kept or not _ok(b):
            continue
        if any(kw in b for kw in _VIEWPOINT_KEYWORDS_HIGH):
            kept.append(b)

    # 3) if still too short, allow low-weight keyword bullets
    if sum(len(b) for b in kept) < 120:
        for b in bullets:
            if b in kept or not _ok(b):
                continue
            if any(kw in b for kw in _VIEWPOINT_KEYWORDS_LOW):
                kept.append(b)
                break

    out = "\n\n".join(kept).strip()
    if len(out) > max_chars:
        out = out[:max_chars].rstrip() + "…"
    return out


def _normalize_item(doc: dict) -> dict:
    """Map a raw Mongo doc to a uniform front-end shape."""
    content = doc.get("content") or ""
    preview = content if len(content) <= 400 else content[:400] + "…"
    # Sub-category tag (report 特有): list_item.reportType = {"type": 1, "name": "A股公司研究"}
    # 7 known types: A股公司研究(1) / 港股研究(4) / 固收研究(5) / 日报晨会(6) / 金融工程(7) /
    #                行业研究(13) / 宏观研究(14).  roadshow.type/marketTypeV2 也是 UI 分区点.
    li = doc.get("list_item") or {}
    rpt_type = li.get("reportType") if isinstance(li, dict) else None
    if isinstance(rpt_type, dict):
        rpt_type_id = rpt_type.get("type")
        rpt_type_name = rpt_type.get("name")
    else:
        rpt_type_id = None
        rpt_type_name = None
    # roadshow market (10=A股, 20=港美股)
    market_v2 = li.get("marketTypeV2") if isinstance(li, dict) else None
    market_name = {10: "A股", 20: "港美股"}.get(market_v2)

    item = {
        "id": doc["_id"],
        "category": doc.get("category"),
        "title": doc.get("title"),
        "publish_time": doc.get("publish_time"),
        "web_url": doc.get("web_url"),
        "institution": _extract_institution(doc),
        "stocks": _extract_stocks(doc),
        "industries": _extract_industries(doc),
        "analysts": _extract_analysts(doc),
        "content_preview": preview,
        "content_length": len(content),
        "has_pdf": bool(doc.get("pdf_flag") or doc.get("pdf_local_path")),
        "account_name": doc.get("accountName"),
        "source_url": doc.get("url"),
        # Sub-category: UI tag on list row
        "report_type_id": rpt_type_id,
        "report_type_name": rpt_type_name,
        "market_v2": market_v2,
        "market_name": market_name,
        "crawled_at": doc.get("crawled_at"),
    }
    # Core viewpoint — research-report focused; other categories rarely have
    # a distinct "view" section, so we only extract for reports.
    if doc.get("category") == "report":
        item["core_viewpoint"] = _extract_core_viewpoint(content)
    return item


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
    recent_publishers: dict[str, list[dict]]
    latest_per_category: dict[str, str | None]


# ------------------------------------------------------------------ #
# Items
# ------------------------------------------------------------------ #
@router.get("/items", response_model=ItemListResponse)
async def list_items(
    category: str = Query("roadshow", pattern="^(roadshow|report|comment|wechat)$"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    q: str | None = Query(None, description="Full-text filter on title/content"),
    institution: str | None = None,
    ticker: str | None = Query(None, description="Stock code or name fragment"),
    report_type: int | None = Query(
        None,
        description="report only: 1=A股公司研究, 4=港股研究, 5=固收, 6=日报晨会, 7=金融工程, 13=行业研究, 14=宏观研究",
    ),
    market_v2: int | None = Query(
        None,
        description="roadshow only: 10=A股, 20=港美股",
    ),
    subcategory: str | None = Query(
        None,
        pattern="^(ashare|hk|us|web|ir|hot|indep|selected|regular)$",
        description="Per-category sub-tabs (from AlphaPai SPA tabs). "
                    "roadshow: ashare / hk / us / web / ir / hot. "
                    "report: ashare / us / indep. "
                    "comment: selected / regular.",
    ),
    user: User = Depends(get_current_user),
):
    db = _mongo_db()
    coll = db[CATEGORY_COLLECTION[category]]

    match: dict[str, Any] = {}
    if q:
        match["$or"] = [
            {"title": {"$regex": q, "$options": "i"}},
            {"content": {"$regex": q, "$options": "i"}},
        ]
    if institution:
        match["$or"] = match.get("$or", []) + [
            {"publishInstitution": {"$regex": institution, "$options": "i"}},
            {"accountName": {"$regex": institution, "$options": "i"}},
            {"institution.name": {"$regex": institution, "$options": "i"}},
        ]
    if ticker:
        match["$or"] = match.get("$or", []) + [
            {"stock.code": {"$regex": ticker, "$options": "i"}},
            {"stock.name": {"$regex": ticker, "$options": "i"}},
        ]
    if report_type is not None and category == "report":
        match["list_item.reportType.type"] = report_type
    if market_v2 is not None and category == "roadshow":
        match["list_item.marketTypeV2"] = market_v2
    if subcategory and category in {"roadshow", "report", "comment"}:
        # Array-valued tag (a doc can live in multiple tabs); Mongo `{field: val}`
        # matches both array-element-equal and legacy single-string docs.
        match["$or"] = (match.get("$or") or []) + [
            {f"_{category}_subcategories": subcategory},
            {f"_{category}_subcategory": subcategory},
        ]

    total = await coll.count_documents(match)
    cursor = (
        coll.find(match)
        .sort("publish_time", -1)
        .skip((page - 1) * page_size)
        .limit(page_size)
    )
    docs = [_normalize_item(d) async for d in cursor]
    return ItemListResponse(
        items=docs,
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
    db = _mongo_db()
    doc = await db[CATEGORY_COLLECTION[category]].find_one({"_id": item_id})
    if not doc:
        raise HTTPException(404, "Item not found")
    # Strip raw detail/list_item payload to keep response light; expose content
    return {
        **_normalize_item(doc),
        "content": doc.get("content") or "",
        "pdf_local_path": doc.get("pdf_local_path"),
        "pdf_size": doc.get("pdf_size"),
        "raw_id": doc.get("raw_id"),
    }


@router.get("/items/report/{item_id}/pdf")
async def get_report_pdf(
    item_id: str,
    download: int = Query(0, ge=0, le=1,
                          description="1=强制下载 (Content-Disposition: attachment); 0=浏览器内联预览"),
    user: User = Depends(get_current_user),
):
    """流式返回研报 PDF 给前端预览/下载.

    PDF 由 `crawl/alphapai_crawl/scraper.py` 落盘到 `alphapai_pdf_dir`; 这里读 doc
    的 `pdf_local_path`, 校验路径在允许目录内后用 FileResponse 流式返回.
    """
    db = _mongo_db()
    doc = await db["reports"].find_one(
        {"_id": item_id},
        projection={"pdf_local_path": 1, "pdf_size": 1, "pdf_error": 1,
                    "pdf_flag": 1, "title": 1},
    )
    if not doc:
        raise HTTPException(404, "Report not found")
    if not doc.get("pdf_flag"):
        raise HTTPException(404, "This report has no PDF on the platform")

    rel = doc.get("pdf_local_path")
    if not rel:
        err = doc.get("pdf_error") or "pdf_local_path 未记录 (PDF 未下载)"
        raise HTTPException(404, f"PDF not available: {err}")

    # 2026-04-23: 走 Mongo GridFS 优先 / 本地 fallback 的统一读取器.
    # 文件名用 title (去文件名不安全字符) + .pdf, 方便"另存为".
    title = (doc.get("title") or f"report-{item_id[:12]}")[:120]
    from ..services.pdf_storage import stream_pdf_or_file
    settings = get_settings()
    return await stream_pdf_or_file(
        db=db,
        pdf_rel_path=rel,
        pdf_root=settings.alphapai_pdf_dir,
        download_filename=title,
        download=bool(download),
    )


# ------------------------------------------------------------------ #
# Stats — powers the visualization dashboard
# ------------------------------------------------------------------ #
@router.get("/stats", response_model=StatsResponse)
async def get_stats(user: User = Depends(get_current_user)):
    db = _mongo_db()
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    per_category: dict[str, int] = {}
    today: dict[str, int] = {}
    latest_per_category: dict[str, str | None] = {}
    recent_publishers: dict[str, list[dict]] = {}

    for cat, coll_name in CATEGORY_COLLECTION.items():
        coll = db[coll_name]
        # per_category: 全档案 (包括平台已删的历史帖, 因为我们归档不剔除)
        per_category[cat] = await coll.count_documents({})
        # today: 只统计 "在平台上当前仍可见" 的当日发布. 平台删帖后对账脚本
        # 会标 _platform_removed=True, 这些不再计入"今日新增"展示, 避免我们
        # 库的归档数和平台当前显示数对不上.
        today[cat] = await coll.count_documents(
            {
                "publish_time": {"$regex": f"^{today_str}"},
                "_platform_removed": {"$ne": True},
            }
        )
        latest_doc = await coll.find_one(
            {}, sort=[("publish_time", -1)], projection={"publish_time": 1}
        )
        latest_per_category[cat] = latest_doc.get("publish_time") if latest_doc else None

        # Build publisher aggregation: report/comment store institution as an array
        # of {name, ...} objects, so we must $unwind first; roadshow/wechat have
        # scalar string fields.
        if cat in ("report", "comment"):
            pipeline = [
                {"$match": {"institution": {"$ne": None}}},
                {"$unwind": "$institution"},
                {"$match": {"institution.name": {"$ne": None, "$ne": ""}}},
                {"$group": {"_id": "$institution.name", "n": {"$sum": 1}}},
                {"$sort": {"n": -1}},
                {"$limit": 8},
            ]
        else:
            field = "publishInstitution" if cat == "roadshow" else "accountName"
            pipeline = [
                {"$match": {field: {"$nin": [None, ""]}}},
                {"$group": {"_id": f"${field}", "n": {"$sum": 1}}},
                {"$sort": {"n": -1}},
                {"$limit": 8},
            ]
        recent_publishers[cat] = [
            {"name": d["_id"], "count": d["n"]}
            async for d in coll.aggregate(pipeline)
            if d.get("_id")
        ]

    # Last 7 days buckets by publish_time prefix across all categories
    last_7_days: dict[str, dict[str, int]] = {}
    for cat, coll_name in CATEGORY_COLLECTION.items():
        # 同样剔除 _platform_removed=True, 让 7 日趋势与平台现状一致
        pipeline = [
            {
                "$match": {
                    "publish_time": {"$type": "string"},
                    "_platform_removed": {"$ne": True},
                }
            },
            {
                "$group": {
                    "_id": {"$substrBytes": ["$publish_time", 0, 10]},
                    "n": {"$sum": 1},
                }
            },
            {"$sort": {"_id": -1}},
            {"$limit": 7},
        ]
        async for d in db[coll_name].aggregate(pipeline):
            date = d["_id"]
            if date not in last_7_days:
                last_7_days[date] = {c: 0 for c in CATEGORY_COLLECTION}
            last_7_days[date][cat] = d["n"]

    last_7_sorted = sorted(last_7_days.items())[-7:]
    last_7_list = [{"date": d, **counts} for d, counts in last_7_sorted]

    # Crawler checkpoints + today's platform snapshot
    state_coll = db["_state"]
    crawler_state: list[dict] = []
    async for s in state_coll.find({"_id": {"$regex": "^crawler_"}}):
        crawler_state.append(
            {
                "category": s["_id"].replace("crawler_", ""),
                "last_processed_at": s.get("last_processed_at"),
                "last_run_end_at": s.get("last_run_end_at"),
                "last_run_stats": s.get("last_run_stats") or {},
                "in_progress": bool(s.get("in_progress")),
            }
        )

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

    total = sum(per_category.values())
    return StatsResponse(
        total=total,
        per_category=per_category,
        today=today,
        last_7_days=last_7_list,
        crawler_state=crawler_state,
        daily_platform_stats=daily_platform_stats,
        recent_publishers=recent_publishers,
        latest_per_category=latest_per_category,
    )
