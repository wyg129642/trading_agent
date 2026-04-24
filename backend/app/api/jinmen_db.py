"""REST API exposing MongoDB-backed Jinmen (进门财经 / brm.comein.cn) meeting data.

Reads directly from the `jinmen` MongoDB database populated by
`crawl/jinmen/scraper.py`. Collection `meetings` holds AI-summarized meeting
records with points_md / chapter_summary_md / indicators_md / transcript_md.
"""
from __future__ import annotations

import asyncio
import base64
import json as _json
import logging
import re
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any
from urllib.parse import quote as urlquote

import httpx
from bson.int64 import Int64
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pydantic import BaseModel

from backend.app.config import get_settings
from backend.app.deps import get_current_user
from backend.app.models.user import User

logger = logging.getLogger(__name__)
router = APIRouter()

# Repo root, used to read crawler credentials for on-demand PDF fetch.
_REPO_ROOT = Path(__file__).resolve().parents[3]


@lru_cache(maxsize=1)
def _mongo_client() -> AsyncIOMotorClient:
    return AsyncIOMotorClient(get_settings().jinmen_mongo_uri, tz_aware=True)


def _db() -> AsyncIOMotorDatabase:
    return _mongo_client()[get_settings().jinmen_mongo_db]


def _to_py_id(value: Any) -> Any:
    """Mongo may store _id as NumberLong (Int64); serialize to plain int."""
    if isinstance(value, Int64):
        return int(value)
    return value


# ------------------------------------------------------------------ #
# On-demand PDF fetch: local file → fall back to upstream (brm.comein.cn).
# The scraper can't keep up with 1 600+ oversea reports in real time, so many
# docs land in Mongo with `pdf_local_path=""`. When a user hits the PDF URL we
# try upstream once, cache the bytes, and serve. On failure we surface the
# HTTP / magic-byte error so the UI can show "下载失败" instead of silently 404.
# ------------------------------------------------------------------ #
_JM_AUTH_CACHE: dict[str, Any] | None = None


def _load_jm_auth() -> dict[str, str] | None:
    """Load `JM_AUTH_INFO` from crawl/jinmen/credentials.json → scraper.py const.

    Cached at module level. Returns None if no token can be decoded.
    """
    global _JM_AUTH_CACHE
    if _JM_AUTH_CACHE is not None:
        return _JM_AUTH_CACHE or None

    blob = ""
    creds_path = _REPO_ROOT / "crawl" / "jinmen" / "credentials.json"
    if creds_path.exists():
        try:
            creds = _json.loads(creds_path.read_text(encoding="utf-8"))
            blob = creds.get("token") or creds.get("JM_AUTH_INFO") or ""
        except Exception as exc:
            logger.warning("jinmen credentials.json parse failed: %s", exc)
    if not blob:
        scraper_path = _REPO_ROOT / "crawl" / "jinmen" / "scraper.py"
        if scraper_path.exists():
            try:
                m = re.search(
                    r'JM_AUTH_INFO\s*=\s*"([^"]+)"',
                    scraper_path.read_text(encoding="utf-8"),
                )
                if m:
                    blob = m.group(1)
            except Exception as exc:
                logger.warning("jinmen scraper.py read failed: %s", exc)
    if not blob:
        _JM_AUTH_CACHE = {}
        return None

    try:
        decoded = base64.b64decode(blob).decode("utf-8", errors="replace")
        auth = _json.loads(decoded).get("value") or {}
    except Exception as exc:
        logger.warning("jinmen JM_AUTH_INFO decode failed: %s", exc)
        _JM_AUTH_CACHE = {}
        return None

    uid = auth.get("uid")
    webtoken = auth.get("webtoken") or auth.get("token")
    if not (uid and webtoken):
        _JM_AUTH_CACHE = {}
        return None
    _JM_AUTH_CACHE = {
        "uid": str(uid),
        "token": str(webtoken),
        "realm": str(auth.get("organizationId") or ""),
    }
    return _JM_AUTH_CACHE


def _jm_pdf_headers() -> dict[str, str]:
    """Mimic crawl/jinmen/scraper.py::create_session headers for PDF OSS fetch."""
    auth = _load_jm_auth() or {}
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/pdf,application/octet-stream,*/*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Referer": "https://brm.comein.cn/",
        "Origin": "https://brm.comein.cn",
        "uid": auth.get("uid", ""),
        "token": auth.get("token", ""),
        "web_token": auth.get("token", ""),
        "realm": auth.get("realm", ""),
        "os": "brm",
        "c": "pc",
        "b": "4.2.0800",
        "brandChannel": "windows",
        "webenv": "comein",
        "language": "zh-CN",
    }


def _safe_fname(s: str, max_len: int = 120) -> str:
    s = re.sub(r'[\\/:*?"<>|\r\n\t]', "_", s or "").strip()
    return s[:max_len] or "report"


def _pick_source_url(doc: dict) -> str:
    """Pick the best upstream URL to fetch the PDF from.

    For oversea_reports, `detail_result.homeOssPdfUrl` is the preview-endpoint
    URL that bypasses the pay-wall (§9.5.8 case B). For internal reports,
    `original_url` is the aliyuncs OSS link set by scraper. HTML URLs (e.g.
    hzinsights vmp pages) get filtered below via `_looks_like_pdf_url`.
    """
    detail = doc.get("detail_result") or {}
    return (
        detail.get("homeOssPdfUrl")
        or doc.get("original_url")
        or detail.get("originalUrl")
        or ""
    )


def _looks_like_pdf_url(url: str) -> bool:
    """Whitelist: URL path has .pdf / .doc / .docx / .xls / .xlsx / .pptx extension.

    Avoids proxying HTML display pages (hzinsights vmp) as PDFs.
    """
    if not url:
        return False
    m = re.search(r"\.([a-zA-Z0-9]{2,5})(?:$|\?|#)", url)
    ext = m.group(1).lower() if m else ""
    return ext in {"pdf", "doc", "docx", "xls", "xlsx", "pptx"}


def _pdf_can_be_fetched(doc: dict) -> bool:
    return _looks_like_pdf_url(_pick_source_url(doc))


def _pdf_dest_for(doc: dict) -> Path:
    """Choose a local on-disk destination for an on-demand PDF."""
    settings = get_settings()
    base = Path(settings.jinmen_pdf_dir)
    release_ms = doc.get("release_time_ms") or 0
    try:
        ym = (
            datetime.fromtimestamp(int(release_ms) / 1000).strftime("%Y-%m")
            if release_ms
            else "unknown"
        )
    except Exception:
        ym = "unknown"
    rid = doc.get("_id")
    report_id = doc.get("report_id") or ""
    title = doc.get("title") or f"report_{rid}"
    name = _safe_fname(report_id or title) + ".pdf"
    return base / ym / name


async def _fetch_pdf_bytes(url: str, timeout: float = 45.0) -> bytes:
    """Download PDF bytes from upstream. Raises HTTPException on failure."""
    headers = _jm_pdf_headers()
    try:
        async with httpx.AsyncClient(
            timeout=timeout, trust_env=False, follow_redirects=True,
        ) as client:
            r = await client.get(url, headers=headers)
    except httpx.HTTPError as exc:
        raise HTTPException(502, f"源站请求失败: {type(exc).__name__}: {exc}") from exc
    if r.status_code != 200:
        raise HTTPException(502, f"源站 HTTP {r.status_code}")
    data = r.content
    if not data:
        raise HTTPException(502, "源站返回空响应")
    # Accept PDF (%PDF), MS Office CFB (.doc/.xls), and ZIP (.docx/.xlsx/.pptx).
    pdf_ok = data.startswith(b"%PDF")
    cfb_ok = data.startswith(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1")
    zip_ok = data.startswith(b"PK\x03\x04")
    if not (pdf_ok or cfb_ok or zip_ok):
        raise HTTPException(
            502,
            f"源站响应非 PDF/Office (magic={data[:8].hex()})",
        )
    return data


async def _ensure_local_pdf(
    coll: str, doc: dict,
) -> Path:
    """Return a local PDF path for `doc`. Downloads + caches if missing.

    `coll` is the Mongo collection name so we can upsert `pdf_local_path` /
    `pdf_size_bytes` fields after a successful fetch.
    """
    settings = get_settings()
    base = Path(settings.jinmen_pdf_dir).resolve()

    rel = doc.get("pdf_local_path")
    if rel:
        target: Path | None = None
        try:
            target = Path(rel).resolve()
            target.relative_to(base)
        except ValueError:
            # Stale path from 2026-04-17 PDF data migration (old
            # crawl/jinmen/pdfs/... → /home/ygwang/crawl_data/jinmen_pdfs/...).
            # Don't 403 — fall through to upstream fetch; success will overwrite
            # pdf_local_path with the correct base path.
            logger.info("jinmen pdf outside base, refetching: %s", rel)
            target = None
        except (OSError, RuntimeError) as exc:
            raise HTTPException(500, f"path resolve failed: {exc}") from exc
        if target and target.is_file():
            return target

    # Local file missing — try upstream.
    url = _pick_source_url(doc)
    if not _looks_like_pdf_url(url):
        err = doc.get("pdf_download_error") or "源链接缺失或非 PDF"
        raise HTTPException(404, f"PDF not available: {err}")

    logger.info("jinmen on-demand fetch: coll=%s _id=%s url=%s", coll, doc.get("_id"), url)
    data = await _fetch_pdf_bytes(url)

    dest = _pdf_dest_for(doc)
    # Belt-and-suspenders: dest must be under base.
    try:
        dest_resolved = dest.resolve(strict=False)
        dest_resolved.relative_to(base)
    except ValueError:
        raise HTTPException(500, "计算出的缓存路径越界")
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    try:
        await asyncio.to_thread(tmp.write_bytes, data)
        await asyncio.to_thread(tmp.replace, dest)
    except OSError as exc:
        raise HTTPException(500, f"缓存写盘失败: {exc}") from exc

    # Upsert path back into Mongo so the next request is a cache hit.
    try:
        await _db()[coll].update_one(
            {"_id": doc["_id"]},
            {"$set": {
                "pdf_local_path": str(dest),
                "pdf_size_bytes": len(data),
                "pdf_download_error": "",
                "stats.pdf_大小": len(data),
            }},
        )
    except Exception as exc:  # non-fatal; file is on disk
        logger.warning("jinmen pdf path upsert failed (id=%s): %s", doc.get("_id"), exc)

    return dest


def _brief(doc: dict) -> dict:
    preview = (doc.get("points_md") or "").strip()
    if len(preview) > 360:
        preview = preview[:360] + "…"
    stocks = doc.get("stocks") or []
    industries = doc.get("industry") or []
    themes = doc.get("themes") or []
    stats = doc.get("stats") or {}
    return {
        "id": str(_to_py_id(doc["_id"])),
        "roadshow_id": str(_to_py_id(doc.get("roadshowId") or doc["_id"])),
        "title": doc.get("title"),
        "release_time": doc.get("release_time"),
        "organization": doc.get("organization"),
        "industries": industries if isinstance(industries, list) else [],
        "themes": themes if isinstance(themes, list) else [],
        "stocks": [
            {"name": s.get("name"), "code": s.get("code"), "market": s.get("market")}
            for s in stocks
            if isinstance(s, dict)
        ],
        "creators": doc.get("creators") or [],
        "guests": doc.get("guests") or [],
        "featured_tag": doc.get("featured_tag"),
        "auth_tag": doc.get("auth_tag"),
        "speaker_tag": doc.get("speaker_tag"),
        "content_types": doc.get("content_types") or [],
        "preview": preview,
        "stats": {
            "points_chars": int(stats.get("速览字数") or 0),
            "chapters": int(stats.get("章节") or 0),
            "indicators": int(stats.get("指标") or 0),
            "transcript_items": int(stats.get("对话条数") or 0),
        },
        "has_transcript": bool(doc.get("transcript_md")),
        "has_chapters": bool(doc.get("chapter_summary_md")),
        "has_indicators": bool(doc.get("indicators_md")),
        "web_url": doc.get("web_url") or doc.get("present_url"),
        "crawled_at": doc.get("crawled_at"),
    }


class MeetingListResponse(BaseModel):
    items: list[dict]
    total: int
    page: int
    page_size: int
    has_next: bool


class StatsResponse(BaseModel):
    total: int
    today: int
    last_7_days: list[dict]
    top_organizations: list[dict]
    top_industries: list[dict]
    top_themes: list[dict]
    latest_release_time: str | None
    crawler_state: dict | None
    daily_platform_stats: dict | None
    content_coverage: dict


# ------------------------------------------------------------------ #
# Meeting list + detail
# ------------------------------------------------------------------ #
@router.get("/meetings", response_model=MeetingListResponse)
async def list_meetings(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    q: str | None = Query(None, description="Full-text filter on title/preview"),
    organization: str | None = None,
    industry: str | None = None,
    theme: str | None = None,
    stock: str | None = None,
    user: User = Depends(get_current_user),
):
    coll = _db()["meetings"]
    match: dict[str, Any] = {}
    if q:
        match["$or"] = [
            {"title": {"$regex": q, "$options": "i"}},
            {"points_md": {"$regex": q, "$options": "i"}},
        ]
    if organization:
        match["organization"] = {"$regex": organization, "$options": "i"}
    if industry:
        match["industry"] = {"$regex": industry, "$options": "i"}
    if theme:
        match["themes"] = {"$regex": theme, "$options": "i"}
    if stock:
        match["$or"] = match.get("$or", []) + [
            {"stocks.code": {"$regex": stock, "$options": "i"}},
            {"stocks.name": {"$regex": stock, "$options": "i"}},
        ]

    total = await coll.count_documents(match)
    cursor = (
        coll.find(match, projection={
            # Exclude large raw fields from list to keep responses light
            "list_item": 0,
            "summary_info": 0,
            "detail_auth": 0,
            "chapters": 0,
            "indicators": 0,
            "content_items": 0,
            "transcript_md": 0,
            "chapter_summary_md": 0,
            "indicators_md": 0,
        })
        .sort("release_time", -1)
        .skip((page - 1) * page_size)
        .limit(page_size)
    )
    items = [_brief(d) async for d in cursor]
    return MeetingListResponse(
        items=items,
        total=total,
        page=page,
        page_size=page_size,
        has_next=(page * page_size) < total,
    )


@router.get("/meetings/{meeting_id}")
async def get_meeting(meeting_id: str, user: User = Depends(get_current_user)):
    coll = _db()["meetings"]
    # _id may be an Int64 — try both string and integer lookups
    doc: dict | None = None
    try:
        as_int = int(meeting_id)
        doc = await coll.find_one({"_id": as_int})
        if not doc:
            doc = await coll.find_one({"_id": Int64(as_int)})
    except ValueError:
        pass
    if not doc:
        doc = await coll.find_one({"_id": meeting_id})
    if not doc:
        raise HTTPException(404, "Meeting not found")

    brief = _brief(doc)
    return {
        **brief,
        "points_md": doc.get("points_md") or "",
        "chapter_summary_md": doc.get("chapter_summary_md") or "",
        "indicators_md": doc.get("indicators_md") or "",
        "transcript_md": doc.get("transcript_md") or "",
        "present_url": doc.get("present_url"),
    }


# ------------------------------------------------------------------ #
# ------------------------------------------------------------------ #
# 研报 (reports collection) — brm.comein.cn/reportManage/index
# ------------------------------------------------------------------ #

def _report_brief(doc: dict) -> dict:
    stats = doc.get("stats") or {}
    # 组织方: organization_name 或 organizations[0].name
    org = doc.get("organization_name") or ""
    if not org:
        orgs = doc.get("organizations") or []
        if orgs and isinstance(orgs, list) and isinstance(orgs[0], dict):
            org = orgs[0].get("name") or ""
    companies = doc.get("companies") or []
    company_names = [c.get("name") for c in companies if isinstance(c, dict) and c.get("name")]
    has_pdf = bool(doc.get("pdf_local_path") and doc.get("pdf_size_bytes", 0) > 0)
    return {
        "id": str(_to_py_id(doc["_id"])),
        "report_id": doc.get("report_id") or "",
        "title": doc.get("title") or "",
        "release_time": doc.get("release_time") or "",
        "release_time_ms": doc.get("release_time_ms"),
        "organization_name": org,
        "type_name": doc.get("type_name") or "",
        "content_tags": doc.get("content_tags") or [],
        "industry_tags": doc.get("industry_tags") or [],
        "companies": company_names,
        "is_vip": bool(doc.get("is_vip")),
        "pdf_num": doc.get("pdf_num") or 0,
        "has_pdf": has_pdf,
        "pdf_downloadable": has_pdf or _pdf_can_be_fetched(doc),
        "pdf_size": doc.get("pdf_size_bytes") or 0,
        "summary_preview": (doc.get("summary_md") or "")[:240],
        "stats": stats,
        "web_url": doc.get("web_url") or doc.get("link_url"),
        "crawled_at": doc.get("crawled_at"),
    }


@router.get("/reports")
async def list_reports(
    q: str | None = Query(None, description="搜索标题 / 摘要"),
    organization: str | None = Query(None, description="发布机构关键词"),
    ticker: str | None = Query(None, description="股票代码或名称"),
    has_pdf: bool | None = Query(None, description="仅含 PDF 已下载的"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    user: User = Depends(get_current_user),
):
    """列表 — 对应 jinmen.reports collection (由 crawl/jinmen/scraper.py --reports 灌入)."""
    coll = _db()["reports"]
    match: dict[str, Any] = {}
    if q:
        match["$or"] = [
            {"title": {"$regex": q, "$options": "i"}},
            {"summary_md": {"$regex": q, "$options": "i"}},
        ]
    if organization:
        match["organization_name"] = {"$regex": organization, "$options": "i"}
    if ticker:
        match["$or"] = match.get("$or", []) + [
            {"companies.ticker": {"$regex": ticker, "$options": "i"}},
            {"companies.name": {"$regex": ticker, "$options": "i"}},
            {"title": {"$regex": ticker, "$options": "i"}},
        ]
    if has_pdf is True:
        match["pdf_size_bytes"] = {"$gt": 0}
    elif has_pdf is False:
        match["pdf_size_bytes"] = {"$in": [0, None]}

    total = await coll.count_documents(match)
    # Tie-breaker: 平台 releaseTime 只精确到 "YYYY-MM-DD 00:00", 同日全部 147
    # 条研报共享 release_time_ms → 纯 release_time_ms desc 返回顺序不确定,
    # 最新的 _id 可能被甩到第 8 页. _id 就是 researchId, 平台单调递增分配,
    # 用它做第二排序键 = 新发布排前面.
    cursor = (
        coll.find(match)
        .sort([("release_time_ms", -1), ("_id", -1)])
        .skip((page - 1) * page_size)
        .limit(page_size)
    )
    items = [_report_brief(d) async for d in cursor]
    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "has_next": (page * page_size) < total,
    }


@router.get("/reports/{report_id}")
async def get_report(
    report_id: str,
    user: User = Depends(get_current_user),
):
    # _id 是 int (jinmen 平台 research id)
    try:
        _id: Any = int(report_id)
    except ValueError:
        _id = report_id
    coll = _db()["reports"]
    doc = await coll.find_one({"_id": _id})
    if not doc:
        raise HTTPException(404, "Report not found")
    return {
        **_report_brief(doc),
        "summary_md": doc.get("summary_md") or "",
        "summary_point_md": doc.get("summary_point_md") or "",
        "original_url": doc.get("original_url") or "",
        "link_url": doc.get("link_url") or "",
        "pdf_local_path": doc.get("pdf_local_path") or "",
        "pdf_download_error": doc.get("pdf_download_error") or "",
    }


@router.get("/reports/{report_id}/pdf")
async def get_report_pdf(
    report_id: str,
    download: int = Query(0, ge=0, le=1,
                          description="1=强制下载, 0=浏览器内嵌预览"),
    user: User = Depends(get_current_user),
):
    """流式返回进门研报 PDF.

    优先走本地缓存 (pdf_local_path); 不存在时用 scraper 鉴权头回源拉一次,
    写盘 + 回填 Mongo. 源链不是 PDF (hzinsights HTML) 时保持 404.
    """
    try:
        _id: Any = int(report_id)
    except ValueError:
        _id = report_id
    coll_name = "reports"
    doc = await _db()[coll_name].find_one(
        {"_id": _id},
        projection={
            "pdf_local_path": 1, "pdf_size_bytes": 1,
            "pdf_download_error": 1, "title": 1, "report_id": 1,
            "release_time_ms": 1, "original_url": 1,
            "detail_result.homeOssPdfUrl": 1, "detail_result.originalUrl": 1,
        },
    )
    if not doc:
        raise HTTPException(404, "Report not found")

    # 先查 GridFS, miss 时才走 _ensure_local_pdf 源头回源
    from ..services.pdf_storage import stream_pdf_or_file, _filename_for_pdf
    settings = get_settings()
    rel_from_doc = doc.get("pdf_local_path")
    if rel_from_doc:
        gfs_name = _filename_for_pdf(rel_from_doc, settings.jinmen_pdf_dir)
        existing = await _db()["fs.files"].find_one(
            {"filename": gfs_name}, projection={"_id": 1})
        if existing:
            title = (doc.get("title") or f"jinmen-{report_id}")[:120]
            return await stream_pdf_or_file(
                db=_db(),
                pdf_rel_path=rel_from_doc,
                pdf_root=settings.jinmen_pdf_dir,
                download_filename=title,
                download=bool(download),
            )
    # GridFS miss → 维持原路径: 本地缓存 + 源头回源 + 本地流
    target = await _ensure_local_pdf(coll_name, doc)
    title = (doc.get("title") or target.stem)[:120]
    return await stream_pdf_or_file(
        db=_db(),
        pdf_rel_path=str(target),
        pdf_root=settings.jinmen_pdf_dir,
        download_filename=title,
        download=bool(download),
    )


@router.get("/reports-stats")
async def get_reports_stats(user: User = Depends(get_current_user)):
    coll = _db()["reports"]
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    total = await coll.count_documents({})
    today = await coll.count_documents({"release_time": {"$regex": f"^{today_str}"}})
    with_pdf = await coll.count_documents({"pdf_size_bytes": {"$gt": 0}})
    latest = await coll.find_one({}, sort=[("release_time_ms", -1)],
                                  projection={"release_time": 1})
    # 发布机构 Top10
    pipeline = [
        {"$group": {"_id": "$organization_name", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 10},
    ]
    orgs = []
    async for row in coll.aggregate(pipeline):
        if row.get("_id"):
            orgs.append({"name": row["_id"], "count": row["count"]})
    return {
        "total": total,
        "today": today,
        "with_pdf": with_pdf,
        "without_pdf": total - with_pdf,
        "latest_release_time": latest.get("release_time") if latest else None,
        "top_organizations": orgs,
    }


# ------------------------------------------------------------------ #
# 外资研报 (oversea_reports collection) — brm.comein.cn/foreignResearch
# ------------------------------------------------------------------ #
# Schema 与 reports 几乎一致, 多了 title_cn/title_en/country_list/language_list
# 等 i18n 字段 (从 /json_oversea-research_search 灌入). 2026-04-22 前端侧栏
# 新加 /jinmen/oversea-reports 入口, 复用 JinmenReports.tsx 组件只换端点前缀.

def _oversea_report_brief(doc: dict) -> dict:
    """外资研报列表简要字段 — 保持与 _report_brief 字段签名对齐, 方便前端共用."""
    stats = doc.get("stats") or {}
    org_cn = doc.get("organization_name") or ""
    org_en = doc.get("organization_name_en") or ""
    org = org_cn or org_en
    stocks = doc.get("stocks") or []
    company_names = [s.get("name") for s in stocks if isinstance(s, dict) and s.get("name")]
    has_pdf = bool(doc.get("pdf_local_path") and doc.get("pdf_size_bytes", 0) > 0)
    return {
        "id": str(_to_py_id(doc["_id"])),
        "report_id": doc.get("report_id") or "",
        "title": doc.get("title") or "",
        "title_cn": doc.get("title_cn") or "",
        "title_en": doc.get("title_en") or "",
        "release_time": doc.get("release_time") or "",
        "release_time_ms": doc.get("release_time_ms"),
        "organization_name": org,
        "organization_name_en": org_en,
        "type_name": doc.get("report_type") or "",
        "content_tags": doc.get("language_list") or [],
        "industry_tags": doc.get("country_list") or [],
        "companies": company_names,
        "is_vip": False,
        "pdf_num": doc.get("pdf_num") or 0,
        "has_pdf": has_pdf,
        "pdf_downloadable": has_pdf or _pdf_can_be_fetched(doc),
        "pdf_size": doc.get("pdf_size_bytes") or 0,
        "summary_preview": (doc.get("summary_md") or "")[:240],
        "stats": stats,
        "web_url": doc.get("web_url") or doc.get("link_url"),
        "crawled_at": doc.get("crawled_at"),
    }


@router.get("/oversea-reports")
async def list_oversea_reports(
    q: str | None = Query(None, description="搜索标题 / 摘要 (CN+EN)"),
    organization: str | None = Query(None, description="发布机构关键词 (CN 或 EN)"),
    country: str | None = Query(None, description="国家/地区过滤"),
    ticker: str | None = Query(None, description="股票代码或名称"),
    has_pdf: bool | None = Query(None, description="仅含 PDF 已下载的"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    user: User = Depends(get_current_user),
):
    """列表 — jinmen.oversea_reports (由 scraper.py --oversea-reports 灌入)."""
    coll = _db()["oversea_reports"]
    match: dict[str, Any] = {}
    if q:
        match["$or"] = [
            {"title": {"$regex": q, "$options": "i"}},
            {"title_cn": {"$regex": q, "$options": "i"}},
            {"title_en": {"$regex": q, "$options": "i"}},
            {"summary_md": {"$regex": q, "$options": "i"}},
        ]
    if organization:
        match["$or"] = match.get("$or", []) + [
            {"organization_name": {"$regex": organization, "$options": "i"}},
            {"organization_name_en": {"$regex": organization, "$options": "i"}},
        ]
    if country:
        match["country_list"] = {"$elemMatch": {"$regex": country, "$options": "i"}}
    if ticker:
        match["$or"] = match.get("$or", []) + [
            {"stocks.code": {"$regex": ticker, "$options": "i"}},
            {"stocks.name": {"$regex": ticker, "$options": "i"}},
            {"title": {"$regex": ticker, "$options": "i"}},
        ]
    if has_pdf is True:
        match["pdf_size_bytes"] = {"$gt": 0}
    elif has_pdf is False:
        match["pdf_size_bytes"] = {"$in": [0, None]}

    total = await coll.count_documents(match)
    # 同 /reports: 平台 releaseTime 只到日期粒度, 加 _id desc 做 tie-break.
    cursor = (
        coll.find(match)
        .sort([("release_time_ms", -1), ("_id", -1)])
        .skip((page - 1) * page_size)
        .limit(page_size)
    )
    items = [_oversea_report_brief(d) async for d in cursor]
    return {
        "items": items,
        "total": total,
        "page": page,
        "page_size": page_size,
        "has_next": (page * page_size) < total,
    }


@router.get("/oversea-reports/{report_id}")
async def get_oversea_report(
    report_id: str,
    user: User = Depends(get_current_user),
):
    try:
        _id: Any = int(report_id)
    except ValueError:
        _id = report_id
    coll = _db()["oversea_reports"]
    doc = await coll.find_one({"_id": _id})
    if not doc:
        raise HTTPException(404, "Oversea report not found")
    return {
        **_oversea_report_brief(doc),
        "summary_md": doc.get("summary_md") or "",
        "summary_point_md": doc.get("summary_point_md") or "",
        "original_url": doc.get("original_url") or "",
        "link_url": doc.get("link_url") or "",
        "pdf_local_path": doc.get("pdf_local_path") or "",
        "pdf_download_error": doc.get("pdf_download_error") or "",
        "country_list": doc.get("country_list") or [],
        "language_list": doc.get("language_list") or [],
        "authors": doc.get("authors") or [],
    }


@router.get("/oversea-reports/{report_id}/pdf")
async def get_oversea_report_pdf(
    report_id: str,
    download: int = Query(0, ge=0, le=1,
                          description="1=强制下载, 0=浏览器内嵌预览"),
    user: User = Depends(get_current_user),
):
    """流式返回进门外资研报 PDF — 与 /reports/{id}/pdf 同构 (local → 回源)."""
    try:
        _id: Any = int(report_id)
    except ValueError:
        _id = report_id
    coll_name = "oversea_reports"
    doc = await _db()[coll_name].find_one(
        {"_id": _id},
        projection={
            "pdf_local_path": 1, "pdf_size_bytes": 1,
            "pdf_download_error": 1, "title": 1, "report_id": 1,
            "release_time_ms": 1, "original_url": 1,
            "detail_result.homeOssPdfUrl": 1, "detail_result.originalUrl": 1,
        },
    )
    if not doc:
        raise HTTPException(404, "Oversea report not found")

    from ..services.pdf_storage import stream_pdf_or_file, _filename_for_pdf
    settings = get_settings()
    rel_from_doc = doc.get("pdf_local_path")
    if rel_from_doc:
        gfs_name = _filename_for_pdf(rel_from_doc, settings.jinmen_pdf_dir)
        existing = await _db()["fs.files"].find_one(
            {"filename": gfs_name}, projection={"_id": 1})
        if existing:
            title = (doc.get("title") or f"jinmen-oversea-{report_id}")[:120]
            return await stream_pdf_or_file(
                db=_db(),
                pdf_rel_path=rel_from_doc,
                pdf_root=settings.jinmen_pdf_dir,
                download_filename=title,
                download=bool(download),
            )
    target = await _ensure_local_pdf(coll_name, doc)
    title = (doc.get("title") or target.stem)[:120]
    return await stream_pdf_or_file(
        db=_db(),
        pdf_rel_path=str(target),
        pdf_root=settings.jinmen_pdf_dir,
        download_filename=title,
        download=bool(download),
    )


@router.get("/oversea-reports-stats")
async def get_oversea_reports_stats(user: User = Depends(get_current_user)):
    coll = _db()["oversea_reports"]
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    total = await coll.count_documents({})
    today = await coll.count_documents({"release_time": {"$regex": f"^{today_str}"}})
    with_pdf = await coll.count_documents({"pdf_size_bytes": {"$gt": 0}})
    latest = await coll.find_one({}, sort=[("release_time_ms", -1)],
                                  projection={"release_time": 1})
    # Top orgs (用 name_en 优先 fallback name_cn)
    pipeline = [
        {"$group": {
            "_id": {"$ifNull": ["$organization_name_en", "$organization_name"]},
            "count": {"$sum": 1},
        }},
        {"$sort": {"count": -1}}, {"$limit": 10},
    ]
    orgs = []
    async for row in coll.aggregate(pipeline):
        if row.get("_id"):
            orgs.append({"name": row["_id"], "count": row["count"]})
    return {
        "total": total,
        "today": today,
        "with_pdf": with_pdf,
        "without_pdf": total - with_pdf,
        "latest_release_time": latest.get("release_time") if latest else None,
        "top_organizations": orgs,
    }


# ------------------------------------------------------------------ #
# Stats
# ------------------------------------------------------------------ #
@router.get("/stats", response_model=StatsResponse)
async def get_stats(user: User = Depends(get_current_user)):
    coll = _db()["meetings"]
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    total = await coll.count_documents({})
    today = await coll.count_documents({"release_time": {"$regex": f"^{today_str}"}})

    latest_doc = await coll.find_one(
        {}, sort=[("release_time", -1)], projection={"release_time": 1}
    )
    latest = latest_doc.get("release_time") if latest_doc else None

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

    # Top organizations
    pipeline_orgs = [
        {"$match": {"organization": {"$ne": None}}},
        {"$group": {"_id": "$organization", "n": {"$sum": 1}}},
        {"$sort": {"n": -1}},
        {"$limit": 10},
    ]
    top_orgs = [
        {"name": d["_id"], "count": d["n"]}
        async for d in coll.aggregate(pipeline_orgs)
        if d.get("_id")
    ]

    # Top industries (industry is an array field)
    pipeline_inds = [
        {"$unwind": "$industry"},
        {"$match": {"industry": {"$ne": None, "$ne": ""}}},
        {"$group": {"_id": "$industry", "n": {"$sum": 1}}},
        {"$sort": {"n": -1}},
        {"$limit": 10},
    ]
    top_inds = [
        {"name": d["_id"], "count": d["n"]}
        async for d in coll.aggregate(pipeline_inds)
        if d.get("_id")
    ]

    # Top themes
    pipeline_themes = [
        {"$unwind": "$themes"},
        {"$match": {"themes": {"$ne": None, "$ne": ""}}},
        {"$group": {"_id": "$themes", "n": {"$sum": 1}}},
        {"$sort": {"n": -1}},
        {"$limit": 10},
    ]
    top_themes = [
        {"name": d["_id"], "count": d["n"]}
        async for d in coll.aggregate(pipeline_themes)
        if d.get("_id")
    ]

    # Content coverage — how many meetings have each content type
    coverage = {
        "points": await coll.count_documents({"points_md": {"$ne": ""}}),
        "chapters": await coll.count_documents({"chapter_summary_md": {"$ne": ""}}),
        "indicators": await coll.count_documents({"indicators_md": {"$ne": ""}}),
        "transcript": await coll.count_documents({"transcript_md": {"$ne": ""}}),
    }

    # Crawler checkpoint + daily platform stats
    state_coll = _db()["_state"]
    crawler_doc = await state_coll.find_one({"_id": "crawler"})
    crawler_state = None
    if crawler_doc:
        crawler_state = {
            "in_progress": bool(crawler_doc.get("in_progress")),
            "last_processed_at": crawler_doc.get("last_processed_at"),
            "last_run_end_at": crawler_doc.get("last_run_end_at"),
            "last_run_stats": crawler_doc.get("last_run_stats") or {},
        }
    daily = await state_coll.find_one({"_id": f"daily_{today_str}"})
    daily_platform_stats = None
    if daily:
        daily_platform_stats = {
            "total_on_platform": daily.get("total_on_platform", 0),
            "in_db": daily.get("in_db", 0),
            "not_in_db": daily.get("not_in_db", 0),
            "by_organization_top10": daily.get("by_organization_top10") or [],
            "by_industry_top10": daily.get("by_industry_top10") or [],
            "by_tag": daily.get("by_tag") or {},
        }

    return StatsResponse(
        total=total,
        today=today,
        last_7_days=last_7,
        top_organizations=top_orgs,
        top_industries=top_inds,
        top_themes=top_themes,
        latest_release_time=latest,
        crawler_state=crawler_state,
        daily_platform_stats=daily_platform_stats,
        content_coverage=coverage,
    )


# ------------------------------------------------------------------ #
# 平台信息 (Platform Info) — mirror the Jinmen SPA homepage widgets.
# Populates the "进门 · 平台信息" sidebar page with: 热搜 / 快速入口 /
# 机构热议 (推荐公众号) / 资讯 / 活动日历 / 一级行业.
#
# Each widget is backed by an upstream call with 15-min cache (see
# `services.jinmen_platform_info`). Frontend traffic doesn't hit Jinmen —
# one background refresh per widget per 15 min. Stale-serve on upstream
# error. All scoped to the authed user (not anonymous) to keep the tab
# behind login like the rest of the app.
# ------------------------------------------------------------------ #
from backend.app.services import jinmen_platform_info as _pi  # noqa: E402


@router.get("/platform-info/hot-search")
async def platform_info_hot_search(user: User = Depends(get_current_user)):
    """热搜 Top-10 — `{ isUp: -1|1, name: str }`. `isUp` 是平台显示的涨跌箭头."""
    return await _pi.get_platform_info("hot_search")


@router.get("/platform-info/search-recommend")
async def platform_info_search_recommend(user: User = Depends(get_current_user)):
    """快速入口 — 搜索推荐 5 条, `{ content, stime, etime }`."""
    return await _pi.get_platform_info("search_recommend")


@router.get("/platform-info/news-accounts")
async def platform_info_news_accounts(user: User = Depends(get_current_user)):
    """机构热议 — 推荐公众号 (研报/调研/点评类账号)."""
    return await _pi.get_platform_info("news_accounts")


@router.get("/platform-info/news-articles")
async def platform_info_news_articles(
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=50),
    user: User = Depends(get_current_user),
):
    """资讯流 — 公众号聚合的 news feed."""
    return await _pi.get_platform_info("news_articles", page=page, size=size)


@router.get("/platform-info/meeting-calendar")
async def platform_info_meeting_calendar(
    start_ms: int | None = Query(None, description="ms epoch, defaults to T-1d"),
    end_ms: int | None = Query(None, description="ms epoch, defaults to T+2d"),
    user: User = Depends(get_current_user),
):
    """活动日历 — 每日会议/研讨会数量."""
    return await _pi.get_platform_info(
        "meeting_calendar", start_ms=start_ms, end_ms=end_ms,
    )


@router.get("/platform-info/industries")
async def platform_info_industries(user: User = Depends(get_current_user)):
    """一级行业 — 总量/消费/科技/制造/金融/健康/周期/海外/..."""
    return await _pi.get_platform_info("industries")


# ---------- Content feeds via master search-by-type index ----------
# All 4 return ``data: {list, total}`` normalised in the service layer.

@router.get("/platform-info/latest-roadshow")
async def platform_info_latest_roadshow(
    size: int = Query(10, ge=1, le=30),
    user: User = Depends(get_current_user),
):
    """最新路演 (type=4) — hasAISummary/hasVideo/industries/themes/organization."""
    return await _pi.get_platform_info("latest_roadshow", size=size)


@router.get("/platform-info/latest-report")
async def platform_info_latest_report(
    size: int = Query(10, ge=1, le=30),
    user: User = Depends(get_current_user),
):
    """最新研报 (type=2) — 机构/作者/标的/PDF页数/摘要."""
    return await _pi.get_platform_info("latest_report", size=size)


@router.get("/platform-info/latest-summary")
async def platform_info_latest_summary(
    size: int = Query(10, ge=1, le=30),
    user: User = Depends(get_current_user),
):
    """最新纪要 (type=13) — 首席分析师/行业/主题/关联股票/contentType."""
    return await _pi.get_platform_info("latest_summary", size=size)


@router.get("/platform-info/latest-comment")
async def platform_info_latest_comment(
    size: int = Query(10, ge=1, le=30),
    user: User = Depends(get_current_user),
):
    """最新点评 (type=11) — 股票/行业/机构/摘要."""
    return await _pi.get_platform_info("latest_comment", size=size)


# ---------- Aggregate: all 10 widgets in one parallel fetch ---------- #
# Frontend's JinmenPlatformInfo previously fired 10 independent HTTP
# requests on mount (one per widget). Each went through axios + auth +
# router overhead for ~30-60 ms even when every widget was warm in
# Redis, so first-paint lag was 300-600 ms just in RTT amplification.
#
# This single endpoint fans out in-process via asyncio.gather, so all
# widgets return in max(per-widget-latency) instead of sum. Cold-path
# (full cache miss) is bounded by the slowest upstream (~3-5 s), warm
# path (all cached) is ~20 ms.
@router.get("/platform-info/summary")
async def platform_info_summary(
    feed_size: int = Query(10, ge=1, le=30),
    news_size: int = Query(20, ge=1, le=50),
    user: User = Depends(get_current_user),
):
    """One call → all Jinmen platform-info widgets in parallel.

    Shape:
        {
          hot_search:        Envelope<HotItem[]>,
          search_recommend:  Envelope<RecItem[]>,
          news_accounts:     Envelope<NewsAccount[]>,
          meeting_calendar:  Envelope<CalendarDay[]>,
          industries:        Envelope<Industry[]>,
          news_articles:     Envelope<NewsList>,
          latest_summary:    Envelope<FeedList>,
          latest_report:     Envelope<FeedList>,
          latest_roadshow:   Envelope<FeedList>,
          latest_comment:    Envelope<FeedList>,
        }

    Each Envelope keeps the same ``{ok, data, fetched_at, stale, msg}``
    contract as the single-widget endpoints, so the frontend can fall
    back to per-widget calls if this one fails.
    """
    import asyncio
    keys_and_kwargs: list[tuple[str, dict]] = [
        ("hot_search", {}),
        ("search_recommend", {}),
        ("news_accounts", {}),
        ("meeting_calendar", {}),
        ("industries", {}),
        ("news_articles", {"page": 1, "size": news_size}),
        ("latest_summary", {"size": feed_size}),
        ("latest_report", {"size": feed_size}),
        ("latest_roadshow", {"size": feed_size}),
        ("latest_comment", {"size": feed_size}),
    ]
    results = await asyncio.gather(
        *[_pi.get_platform_info(k, **kw) for k, kw in keys_and_kwargs],
        return_exceptions=True,
    )
    out: dict = {}
    for (key, _kw), res in zip(keys_and_kwargs, results):
        if isinstance(res, Exception):
            out[key] = {"ok": False, "data": None, "msg": str(res)[:200]}
        else:
            out[key] = res
    return out
