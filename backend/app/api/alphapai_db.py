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
from backend.app.deps import get_current_boss_or_admin, get_current_user
from backend.app.models.user import User
from backend.app.services.ticker_tags_builder import build_ticker_tags

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


# --------------------------------------------------------------------------- #
# PDF error classification
# --------------------------------------------------------------------------- #
# After the 2026-04-23 remote-Mongo migration, ~22k of the 42k pdf_flag=True
# reports are missing pdf_local_path. The DB stores a free-form `pdf_error`
# string per doc; classify it so the frontend can show meaningful messages
# (98% are permanent permission denials — retrying does nothing).
def _classify_pdf_error(err: str | None) -> tuple[str, str]:
    """Map a raw `pdf_error` string to (kind, human_message).

    Kinds:
      - permission_denied  : 账号无访问权限 (code=10222) — permanent until
                             account upgrades; --resume re-fetch is wasted.
      - filename_too_long  : OS 文件名超长 (Errno 36) — fixable by re-running
                             with a shorter destination filename.
      - transient_network  : 临时网络错误 (timeout / chunked / broken pipe) —
                             worth retrying.
      - not_found          : 平台返回未找到资源 (404 / empty data).
      - relpath_unknown    : relpath 步骤其它未知失败.
      - download_unknown   : download 步骤其它未知失败.
      - unknown            : 未识别的错误格式.
      - none               : 没有错误记录 (orphan: pdf_flag 但既无 path 也无
                             error — 一般是迁移前老数据).
    """
    if not err:
        return "none", ""
    s = str(err)
    low = s.lower()
    # Foreign-report daily quota exhausted (tier=0/day on our account). Probed
    # 2026-04-24: passing originType=1 to reading/report/detail/pdf returns
    # the precise message "您今日外资报告-禁运期报告查看已达0篇上限" (code=810002).
    # Without originType the server falls back to the generic 10222 — that's
    # why ~21k historical docs say permission_denied when the underlying cause
    # is actually quota. Treat both kinds the same way (don't auto-retry, no
    # operator retry button) but use the precise wording when we know.
    if "code=810002" in s or "上限" in s:
        return "quota_exhausted", "今日外资研报 PDF 查看额度已用尽 (账号 tier 限制)"
    # Permission denied — the masked version of quota_exhausted, plus
    # genuine no-access cases. 2026-04-24: hasPermission=false 短路 (list 已经
    # 告诉我们没权限, 不必打 detail/pdf; scraper.enrich_report_doc 写此 marker).
    if "code=10222" in s or "无权限查看" in s or "hasPermission=false" in s:
        return "permission_denied", "当前账号无该研报 PDF 访问权限 (多为外资研报 tier 限制)"
    # OS filename too long (Errno 36)
    if "file name too long" in low or "errno 36" in low:
        return "filename_too_long", "文件名超长 (OS 拒绝写盘)"
    # Transient network conditions worth retrying
    transient_markers = (
        "chunkedencodingerror", "broken pipe", "connectionerror",
        "ssl", "eof", "timeout", "timed out", "resetbyperror",
        "connection reset", "remote disconnected",
    )
    if any(m in low for m in transient_markers):
        return "transient_network", "网络中断 / 临时下载失败"
    # 2026-04-24: empty_data_type=str 实测来自 hasPermission=false 的付费墙条目 —
    # 服务端降级返回 data="" 而不是 code=10222. 按 permission_denied 分类, 前端
    # 不展示 retry 按钮, 避免操作员白点 (retry 不会成功).
    if "empty_data" in s:
        return "permission_denied", "平台返回空 PDF 数据 (付费墙降级响应, 同 tier 限制)"
    # Server-side 404 from the relpath endpoint
    if "code=404" in s or "code=400404" in s:
        return "not_found", "平台未提供该研报的 PDF"
    # Step-typed fallbacks
    if s.startswith("relpath_err"):
        return "relpath_unknown", "获取 PDF 路径失败"
    if s.startswith("download_err"):
        return "download_unknown", "下载 PDF 失败"
    return "unknown", s[:120]


# Kinds that the on-demand retry endpoint should attempt. Permission_denied is
# explicitly excluded — retrying just burns API calls and may attract bot
# detection.
_PDF_ERROR_RETRYABLE: set[str] = {
    "filename_too_long", "transient_network", "relpath_unknown",
    "download_unknown", "none", "unknown",
}


def _pdf_status(doc: dict) -> dict:
    """Build the front-end PDF status block for a report doc."""
    has_flag = bool(doc.get("pdf_flag"))
    has_path = bool(doc.get("pdf_local_path"))
    err = doc.get("pdf_error") or ""
    kind, human = _classify_pdf_error(err)
    # If we already have a path, the error (if any) is stale — clear the
    # categorization so the UI doesn't show a confusing "failed" tag for a
    # PDF the user can in fact open.
    if has_path:
        kind, human, err = "none", "", ""
    return {
        "pdf_flag": has_flag,
        "pdf_available": has_flag and has_path,
        "pdf_error": err,
        "pdf_error_kind": kind,
        "pdf_error_human": human,
        "pdf_retryable": kind in _PDF_ERROR_RETRYABLE and has_flag and not has_path,
    }


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
        # Categorized PDF status — replaces the plain has_pdf so the UI can
        # tell "PDF available" vs "no permission" vs "transient failure".
        item.update(_pdf_status(doc))
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

    # Visibility gates. Mirrors stock_hub's per-stock feed.
    #   deleted=True — thin-clip tombstones (cleanup_alphapai_thin_clips.py).
    #   content_truncated=True — detail RPC hit the daily 400000 quota and
    #     only the ~136-220 char list-card preview made it into `content`.
    #     For roadshows (which never have a PDF) this means there's literally
    #     no body to show; suppress until retry_truncated_roadshows refills
    #     it next-day. For reports the PDF often DID download (16k of 17k
    #     truncated reports have pdf_text_md), so we only suppress the rare
    #     truncated-AND-no-PDF case — otherwise we'd hide 16k useful reports.
    match: dict[str, Any] = {"deleted": {"$ne": True}}
    if category == "roadshow":
        match["content_truncated"] = {"$ne": True}
    elif category == "report":
        match["$and"] = [{
            "$or": [
                {"content_truncated": {"$ne": True}},
                {"pdf_text_md": {"$nin": [None, ""]}},
                {"pdf_local_path": {"$nin": [None, ""]}},
            ]
        }]
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
    # Strip raw detail/list_item payload to keep response light; expose content.
    # _normalize_item already merges the categorized pdf_status block for
    # report docs; pdf_local_path / pdf_size stay for backward compat with the
    # frontend's "查看 PDF" button gate (which historically read pdf_local_path).
    # Also surface foreign-broker bilingual fields (titleCn / contentCn) when
    # present — these come from list_item and let the UI show the same Chinese
    # translation the AlphaPai SPA renders, which is the only viable substitute
    # when the original PDF is locked behind tier quota.
    li = doc.get("list_item") or {}
    title_cn = li.get("titleCn") if isinstance(li, dict) else None
    content_cn = li.get("contentCn") if isinstance(li, dict) else None
    return {
        **_normalize_item(doc),
        "content": doc.get("content") or "",
        "pdf_text_md": doc.get("pdf_text_md") or "",
        "title_cn": title_cn or None,
        "content_cn": content_cn or None,
        "pdf_local_path": doc.get("pdf_local_path"),
        "pdf_size": doc.get("pdf_size"),
        "raw_id": doc.get("raw_id"),
        "ticker_tags": build_ticker_tags(doc, "alphapai", CATEGORY_COLLECTION[category]),
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

    # 2026-04-27: 仅本地 SSD 读取 (GridFS fallback 已弃用, fs.files 已 drop).
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
# On-demand PDF retry (admin/boss only)
# ------------------------------------------------------------------ #
# After the 2026-04-23 remote-Mongo migration, ~370 reports have transient
# pdf_error states (broken pipe / timeout / filename-too-long / orphaned). The
# scraper retries them on `--resume` but only if they still appear in the
# (recent-pages) list — older docs are stuck. This endpoint lets an operator
# force a single-shot re-fetch from the AlphaPai detail+storage APIs without
# spinning up the whole crawler. Permission_denied errors (98% of failures)
# are explicitly refused since they can't be cured by retrying.
@router.post("/items/report/{item_id}/pdf-retry")
async def retry_report_pdf(
    item_id: str,
    user: User = Depends(get_current_boss_or_admin),
):
    db = _mongo_db()
    doc = await db["reports"].find_one(
        {"_id": item_id},
        projection={
            "pdf_local_path": 1, "pdf_size": 1, "pdf_error": 1,
            "pdf_flag": 1, "title": 1, "publish_time": 1, "raw_id": 1,
            "list_item": 1,
        },
    )
    if not doc:
        raise HTTPException(404, "Report not found")
    if not doc.get("pdf_flag"):
        raise HTTPException(400, "This report has no PDF on the platform")

    kind, _ = _classify_pdf_error(doc.get("pdf_error") or "")
    if kind in ("permission_denied", "quota_exhausted"):
        # Don't burn an API call (and risk the scraper account's day-quota)
        # on something we know will fail. quota_exhausted resets nightly but
        # since our tier is 0/day for foreign reports, retrying still fails.
        raise HTTPException(
            409,
            "PDF 当前账号无访问权限 (code=10222 / 810002 — 多为外资研报 tier 限制), "
            "重试不会成功. 文本内容已抓取并展示, 可直接阅读 (与 AlphaPai SPA 同步).",
        )
    if doc.get("pdf_local_path"):
        # Nothing to do — viewer can already serve it. Refresh the response so
        # the caller's UI flips to the success state.
        return {
            "ok": True,
            "skipped": True,
            "reason": "pdf_local_path already set",
            **_pdf_status(doc),
        }

    raw_id = doc.get("raw_id")
    if not raw_id:
        # Fall back to list_item.id if raw_id wasn't captured for older docs.
        li = doc.get("list_item") or {}
        raw_id = li.get("id") if isinstance(li, dict) else None
    if not raw_id:
        raise HTTPException(400, "Doc missing raw_id, can't query AlphaPai detail API")

    settings = get_settings()
    result = await _retry_pdf_download(
        db=db,
        item_id=item_id,
        raw_id=str(raw_id),
        publish_time=doc.get("publish_time") or "",
        title=doc.get("title") or "",
        pdf_root=settings.alphapai_pdf_dir,
    )
    return result


@lru_cache(maxsize=1)
def _load_scraper_module():
    """Lazy-load the alphapai scraper module (file-based, since crawl/ isn't a package).

    Cached so repeated retry calls don't re-exec module-level code (antibot
    throttle init, jieba load, etc).
    """
    import importlib.util
    scraper_path = Path("/home/ygwang/trading_agent_staging/crawl/alphapai_crawl/scraper.py")
    spec = importlib.util.spec_from_file_location("alphapai_scraper_retry", scraper_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("can't load scraper.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


async def _retry_pdf_download(
    *, db: AsyncIOMotorDatabase, item_id: str, raw_id: str,
    publish_time: str, title: str, pdf_root: str,
) -> dict:
    """Single-shot re-fetch: detail/pdf → storage download → local SSD.

    GridFS mirror retired 2026-04-27 — disk is the only source of truth.

    Runs in a thread executor because the underlying crawl helpers are
    sync `requests` calls. Uses the same credentials.json the scraper does.
    """
    import asyncio
    loop = asyncio.get_event_loop()

    def _do_work() -> dict:
        mod = _load_scraper_module()
        token = mod._load_token_from_file()
        if not token:
            raise RuntimeError("alphapai credentials.json has no token; auto-login first")
        session = mod.create_session(token)

        relpath, err = mod.fetch_report_pdf_relpath(session, raw_id, version=1)
        if err or not relpath:
            return {"ok": False, "stage": "relpath", "error": err or "no relpath"}

        dest = mod._pdf_dest_path(Path(pdf_root), publish_time, relpath, title)
        # Some failures (e.g. SMB read_back_err) leave the file actually on
        # disk but with stale empty pdf_local_path in Mongo. Detect and adopt
        # without re-downloading.
        if dest.exists() and dest.stat().st_size > 0:
            with open(dest, "rb") as fh:
                head = fh.read(4)
            if head == b"%PDF":
                size = dest.stat().st_size
                derr = None
                logger.info("alphapai pdf-retry: adopt existing file %s (%dB)",
                            dest, size)
            else:
                # Bad magic — file's broken, blow it away and re-download.
                try: dest.unlink()
                except Exception: pass
                size, derr = mod.download_report_pdf(session, relpath, token, dest)
        else:
            size, derr = mod.download_report_pdf(session, relpath, token, dest)
        if derr:
            return {"ok": False, "stage": "download", "error": derr,
                    "relpath": relpath}

        # 本地 SSD 是 PDF 的唯一 source of truth (GridFS 已于 2026-04-27 弃用).
        # stream_pdf_or_file 只读磁盘, 所以这里写盘成功即可, 不再镜像到 GridFS.
        from pymongo import MongoClient
        settings = get_settings()
        cli = MongoClient(settings.alphapai_mongo_uri,
                          serverSelectionTimeoutMS=15000,
                          socketTimeoutMS=300000)
        try:
            sync_db = cli[settings.alphapai_mongo_db]
            sync_db["reports"].update_one(
                {"_id": item_id},
                {"$set": {
                    "pdf_local_path": str(dest),
                    "pdf_size": size,
                    "pdf_error": "",
                }},
            )
        finally:
            cli.close()

        return {"ok": True, "stage": "done", "size": size,
                "pdf_local_path": str(dest)}

    try:
        result = await loop.run_in_executor(None, _do_work)
    except Exception as exc:
        logger.exception("alphapai PDF retry failed for %s", item_id)
        raise HTTPException(500, f"retry failed: {exc}")

    if not result.get("ok"):
        # Surface the underlying error to the operator instead of wrapping it
        # in a generic 500. 422 reads as "we tried, here's what broke".
        raise HTTPException(
            422,
            f"retry failed at {result.get('stage')}: {result.get('error')}",
        )
    return result


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
