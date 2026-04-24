"""Per-stock research hub — aggregate every piece of content tied to one
canonical ticker into a single categorized feed.

This is the backend for the "click a holding → open new tab with everything"
feature on the portfolio dashboard.

Categories (user-facing labels):
    research   研报          — analyst reports, earnings filings
    commentary 点评          — daily comments, chief opinions, article notes
    minutes    会议纪要       — roadshow / meeting / earnings-call transcripts
    interview  专家访谈       — expert interviews, forum Q&A
    breaking   突发新闻       — real-time news items (Postgres + alphaengine news)

Endpoint
--------
``GET /api/stock-hub/{canonical_id}``
    Fan-out across ~21 MongoDB collections (by `_canonical_tickers`) + Postgres
    `news_items` join (by `analysis_results.affected_tickers` LIKE).

    Returns per-category counts + the requested slice of items, sorted by
    release_time desc. Supports category filter + cursor-based pagination
    via `before_ms`.
"""
from __future__ import annotations

import asyncio
import logging
from functools import lru_cache
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel
from sqlalchemy import String, desc, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.config import Settings, get_settings
from backend.app.deps import get_current_user, get_db
from backend.app.models.news import AnalysisResult, NewsItem
from backend.app.models.user import User

logger = logging.getLogger(__name__)
router = APIRouter()


# Categories — label ordering matches the UI chip order
CATEGORY_ORDER = ["research", "commentary", "minutes", "interview", "breaking"]
CATEGORY_LABELS = {
    "research": "研报",
    "commentary": "点评",
    "minutes": "会议纪要",
    "interview": "专家访谈",
    "breaking": "突发新闻",
}


# --------------------------------------------------------------------------- #
# Source registry — one row per (platform, collection) tagged with a category.
# --------------------------------------------------------------------------- #
class _Source:
    def __init__(
        self,
        *,
        source: str,
        db_attr: str,
        uri_attr: str,
        collection: str,
        category: str,
        time_field: str,
        time_ms_field: str | None,
        url_field: str | None,
        preview_field: str | None,
        title_field: str = "title",
        org_field: str | None = None,
        pdf_route: str | None = None,
        source_label: str,
    ) -> None:
        self.source = source
        self.db_attr = db_attr
        self.uri_attr = uri_attr
        self.collection = collection
        self.category = category
        self.time_field = time_field
        self.time_ms_field = time_ms_field
        self.url_field = url_field
        self.preview_field = preview_field
        self.title_field = title_field
        self.org_field = org_field
        self.pdf_route = pdf_route  # Backend URL template — callers prefix /api
        self.source_label = source_label


SOURCES: list[_Source] = [
    # ── AlphaPai ───────────────────────────────────────────────────────────
    _Source(
        source="alphapai", db_attr="alphapai_mongo_db", uri_attr="alphapai_mongo_uri",
        collection="reports", category="research",
        time_field="publish_time", time_ms_field="release_time_ms",
        url_field="web_url", preview_field="list_item.content",
        org_field="institution",
        pdf_route="/api/alphapai-db/reports/{id}/pdf",
        source_label="AlphaPai · 研报",
    ),
    _Source(
        source="alphapai", db_attr="alphapai_mongo_db", uri_attr="alphapai_mongo_uri",
        collection="comments", category="commentary",
        time_field="publish_time", time_ms_field="release_time_ms",
        url_field="web_url", preview_field="list_item.content",
        source_label="AlphaPai · 点评",
    ),
    _Source(
        source="alphapai", db_attr="alphapai_mongo_db", uri_attr="alphapai_mongo_uri",
        collection="roadshows", category="minutes",
        time_field="publish_time", time_ms_field="release_time_ms",
        url_field="web_url", preview_field="list_item.content",
        source_label="AlphaPai · 路演纪要",
    ),
    _Source(
        source="alphapai", db_attr="alphapai_mongo_db", uri_attr="alphapai_mongo_uri",
        collection="wechat_articles", category="commentary",
        time_field="publish_time", time_ms_field="release_time_ms",
        url_field="web_url", preview_field="list_item.content",
        source_label="AlphaPai · 微信研究",
    ),
    # ── Jinmen ─────────────────────────────────────────────────────────────
    _Source(
        source="jinmen", db_attr="jinmen_mongo_db", uri_attr="jinmen_mongo_uri",
        collection="reports", category="research",
        time_field="release_time", time_ms_field="release_time_ms",
        url_field=None, preview_field=None,
        org_field="organization",
        pdf_route="/api/jinmen-db/reports/{id}/pdf",
        source_label="进门 · A股研报",
    ),
    _Source(
        source="jinmen", db_attr="jinmen_mongo_db", uri_attr="jinmen_mongo_uri",
        collection="oversea_reports", category="research",
        time_field="release_time", time_ms_field="release_time_ms",
        url_field=None, preview_field=None,
        org_field="organization",
        pdf_route="/api/jinmen-db/oversea_reports/{id}/pdf",
        source_label="进门 · 海外研报",
    ),
    _Source(
        source="jinmen", db_attr="jinmen_mongo_db", uri_attr="jinmen_mongo_uri",
        collection="meetings", category="minutes",
        time_field="release_time", time_ms_field="release_time_ms",
        url_field="present_url", preview_field="points_md",
        org_field="organization",
        source_label="进门 · 会议纪要",
    ),
    # ── Gangtise ───────────────────────────────────────────────────────────
    _Source(
        source="gangtise", db_attr="gangtise_mongo_db", uri_attr="gangtise_mongo_uri",
        collection="researches", category="research",
        time_field="release_time", time_ms_field="release_time_ms",
        url_field="web_url", preview_field="brief_md",
        org_field="organization",
        pdf_route="/api/gangtise-db/researches/{id}/pdf",
        source_label="港推 · 研报",
    ),
    _Source(
        source="gangtise", db_attr="gangtise_mongo_db", uri_attr="gangtise_mongo_uri",
        collection="summaries", category="minutes",
        time_field="release_time", time_ms_field="release_time_ms",
        url_field="web_url", preview_field="content_md",
        org_field="organization",
        pdf_route="/api/gangtise-db/summaries/{id}/pdf",
        source_label="港推 · 会议纪要",
    ),
    _Source(
        source="gangtise", db_attr="gangtise_mongo_db", uri_attr="gangtise_mongo_uri",
        collection="chief_opinions", category="commentary",
        time_field="release_time", time_ms_field="release_time_ms",
        url_field="web_url", preview_field="description_md",
        org_field="organization",
        source_label="港推 · 首席观点",
    ),
    # ── Funda (US) ─────────────────────────────────────────────────────────
    _Source(
        source="funda", db_attr="funda_mongo_db", uri_attr="funda_mongo_uri",
        collection="posts", category="research",
        time_field="release_time", time_ms_field="release_time_ms",
        url_field="web_url", preview_field="content_md",
        org_field="author",
        source_label="Funda · 独立研究",
    ),
    _Source(
        source="funda", db_attr="funda_mongo_db", uri_attr="funda_mongo_uri",
        collection="earnings_reports", category="research",
        time_field="release_time", time_ms_field="release_time_ms",
        url_field="web_url", preview_field="content_md",
        source_label="Funda · 财报 (8-K)",
    ),
    _Source(
        source="funda", db_attr="funda_mongo_db", uri_attr="funda_mongo_uri",
        collection="earnings_transcripts", category="minutes",
        time_field="release_time", time_ms_field="release_time_ms",
        url_field="web_url", preview_field="content_md",
        source_label="Funda · 业绩会实录",
    ),
    # ── AlphaEngine ────────────────────────────────────────────────────────
    _Source(
        source="alphaengine", db_attr="alphaengine_mongo_db", uri_attr="alphaengine_mongo_uri",
        collection="china_reports", category="research",
        time_field="release_time", time_ms_field="release_time_ms",
        url_field="web_url", preview_field="doc_introduce",
        org_field="organization",
        pdf_route="/api/alphaengine-db/china_reports/{id}/pdf",
        source_label="AlphaEngine · 国内研报",
    ),
    _Source(
        source="alphaengine", db_attr="alphaengine_mongo_db", uri_attr="alphaengine_mongo_uri",
        collection="foreign_reports", category="research",
        time_field="release_time", time_ms_field="release_time_ms",
        url_field="web_url", preview_field="doc_introduce",
        org_field="organization",
        pdf_route="/api/alphaengine-db/foreign_reports/{id}/pdf",
        source_label="AlphaEngine · 海外研报",
    ),
    _Source(
        source="alphaengine", db_attr="alphaengine_mongo_db", uri_attr="alphaengine_mongo_uri",
        collection="summaries", category="minutes",
        time_field="release_time", time_ms_field="release_time_ms",
        url_field="web_url", preview_field="doc_introduce",
        org_field="organization",
        source_label="AlphaEngine · 会议纪要",
    ),
    _Source(
        source="alphaengine", db_attr="alphaengine_mongo_db", uri_attr="alphaengine_mongo_uri",
        collection="news_items", category="breaking",
        time_field="release_time", time_ms_field="release_time_ms",
        url_field="web_url", preview_field="doc_introduce",
        org_field="organization",
        source_label="AlphaEngine · 资讯",
    ),
    # ── AceCamp ────────────────────────────────────────────────────────────
    _Source(
        source="acecamp", db_attr="acecamp_mongo_db", uri_attr="acecamp_mongo_uri",
        collection="articles", category="commentary",
        time_field="release_time", time_ms_field="release_time_ms",
        url_field="web_url", preview_field="content_md",
        source_label="本营 · 观点/纪要",
    ),
    # ── Meritco (Jiuqian) ──────────────────────────────────────────────────
    _Source(
        source="meritco", db_attr="meritco_mongo_db", uri_attr="meritco_mongo_uri",
        collection="forum", category="interview",
        time_field="release_time", time_ms_field="release_time_ms",
        url_field=None, preview_field="summary_md",
        pdf_route="/api/meritco-db/forum/{id}/pdf",
        source_label="久谦中台 · 专家论坛",
    ),
    # ── Third Bridge ───────────────────────────────────────────────────────
    _Source(
        source="thirdbridge", db_attr="thirdbridge_mongo_db", uri_attr="thirdbridge_mongo_uri",
        collection="interviews", category="interview",
        time_field="release_time", time_ms_field="release_time_ms",
        url_field=None, preview_field="agenda_md",
        source_label="高临 · 专家访谈",
    ),
]


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
@lru_cache(maxsize=8)
def _client(uri: str) -> AsyncIOMotorClient:
    # Bounded timeouts so a dead Mongo node doesn't hang the request.
    return AsyncIOMotorClient(
        uri, tz_aware=True, serverSelectionTimeoutMS=2500, connectTimeoutMS=2500,
    )


def _preview(text: Any, limit: int = 320) -> str:
    if not isinstance(text, str):
        return ""
    s = text.strip()
    return s[:limit] + ("…" if len(s) > limit else "")


def _pick_nested(doc: dict, path: str) -> Any:
    cur: Any = doc
    for part in path.split("."):
        if isinstance(cur, dict):
            cur = cur.get(part)
        else:
            return None
    return cur


def _extract_org(spec: _Source, doc: dict) -> str:
    """Resolve institution/author into a short string for the UI."""
    if not spec.org_field:
        return ""
    v = doc.get(spec.org_field)
    if isinstance(v, str):
        return v[:80]
    if isinstance(v, list) and v:
        first = v[0]
        if isinstance(first, dict):
            return str(first.get("name") or first.get("code") or "")[:80]
        return str(first)[:80]
    if isinstance(v, dict):
        return str(v.get("name") or "")[:80]
    return ""


# --------------------------------------------------------------------------- #
# Mongo fan-out
# --------------------------------------------------------------------------- #
async def _query_spec(
    spec: _Source,
    settings: Settings,
    canonical_id: str,
    *,
    before_ms: int | None,
    limit: int,
) -> tuple[int, list[dict]]:
    """Return (total_count, slice) for one collection."""
    uri = getattr(settings, spec.uri_attr)
    db_name = getattr(settings, spec.db_attr)
    if not uri or not db_name:
        return 0, []
    coll = _client(uri)[db_name][spec.collection]

    base_match: dict[str, Any] = {"_canonical_tickers": canonical_id}

    # Count (fast with the canonical-ticker index)
    try:
        total = await coll.count_documents(base_match, maxTimeMS=4000)
    except Exception as e:
        logger.warning("stock_hub count %s.%s failed: %s", db_name, spec.collection, e)
        return 0, []

    if total == 0:
        return 0, []

    # Pagination: if caller passed before_ms and the collection has a *_ms
    # field, use it as a strict less-than; otherwise fall back to skip-less
    # pagination sorted by string time_field desc.
    match = dict(base_match)
    sort_field = spec.time_ms_field or spec.time_field
    if before_ms is not None and spec.time_ms_field:
        match[spec.time_ms_field] = {"$lt": before_ms}

    projection: dict[str, Any] = {
        spec.title_field: 1,
        spec.time_field: 1,
        "_canonical_tickers": 1,
        "_id": 1,
    }
    if spec.time_ms_field:
        projection[spec.time_ms_field] = 1
    if spec.url_field:
        projection[spec.url_field] = 1
    if spec.org_field:
        projection[spec.org_field] = 1
    # Preview projection supports nested dotted paths; drop the $substr
    # optimization because Mongo's $project on dotted paths with $substr
    # requires aggregation. We'll cap in Python post-fetch.
    if spec.preview_field:
        projection[spec.preview_field.split(".")[0]] = 1

    # pdf indicators (cheap)
    projection["pdf_size"] = 1
    projection["pdf_size_bytes"] = 1
    projection["has_pdf"] = 1

    cursor = (
        coll.find(match, projection=projection).sort(sort_field, -1).limit(limit)
    )

    items: list[dict] = []
    try:
        async for doc in cursor:
            title = doc.get(spec.title_field) or ""
            url = doc.get(spec.url_field) if spec.url_field else None
            # Third-bridge URL reconstruction
            if spec.source == "thirdbridge" and not url:
                url = f"https://forum.thirdbridge.com/zh/interview/{doc.get('_id')}"
            preview = ""
            if spec.preview_field:
                preview = _preview(_pick_nested(doc, spec.preview_field))
            pdf_url = None
            if spec.pdf_route and (
                doc.get("pdf_size") or doc.get("pdf_size_bytes") or doc.get("has_pdf")
            ):
                pdf_url = spec.pdf_route.format(id=str(doc.get("_id")))
            org = _extract_org(spec, doc)
            rt = doc.get(spec.time_field)
            rt_ms = doc.get(spec.time_ms_field) if spec.time_ms_field else None

            items.append(
                {
                    "id": str(doc.get("_id")),
                    "source": spec.source,
                    "source_label": spec.source_label,
                    "collection": spec.collection,
                    "category": spec.category,
                    "category_label": CATEGORY_LABELS[spec.category],
                    "title": str(title)[:400],
                    "release_time": rt,
                    "release_time_ms": rt_ms,
                    "url": url,
                    "pdf_url": pdf_url,
                    "preview": preview,
                    "organization": org,
                    "tickers": doc.get("_canonical_tickers") or [],
                }
            )
    except Exception as e:
        logger.warning("stock_hub fetch %s.%s failed: %s", db_name, spec.collection, e)

    return total, items


# --------------------------------------------------------------------------- #
# Postgres breaking news join (uses the same ILIKE pattern as news.py)
# --------------------------------------------------------------------------- #
def _escape_like(s: str) -> str:
    return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


async def _query_breaking_news(
    db: AsyncSession,
    canonical_id: str,
    stock_name: str | None,
    *,
    before_ms: int | None,
    limit: int,
) -> tuple[int, list[dict]]:
    """Breaking news from Postgres news_items + analysis_results.

    Matching strategy: `affected_tickers` is a JSONB list whose items look like
    "英特尔(INTC)" or "天孚通信(300394.SZ)", so we cast to text and ILIKE by
    the code portion. We also fall back to title ILIKE so raw feed items
    without affected_tickers still surface.
    """
    # Extract bare code from canonical (e.g. "600519.SH" -> "600519", "AAPL.US" -> "AAPL")
    code = canonical_id.split(".")[0]
    like_patterns = [code]
    if stock_name:
        like_patterns.append(stock_name)

    ticker_match = or_(
        *[
            AnalysisResult.affected_tickers.cast(String).ilike(f"%{_escape_like(p)}%")
            for p in like_patterns
        ],
        *[NewsItem.title.ilike(f"%{_escape_like(p)}%") for p in like_patterns],
    )

    # Count first
    count_q = (
        select(NewsItem.id)
        .outerjoin(AnalysisResult, AnalysisResult.news_item_id == NewsItem.id)
        .where(ticker_match)
    )
    # Rough count via len; for large news tables this is fine (typically <50k).
    total = len((await db.execute(count_q)).all())

    if total == 0:
        return 0, []

    from datetime import datetime, timezone

    stmt = (
        select(NewsItem, AnalysisResult)
        .outerjoin(AnalysisResult, AnalysisResult.news_item_id == NewsItem.id)
        .where(ticker_match)
    )
    if before_ms is not None:
        stmt = stmt.where(
            NewsItem.published_at
            < datetime.fromtimestamp(before_ms / 1000, tz=timezone.utc)
        )
    stmt = stmt.order_by(desc(NewsItem.published_at)).limit(limit)

    rows = (await db.execute(stmt)).all()
    items: list[dict] = []
    for n, a in rows:
        pub = n.published_at
        rt_str = pub.strftime("%Y-%m-%d %H:%M") if pub else None
        rt_ms = int(pub.timestamp() * 1000) if pub else None
        summary = (a.summary if a else "") or ""
        items.append(
            {
                "id": n.id,
                "source": "newsfeed",
                "source_label": f"资讯中心 · {n.source_name}",
                "collection": "news_items",
                "category": "breaking",
                "category_label": CATEGORY_LABELS["breaking"],
                "title": (n.title or "")[:400],
                "release_time": rt_str,
                "release_time_ms": rt_ms,
                "url": n.url,
                "pdf_url": None,
                "preview": _preview(summary or (n.content or "")),
                "organization": n.source_name,
                "sentiment": (a.sentiment if a else None),
                "impact_magnitude": (a.impact_magnitude if a else None),
                "tickers": [],
            }
        )
    return total, items


# --------------------------------------------------------------------------- #
# Response schema
# --------------------------------------------------------------------------- #
class HubItem(BaseModel):
    id: str
    source: str
    source_label: str
    collection: str
    category: str
    category_label: str
    title: str
    release_time: str | None = None
    release_time_ms: int | None = None
    url: str | None = None
    pdf_url: str | None = None
    preview: str = ""
    organization: str = ""
    sentiment: str | None = None
    impact_magnitude: str | None = None
    tickers: list[str] = []


class HubResponse(BaseModel):
    canonical_id: str
    stock_name: str | None = None
    by_category: dict[str, int]
    by_source: dict[str, int]
    total: int
    items: list[HubItem]
    next_before_ms: int | None = None


# --------------------------------------------------------------------------- #
# Endpoint
# --------------------------------------------------------------------------- #
@router.get("/{canonical_id}", response_model=HubResponse)
async def stock_hub(
    canonical_id: str,
    category: str | None = Query(
        None,
        description="Filter by category. Omit for all. One of: research, commentary, minutes, interview, breaking",
    ),
    limit: int = Query(80, ge=1, le=300),
    before_ms: int | None = Query(
        None, description="Cursor: only return items with release_time_ms < this value"
    ),
    stock_name: str | None = Query(
        None,
        description="Optional display name — improves breaking-news recall when titles don't contain the ticker",
    ),
    user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Per-stock aggregated content across 8 crawler platforms + Postgres newsfeed."""
    canonical_id = canonical_id.strip().upper().replace(" ", "")
    if "." not in canonical_id or len(canonical_id) > 32:
        raise HTTPException(
            400, "canonical_id must be CODE.MARKET, e.g. AAPL.US or 600519.SH"
        )
    if category and category not in CATEGORY_LABELS:
        raise HTTPException(400, f"Unknown category '{category}'")

    settings = get_settings()
    # Per-source slice size: when filtering we pull `limit`; when aggregating all
    # categories we pull ~limit/4 per source so 21+ sources stay cheap.
    if category:
        per_src = limit
        active_specs = [s for s in SOURCES if s.category == category]
    else:
        per_src = max(10, limit // 4)
        active_specs = list(SOURCES)

    mongo_task = asyncio.gather(
        *(
            _query_spec(spec, settings, canonical_id, before_ms=before_ms, limit=per_src)
            for spec in active_specs
        ),
        return_exceptions=True,
    )

    breaking_task: Any = None
    if category in (None, "breaking"):
        breaking_task = asyncio.create_task(
            _query_breaking_news(
                db, canonical_id, stock_name, before_ms=before_ms, limit=per_src
            )
        )

    mongo_results = await mongo_task

    by_category: dict[str, int] = {k: 0 for k in CATEGORY_ORDER}
    by_source: dict[str, int] = {}
    items: list[dict] = []
    for spec, res in zip(active_specs, mongo_results):
        if isinstance(res, Exception):
            logger.warning("stock_hub: %s.%s errored: %s", spec.source, spec.collection, res)
            continue
        total, slice_ = res
        by_category[spec.category] = by_category.get(spec.category, 0) + total
        by_source[spec.source] = by_source.get(spec.source, 0) + total
        items.extend(slice_)

    if breaking_task is not None:
        try:
            bk_total, bk_items = await breaking_task
        except Exception as e:
            logger.warning("stock_hub: breaking news query failed: %s", e)
            bk_total, bk_items = 0, []
        by_category["breaking"] = by_category.get("breaking", 0) + bk_total
        by_source["newsfeed"] = by_source.get("newsfeed", 0) + bk_total
        items.extend(bk_items)

    # Sort by release_time_ms desc (fall back to release_time string)
    def _sort_key(x: dict) -> tuple[int, str]:
        ms = x.get("release_time_ms") or 0
        return (-int(ms or 0), x.get("release_time") or "")

    items.sort(key=_sort_key)
    clipped = items[:limit]

    next_before = None
    if len(items) > limit and clipped:
        tail = clipped[-1]
        next_before = tail.get("release_time_ms")

    # Total is "total items available for the active filter", not just
    # this page. When category is set, total = by_category[category];
    # otherwise total = sum of all categories.
    if category:
        total = by_category.get(category, 0)
    else:
        total = sum(by_category.values())

    return HubResponse(
        canonical_id=canonical_id,
        stock_name=stock_name,
        by_category=by_category,
        by_source=by_source,
        total=total,
        items=[HubItem(**x) for x in clipped],
        next_before_ms=next_before,
    )


# --------------------------------------------------------------------------- #
# Tiny utility endpoint — lets the frontend avoid re-implementing the
# canonical normalizer when the portfolio already knows `(code, market_label)`.
# --------------------------------------------------------------------------- #
class CanonicalResolve(BaseModel):
    canonical_id: str | None


@router.get("/_resolve/by-portfolio", response_model=CanonicalResolve)
async def resolve_portfolio_ticker(
    ticker: str = Query(..., description="Raw stock_ticker e.g. 'AAPL', '600519', '03690'"),
    market: str = Query(..., description="stock_market label from portfolio yaml"),
    user: User = Depends(get_current_user),
):
    from backend.app.services.ticker_normalizer import (
        _canonical_from_code_market,
        _classify_ashare,
        _pad_hk,
        normalize,
    )

    market_map = {
        "美股": "us",
        "港股": "hk",
        "主板": None,  # classify by code
        "创业板": None,
        "科创板": None,
        "韩股": "kr",
        "日股": "jp",
        "澳股": "au",
        "德股": "de",
    }
    if market in ("主板", "创业板", "科创板"):
        cls = _classify_ashare(ticker)
        if cls:
            return CanonicalResolve(canonical_id=f"{ticker}.{cls}")
    if market == "港股":
        return CanonicalResolve(canonical_id=f"{_pad_hk(ticker)}.HK")
    mk = market_map.get(market)
    if mk:
        r = _canonical_from_code_market(ticker, mk)
        if r:
            return CanonicalResolve(canonical_id=r)
    # Last-chance: run full normalizer
    r = normalize(ticker)
    return CanonicalResolve(canonical_id=(r[0] if r else None))
