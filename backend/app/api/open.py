"""Open API for external agents (e.g. OpenClaw).

Provides stock-based search and detail retrieval across all data sources.
Authenticated via X-API-Key header instead of JWT.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import String, cast, select, desc, or_
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.config import get_settings
from backend.app.deps import get_db, verify_api_key
from backend.app.models.alphapai import (
    AlphaPaiArticle,
    AlphaPaiComment,
    AlphaPaiRoadshowCN,
    AlphaPaiRoadshowUS,
)
from backend.app.models.jiuqian import JiuqianForum, JiuqianMinutes, JiuqianWechat
from backend.app.models.news import AnalysisResult, FilterResult, NewsItem, ResearchReport
from backend.app.schemas.open import (
    AnalysisBrief,
    DetailResponse,
    KbCollectionInfo,
    KbFacetsRequest,
    KbFacetsResponse,
    KbFetchRequest,
    KbFetchResponse,
    KbMetaResponse,
    KbSearchRequest,
    KbSearchResponse,
    ResearchBrief,
    ResolvedStock,
    SearchItem,
    SearchResponse,
    StockSuggestion,
    SuggestResponse,
)
from backend.app.services import kb_service
from backend.app.services.stock_verifier import get_stock_verifier

logger = logging.getLogger(__name__)
router = APIRouter()


# ── Helpers ────────────────────────────────────────────────────────────


def _escape_like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _cutoff(hours: int) -> datetime:
    return datetime.now(timezone.utc) - timedelta(hours=hours)


def _base_url() -> str:
    return get_settings().open_api_base_url.rstrip("/")


def _site_url(source_type: str, item_id: str) -> str:
    """Build the frontend page URL for a given item."""
    base = _base_url()
    mapping = {
        "news": f"{base}/news/{item_id}",
        "alphapai_wechat": f"{base}/alphapai/feed?detail={item_id}",
        "alphapai_comment": f"{base}/alphapai/comments?detail={item_id}",
        "alphapai_roadshow_cn": f"{base}/alphapai/roadshows?detail={item_id}",
        "alphapai_roadshow_us": f"{base}/alphapai/roadshows?detail={item_id}&market=us",
        "jiuqian_forum": f"{base}/jiuqian/forum?detail={item_id}",
        "jiuqian_minutes": f"{base}/jiuqian/minutes?detail={item_id}",
        "jiuqian_wechat": f"{base}/jiuqian/wechat?detail={item_id}",
    }
    return mapping.get(source_type, f"{base}/")


def _detail_url(source_type: str, item_id: str) -> str:
    """Build the Open API detail endpoint URL."""
    base = _base_url()
    return f"{base}/api/open/detail/{source_type}/{item_id}"


def _build_match_conditions(search_terms: list[str], tickers_col, title_col=None):
    conditions = []
    for term in search_terms:
        safe = _escape_like(term)
        conditions.append(cast(tickers_col, String).ilike(f"%{safe}%"))
        if title_col is not None and len(term) >= 2 and not term.isascii():
            conditions.append(title_col.ilike(f"%{safe}%"))
    return conditions


def _resolve_search_terms(q: str) -> tuple[list[str], ResolvedStock | None]:
    """Expand a query into multiple search terms via StockVerifier.

    Returns (search_terms, resolved_stock_or_None).
    """
    verifier = get_stock_verifier()
    search_terms = [q]
    resolved: ResolvedStock | None = None

    result = verifier._lookup_by_code(q.upper())
    if result:
        search_terms.append(result[0])  # name
        search_terms.append(result[1])  # full code
        resolved = ResolvedStock(name=result[0], code=result[1], market=_detect_market(result[1]))
    else:
        result = verifier._lookup_by_name(q)
        if result:
            search_terms.append(result[0])
            search_terms.append(result[1])
            bare_code = result[1].split(".")[0] if "." in result[1] else result[1]
            search_terms.append(bare_code)
            resolved = ResolvedStock(name=result[0], code=result[1], market=_detect_market(result[1]))

    search_terms = list(set(t for t in search_terms if t))
    return search_terms, resolved


def _detect_market(code: str) -> str:
    if code.endswith((".SH", ".SZ", ".BJ")):
        return "A股"
    if code.endswith(".HK"):
        return "港股"
    verifier = get_stock_verifier()
    m = verifier._custom_code_to_market.get(code) or verifier._custom_code_to_market.get(code.upper())
    if m:
        return m
    return "美股"


# ── Endpoint 1: Stock suggest ──────────────────────────────────────────


@router.get("/stock/suggest", response_model=SuggestResponse)
async def suggest_stocks(
    q: str = Query(min_length=1, max_length=50),
    limit: int = Query(10, ge=1, le=30),
    _api_key=Depends(verify_api_key),
):
    """Fast autocomplete for stock name/code. Use this to confirm which stock to search."""
    q = q.strip()
    if not q:
        return SuggestResponse(suggestions=[])

    verifier = get_stock_verifier()
    if not verifier._loaded:
        verifier.load_stock_lists()

    q_upper = q.upper()
    q_lower = q.lower()
    results: list[StockSuggestion] = []
    seen: set[str] = set()

    def _add(name: str, code: str, market: str, rank: int):
        key = f"{code}|{name}"
        if key in seen:
            return
        seen.add(key)
        results.append(StockSuggestion(
            name=name, code=code, market=market,
            label=f"{name}({code})",
        ))

    # Exact code match
    result = verifier._lookup_by_code(q_upper)
    if result:
        _add(result[0], result[1], _detect_market(result[1]), 0)

    # Exact name match
    result = verifier._lookup_by_name(q)
    if result:
        _add(result[0], result[1], _detect_market(result[1]), 1)

    # Prefix/substring search across all lists
    for code, name in verifier._a_code_to_name.items():
        if "." not in code:
            continue
        if len(results) >= limit:
            break
        if code.upper().startswith(q_upper) or q in name:
            _add(name, code, "A股", 2)

    for code, name in verifier._us_code_to_name.items():
        if len(results) >= limit:
            break
        if code.upper().startswith(q_upper) or q_lower in name.lower() or q in name:
            _add(name, code, "美股", 2)

    for code, name in verifier._hk_code_to_name.items():
        if len(results) >= limit:
            break
        if code.upper().startswith(q_upper) or q in name:
            _add(name, code, "港股", 2)

    for code, name in verifier._custom_code_to_name.items():
        if len(results) >= limit:
            break
        if code.upper().startswith(q_upper) or q in name:
            _add(name, code, "其他", 2)

    return SuggestResponse(suggestions=results[:limit])


# ── Endpoint 2: Unified search ─────────────────────────────────────────


@router.get("/search", response_model=SearchResponse)
async def search(
    q: str = Query(min_length=1, max_length=100, description="Stock name or code (fuzzy)"),
    hours: int = Query(168, ge=1, le=720, description="Time window in hours (default 7 days)"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=50),
    source: str = Query("all", description="Filter: all / news / alphapai / jiuqian"),
    sentiment: str | None = Query(None, description="Filter: bullish / bearish / very_bullish / very_bearish"),
    db: AsyncSession = Depends(get_db),
    _api_key=Depends(verify_api_key),
):
    """Search across all data sources by stock name/code.

    Returns a list of items with title, summary, tickers, sentiment,
    plus URLs for detail retrieval and human viewing.
    """
    q = q.strip()
    cutoff = _cutoff(hours)
    search_terms, resolved = _resolve_search_terms(q)

    all_results: list[dict] = []
    include_news = source in ("all", "news")
    include_alphapai = source in ("all", "alphapai")
    include_jiuqian = source in ("all", "jiuqian")

    # --- AlphaPai Articles ---
    if include_alphapai:
        conds = _build_match_conditions(
            search_terms, AlphaPaiArticle.enrichment["tickers"], AlphaPaiArticle.arc_name,
        )
        stmt = (
            select(AlphaPaiArticle)
            .where(AlphaPaiArticle.publish_time >= cutoff)
            .where(AlphaPaiArticle.is_enriched == True)  # noqa: E712
            .where(or_(
                AlphaPaiArticle.enrichment["skipped"].as_boolean().is_(False),
                AlphaPaiArticle.enrichment["skipped"].is_(None),
            ))
            .where(or_(*conds))
            .order_by(desc(AlphaPaiArticle.publish_time))
            .limit(50)
        )
        for r in (await db.execute(stmt)).scalars():
            enr = r.enrichment or {}
            s = enr.get("sentiment", "")
            if sentiment and s != sentiment:
                continue
            all_results.append({
                "source_type": "alphapai_wechat",
                "source_label": "AlphaPai公众号",
                "id": r.arc_code,
                "title": r.arc_name,
                "title_zh": None,
                "summary": enr.get("summary", ""),
                "tickers": enr.get("tickers", []),
                "sectors": enr.get("sectors", []),
                "sentiment": s,
                "impact_magnitude": None,
                "concept_tags": enr.get("concept_tags", []),
                "published_at": r.publish_time.isoformat() if r.publish_time else None,
                "original_url": r.url or None,
            })

    # --- AlphaPai Comments ---
    if include_alphapai:
        conds = _build_match_conditions(
            search_terms, AlphaPaiComment.enrichment["tickers"], AlphaPaiComment.title,
        )
        stmt = (
            select(AlphaPaiComment)
            .where(AlphaPaiComment.cmnt_date >= cutoff)
            .where(AlphaPaiComment.is_enriched == True)  # noqa: E712
            .where(or_(*conds))
            .order_by(desc(AlphaPaiComment.cmnt_date))
            .limit(30)
        )
        for r in (await db.execute(stmt)).scalars():
            enr = r.enrichment or {}
            s = enr.get("sentiment", "")
            if sentiment and s != sentiment:
                continue
            all_results.append({
                "source_type": "alphapai_comment",
                "source_label": "AlphaPai券商点评",
                "id": r.cmnt_hcode,
                "title": r.title,
                "title_zh": None,
                "summary": enr.get("summary", ""),
                "tickers": enr.get("tickers", []),
                "sectors": enr.get("sectors", []),
                "sentiment": s,
                "impact_magnitude": None,
                "concept_tags": enr.get("concept_tags", []),
                "published_at": r.cmnt_date.isoformat() if r.cmnt_date else None,
                "original_url": None,
            })

    # --- AlphaPai Roadshows CN ---
    if include_alphapai:
        conds = _build_match_conditions(
            search_terms, AlphaPaiRoadshowCN.enrichment["tickers"], AlphaPaiRoadshowCN.show_title,
        )
        stmt = (
            select(AlphaPaiRoadshowCN)
            .where(AlphaPaiRoadshowCN.stime >= cutoff)
            .where(AlphaPaiRoadshowCN.is_enriched == True)  # noqa: E712
            .where(or_(*conds))
            .order_by(desc(AlphaPaiRoadshowCN.stime))
            .limit(20)
        )
        for r in (await db.execute(stmt)).scalars():
            enr = r.enrichment or {}
            s = enr.get("sentiment", "")
            if sentiment and s != sentiment:
                continue
            all_results.append({
                "source_type": "alphapai_roadshow_cn",
                "source_label": "AlphaPai路演纪要(A股)",
                "id": r.trans_id,
                "title": r.show_title,
                "title_zh": None,
                "summary": enr.get("summary", ""),
                "tickers": enr.get("tickers", []),
                "sectors": enr.get("sectors", []),
                "sentiment": s,
                "impact_magnitude": None,
                "concept_tags": enr.get("concept_tags", []),
                "published_at": r.stime.isoformat() if r.stime else None,
                "original_url": None,
            })

    # --- AlphaPai Roadshows US ---
    if include_alphapai:
        conds = _build_match_conditions(
            search_terms, AlphaPaiRoadshowUS.enrichment["tickers"], AlphaPaiRoadshowUS.show_title,
        )
        stmt = (
            select(AlphaPaiRoadshowUS)
            .where(AlphaPaiRoadshowUS.stime >= cutoff)
            .where(AlphaPaiRoadshowUS.is_enriched == True)  # noqa: E712
            .where(or_(*conds))
            .order_by(desc(AlphaPaiRoadshowUS.stime))
            .limit(20)
        )
        for r in (await db.execute(stmt)).scalars():
            enr = r.enrichment or {}
            s = enr.get("sentiment", "")
            if sentiment and s != sentiment:
                continue
            all_results.append({
                "source_type": "alphapai_roadshow_us",
                "source_label": "AlphaPai路演纪要(美股)",
                "id": r.trans_id,
                "title": r.show_title,
                "title_zh": None,
                "summary": enr.get("summary", ""),
                "tickers": enr.get("tickers", []),
                "sectors": enr.get("sectors", []),
                "sentiment": s,
                "impact_magnitude": None,
                "concept_tags": enr.get("concept_tags", []),
                "published_at": r.stime.isoformat() if r.stime else None,
                "original_url": None,
            })

    # --- Jiuqian Forum ---
    if include_jiuqian:
        conds = _build_match_conditions(
            search_terms, JiuqianForum.enrichment["tickers"], JiuqianForum.title,
        )
        stmt = (
            select(JiuqianForum)
            .where(JiuqianForum.is_enriched == True)  # noqa: E712
            .where(or_(*conds))
            .order_by(desc(JiuqianForum.meeting_time))
            .limit(20)
        )
        for r in (await db.execute(stmt)).scalars():
            enr = r.enrichment or {}
            s = enr.get("sentiment", "")
            if sentiment and s != sentiment:
                continue
            all_results.append({
                "source_type": "jiuqian_forum",
                "source_label": "久谦专家访谈",
                "id": str(r.id),
                "title": r.title,
                "title_zh": None,
                "summary": enr.get("summary", "") or r.summary or "",
                "tickers": enr.get("tickers", []),
                "sectors": enr.get("sectors", []),
                "sentiment": s,
                "impact_magnitude": None,
                "concept_tags": enr.get("concept_tags", []),
                "published_at": r.meeting_time.isoformat() if r.meeting_time else None,
                "original_url": None,
            })

    # --- Jiuqian Minutes ---
    if include_jiuqian:
        conds = _build_match_conditions(
            search_terms, JiuqianMinutes.enrichment["tickers"], JiuqianMinutes.title,
        )
        stmt = (
            select(JiuqianMinutes)
            .where(JiuqianMinutes.pub_time >= cutoff)
            .where(JiuqianMinutes.is_enriched == True)  # noqa: E712
            .where(or_(*conds))
            .order_by(desc(JiuqianMinutes.pub_time))
            .limit(20)
        )
        for r in (await db.execute(stmt)).scalars():
            enr = r.enrichment or {}
            s = enr.get("sentiment", "")
            if sentiment and s != sentiment:
                continue
            all_results.append({
                "source_type": "jiuqian_minutes",
                "source_label": "久谦研究纪要",
                "id": r.id,
                "title": r.title,
                "title_zh": None,
                "summary": enr.get("summary", "") or r.summary or "",
                "tickers": enr.get("tickers", []),
                "sectors": enr.get("sectors", []),
                "sentiment": s,
                "impact_magnitude": None,
                "concept_tags": enr.get("concept_tags", []),
                "published_at": r.pub_time.isoformat() if r.pub_time else None,
                "original_url": None,
            })

    # --- Jiuqian WeChat ---
    if include_jiuqian:
        conds = _build_match_conditions(
            search_terms, JiuqianWechat.enrichment["tickers"], JiuqianWechat.title,
        )
        stmt = (
            select(JiuqianWechat)
            .where(JiuqianWechat.pub_time >= cutoff)
            .where(JiuqianWechat.is_enriched == True)  # noqa: E712
            .where(or_(
                JiuqianWechat.enrichment["skipped"].as_boolean().is_(False),
                JiuqianWechat.enrichment["skipped"].is_(None),
            ))
            .where(or_(*conds))
            .order_by(desc(JiuqianWechat.pub_time))
            .limit(20)
        )
        for r in (await db.execute(stmt)).scalars():
            enr = r.enrichment or {}
            s = enr.get("sentiment", "")
            if sentiment and s != sentiment:
                continue
            all_results.append({
                "source_type": "jiuqian_wechat",
                "source_label": "久谦公众号",
                "id": r.id,
                "title": r.title,
                "title_zh": None,
                "summary": enr.get("summary", "") or r.summary or "",
                "tickers": enr.get("tickers", []),
                "sectors": enr.get("sectors", []),
                "sentiment": s,
                "impact_magnitude": None,
                "concept_tags": enr.get("concept_tags", []),
                "published_at": r.pub_time.isoformat() if r.pub_time else None,
                "original_url": r.post_url or None,
            })

    # --- News Center ---
    if include_news:
        conds = _build_match_conditions(
            search_terms, AnalysisResult.affected_tickers, NewsItem.title,
        )
        stmt = (
            select(NewsItem)
            .join(AnalysisResult, NewsItem.id == AnalysisResult.news_item_id)
            .join(FilterResult, NewsItem.id == FilterResult.news_item_id)
            .where(NewsItem.fetched_at >= cutoff)
            .where(FilterResult.is_relevant.is_(True))
            .where(AnalysisResult.sentiment.isnot(None))
            .where(AnalysisResult.sentiment.notin_(["neutral"]))
            .where(or_(*conds))
            .order_by(desc(NewsItem.fetched_at))
            .limit(30)
        )
        for r in (await db.execute(stmt)).scalars().unique():
            analysis = await db.scalar(
                select(AnalysisResult).where(AnalysisResult.news_item_id == r.id)
            )
            if not analysis:
                continue
            s = analysis.sentiment or ""
            if sentiment and s != sentiment:
                continue
            title_zh = (r.metadata_ or {}).get("title_zh")
            all_results.append({
                "source_type": "news",
                "source_label": "资讯中心",
                "id": r.id,
                "title": title_zh or r.title,
                "title_zh": title_zh if title_zh != r.title else None,
                "summary": analysis.summary or "",
                "tickers": analysis.affected_tickers or [],
                "sectors": analysis.affected_sectors or [],
                "sentiment": s,
                "impact_magnitude": analysis.impact_magnitude,
                "concept_tags": analysis.concept_tags or [],
                "published_at": (r.published_at or r.fetched_at).isoformat(),
                "original_url": r.url,
            })

    # Sort by time descending
    all_results.sort(
        key=lambda x: x.get("published_at") or "1970-01-01T00:00:00",
        reverse=True,
    )

    total = len(all_results)
    start = (page - 1) * page_size
    paged = all_results[start: start + page_size]

    # Source breakdown
    source_counts: dict[str, int] = {}
    for r in all_results:
        src = r["source_type"]
        source_counts[src] = source_counts.get(src, 0) + 1

    # Build response items with URLs
    items = [
        SearchItem(
            id=r["id"],
            source_type=r["source_type"],
            source_label=r["source_label"],
            title=r["title"],
            title_zh=r.get("title_zh"),
            summary=r.get("summary"),
            tickers=r.get("tickers", []),
            sectors=r.get("sectors", []),
            sentiment=r.get("sentiment"),
            impact_magnitude=r.get("impact_magnitude"),
            concept_tags=r.get("concept_tags", []),
            published_at=r.get("published_at"),
            detail_url=_detail_url(r["source_type"], r["id"]),
            site_url=_site_url(r["source_type"], r["id"]),
            original_url=r.get("original_url"),
        )
        for r in paged
    ]

    return SearchResponse(
        query=q,
        resolved_stock=resolved,
        total=total,
        page=page,
        page_size=page_size,
        has_next=(page * page_size) < total,
        items=items,
        source_counts=source_counts,
    )


# ── Endpoint 3: Detail ─────────────────────────────────────────────────

_VALID_SOURCE_TYPES = {
    "news", "alphapai_wechat", "alphapai_comment",
    "alphapai_roadshow_cn", "alphapai_roadshow_us",
    "jiuqian_forum", "jiuqian_minutes", "jiuqian_wechat",
}


@router.get("/detail/{source_type}/{item_id}", response_model=DetailResponse)
async def get_detail(
    source_type: str,
    item_id: str,
    db: AsyncSession = Depends(get_db),
    _api_key=Depends(verify_api_key),
):
    """Get full content for a specific item. Use the detail_url from search results."""
    if source_type not in _VALID_SOURCE_TYPES:
        raise HTTPException(400, f"Invalid source_type. Must be one of: {', '.join(sorted(_VALID_SOURCE_TYPES))}")

    if source_type == "news":
        return await _detail_news(item_id, db)
    elif source_type == "alphapai_wechat":
        return await _detail_alphapai_article(item_id, db)
    elif source_type == "alphapai_comment":
        return await _detail_alphapai_comment(item_id, db)
    elif source_type == "alphapai_roadshow_cn":
        return await _detail_alphapai_roadshow_cn(item_id, db)
    elif source_type == "alphapai_roadshow_us":
        return await _detail_alphapai_roadshow_us(item_id, db)
    elif source_type == "jiuqian_forum":
        return await _detail_jiuqian_forum(item_id, db)
    elif source_type == "jiuqian_minutes":
        return await _detail_jiuqian_minutes(item_id, db)
    elif source_type == "jiuqian_wechat":
        return await _detail_jiuqian_wechat(item_id, db)


# ── Detail handlers per source ─────────────────────────────────────────


async def _detail_news(item_id: str, db: AsyncSession) -> DetailResponse:
    news = await db.scalar(select(NewsItem).where(NewsItem.id == item_id))
    if not news:
        raise HTTPException(404, "News item not found")

    analysis = await db.scalar(
        select(AnalysisResult).where(AnalysisResult.news_item_id == item_id)
    )
    research = await db.scalar(
        select(ResearchReport).where(ResearchReport.news_item_id == item_id)
    )

    analysis_brief = None
    if analysis:
        analysis_brief = AnalysisBrief(
            sentiment=analysis.sentiment,
            impact_magnitude=analysis.impact_magnitude,
            impact_timeframe=analysis.impact_timeframe,
            summary=analysis.summary,
            key_facts=analysis.key_facts or [],
            bull_case=analysis.bull_case,
            bear_case=analysis.bear_case,
            ticker_sentiments=analysis.ticker_sentiments or {},
            surprise_factor=analysis.surprise_factor,
            concept_tags=analysis.concept_tags or [],
            industry_tags=analysis.industry_tags or [],
        )

    research_brief = None
    if research:
        deep = research.deep_research_data or {}
        research_brief = ResearchBrief(
            executive_summary=research.executive_summary,
            context=research.context,
            historical_precedent=research.historical_precedent,
            bull_scenario=research.bull_scenario,
            bear_scenario=research.bear_scenario,
            recommended_actions=research.recommended_actions,
            risk_factors=research.risk_factors,
            confidence=research.confidence,
            citations=deep.get("citations", []),
        )

    title_zh = (news.metadata_ or {}).get("title_zh")
    return DetailResponse(
        id=news.id,
        source_type="news",
        title=news.title,
        title_zh=title_zh,
        content=news.content,
        published_at=(news.published_at or news.fetched_at).isoformat(),
        original_url=news.url,
        site_url=_site_url("news", news.id),
        tickers=analysis.affected_tickers if analysis else [],
        sectors=analysis.affected_sectors if analysis else [],
        analysis=analysis_brief,
        research=research_brief,
    )


async def _detail_alphapai_article(item_id: str, db: AsyncSession) -> DetailResponse:
    row = await db.scalar(
        select(AlphaPaiArticle).where(AlphaPaiArticle.arc_code == item_id)
    )
    if not row:
        raise HTTPException(404, "Article not found")

    # Lazy-load content if needed
    if not row.content_cached and row.content_html_path:
        try:
            from backend.app.services.alphapai_client import AlphaPaiClient
            settings = get_settings()
            client = AlphaPaiClient(settings.alphapai_base_url, settings.alphapai_app_agent)
            row.content_cached = await client.download_content(row.content_html_path)
            await client.close()
            await db.commit()
        except Exception as exc:
            logger.warning("Failed to download article content: %s", exc)

    enr = row.enrichment or {}
    return DetailResponse(
        id=row.arc_code,
        source_type="alphapai_wechat",
        title=row.arc_name,
        content=row.content_cached or "",
        published_at=row.publish_time.isoformat() if row.publish_time else None,
        original_url=row.url or None,
        site_url=_site_url("alphapai_wechat", row.arc_code),
        tickers=enr.get("tickers", []),
        sectors=enr.get("sectors", []),
        analysis=AnalysisBrief(
            sentiment=enr.get("sentiment"),
            summary=enr.get("summary"),
            concept_tags=enr.get("concept_tags", []),
        ) if enr.get("summary") else None,
    )


async def _detail_alphapai_comment(item_id: str, db: AsyncSession) -> DetailResponse:
    row = await db.scalar(
        select(AlphaPaiComment).where(AlphaPaiComment.cmnt_hcode == item_id)
    )
    if not row:
        raise HTTPException(404, "Comment not found")

    enr = row.enrichment or {}
    return DetailResponse(
        id=row.cmnt_hcode,
        source_type="alphapai_comment",
        title=row.title,
        content=row.content or "",
        published_at=row.cmnt_date.isoformat() if row.cmnt_date else None,
        site_url=_site_url("alphapai_comment", row.cmnt_hcode),
        tickers=enr.get("tickers", []),
        sectors=enr.get("sectors", []),
        analysis=AnalysisBrief(
            sentiment=enr.get("sentiment"),
            summary=enr.get("summary"),
            concept_tags=enr.get("concept_tags", []),
        ) if enr.get("summary") else None,
    )


async def _detail_alphapai_roadshow_cn(item_id: str, db: AsyncSession) -> DetailResponse:
    row = await db.scalar(
        select(AlphaPaiRoadshowCN).where(AlphaPaiRoadshowCN.trans_id == item_id)
    )
    if not row:
        raise HTTPException(404, "Roadshow not found")

    # Lazy-load content
    if not row.content_cached and row.content_path:
        try:
            from backend.app.services.alphapai_client import AlphaPaiClient
            settings = get_settings()
            client = AlphaPaiClient(settings.alphapai_base_url, settings.alphapai_app_agent)
            row.content_cached = await client.download_content(row.content_path)
            await client.close()
            await db.commit()
        except Exception as exc:
            logger.warning("Failed to download roadshow content: %s", exc)

    enr = row.enrichment or {}
    return DetailResponse(
        id=row.trans_id,
        source_type="alphapai_roadshow_cn",
        title=row.show_title,
        content=row.content_cached or "",
        published_at=row.stime.isoformat() if row.stime else None,
        site_url=_site_url("alphapai_roadshow_cn", row.trans_id),
        tickers=enr.get("tickers", []),
        sectors=enr.get("sectors", []),
        analysis=AnalysisBrief(
            sentiment=enr.get("sentiment"),
            summary=enr.get("summary"),
            concept_tags=enr.get("concept_tags", []),
        ) if enr.get("summary") else None,
    )


async def _detail_alphapai_roadshow_us(item_id: str, db: AsyncSession) -> DetailResponse:
    row = await db.scalar(
        select(AlphaPaiRoadshowUS).where(AlphaPaiRoadshowUS.trans_id == item_id)
    )
    if not row:
        raise HTTPException(404, "Roadshow not found")

    if not row.content_cached and row.content_path:
        try:
            from backend.app.services.alphapai_client import AlphaPaiClient
            settings = get_settings()
            client = AlphaPaiClient(settings.alphapai_base_url, settings.alphapai_app_agent)
            row.content_cached = await client.download_content(row.content_path)
            await client.close()
            await db.commit()
        except Exception as exc:
            logger.warning("Failed to download roadshow content: %s", exc)

    enr = row.enrichment or {}
    return DetailResponse(
        id=row.trans_id,
        source_type="alphapai_roadshow_us",
        title=row.show_title,
        content=row.content_cached or "",
        published_at=row.stime.isoformat() if row.stime else None,
        site_url=_site_url("alphapai_roadshow_us", row.trans_id),
        tickers=enr.get("tickers", []),
        sectors=enr.get("sectors", []),
        analysis=AnalysisBrief(
            sentiment=enr.get("sentiment"),
            summary=enr.get("summary"),
            concept_tags=enr.get("concept_tags", []),
        ) if enr.get("summary") else None,
    )


async def _detail_jiuqian_forum(item_id: str, db: AsyncSession) -> DetailResponse:
    try:
        forum_id = int(item_id)
    except (ValueError, TypeError):
        raise HTTPException(400, "Invalid forum ID (must be integer)")
    row = await db.scalar(
        select(JiuqianForum).where(JiuqianForum.id == forum_id)
    )
    if not row:
        raise HTTPException(404, "Forum item not found")

    enr = row.enrichment or {}
    return DetailResponse(
        id=str(row.id),
        source_type="jiuqian_forum",
        title=row.title,
        content=row.content or "",
        published_at=row.meeting_time.isoformat() if row.meeting_time else None,
        site_url=_site_url("jiuqian_forum", str(row.id)),
        tickers=enr.get("tickers", []),
        sectors=enr.get("sectors", []),
        analysis=AnalysisBrief(
            sentiment=enr.get("sentiment"),
            summary=enr.get("summary") or row.summary,
            concept_tags=enr.get("concept_tags", []),
        ) if enr.get("summary") or row.summary else None,
    )


async def _detail_jiuqian_minutes(item_id: str, db: AsyncSession) -> DetailResponse:
    row = await db.scalar(
        select(JiuqianMinutes).where(JiuqianMinutes.id == item_id)
    )
    if not row:
        raise HTTPException(404, "Minutes item not found")

    enr = row.enrichment or {}
    return DetailResponse(
        id=row.id,
        source_type="jiuqian_minutes",
        title=row.title,
        content=row.content or "",
        published_at=row.pub_time.isoformat() if row.pub_time else None,
        site_url=_site_url("jiuqian_minutes", row.id),
        tickers=enr.get("tickers", []),
        sectors=enr.get("sectors", []),
        analysis=AnalysisBrief(
            sentiment=enr.get("sentiment"),
            summary=enr.get("summary") or row.summary,
            concept_tags=enr.get("concept_tags", []),
        ) if enr.get("summary") or row.summary else None,
    )


async def _detail_jiuqian_wechat(item_id: str, db: AsyncSession) -> DetailResponse:
    row = await db.scalar(
        select(JiuqianWechat).where(JiuqianWechat.id == item_id)
    )
    if not row:
        raise HTTPException(404, "WeChat article not found")

    enr = row.enrichment or {}
    return DetailResponse(
        id=row.id,
        source_type="jiuqian_wechat",
        title=row.title,
        content=row.content or "",
        published_at=row.pub_time.isoformat() if row.pub_time else None,
        original_url=row.post_url or None,
        site_url=_site_url("jiuqian_wechat", row.id),
        tickers=enr.get("tickers", []),
        sectors=enr.get("sectors", []),
        analysis=AnalysisBrief(
            sentiment=enr.get("sentiment"),
            summary=enr.get("summary") or row.summary,
            concept_tags=enr.get("concept_tags", []),
        ) if enr.get("summary") or row.summary else None,
    )


# ── Knowledge Base endpoints ───────────────────────────────────────────
#
# These are thin wrappers around backend.app.services.kb_service. The same
# service functions back the internal AI-chat tools (kb_search / kb_fetch_document
# / kb_list_facets), so upgrading the KB upgrades both surfaces in lockstep.


@router.get("/kb/meta", response_model=KbMetaResponse)
async def kb_meta(_api_key=Depends(verify_api_key)):
    """Introspect the KB: list all source platforms, doc_type enums, and collections.

    Call this once when setting up an agent to build valid `doc_types` and
    `sources` filter values.
    """
    collections = [
        KbCollectionInfo(
            source=s.db,
            collection=s.collection,
            doc_type=s.doc_type,
            doc_type_cn=s.doc_type_cn,
            has_pdf=s.has_pdf,
        )
        for s in kb_service.SPECS_LIST
    ]
    return KbMetaResponse(
        sources=kb_service.ALL_SOURCES,
        doc_types=kb_service.ALL_DOC_TYPES,
        collections=collections,
        notes=(
            "Ticker format: CODE.MARKET (NVDA.US, 0700.HK, 600519.SH). "
            "Bare codes like 'NVDA' or '0700' are auto-expanded. "
            "HK codes are zero-padded to 5 digits internally."
        ),
    )


@router.post("/kb/search", response_model=KbSearchResponse)
async def kb_search(
    req: KbSearchRequest,
    _api_key=Depends(verify_api_key),
):
    """Hybrid filter + text-match search across all 16 KB collections.

    Filter stack: tickers (canonical or bare), doc_types, sources, date_range.
    Scoring: CJK bigram + Latin token match, title boost 3x, recency bonus.
    Returns top_k hits with stable `doc_id` for the follow-up /kb/fetch call.
    """
    date_range = req.date_range.model_dump(exclude_none=True) if req.date_range else None
    hits = await kb_service.search(
        req.query or "",
        tickers=req.tickers,
        doc_types=req.doc_types,
        sources=req.sources,
        date_range=date_range,
        top_k=req.top_k,
    )
    return KbSearchResponse(query=req.query or "", total=len(hits), hits=hits)


@router.post("/kb/fetch", response_model=KbFetchResponse)
async def kb_fetch(
    req: KbFetchRequest,
    _api_key=Depends(verify_api_key),
):
    """Fetch the full text of a KB document by `doc_id`.

    The doc_id format is `<source>:<collection>:<_id>` — exactly the value
    returned by /kb/search in each hit.
    """
    res = await kb_service.fetch_document(req.doc_id, max_chars=req.max_chars)
    return KbFetchResponse(**res)


@router.post("/kb/facets", response_model=KbFacetsResponse)
async def kb_facets(
    req: KbFacetsRequest,
    _api_key=Depends(verify_api_key),
):
    """Count KB docs along a dimension subject to filters.

    Dimensions: sources | doc_types | tickers | date_histogram.
    Use this to scope a search before running kb_search — e.g. "how many
    broker reports on NVDA in the last 3 months?".
    """
    filters_dict: dict = {}
    if req.filters:
        f = req.filters.model_dump(exclude_none=True)
        if "date_range" in f and isinstance(f["date_range"], dict):
            # already a dict, pass through
            pass
        filters_dict = f
    try:
        rows = await kb_service.list_facets(req.dimension, filters=filters_dict, top=req.top)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return KbFacetsResponse(dimension=req.dimension, rows=rows)
