"""Unified stock search API — aggregates results from AlphaPai, Jiuqian, and News.

Allows traders to search by stock name or code and get all related information
across all data sources in one place.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Body, Depends, Query
from sqlalchemy import String, cast, select, func, desc, or_
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.deps import get_db, get_current_user
from backend.app.models.alphapai import (
    AlphaPaiArticle,
    AlphaPaiComment,
    AlphaPaiRoadshowCN,
    AlphaPaiRoadshowUS,
)
from backend.app.models.jiuqian import JiuqianForum, JiuqianMinutes, JiuqianWechat
from backend.app.models.news import AnalysisResult, FilterResult, NewsItem
from backend.app.models.user import User
from backend.app.services.stock_verifier import get_stock_verifier
from backend.app.services.ticker_normalizer import normalize_one


# Mirrors frontend Portfolio.tsx::toCanonical so each suggestion's
# canonical_id matches what scripts/enrich_tickers.py writes into Mongo
# `_canonical_tickers`. Custom KR/JP/AU/DE codes bypass normalize_one()
# because a bare 6-digit Korean code (e.g. "005930") would otherwise be
# misclassified as A-share by the bare-code parser.
_MARKET_SUFFIX = {"韩股": "KS", "日股": "JP", "澳股": "AU", "德股": "DE"}


def _suggestion_canonical(code: str, market: str) -> str | None:
    code = (code or "").strip()
    if not code:
        return None
    if "." in code and code.endswith((".SH", ".SZ", ".BJ", ".HK", ".US")):
        return code.upper()
    if market == "A股":
        # Bare CN code that didn't get a suffix on its way in
        return normalize_one(code)
    if market == "美股":
        return f"{code.upper()}.US"
    if market == "港股":
        digits = re.sub(r"\D", "", code).lstrip("0") or "0"
        return f"{digits.zfill(5)}.HK"
    suffix = _MARKET_SUFFIX.get(market)
    if suffix:
        return f"{code.upper()}.{suffix}"
    if market == "其他":
        # Resolve the real market via portfolio_sources.yaml registration
        verifier = get_stock_verifier()
        real_market = (
            verifier._custom_code_to_market.get(code)
            or verifier._custom_code_to_market.get(code.upper())
        )
        if real_market:
            sfx = _MARKET_SUFFIX.get(real_market)
            if sfx:
                return f"{code.upper()}.{sfx}"
    try:
        return normalize_one(code)
    except Exception:
        return None

logger = logging.getLogger(__name__)
router = APIRouter()


def _escape_like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _cutoff(hours: int) -> datetime:
    return datetime.now(timezone.utc) - timedelta(hours=hours)


def _build_match_conditions(search_terms: list[str], tickers_col, title_col=None):
    """Build OR conditions matching search terms against enrichment tickers + optional title.

    Searches enrichment["tickers"] for all terms, and also searches the title
    column for non-code terms (Chinese names >= 2 chars) to catch articles that
    mention a stock in the title but weren't tagged during enrichment.
    """
    conditions = []
    for term in search_terms:
        safe = _escape_like(term)
        conditions.append(cast(tickers_col, String).ilike(f"%{safe}%"))
        # Also match title for Chinese name terms (not pure codes)
        if title_col is not None and len(term) >= 2 and not term.isascii():
            conditions.append(title_col.ilike(f"%{safe}%"))
    return conditions


@router.get("/suggest")
async def suggest_stocks(
    q: str = Query(min_length=1, max_length=50),
    limit: int = Query(10, ge=1, le=30),
    user: User = Depends(get_current_user),
):
    """Fast autocomplete: return matching stocks from local stock lists.

    Returns list of {name, code, market} sorted by relevance.
    Used for the search bar dropdown as the user types.
    """
    q = q.strip()
    if not q:
        return []

    verifier = get_stock_verifier()
    if not verifier._loaded:
        verifier.load_stock_lists()

    q_upper = q.upper()
    q_lower = q.lower()
    results: list[dict] = []
    seen_names: set[str] = set()  # dedupe by canonical name across all sources

    def _canonicalize(code: str) -> str:
        """Normalize a raw code to its canonical form.

        HK stocks can arrive either as ``00100`` or ``00100.HK`` (the
        verifier indexes both); we always present the ``.HK`` suffix so the
        dropdown shows one row per listing.
        """
        if code.endswith((".SH", ".SZ", ".BJ", ".HK")):
            return code
        if code in verifier._hk_code_to_name:
            # Numeric HK codes without the suffix.
            return f"{code}.HK"
        return code

    def _detect_market(code: str) -> str:
        """Determine market label from stock code."""
        if code.endswith((".SH", ".SZ", ".BJ")):
            return "A股"
        if code.endswith(".HK"):
            return "港股"
        # Check custom portfolio stocks for JP/KR market labels
        custom_market = verifier._custom_code_to_market.get(code) or verifier._custom_code_to_market.get(code.upper())
        if custom_market:
            return custom_market
        # Bare numeric HK codes (pre-canonicalization) also fall here if
        # they somehow escaped the normalizer.
        if code in verifier._hk_code_to_name:
            return "港股"
        return "美股"

    def _add(name: str, code: str, market: str, rank: int):
        # Canonicalize once here so every call site (exact, fuzzy, loop)
        # gets a consistent code shape.
        code = _canonicalize(code)
        if market == "美股" and _detect_market(code) == "港股":
            # Fix the market label if we canonicalized into a .HK suffix.
            market = "港股"
        key = f"{code}|{name}"
        if key in seen_names:
            return
        # Also dedupe by (market, name) so "MINIMAX-W" under 港股 only shows once.
        # A-share + US overlap on ticker is rare enough that name dedup is a
        # net win for readability.
        same_name_key = f"{market}|{name}"
        if same_name_key in seen_names:
            return
        seen_names.add(key)
        seen_names.add(same_name_key)
        results.append({
            "name": name,
            "code": code,
            "market": market,
            "label": f"{name}({code})",
            "rank": rank,
            "canonical_id": _suggestion_canonical(code, market),
        })

    # --- Exact code match (highest priority) ---
    result = verifier._lookup_by_code(q_upper)
    if result:
        name, code = result
        _add(name, code, _detect_market(code), 0)

    # --- Exact name match ---
    result = verifier._lookup_by_name(q)
    if result:
        name, code = result
        _add(name, code, _detect_market(code), 1)

    # --- Prefix/substring matching across all lists ---
    # Every block uses case-insensitive substring on the name so queries
    # like "minimax" match HK listings like "MINIMAX-W". (Historically only
    # the US block did this; HK/A/custom were case-sensitive, which meant
    # newer HK listings that only have an uppercase English name were
    # invisible to lower-case queries.)
    def _name_matches(name: str) -> bool:
        return q_lower in name.lower() or q in name

    # A-shares: match by code prefix or name substring
    for code, name in verifier._a_code_to_name.items():
        if "." not in code:
            continue  # skip bare codes, only use full codes
        if len(results) >= limit:
            break
        if code.upper().startswith(q_upper) or _name_matches(name):
            _add(name, code, "A股", 2)

    # US stocks
    for code, name in verifier._us_code_to_name.items():
        if len(results) >= limit:
            break
        if code.upper().startswith(q_upper) or _name_matches(name):
            _add(name, code, "美股", 2)

    # HK stocks — iterate by name so each listing is unique. The verifier
    # stores each HK stock under two code keys (``00100`` and ``00100.HK``),
    # which makes ``_hk_code_to_name`` return duplicates; the name→code map
    # is already deduped. ``_add`` canonicalizes the code to the .HK form.
    for name, bare_code in verifier._hk_name_to_code.items():
        if len(results) >= limit:
            break
        full_code = bare_code if bare_code.endswith(".HK") else f"{bare_code}.HK"
        if (
            bare_code.upper().startswith(q_upper)
            or full_code.upper().startswith(q_upper)
            or _name_matches(name)
        ):
            _add(name, full_code, "港股", 2)

    # Custom/portfolio stocks (JP, KR, etc.)
    for code, name in verifier._custom_code_to_name.items():
        if len(results) >= limit:
            break
        if code.upper().startswith(q_upper) or _name_matches(name):
            _add(name, code, "其他", 2)

    # Sort: exact match first, then prefix, then substring
    results.sort(key=lambda x: x["rank"])
    return results[:limit]


@router.get("/search")
async def search_by_stock(
    q: str = Query(min_length=1, max_length=100, description="Stock name or code"),
    hours: int = Query(168, ge=1, le=720, description="Time window in hours"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Search across all data sources by stock name or code.

    Searches in: AlphaPai (articles, comments, roadshows), Jiuqian (forum, minutes, wechat),
    and News Center (news items with analysis).
    """
    q = q.strip()
    safe_q = _escape_like(q)
    like_q = f"%{safe_q}%"
    cutoff = _cutoff(hours)

    # Try to resolve stock name/code for better matching
    verifier = get_stock_verifier()
    search_terms = [q]

    # If user typed a code, find the name; if typed a name, find the code
    result = verifier._lookup_by_code(q.upper())
    if result:
        search_terms.append(result[0])  # Add name
        search_terms.append(result[1])  # Add full code
    else:
        result = verifier._lookup_by_name(q)
        if result:
            search_terms.append(result[0])  # Add canonical name
            search_terms.append(result[1])  # Add code
            # Also add bare code without suffix
            bare_code = result[1].split(".")[0] if "." in result[1] else result[1]
            search_terms.append(bare_code)

    # Deduplicate search terms
    search_terms = list(set(t for t in search_terms if t))

    all_results: list[dict] = []

    # --- AlphaPai Articles ---
    art_conditions = _build_match_conditions(
        search_terms, AlphaPaiArticle.enrichment["tickers"], AlphaPaiArticle.arc_name,
    )
    stmt = (
        select(AlphaPaiArticle)
        .where(AlphaPaiArticle.publish_time >= cutoff)
        .where(AlphaPaiArticle.is_enriched == True)  # noqa
        .where(or_(
            AlphaPaiArticle.enrichment["skipped"].as_boolean().is_(False),
            AlphaPaiArticle.enrichment["skipped"].is_(None),
        ))
        .where(or_(*art_conditions))
        .order_by(desc(AlphaPaiArticle.publish_time))
        .limit(50)
    )
    for r in (await db.execute(stmt)).scalars():
        enr = r.enrichment or {}
        all_results.append({
            "source": "alphapai_wechat",
            "source_label": "AlphaPai公众号",
            "id": r.arc_code,
            "title": r.arc_name,
            "time": r.publish_time.isoformat() if r.publish_time else None,
            "summary": enr.get("summary", ""),
            "tickers": enr.get("tickers", []),
            "sectors": enr.get("sectors", []),
            "sentiment": enr.get("sentiment", ""),
            "relevance_score": enr.get("relevance_score", 0),
            "market_impact_score": enr.get("market_impact_score"),
            "url": r.url or "",
            "author": r.author,
        })

    # --- AlphaPai Comments ---
    cmt_conditions = _build_match_conditions(
        search_terms, AlphaPaiComment.enrichment["tickers"], AlphaPaiComment.title,
    )
    stmt = (
        select(AlphaPaiComment)
        .where(AlphaPaiComment.cmnt_date >= cutoff)
        .where(AlphaPaiComment.is_enriched == True)  # noqa
        .where(or_(*cmt_conditions))
        .order_by(desc(AlphaPaiComment.cmnt_date))
        .limit(30)
    )
    for r in (await db.execute(stmt)).scalars():
        enr = r.enrichment or {}
        all_results.append({
            "source": "alphapai_comment",
            "source_label": "AlphaPai券商点评",
            "id": r.cmnt_hcode,
            "title": r.title,
            "time": r.cmnt_date.isoformat() if r.cmnt_date else None,
            "summary": enr.get("summary", ""),
            "tickers": enr.get("tickers", []),
            "sectors": enr.get("sectors", []),
            "sentiment": enr.get("sentiment", ""),
            "relevance_score": enr.get("relevance_score", 0),
            "institution": r.inst_cname,
            "analyst": r.psn_name,
        })

    # --- AlphaPai Roadshows CN ---
    rs_conditions = _build_match_conditions(
        search_terms, AlphaPaiRoadshowCN.enrichment["tickers"], AlphaPaiRoadshowCN.show_title,
    )
    stmt = (
        select(AlphaPaiRoadshowCN)
        .where(AlphaPaiRoadshowCN.stime >= cutoff)
        .where(AlphaPaiRoadshowCN.is_enriched == True)  # noqa
        .where(or_(*rs_conditions))
        .order_by(desc(AlphaPaiRoadshowCN.stime))
        .limit(20)
    )
    for r in (await db.execute(stmt)).scalars():
        enr = r.enrichment or {}
        all_results.append({
            "source": "alphapai_roadshow",
            "source_label": "AlphaPai路演纪要",
            "id": r.trans_id,
            "title": r.show_title,
            "time": r.stime.isoformat() if r.stime else None,
            "summary": enr.get("summary", ""),
            "tickers": enr.get("tickers", []),
            "sectors": enr.get("sectors", []),
            "sentiment": enr.get("sentiment", ""),
            "relevance_score": enr.get("relevance_score", 0),
            "company": r.company,
        })

    # --- Jiuqian Forum ---
    jq_forum_conditions = _build_match_conditions(
        search_terms, JiuqianForum.enrichment["tickers"], JiuqianForum.title,
    )
    stmt = (
        select(JiuqianForum)
        .where(JiuqianForum.is_enriched == True)  # noqa
        .where(or_(*jq_forum_conditions))
        .order_by(desc(JiuqianForum.meeting_time))
        .limit(20)
    )
    for r in (await db.execute(stmt)).scalars():
        enr = r.enrichment or {}
        all_results.append({
            "source": "jiuqian_forum",
            "source_label": "久谦专家访谈",
            "id": str(r.id),
            "title": r.title,
            "time": r.meeting_time.isoformat() if r.meeting_time else None,
            "summary": enr.get("summary", "") or r.summary or "",
            "tickers": enr.get("tickers", []),
            "sectors": enr.get("sectors", []),
            "sentiment": enr.get("sentiment", ""),
            "relevance_score": enr.get("relevance_score", 0),
            "industry": r.industry,
        })

    # --- Jiuqian Minutes ---
    jq_min_conditions = _build_match_conditions(
        search_terms, JiuqianMinutes.enrichment["tickers"], JiuqianMinutes.title,
    )
    stmt = (
        select(JiuqianMinutes)
        .where(JiuqianMinutes.pub_time >= cutoff)
        .where(JiuqianMinutes.is_enriched == True)  # noqa
        .where(or_(*jq_min_conditions))
        .order_by(desc(JiuqianMinutes.pub_time))
        .limit(20)
    )
    for r in (await db.execute(stmt)).scalars():
        enr = r.enrichment or {}
        all_results.append({
            "source": "jiuqian_minutes",
            "source_label": "久谦研究纪要",
            "id": r.id,
            "title": r.title,
            "time": r.pub_time.isoformat() if r.pub_time else None,
            "summary": enr.get("summary", "") or r.summary or "",
            "tickers": enr.get("tickers", []),
            "sectors": enr.get("sectors", []),
            "sentiment": enr.get("sentiment", ""),
            "relevance_score": enr.get("relevance_score", 0),
            "source_name": r.source,
        })

    # --- Jiuqian WeChat ---
    jq_wx_conditions = _build_match_conditions(
        search_terms, JiuqianWechat.enrichment["tickers"], JiuqianWechat.title,
    )
    stmt = (
        select(JiuqianWechat)
        .where(JiuqianWechat.pub_time >= cutoff)
        .where(JiuqianWechat.is_enriched == True)  # noqa
        .where(or_(
            JiuqianWechat.enrichment["skipped"].as_boolean().is_(False),
            JiuqianWechat.enrichment["skipped"].is_(None),
        ))
        .where(or_(*jq_wx_conditions))
        .order_by(desc(JiuqianWechat.pub_time))
        .limit(20)
    )
    for r in (await db.execute(stmt)).scalars():
        enr = r.enrichment or {}
        all_results.append({
            "source": "jiuqian_wechat",
            "source_label": "久谦公众号",
            "id": r.id,
            "title": r.title,
            "time": r.pub_time.isoformat() if r.pub_time else None,
            "summary": enr.get("summary", "") or r.summary or "",
            "tickers": enr.get("tickers", []),
            "sectors": enr.get("sectors", []),
            "sentiment": enr.get("sentiment", ""),
            "relevance_score": enr.get("relevance_score", 0),
            "url": r.post_url or "",
        })

    # --- News Center (资讯中心) ---
    news_conditions = _build_match_conditions(
        search_terms, AnalysisResult.affected_tickers, NewsItem.title,
    )
    stmt = (
        select(NewsItem)
        .join(AnalysisResult, NewsItem.id == AnalysisResult.news_item_id)
        .join(FilterResult, NewsItem.id == FilterResult.news_item_id)
        .where(NewsItem.fetched_at >= cutoff)
        # Only show items that passed quality filter: relevant + non-neutral sentiment
        .where(FilterResult.is_relevant.is_(True))
        .where(AnalysisResult.sentiment.isnot(None))
        .where(AnalysisResult.sentiment.notin_(["neutral"]))
        .where(or_(*news_conditions))
        .order_by(desc(NewsItem.fetched_at))
        .limit(30)
    )
    for r in (await db.execute(stmt)).scalars().unique():
        analysis = await db.scalar(
            select(AnalysisResult).where(AnalysisResult.news_item_id == r.id)
        )
        if not analysis:
            continue
        title_zh = (r.metadata_ or {}).get("title_zh")
        all_results.append({
            "source": "news",
            "source_label": "资讯中心",
            "id": r.id,
            "title": title_zh or r.title,
            "original_title": r.title if title_zh else None,
            "time": (r.published_at or r.fetched_at).isoformat(),
            "summary": analysis.summary or "",
            "tickers": analysis.affected_tickers or [],
            "sectors": analysis.affected_sectors or [],
            "sentiment": analysis.sentiment or "",
            "impact_magnitude": analysis.impact_magnitude,
            "surprise_factor": analysis.surprise_factor,
            "url": r.url,
            "source_name": r.source_name,
        })

    # Sort all results by time (newest first)
    all_results.sort(
        key=lambda x: x.get("time") or "1970-01-01T00:00:00",
        reverse=True,
    )

    total = len(all_results)
    start = (page - 1) * page_size
    paged = all_results[start: start + page_size]

    # Source breakdown counts
    source_counts = {}
    for r in all_results:
        src = r["source"]
        source_counts[src] = source_counts.get(src, 0) + 1

    return {
        "items": paged,
        "total": total,
        "page": page,
        "page_size": page_size,
        "has_next": (page * page_size) < total,
        "query": q,
        "search_terms": search_terms,
        "source_counts": source_counts,
    }


@router.post("/counts")
async def batch_stock_counts(
    tickers: list[str] = Body(embed=True),
    hours: int = Query(168, ge=1, le=720),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Return the number of related items across all sources for each ticker.

    Used by the portfolio page to show update badges without fetching full results.
    Runs all 7 source queries per ticker in parallel for speed.
    """
    cutoff = _cutoff(hours)
    verifier = get_stock_verifier()

    # Pre-resolve all tickers' search terms (CPU-only, fast)
    ticker_terms: list[tuple[str, list[str]]] = []
    for ticker in tickers[:50]:
        ticker = ticker.strip()
        if not ticker:
            continue
        search_terms = [ticker]
        lookup = verifier._lookup_by_code(ticker.upper())
        if lookup:
            search_terms.append(lookup[0])
            search_terms.append(lookup[1])
        else:
            lookup = verifier._lookup_by_name(ticker)
            if lookup:
                search_terms.append(lookup[0])
                search_terms.append(lookup[1])
                bare = lookup[1].split(".")[0] if "." in lookup[1] else lookup[1]
                search_terms.append(bare)
        search_terms = list(set(t for t in search_terms if t))
        ticker_terms.append((ticker, search_terms))

    result: dict[str, int] = {}
    for tk, terms in ticker_terms:
        # Build 7 scalar subqueries and sum them in ONE SQL round-trip
        conds = _build_match_conditions(terms, AlphaPaiArticle.enrichment["tickers"], AlphaPaiArticle.arc_name)
        sq1 = (
            select(func.count()).select_from(AlphaPaiArticle)
            .where(AlphaPaiArticle.publish_time >= cutoff)
            .where(AlphaPaiArticle.is_enriched == True)  # noqa
            .where(or_(AlphaPaiArticle.enrichment["skipped"].as_boolean().is_(False), AlphaPaiArticle.enrichment["skipped"].is_(None)))
            .where(or_(*conds))
            .correlate(None).scalar_subquery()
        )

        conds = _build_match_conditions(terms, AlphaPaiComment.enrichment["tickers"], AlphaPaiComment.title)
        sq2 = (
            select(func.count()).select_from(AlphaPaiComment)
            .where(AlphaPaiComment.cmnt_date >= cutoff)
            .where(AlphaPaiComment.is_enriched == True)  # noqa
            .where(or_(*conds))
            .correlate(None).scalar_subquery()
        )

        conds = _build_match_conditions(terms, AlphaPaiRoadshowCN.enrichment["tickers"], AlphaPaiRoadshowCN.show_title)
        sq3 = (
            select(func.count()).select_from(AlphaPaiRoadshowCN)
            .where(AlphaPaiRoadshowCN.stime >= cutoff)
            .where(AlphaPaiRoadshowCN.is_enriched == True)  # noqa
            .where(or_(*conds))
            .correlate(None).scalar_subquery()
        )

        conds = _build_match_conditions(terms, JiuqianForum.enrichment["tickers"], JiuqianForum.title)
        sq4 = (
            select(func.count()).select_from(JiuqianForum)
            .where(JiuqianForum.is_enriched == True)  # noqa
            .where(or_(*conds))
            .correlate(None).scalar_subquery()
        )

        conds = _build_match_conditions(terms, JiuqianMinutes.enrichment["tickers"], JiuqianMinutes.title)
        sq5 = (
            select(func.count()).select_from(JiuqianMinutes)
            .where(JiuqianMinutes.pub_time >= cutoff)
            .where(JiuqianMinutes.is_enriched == True)  # noqa
            .where(or_(*conds))
            .correlate(None).scalar_subquery()
        )

        conds = _build_match_conditions(terms, JiuqianWechat.enrichment["tickers"], JiuqianWechat.title)
        sq6 = (
            select(func.count()).select_from(JiuqianWechat)
            .where(JiuqianWechat.pub_time >= cutoff)
            .where(JiuqianWechat.is_enriched == True)  # noqa
            .where(or_(JiuqianWechat.enrichment["skipped"].as_boolean().is_(False), JiuqianWechat.enrichment["skipped"].is_(None)))
            .where(or_(*conds))
            .correlate(None).scalar_subquery()
        )

        conds = _build_match_conditions(terms, AnalysisResult.affected_tickers, NewsItem.title)
        sq7 = (
            select(func.count()).select_from(NewsItem)
            .join(AnalysisResult, NewsItem.id == AnalysisResult.news_item_id)
            .join(FilterResult, NewsItem.id == FilterResult.news_item_id)
            .where(NewsItem.fetched_at >= cutoff)
            .where(FilterResult.is_relevant.is_(True))
            .where(AnalysisResult.sentiment.isnot(None))
            .where(AnalysisResult.sentiment.notin_(["neutral"]))
            .where(or_(*conds))
            .correlate(None).scalar_subquery()
        )

        total = (await db.execute(select(sq1 + sq2 + sq3 + sq4 + sq5 + sq6 + sq7))).scalar() or 0
        result[tk] = total

    return {"counts": result, "hours": hours}
