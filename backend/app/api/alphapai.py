"""REST API routes for AlphaPai data — articles, roadshows, comments, digests, unified feed.

Key changes:
- Default filtering: only show enriched items with sufficient relevance
- Articles default to relevance >= 0.4 and not skipped
- Comments default to relevance >= 0.4
- Roadshows CN default to AI source only
- Stats endpoint returns data matching frontend expectations
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, func, desc, or_
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.deps import get_db, get_current_user, get_current_admin
from backend.app.models.alphapai import (
    AlphaPaiArticle,
    AlphaPaiComment,
    AlphaPaiDigest,
    AlphaPaiRoadshowCN,
    AlphaPaiRoadshowUS,
    AlphaPaiSyncState,
)
from backend.app.models.user import User


def _escape_like(value: str) -> str:
    """Escape special LIKE/ILIKE characters to prevent wildcard injection."""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
from backend.app.schemas.alphapai import (
    ArticleBrief,
    ArticleDetail,
    CommentBrief,
    CommentDetail,
    DigestResponse,
    FeedItem,
    PaginatedResponse,
    RoadshowCNBrief,
    RoadshowCNDetail,
    RoadshowUSBrief,
    RoadshowUSDetail,
    StatsResponse,
    SyncStatusItem,
)

logger = logging.getLogger(__name__)
router = APIRouter()


# ====================================================================== #
# Helper
# ====================================================================== #
def _cutoff(hours: int | None) -> datetime | None:
    if hours is None:
        return None
    return datetime.now(timezone.utc) - timedelta(hours=hours)


async def _paginate(db: AsyncSession, base_stmt, model, page: int, page_size: int):
    count_stmt = select(func.count()).select_from(base_stmt.subquery())
    total = await db.scalar(count_stmt) or 0
    offset = (page - 1) * page_size
    rows = (await db.execute(base_stmt.offset(offset).limit(page_size))).scalars().all()
    return rows, total


# ====================================================================== #
# WeChat articles — filtered by default
# ====================================================================== #
@router.get("/wechat", response_model=PaginatedResponse)
async def list_articles(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    hours: int | None = Query(48),
    min_relevance: float = Query(0.55, ge=0.0, le=1.0),
    min_impact: int = Query(5, ge=0, le=10),
    author: str | None = None,
    ticker: str | None = None,
    sector: str | None = None,
    show_all: bool = False,
    sort_by: str = Query("impact", pattern="^(impact|time|relevance)$"),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    stmt = select(AlphaPaiArticle)
    cutoff = _cutoff(hours)
    if cutoff:
        stmt = stmt.where(AlphaPaiArticle.publish_time >= cutoff)

    # Default: only show enriched, non-skipped, non-neutral, relevant articles
    if not show_all:
        stmt = stmt.where(AlphaPaiArticle.is_enriched == True)  # noqa: E712
        # Handle NULL: articles without 'skipped' key should be included
        stmt = stmt.where(or_(
            AlphaPaiArticle.enrichment["skipped"].as_boolean().is_(False),
            AlphaPaiArticle.enrichment["skipped"].is_(None),
        ))
        # Exclude neutral sentiment
        stmt = stmt.where(or_(
            AlphaPaiArticle.enrichment["sentiment"].as_string() != "neutral",
            AlphaPaiArticle.enrichment["sentiment"].is_(None),
        ))
        stmt = stmt.where(
            AlphaPaiArticle.enrichment["relevance_score"].as_float() >= min_relevance
        )
        # Filter by market_impact_score if available (new field, graceful fallback)
        if min_impact > 0:
            stmt = stmt.where(or_(
                AlphaPaiArticle.enrichment["market_impact_score"].as_float() >= min_impact,
                # Include articles enriched before market_impact_score was added
                AlphaPaiArticle.enrichment["market_impact_score"].is_(None),
            ))

    if author:
        stmt = stmt.where(AlphaPaiArticle.author.ilike(f"%{_escape_like(author)}%"))
    if ticker:
        stmt = stmt.where(
            AlphaPaiArticle.enrichment["tickers"].astext.ilike(f"%{_escape_like(ticker)}%")
        )
    if sector:
        stmt = stmt.where(
            AlphaPaiArticle.enrichment["sectors"].astext.ilike(f"%{_escape_like(sector)}%")
        )

    # Sort: by market_impact_score (desc) then time (desc) by default
    if sort_by == "impact":
        stmt = stmt.order_by(
            desc(AlphaPaiArticle.enrichment["market_impact_score"].as_float()),
            desc(AlphaPaiArticle.publish_time),
        )
    elif sort_by == "relevance":
        stmt = stmt.order_by(
            desc(AlphaPaiArticle.enrichment["relevance_score"].as_float()),
            desc(AlphaPaiArticle.publish_time),
        )
    else:
        stmt = stmt.order_by(desc(AlphaPaiArticle.publish_time))

    rows, total = await _paginate(db, stmt, AlphaPaiArticle, page, page_size)
    return PaginatedResponse(
        items=[ArticleBrief.model_validate(r) for r in rows],
        total=total, page=page, page_size=page_size,
        has_next=(page * page_size) < total,
    )


@router.get("/wechat/{arc_code}", response_model=ArticleDetail)
async def get_article(
    arc_code: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    row = await db.scalar(
        select(AlphaPaiArticle).where(AlphaPaiArticle.arc_code == arc_code)
    )
    if not row:
        raise HTTPException(404, "Article not found")

    # Lazy download content if needed
    if not row.content_cached and row.content_html_path:
        try:
            from backend.app.services.alphapai_client import AlphaPaiClient
            from backend.app.config import get_settings
            settings = get_settings()
            client = AlphaPaiClient(settings.alphapai_base_url, settings.alphapai_app_agent)
            row.content_cached = await client.download_content(row.content_html_path)
            await client.close()
            await db.commit()
        except Exception as exc:
            logger.warning("Failed to download article content: %s", exc)

    return ArticleDetail.model_validate(row)


# ====================================================================== #
# A-share roadshows — AI source only by default
# ====================================================================== #
@router.get("/roadshows/cn", response_model=PaginatedResponse)
async def list_roadshows_cn(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    hours: int | None = Query(48),
    company: str | None = None,
    trans_source: str | None = Query("AI"),
    industry: str | None = None,
    min_relevance: float = Query(0.0, ge=0.0, le=1.0),
    ticker: str | None = None,
    sector: str | None = None,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    stmt = select(AlphaPaiRoadshowCN)
    cutoff = _cutoff(hours)
    if cutoff:
        stmt = stmt.where(AlphaPaiRoadshowCN.stime >= cutoff)
    if company:
        stmt = stmt.where(AlphaPaiRoadshowCN.company.ilike(f"%{_escape_like(company)}%"))
    if trans_source:
        stmt = stmt.where(AlphaPaiRoadshowCN.trans_source == trans_source)
    if industry:
        stmt = stmt.where(
            AlphaPaiRoadshowCN.ind_json.op("@>")(f'[{{"name": "{industry}"}}]')
        )
    # Exclude neutral sentiment
    stmt = stmt.where(or_(
        AlphaPaiRoadshowCN.enrichment["sentiment"].as_string() != "neutral",
        AlphaPaiRoadshowCN.enrichment["sentiment"].is_(None),
        AlphaPaiRoadshowCN.enrichment.is_(None),
    ))
    if min_relevance > 0:
        stmt = stmt.where(
            AlphaPaiRoadshowCN.enrichment["relevance_score"].as_float() >= min_relevance
        )
    if ticker:
        stmt = stmt.where(
            AlphaPaiRoadshowCN.enrichment["tickers"].astext.ilike(f"%{_escape_like(ticker)}%")
        )
    if sector:
        stmt = stmt.where(
            AlphaPaiRoadshowCN.enrichment["sectors"].astext.ilike(f"%{_escape_like(sector)}%")
        )
    stmt = stmt.order_by(desc(AlphaPaiRoadshowCN.stime))

    rows, total = await _paginate(db, stmt, AlphaPaiRoadshowCN, page, page_size)
    return PaginatedResponse(
        items=[RoadshowCNBrief.model_validate(r) for r in rows],
        total=total, page=page, page_size=page_size,
        has_next=(page * page_size) < total,
    )


@router.get("/roadshows/cn/{trans_id}", response_model=RoadshowCNDetail)
async def get_roadshow_cn(
    trans_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    row = await db.scalar(
        select(AlphaPaiRoadshowCN).where(AlphaPaiRoadshowCN.trans_id == trans_id)
    )
    if not row:
        raise HTTPException(404, "Roadshow not found")

    if not row.content_cached and row.content_path:
        try:
            from backend.app.services.alphapai_client import AlphaPaiClient
            from backend.app.config import get_settings
            settings = get_settings()
            client = AlphaPaiClient(settings.alphapai_base_url, settings.alphapai_app_agent)
            row.content_cached = await client.download_content(row.content_path)
            await client.close()
            await db.commit()
        except Exception as exc:
            logger.warning("Failed to download roadshow content: %s", exc)

    return RoadshowCNDetail.model_validate(row)


# ====================================================================== #
# US roadshows
# ====================================================================== #
@router.get("/roadshows/us", response_model=PaginatedResponse)
async def list_roadshows_us(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    hours: int | None = Query(48),
    company: str | None = None,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    stmt = select(AlphaPaiRoadshowUS)
    cutoff = _cutoff(hours)
    if cutoff:
        stmt = stmt.where(AlphaPaiRoadshowUS.stime >= cutoff)
    if company:
        stmt = stmt.where(AlphaPaiRoadshowUS.show_title.ilike(f"%{_escape_like(company)}%"))
    # Exclude neutral sentiment
    stmt = stmt.where(or_(
        AlphaPaiRoadshowUS.enrichment["sentiment"].as_string() != "neutral",
        AlphaPaiRoadshowUS.enrichment["sentiment"].is_(None),
        AlphaPaiRoadshowUS.enrichment.is_(None),
    ))
    stmt = stmt.order_by(desc(AlphaPaiRoadshowUS.stime))

    rows, total = await _paginate(db, stmt, AlphaPaiRoadshowUS, page, page_size)
    return PaginatedResponse(
        items=[RoadshowUSBrief.model_validate(r) for r in rows],
        total=total, page=page, page_size=page_size,
        has_next=(page * page_size) < total,
    )


@router.get("/roadshows/us/{trans_id}", response_model=RoadshowUSDetail)
async def get_roadshow_us(
    trans_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    row = await db.scalar(
        select(AlphaPaiRoadshowUS).where(AlphaPaiRoadshowUS.trans_id == trans_id)
    )
    if not row:
        raise HTTPException(404, "US roadshow not found")

    if not row.content_cached and row.content_path:
        try:
            from backend.app.services.alphapai_client import AlphaPaiClient
            from backend.app.config import get_settings
            settings = get_settings()
            client = AlphaPaiClient(settings.alphapai_base_url, settings.alphapai_app_agent)
            row.content_cached = await client.download_content(row.content_path)
            await client.close()
            await db.commit()
        except Exception as exc:
            logger.warning("Failed to download US roadshow content: %s", exc)

    return RoadshowUSDetail.model_validate(row)


# ====================================================================== #
# Analyst comments — filtered by default
# ====================================================================== #
@router.get("/comments", response_model=PaginatedResponse)
async def list_comments(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    hours: int | None = Query(24),
    institution: str | None = None,
    analyst: str | None = None,
    fortune_only: bool = False,
    ticker: str | None = None,
    sector: str | None = None,
    sentiment: str | None = None,
    min_relevance: float = Query(0.4, ge=0.0, le=1.0),
    show_all: bool = False,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    stmt = select(AlphaPaiComment)
    cutoff = _cutoff(hours)
    if cutoff:
        stmt = stmt.where(AlphaPaiComment.cmnt_date >= cutoff)

    # Default: only show enriched, non-neutral items with decent relevance
    if not show_all:
        stmt = stmt.where(AlphaPaiComment.is_enriched == True)  # noqa: E712
        stmt = stmt.where(
            AlphaPaiComment.enrichment["relevance_score"].as_float() >= min_relevance
        )
        # Exclude neutral sentiment unless explicitly requesting it
        if not sentiment:
            stmt = stmt.where(or_(
                AlphaPaiComment.enrichment["sentiment"].as_string() != "neutral",
                AlphaPaiComment.enrichment["sentiment"].is_(None),
            ))

    if institution:
        stmt = stmt.where(AlphaPaiComment.inst_cname.ilike(f"%{_escape_like(institution)}%"))
    if analyst:
        stmt = stmt.where(AlphaPaiComment.psn_name.ilike(f"%{_escape_like(analyst)}%"))
    if fortune_only:
        stmt = stmt.where(AlphaPaiComment.is_new_fortune == True)  # noqa: E712
    if ticker:
        stmt = stmt.where(
            AlphaPaiComment.enrichment["tickers"].astext.ilike(f"%{_escape_like(ticker)}%")
        )
    if sector:
        stmt = stmt.where(
            AlphaPaiComment.enrichment["sectors"].astext.ilike(f"%{_escape_like(sector)}%")
        )
    if sentiment:
        stmt = stmt.where(
            AlphaPaiComment.enrichment["sentiment"].astext == sentiment
        )
    stmt = stmt.order_by(desc(AlphaPaiComment.cmnt_date))

    rows, total = await _paginate(db, stmt, AlphaPaiComment, page, page_size)
    return PaginatedResponse(
        items=[CommentBrief.model_validate(r) for r in rows],
        total=total, page=page, page_size=page_size,
        has_next=(page * page_size) < total,
    )


@router.get("/comments/{cmnt_hcode}", response_model=CommentDetail)
async def get_comment(
    cmnt_hcode: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    row = await db.scalar(
        select(AlphaPaiComment).where(AlphaPaiComment.cmnt_hcode == cmnt_hcode)
    )
    if not row:
        raise HTTPException(404, "Comment not found")
    return CommentDetail.model_validate(row)


# ====================================================================== #
# Unified smart feed (filtered)
# ====================================================================== #
@router.get("/feed")
async def smart_feed(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    hours: int | None = Query(24),
    source_type: str | None = None,
    ticker: str | None = None,
    sector: str | None = None,
    min_relevance: float = Query(0.4, ge=0.0, le=1.0),
    sort_by: str = Query("time", pattern="^(time|relevance)$"),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Unified feed across all AlphaPai sources — only shows enriched, relevant, non-neutral items."""
    cutoff = _cutoff(hours)
    types = set(source_type.split(",")) if source_type else {"wechat", "roadshow_cn", "roadshow_us", "comment"}

    all_items: list[FeedItem] = []

    if "wechat" in types:
        stmt = select(AlphaPaiArticle).where(AlphaPaiArticle.is_enriched == True)  # noqa
        stmt = stmt.where(or_(
            AlphaPaiArticle.enrichment["skipped"].as_boolean().is_(False),
            AlphaPaiArticle.enrichment["skipped"].is_(None),
        ))
        # Exclude neutral sentiment (allow NULL sentiment for backward compat)
        stmt = stmt.where(or_(
            AlphaPaiArticle.enrichment["sentiment"].as_string() != "neutral",
            AlphaPaiArticle.enrichment["sentiment"].is_(None),
        ))
        if cutoff:
            stmt = stmt.where(AlphaPaiArticle.publish_time >= cutoff)
        if min_relevance > 0:
            stmt = stmt.where(AlphaPaiArticle.enrichment["relevance_score"].as_float() >= min_relevance)
        if ticker:
            stmt = stmt.where(AlphaPaiArticle.enrichment["tickers"].astext.ilike(f"%{_escape_like(ticker)}%"))
        if sector:
            stmt = stmt.where(AlphaPaiArticle.enrichment["sectors"].astext.ilike(f"%{_escape_like(sector)}%"))
        stmt = stmt.order_by(desc(AlphaPaiArticle.publish_time)).limit(100)
        for r in (await db.execute(stmt)).scalars():
            enr = r.enrichment or {}
            all_items.append(FeedItem(
                source_type="wechat", pk=r.arc_code, title=r.arc_name,
                time=r.publish_time, summary=enr.get("summary", ""),
                relevance_score=enr.get("relevance_score", 0),
                tags=enr.get("tags", []), tickers=enr.get("tickers", []),
                sectors=enr.get("sectors", []), sentiment=enr.get("sentiment", ""),
                author=r.author, url=r.url,
            ))

    if "roadshow_cn" in types:
        stmt = select(AlphaPaiRoadshowCN).where(AlphaPaiRoadshowCN.trans_source == "AI")
        stmt = stmt.where(AlphaPaiRoadshowCN.is_enriched == True)  # noqa
        # Exclude neutral sentiment
        stmt = stmt.where(or_(
            AlphaPaiRoadshowCN.enrichment["sentiment"].as_string() != "neutral",
            AlphaPaiRoadshowCN.enrichment["sentiment"].is_(None),
        ))
        if cutoff:
            stmt = stmt.where(AlphaPaiRoadshowCN.stime >= cutoff)
        stmt = stmt.order_by(desc(AlphaPaiRoadshowCN.stime)).limit(80)
        for r in (await db.execute(stmt)).scalars():
            enr = r.enrichment or {}
            industries = [i.get("name", "") for i in (r.ind_json or []) if isinstance(i, dict)]
            all_items.append(FeedItem(
                source_type="roadshow_cn", pk=r.trans_id, title=r.show_title,
                time=r.stime, summary=enr.get("summary", ""),
                relevance_score=enr.get("relevance_score", 0),
                tags=enr.get("tags", []) or industries,
                tickers=enr.get("tickers", []),
                sectors=enr.get("sectors", []) or industries,
                sentiment=enr.get("sentiment", ""),
                company=r.company, trans_source=r.trans_source,
            ))

    if "roadshow_us" in types:
        stmt = select(AlphaPaiRoadshowUS).where(AlphaPaiRoadshowUS.is_enriched == True)  # noqa
        # Exclude neutral sentiment
        stmt = stmt.where(or_(
            AlphaPaiRoadshowUS.enrichment["sentiment"].as_string() != "neutral",
            AlphaPaiRoadshowUS.enrichment["sentiment"].is_(None),
        ))
        if cutoff:
            stmt = stmt.where(AlphaPaiRoadshowUS.stime >= cutoff)
        stmt = stmt.order_by(desc(AlphaPaiRoadshowUS.stime)).limit(50)
        for r in (await db.execute(stmt)).scalars():
            enr = r.enrichment or {}
            aux = r.ai_auxiliary_json or {}
            summary = enr.get("summary", "") or aux.get("full_text_summary", "")
            all_items.append(FeedItem(
                source_type="roadshow_us", pk=r.trans_id, title=r.show_title,
                time=r.stime, summary=summary,
                relevance_score=enr.get("relevance_score", 0),
                tickers=enr.get("tickers", []), sectors=enr.get("sectors", []),
                sentiment=enr.get("sentiment", ""), company=r.company,
                trans_source=r.trans_source,
            ))

    if "comment" in types:
        stmt = select(AlphaPaiComment).where(AlphaPaiComment.is_enriched == True)  # noqa
        # Exclude neutral sentiment
        stmt = stmt.where(or_(
            AlphaPaiComment.enrichment["sentiment"].as_string() != "neutral",
            AlphaPaiComment.enrichment["sentiment"].is_(None),
        ))
        if cutoff:
            stmt = stmt.where(AlphaPaiComment.cmnt_date >= cutoff)
        if min_relevance > 0:
            stmt = stmt.where(AlphaPaiComment.enrichment["relevance_score"].as_float() >= min_relevance)
        if ticker:
            stmt = stmt.where(AlphaPaiComment.enrichment["tickers"].astext.ilike(f"%{_escape_like(ticker)}%"))
        stmt = stmt.order_by(desc(AlphaPaiComment.cmnt_date)).limit(100)
        for r in (await db.execute(stmt)).scalars():
            enr = r.enrichment or {}
            all_items.append(FeedItem(
                source_type="comment", pk=r.cmnt_hcode, title=r.title,
                time=r.cmnt_date, summary=enr.get("summary", ""),
                relevance_score=enr.get("relevance_score", 0),
                tags=enr.get("tags", []), tickers=enr.get("tickers", []),
                sectors=enr.get("sectors", []), sentiment=enr.get("sentiment", ""),
                institution=r.inst_cname, analyst=r.psn_name,
                is_new_fortune=r.is_new_fortune,
            ))

    # Sort
    if sort_by == "relevance":
        all_items.sort(key=lambda x: x.relevance_score, reverse=True)
    else:
        all_items.sort(key=lambda x: x.time or datetime.min.replace(tzinfo=timezone.utc), reverse=True)

    total = len(all_items)
    start = (page - 1) * page_size
    items = all_items[start: start + page_size]

    return PaginatedResponse(
        items=[item.model_dump() for item in items],
        total=total, page=page, page_size=page_size,
        has_next=(page * page_size) < total,
    )


# ====================================================================== #
# Digests
# ====================================================================== #
@router.get("/digests/latest", response_model=DigestResponse | None)
async def get_latest_digest(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    row = await db.scalar(
        select(AlphaPaiDigest).order_by(desc(AlphaPaiDigest.generated_at)).limit(1)
    )
    if not row:
        return None
    return DigestResponse.model_validate(row)


@router.get("/digests", response_model=list[DigestResponse])
async def list_digests(
    limit: int = Query(7, ge=1, le=30),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    rows = (await db.execute(
        select(AlphaPaiDigest).order_by(desc(AlphaPaiDigest.generated_at)).limit(limit)
    )).scalars().all()
    return [DigestResponse.model_validate(r) for r in rows]


@router.post("/digests/generate", status_code=202)
async def trigger_digest(
    user: User = Depends(get_current_admin),
):
    """Manually trigger digest generation."""
    from fastapi import Request
    # Will be handled by processor in next cycle
    return {"detail": "Digest generation triggered"}


# ====================================================================== #
# Stats & sync status
# ====================================================================== #
@router.get("/stats", response_model=StatsResponse)
async def get_stats(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    async def _count(model, time_col=None):
        total = await db.scalar(select(func.count()).select_from(model)) or 0
        today = 0
        if time_col is not None:
            today = await db.scalar(
                select(func.count()).select_from(model).where(time_col >= today_start)
            ) or 0
        return total, today

    art_t, art_d = await _count(AlphaPaiArticle, AlphaPaiArticle.synced_at)
    rs_cn_t, rs_cn_d = await _count(AlphaPaiRoadshowCN, AlphaPaiRoadshowCN.synced_at)
    rs_us_t, rs_us_d = await _count(AlphaPaiRoadshowUS, AlphaPaiRoadshowUS.synced_at)
    cmt_t, cmt_d = await _count(AlphaPaiComment, AlphaPaiComment.synced_at)

    enriched = 0
    for model in [AlphaPaiArticle, AlphaPaiRoadshowCN, AlphaPaiRoadshowUS, AlphaPaiComment]:
        enriched += await db.scalar(
            select(func.count()).select_from(model).where(model.is_enriched == True)  # noqa: E712
        ) or 0

    sync_rows = (await db.execute(select(AlphaPaiSyncState))).scalars().all()
    last_sync = None
    for sr in sync_rows:
        if last_sync is None or sr.updated_at > last_sync:
            last_sync = sr.updated_at

    return StatsResponse(
        articles_total=art_t, articles_today=art_d,
        roadshows_cn_total=rs_cn_t, roadshows_cn_today=rs_cn_d,
        roadshows_us_total=rs_us_t, roadshows_us_today=rs_us_d,
        comments_total=cmt_t, comments_today=cmt_d,
        enriched_total=enriched,
        last_sync_at=last_sync,
        sync_status=[SyncStatusItem.model_validate(r) for r in sync_rows],
    )


# ====================================================================== #
# Admin: manual sync trigger
# ====================================================================== #
@router.post("/sync/trigger", status_code=202)
async def trigger_sync(
    user: User = Depends(get_current_admin),
):
    """Trigger an immediate sync cycle."""
    return {"detail": "Sync cycle will run on next interval"}


# ====================================================================== #
# Admin: reset bad enrichments
# ====================================================================== #
@router.post("/enrichment/reset", status_code=200)
async def reset_bad_enrichments(
    model_type: str = Query(..., pattern="^(articles|roadshows_cn|roadshows_us|comments|all)$"),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_admin),
):
    """Reset enrichment for items with relevance_score = 0 so they get re-processed."""
    counts = {}
    models = {
        "articles": AlphaPaiArticle,
        "roadshows_cn": AlphaPaiRoadshowCN,
        "roadshows_us": AlphaPaiRoadshowUS,
        "comments": AlphaPaiComment,
    }
    targets = models if model_type == "all" else {model_type: models[model_type]}

    for name, model in targets.items():
        rows = (await db.execute(
            select(model)
            .where(model.is_enriched == True)  # noqa: E712
            .where(model.enrichment["relevance_score"].as_float() == 0.0)
        )).scalars().all()
        for row in rows:
            row.is_enriched = False
            row.enrichment = {}
        counts[name] = len(rows)

    await db.commit()
    return {"reset_counts": counts}
