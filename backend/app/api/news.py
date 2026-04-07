"""News feed API: list, detail, search, stats, categories."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import and_, desc, func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession


def _escape_like(value: str) -> str:
    """Escape special LIKE/ILIKE characters to prevent wildcard injection."""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")

from backend.app.deps import get_current_user, get_db
from backend.app.models.news import AnalysisResult, FilterResult, NewsItem, ResearchReport

# Hot news sources belong exclusively to 舆情雷达 (Topic Radar), not 资讯中心 (News Center).
_HOT_NEWS_SOURCES = ("华尔街见闻热点", "财联社热点", "雪球热榜", "微博热搜")
from backend.app.models.user import User
from backend.app.models.user_preference import UserNewsRead
from backend.app.schemas.news import (
    AnalysisResultDetail,
    FilterResultDetail,
    NewsItemBrief,
    NewsItemDetail,
    NewsListResponse,
    NewsStatsResponse,
    ResearchReportDetail,
)

router = APIRouter()


def _apply_quality_filter(base, user: User, unfiltered: bool):
    """Apply role-based quality filtering.

    Regular users (trader/boss/viewer) only see news that:
    - Has been analyzed (AnalysisResult exists)
    - Is flagged as relevant (FilterResult.is_relevant = True)
    - Has a bullish or bearish sentiment signal (not neutral)
    - Is NOT from hot news sources (those belong to 舆情雷达 only)

    Admins can see everything (unfiltered master feed).
    """
    # Always exclude hot news sources from News Center — they belong to 舆情雷达
    base = base.where(NewsItem.source_name.notin_(_HOT_NEWS_SOURCES))

    if user.role == "admin" and unfiltered:
        return base

    # For non-admin users (or admin without unfiltered flag),
    # only show quality-filtered news
    base = base.where(
        and_(
            FilterResult.is_relevant.is_(True),
            AnalysisResult.sentiment.isnot(None),
            AnalysisResult.sentiment.notin_(["neutral"]),
        )
    )
    return base


@router.get("", response_model=NewsListResponse)
async def list_news(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    sentiment: str | None = None,
    impact: str | None = None,
    sector: str | None = None,
    ticker: str | None = None,
    source: str | None = None,
    category: str | None = None,
    hours: int | None = None,
    unfiltered: bool = Query(False, description="Admin only: show all news including unprocessed"),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Get paginated news feed with optional filters."""
    base = (
        select(NewsItem)
        .outerjoin(AnalysisResult, NewsItem.id == AnalysisResult.news_item_id)
        .outerjoin(FilterResult, NewsItem.id == FilterResult.news_item_id)
    )

    # Apply role-based quality filtering
    base = _apply_quality_filter(base, user, unfiltered)

    conditions = []
    if sentiment:
        conditions.append(AnalysisResult.sentiment == sentiment)
    if impact:
        conditions.append(AnalysisResult.impact_magnitude == impact)
    if sector:
        conditions.append(AnalysisResult.affected_sectors.op("@>")(f'["{sector}"]'))
    if ticker:
        safe_ticker = _escape_like(ticker)
        conditions.append(
            or_(
                AnalysisResult.affected_tickers.op("@>")(f'["{ticker}"]'),
                NewsItem.title.ilike(f"%{safe_ticker}%"),
            )
        )
    if source:
        conditions.append(NewsItem.source_name == source)
    if category:
        conditions.append(AnalysisResult.category == category)
    if hours:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        conditions.append(NewsItem.fetched_at >= cutoff)

    if conditions:
        base = base.where(and_(*conditions))

    # Count total
    count_stmt = select(func.count()).select_from(base.subquery())
    total = await db.scalar(count_stmt) or 0

    # Fetch page
    stmt = (
        base.order_by(desc(NewsItem.fetched_at))
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    result = await db.execute(stmt)
    news_items = result.scalars().unique().all()

    # Get read states for this user
    if news_items:
        item_ids = [n.id for n in news_items]
        read_stmt = select(UserNewsRead.news_item_id).where(
            and_(UserNewsRead.user_id == user.id, UserNewsRead.news_item_id.in_(item_ids))
        )
        read_result = await db.execute(read_stmt)
        read_ids = {r[0] for r in read_result}
    else:
        read_ids = set()

    # Build response
    items = []
    for n in news_items:
        # Fetch analysis for this item
        analysis = await db.scalar(
            select(AnalysisResult).where(AnalysisResult.news_item_id == n.id)
        )
        filter_res = await db.scalar(
            select(FilterResult).where(FilterResult.news_item_id == n.id)
        )
        items.append(
            NewsItemBrief(
                id=n.id,
                source_name=n.source_name,
                title=n.title,
                title_zh=(n.metadata_ or {}).get("title_zh"),
                url=n.url,
                published_at=n.published_at,
                fetched_at=n.fetched_at,
                language=n.language,
                market=n.market,
                sentiment=analysis.sentiment if analysis else None,
                impact_magnitude=analysis.impact_magnitude if analysis else None,
                surprise_factor=analysis.surprise_factor if analysis else None,
                affected_tickers=analysis.affected_tickers if analysis else [],
                affected_sectors=analysis.affected_sectors if analysis else [],
                summary=analysis.summary if analysis else None,
                category=analysis.category if analysis else None,
                concept_tags=analysis.concept_tags if analysis and analysis.concept_tags else [],
                industry_tags=analysis.industry_tags if analysis and analysis.industry_tags else [],
                ticker_sentiments=analysis.ticker_sentiments if analysis and analysis.ticker_sentiments else {},
                sector_sentiments=analysis.sector_sentiments if analysis and analysis.sector_sentiments else {},
                is_read=n.id in read_ids,
                time_type="published" if n.published_at else "crawled",
                is_relevant=filter_res.is_relevant if filter_res else None,
                has_analysis=analysis is not None,
            )
        )

    return NewsListResponse(
        items=items,
        total=total,
        page=page,
        page_size=page_size,
        has_next=(page * page_size) < total,
    )


@router.get("/search", response_model=NewsListResponse)
async def search_news(
    q: str = Query(min_length=1, max_length=500),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Full-text search across news titles and content."""
    like_q = f"%{_escape_like(q)}%"
    base = (
        select(NewsItem)
        .outerjoin(AnalysisResult, NewsItem.id == AnalysisResult.news_item_id)
        .outerjoin(FilterResult, NewsItem.id == FilterResult.news_item_id)
        .where(or_(NewsItem.title.ilike(like_q), NewsItem.content.ilike(like_q)))
    )

    # Apply quality filter for search too (non-admins see only relevant results)
    base = _apply_quality_filter(base, user, unfiltered=False)

    count_stmt = select(func.count()).select_from(base.subquery())
    total = await db.scalar(count_stmt) or 0

    stmt = base.order_by(desc(NewsItem.fetched_at)).offset((page - 1) * page_size).limit(page_size)
    result = await db.execute(stmt)
    news_items = result.scalars().unique().all()

    items = []
    for n in news_items:
        analysis = await db.scalar(
            select(AnalysisResult).where(AnalysisResult.news_item_id == n.id)
        )
        items.append(
            NewsItemBrief(
                id=n.id,
                source_name=n.source_name,
                title=n.title,
                title_zh=(n.metadata_ or {}).get("title_zh"),
                url=n.url,
                published_at=n.published_at,
                fetched_at=n.fetched_at,
                language=n.language,
                market=n.market,
                sentiment=analysis.sentiment if analysis else None,
                impact_magnitude=analysis.impact_magnitude if analysis else None,
                surprise_factor=analysis.surprise_factor if analysis else None,
                affected_tickers=analysis.affected_tickers if analysis else [],
                affected_sectors=analysis.affected_sectors if analysis else [],
                summary=analysis.summary if analysis else None,
                category=analysis.category if analysis else None,
                concept_tags=analysis.concept_tags if analysis and analysis.concept_tags else [],
                industry_tags=analysis.industry_tags if analysis and analysis.industry_tags else [],
                ticker_sentiments=analysis.ticker_sentiments if analysis and analysis.ticker_sentiments else {},
                sector_sentiments=analysis.sector_sentiments if analysis and analysis.sector_sentiments else {},
                is_read=False,
                time_type="published" if n.published_at else "crawled",
                has_analysis=analysis is not None,
            )
        )

    return NewsListResponse(
        items=items, total=total, page=page, page_size=page_size,
        has_next=(page * page_size) < total,
    )


@router.get("/categories")
async def get_categories(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Get distinct news categories from analyzed items."""
    result = await db.execute(
        select(AnalysisResult.category, func.count())
        .group_by(AnalysisResult.category)
        .order_by(desc(func.count()))
    )
    return [{"name": row[0], "count": row[1]} for row in result if row[0]]


@router.get("/sectors")
async def get_sectors(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Get distinct affected sectors from analyzed items."""
    # Use raw SQL for JSONB array unnesting
    result = await db.execute(
        text("""
            SELECT sector, COUNT(*) as cnt
            FROM analysis_results, jsonb_array_elements_text(affected_sectors) AS sector
            GROUP BY sector
            ORDER BY cnt DESC
            LIMIT 50
        """)
    )
    return [{"name": row[0], "count": row[1]} for row in result]


@router.get("/stats", response_model=NewsStatsResponse)
async def get_news_stats(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Aggregate news statistics for the dashboard."""
    now = datetime.now(timezone.utc)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    week_start = today_start - timedelta(days=7)

    # Total today
    total_today = await db.scalar(
        select(func.count()).select_from(NewsItem).where(NewsItem.fetched_at >= today_start)
    ) or 0

    # Total this week
    total_week = await db.scalar(
        select(func.count()).select_from(NewsItem).where(NewsItem.fetched_at >= week_start)
    ) or 0

    # Analyzed today
    analyzed_today = await db.scalar(
        select(func.count()).select_from(AnalysisResult)
        .join(NewsItem, NewsItem.id == AnalysisResult.news_item_id)
        .where(NewsItem.fetched_at >= today_start)
    ) or 0

    # Sentiment distribution (last 24h)
    sentiment_rows = await db.execute(
        select(AnalysisResult.sentiment, func.count())
        .join(NewsItem, NewsItem.id == AnalysisResult.news_item_id)
        .where(NewsItem.fetched_at >= now - timedelta(hours=24))
        .group_by(AnalysisResult.sentiment)
    )
    sentiment_distribution = {row[0]: row[1] for row in sentiment_rows}

    # Impact distribution (last 24h)
    impact_rows = await db.execute(
        select(AnalysisResult.impact_magnitude, func.count())
        .join(NewsItem, NewsItem.id == AnalysisResult.news_item_id)
        .where(NewsItem.fetched_at >= now - timedelta(hours=24))
        .group_by(AnalysisResult.impact_magnitude)
    )
    impact_distribution = {row[0]: row[1] for row in impact_rows}

    return NewsStatsResponse(
        total_today=total_today,
        total_week=total_week,
        analyzed_today=analyzed_today,
        sentiment_distribution=sentiment_distribution,
        impact_distribution=impact_distribution,
    )


@router.get("/{news_id}", response_model=NewsItemDetail)
async def get_news_detail(
    news_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Get full news item with all analysis phases."""
    news = await db.scalar(select(NewsItem).where(NewsItem.id == news_id))
    if not news:
        raise HTTPException(status_code=404, detail="News item not found")

    filter_result = await db.scalar(
        select(FilterResult).where(FilterResult.news_item_id == news_id)
    )
    analysis = await db.scalar(
        select(AnalysisResult).where(AnalysisResult.news_item_id == news_id)
    )
    research = await db.scalar(
        select(ResearchReport).where(ResearchReport.news_item_id == news_id)
    )

    return NewsItemDetail(
        id=news.id,
        source_name=news.source_name,
        title=news.title,
        url=news.url,
        content=news.content,
        published_at=news.published_at,
        fetched_at=news.fetched_at,
        language=news.language,
        market=news.market,
        metadata=news.metadata_ or {},
        filter_result=FilterResultDetail.model_validate(filter_result) if filter_result else None,
        analysis=AnalysisResultDetail.model_validate(analysis) if analysis else None,
        research=ResearchReportDetail.model_validate(research) if research else None,
        time_type="published" if news.published_at else "crawled",
    )


@router.post("/{news_id}/read", status_code=204)
async def mark_as_read(
    news_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Mark a news item as read for the current user."""
    # Check news exists
    exists = await db.scalar(select(NewsItem.id).where(NewsItem.id == news_id))
    if not exists:
        raise HTTPException(status_code=404, detail="News item not found")

    # Upsert read state
    existing = await db.scalar(
        select(UserNewsRead).where(
            and_(UserNewsRead.user_id == user.id, UserNewsRead.news_item_id == news_id)
        )
    )
    if not existing:
        db.add(UserNewsRead(user_id=user.id, news_item_id=news_id))
        await db.commit()
