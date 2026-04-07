"""Analytics API: token usage, pipeline stats, ticker analytics."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.deps import get_current_user, get_db
from backend.app.models.news import AnalysisResult, NewsItem
from backend.app.models.token_usage import TokenUsage
from backend.app.models.user import User
from backend.app.schemas.analytics import (
    PipelineStats,
    TickerAnalytics,
    TickerSentimentPoint,
    TokenUsageStats,
)

router = APIRouter()


@router.get("/token-usage", response_model=TokenUsageStats)
async def get_token_usage(
    days: int = Query(1, ge=1, le=365),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Get token usage statistics for the last N days."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    # Totals
    totals = await db.execute(
        select(
            func.count(),
            func.coalesce(func.sum(TokenUsage.prompt_tokens), 0),
            func.coalesce(func.sum(TokenUsage.completion_tokens), 0),
            func.coalesce(func.sum(TokenUsage.cost_cny), 0),
        ).where(TokenUsage.timestamp >= cutoff)
    )
    row = totals.one()

    # By stage
    stage_rows = await db.execute(
        select(
            TokenUsage.stage,
            func.count(),
            func.sum(TokenUsage.prompt_tokens),
            func.sum(TokenUsage.completion_tokens),
            func.sum(TokenUsage.cost_cny),
        )
        .where(TokenUsage.timestamp >= cutoff)
        .group_by(TokenUsage.stage)
    )
    by_stage = {}
    for r in stage_rows:
        by_stage[r[0]] = {
            "calls": r[1],
            "prompt_tokens": r[2] or 0,
            "completion_tokens": r[3] or 0,
            "cost_cny": round(float(r[4] or 0), 4),
        }

    # By model
    model_rows = await db.execute(
        select(
            TokenUsage.model,
            func.count(),
            func.sum(TokenUsage.prompt_tokens),
            func.sum(TokenUsage.completion_tokens),
            func.sum(TokenUsage.cost_cny),
        )
        .where(TokenUsage.timestamp >= cutoff)
        .group_by(TokenUsage.model)
    )
    by_model = {}
    for r in model_rows:
        by_model[r[0]] = {
            "calls": r[1],
            "prompt_tokens": r[2] or 0,
            "completion_tokens": r[3] or 0,
            "cost_cny": round(float(r[4] or 0), 4),
        }

    # Daily trend
    daily_rows = await db.execute(
        select(
            func.date_trunc("day", TokenUsage.timestamp).label("day"),
            func.count(),
            func.sum(TokenUsage.cost_cny),
        )
        .where(TokenUsage.timestamp >= cutoff)
        .group_by("day")
        .order_by("day")
    )
    daily_trend = [
        {"date": r[0].isoformat(), "calls": r[1], "cost_cny": round(float(r[2] or 0), 4)}
        for r in daily_rows
    ]

    return TokenUsageStats(
        total_calls=row[0],
        total_prompt_tokens=row[1],
        total_completion_tokens=row[2],
        total_tokens=row[1] + row[2],
        total_cost_cny=round(float(row[3]), 4),
        by_stage=by_stage,
        by_model=by_model,
        daily_trend=daily_trend,
    )


@router.get("/pipeline", response_model=PipelineStats)
async def get_pipeline_stats(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Get pipeline processing statistics."""
    now = datetime.now(timezone.utc)
    day_ago = now - timedelta(hours=24)

    total_processed = await db.scalar(
        select(func.count()).select_from(NewsItem).where(NewsItem.fetched_at >= day_ago)
    ) or 0

    total_analyzed = await db.scalar(
        select(func.count())
        .select_from(AnalysisResult)
        .join(NewsItem, NewsItem.id == AnalysisResult.news_item_id)
        .where(NewsItem.fetched_at >= day_ago)
    ) or 0

    pass_rate = (total_analyzed / total_processed * 100) if total_processed > 0 else 0.0

    return PipelineStats(
        total_processed=total_processed,
        pass_rate_phase1=round(pass_rate, 1),
    )


@router.get("/ticker/{ticker}", response_model=TickerAnalytics)
async def get_ticker_analytics(
    ticker: str,
    days: int = Query(30, ge=1, le=365),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Get analytics for a specific ticker."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    # Find all news mentioning this ticker
    stmt = (
        select(NewsItem, AnalysisResult)
        .join(AnalysisResult, NewsItem.id == AnalysisResult.news_item_id)
        .where(
            NewsItem.fetched_at >= cutoff,
            AnalysisResult.affected_tickers.op("@>")(f'["{ticker}"]'),
        )
        .order_by(NewsItem.fetched_at)
    )
    result = await db.execute(stmt)
    rows = result.all()

    # Sentiment mapping for numeric trend
    sentiment_map = {
        "very_bullish": 1.0,
        "bullish": 0.5,
        "neutral": 0.0,
        "bearish": -0.5,
        "very_bearish": -1.0,
    }

    # Group by date
    daily: dict[str, list[float]] = {}
    recent_news = []
    for news, analysis in rows:
        date_str = news.fetched_at.strftime("%Y-%m-%d")
        score = sentiment_map.get(analysis.sentiment, 0.0)
        daily.setdefault(date_str, []).append(score)

        if len(recent_news) < 20:
            recent_news.append({
                "id": news.id,
                "title": news.title,
                "sentiment": analysis.sentiment,
                "impact": analysis.impact_magnitude,
                "date": news.fetched_at.isoformat(),
            })

    sentiment_trend = [
        TickerSentimentPoint(
            date=d,
            sentiment_score=round(sum(scores) / len(scores), 2),
            news_count=len(scores),
        )
        for d, scores in sorted(daily.items())
    ]

    return TickerAnalytics(
        ticker=ticker,
        display_name=ticker,
        total_mentions=len(rows),
        sentiment_trend=sentiment_trend,
        recent_news=recent_news,
    )
