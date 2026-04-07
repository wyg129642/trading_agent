"""REST API routes for Jiuqian (久谦) data — forum, minutes, wechat.

Quality thresholds:
- Forum: min_relevance=0.3 (high-value expert calls, nearly all pass)
- Minutes: min_relevance=0.4
- WeChat: min_relevance=0.6 (strict filtering for 公众号 noise)
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, func, desc, or_
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.deps import get_db, get_current_user
from backend.app.models.jiuqian import (
    JiuqianForum,
    JiuqianMinutes,
    JiuqianSyncState,
    JiuqianWechat,
)
from backend.app.models.user import User
from backend.app.schemas.alphapai import PaginatedResponse
from backend.app.schemas.jiuqian import (
    ForumBrief,
    ForumDetail,
    JiuqianStatsResponse,
    MinutesBrief,
    MinutesDetail,
    WechatBrief,
    WechatDetail,
)

logger = logging.getLogger(__name__)
router = APIRouter()


def _escape_like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _cutoff(hours: int | None) -> datetime | None:
    if hours is None:
        return None
    return datetime.now(timezone.utc) - timedelta(hours=hours)


async def _paginate(db: AsyncSession, base_stmt, page: int, page_size: int):
    count_stmt = select(func.count()).select_from(base_stmt.subquery())
    total = await db.scalar(count_stmt) or 0
    offset = (page - 1) * page_size
    rows = (await db.execute(base_stmt.offset(offset).limit(page_size))).scalars().all()
    return rows, total


# ====================================================================== #
# Forum expert calls — low threshold, high value
# ====================================================================== #
@router.get("/forum", response_model=PaginatedResponse)
async def list_forum(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    hours: int | None = Query(None),  # No time limit by default (few records)
    industry: str | None = None,
    ticker: str | None = None,
    min_relevance: float = Query(0.3, ge=0.0, le=1.0),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    stmt = select(JiuqianForum)
    cutoff = _cutoff(hours)
    if cutoff:
        stmt = stmt.where(JiuqianForum.meeting_time >= cutoff)

    # Only show enriched, non-neutral items by default
    stmt = stmt.where(JiuqianForum.is_enriched == True)  # noqa: E712
    stmt = stmt.where(or_(
        JiuqianForum.enrichment["sentiment"].as_string() != "neutral",
        JiuqianForum.enrichment["sentiment"].is_(None),
    ))
    if min_relevance > 0:
        stmt = stmt.where(
            JiuqianForum.enrichment["relevance_score"].as_float() >= min_relevance
        )

    if industry:
        stmt = stmt.where(JiuqianForum.industry.ilike(f"%{_escape_like(industry)}%"))
    if ticker:
        stmt = stmt.where(
            JiuqianForum.enrichment["tickers"].astext.ilike(f"%{_escape_like(ticker)}%")
        )

    stmt = stmt.order_by(desc(JiuqianForum.meeting_time))
    rows, total = await _paginate(db, stmt, page, page_size)
    return PaginatedResponse(
        items=[ForumBrief.model_validate(r) for r in rows],
        total=total, page=page, page_size=page_size,
        has_next=(page * page_size) < total,
    )


@router.get("/forum/{forum_id}", response_model=ForumDetail)
async def get_forum(
    forum_id: int,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    row = await db.scalar(select(JiuqianForum).where(JiuqianForum.id == forum_id))
    if not row:
        raise HTTPException(404, "Forum item not found")
    return ForumDetail.model_validate(row)


# ====================================================================== #
# Research minutes — moderate threshold
# ====================================================================== #
@router.get("/minutes", response_model=PaginatedResponse)
async def list_minutes(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    hours: int | None = Query(48),
    source: str | None = None,
    ticker: str | None = None,
    sector: str | None = None,
    min_relevance: float = Query(0.4, ge=0.0, le=1.0),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    stmt = select(JiuqianMinutes)
    cutoff = _cutoff(hours)
    if cutoff:
        stmt = stmt.where(JiuqianMinutes.pub_time >= cutoff)

    stmt = stmt.where(JiuqianMinutes.is_enriched == True)  # noqa: E712
    stmt = stmt.where(or_(
        JiuqianMinutes.enrichment["sentiment"].as_string() != "neutral",
        JiuqianMinutes.enrichment["sentiment"].is_(None),
    ))
    if min_relevance > 0:
        stmt = stmt.where(
            JiuqianMinutes.enrichment["relevance_score"].as_float() >= min_relevance
        )

    if source:
        stmt = stmt.where(JiuqianMinutes.source.ilike(f"%{_escape_like(source)}%"))
    if ticker:
        stmt = stmt.where(
            JiuqianMinutes.enrichment["tickers"].astext.ilike(f"%{_escape_like(ticker)}%")
        )
    if sector:
        stmt = stmt.where(
            JiuqianMinutes.enrichment["sectors"].astext.ilike(f"%{_escape_like(sector)}%")
        )

    stmt = stmt.order_by(desc(JiuqianMinutes.pub_time))
    rows, total = await _paginate(db, stmt, page, page_size)
    return PaginatedResponse(
        items=[MinutesBrief.model_validate(r) for r in rows],
        total=total, page=page, page_size=page_size,
        has_next=(page * page_size) < total,
    )


@router.get("/minutes/{minutes_id}", response_model=MinutesDetail)
async def get_minutes(
    minutes_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    row = await db.scalar(select(JiuqianMinutes).where(JiuqianMinutes.id == minutes_id))
    if not row:
        raise HTTPException(404, "Minutes item not found")
    return MinutesDetail.model_validate(row)


# ====================================================================== #
# WeChat articles — strictest threshold (0.6)
# ====================================================================== #
@router.get("/wechat", response_model=PaginatedResponse)
async def list_wechat(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    hours: int | None = Query(48),
    source: str | None = None,
    ticker: str | None = None,
    sector: str | None = None,
    min_relevance: float = Query(0.6, ge=0.0, le=1.0),
    show_all: bool = False,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    stmt = select(JiuqianWechat)
    cutoff = _cutoff(hours)
    if cutoff:
        stmt = stmt.where(JiuqianWechat.pub_time >= cutoff)

    if not show_all:
        stmt = stmt.where(JiuqianWechat.is_enriched == True)  # noqa: E712
        stmt = stmt.where(or_(
            JiuqianWechat.enrichment["skipped"].as_boolean().is_(False),
            JiuqianWechat.enrichment["skipped"].is_(None),
        ))
        stmt = stmt.where(or_(
            JiuqianWechat.enrichment["sentiment"].as_string() != "neutral",
            JiuqianWechat.enrichment["sentiment"].is_(None),
        ))
        stmt = stmt.where(
            JiuqianWechat.enrichment["relevance_score"].as_float() >= min_relevance
        )

    if source:
        stmt = stmt.where(JiuqianWechat.source.ilike(f"%{_escape_like(source)}%"))
    if ticker:
        stmt = stmt.where(
            JiuqianWechat.enrichment["tickers"].astext.ilike(f"%{_escape_like(ticker)}%")
        )
    if sector:
        stmt = stmt.where(
            JiuqianWechat.enrichment["sectors"].astext.ilike(f"%{_escape_like(sector)}%")
        )

    stmt = stmt.order_by(desc(JiuqianWechat.pub_time))
    rows, total = await _paginate(db, stmt, page, page_size)
    return PaginatedResponse(
        items=[WechatBrief.model_validate(r) for r in rows],
        total=total, page=page, page_size=page_size,
        has_next=(page * page_size) < total,
    )


@router.get("/wechat/{wechat_id}", response_model=WechatDetail)
async def get_wechat(
    wechat_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    row = await db.scalar(select(JiuqianWechat).where(JiuqianWechat.id == wechat_id))
    if not row:
        raise HTTPException(404, "WeChat article not found")
    return WechatDetail.model_validate(row)


# ====================================================================== #
# Stats
# ====================================================================== #
@router.get("/stats", response_model=JiuqianStatsResponse)
async def get_stats(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    cutoff_48h = datetime.now(timezone.utc) - timedelta(hours=48)

    forum_total = await db.scalar(select(func.count()).select_from(JiuqianForum)) or 0
    forum_recent = await db.scalar(
        select(func.count()).select_from(JiuqianForum).where(JiuqianForum.meeting_time >= cutoff_48h)
    ) or 0

    minutes_total = await db.scalar(select(func.count()).select_from(JiuqianMinutes)) or 0
    minutes_recent = await db.scalar(
        select(func.count()).select_from(JiuqianMinutes).where(JiuqianMinutes.pub_time >= cutoff_48h)
    ) or 0

    wechat_total = await db.scalar(select(func.count()).select_from(JiuqianWechat)) or 0
    wechat_recent = await db.scalar(
        select(func.count()).select_from(JiuqianWechat).where(JiuqianWechat.pub_time >= cutoff_48h)
    ) or 0

    enriched = 0
    for model in [JiuqianForum, JiuqianMinutes, JiuqianWechat]:
        enriched += await db.scalar(
            select(func.count()).select_from(model).where(model.is_enriched == True)  # noqa
        ) or 0

    sync_rows = (await db.execute(select(JiuqianSyncState))).scalars().all()
    last_sync = None
    for sr in sync_rows:
        if last_sync is None or sr.last_sync_time > last_sync:
            last_sync = sr.last_sync_time

    return JiuqianStatsResponse(
        forum_total=forum_total, forum_recent=forum_recent,
        minutes_total=minutes_total, minutes_recent=minutes_recent,
        wechat_total=wechat_total, wechat_recent=wechat_recent,
        enriched_total=enriched, last_sync_at=last_sync,
    )
