"""Favorites / Bookmarks API: add, remove, list, and check favorites."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import and_, delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.deps import get_current_user, get_db
from backend.app.models.alphapai import (
    AlphaPaiArticle,
    AlphaPaiComment,
    AlphaPaiRoadshowCN,
    AlphaPaiRoadshowUS,
)
from backend.app.models.news import AnalysisResult, NewsItem
from backend.app.models.user import User
from backend.app.models.user_preference import UserFavorite
from backend.app.schemas.favorites import (
    FavoriteCheckResponse,
    FavoriteCreate,
    FavoriteIdsResponse,
    FavoriteListResponse,
    FavoriteResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter()

VALID_ITEM_TYPES = {"news", "wechat", "roadshow_cn", "roadshow_us", "comment"}


@router.post("", response_model=FavoriteResponse, status_code=status.HTTP_201_CREATED)
async def add_favorite(
    body: FavoriteCreate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Add a new favorite / bookmark."""
    # Check for duplicate
    existing = await db.scalar(
        select(UserFavorite).where(
            and_(
                UserFavorite.user_id == user.id,
                UserFavorite.item_type == body.item_type,
                UserFavorite.item_id == body.item_id,
            )
        )
    )
    if existing:
        raise HTTPException(status_code=409, detail="Item already favorited")

    fav = UserFavorite(
        user_id=user.id,
        item_type=body.item_type,
        item_id=body.item_id,
        note=body.note,
    )
    db.add(fav)
    await db.commit()
    await db.refresh(fav)

    return FavoriteResponse(
        id=fav.id,
        item_type=fav.item_type,
        item_id=fav.item_id,
        note=fav.note,
        created_at=fav.created_at,
    )


@router.delete("/{favorite_id}", status_code=204)
async def remove_favorite_by_id(
    favorite_id: int,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Remove a favorite by its ID."""
    fav = await db.scalar(
        select(UserFavorite).where(
            and_(UserFavorite.id == favorite_id, UserFavorite.user_id == user.id)
        )
    )
    if not fav:
        raise HTTPException(status_code=404, detail="Favorite not found")

    await db.delete(fav)
    await db.commit()


@router.delete("", status_code=204)
async def remove_favorite_by_item(
    item_type: str = Query(..., description="Type of the favorited item"),
    item_id: str = Query(..., description="ID of the favorited item"),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Remove a favorite by item_type + item_id."""
    fav = await db.scalar(
        select(UserFavorite).where(
            and_(
                UserFavorite.user_id == user.id,
                UserFavorite.item_type == item_type,
                UserFavorite.item_id == item_id,
            )
        )
    )
    if not fav:
        raise HTTPException(status_code=404, detail="Favorite not found")

    await db.delete(fav)
    await db.commit()


@router.get("", response_model=FavoriteListResponse)
async def list_favorites(
    item_type: str | None = Query(None, description="Filter by item type"),
    offset: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """List the current user's favorites with pagination."""
    conditions = [UserFavorite.user_id == user.id]
    if item_type:
        conditions.append(UserFavorite.item_type == item_type)

    where_clause = and_(*conditions)

    # Total count
    total = await db.scalar(select(func.count()).select_from(UserFavorite).where(where_clause))

    # Fetch page
    stmt = (
        select(UserFavorite)
        .where(where_clause)
        .order_by(UserFavorite.created_at.desc())
        .offset(offset)
        .limit(limit)
    )
    result = await db.execute(stmt)
    favs = result.scalars().all()

    # Enrich with titles from source tables
    enriched = []
    for f in favs:
        title = await _resolve_title(db, f.item_type, f.item_id)
        enriched.append(
            FavoriteResponse(
                id=f.id,
                item_type=f.item_type,
                item_id=f.item_id,
                note=f.note,
                title=title,
                created_at=f.created_at,
            )
        )

    return FavoriteListResponse(favorites=enriched, total=total or 0)


async def _resolve_title(db: AsyncSession, item_type: str, item_id: str) -> str | None:
    """Look up the display title for a favorited item."""
    try:
        if item_type == "news":
            news = await db.scalar(select(NewsItem).where(NewsItem.id == item_id))
            if news:
                title_zh = (news.metadata_ or {}).get("title_zh")
                return title_zh or news.title
        elif item_type == "wechat":
            art = await db.scalar(select(AlphaPaiArticle).where(AlphaPaiArticle.arc_code == item_id))
            if art:
                return art.arc_name
        elif item_type == "roadshow_cn":
            rs = await db.scalar(select(AlphaPaiRoadshowCN).where(AlphaPaiRoadshowCN.trans_id == item_id))
            if rs:
                return rs.show_title
        elif item_type == "roadshow_us":
            rs = await db.scalar(select(AlphaPaiRoadshowUS).where(AlphaPaiRoadshowUS.trans_id == item_id))
            if rs:
                return rs.show_title
        elif item_type == "comment":
            cmt = await db.scalar(select(AlphaPaiComment).where(AlphaPaiComment.cmnt_hcode == item_id))
            if cmt:
                return cmt.title
    except Exception as e:
        logger.debug("Failed to resolve title for %s/%s: %s", item_type, item_id, e)
    return None


@router.get("/check", response_model=FavoriteCheckResponse)
async def check_favorite(
    item_type: str = Query(..., description="Type of the item"),
    item_id: str = Query(..., description="ID of the item"),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Check if a specific item is favorited by the current user."""
    fav = await db.scalar(
        select(UserFavorite).where(
            and_(
                UserFavorite.user_id == user.id,
                UserFavorite.item_type == item_type,
                UserFavorite.item_id == item_id,
            )
        )
    )
    if fav:
        return FavoriteCheckResponse(is_favorited=True, favorite_id=fav.id)
    return FavoriteCheckResponse(is_favorited=False, favorite_id=None)


@router.get("/ids", response_model=FavoriteIdsResponse)
async def get_favorite_ids(
    item_type: str = Query(..., description="Type of the item"),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Get all favorited item IDs for a given type (for quick UI checks)."""
    stmt = (
        select(UserFavorite.item_id)
        .where(
            and_(
                UserFavorite.user_id == user.id,
                UserFavorite.item_type == item_type,
            )
        )
        .order_by(UserFavorite.created_at.desc())
    )
    result = await db.execute(stmt)
    ids = result.scalars().all()
    return FavoriteIdsResponse(item_ids=list(ids))
