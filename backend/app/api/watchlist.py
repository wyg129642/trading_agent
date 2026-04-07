"""Watchlist API: CRUD watchlists and items."""

from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException, status


def _parse_uuid(value: str, label: str = "ID") -> uuid.UUID:
    """Parse a UUID string, raising 400 on invalid format."""
    try:
        return uuid.UUID(value)
    except (ValueError, AttributeError):
        raise HTTPException(status_code=400, detail=f"Invalid {label}: {value}")
from sqlalchemy import and_, delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from backend.app.deps import get_current_user, get_db
from backend.app.models.user import User
from backend.app.models.watchlist import Watchlist, WatchlistItem
from backend.app.schemas.watchlist import (
    WatchlistCreate,
    WatchlistItemCreate,
    WatchlistItemResponse,
    WatchlistListResponse,
    WatchlistResponse,
    WatchlistUpdate,
)

router = APIRouter()


@router.get("", response_model=WatchlistListResponse)
async def list_watchlists(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """List all watchlists for the current user."""
    stmt = (
        select(Watchlist)
        .options(selectinload(Watchlist.items))
        .where(Watchlist.user_id == user.id)
        .order_by(Watchlist.created_at)
    )
    result = await db.execute(stmt)
    watchlists = result.scalars().unique().all()

    return WatchlistListResponse(
        watchlists=[
            WatchlistResponse(
                id=str(w.id),
                name=w.name,
                description=w.description,
                is_default=w.is_default,
                created_at=w.created_at,
                items=[
                    WatchlistItemResponse(
                        id=str(item.id),
                        item_type=item.item_type,
                        value=item.value,
                        display_name=item.display_name,
                        metadata=item.metadata_ or {},
                        added_at=item.added_at,
                    )
                    for item in w.items
                ],
                item_count=len(w.items),
            )
            for w in watchlists
        ]
    )


@router.post("", response_model=WatchlistResponse, status_code=status.HTTP_201_CREATED)
async def create_watchlist(
    body: WatchlistCreate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Create a new watchlist."""
    watchlist = Watchlist(
        user_id=user.id,
        name=body.name,
        description=body.description,
    )
    db.add(watchlist)
    await db.commit()
    await db.refresh(watchlist)

    return WatchlistResponse(
        id=str(watchlist.id),
        name=watchlist.name,
        description=watchlist.description,
        is_default=watchlist.is_default,
        created_at=watchlist.created_at,
        items=[],
        item_count=0,
    )


@router.put("/{watchlist_id}", response_model=WatchlistResponse)
async def update_watchlist(
    watchlist_id: str,
    body: WatchlistUpdate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Update a watchlist's name or description."""
    watchlist = await db.scalar(
        select(Watchlist).where(
            and_(Watchlist.id == _parse_uuid(watchlist_id, "watchlist_id"), Watchlist.user_id == user.id)
        )
    )
    if not watchlist:
        raise HTTPException(status_code=404, detail="Watchlist not found")

    if body.name is not None:
        watchlist.name = body.name
    if body.description is not None:
        watchlist.description = body.description

    await db.commit()
    await db.refresh(watchlist)

    # Fetch items
    items_result = await db.execute(
        select(WatchlistItem).where(WatchlistItem.watchlist_id == watchlist.id)
    )
    items = items_result.scalars().all()

    return WatchlistResponse(
        id=str(watchlist.id),
        name=watchlist.name,
        description=watchlist.description,
        is_default=watchlist.is_default,
        created_at=watchlist.created_at,
        items=[
            WatchlistItemResponse(
                id=str(i.id), item_type=i.item_type, value=i.value,
                display_name=i.display_name, metadata=i.metadata_ or {},
                added_at=i.added_at,
            )
            for i in items
        ],
        item_count=len(items),
    )


@router.delete("/{watchlist_id}", status_code=204)
async def delete_watchlist(
    watchlist_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Delete a watchlist and all its items."""
    watchlist = await db.scalar(
        select(Watchlist).where(
            and_(Watchlist.id == _parse_uuid(watchlist_id, "watchlist_id"), Watchlist.user_id == user.id)
        )
    )
    if not watchlist:
        raise HTTPException(status_code=404, detail="Watchlist not found")

    await db.delete(watchlist)
    await db.commit()


@router.post("/{watchlist_id}/items", response_model=WatchlistItemResponse, status_code=status.HTTP_201_CREATED)
async def add_watchlist_item(
    watchlist_id: str,
    body: WatchlistItemCreate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Add a ticker, sector, or keyword to a watchlist."""
    watchlist = await db.scalar(
        select(Watchlist).where(
            and_(Watchlist.id == _parse_uuid(watchlist_id, "watchlist_id"), Watchlist.user_id == user.id)
        )
    )
    if not watchlist:
        raise HTTPException(status_code=404, detail="Watchlist not found")

    # Check duplicate
    existing = await db.scalar(
        select(WatchlistItem).where(
            and_(
                WatchlistItem.watchlist_id == watchlist.id,
                WatchlistItem.item_type == body.item_type,
                WatchlistItem.value == body.value,
            )
        )
    )
    if existing:
        raise HTTPException(status_code=409, detail="Item already in watchlist")

    item = WatchlistItem(
        watchlist_id=watchlist.id,
        item_type=body.item_type,
        value=body.value,
        display_name=body.display_name,
        metadata_=body.metadata,
    )
    db.add(item)
    await db.commit()
    await db.refresh(item)

    return WatchlistItemResponse(
        id=str(item.id),
        item_type=item.item_type,
        value=item.value,
        display_name=item.display_name,
        metadata=item.metadata_ or {},
        added_at=item.added_at,
    )


@router.delete("/{watchlist_id}/items/{item_id}", status_code=204)
async def remove_watchlist_item(
    watchlist_id: str,
    item_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Remove an item from a watchlist."""
    # Verify ownership
    watchlist = await db.scalar(
        select(Watchlist).where(
            and_(Watchlist.id == _parse_uuid(watchlist_id, "watchlist_id"), Watchlist.user_id == user.id)
        )
    )
    if not watchlist:
        raise HTTPException(status_code=404, detail="Watchlist not found")

    item = await db.scalar(
        select(WatchlistItem).where(
            and_(WatchlistItem.id == _parse_uuid(item_id, "item_id"), WatchlistItem.watchlist_id == watchlist.id)
        )
    )
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")

    await db.delete(item)
    await db.commit()
