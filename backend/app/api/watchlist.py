"""Watchlist API: CRUD watchlists and items."""

from __future__ import annotations

import csv
import os
import uuid
from functools import lru_cache
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status


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

# ---------------------------------------------------------------------------
# Stock search across CSV market data files
# ---------------------------------------------------------------------------

_DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "..", "data")

_MARKET_FILES = {
    "A": "a_stock_list.csv",
    "HK": "hk_stock_list.csv",
    "US": "us_stock_list.csv",
    "KR": "kr_stock_list.csv",
    "JP": "jp_stock_list.csv",
}


@lru_cache(maxsize=1)
def _load_all_stocks() -> dict[str, list[dict]]:
    """Load all stock lists from CSV files, cached in memory.

    Each stock dict has: code, name, and optionally name_cn (simplified Chinese,
    used for HK stocks whose primary name is traditional Chinese).
    """
    result: dict[str, list[dict]] = {}
    for market, filename in _MARKET_FILES.items():
        path = os.path.join(_DATA_DIR, filename)
        stocks: list[dict] = []
        if not os.path.exists(path):
            result[market] = stocks
            continue
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                code = (row.get("code") or row.get("代码", "")).strip()
                name = (row.get("name") or row.get("名称", "")).strip()
                name_cn = (row.get("name_cn") or "").strip()
                entry: dict = {"code": code, "name": name}
                if name_cn and name_cn != name:
                    entry["name_cn"] = name_cn
                stocks.append(entry)
        result[market] = stocks
    return result


@router.get("/stock-search")
async def stock_search(
    q: str = Query("", min_length=0, description="Search query"),
    market: Optional[str] = Query(None, description="Market filter: A, HK, US, KR, JP"),
    limit: int = Query(30, ge=1, le=100),
):
    """Fuzzy search stocks by code or name across markets.

    For HK stocks, searches against both traditional and simplified Chinese names.
    """
    all_stocks = _load_all_stocks()
    query = q.strip().upper()

    markets_to_search = [market.upper()] if market else list(all_stocks.keys())
    results: List[dict] = []

    for mkt in markets_to_search:
        stocks = all_stocks.get(mkt, [])
        if not query:
            for s in stocks[:limit]:
                results.append({**s, "market": mkt})
            continue
        for s in stocks:
            code_upper = s["code"].upper()
            name_upper = s["name"].upper()
            name_cn_upper = s.get("name_cn", "").upper()
            if query in code_upper or query in name_upper or (name_cn_upper and query in name_cn_upper):
                results.append({**s, "market": mkt})
            if len(results) >= limit * 3:
                break

    # Sort: exact code prefix first, then code contains, then name contains
    def sort_key(item: dict) -> tuple:
        code_upper = item["code"].upper()
        if code_upper == query:
            return (0, item["code"])
        if code_upper.startswith(query):
            return (1, item["code"])
        if query in code_upper:
            return (2, item["code"])
        return (3, item["code"])

    if query:
        results.sort(key=sort_key)

    return {"results": results[:limit], "total": len(results)}


@router.get("/markets")
async def list_markets():
    """Return available markets and their stock counts."""
    all_stocks = _load_all_stocks()
    return {
        "markets": [
            {"key": mkt, "count": len(stocks)}
            for mkt, stocks in all_stocks.items()
        ]
    }


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


@router.get("/all-values")
async def get_all_watchlist_values(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Return flat sets of all tickers, sectors, keywords across user's watchlists."""
    stmt = (
        select(WatchlistItem.item_type, WatchlistItem.value, WatchlistItem.display_name)
        .join(Watchlist, Watchlist.id == WatchlistItem.watchlist_id)
        .where(Watchlist.user_id == user.id)
    )
    result = await db.execute(stmt)
    tickers, sectors, keywords = [], [], []
    for item_type, value, display_name in result:
        entry = {"value": value, "display_name": display_name}
        if item_type == "ticker":
            tickers.append(entry)
        elif item_type == "sector":
            sectors.append(entry)
        elif item_type == "keyword":
            keywords.append(entry)
    return {"tickers": tickers, "sectors": sectors, "keywords": keywords}


@router.post("/quick-add", response_model=WatchlistItemResponse, status_code=status.HTTP_201_CREATED)
async def quick_add_ticker(
    body: WatchlistItemCreate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Quick-add an item to the user's default (first) watchlist, creating one if needed."""
    # Find or create default watchlist
    stmt = (
        select(Watchlist)
        .where(Watchlist.user_id == user.id)
        .order_by(Watchlist.is_default.desc(), Watchlist.created_at)
        .limit(1)
    )
    watchlist = await db.scalar(stmt)
    if not watchlist:
        watchlist = Watchlist(user_id=user.id, name="我的关注", is_default=True)
        db.add(watchlist)
        await db.flush()

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
