"""Revenue snapshot API — cross-model segment revenue lookup."""
from __future__ import annotations

import logging
from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.deps import get_current_user, get_db
from backend.app.models.user import User
from backend.app.services.segment_snapshot_service import (
    get_cross_model_comparison,
    get_snapshot_for_ticker,
)

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/ticker/{ticker}")
async def get_ticker_snapshot(
    ticker: str,
    period: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
):
    rows = await get_snapshot_for_ticker(db, ticker, period=period)
    return {"ticker": ticker, "period": period, "rows": rows, "count": len(rows)}


@router.get("/compare")
async def compare_tickers(
    tickers: str = Query(..., description="Comma-separated list of tickers"),
    metric: str = Query("revenue"),
    period: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
):
    ticker_list = [t.strip() for t in tickers.split(",") if t.strip()]
    rows = await get_cross_model_comparison(db, ticker_list, metric=metric, period=period)
    return {"metric": metric, "period": period, "tickers": ticker_list, "groups": rows}
