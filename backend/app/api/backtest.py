"""Backtest & calibration API."""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.deps import get_current_boss_or_admin, get_current_user, get_db
from backend.app.models.revenue_model_extras import RevenueModelBacktest
from backend.app.models.user import User
from backend.app.services.confidence_calibration import (
    compute_calibration, record_actual,
)

logger = logging.getLogger(__name__)
router = APIRouter()


class ActualReport(BaseModel):
    model_id: str
    cell_path: str
    period: str
    actual_value: float
    actual_source: str = ""
    actual_reported_at: datetime | None = None


@router.post("/actuals")
async def report_actual(
    body: ActualReport,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_boss_or_admin),
):
    row = await record_actual(
        db,
        model_id=uuid.UUID(body.model_id),
        cell_path=body.cell_path,
        period=body.period,
        actual_value=body.actual_value,
        actual_source=body.actual_source,
        actual_reported_at=body.actual_reported_at or datetime.now(timezone.utc),
    )
    await db.commit()
    return {
        "id": str(row.id),
        "predicted_value": row.predicted_value,
        "actual_value": row.actual_value,
        "pct_error": row.pct_error,
    }


@router.get("/calibration")
async def get_calibration(
    industry: str | None = Query(None),
    since_days: int = Query(365, ge=30, le=1825),
    user: User = Depends(get_current_user),
):
    buckets = await compute_calibration(industry=industry, since_days=since_days)
    return [b.__dict__ for b in buckets]


@router.get("/backtests/{model_id}")
async def list_backtests(
    model_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    q = select(RevenueModelBacktest).where(RevenueModelBacktest.model_id == model_id)
    rows = list((await db.execute(q)).scalars().all())
    return [
        {
            "id": str(r.id),
            "cell_path": r.cell_path,
            "period": r.period,
            "predicted_value": r.predicted_value,
            "predicted_confidence": r.predicted_confidence,
            "actual_value": r.actual_value,
            "abs_error": r.abs_error,
            "pct_error": r.pct_error,
            "actual_source": r.actual_source,
            "actual_reported_at": r.actual_reported_at.isoformat() if r.actual_reported_at else None,
            "created_at": r.created_at.isoformat(),
        }
        for r in rows
    ]
