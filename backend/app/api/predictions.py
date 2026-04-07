"""API routes for stock prediction scoring system."""

from __future__ import annotations

import logging
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.deps import get_db, get_current_user, get_current_boss_or_admin
from backend.app.models.user import User
from backend.app.schemas.prediction import (
    PredictionCreate,
    PredictionUpdate,
    PredictionResponse,
    PredictionListResponse,
    BacktestRequest,
    BacktestResultResponse,
    RankingListResponse,
    RankingEntry,
    AnalystStatsResponse,
)
from backend.app.services import prediction_service

logger = logging.getLogger(__name__)
router = APIRouter()


# ── Create prediction ────────────────────────────────────────────


@router.post("/", response_model=PredictionResponse)
async def create_prediction(
    body: PredictionCreate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Create a new stock prediction.

    - Regular users submit for themselves.
    - Admin/boss can specify user_id to submit on behalf of an analyst.
    """
    target_user_id = str(current_user.id)

    if body.user_id and body.user_id != str(current_user.id):
        # Only boss/admin can submit on behalf of others
        if current_user.role not in ("admin", "boss"):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="只有管理员或负责人可以代替其他员工提交预测",
            )
        target_user_id = body.user_id

    prediction = await prediction_service.create_prediction(
        db,
        user_id=target_user_id,
        submitted_by_id=str(current_user.id),
        stock_code=body.stock_code,
        stock_name=body.stock_name,
        market=body.market,
        direction=body.direction,
        time_horizon=body.time_horizon,
        reason=body.reason,
        confidence=body.confidence,
        target_price=body.target_price,
    )
    return prediction


# ── List predictions ─────────────────────────────────────────────


@router.get("/", response_model=PredictionListResponse)
async def list_predictions(
    user_id: str | None = Query(None),
    stock_code: str | None = Query(None),
    market: str | None = Query(None),
    direction: str | None = Query(None),
    prediction_status: str | None = Query(None, alias="status"),
    time_horizon: str | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """List predictions with optional filters.

    Regular users see all predictions (transparency for team).
    """
    items, total = await prediction_service.list_predictions(
        db,
        user_id=user_id,
        stock_code=stock_code,
        market=market,
        direction=direction,
        status=prediction_status,
        time_horizon=time_horizon,
        page=page,
        page_size=page_size,
    )
    return PredictionListResponse(
        items=items,
        total=total,
        page=page,
        page_size=page_size,
    )


# ── Get single prediction ───────────────────────────────────────


@router.get("/{prediction_id}", response_model=PredictionResponse)
async def get_prediction(
    prediction_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    prediction = await prediction_service.get_prediction(db, prediction_id)
    if not prediction:
        raise HTTPException(status_code=404, detail="预测记录不存在")
    return prediction


# ── Update prediction ────────────────────────────────────────────


@router.put("/{prediction_id}", response_model=PredictionResponse)
async def update_prediction(
    prediction_id: str,
    body: PredictionUpdate,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Update a prediction. Only allowed before evaluation.

    - The prediction owner can edit their own prediction.
    - Admin/boss can edit any prediction.
    """
    prediction = await prediction_service.get_prediction(db, prediction_id)
    if not prediction:
        raise HTTPException(status_code=404, detail="预测记录不存在")

    if prediction.status == "evaluated":
        raise HTTPException(status_code=400, detail="已评估的预测不可修改")

    # Permission check
    is_owner = str(prediction.user_id) == str(current_user.id)
    is_privileged = current_user.role in ("admin", "boss")
    if not is_owner and not is_privileged:
        raise HTTPException(status_code=403, detail="只能修改自己的预测")

    updates = body.model_dump(exclude_unset=True)
    updated = await prediction_service.update_prediction(
        db, prediction, str(current_user.id), updates,
    )
    return updated


# ── Delete prediction ────────────────────────────────────────────


@router.delete("/{prediction_id}", status_code=204)
async def delete_prediction(
    prediction_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_boss_or_admin),
):
    """Delete a prediction (admin/boss only)."""
    prediction = await prediction_service.get_prediction(db, prediction_id)
    if not prediction:
        raise HTTPException(status_code=404, detail="预测记录不存在")
    await prediction_service.delete_prediction(db, prediction)


# ── Backtest ─────────────────────────────────────────────────────


@router.post("/backtest", response_model=BacktestResultResponse)
async def run_backtest(
    body: BacktestRequest,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_boss_or_admin),
):
    """Trigger backtest evaluation for expired predictions (admin/boss only)."""
    result = await prediction_service.run_backtest(
        db,
        prediction_ids=body.prediction_ids,
        force=body.force,
    )
    return BacktestResultResponse(**result)


# ── Rankings ─────────────────────────────────────────────────────


@router.get("/rankings/list", response_model=RankingListResponse)
async def get_rankings(
    market: str | None = Query(None),
    time_horizon: str | None = Query(None),
    min_predictions: int = Query(3, ge=1),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get analyst rankings leaderboard."""
    rankings = await prediction_service.get_analyst_rankings(
        db,
        market=market,
        time_horizon=time_horizon,
        min_predictions=min_predictions,
    )
    return RankingListResponse(
        rankings=[RankingEntry(**r) for r in rankings],
        total_analysts=len(rankings),
        last_updated=None,
    )


# ── Analyst stats ────────────────────────────────────────────────


@router.get("/analyst/{user_id}", response_model=AnalystStatsResponse)
async def get_analyst_stats(
    user_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Get detailed prediction stats for a specific analyst."""
    stats = await prediction_service.get_analyst_stats(db, user_id)
    if not stats:
        raise HTTPException(status_code=404, detail="用户不存在")
    return stats


# ── My predictions shortcut ──────────────────────────────────────


@router.get("/my/list", response_model=PredictionListResponse)
async def my_predictions(
    prediction_status: str | None = Query(None, alias="status"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """Shortcut to list the current user's own predictions."""
    items, total = await prediction_service.list_predictions(
        db,
        user_id=str(current_user.id),
        status=prediction_status,
        page=page,
        page_size=page_size,
    )
    return PredictionListResponse(
        items=items,
        total=total,
        page=page,
        page_size=page_size,
    )


# ── All users list (for admin/boss to select analyst) ────────────


@router.get("/users/analysts")
async def list_analysts(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_boss_or_admin),
):
    """List all active users for the admin/boss to select when submitting predictions."""
    from sqlalchemy import select
    stmt = select(User).where(User.is_active == True).order_by(User.username)
    result = await db.execute(stmt)
    users = result.scalars().all()
    return [
        {
            "id": str(u.id),
            "username": u.username,
            "display_name": u.display_name,
            "role": u.role,
        }
        for u in users
    ]
