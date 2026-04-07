"""Pydantic schemas for stock prediction scoring system."""

from __future__ import annotations

from datetime import datetime
from pydantic import BaseModel, Field


# ── Request schemas ──────────────────────────────────────────────


class PredictionCreate(BaseModel):
    """Create a new stock prediction."""
    user_id: str | None = None  # If admin/boss submitting on behalf of another user
    stock_code: str = Field(min_length=1, max_length=20)
    stock_name: str = Field(min_length=1, max_length=100)
    market: str = Field(pattern=r"^(A股|港股|美股)$")
    direction: str = Field(pattern=r"^(bullish|bearish)$")
    time_horizon: str = Field(pattern=r"^(1w|2w|1m|3m|6m)$")
    reason: str | None = None
    confidence: int = Field(default=3, ge=1, le=5)
    target_price: float | None = None


class PredictionUpdate(BaseModel):
    """Update an existing prediction (only before evaluation)."""
    direction: str | None = Field(default=None, pattern=r"^(bullish|bearish)$")
    time_horizon: str | None = Field(default=None, pattern=r"^(1w|2w|1m|3m|6m)$")
    reason: str | None = None
    confidence: int | None = Field(default=None, ge=1, le=5)
    target_price: float | None = None


class PredictionListQuery(BaseModel):
    """Query params for listing predictions."""
    user_id: str | None = None
    stock_code: str | None = None
    market: str | None = None
    direction: str | None = None
    status: str | None = None
    time_horizon: str | None = None
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=20, ge=1, le=100)


class BacktestRequest(BaseModel):
    """Trigger a backtest run."""
    prediction_ids: list[str] | None = None  # None = backtest all expired
    force: bool = False  # Re-evaluate already evaluated predictions


# ── Response schemas ─────────────────────────────────────────────


class UserBrief(BaseModel):
    id: str
    username: str
    display_name: str | None

    model_config = {"from_attributes": True}


class EditLogResponse(BaseModel):
    id: str
    field_changed: str
    old_value: str | None
    new_value: str | None
    edited_at: datetime
    edited_by: UserBrief | None

    model_config = {"from_attributes": True}


class EvaluationResponse(BaseModel):
    id: str
    price_at_end: float | None
    return_pct: float | None
    is_direction_correct: bool | None
    score: float | None
    max_favorable_pct: float | None
    max_adverse_pct: float | None
    price_series: list | None
    evaluated_at: datetime

    model_config = {"from_attributes": True}


class PredictionResponse(BaseModel):
    id: str
    user: UserBrief | None
    submitted_by: UserBrief | None
    stock_code: str
    stock_name: str
    market: str
    direction: str
    time_horizon: str
    reason: str | None
    confidence: int
    price_at_submit: float | None
    target_price: float | None
    status: str
    expires_at: datetime | None
    created_at: datetime
    updated_at: datetime
    evaluation: EvaluationResponse | None = None
    edit_logs: list[EditLogResponse] = []

    model_config = {"from_attributes": True}


class PredictionListResponse(BaseModel):
    items: list[PredictionResponse]
    total: int
    page: int
    page_size: int


class BacktestResultResponse(BaseModel):
    total_evaluated: int
    successful: int
    failed: int
    errors: list[str] = []


# ── Analyst ranking schemas ──────────────────────────────────────


class AnalystStatsResponse(BaseModel):
    user_id: str
    username: str
    display_name: str | None
    total_predictions: int
    evaluated_predictions: int
    correct_predictions: int
    accuracy_rate: float  # 0-1
    avg_return_pct: float
    avg_score: float
    best_prediction: PredictionResponse | None = None
    worst_prediction: PredictionResponse | None = None
    # Breakdown by time horizon
    horizon_stats: dict = {}
    # Breakdown by direction
    direction_stats: dict = {}
    # Confidence calibration: {confidence_level: accuracy}
    confidence_calibration: dict = {}
    # Recent trend (last 10 predictions accuracy)
    recent_accuracy: float | None = None


class RankingEntry(BaseModel):
    rank: int
    user_id: str
    username: str
    display_name: str | None
    total_predictions: int
    evaluated_predictions: int
    accuracy_rate: float
    avg_return_pct: float
    avg_score: float
    composite_score: float  # The ranking score


class RankingListResponse(BaseModel):
    rankings: list[RankingEntry]
    total_analysts: int
    last_updated: datetime | None = None
