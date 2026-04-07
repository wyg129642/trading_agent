"""Service layer for stock prediction scoring system.

Handles prediction CRUD, akshare price fetching, backtest evaluation,
and analyst ranking computation.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import akshare as ak
import pandas as pd
from sqlalchemy import select, func, case, and_, desc
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from backend.app.models.prediction import (
    StockPrediction, PredictionEditLog, PredictionEvaluation,
)
from backend.app.models.user import User

logger = logging.getLogger(__name__)

# Time horizon → calendar days mapping
HORIZON_DAYS = {
    "1w": 7,
    "2w": 14,
    "1m": 30,
    "3m": 90,
    "6m": 180,
}

HORIZON_LABELS = {
    "1w": "1周",
    "2w": "2周",
    "1m": "1个月",
    "3m": "3个月",
    "6m": "6个月",
}


def _compute_expires_at(created_at: datetime, horizon: str) -> datetime:
    days = HORIZON_DAYS.get(horizon, 30)
    return created_at + timedelta(days=days)


# ── Price fetching via akshare ───────────────────────────────────


def fetch_stock_price_history(
    stock_code: str,
    market: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame | None:
    """Fetch daily OHLCV data via akshare.

    Returns DataFrame with columns: date, open, close, high, low, volume
    or None on failure.
    """
    try:
        if market == "A股":
            df = ak.stock_zh_a_hist(
                symbol=stock_code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq",  # 前复权
            )
            if df is not None and not df.empty:
                df = df.rename(columns={
                    "日期": "date",
                    "开盘": "open",
                    "收盘": "close",
                    "最高": "high",
                    "最低": "low",
                    "成交量": "volume",
                })
                df["date"] = pd.to_datetime(df["date"])
                return df

        elif market == "港股":
            df = ak.stock_hk_hist(
                symbol=stock_code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq",
            )
            if df is not None and not df.empty:
                df = df.rename(columns={
                    "日期": "date",
                    "开盘": "open",
                    "收盘": "close",
                    "最高": "high",
                    "最低": "low",
                    "成交量": "volume",
                })
                df["date"] = pd.to_datetime(df["date"])
                return df

        elif market == "美股":
            df = ak.stock_us_hist(
                symbol=stock_code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq",
            )
            if df is not None and not df.empty:
                df = df.rename(columns={
                    "日期": "date",
                    "开盘": "open",
                    "收盘": "close",
                    "最高": "high",
                    "最低": "low",
                    "成交量": "volume",
                })
                df["date"] = pd.to_datetime(df["date"])
                return df

    except Exception as e:
        logger.warning("Failed to fetch price for %s (%s): %s", stock_code, market, e)

    return None


def fetch_current_price(stock_code: str, market: str) -> float | None:
    """Fetch latest close price for a stock."""
    end = datetime.now().strftime("%Y%m%d")
    start = (datetime.now() - timedelta(days=10)).strftime("%Y%m%d")
    df = fetch_stock_price_history(stock_code, market, start, end)
    if df is not None and not df.empty:
        return float(df.iloc[-1]["close"])
    return None


# ── Scoring algorithm ────────────────────────────────────────────


def compute_prediction_score(
    direction: str,
    confidence: int,
    return_pct: float,
    max_favorable_pct: float,
    max_adverse_pct: float,
) -> float:
    """Compute a 0-100 score for a single prediction.

    Scoring factors:
    1. Direction accuracy (40%): correct direction = base 40 pts
    2. Return magnitude (30%): bigger correct return = more pts
    3. Confidence calibration (20%): high confidence + correct = bonus;
       high confidence + wrong = penalty
    4. Risk management (10%): low max adverse move = bonus
    """
    is_correct = (
        (direction == "bullish" and return_pct > 0)
        or (direction == "bearish" and return_pct < 0)
    )

    # 1. Direction accuracy: 40 pts for correct, 0 for wrong
    direction_score = 40.0 if is_correct else 0.0

    # 2. Return magnitude (0-30 pts)
    abs_return = abs(return_pct)
    if is_correct:
        # Cap at 20% return for max score
        magnitude_score = min(abs_return / 20.0, 1.0) * 30.0
    else:
        magnitude_score = 0.0

    # 3. Confidence calibration (0-20 pts)
    # confidence is 1-5
    conf_norm = confidence / 5.0  # 0.2 - 1.0
    if is_correct:
        # Bonus for high-confidence correct calls
        calibration_score = conf_norm * 20.0
    else:
        # Penalty: high confidence wrong predictions score lower
        calibration_score = (1.0 - conf_norm) * 10.0

    # 4. Risk management (0-10 pts)
    # Reward predictions where the adverse move was contained
    if max_adverse_pct is not None and abs(max_adverse_pct) > 0:
        # Less adverse = better
        risk_score = max(0, 10.0 - abs(max_adverse_pct) / 2.0)
    else:
        risk_score = 5.0  # neutral

    total = direction_score + magnitude_score + calibration_score + risk_score
    return round(min(max(total, 0), 100), 2)


# ── CRUD operations ──────────────────────────────────────────────


async def create_prediction(
    db: AsyncSession,
    *,
    user_id: str,
    submitted_by_id: str,
    stock_code: str,
    stock_name: str,
    market: str,
    direction: str,
    time_horizon: str,
    reason: str | None = None,
    confidence: int = 3,
    target_price: float | None = None,
) -> StockPrediction:
    """Create a new stock prediction."""
    now = datetime.now(timezone.utc)
    price = fetch_current_price(stock_code, market)

    prediction = StockPrediction(
        user_id=user_id,
        submitted_by_id=submitted_by_id,
        stock_code=stock_code,
        stock_name=stock_name,
        market=market,
        direction=direction,
        time_horizon=time_horizon,
        reason=reason,
        confidence=confidence,
        target_price=target_price,
        price_at_submit=price,
        status="active",
        expires_at=_compute_expires_at(now, time_horizon),
        created_at=now,
        updated_at=now,
    )
    db.add(prediction)
    await db.commit()
    await db.refresh(prediction, attribute_names=[
        "user", "submitted_by", "edit_logs", "evaluation",
    ])
    return prediction


async def update_prediction(
    db: AsyncSession,
    prediction: StockPrediction,
    editor_id: str,
    updates: dict[str, Any],
) -> StockPrediction:
    """Update a prediction and log changes."""
    editable_fields = {"direction", "time_horizon", "reason", "confidence", "target_price"}
    now = datetime.now(timezone.utc)

    for field, new_val in updates.items():
        if field not in editable_fields or new_val is None:
            continue
        old_val = getattr(prediction, field, None)
        if str(old_val) == str(new_val):
            continue

        # Log the edit
        log = PredictionEditLog(
            prediction_id=prediction.id,
            edited_by_id=editor_id,
            field_changed=field,
            old_value=str(old_val) if old_val is not None else None,
            new_value=str(new_val),
            edited_at=now,
        )
        db.add(log)

        setattr(prediction, field, new_val)

    # Recalculate expires_at if time_horizon changed
    if "time_horizon" in updates and updates["time_horizon"]:
        prediction.expires_at = _compute_expires_at(prediction.created_at, prediction.time_horizon)

    prediction.updated_at = now
    await db.commit()
    await db.refresh(prediction, attribute_names=[
        "user", "submitted_by", "edit_logs", "evaluation",
    ])
    return prediction


async def get_prediction(db: AsyncSession, prediction_id: str) -> StockPrediction | None:
    stmt = (
        select(StockPrediction)
        .options(
            selectinload(StockPrediction.edit_logs),
            selectinload(StockPrediction.evaluation),
        )
        .where(StockPrediction.id == prediction_id)
    )
    return await db.scalar(stmt)


async def list_predictions(
    db: AsyncSession,
    *,
    user_id: str | None = None,
    stock_code: str | None = None,
    market: str | None = None,
    direction: str | None = None,
    status: str | None = None,
    time_horizon: str | None = None,
    page: int = 1,
    page_size: int = 20,
) -> tuple[list[StockPrediction], int]:
    """List predictions with filters, returns (items, total_count)."""
    conditions = []
    if user_id:
        conditions.append(StockPrediction.user_id == user_id)
    if stock_code:
        conditions.append(StockPrediction.stock_code == stock_code)
    if market:
        conditions.append(StockPrediction.market == market)
    if direction:
        conditions.append(StockPrediction.direction == direction)
    if status:
        conditions.append(StockPrediction.status == status)
    if time_horizon:
        conditions.append(StockPrediction.time_horizon == time_horizon)

    base = select(StockPrediction).where(*conditions) if conditions else select(StockPrediction)

    # Count
    count_stmt = select(func.count()).select_from(base.subquery())
    total = await db.scalar(count_stmt) or 0

    # Paginated results
    stmt = (
        base
        .options(
            selectinload(StockPrediction.edit_logs),
            selectinload(StockPrediction.evaluation),
        )
        .order_by(StockPrediction.created_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
    )
    result = await db.execute(stmt)
    items = list(result.scalars().all())
    return items, total


async def delete_prediction(db: AsyncSession, prediction: StockPrediction) -> None:
    await db.delete(prediction)
    await db.commit()


# ── Backtest evaluation ──────────────────────────────────────────


async def run_backtest(
    db: AsyncSession,
    prediction_ids: list[str] | None = None,
    force: bool = False,
) -> dict:
    """Run backtest evaluation for expired predictions.

    Returns summary dict with counts and errors.
    """
    now = datetime.now(timezone.utc)
    conditions = [StockPrediction.expires_at <= now]

    if not force:
        conditions.append(StockPrediction.status.in_(["active", "expired"]))
    if prediction_ids:
        conditions.append(StockPrediction.id.in_(prediction_ids))

    stmt = (
        select(StockPrediction)
        .options(selectinload(StockPrediction.evaluation))
        .where(*conditions)
    )
    result = await db.execute(stmt)
    predictions = list(result.scalars().all())

    total = len(predictions)
    successful = 0
    failed = 0
    errors: list[str] = []

    for pred in predictions:
        try:
            # Skip if already evaluated and not forcing
            if pred.evaluation and not force:
                continue

            eval_result = _evaluate_single_prediction(pred)
            if eval_result is None:
                errors.append(f"{pred.stock_code}: 无法获取价格数据")
                failed += 1
                continue

            # Upsert evaluation
            if pred.evaluation:
                for k, v in eval_result.items():
                    setattr(pred.evaluation, k, v)
                pred.evaluation.evaluated_at = now
            else:
                evaluation = PredictionEvaluation(
                    prediction_id=pred.id,
                    evaluated_at=now,
                    **eval_result,
                )
                db.add(evaluation)

            pred.status = "evaluated"
            pred.updated_at = now
            successful += 1

        except Exception as e:
            logger.exception("Failed to evaluate prediction %s", pred.id)
            errors.append(f"{pred.stock_code}: {str(e)}")
            failed += 1

    await db.commit()

    return {
        "total_evaluated": total,
        "successful": successful,
        "failed": failed,
        "errors": errors,
    }


def _evaluate_single_prediction(pred: StockPrediction) -> dict | None:
    """Evaluate a single prediction against actual price data."""
    if pred.price_at_submit is None:
        # Try to fetch the submit-time price
        submit_date = pred.created_at.strftime("%Y%m%d")
        start = (pred.created_at - timedelta(days=5)).strftime("%Y%m%d")
        df = fetch_stock_price_history(pred.stock_code, pred.market, start, submit_date)
        if df is None or df.empty:
            return None
        pred.price_at_submit = float(df.iloc[-1]["close"])

    # Fetch price data for the prediction window
    start_date = pred.created_at.strftime("%Y%m%d")
    end_date = pred.expires_at.strftime("%Y%m%d")
    df = fetch_stock_price_history(pred.stock_code, pred.market, start_date, end_date)

    if df is None or df.empty:
        return None

    submit_price = pred.price_at_submit
    end_price = float(df.iloc[-1]["close"])
    return_pct = ((end_price - submit_price) / submit_price) * 100

    # Calculate max favorable and adverse moves
    prices = df["close"].astype(float)
    if pred.direction == "bullish":
        max_favorable = ((prices.max() - submit_price) / submit_price) * 100
        max_adverse = ((prices.min() - submit_price) / submit_price) * 100
    else:
        max_favorable = ((submit_price - prices.min()) / submit_price) * 100
        max_adverse = ((prices.max() - submit_price) / submit_price) * 100

    is_correct = (
        (pred.direction == "bullish" and return_pct > 0)
        or (pred.direction == "bearish" and return_pct < 0)
    )

    score = compute_prediction_score(
        direction=pred.direction,
        confidence=pred.confidence,
        return_pct=return_pct,
        max_favorable_pct=max_favorable,
        max_adverse_pct=max_adverse,
    )

    # Build price series for charting
    price_series = [
        {"date": row["date"].strftime("%Y-%m-%d"), "close": float(row["close"])}
        for _, row in df.iterrows()
    ]

    return {
        "price_at_end": end_price,
        "return_pct": round(return_pct, 4),
        "is_direction_correct": is_correct,
        "score": score,
        "max_favorable_pct": round(max_favorable, 4),
        "max_adverse_pct": round(max_adverse, 4),
        "price_series": price_series,
    }


# ── Analyst rankings ─────────────────────────────────────────────


async def get_analyst_rankings(
    db: AsyncSession,
    market: str | None = None,
    time_horizon: str | None = None,
    min_predictions: int = 3,
) -> list[dict]:
    """Compute analyst rankings based on evaluated predictions."""
    conditions = [StockPrediction.status == "evaluated"]
    if market:
        conditions.append(StockPrediction.market == market)
    if time_horizon:
        conditions.append(StockPrediction.time_horizon == time_horizon)

    stmt = (
        select(StockPrediction)
        .options(selectinload(StockPrediction.evaluation))
        .where(*conditions)
    )
    result = await db.execute(stmt)
    predictions = list(result.scalars().all())

    # Group by user
    user_preds: dict[str, list[StockPrediction]] = {}
    for p in predictions:
        uid = str(p.user_id)
        user_preds.setdefault(uid, []).append(p)

    # Fetch user info
    user_ids = list(user_preds.keys())
    if not user_ids:
        return []
    users_stmt = select(User).where(User.id.in_(user_ids))
    users_result = await db.execute(users_stmt)
    users = {str(u.id): u for u in users_result.scalars().all()}

    rankings = []
    for uid, preds in user_preds.items():
        evaluated = [p for p in preds if p.evaluation]
        if len(evaluated) < min_predictions:
            continue

        user = users.get(uid)
        if not user:
            continue

        correct = sum(1 for p in evaluated if p.evaluation and p.evaluation.is_direction_correct)
        returns = [p.evaluation.return_pct for p in evaluated if p.evaluation and p.evaluation.return_pct is not None]
        scores = [p.evaluation.score for p in evaluated if p.evaluation and p.evaluation.score is not None]

        accuracy = correct / len(evaluated) if evaluated else 0
        avg_return = sum(returns) / len(returns) if returns else 0
        avg_score = sum(scores) / len(scores) if scores else 0

        # Composite ranking score:
        # 50% accuracy * 100 + 30% avg_score + 20% normalized avg_return
        composite = (
            accuracy * 50
            + (avg_score / 100) * 30
            + min(max(avg_return, -20), 20) / 20 * 20  # clamp to [-20, 20]
        )

        rankings.append({
            "user_id": uid,
            "username": user.username,
            "display_name": user.display_name,
            "total_predictions": len(preds),
            "evaluated_predictions": len(evaluated),
            "accuracy_rate": round(accuracy, 4),
            "avg_return_pct": round(avg_return, 4),
            "avg_score": round(avg_score, 2),
            "composite_score": round(composite, 2),
        })

    # Sort by composite score descending
    rankings.sort(key=lambda x: x["composite_score"], reverse=True)

    # Assign ranks
    for i, r in enumerate(rankings, 1):
        r["rank"] = i

    return rankings


async def get_analyst_stats(
    db: AsyncSession,
    user_id: str,
) -> dict | None:
    """Get detailed stats for a single analyst."""
    user = await db.scalar(select(User).where(User.id == user_id))
    if not user:
        return None

    stmt = (
        select(StockPrediction)
        .options(selectinload(StockPrediction.evaluation))
        .where(StockPrediction.user_id == user_id)
        .order_by(StockPrediction.created_at.desc())
    )
    result = await db.execute(stmt)
    all_preds = list(result.scalars().all())

    evaluated = [p for p in all_preds if p.status == "evaluated" and p.evaluation]
    correct = [p for p in evaluated if p.evaluation.is_direction_correct]

    accuracy = len(correct) / len(evaluated) if evaluated else 0
    returns = [p.evaluation.return_pct for p in evaluated if p.evaluation.return_pct is not None]
    scores = [p.evaluation.score for p in evaluated if p.evaluation.score is not None]

    # Horizon breakdown
    horizon_stats = {}
    for h in HORIZON_DAYS:
        h_preds = [p for p in evaluated if p.time_horizon == h]
        if h_preds:
            h_correct = sum(1 for p in h_preds if p.evaluation.is_direction_correct)
            h_returns = [p.evaluation.return_pct for p in h_preds if p.evaluation.return_pct is not None]
            horizon_stats[h] = {
                "label": HORIZON_LABELS[h],
                "total": len(h_preds),
                "correct": h_correct,
                "accuracy": round(h_correct / len(h_preds), 4),
                "avg_return": round(sum(h_returns) / len(h_returns), 4) if h_returns else 0,
            }

    # Direction breakdown
    direction_stats = {}
    for d in ["bullish", "bearish"]:
        d_preds = [p for p in evaluated if p.direction == d]
        if d_preds:
            d_correct = sum(1 for p in d_preds if p.evaluation.is_direction_correct)
            d_returns = [p.evaluation.return_pct for p in d_preds if p.evaluation.return_pct is not None]
            direction_stats[d] = {
                "total": len(d_preds),
                "correct": d_correct,
                "accuracy": round(d_correct / len(d_preds), 4),
                "avg_return": round(sum(d_returns) / len(d_returns), 4) if d_returns else 0,
            }

    # Confidence calibration
    confidence_calibration = {}
    for c in range(1, 6):
        c_preds = [p for p in evaluated if p.confidence == c]
        if c_preds:
            c_correct = sum(1 for p in c_preds if p.evaluation.is_direction_correct)
            confidence_calibration[str(c)] = {
                "total": len(c_preds),
                "correct": c_correct,
                "accuracy": round(c_correct / len(c_preds), 4),
            }

    # Recent accuracy (last 10 evaluated)
    recent = evaluated[:10]
    recent_accuracy = None
    if recent:
        recent_correct = sum(1 for p in recent if p.evaluation.is_direction_correct)
        recent_accuracy = round(recent_correct / len(recent), 4)

    # Best and worst predictions
    best = max(evaluated, key=lambda p: p.evaluation.score or 0) if evaluated else None
    worst = min(evaluated, key=lambda p: p.evaluation.score or 100) if evaluated else None

    return {
        "user_id": str(user.id),
        "username": user.username,
        "display_name": user.display_name,
        "total_predictions": len(all_preds),
        "evaluated_predictions": len(evaluated),
        "correct_predictions": len(correct),
        "accuracy_rate": round(accuracy, 4),
        "avg_return_pct": round(sum(returns) / len(returns), 4) if returns else 0,
        "avg_score": round(sum(scores) / len(scores), 2) if scores else 0,
        "best_prediction": best,
        "worst_prediction": worst,
        "horizon_stats": horizon_stats,
        "direction_stats": direction_stats,
        "confidence_calibration": confidence_calibration,
        "recent_accuracy": recent_accuracy,
    }
