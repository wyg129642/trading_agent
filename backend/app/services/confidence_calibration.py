"""Confidence calibration — learn from backtest results.

Workflow:
  1. When a model is built, every cell has a ``confidence`` label
     (HIGH/MEDIUM/LOW) and ``value``.
  2. When actuals report (quarterly earnings), a ``RevenueModelBacktest``
     row is written for each cell with ``predicted_value`` vs
     ``actual_value``.
  3. This service aggregates the actual errors per confidence bucket
     per industry and returns a "calibration map" that tells us:
       * The empirical MAE for each bucket
       * Whether the label is well-calibrated (expected vs actual)
  4. The frontend shows a "raw=HIGH, calibrated=MEDIUM" tooltip on cells
     whose bucket has under-performed.
"""
from __future__ import annotations

import logging
import statistics
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.core.database import async_session_factory
from backend.app.models.revenue_model import RevenueModel, ModelCell
from backend.app.models.revenue_model_extras import RevenueModelBacktest

logger = logging.getLogger(__name__)


_CONFIDENCE_EXPECTATION = {
    "HIGH":   {"mae_target": 0.10, "hit_rate_target": 0.70},
    "MEDIUM": {"mae_target": 0.20, "hit_rate_target": 0.50},
    "LOW":    {"mae_target": 0.40, "hit_rate_target": 0.30},
}


@dataclass
class CalibrationBucket:
    label: str
    samples: int
    mae: float
    p50_err: float
    p90_err: float
    hit_rate: float
    expected_mae: float
    calibrated_label: str


async def record_actual(
    db: AsyncSession,
    *,
    model_id: uuid.UUID,
    cell_path: str,
    period: str,
    actual_value: float,
    actual_source: str = "",
    actual_reported_at: datetime | None = None,
) -> RevenueModelBacktest:
    """Write a backtest row when actuals come in for a period."""
    q = select(ModelCell).where(
        ModelCell.model_id == model_id,
        ModelCell.path == cell_path,
    )
    cell = (await db.execute(q)).scalar_one_or_none()
    predicted_value = cell.value if cell else None
    predicted_confidence = cell.confidence if cell else "MEDIUM"

    abs_err = None
    pct_err = None
    if predicted_value is not None:
        abs_err = abs(actual_value - predicted_value)
        if actual_value != 0:
            pct_err = abs_err / abs(actual_value)

    row = RevenueModelBacktest(
        model_id=model_id,
        cell_path=cell_path,
        period=period,
        predicted_value=predicted_value,
        predicted_confidence=predicted_confidence,
        actual_value=actual_value,
        abs_error=abs_err,
        pct_error=pct_err,
        actual_source=actual_source,
        actual_reported_at=actual_reported_at or datetime.now(timezone.utc),
        prediction_made_at=cell.created_at if cell else None,
    )
    db.add(row)
    await db.flush()
    return row


async def compute_calibration(
    industry: str | None = None, since_days: int = 365,
) -> list[CalibrationBucket]:
    async with async_session_factory() as db:
        cutoff = datetime.now(timezone.utc) - timedelta(days=since_days)
        q = select(RevenueModelBacktest).where(
            RevenueModelBacktest.created_at >= cutoff,
            RevenueModelBacktest.pct_error.is_not(None),
        )
        rows = list((await db.execute(q)).scalars().all())

        if industry:
            model_ids = [r.model_id for r in rows]
            if model_ids:
                mq = select(RevenueModel).where(
                    RevenueModel.id.in_(set(model_ids)),
                    RevenueModel.industry == industry,
                )
                keep = {m.id for m in (await db.execute(mq)).scalars().all()}
                rows = [r for r in rows if r.model_id in keep]

        buckets_err: dict[str, list[float]] = {"HIGH": [], "MEDIUM": [], "LOW": []}
        for r in rows:
            buckets_err.setdefault(r.predicted_confidence, []).append(float(r.pct_error or 0))

        out: list[CalibrationBucket] = []
        for label, errs in buckets_err.items():
            if not errs:
                continue
            mae = statistics.mean(errs)
            p50 = statistics.median(errs)
            p90 = sorted(errs)[int(len(errs) * 0.9) - 1] if len(errs) >= 10 else max(errs)
            expected = _CONFIDENCE_EXPECTATION.get(label, {}).get("mae_target", 0.25)
            hit_target = _CONFIDENCE_EXPECTATION.get(label, {}).get("hit_rate_target", 0.5)
            hit_rate = sum(1 for e in errs if e <= expected) / len(errs)
            # If raw MAE > expected × 1.5, recalibrate down one bucket
            if mae > expected * 1.5:
                downgrade = {"HIGH": "MEDIUM", "MEDIUM": "LOW", "LOW": "LOW"}[label]
                cal = downgrade
            else:
                cal = label
            out.append(CalibrationBucket(
                label=label,
                samples=len(errs),
                mae=mae,
                p50_err=p50,
                p90_err=p90,
                hit_rate=hit_rate,
                expected_mae=expected,
                calibrated_label=cal,
            ))
        return out


async def get_calibration_map(industry: str | None = None) -> dict[str, str]:
    buckets = await compute_calibration(industry=industry)
    return {b.label: b.calibrated_label for b in buckets}
