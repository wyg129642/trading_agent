"""Source Leaderboard API — ranks news sources by signal accuracy.

Compares predicted sentiment (bullish/bearish) against actual stock price
movements at T+0 (same day), T+1, T+5, T+20 horizons.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy import select, func, case, and_, literal, Float
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.deps import get_current_user, get_db
from backend.app.models.leaderboard import SignalEvaluation
from backend.app.models.user import User

logger = logging.getLogger(__name__)
router = APIRouter()


# ─── Response schemas ────────────────────────────────────────────

class LeaderboardEntry(BaseModel):
    rank: int
    source_name: str
    category: str
    total_signals: int
    accuracy_t0: float | None = None
    accuracy_t1: float | None = None
    accuracy_t5: float | None = None
    accuracy_t20: float | None = None
    avg_return_bullish: float | None = None
    avg_return_bearish: float | None = None
    timeliness_score: float | None = None
    composite_score: float
    ic_t1: float | None = None
    ic_t5: float | None = None
    ic_t20: float | None = None
    icir: float | None = None
    avg_confidence: float | None = None


class LeaderboardResponse(BaseModel):
    entries: list[LeaderboardEntry]
    total_sources: int
    total_signals: int
    last_evaluated: str | None = None
    period_start: str | None = None
    period_end: str | None = None


class EvaluationStats(BaseModel):
    total_evaluations: int
    sources_evaluated: int
    last_run: str | None = None
    last_signal_time: str | None = None
    hours_since_last_run: float | None = None
    period_days: int
    is_stale: bool = False


# ─── Endpoints ───────────────────────────────────────────────────

@router.get("", response_model=LeaderboardResponse)
async def get_leaderboard(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
    days: int = Query(default=90, ge=7, le=365, description="Lookback period in days"),
    min_signals: int = Query(default=3, ge=1, le=50, description="Minimum signals to rank"),
    category: str = Query(default="", description="Filter by source category"),
    market: str = Query(default="", description="Filter by market (china, us, hk, global)"),
    min_confidence: float = Query(default=0.0, ge=0.0, le=1.0, description="Minimum confidence threshold"),
    min_score: float = Query(default=0.0, ge=0.0, le=1.0, description="Minimum |sentiment_score| threshold"),
):
    """Get the source accuracy leaderboard.

    Ranks sources by composite accuracy score across multiple time horizons.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    # Build base query
    filters = [SignalEvaluation.signal_time >= cutoff]
    if category:
        filters.append(SignalEvaluation.category == category)
    if market:
        filters.append(SignalEvaluation.market == market)
    if min_confidence > 0:
        # Use T+1 confidence as the primary gate — it's populated for most rows.
        # Include NULL confidence rows (treated as "meets threshold") so we don't
        # accidentally drop older data that lacks per-horizon confidence.
        filters.append(
            (SignalEvaluation.confidence_t1.is_(None)) |
            (SignalEvaluation.confidence_t1 >= min_confidence)
        )
    if min_score > 0:
        # Filter on sentiment_score if available, otherwise keep NULL rows.
        filters.append(
            (SignalEvaluation.sentiment_score_t5.is_(None)) |
            (func.abs(SignalEvaluation.sentiment_score_t5) >= min_score)
        )

    # Aggregate per source
    stmt = (
        select(
            SignalEvaluation.source_name,
            SignalEvaluation.category,
            func.count().label("total_signals"),
            # Accuracy: T+0 uses simple accuracy; T+1/5/20 use confidence-weighted
            _accuracy_expr("correct_t0").label("accuracy_t0"),
            _cw_accuracy_expr("correct_t1", "confidence_t1").label("accuracy_t1"),
            _cw_accuracy_expr("correct_t5", "confidence_t5").label("accuracy_t5"),
            _cw_accuracy_expr("correct_t20", "confidence_t20").label("accuracy_t20"),
            # Average returns by direction
            func.avg(
                case(
                    (SignalEvaluation.predicted_sentiment.in_(["bullish", "very_bullish"]),
                     SignalEvaluation.return_t5),
                    else_=None,
                )
            ).label("avg_return_bullish"),
            func.avg(
                case(
                    (SignalEvaluation.predicted_sentiment.in_(["bearish", "very_bearish"]),
                     SignalEvaluation.return_t5),
                    else_=None,
                )
            ).label("avg_return_bearish"),
            # Timeliness: T+0 accuracy
            _accuracy_expr("correct_t0").label("timeliness_score"),
            # Last evaluation time
            func.max(SignalEvaluation.evaluated_at).label("last_eval"),
        )
        .where(and_(*filters))
        .group_by(SignalEvaluation.source_name, SignalEvaluation.category)
        .having(func.count() >= min_signals)
    )

    result = await db.execute(stmt)
    rows = result.all()

    # Compute IC/ICIR per source from raw evaluation rows
    ic_metrics = await _compute_ic_from_db(db, filters)

    # Composite score weights (sum to 1.0)
    HORIZON_WEIGHTS = (("t0", 0.10), ("t1", 0.30), ("t5", 0.35), ("t20", 0.25))

    # Compute composite score and build entries
    entries = []
    for row in rows:
        # Missing horizons contribute 0.5 (random-baseline) so sources with
        # more horizons of data aren't penalised vs. sources with only one
        # horizon at 60%. The min_signals filter already guards against
        # tiny-sample noise.
        composite = sum(
            (float(getattr(row, f"accuracy_{h}")) if getattr(row, f"accuracy_{h}") is not None else 0.5) * w
            for h, w in HORIZON_WEIGHTS
        )

        src_ic = ic_metrics.get(row.source_name, {})
        entries.append({
            "source_name": row.source_name,
            "category": row.category or "",
            "total_signals": row.total_signals,
            "accuracy_t0": _safe_float(row.accuracy_t0),
            "accuracy_t1": _safe_float(row.accuracy_t1),
            "accuracy_t5": _safe_float(row.accuracy_t5),
            "accuracy_t20": _safe_float(row.accuracy_t20),
            "avg_return_bullish": _safe_float(row.avg_return_bullish),
            "avg_return_bearish": _safe_float(row.avg_return_bearish),
            "timeliness_score": _safe_float(row.timeliness_score),
            "composite_score": composite,
            "ic_t1": src_ic.get("ic_t1"),
            "ic_t5": src_ic.get("ic_t5"),
            "ic_t20": src_ic.get("ic_t20"),
            "icir": src_ic.get("icir"),
            "avg_confidence": src_ic.get("avg_confidence"),
        })

    # Sort by composite score descending
    entries.sort(key=lambda e: e["composite_score"], reverse=True)

    # Add rank
    ranked = [
        LeaderboardEntry(rank=i + 1, **entry)
        for i, entry in enumerate(entries)
    ]

    # Get total signals count
    total_stmt = (
        select(func.count())
        .select_from(SignalEvaluation)
        .where(SignalEvaluation.signal_time >= cutoff)
    )
    total_signals = await db.scalar(total_stmt) or 0

    # Last evaluation time
    last_eval_stmt = select(func.max(SignalEvaluation.evaluated_at))
    last_eval = await db.scalar(last_eval_stmt)

    return LeaderboardResponse(
        entries=ranked,
        total_sources=len(ranked),
        total_signals=total_signals,
        last_evaluated=last_eval.isoformat() if last_eval else None,
        period_start=cutoff.isoformat(),
        period_end=datetime.now(timezone.utc).isoformat(),
    )


@router.get("/quick", response_model=LeaderboardResponse)
async def get_quick_leaderboard(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
    days: int = Query(default=7, ge=1, le=90, description="Lookback period in days"),
    min_signals: int = Query(default=2, ge=1, le=50, description="Minimum signals to rank"),
    horizon: str = Query(default="t1", description="Primary horizon: t1 or t5"),
    min_confidence: float = Query(default=0.0, ge=0.0, le=1.0, description="Minimum confidence threshold"),
    min_score: float = Query(default=0.0, ge=0.0, le=1.0, description="Minimum |sentiment_score| threshold"),
    category: str = Query(default="", description="Filter by source category"),
    market: str = Query(default="", description="Filter by market"),
):
    """Quick leaderboard — short-horizon view (1-5 days) with confidence/score filtering.

    Unlike the full leaderboard which needs 30+ days for T+20 data,
    this endpoint works with as little as 1-2 days of data by focusing on T+1 and T+5.
    Supports confidence and sentiment_score threshold filtering for fine-grained backtesting.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    filters = [SignalEvaluation.signal_time >= cutoff]
    if category:
        filters.append(SignalEvaluation.category == category)
    if market:
        filters.append(SignalEvaluation.market == market)
    if min_confidence > 0:
        conf_col = SignalEvaluation.confidence_t5 if horizon == "t5" else SignalEvaluation.confidence_t1
        # Include NULL rows — legacy signals lack per-horizon confidence
        filters.append((conf_col.is_(None)) | (conf_col >= min_confidence))
    if min_score > 0:
        score_col = SignalEvaluation.sentiment_score_t5 if horizon == "t5" else SignalEvaluation.sentiment_score_t1
        filters.append((score_col.is_(None)) | (func.abs(score_col) >= min_score))

    # Determine which accuracy columns to use
    if horizon == "t5":
        primary_acc = _cw_accuracy_expr("correct_t5", "confidence_t5").label("primary_accuracy")
        secondary_acc = _cw_accuracy_expr("correct_t1", "confidence_t1").label("secondary_accuracy")
    else:
        primary_acc = _cw_accuracy_expr("correct_t1", "confidence_t1").label("primary_accuracy")
        secondary_acc = _cw_accuracy_expr("correct_t5", "confidence_t5").label("secondary_accuracy")

    stmt = (
        select(
            SignalEvaluation.source_name,
            SignalEvaluation.category,
            func.count().label("total_signals"),
            _accuracy_expr("correct_t0").label("accuracy_t0"),
            _cw_accuracy_expr("correct_t1", "confidence_t1").label("accuracy_t1"),
            _cw_accuracy_expr("correct_t5", "confidence_t5").label("accuracy_t5"),
            primary_acc,
            secondary_acc,
            func.avg(
                case(
                    (SignalEvaluation.predicted_sentiment.in_(["bullish", "very_bullish"]),
                     SignalEvaluation.return_t5),
                    else_=None,
                )
            ).label("avg_return_bullish"),
            func.avg(
                case(
                    (SignalEvaluation.predicted_sentiment.in_(["bearish", "very_bearish"]),
                     SignalEvaluation.return_t5),
                    else_=None,
                )
            ).label("avg_return_bearish"),
            _accuracy_expr("correct_t0").label("timeliness_score"),
            func.max(SignalEvaluation.evaluated_at).label("last_eval"),
        )
        .where(and_(*filters))
        .group_by(SignalEvaluation.source_name, SignalEvaluation.category)
        .having(func.count() >= min_signals)
    )

    result = await db.execute(stmt)
    rows = result.all()

    # Compute IC/ICIR per source
    ic_metrics = await _compute_ic_from_db(db, filters)

    # Quick composite weights sum to 1.0 so sources with missing horizons aren't
    # inflated. T+20 is intentionally omitted (not available in short windows).
    if horizon == "t5":
        QUICK_WEIGHTS = (("t0", 0.10), ("t1", 0.30), ("t5", 0.60))
    else:
        QUICK_WEIGHTS = (("t0", 0.10), ("t1", 0.70), ("t5", 0.20))

    entries = []
    for row in rows:
        composite = sum(
            (float(getattr(row, f"accuracy_{h}")) if getattr(row, f"accuracy_{h}") is not None else 0.5) * w
            for h, w in QUICK_WEIGHTS
        )

        src_ic = ic_metrics.get(row.source_name, {})
        entries.append({
            "source_name": row.source_name,
            "category": row.category or "",
            "total_signals": row.total_signals,
            "accuracy_t0": _safe_float(row.accuracy_t0),
            "accuracy_t1": _safe_float(row.accuracy_t1),
            "accuracy_t5": _safe_float(row.accuracy_t5),
            "accuracy_t20": None,  # Not available in quick view
            "avg_return_bullish": _safe_float(row.avg_return_bullish),
            "avg_return_bearish": _safe_float(row.avg_return_bearish),
            "timeliness_score": _safe_float(row.timeliness_score),
            "composite_score": composite,
            "ic_t1": src_ic.get("ic_t1"),
            "ic_t5": src_ic.get("ic_t5"),
            "ic_t20": src_ic.get("ic_t20"),
            "icir": src_ic.get("icir"),
            "avg_confidence": src_ic.get("avg_confidence"),
        })

    entries.sort(key=lambda e: e["composite_score"], reverse=True)
    ranked = [LeaderboardEntry(rank=i + 1, **entry) for i, entry in enumerate(entries)]

    total_stmt = (
        select(func.count()).select_from(SignalEvaluation)
        .where(SignalEvaluation.signal_time >= cutoff)
    )
    total_signals = await db.scalar(total_stmt) or 0

    # Include last_evaluated so the UI can show staleness warnings even in quick mode
    last_eval = await db.scalar(select(func.max(SignalEvaluation.evaluated_at)))

    return LeaderboardResponse(
        entries=ranked,
        total_sources=len(ranked),
        total_signals=total_signals,
        last_evaluated=last_eval.isoformat() if last_eval else None,
        period_start=cutoff.isoformat(),
        period_end=datetime.now(timezone.utc).isoformat(),
    )


@router.get("/stats", response_model=EvaluationStats)
async def get_evaluation_stats(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Get evaluation statistics — including staleness so the UI can warn users."""
    total = await db.scalar(select(func.count()).select_from(SignalEvaluation)) or 0
    sources = await db.scalar(
        select(func.count(func.distinct(SignalEvaluation.source_name)))
    ) or 0
    last_run = await db.scalar(select(func.max(SignalEvaluation.evaluated_at)))
    last_signal = await db.scalar(select(func.max(SignalEvaluation.signal_time)))
    oldest = await db.scalar(select(func.min(SignalEvaluation.signal_time)))

    now = datetime.now(timezone.utc)
    period_days = (now - oldest).days if oldest else 0
    hours_since = None
    is_stale = False
    if last_run is not None:
        # last_run is timezone-aware (DateTime(timezone=True)); guard against naive rows just in case
        if last_run.tzinfo is None:
            last_run_aware = last_run.replace(tzinfo=timezone.utc)
        else:
            last_run_aware = last_run
        hours_since = (now - last_run_aware).total_seconds() / 3600.0
        # Stale if the last evaluation is more than 36 hours old
        is_stale = hours_since > 36

    return EvaluationStats(
        total_evaluations=total,
        sources_evaluated=sources,
        last_run=last_run.isoformat() if last_run else None,
        last_signal_time=last_signal.isoformat() if last_signal else None,
        hours_since_last_run=round(hours_since, 1) if hours_since is not None else None,
        period_days=period_days,
        is_stale=is_stale,
    )


@router.post("/evaluate")
async def trigger_evaluation(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
    days: int = Query(default=90, ge=1, le=365),
):
    """Trigger a batch evaluation of signals from the past N days.

    Blocking akshare calls run in a thread pool so the event loop stays
    responsive. For very large windows consider running
    ``scripts/backtest_historical.py --step evaluate`` offline instead.
    """
    if user.role != "admin":
        from fastapi import HTTPException
        raise HTTPException(403, "Admin only")

    from backend.app.services.signal_evaluator import run_evaluation
    return await run_evaluation(db, days=days)


# ─── Helpers ──────────────────────────────────────────────────────

async def _compute_ic_from_db(
    db: AsyncSession,
    filters: list,
) -> dict[str, dict]:
    """Compute IC, ICIR, and avg_confidence per source from DB rows.

    Uses sentiment_score (continuous) correlated with actual returns.
    Falls back to direction-based proxy if sentiment_score is null.
    """
    from collections import defaultdict
    import warnings
    import numpy as np

    # Fetch raw rows for IC computation
    ic_stmt = (
        select(
            SignalEvaluation.source_name,
            SignalEvaluation.signal_time,
            SignalEvaluation.sentiment_score_t1,
            SignalEvaluation.sentiment_score_t5,
            SignalEvaluation.sentiment_score_t20,
            SignalEvaluation.return_t1,
            SignalEvaluation.return_t5,
            SignalEvaluation.return_t20,
            SignalEvaluation.confidence_t1,
            SignalEvaluation.confidence_t5,
            SignalEvaluation.predicted_sentiment,
            SignalEvaluation.predicted_sentiment_t1,
            SignalEvaluation.predicted_sentiment_t5,
            SignalEvaluation.predicted_sentiment_t20,
        )
        .where(and_(*filters))
    )
    ic_result = await db.execute(ic_stmt)
    ic_rows = ic_result.all()

    SENTIMENT_DIR = {"very_bullish": 1, "bullish": 1, "neutral": 0, "bearish": -1, "very_bearish": -1}

    def _get_score(row, horizon: str) -> float | None:
        """Get sentiment_score, falling back to direction × confidence."""
        score_attr = f"sentiment_score_{horizon}"
        score = getattr(row, score_attr, None)
        if score is not None:
            return float(score)
        # Fallback: synthesize from sentiment direction × confidence
        pred_attr = f"predicted_sentiment_{horizon}"
        pred = getattr(row, pred_attr, None) or row.predicted_sentiment
        d = SENTIMENT_DIR.get(pred, 0)
        if d == 0:
            return None
        conf_attr = f"confidence_{horizon}"
        conf = getattr(row, conf_attr, None)
        return d * (float(conf) if conf is not None else 0.5)

    by_source = defaultdict(list)
    for r in ic_rows:
        by_source[r.source_name].append(r)

    results = {}
    for source, evs in by_source.items():
        ic_per_horizon = {}

        for horizon, return_attr in [("t1", "return_t1"), ("t5", "return_t5"), ("t20", "return_t20")]:
            scores = []
            returns = []
            for ev in evs:
                s = _get_score(ev, horizon)
                r = getattr(ev, return_attr)
                if s is not None and r is not None:
                    scores.append(s)
                    returns.append(float(r))

            if len(scores) >= 5:
                try:
                    from scipy.stats import spearmanr
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        corr, _ = spearmanr(scores, returns)
                    ic_per_horizon[f"ic_{horizon}"] = round(float(corr), 4) if not np.isnan(corr) else None
                except Exception:
                    ic_per_horizon[f"ic_{horizon}"] = None
            else:
                ic_per_horizon[f"ic_{horizon}"] = None

        # ICIR: weekly IC stability for T+1
        weekly_ics = []
        by_week: dict[tuple, tuple[list, list]] = defaultdict(lambda: ([], []))
        for ev in evs:
            s = _get_score(ev, "t1")
            r = ev.return_t1
            if s is not None and r is not None:
                if hasattr(ev.signal_time, "isocalendar"):
                    wk = ev.signal_time.isocalendar()[:2]
                else:
                    wk = str(ev.signal_time)[:10]
                by_week[wk][0].append(s)
                by_week[wk][1].append(float(r))

        for wk, (wk_scores, wk_returns) in by_week.items():
            if len(wk_scores) >= 3:
                try:
                    from scipy.stats import spearmanr
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        corr, _ = spearmanr(wk_scores, wk_returns)
                    if not np.isnan(corr):
                        weekly_ics.append(corr)
                except Exception:
                    pass

        # ICIR needs at least 4 weeks of data to be meaningful. Use sample
        # std (ddof=1) so small samples aren't artificially pushed high.
        if len(weekly_ics) >= 4:
            std = float(np.std(weekly_ics, ddof=1))
            mean = float(np.mean(weekly_ics))
            icir = mean / std * np.sqrt(52) if std > 0 else 0.0
            ic_per_horizon["icir"] = round(float(icir), 3)
        else:
            ic_per_horizon["icir"] = None

        # Average confidence — prefer T+1, fall back to T+5. Use ``is not None``
        # so a valid 0.0 confidence doesn't get treated as missing.
        confs = []
        for ev in evs:
            c = ev.confidence_t1 if ev.confidence_t1 is not None else ev.confidence_t5
            if c is not None:
                confs.append(float(c))
        ic_per_horizon["avg_confidence"] = round(float(np.mean(confs)), 3) if confs else None

        results[source] = ic_per_horizon

    return results


def _accuracy_expr(column_name: str):
    """Build a SQL expression for accuracy: count(True) / count(NOT NULL)."""
    col = getattr(SignalEvaluation, column_name)
    return func.cast(
        func.sum(case((col == True, 1), else_=0)),
        Float,
    ) / func.nullif(
        func.sum(case((col.isnot(None), 1), else_=0)),
        0,
    )


def _cw_accuracy_expr(correct_col: str, confidence_col: str):
    """Confidence-weighted accuracy: sum(conf where correct) / sum(conf where evaluated)."""
    correct = getattr(SignalEvaluation, correct_col)
    conf = getattr(SignalEvaluation, confidence_col)
    return func.cast(
        func.sum(case((correct == True, func.coalesce(conf, 1.0)), else_=literal(0.0))), Float
    ) / func.nullif(
        func.sum(case((correct.isnot(None), func.coalesce(conf, 1.0)), else_=literal(0.0))), 0
    )


def _safe_float(val) -> float | None:
    if val is None:
        return None
    try:
        return round(float(val), 4)
    except (TypeError, ValueError):
        return None
