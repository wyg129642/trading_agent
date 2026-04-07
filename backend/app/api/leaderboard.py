"""Source Leaderboard API — ranks news sources by signal accuracy.

Compares predicted sentiment (bullish/bearish) against actual stock price
movements at T+0 (same day), T+1, T+5, T+20 horizons.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy import select, func, case, and_, text, literal, Float
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
    period_days: int


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
        filters.append(SignalEvaluation.confidence_t5 >= min_confidence)
    if min_score > 0:
        # Filter on sentiment_score if available, otherwise skip (don't exclude nulls)
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

    # Compute composite score and build entries
    entries = []
    for row in rows:
        accs = []
        weights = []
        for acc, w in [
            (row.accuracy_t0, 0.1),
            (row.accuracy_t1, 0.3),
            (row.accuracy_t5, 0.35),
            (row.accuracy_t20, 0.25),
        ]:
            if acc is not None:
                accs.append(float(acc) * w)
                weights.append(w)
        composite = sum(accs) / sum(weights) if weights else 0.0

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
    min_signals: int = Query(default=2, ge=1, le=20, description="Minimum signals to rank"),
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
        if horizon == "t5":
            filters.append(SignalEvaluation.confidence_t5 >= min_confidence)
        else:
            filters.append(SignalEvaluation.confidence_t1 >= min_confidence)
    if min_score > 0:
        if horizon == "t5":
            score_col = SignalEvaluation.sentiment_score_t5
        else:
            score_col = SignalEvaluation.sentiment_score_t1
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

    entries = []
    for row in rows:
        # Quick composite: weighted by selected horizon
        if horizon == "t5":
            weights = [(row.accuracy_t0, 0.1), (row.accuracy_t1, 0.3), (row.accuracy_t5, 0.6)]
        else:
            weights = [(row.accuracy_t0, 0.1), (row.accuracy_t1, 0.7), (row.accuracy_t5, 0.2)]

        accs = []
        ws = []
        for acc, w in weights:
            if acc is not None:
                accs.append(float(acc) * w)
                ws.append(w)
        composite = sum(accs) / sum(ws) if ws else 0.0

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

    return LeaderboardResponse(
        entries=ranked,
        total_sources=len(ranked),
        total_signals=total_signals,
        period_start=cutoff.isoformat(),
        period_end=datetime.now(timezone.utc).isoformat(),
    )


@router.get("/stats", response_model=EvaluationStats)
async def get_evaluation_stats(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Get evaluation statistics."""
    total = await db.scalar(select(func.count()).select_from(SignalEvaluation)) or 0
    sources = await db.scalar(
        select(func.count(func.distinct(SignalEvaluation.source_name)))
    ) or 0
    last_run = await db.scalar(select(func.max(SignalEvaluation.evaluated_at)))

    # Determine period from oldest to newest evaluation
    oldest = await db.scalar(select(func.min(SignalEvaluation.signal_time)))
    period_days = (datetime.now(timezone.utc) - oldest).days if oldest else 0

    return EvaluationStats(
        total_evaluations=total,
        sources_evaluated=sources,
        last_run=last_run.isoformat() if last_run else None,
        period_days=period_days,
    )


@router.post("/evaluate")
async def trigger_evaluation(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
    days: int = Query(default=90, ge=7, le=365),
):
    """Trigger a batch evaluation of signals from the past N days.

    This runs synchronously for small batches. For large evaluations,
    consider running the engine/analysis/source_scorer.py offline.
    """
    if user.role not in ("admin",):
        from fastapi import HTTPException
        raise HTTPException(403, "Admin only")

    from backend.app.models.news import NewsItem, AnalysisResult
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    NON_NEUTRAL = ["bullish", "very_bullish", "bearish", "very_bearish"]

    # Fetch analyzed signals — include items with per-stock sentiment OR global non-neutral
    stmt = (
        select(
            NewsItem.id,
            NewsItem.source_name,
            NewsItem.published_at,
            NewsItem.fetched_at,
            NewsItem.market,
            AnalysisResult.sentiment,
            AnalysisResult.affected_tickers,
            AnalysisResult.ticker_sentiments,
        )
        .join(AnalysisResult, NewsItem.id == AnalysisResult.news_item_id)
        .where(
            and_(
                NewsItem.fetched_at >= cutoff,
                AnalysisResult.affected_tickers.isnot(None),
                # Include if global sentiment is non-neutral OR ticker_sentiments is populated
                text("""(
                    analysis_results.sentiment IN ('bullish','very_bullish','bearish','very_bearish')
                    OR (analysis_results.ticker_sentiments IS NOT NULL
                        AND analysis_results.ticker_sentiments != '{}'::jsonb)
                )"""),
            )
        )
        .order_by(NewsItem.fetched_at.desc())
    )
    result = await db.execute(stmt)
    rows = result.all()

    if not rows:
        return {"message": "No signals to evaluate", "evaluated": 0}

    # Get already-evaluated (news_item_id, ticker) pairs and their null-horizon status
    existing_stmt = select(
        SignalEvaluation.news_item_id,
        SignalEvaluation.ticker,
        SignalEvaluation.id,
        SignalEvaluation.correct_t1,
        SignalEvaluation.correct_t5,
        SignalEvaluation.correct_t20,
    )
    existing_result = await db.execute(existing_stmt)
    # Map (news_item_id, ticker) -> {id, has_null_horizons}
    existing_evals: dict[tuple[str, str], dict] = {}
    for r in existing_result.all():
        key = (r.news_item_id, r.ticker)
        existing_evals[key] = {
            "id": r.id,
            "missing_t1": r.correct_t1 is None,
            "missing_t5": r.correct_t5 is None,
            "missing_t20": r.correct_t20 is None,
        }

    # Build signal list (one per ticker per news item)
    from engine.analysis.source_scorer import evaluate_signal

    # Load source categories from config
    from backend.app.api.sources import _load_sources_yaml, _load_portfolio_yaml
    source_cats = {}
    for s in _load_sources_yaml() + _load_portfolio_yaml():
        source_cats[s.get("name", "")] = s.get("category", s.get("group", ""))

    evaluated = 0
    skipped = 0
    updated = 0
    errors = 0

    for row in rows:
        news_id = row.id

        tickers = row.affected_tickers or []
        if isinstance(tickers, str):
            tickers = [tickers]

        signal_time = row.published_at or row.fetched_at
        if not signal_time:
            continue

        # Ensure signal_time is timezone-aware
        if signal_time.tzinfo is None:
            signal_time = signal_time.replace(tzinfo=timezone.utc)

        market = row.market or "global"

        # Per-stock sentiment dict: {"ticker_display": "bullish", ...}
        ticker_sents = row.ticker_sentiments or {}
        if isinstance(ticker_sents, str):
            import json as _json
            try:
                ticker_sents = _json.loads(ticker_sents)
            except (ValueError, TypeError):
                ticker_sents = {}

        for ticker_entry in tickers[:3]:  # limit to first 3 tickers
            ticker = ticker_entry if isinstance(ticker_entry, str) else str(ticker_entry)
            if not ticker or len(ticker) < 2:
                continue

            # Resolve per-stock sentiment: look up in ticker_sentiments dict,
            # matching by exact key, or by ticker appearing in the key
            stock_sentiment = None
            if ticker_sents:
                # Try exact match first
                stock_sentiment = ticker_sents.get(ticker)
                if not stock_sentiment:
                    # Try partial match (ticker key might be "Name(Code)")
                    for ts_key, ts_val in ticker_sents.items():
                        if ticker in ts_key or ts_key in ticker:
                            stock_sentiment = ts_val
                            break
            # Fall back to global sentiment
            if not stock_sentiment:
                stock_sentiment = row.sentiment

            # Resolve multi-horizon predictions
            pred_t1 = pred_t5 = pred_t20 = None
            score_t1 = score_t5 = score_t20 = None
            conf_t1 = conf_t5 = conf_t20 = None

            if isinstance(stock_sentiment, dict):
                # New multi-horizon format
                st = stock_sentiment.get("short_term", {})
                mt = stock_sentiment.get("medium_term", {})
                lt = stock_sentiment.get("long_term", {})
                pred_t1 = st.get("sentiment") if isinstance(st, dict) else None
                pred_t5 = mt.get("sentiment") if isinstance(mt, dict) else None
                pred_t20 = lt.get("sentiment") if isinstance(lt, dict) else None
                score_t1 = st.get("sentiment_score") if isinstance(st, dict) else None
                score_t5 = mt.get("sentiment_score") if isinstance(mt, dict) else None
                score_t20 = lt.get("sentiment_score") if isinstance(lt, dict) else None
                conf_t1 = st.get("confidence") if isinstance(st, dict) else None
                conf_t5 = mt.get("confidence") if isinstance(mt, dict) else None
                conf_t20 = lt.get("confidence") if isinstance(lt, dict) else None
                # Use short-term as overall prediction
                overall_sentiment = pred_t1 or row.sentiment
            elif isinstance(stock_sentiment, str):
                # Old flat format — replicate across horizons
                overall_sentiment = stock_sentiment
                pred_t1 = pred_t5 = pred_t20 = stock_sentiment
            else:
                overall_sentiment = row.sentiment
                pred_t1 = pred_t5 = pred_t20 = row.sentiment

            # Skip neutral signals
            if overall_sentiment not in NON_NEUTRAL:
                continue

            # Check if this (news_item_id, ticker) was already evaluated
            eval_key = (news_id, ticker)
            existing = existing_evals.get(eval_key)
            if existing and not existing["missing_t1"] and not existing["missing_t5"] and not existing["missing_t20"]:
                skipped += 1
                continue  # fully evaluated, skip

            try:
                ev = evaluate_signal(signal_time, overall_sentiment, ticker, market)
                if ev["price_at_signal"] is None:
                    continue  # couldn't get price data

                # Re-compute per-horizon correctness using per-horizon predictions
                from engine.analysis.source_scorer import SENTIMENT_DIRECTION

                correct_t1 = ev["correct_t1"]
                if pred_t1 and ev["return_t1"] is not None:
                    d = SENTIMENT_DIRECTION.get(pred_t1, 0)
                    if d != 0:
                        correct_t1 = (d > 0 and ev["return_t1"] > 0) or (d < 0 and ev["return_t1"] < 0)
                    else:
                        correct_t1 = None

                correct_t5 = ev["correct_t5"]
                if pred_t5 and ev["return_t5"] is not None:
                    d = SENTIMENT_DIRECTION.get(pred_t5, 0)
                    if d != 0:
                        correct_t5 = (d > 0 and ev["return_t5"] > 0) or (d < 0 and ev["return_t5"] < 0)
                    else:
                        correct_t5 = None

                correct_t20 = ev["correct_t20"]
                if pred_t20 and ev["return_t20"] is not None:
                    d = SENTIMENT_DIRECTION.get(pred_t20, 0)
                    if d != 0:
                        correct_t20 = (d > 0 and ev["return_t20"] > 0) or (d < 0 and ev["return_t20"] < 0)
                    else:
                        correct_t20 = None

                # Synthesize sentiment_score from direction × confidence
                def _synth_score(sentiment: str | None, confidence: float | None) -> float | None:
                    if not sentiment:
                        return None
                    d = SENTIMENT_DIRECTION.get(sentiment, 0)
                    if d == 0:
                        return 0.0
                    c = confidence if confidence is not None else 0.5
                    return round(d * c, 4)

                if score_t1 is None:
                    score_t1 = _synth_score(pred_t1, conf_t1)
                if score_t5 is None:
                    score_t5 = _synth_score(pred_t5, conf_t5)
                if score_t20 is None:
                    score_t20 = _synth_score(pred_t20, conf_t20)

                if existing:
                    # Update existing row with newly available horizon data
                    from sqlalchemy import update as sql_update
                    update_fields = {}
                    if existing["missing_t1"] and ev["return_t1"] is not None:
                        update_fields["return_t1"] = ev["return_t1"]
                        update_fields["correct_t1"] = correct_t1
                    if existing["missing_t5"] and ev["return_t5"] is not None:
                        update_fields["return_t5"] = ev["return_t5"]
                        update_fields["correct_t5"] = correct_t5
                    if existing["missing_t20"] and ev["return_t20"] is not None:
                        update_fields["return_t20"] = ev["return_t20"]
                        update_fields["correct_t20"] = correct_t20
                    # Always update sentiment_score if still null
                    if score_t1 is not None:
                        update_fields["sentiment_score_t1"] = score_t1
                    if score_t5 is not None:
                        update_fields["sentiment_score_t5"] = score_t5
                    if score_t20 is not None:
                        update_fields["sentiment_score_t20"] = score_t20
                    update_fields["evaluated_at"] = datetime.now(timezone.utc)

                    if update_fields:
                        await db.execute(
                            sql_update(SignalEvaluation)
                            .where(SignalEvaluation.id == existing["id"])
                            .values(**update_fields)
                        )
                        updated += 1
                else:
                    db.add(SignalEvaluation(
                        news_item_id=news_id,
                        source_name=row.source_name,
                        category=source_cats.get(row.source_name, ""),
                        ticker=ticker,
                        market=market,
                        signal_time=signal_time,
                        predicted_sentiment=overall_sentiment,
                        predicted_sentiment_t1=pred_t1,
                        predicted_sentiment_t5=pred_t5,
                        predicted_sentiment_t20=pred_t20,
                        sentiment_score_t1=score_t1,
                        sentiment_score_t5=score_t5,
                        sentiment_score_t20=score_t20,
                        confidence_t1=conf_t1,
                        confidence_t5=conf_t5,
                        confidence_t20=conf_t20,
                        price_at_signal=ev["price_at_signal"],
                        return_t0=ev["return_t0"],
                        return_t1=ev["return_t1"],
                        return_t5=ev["return_t5"],
                        return_t20=ev["return_t20"],
                        correct_t0=ev["correct_t0"],
                        correct_t1=correct_t1,
                        correct_t5=correct_t5,
                        correct_t20=correct_t20,
                    ))
                    evaluated += 1
            except Exception as e:
                errors += 1
                logger.warning("Eval error for %s/%s: %s", news_id, ticker, e)

        # Commit in batches of 50
        if (evaluated + updated) > 0 and (evaluated + updated) % 50 == 0:
            await db.commit()

    await db.commit()
    return {
        "message": "Evaluation complete",
        "evaluated": evaluated,
        "updated": updated,
        "skipped": skipped,
        "errors": errors,
        "total_signals_found": len(rows),
    }


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

        if len(weekly_ics) >= 2:
            std = float(np.std(weekly_ics))
            icir = float(np.mean(weekly_ics)) / std * np.sqrt(52) if std > 0 else 0.0
            ic_per_horizon["icir"] = round(float(icir), 3)
        else:
            ic_per_horizon["icir"] = None

        # Average confidence
        confs = []
        for ev in evs:
            c = ev.confidence_t1 or ev.confidence_t5
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
