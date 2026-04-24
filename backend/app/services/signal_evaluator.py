"""Signal evaluation service — populates signal_evaluations from news_items.

Shared logic used by:
  * POST /api/leaderboard/evaluate (admin trigger)
  * BacktestScheduler daily run

Extracted from backend/app/api/leaderboard.py so the scheduler and API both
use the same code path. Blocking akshare calls are wrapped in asyncio.to_thread
so the backend event loop stays responsive while prices are being fetched.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import and_, select, text, update as sql_update
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.models.leaderboard import SignalEvaluation
from backend.app.models.news import AnalysisResult, NewsItem

logger = logging.getLogger(__name__)

NON_NEUTRAL = {"bullish", "very_bullish", "bearish", "very_bearish"}


def _load_source_categories() -> dict[str, str]:
    """Load {source_name: category} from sources.yaml + portfolio.yaml."""
    from backend.app.api.sources import _load_sources_yaml, _load_portfolio_yaml
    cats: dict[str, str] = {}
    for s in _load_sources_yaml() + _load_portfolio_yaml():
        name = s.get("name", "")
        if not name:
            continue
        cats[name] = s.get("category", s.get("group", ""))
    return cats


def _resolve_stock_sentiment(ticker_sents: dict, ticker: str) -> Any:
    """Look up per-stock sentiment from ticker_sentiments dict.

    Tries exact match first, then partial (key like ``Name(Code)``).
    Returns either a dict (multi-horizon), a str (legacy flat), or None.
    """
    if not ticker_sents:
        return None
    hit = ticker_sents.get(ticker)
    if hit:
        return hit
    for k, v in ticker_sents.items():
        if ticker in k or k in ticker:
            return v
    return None


def _extract_horizon_predictions(stock_sentiment: Any, fallback_sentiment: str | None):
    """Unpack multi-horizon predictions from a sentiment entry.

    Returns (overall_sentiment, pred_t1, pred_t5, pred_t20,
             score_t1, score_t5, score_t20, conf_t1, conf_t5, conf_t20).
    """
    pred_t1 = pred_t5 = pred_t20 = None
    score_t1 = score_t5 = score_t20 = None
    conf_t1 = conf_t5 = conf_t20 = None

    if isinstance(stock_sentiment, dict):
        st = stock_sentiment.get("short_term") or {}
        mt = stock_sentiment.get("medium_term") or {}
        lt = stock_sentiment.get("long_term") or {}
        if isinstance(st, dict):
            pred_t1 = st.get("sentiment")
            score_t1 = st.get("sentiment_score")
            conf_t1 = st.get("confidence")
        if isinstance(mt, dict):
            pred_t5 = mt.get("sentiment")
            score_t5 = mt.get("sentiment_score")
            conf_t5 = mt.get("confidence")
        if isinstance(lt, dict):
            pred_t20 = lt.get("sentiment")
            score_t20 = lt.get("sentiment_score")
            conf_t20 = lt.get("confidence")
        overall = pred_t1 or pred_t5 or pred_t20 or fallback_sentiment
    elif isinstance(stock_sentiment, str):
        overall = stock_sentiment
        pred_t1 = pred_t5 = pred_t20 = stock_sentiment
    else:
        overall = fallback_sentiment
        pred_t1 = pred_t5 = pred_t20 = fallback_sentiment

    return (overall, pred_t1, pred_t5, pred_t20,
            score_t1, score_t5, score_t20,
            conf_t1, conf_t5, conf_t20)


def _synth_score(sentiment: str | None, confidence: float | None) -> float | None:
    """direction × confidence, used when the LLM didn't emit sentiment_score."""
    from engine.analysis.source_scorer import SENTIMENT_DIRECTION
    if not sentiment:
        return None
    d = SENTIMENT_DIRECTION.get(sentiment, 0)
    if d == 0:
        return 0.0
    c = confidence if confidence is not None else 0.5
    return round(d * c, 4)


def _per_horizon_correct(predicted: str | None, actual_return: float | None) -> bool | None:
    """Compute correctness for a specific horizon prediction."""
    if not predicted or actual_return is None:
        return None
    from engine.analysis.source_scorer import SENTIMENT_DIRECTION
    d = SENTIMENT_DIRECTION.get(predicted, 0)
    if d == 0:
        return None
    return (d > 0 and actual_return > 0) or (d < 0 and actual_return < 0)


async def run_evaluation(
    db: AsyncSession,
    days: int,
    max_tickers_per_news: int = 3,
    commit_every: int = 50,
) -> dict:
    """Populate/refresh signal_evaluations for the last ``days`` days.

    Safe to call repeatedly — skips rows that are already fully evaluated
    and fills in missing-horizon rows when more price data becomes available.

    Returns a dict with counts: evaluated / updated / skipped / errors / total.
    """
    from engine.analysis.source_scorer import (
        MARKET_TZ_OFFSETS, SENTIMENT_DIRECTION,
        _find_trading_day_index, _signal_utc_to_trading_date,
        fetch_price_series, infer_market_from_ticker,
    )

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    logger.info("Signal evaluation starting — cutoff=%s (days=%d)", cutoff.isoformat(), days)

    # Per-run price cache: {(ticker, market): DataFrame|None}.
    # Without this, evaluating 1000 news items about the same stock hits
    # akshare 1000 times. The cache cuts price fetches to one per unique
    # (ticker, market) pair per run, which is the dominant cost.
    price_cache: dict[tuple[str, str], Any] = {}

    def _cached_evaluate(signal_time_: datetime, predicted: str, ticker_: str, market_: str) -> dict:
        """Drop-in replacement for evaluate_signal that reuses cached price series."""
        result = {
            "price_at_signal": None,
            "return_t0": None, "return_t1": None, "return_t5": None, "return_t20": None,
            "correct_t0": None, "correct_t1": None, "correct_t5": None, "correct_t20": None,
        }
        direction = SENTIMENT_DIRECTION.get(predicted, 0)
        if direction == 0:
            return result

        inferred = infer_market_from_ticker(ticker_, fallback=market_)
        if inferred in ("china", "us", "hk") and inferred != market_:
            market_ = inferred

        trading_date = _signal_utc_to_trading_date(signal_time_, market_)

        cache_key = (ticker_, market_)
        prices = price_cache.get(cache_key)
        if cache_key not in price_cache:
            # Fetch a wide window so the same cache entry can serve many
            # signals about this ticker across the run.
            start = (trading_date - timedelta(days=45)).strftime("%Y%m%d")
            end = (trading_date + timedelta(days=60)).strftime("%Y%m%d")
            prices = fetch_price_series(ticker_, market_, start, end)
            price_cache[cache_key] = prices
        if prices is None or len(prices) < 2:
            return result

        sig_idx = _find_trading_day_index(prices, trading_date)
        if sig_idx is None:
            return result

        import pandas as _pd  # noqa: F401 (ensure pandas is loaded in this thread)
        signal_close = prices.loc[sig_idx, "close"]
        signal_open = prices.loc[sig_idx, "open"]
        if signal_close is None:
            return result
        result["price_at_signal"] = float(signal_close)

        if signal_open and signal_open > 0:
            ret_t0 = (signal_close - signal_open) / signal_open
            result["return_t0"] = float(ret_t0)
            result["correct_t0"] = (
                (direction > 0 and ret_t0 > 0) or (direction < 0 and ret_t0 < 0)
            )

        for horizon_name, n_days in [("t1", 1), ("t5", 5), ("t20", 20)]:
            future_idx = sig_idx + n_days
            if future_idx < len(prices):
                future_close = prices.loc[future_idx, "close"]
                if signal_close and signal_close > 0 and future_close is not None:
                    ret = (future_close - signal_close) / signal_close
                    result[f"return_{horizon_name}"] = float(ret)
                    result[f"correct_{horizon_name}"] = (
                        (direction > 0 and ret > 0) or (direction < 0 and ret < 0)
                    )
        return result

    _ = MARKET_TZ_OFFSETS  # silence "unused import" — kept for symmetry with scorer

    # Fetch candidate news items from the cutoff window
    news_stmt = (
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
                text(
                    "analysis_results.sentiment IN ('bullish','very_bullish','bearish','very_bearish')"
                    " OR (analysis_results.ticker_sentiments IS NOT NULL"
                    " AND analysis_results.ticker_sentiments::text != '{}'::text)"
                ),
            )
        )
        .order_by(NewsItem.fetched_at.desc())
    )
    rows = (await db.execute(news_stmt)).all()
    if not rows:
        return {"message": "No signals to evaluate", "evaluated": 0,
                "updated": 0, "skipped": 0, "errors": 0, "total_signals_found": 0}

    # Load existing evals only within the same cutoff window (scalability)
    existing_stmt = (
        select(
            SignalEvaluation.news_item_id,
            SignalEvaluation.ticker,
            SignalEvaluation.id,
            SignalEvaluation.correct_t1,
            SignalEvaluation.correct_t5,
            SignalEvaluation.correct_t20,
        )
        .where(SignalEvaluation.signal_time >= cutoff)
    )
    existing_evals: dict[tuple[str, str], dict] = {}
    for r in (await db.execute(existing_stmt)).all():
        existing_evals[(r.news_item_id, r.ticker)] = {
            "id": r.id,
            "missing_t1": r.correct_t1 is None,
            "missing_t5": r.correct_t5 is None,
            "missing_t20": r.correct_t20 is None,
        }

    source_cats = _load_source_categories()

    evaluated = updated = skipped = errors = 0
    no_price = 0
    flush_counter = 0

    for row in rows:
        news_id = row.id
        signal_time = row.published_at or row.fetched_at
        if not signal_time:
            continue
        if signal_time.tzinfo is None:
            signal_time = signal_time.replace(tzinfo=timezone.utc)

        market = row.market or "global"

        tickers = row.affected_tickers or []
        if isinstance(tickers, str):
            try:
                tickers = json.loads(tickers)
            except (ValueError, TypeError):
                tickers = [tickers]
        if not isinstance(tickers, list):
            continue

        ticker_sents = row.ticker_sentiments or {}
        if isinstance(ticker_sents, str):
            try:
                ticker_sents = json.loads(ticker_sents)
            except (ValueError, TypeError):
                ticker_sents = {}
        if not isinstance(ticker_sents, dict):
            ticker_sents = {}

        from engine.analysis.source_scorer import _extract_raw_code

        for ticker_entry in tickers[:max_tickers_per_news]:
            raw_ticker = ticker_entry if isinstance(ticker_entry, str) else str(ticker_entry)
            if not raw_ticker or len(raw_ticker) < 2:
                continue

            # Persist the normalized code (column is VARCHAR(20)); keep the
            # raw label around so ticker_sentiments lookup still works.
            normalized = _extract_raw_code(raw_ticker)[:20]
            if len(normalized) < 2:
                continue

            stock_sentiment = _resolve_stock_sentiment(ticker_sents, raw_ticker) or row.sentiment
            (overall_sentiment, pred_t1, pred_t5, pred_t20,
             score_t1, score_t5, score_t20,
             conf_t1, conf_t5, conf_t20) = _extract_horizon_predictions(
                stock_sentiment, row.sentiment)

            if overall_sentiment not in NON_NEUTRAL:
                continue

            # Let the ticker suffix override the news-level market so HK/US
            # names inside Chinese news items route to the right exchange.
            from engine.analysis.source_scorer import infer_market_from_ticker
            effective_market = infer_market_from_ticker(normalized, fallback=market)
            if effective_market not in ("china", "us", "hk"):
                effective_market = market  # keep 'global' / unknown as-is

            eval_key = (news_id, normalized)
            existing = existing_evals.get(eval_key)
            if (existing
                    and not existing["missing_t1"]
                    and not existing["missing_t5"]
                    and not existing["missing_t20"]):
                skipped += 1
                continue

            try:
                # Run blocking akshare fetch off the event loop
                ev = await asyncio.to_thread(
                    _cached_evaluate, signal_time, overall_sentiment, normalized, effective_market,
                )
            except Exception as e:
                errors += 1
                logger.warning("Eval error for %s/%s: %s", news_id, ticker, e)
                continue

            if ev["price_at_signal"] is None:
                # Couldn't get price data — don't count as an error (e.g. unknown market)
                no_price += 1
                continue

            correct_t1 = _per_horizon_correct(pred_t1, ev["return_t1"])
            correct_t5 = _per_horizon_correct(pred_t5, ev["return_t5"])
            correct_t20 = _per_horizon_correct(pred_t20, ev["return_t20"])

            if score_t1 is None:
                score_t1 = _synth_score(pred_t1, conf_t1)
            if score_t5 is None:
                score_t5 = _synth_score(pred_t5, conf_t5)
            if score_t20 is None:
                score_t20 = _synth_score(pred_t20, conf_t20)

            if existing:
                update_fields: dict[str, Any] = {}
                if existing["missing_t1"] and ev["return_t1"] is not None:
                    update_fields["return_t1"] = ev["return_t1"]
                    update_fields["correct_t1"] = correct_t1
                if existing["missing_t5"] and ev["return_t5"] is not None:
                    update_fields["return_t5"] = ev["return_t5"]
                    update_fields["correct_t5"] = correct_t5
                if existing["missing_t20"] and ev["return_t20"] is not None:
                    update_fields["return_t20"] = ev["return_t20"]
                    update_fields["correct_t20"] = correct_t20
                if score_t1 is not None:
                    update_fields["sentiment_score_t1"] = score_t1
                if score_t5 is not None:
                    update_fields["sentiment_score_t5"] = score_t5
                if score_t20 is not None:
                    update_fields["sentiment_score_t20"] = score_t20
                if update_fields:
                    update_fields["evaluated_at"] = datetime.now(timezone.utc)
                    await db.execute(
                        sql_update(SignalEvaluation)
                        .where(SignalEvaluation.id == existing["id"])
                        .values(**update_fields)
                    )
                    updated += 1
                    flush_counter += 1
            else:
                db.add(SignalEvaluation(
                    news_item_id=news_id,
                    source_name=(row.source_name or "")[:200],
                    category=(source_cats.get(row.source_name, "") or "")[:50],
                    ticker=normalized,
                    market=(effective_market or "global")[:20],
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
                flush_counter += 1

            if flush_counter >= commit_every:
                await db.commit()
                flush_counter = 0

    if flush_counter:
        await db.commit()

    summary = {
        "message": "Evaluation complete",
        "evaluated": evaluated,
        "updated": updated,
        "skipped": skipped,
        "no_price_data": no_price,
        "errors": errors,
        "total_signals_found": len(rows),
        "unique_tickers": len(price_cache),
    }
    logger.info("Signal evaluation finished: %s", summary)
    return summary
