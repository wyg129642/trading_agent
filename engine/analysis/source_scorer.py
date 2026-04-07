"""Source accuracy scorer — evaluates signal quality by comparing
predicted sentiment against actual stock price movements.

Uses akshare for free stock price data across markets:
  - China A-shares: ak.stock_zh_a_hist
  - US stocks: ak.stock_us_daily
  - HK stocks: ak.stock_hk_daily

Time horizons: T+0 (same day), T+1, T+5, T+20 (≈1 month).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import NamedTuple

import akshare as ak
import pandas as pd

logger = logging.getLogger(__name__)

# Market timezone offsets (hours from UTC)
MARKET_TZ_OFFSETS = {
    "china": 8,      # Asia/Shanghai  (CST)
    "us":   -5,      # America/New_York (EST, approximate — ignoring DST here)
    "hk":    8,      # Asia/Hong_Kong
    "global": 0,     # UTC fallback
    "kr":    9,      # Asia/Seoul
    "jp":    9,      # Asia/Tokyo
}

# Horizons: name → number of trading days
HORIZONS = {"t0": 0, "t1": 1, "t5": 5, "t20": 20}

# Sentiment direction: +1 = bullish, -1 = bearish, 0 = neutral
SENTIMENT_DIRECTION = {
    "very_bullish": 1,
    "bullish": 1,
    "neutral": 0,
    "bearish": -1,
    "very_bearish": -1,
}


class SignalEvaluation(NamedTuple):
    news_item_id: str
    source_name: str
    ticker: str
    market: str
    signal_time: datetime       # UTC
    predicted_sentiment: str
    price_at_signal: float | None
    return_t0: float | None
    return_t1: float | None
    return_t5: float | None
    return_t20: float | None
    correct_t0: bool | None
    correct_t1: bool | None
    correct_t5: bool | None
    correct_t20: bool | None


class SourceLeaderboardEntry(NamedTuple):
    source_name: str
    category: str
    total_signals: int
    accuracy_t0: float | None
    accuracy_t1: float | None
    accuracy_t5: float | None
    accuracy_t20: float | None
    avg_return_bullish: float | None
    avg_return_bearish: float | None
    timeliness_score: float | None
    composite_score: float


# ─── Price data fetchers ────────────────────────────────────────────

def _normalize_ticker_cn(ticker: str) -> str:
    """Normalize A-share ticker to 6-digit string."""
    t = ticker.strip().replace(".SZ", "").replace(".SS", "").replace(".SH", "")
    return t.zfill(6) if t.isdigit() else t


def _normalize_ticker_hk(ticker: str) -> str:
    """Normalize HK ticker: 06869.HK → 06869."""
    t = ticker.strip().replace(".HK", "").replace(".hk", "")
    return t.zfill(5) if t.isdigit() else t


def _unproxy_context():
    """Context manager to temporarily remove HTTP proxy env vars.

    akshare uses requests which picks up HTTP_PROXY. For China-domestic
    endpoints (eastmoney.com), we need direct access without proxy.
    """
    import os
    from contextlib import contextmanager

    @contextmanager
    def _ctx():
        saved = {}
        for key in ("HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy"):
            if key in os.environ:
                saved[key] = os.environ.pop(key)
        try:
            yield
        finally:
            os.environ.update(saved)

    return _ctx()


def fetch_price_series(
    ticker: str,
    market: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame | None:
    """Fetch daily OHLCV for a ticker. Returns DataFrame with columns:
    [date, open, close] indexed by date, or None on failure.

    Parameters:
        start_date, end_date: 'YYYYMMDD' format
    """
    try:
        if market == "china":
            sym = _normalize_ticker_cn(ticker)
            with _unproxy_context():
                df = ak.stock_zh_a_hist(
                    symbol=sym, period="daily",
                    start_date=start_date, end_date=end_date, adjust="qfq",
                )
            if df is None or df.empty:
                return None
            df = df.rename(columns={"日期": "date", "开盘": "open", "收盘": "close"})
            df["date"] = pd.to_datetime(df["date"])
            return df[["date", "open", "close"]].sort_values("date").reset_index(drop=True)

        elif market == "us":
            df = ak.stock_us_daily(symbol=ticker.upper(), adjust="qfq")
            if df is None or df.empty:
                return None
            df["date"] = pd.to_datetime(df["date"])
            sd = pd.to_datetime(start_date)
            ed = pd.to_datetime(end_date)
            df = df[(df["date"] >= sd) & (df["date"] <= ed)]
            return df[["date", "open", "close"]].sort_values("date").reset_index(drop=True)

        elif market in ("hk",):
            sym = _normalize_ticker_hk(ticker)
            df = ak.stock_hk_daily(symbol=sym, adjust="qfq")
            if df is None or df.empty:
                return None
            df["date"] = pd.to_datetime(df["date"])
            sd = pd.to_datetime(start_date)
            ed = pd.to_datetime(end_date)
            df = df[(df["date"] >= sd) & (df["date"] <= ed)]
            return df[["date", "open", "close"]].sort_values("date").reset_index(drop=True)

        else:
            logger.debug("Unsupported market %s for ticker %s", market, ticker)
            return None

    except Exception as e:
        logger.warning("Failed to fetch price for %s (%s): %s", ticker, market, e)
        return None


# ─── Signal evaluation logic ────────────────────────────────────────

def _signal_utc_to_trading_date(signal_utc: datetime, market: str) -> datetime:
    """Convert a UTC signal timestamp to the local trading date."""
    offset_hours = MARKET_TZ_OFFSETS.get(market, 0)
    local_time = signal_utc + timedelta(hours=offset_hours)
    return local_time.replace(hour=0, minute=0, second=0, microsecond=0)


def _find_trading_day_index(prices: pd.DataFrame, target_date: datetime) -> int | None:
    """Find the index of the first trading day on or after target_date."""
    for i, row in prices.iterrows():
        if row["date"].date() >= target_date.date():
            return i
    return None


def evaluate_signal(
    signal_time: datetime,
    predicted_sentiment: str,
    ticker: str,
    market: str,
) -> dict:
    """Evaluate a single signal against actual price data.

    Returns dict with keys:
        price_at_signal, return_t0, return_t1, return_t5, return_t20,
        correct_t0, correct_t1, correct_t5, correct_t20
    """
    result = {
        "price_at_signal": None,
        "return_t0": None, "return_t1": None, "return_t5": None, "return_t20": None,
        "correct_t0": None, "correct_t1": None, "correct_t5": None, "correct_t20": None,
    }

    direction = SENTIMENT_DIRECTION.get(predicted_sentiment, 0)
    if direction == 0:
        return result  # neutral — skip

    # Determine the trading date of the signal in market local time
    trading_date = _signal_utc_to_trading_date(signal_time, market)

    # Fetch price data: from 5 days before signal to 30 days after
    start = (trading_date - timedelta(days=5)).strftime("%Y%m%d")
    end = (trading_date + timedelta(days=45)).strftime("%Y%m%d")  # extra buffer for weekends/holidays

    prices = fetch_price_series(ticker, market, start, end)
    if prices is None or len(prices) < 2:
        return result

    # Find the signal day in the price series
    sig_idx = _find_trading_day_index(prices, trading_date)
    if sig_idx is None:
        return result

    signal_close = prices.loc[sig_idx, "close"]
    signal_open = prices.loc[sig_idx, "open"]
    result["price_at_signal"] = float(signal_close)

    # T+0: same-day return (close vs open)
    if signal_open and signal_open > 0:
        ret_t0 = (signal_close - signal_open) / signal_open
        result["return_t0"] = float(ret_t0)
        result["correct_t0"] = (direction > 0 and ret_t0 > 0) or (direction < 0 and ret_t0 < 0)

    # T+N: future close vs signal close
    for horizon_name, n_days in [("t1", 1), ("t5", 5), ("t20", 20)]:
        future_idx = sig_idx + n_days
        if future_idx < len(prices):
            future_close = prices.loc[future_idx, "close"]
            if signal_close and signal_close > 0:
                ret = (future_close - signal_close) / signal_close
                result[f"return_{horizon_name}"] = float(ret)
                result[f"correct_{horizon_name}"] = (
                    (direction > 0 and ret > 0) or (direction < 0 and ret < 0)
                )

    return result


# ─── Batch evaluation ─────────────────────────────────────────────

def evaluate_signals_batch(
    signals: list[dict],
) -> list[SignalEvaluation]:
    """Evaluate a batch of signals.

    Each signal dict must have:
        news_item_id, source_name, ticker, market, signal_time, predicted_sentiment
    """
    results = []
    # Cache price series per (ticker, market) to avoid redundant fetches
    price_cache: dict[tuple, pd.DataFrame | None] = {}

    for sig in signals:
        ticker = sig["ticker"]
        market = sig["market"]
        signal_time = sig["signal_time"]
        predicted = sig["predicted_sentiment"]

        direction = SENTIMENT_DIRECTION.get(predicted, 0)
        if direction == 0:
            continue  # skip neutral

        eval_result = evaluate_signal(signal_time, predicted, ticker, market)

        results.append(SignalEvaluation(
            news_item_id=sig["news_item_id"],
            source_name=sig["source_name"],
            ticker=ticker,
            market=market,
            signal_time=signal_time,
            predicted_sentiment=predicted,
            price_at_signal=eval_result["price_at_signal"],
            return_t0=eval_result["return_t0"],
            return_t1=eval_result["return_t1"],
            return_t5=eval_result["return_t5"],
            return_t20=eval_result["return_t20"],
            correct_t0=eval_result["correct_t0"],
            correct_t1=eval_result["correct_t1"],
            correct_t5=eval_result["correct_t5"],
            correct_t20=eval_result["correct_t20"],
        ))

    return results


# ─── Leaderboard aggregation ─────────────────────────────────────

def compute_leaderboard(
    evaluations: list[SignalEvaluation],
    source_categories: dict[str, str] | None = None,
) -> list[SourceLeaderboardEntry]:
    """Aggregate signal evaluations into a leaderboard ranked by composite score.

    Parameters:
        evaluations: list of SignalEvaluation results
        source_categories: optional mapping {source_name: category}
    """
    if not evaluations:
        return []

    source_categories = source_categories or {}

    # Group by source
    groups: dict[str, list[SignalEvaluation]] = {}
    for ev in evaluations:
        groups.setdefault(ev.source_name, []).append(ev)

    entries = []
    for source_name, evals in groups.items():
        total = len(evals)
        if total == 0:
            continue

        # Accuracy per horizon
        def _accuracy(field: str) -> float | None:
            scored = [getattr(e, field) for e in evals if getattr(e, field) is not None]
            if not scored:
                return None
            return sum(1 for c in scored if c) / len(scored)

        acc_t0 = _accuracy("correct_t0")
        acc_t1 = _accuracy("correct_t1")
        acc_t5 = _accuracy("correct_t5")
        acc_t20 = _accuracy("correct_t20")

        # Average returns by sentiment direction
        bull_returns = [
            e.return_t5 for e in evals
            if SENTIMENT_DIRECTION.get(e.predicted_sentiment, 0) > 0 and e.return_t5 is not None
        ]
        bear_returns = [
            e.return_t5 for e in evals
            if SENTIMENT_DIRECTION.get(e.predicted_sentiment, 0) < 0 and e.return_t5 is not None
        ]
        avg_bull = sum(bull_returns) / len(bull_returns) if bull_returns else None
        avg_bear = sum(bear_returns) / len(bear_returns) if bear_returns else None

        # Timeliness: proportion of signals where T+0 was correct (early detection)
        t0_scored = [e.correct_t0 for e in evals if e.correct_t0 is not None]
        timeliness = sum(1 for c in t0_scored if c) / len(t0_scored) if t0_scored else None

        # Composite score: weighted average of accuracies
        # T+1 and T+5 weighted more heavily (most actionable horizons)
        scores = []
        weights = []
        for acc, w in [(acc_t0, 0.1), (acc_t1, 0.3), (acc_t5, 0.35), (acc_t20, 0.25)]:
            if acc is not None:
                scores.append(acc * w)
                weights.append(w)
        composite = sum(scores) / sum(weights) if weights else 0.0

        entries.append(SourceLeaderboardEntry(
            source_name=source_name,
            category=source_categories.get(source_name, ""),
            total_signals=total,
            accuracy_t0=acc_t0,
            accuracy_t1=acc_t1,
            accuracy_t5=acc_t5,
            accuracy_t20=acc_t20,
            avg_return_bullish=avg_bull,
            avg_return_bearish=avg_bear,
            timeliness_score=timeliness,
            composite_score=composite,
        ))

    # Sort by composite score descending
    entries.sort(key=lambda e: e.composite_score, reverse=True)
    return entries


# ─── IC / ICIR computation ─────────────────────────────────────

def compute_source_ic(evaluations: list) -> dict[str, dict]:
    """Compute Information Coefficient (IC) and ICIR per source per horizon.

    IC = Spearman rank correlation between predicted sentiment_score and actual return.
    ICIR = mean(weekly_IC) / std(weekly_IC) * sqrt(52) — stability of predictive power.

    Args:
        evaluations: list of dicts with keys: source_name, signal_time,
                     sentiment_score_t1, sentiment_score_t5, sentiment_score_t20,
                     return_t1, return_t5, return_t20, confidence_t1, confidence_t5, confidence_t20

    Returns:
        {source_name: {ic_t1, ic_t5, ic_t20, icir, avg_confidence, calibration_score}}
    """
    from scipy import stats
    import numpy as np
    from collections import defaultdict

    # Group by source
    by_source = defaultdict(list)
    for ev in evaluations:
        by_source[ev["source_name"]].append(ev)

    results = {}
    for source, evs in by_source.items():
        ic_per_horizon = {}

        for horizon, score_key, return_key, conf_key in [
            ("t1", "sentiment_score_t1", "return_t1", "confidence_t1"),
            ("t5", "sentiment_score_t5", "return_t5", "confidence_t5"),
            ("t20", "sentiment_score_t20", "return_t20", "confidence_t20"),
        ]:
            scores = []
            returns = []
            confs = []
            for ev in evs:
                s = ev.get(score_key)
                r = ev.get(return_key)
                c = ev.get(conf_key)
                if s is not None and r is not None:
                    scores.append(float(s))
                    returns.append(float(r))
                    confs.append(float(c) if c is not None else 1.0)

            if len(scores) >= 5:
                corr, _ = stats.spearmanr(scores, returns)
                ic_per_horizon[f"ic_{horizon}"] = round(float(corr), 4) if not np.isnan(corr) else None
            else:
                ic_per_horizon[f"ic_{horizon}"] = None

            # Average confidence
            if confs:
                ic_per_horizon[f"avg_conf_{horizon}"] = round(np.mean(confs), 3)

        # ICIR: compute weekly IC for short-term, then mean/std
        weekly_ics = []
        by_week = defaultdict(lambda: ([], []))
        for ev in evs:
            s = ev.get("sentiment_score_t1")
            r = ev.get("return_t1")
            if s is not None and r is not None:
                week_key = ev["signal_time"].isocalendar()[:2] if hasattr(ev["signal_time"], "isocalendar") else str(ev["signal_time"])[:10]
                by_week[week_key][0].append(float(s))
                by_week[week_key][1].append(float(r))

        for week_key, (scores, returns) in by_week.items():
            if len(scores) >= 3:
                corr, _ = stats.spearmanr(scores, returns)
                if not np.isnan(corr):
                    weekly_ics.append(corr)

        if len(weekly_ics) >= 4:
            icir = float(np.mean(weekly_ics)) / float(np.std(weekly_ics)) * np.sqrt(52) if np.std(weekly_ics) > 0 else 0.0
            icir = round(icir, 3)
        else:
            icir = None

        # Overall avg confidence
        all_confs = [float(ev.get("confidence_t1") or 1.0) for ev in evs if ev.get("sentiment_score_t1") is not None]
        avg_confidence = round(np.mean(all_confs), 3) if all_confs else None

        results[source] = {
            **ic_per_horizon,
            "icir": icir,
            "avg_confidence": avg_confidence,
        }

    return results
