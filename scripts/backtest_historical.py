#!/usr/bin/env python3
"""Batch re-label historical news with per-stock multi-horizon sentiment,
then populate signal_evaluations for the source accuracy leaderboard.

Pipeline:
  Step 1: Apply DB migration (add ticker_sentiments column if missing)
  Step 2: LLM re-label news_items → per-stock multi-horizon sentiment
  Step 3: Populate signal_evaluations from news_items + AlphaPai data
  Step 4: Evaluate signals against actual stock prices (akshare)

Usage:
    python scripts/backtest_historical.py                    # Full pipeline
    python scripts/backtest_historical.py --step relabel     # Only re-label
    python scripts/backtest_historical.py --step evaluate    # Only evaluate
    python scripts/backtest_historical.py --dry-run          # Preview counts
    python scripts/backtest_historical.py --limit 10         # Process first N items
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

PG_DSN = "postgresql://trading_agent:TradingAgent2025Secure@localhost:5432/trading_agent"

SETTINGS_PATH = PROJECT_ROOT / "config" / "settings.yaml"
SOURCES_PATH = PROJECT_ROOT / "config" / "sources.yaml"
PORTFOLIO_PATH = PROJECT_ROOT / "config" / "portfolio.yaml"


def load_settings() -> dict:
    with open(SETTINGS_PATH) as f:
        return yaml.safe_load(f)


def load_source_categories() -> dict[str, str]:
    """Load source name → category mapping from config."""
    cats = {}
    for path in (SOURCES_PATH, PORTFOLIO_PATH):
        if path.exists():
            with open(path) as f:
                data = yaml.safe_load(f) or []
            sources = data if isinstance(data, list) else data.get("sources", [])
            for s in sources:
                name = s.get("name", "")
                cat = s.get("category", s.get("group", ""))
                if name:
                    cats[name] = cat
    return cats


# ---------------------------------------------------------------------------
# Step 1: Apply migration
# ---------------------------------------------------------------------------

async def ensure_ticker_sentiments_column(conn) -> None:
    """Add ticker_sentiments and sector_sentiments columns if missing."""
    existing = await conn.fetch("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'analysis_results' AND column_name IN ('ticker_sentiments', 'sector_sentiments')
    """)
    existing_cols = {r["column_name"] for r in existing}

    if "ticker_sentiments" not in existing_cols:
        await conn.execute(
            "ALTER TABLE analysis_results ADD COLUMN ticker_sentiments JSONB DEFAULT '{}'::jsonb"
        )
        logger.info("Added column: analysis_results.ticker_sentiments")

    if "sector_sentiments" not in existing_cols:
        await conn.execute(
            "ALTER TABLE analysis_results ADD COLUMN sector_sentiments JSONB DEFAULT '{}'::jsonb"
        )
        logger.info("Added column: analysis_results.sector_sentiments")

    # Also ensure multi-horizon signal columns exist (migration i1a2b3c4d5e6)
    signal_cols_needed = [
        ("predicted_sentiment_t1", "VARCHAR(20)"),
        ("predicted_sentiment_t5", "VARCHAR(20)"),
        ("predicted_sentiment_t20", "VARCHAR(20)"),
        ("sentiment_score_t1", "FLOAT"),
        ("sentiment_score_t5", "FLOAT"),
        ("sentiment_score_t20", "FLOAT"),
        ("confidence_t1", "FLOAT"),
        ("confidence_t5", "FLOAT"),
        ("confidence_t20", "FLOAT"),
    ]
    existing_signal_cols = await conn.fetch("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'signal_evaluations'
    """)
    existing_signal_set = {r["column_name"] for r in existing_signal_cols}

    for col_name, col_type in signal_cols_needed:
        if col_name not in existing_signal_set:
            await conn.execute(
                f"ALTER TABLE signal_evaluations ADD COLUMN {col_name} {col_type}"
            )
            logger.info("Added column: signal_evaluations.%s", col_name)

    # Create index if missing
    try:
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS ix_signal_eval_ticker_time "
            "ON signal_evaluations (ticker, signal_time)"
        )
    except Exception:
        pass

    # Update alembic version to latest
    try:
        current = await conn.fetchval("SELECT version_num FROM alembic_version")
        if current in ("g8b6c5d4e3f2", "h9c7d6e5f4a3"):
            await conn.execute(
                "UPDATE alembic_version SET version_num = 'i1a2b3c4d5e6'"
            )
            logger.info("Updated alembic version to i1a2b3c4d5e6")
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Step 2: LLM re-labeling
# ---------------------------------------------------------------------------

RELABEL_SYSTEM_PROMPT = """你是量化交易分析师。根据已有的新闻分析结果，对每只相关股票分别给出三个时间周期的涨跌预测。

三个时间周期（精确定义，用于量化回测）：
- short_term: 下一个交易日收盘价相对于今日收盘价的涨跌（T+1）
- medium_term: 第5个交易日收盘价相对于今日收盘价的涨跌（T+5）
- long_term: 第20个交易日收盘价相对于今日收盘价的涨跌（T+20）

**重要：如果某个时间周期无法判断涨跌方向，该周期输出null。**
只有当你有足够信息和信心判断方向时，才给出具体预测。宁可输出null也不要给出低质量的判断。

当某个周期可以判断时，提供：
- sentiment: bullish|bearish（不要输出neutral，无法判断请输出null）
- confidence: 0.0-1.0（你对判断的把握程度）

confidence校准：
- 0.7-1.0: 有多个独立证据支撑
- 0.5-0.6: 有一定证据但存在不确定性
- 0.3-0.4: 证据有限，依赖推理
- 0.1-0.2: 信息严重不足

不同时间周期可以有不同方向的判断。只输出JSON，不要其他文字。"""

RELABEL_USER_TEMPLATE = """【已有分析】
标题: {title}
摘要: {summary}
全局情绪: {sentiment}
多头逻辑: {bull_case}
空头逻辑: {bear_case}
关键发现: {key_facts}

【相关标的】
{tickers}

请以JSON格式回答：
{{
  "per_stock_sentiment": [
    {{
      "ticker": "股票代码（如600519.SH）",
      "name": "股票名称（如贵州茅台）",
      "short_term": {{"sentiment": "bullish|bearish", "confidence": 0.0到1.0}} 或 null（无法判断时）,
      "medium_term": {{"sentiment": "bullish|bearish", "confidence": 0.0到1.0}} 或 null,
      "long_term": {{"sentiment": "bullish|bearish", "confidence": 0.0到1.0}} 或 null
    }}
  ]
}}"""


async def relabel_news_items(conn, settings: dict, limit: int | None, dry_run: bool) -> int:
    """Re-label news items with per-stock multi-horizon sentiment via LLM."""
    # Fetch non-neutral items that need re-labeling
    query = """
        SELECT ar.news_item_id, ar.sentiment, ar.affected_tickers,
               ar.summary, ar.bull_case, ar.bear_case, ar.key_facts,
               ar.ticker_sentiments,
               ni.title, ni.source_name
        FROM analysis_results ar
        JOIN news_items ni ON ar.news_item_id = ni.id
        WHERE ar.sentiment IN ('bullish', 'very_bullish', 'bearish', 'very_bearish')
          AND ar.affected_tickers IS NOT NULL
          AND jsonb_array_length(ar.affected_tickers) > 0
        ORDER BY ni.fetched_at DESC
    """
    if limit:
        query += f" LIMIT {limit}"

    rows = await conn.fetch(query)
    logger.info("Found %d non-neutral news items with tickers", len(rows))

    # Filter to items that need re-labeling (empty or flat ticker_sentiments)
    to_relabel = []
    for r in rows:
        ts = r["ticker_sentiments"]
        if ts and isinstance(ts, dict) and ts != {}:
            # Check if it's already multi-horizon format
            sample_val = next(iter(ts.values()), None)
            if isinstance(sample_val, dict) and "short_term" in sample_val:
                continue  # Already has multi-horizon data
        to_relabel.append(r)

    logger.info("  %d items need re-labeling (missing per-stock multi-horizon sentiment)",
                len(to_relabel))

    if dry_run or not to_relabel:
        return len(to_relabel)

    # Initialize LLM client
    from openai import AsyncOpenAI
    llm_cfg = settings["llm"]
    client = AsyncOpenAI(
        api_key=llm_cfg["api_key"],
        base_url=llm_cfg.get("base_url", "https://api.minimaxi.com/v1"),
        timeout=llm_cfg.get("timeout", 60),
    )
    model = llm_cfg.get("model_analyzer", "MiniMax-M2")

    labeled = 0
    errors = 0

    for i, row in enumerate(to_relabel):
        tickers = row["affected_tickers"]
        if isinstance(tickers, str):
            tickers = json.loads(tickers)
        tickers_str = ", ".join(tickers[:8])  # Limit to 8 tickers

        key_facts = row["key_facts"]
        if isinstance(key_facts, str):
            try:
                key_facts = json.loads(key_facts)
            except (json.JSONDecodeError, TypeError):
                key_facts = []
        key_facts_str = "\n".join(f"- {f}" for f in (key_facts or [])[:5])

        user_msg = RELABEL_USER_TEMPLATE.format(
            title=row["title"] or "",
            summary=row["summary"] or "",
            sentiment=row["sentiment"],
            bull_case=row["bull_case"] or "",
            bear_case=row["bear_case"] or "",
            key_facts=key_facts_str or "无",
            tickers=tickers_str,
        )

        try:
            response = await client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": RELABEL_SYSTEM_PROMPT},
                    {"role": "user", "content": user_msg},
                ],
                temperature=0.2,
                max_tokens=4000,
            )
            content = response.choices[0].message.content.strip()

            # Parse JSON from response
            parsed = _extract_json(content)
            if not parsed:
                logger.warning("  [%d/%d] Failed to parse JSON for %s",
                               i + 1, len(to_relabel), row["news_item_id"][:16])
                errors += 1
                continue

            per_stock = parsed.get("per_stock_sentiment", [])
            if not per_stock:
                errors += 1
                continue

            # Build ticker_sentiments dict (null horizons preserved)
            ticker_sentiments = {}
            for entry in per_stock:
                ticker = entry.get("ticker", "").strip()
                name = entry.get("name", "").strip()
                if not ticker:
                    continue
                key = f"{name}({ticker})" if name else ticker
                ts_entry = {}
                for tf in ("short_term", "medium_term", "long_term"):
                    tf_data = entry.get(tf)
                    if tf_data is None:
                        # Model can't judge this horizon — store as null
                        ts_entry[tf] = None
                    elif isinstance(tf_data, dict):
                        sent = tf_data.get("sentiment", "")
                        if sent == "neutral":
                            ts_entry[tf] = None
                        else:
                            ts_entry[tf] = {
                                "sentiment": sent,
                                "confidence": min(1.0, max(0.0, float(tf_data.get("confidence", 0.5)))),
                            }
                if ts_entry:
                    ticker_sentiments[key] = ts_entry

            if not ticker_sentiments:
                errors += 1
                continue

            # Update database
            await conn.execute(
                "UPDATE analysis_results SET ticker_sentiments = $1 WHERE news_item_id = $2",
                json.dumps(ticker_sentiments, ensure_ascii=False),
                row["news_item_id"],
            )
            labeled += 1

            if (i + 1) % 10 == 0:
                logger.info("  [%d/%d] Re-labeled %d items so far (%d errors)",
                            i + 1, len(to_relabel), labeled, errors)

            # Rate limit
            await asyncio.sleep(0.3)

        except Exception as e:
            logger.warning("  [%d/%d] LLM error for %s: %s",
                           i + 1, len(to_relabel), row["news_item_id"][:16], e)
            errors += 1
            await asyncio.sleep(1)

    logger.info("Re-labeling complete: %d labeled, %d errors out of %d total",
                labeled, errors, len(to_relabel))
    return labeled


def _extract_json(text: str) -> dict | None:
    """Extract JSON object from LLM response text."""
    # Try parsing as-is first
    text = text.strip()
    if text.startswith("```"):
        # Remove markdown code block
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
        text = text.strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Try to find JSON object in text
    match = re.search(r"\{[\s\S]*\}", text)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass

    return None


# ---------------------------------------------------------------------------
# Step 3: Populate signal_evaluations
# ---------------------------------------------------------------------------

def _ticker_to_market(ticker: str) -> str:
    """Determine market from ticker format."""
    t = ticker.strip().upper()
    if ".SH" in t or ".SZ" in t or ".SS" in t:
        return "china"
    if ".HK" in t:
        return "hk"
    # Check for 6-digit A-share codes
    code = re.sub(r"\(.*?\)", "", t).strip()
    if code.isdigit() and len(code) == 6:
        return "china"
    # US tickers are typically all-alpha
    if re.match(r"^[A-Z]{1,5}$", code):
        return "us"
    return "global"


def _extract_ticker_code(ticker_display: str) -> str:
    """Extract clean ticker code from display format like '贵州茅台(600519.SH)'."""
    # Try to find code in parentheses
    match = re.search(r"\(([^)]+)\)", ticker_display)
    if match:
        return match.group(1).strip()
    return ticker_display.strip()


async def populate_signal_evaluations(
    conn,
    source_cats: dict[str, str],
    limit: int | None,
    dry_run: bool,
    include_alphapai: bool = True,
) -> dict:
    """Populate signal_evaluations from news_items + external sources."""
    from engine.analysis.source_scorer import evaluate_signal, SENTIMENT_DIRECTION

    stats = {"news_evaluated": 0, "alphapai_evaluated": 0, "skipped": 0, "errors": 0, "no_price": 0}

    # Get already-evaluated (news_item_id, ticker) pairs for precise dedup
    existing_pairs = set()
    existing_rows = await conn.fetch("SELECT news_item_id, ticker FROM signal_evaluations")
    existing_pairs = {(r["news_item_id"], r["ticker"]) for r in existing_rows}
    logger.info("Found %d already-evaluated (news_item_id, ticker) pairs", len(existing_pairs))

    # ── Part A: news_items with analysis ──
    query = """
        SELECT ar.news_item_id, ar.sentiment, ar.affected_tickers,
               ar.ticker_sentiments, ni.source_name, ni.published_at,
               ni.fetched_at, ni.market
        FROM analysis_results ar
        JOIN news_items ni ON ar.news_item_id = ni.id
        WHERE ar.sentiment IN ('bullish', 'very_bullish', 'bearish', 'very_bearish')
          AND ar.affected_tickers IS NOT NULL
          AND jsonb_array_length(ar.affected_tickers) > 0
        ORDER BY ni.fetched_at DESC
    """
    if limit:
        query += f" LIMIT {limit}"

    news_rows = await conn.fetch(query)
    logger.info("Processing %d news_items for signal evaluation", len(news_rows))

    batch = []
    for row in news_rows:
        news_id = row["news_item_id"]

        tickers = row["affected_tickers"]
        if isinstance(tickers, str):
            tickers = json.loads(tickers)

        signal_time = row["published_at"] or row["fetched_at"]
        if not signal_time:
            continue
        if signal_time.tzinfo is None:
            signal_time = signal_time.replace(tzinfo=timezone.utc)

        market = row["market"] or "global"
        ticker_sents = row["ticker_sentiments"] or {}
        if isinstance(ticker_sents, str):
            try:
                ticker_sents = json.loads(ticker_sents)
            except (json.JSONDecodeError, TypeError):
                ticker_sents = {}

        for ticker_entry in tickers[:5]:  # limit to first 5 tickers
            ticker_code = _extract_ticker_code(ticker_entry)
            if not ticker_code or len(ticker_code) < 2:
                continue

            # Skip already-evaluated (news_item_id, ticker) pairs
            if (news_id, ticker_code) in existing_pairs:
                stats["skipped"] += 1
                continue

            # Resolve per-stock sentiment
            stock_sentiment = None
            if ticker_sents:
                stock_sentiment = ticker_sents.get(ticker_entry)
                if not stock_sentiment:
                    for ts_key, ts_val in ticker_sents.items():
                        if ticker_code in ts_key or ts_key in ticker_code:
                            stock_sentiment = ts_val
                            break

            # Resolve multi-horizon predictions (null = model can't judge)
            pred_t1 = pred_t5 = pred_t20 = None
            conf_t1 = conf_t5 = conf_t20 = None

            if isinstance(stock_sentiment, dict) and "short_term" in stock_sentiment:
                st = stock_sentiment.get("short_term")
                mt = stock_sentiment.get("medium_term")
                lt = stock_sentiment.get("long_term")
                # null horizons → None prediction (won't be evaluated)
                pred_t1 = st.get("sentiment") if isinstance(st, dict) else None
                pred_t5 = mt.get("sentiment") if isinstance(mt, dict) else None
                pred_t20 = lt.get("sentiment") if isinstance(lt, dict) else None
                conf_t1 = st.get("confidence") if isinstance(st, dict) else None
                conf_t5 = mt.get("confidence") if isinstance(mt, dict) else None
                conf_t20 = lt.get("confidence") if isinstance(lt, dict) else None
                # Use first non-null prediction as overall, or fall back to row sentiment
                overall_sentiment = pred_t1 or pred_t5 or pred_t20 or row["sentiment"]
            else:
                overall_sentiment = row["sentiment"]
                pred_t1 = pred_t5 = pred_t20 = overall_sentiment

            if overall_sentiment not in ("bullish", "very_bullish", "bearish", "very_bearish"):
                continue

            # Determine ticker market — always infer from ticker code
            # (a Chinese news source can mention US/HK stocks)
            ticker_market = _ticker_to_market(ticker_code)

            batch.append({
                "news_item_id": news_id,
                "source_name": row["source_name"],
                "category": source_cats.get(row["source_name"], ""),
                "ticker": ticker_code,
                "market": ticker_market,
                "signal_time": signal_time,
                "overall_sentiment": overall_sentiment,
                "pred_t1": pred_t1, "pred_t5": pred_t5, "pred_t20": pred_t20,
                "conf_t1": conf_t1, "conf_t5": conf_t5, "conf_t20": conf_t20,
                "source_type": "news",
            })

    # ── Part B: AlphaPai data ──
    if include_alphapai:
        alphapai_batch = await _collect_alphapai_signals(conn, existing_pairs, limit)
        batch.extend(alphapai_batch)
        logger.info("Added %d AlphaPai signals", len(alphapai_batch))

    if dry_run:
        news_count = sum(1 for b in batch if b["source_type"] == "news")
        alphapai_count = sum(1 for b in batch if b["source_type"] == "alphapai")
        # Count unique tickers
        unique_tickers = set((b["ticker"], b["market"]) for b in batch)
        logger.info("DRY RUN: would evaluate %d signals (%d news, %d alphapai) across %d unique tickers",
                     len(batch), news_count, alphapai_count, len(unique_tickers))
        return stats

    # ── Evaluate signals using batched price fetching ──
    return await _batch_evaluate_signals(conn, batch, stats)


def _unproxy_all():
    """Context manager to temporarily remove ALL proxy env vars for akshare."""
    import os
    from contextlib import contextmanager

    @contextmanager
    def _ctx():
        saved = {}
        for key in ("HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy",
                     "ALL_PROXY", "all_proxy", "NO_PROXY", "no_proxy"):
            if key in os.environ:
                saved[key] = os.environ.pop(key)
        try:
            yield
        finally:
            os.environ.update(saved)

    return _ctx()


def _fetch_price_no_proxy(ticker: str, market: str, start: str, end: str):
    """Fetch price series with proxy disabled, using Sina API for A-shares.

    Uses ak.stock_zh_a_daily (Sina) instead of ak.stock_zh_a_hist (eastmoney)
    because eastmoney aggressively rate-limits batch requests.
    """
    import time as _time
    import akshare as ak
    import pandas as pd
    from engine.analysis.source_scorer import _normalize_ticker_cn, _normalize_ticker_hk

    for attempt in range(3):
        try:
            with _unproxy_all():
                if market == "china":
                    sym = _normalize_ticker_cn(ticker)
                    # Sina needs "sh" or "sz" prefix
                    if sym.isdigit() and len(sym) == 6:
                        prefix = "sh" if sym[0] in ("5", "6", "9") else "sz"
                        sina_sym = f"{prefix}{sym}"
                    else:
                        sina_sym = f"sh{sym}" if not sym.startswith(("sh", "sz")) else sym
                    df = ak.stock_zh_a_daily(
                        symbol=sina_sym, start_date=start, end_date=end, adjust="qfq",
                    )
                    if df is None or df.empty:
                        return None
                    df["date"] = pd.to_datetime(df["date"])
                    return df[["date", "open", "close"]].sort_values("date").reset_index(drop=True)

                elif market == "us":
                    df = ak.stock_us_daily(symbol=ticker.upper(), adjust="qfq")
                    if df is None or df.empty:
                        return None
                    df["date"] = pd.to_datetime(df["date"])
                    sd, ed = pd.to_datetime(start), pd.to_datetime(end)
                    df = df[(df["date"] >= sd) & (df["date"] <= ed)]
                    return df[["date", "open", "close"]].sort_values("date").reset_index(drop=True)

                elif market == "hk":
                    sym = _normalize_ticker_hk(ticker)
                    df = ak.stock_hk_daily(symbol=sym, adjust="qfq")
                    if df is None or df.empty:
                        return None
                    df["date"] = pd.to_datetime(df["date"])
                    sd, ed = pd.to_datetime(start), pd.to_datetime(end)
                    df = df[(df["date"] >= sd) & (df["date"] <= ed)]
                    return df[["date", "open", "close"]].sort_values("date").reset_index(drop=True)

                else:
                    return None

        except Exception as e:
            err_str = str(e)
            if any(k in err_str for k in ("RemoteDisconnected", "Connection aborted",
                                           "ConnectionReset", "EOF occurred",
                                           "Max retries", "timed out")):
                if attempt < 2:
                    wait = 3 * (attempt + 1)
                    _time.sleep(wait)
                    continue
            return None
    return None


async def _batch_evaluate_signals(conn, batch: list[dict], stats: dict) -> dict:
    """Evaluate signals by fetching prices once per ticker (batched)."""
    from engine.analysis.source_scorer import (
        SENTIMENT_DIRECTION, _signal_utc_to_trading_date,
    )

    # Group signals by (ticker, market)
    from collections import defaultdict
    ticker_groups: dict[tuple, list[dict]] = defaultdict(list)
    for sig in batch:
        ticker_groups[(sig["ticker"], sig["market"])].append(sig)

    logger.info("Evaluating %d signals across %d unique tickers...",
                len(batch), len(ticker_groups))

    now = datetime.now(timezone.utc)
    evaluated_total = 0
    ticker_count = 0

    for (ticker, market), signals in ticker_groups.items():
        ticker_count += 1

        # Determine date range for this ticker's signals
        signal_times = [s["signal_time"] for s in signals]
        earliest = min(signal_times)
        latest = max(signal_times)
        start = (earliest - timedelta(days=10)).strftime("%Y%m%d")
        end = (latest + timedelta(days=50)).strftime("%Y%m%d")

        # Fetch price data once for this ticker (proxy disabled for domestic APIs)
        try:
            prices = await asyncio.to_thread(_fetch_price_no_proxy, ticker, market, start, end)
        except Exception as e:
            logger.debug("Price fetch error for %s (%s): %s", ticker, market, e)
            stats["errors"] += len(signals)
            continue

        if prices is None or len(prices) < 2:
            stats["no_price"] += len(signals)
            continue

        # Evaluate each signal against the cached price series
        for sig in signals:
            trading_date = _signal_utc_to_trading_date(sig["signal_time"], market)

            # Find signal day index
            sig_idx = None
            for i, row in prices.iterrows():
                if row["date"].date() >= trading_date.date():
                    sig_idx = i
                    break

            if sig_idx is None:
                stats["no_price"] += 1
                continue

            signal_close = prices.loc[sig_idx, "close"]
            signal_open = prices.loc[sig_idx, "open"]

            if not signal_close or signal_close <= 0:
                stats["no_price"] += 1
                continue

            direction = SENTIMENT_DIRECTION.get(sig["overall_sentiment"], 0)

            # T+0
            return_t0 = correct_t0 = None
            if signal_open and signal_open > 0:
                return_t0 = float((signal_close - signal_open) / signal_open)
                if direction != 0:
                    correct_t0 = (direction > 0 and return_t0 > 0) or (direction < 0 and return_t0 < 0)

            # T+1, T+5, T+20 (skip horizons where prediction is null)
            returns = {}
            correctness = {}
            for horizon_name, n_days, pred_key in [
                ("t1", 1, "pred_t1"), ("t5", 5, "pred_t5"), ("t20", 20, "pred_t20"),
            ]:
                # Skip evaluation for null predictions (model couldn't judge)
                pred = sig.get(pred_key)
                if pred is None:
                    returns[horizon_name] = None
                    correctness[horizon_name] = None
                    continue

                future_idx = sig_idx + n_days
                if future_idx < len(prices):
                    future_close = prices.loc[future_idx, "close"]
                    ret = float((future_close - signal_close) / signal_close)
                    returns[horizon_name] = ret
                    # Use per-horizon prediction if available
                    pred = sig.get(pred_key) or sig["overall_sentiment"]
                    d = SENTIMENT_DIRECTION.get(pred, 0)
                    if d != 0:
                        correctness[horizon_name] = (d > 0 and ret > 0) or (d < 0 and ret < 0)
                    else:
                        correctness[horizon_name] = None
                else:
                    returns[horizon_name] = None
                    correctness[horizon_name] = None

            # Insert into signal_evaluations
            await conn.execute("""
                INSERT INTO signal_evaluations (
                    id, news_item_id, source_name, category, ticker, market,
                    signal_time, predicted_sentiment,
                    predicted_sentiment_t1, predicted_sentiment_t5, predicted_sentiment_t20,
                    confidence_t1, confidence_t5, confidence_t20,
                    price_at_signal,
                    return_t0, return_t1, return_t5, return_t20,
                    correct_t0, correct_t1, correct_t5, correct_t20,
                    evaluated_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
                    $15, $16, $17, $18, $19, $20, $21, $22, $23, $24
                ) ON CONFLICT DO NOTHING
            """,
                uuid.uuid4(),
                sig["news_item_id"],
                sig["source_name"],
                sig["category"],
                ticker,
                market,
                sig["signal_time"],
                sig["overall_sentiment"],
                sig["pred_t1"], sig["pred_t5"], sig["pred_t20"],
                sig["conf_t1"], sig["conf_t5"], sig["conf_t20"],
                float(signal_close),
                return_t0, returns.get("t1"), returns.get("t5"), returns.get("t20"),
                correct_t0, correctness.get("t1"), correctness.get("t5"), correctness.get("t20"),
                now,
            )

            if sig["source_type"] == "news":
                stats["news_evaluated"] += 1
            else:
                stats["alphapai_evaluated"] += 1
            evaluated_total += 1

        if ticker_count % 10 == 0:
            logger.info("  [%d tickers] evaluated %d signals (%d news, %d alphapai, %d no_price, %d errors)",
                         ticker_count, evaluated_total,
                         stats["news_evaluated"], stats["alphapai_evaluated"],
                         stats["no_price"], stats["errors"])

        # Rate limit for akshare — moderate delay for Sina API
        await asyncio.sleep(1.0)

    logger.info("Batch evaluation complete: %d signals across %d tickers",
                evaluated_total, ticker_count)
    return stats


async def _collect_alphapai_signals(
    conn,
    existing_pairs: set,
    limit: int | None,
) -> list[dict]:
    """Collect signals from AlphaPai enriched data."""
    signals = []

    # AlphaPai Comments — use institution as source_name
    query = """
        SELECT cmnt_hcode, title, enrichment, cmnt_date, inst_cname
        FROM alphapai_comments
        WHERE is_enriched = true
          AND enrichment->>'sentiment' IN ('bullish', 'bearish')
          AND enrichment->'tickers' IS NOT NULL
          AND jsonb_array_length(enrichment->'tickers') > 0
        ORDER BY cmnt_date DESC
    """
    if limit:
        query += f" LIMIT {limit}"

    rows = await conn.fetch(query)
    for r in rows:
        news_id = f"alphapai_comment_{r['cmnt_hcode']}"

        enrichment = r["enrichment"]
        if isinstance(enrichment, str):
            enrichment = json.loads(enrichment)

        sentiment = enrichment.get("sentiment", "neutral")
        if sentiment not in ("bullish", "bearish"):
            continue

        tickers = enrichment.get("tickers", [])
        relevance = float(enrichment.get("relevance_score", 0.5))

        signal_time = r["cmnt_date"]
        if signal_time and signal_time.tzinfo is None:
            signal_time = signal_time.replace(tzinfo=timezone.utc)
        if not signal_time:
            continue

        source_name = f"AlphaPai_{r['inst_cname']}" if r["inst_cname"] else "AlphaPai_Comments"

        for t in tickers[:3]:
            if isinstance(t, dict):
                code = t.get("code", "")
                name = t.get("name", "")
            elif isinstance(t, str):
                code = _extract_ticker_code(t)
                name = t
            else:
                continue

            if not code or len(code) < 2:
                continue

            if (news_id, code) in existing_pairs:
                continue

            market = _ticker_to_market(code)

            signals.append({
                "news_item_id": news_id,
                "source_name": source_name,
                "category": "analyst_research",
                "ticker": code,
                "market": market,
                "signal_time": signal_time,
                "overall_sentiment": sentiment,
                "pred_t1": sentiment, "pred_t5": sentiment, "pred_t20": sentiment,
                "conf_t1": relevance, "conf_t5": relevance * 0.9, "conf_t20": relevance * 0.7,
                "source_type": "alphapai",
            })

    # AlphaPai Roadshows CN — use "AlphaPai_Roadshow" as source
    query2 = """
        SELECT trans_id, enrichment, synced_at
        FROM alphapai_roadshows_cn
        WHERE is_enriched = true
          AND enrichment->>'sentiment' IN ('bullish', 'bearish')
          AND enrichment->'tickers' IS NOT NULL
          AND jsonb_array_length(enrichment->'tickers') > 0
        ORDER BY synced_at DESC
    """
    if limit:
        query2 += f" LIMIT {limit}"

    rows2 = await conn.fetch(query2)
    for r in rows2:
        news_id = f"alphapai_roadshow_{r['trans_id']}"

        enrichment = r["enrichment"]
        if isinstance(enrichment, str):
            enrichment = json.loads(enrichment)

        sentiment = enrichment.get("sentiment", "neutral")
        if sentiment not in ("bullish", "bearish"):
            continue

        tickers = enrichment.get("tickers", [])
        relevance = float(enrichment.get("relevance_score", 0.5))

        signal_time = r["synced_at"]
        if signal_time and signal_time.tzinfo is None:
            signal_time = signal_time.replace(tzinfo=timezone.utc)
        if not signal_time:
            continue

        for t in tickers[:3]:
            if isinstance(t, dict):
                code = t.get("code", "")
            elif isinstance(t, str):
                code = _extract_ticker_code(t)
            else:
                continue

            if not code or len(code) < 2:
                continue

            if (news_id, code) in existing_pairs:
                continue

            market = _ticker_to_market(code)
            signals.append({
                "news_item_id": news_id,
                "source_name": "AlphaPai_Roadshow",
                "category": "roadshow",
                "ticker": code,
                "market": market,
                "signal_time": signal_time,
                "overall_sentiment": sentiment,
                "pred_t1": sentiment, "pred_t5": sentiment, "pred_t20": sentiment,
                "conf_t1": relevance, "conf_t5": relevance * 0.9, "conf_t20": relevance * 0.7,
                "source_type": "alphapai",
            })

    return signals


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

async def run_pipeline(args) -> None:
    """Run the full backtest pipeline."""
    import asyncpg

    settings = load_settings()
    source_cats = load_source_categories()
    conn = await asyncpg.connect(PG_DSN)

    try:
        overall_start = time.monotonic()

        # Step 1: Ensure columns exist
        logger.info("=" * 60)
        logger.info("STEP 1: Ensuring ticker_sentiments column exists")
        logger.info("=" * 60)
        await ensure_ticker_sentiments_column(conn)

        if args.step in (None, "relabel"):
            # Step 2: LLM re-labeling
            logger.info("")
            logger.info("=" * 60)
            logger.info("STEP 2: LLM re-labeling of news items")
            logger.info("=" * 60)
            labeled = await relabel_news_items(conn, settings, args.limit, args.dry_run)
            logger.info("  Re-labeled: %d items", labeled)

        if args.step in (None, "evaluate"):
            # Step 3: Populate signal_evaluations
            logger.info("")
            logger.info("=" * 60)
            logger.info("STEP 3: Populating signal_evaluations")
            logger.info("=" * 60)
            stats = await populate_signal_evaluations(
                conn, source_cats, args.limit, args.dry_run,
                include_alphapai=not args.no_alphapai,
            )
            logger.info("")
            logger.info("Evaluation results:")
            for k, v in stats.items():
                logger.info("  %s: %d", k, v)

        # Summary
        elapsed = time.monotonic() - overall_start
        logger.info("")
        logger.info("=" * 60)
        logger.info("PIPELINE COMPLETE in %.1f seconds (%.1f min)", elapsed, elapsed / 60)
        logger.info("=" * 60)

        # Show final signal_evaluations count
        total = await conn.fetchval("SELECT count(*) FROM signal_evaluations")
        sources = await conn.fetchval(
            "SELECT count(DISTINCT source_name) FROM signal_evaluations"
        )
        logger.info("  Total signal_evaluations: %d", total)
        logger.info("  Total sources tracked: %d", sources)

    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Batch re-label historical news and populate leaderboard"
    )
    parser.add_argument(
        "--step", choices=["relabel", "evaluate"],
        default=None, help="Run only a specific step (default: all steps)"
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Limit number of items to process"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview counts without making changes"
    )
    parser.add_argument(
        "--no-alphapai", action="store_true",
        help="Skip AlphaPai data in evaluation"
    )
    args = parser.parse_args()

    asyncio.run(run_pipeline(args))


if __name__ == "__main__":
    main()
