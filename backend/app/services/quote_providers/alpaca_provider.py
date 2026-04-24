"""Alpaca US equities quote provider.

Uses the `/v2/stocks/snapshots` batch endpoint (IEX feed — free). Returns
latest trade price, previous daily close, and computed change %.

Sign up: https://app.alpaca.markets (paper account is enough for free data).
Put keys in .env: ALPACA_API_KEY / ALPACA_API_SECRET.
"""
from __future__ import annotations

import logging
from typing import Any

import httpx

logger = logging.getLogger(__name__)

# IEX is the only free feed. 'sip' is the paid consolidated feed.
FEED = "iex"


async def fetch_us_quotes(
    tickers: list[str],
    api_key: str,
    api_secret: str,
    data_url: str = "https://data.alpaca.markets",
    timeout: float = 8.0,
) -> dict[str, dict[str, Any]]:
    """Batch-fetch all US quotes in a single Alpaca HTTP call.

    Returns dict keyed by ticker with price/prev_close/change_pct/currency.
    Market cap and PE are NOT provided by Alpaca — fall back to yfinance for those.
    """
    if not tickers or not api_key or not api_secret:
        return {}

    url = f"{data_url}/v2/stocks/snapshots"
    headers = {
        "APCA-API-KEY-ID": api_key,
        "APCA-API-SECRET-KEY": api_secret,
    }
    params = {"symbols": ",".join(tickers), "feed": FEED}

    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.get(url, headers=headers, params=params)
            resp.raise_for_status()
            data = resp.json()
    except httpx.HTTPStatusError as e:
        body = e.response.text[:200] if e.response is not None else ""
        logger.warning("Alpaca snapshots HTTP %s: %s", e.response.status_code, body)
        return {}
    except Exception as e:
        logger.warning("Alpaca snapshots failed: %s", e)
        return {}

    out: dict[str, dict[str, Any]] = {}
    for ticker, snap in data.items():
        if not isinstance(snap, dict):
            continue
        latest_trade = snap.get("latestTrade") or {}
        prev_daily = snap.get("prevDailyBar") or {}
        daily_bar = snap.get("dailyBar") or {}
        # Price preference: latest trade > today's close > minute bar close
        latest_price = (
            latest_trade.get("p")
            or daily_bar.get("c")
            or (snap.get("minuteBar") or {}).get("c")
        )
        prev_close = prev_daily.get("c")
        change_pct = None
        if latest_price and prev_close and prev_close != 0:
            change_pct = (latest_price - prev_close) / prev_close * 100
        out[ticker] = {
            "latest_price": float(latest_price) if latest_price else None,
            "prev_close": float(prev_close) if prev_close else None,
            "change_pct": change_pct,
            "currency": "USD",
            "latest_t": latest_trade.get("t"),
        }
    return out
