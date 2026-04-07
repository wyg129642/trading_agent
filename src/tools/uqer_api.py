"""Uqer (DataYes) market data API wrapper."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

_client_initialized = False


def _init_uqer(token: str) -> bool:
    """Initialize the Uqer client. Returns True on success."""
    global _client_initialized
    if _client_initialized:
        return True
    try:
        import uqer
        uqer.Client(token=token)
        _client_initialized = True
        logger.info("Uqer client initialized successfully.")
        return True
    except Exception as e:
        logger.warning("Failed to initialize Uqer client: %s", e)
        return False


async def get_market_data(ticker: str, begin_date: str, end_date: str, token: str = "") -> str:
    """Get stock market data from Uqer/DataYes API.

    Args:
        ticker: Stock code (e.g., '688256' for Cambricon)
        begin_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format
        token: Uqer API token

    Returns:
        Formatted string of market data for LLM consumption.
    """
    def _fetch():
        if token:
            _init_uqer(token)

        try:
            from uqer import DataAPI
            df = DataAPI.MktEqudGet(
                ticker=ticker,
                beginDate=begin_date,
                endDate=end_date,
                field="ticker,tradeDate,openPrice,highestPrice,lowestPrice,closePrice,turnoverVol,turnoverValue",
                pandas="1",
            )
            if df is None or df.empty:
                return f"No market data found for ticker {ticker} ({begin_date} to {end_date})"

            # Format for LLM
            lines = [f"Market Data for {ticker} ({begin_date} to {end_date}):"]
            lines.append(f"{'Date':<12} {'Open':>8} {'High':>8} {'Low':>8} {'Close':>8} {'Volume':>12}")
            lines.append("-" * 68)
            for _, row in df.iterrows():
                lines.append(
                    f"{row.get('tradeDate', 'N/A'):<12} "
                    f"{row.get('openPrice', 0):>8.2f} "
                    f"{row.get('highestPrice', 0):>8.2f} "
                    f"{row.get('lowestPrice', 0):>8.2f} "
                    f"{row.get('closePrice', 0):>8.2f} "
                    f"{row.get('turnoverVol', 0):>12.0f}"
                )

            if len(df) > 1:
                first_close = df.iloc[0].get("closePrice", 0)
                last_close = df.iloc[-1].get("closePrice", 0)
                if first_close > 0:
                    change_pct = (last_close - first_close) / first_close * 100
                    lines.append(f"\nPeriod Change: {change_pct:+.2f}%")

            return "\n".join(lines)
        except ImportError:
            return "Uqer package not available. Cannot fetch market data."
        except Exception as e:
            return f"Failed to fetch market data for {ticker}: {e}"

    return await asyncio.to_thread(_fetch)


async def get_latest_price(ticker: str, token: str = "") -> dict:
    """Get the latest available price data for a stock.

    Returns a dict with price info, or empty dict on failure.
    """
    today = datetime.now().strftime("%Y%m%d")
    week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d")

    result = await get_market_data(ticker, week_ago, today, token)
    return {"raw_text": result, "ticker": ticker}
