"""Market data source — stock prices via akshare for all markets.

Supports A-shares, US stocks, HK stocks, and wraps the proxy handling
needed for domestic endpoints.
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

from engine.proactive.data_sources.base import DataSourcePlugin, DataSourceResult
from engine.proactive.models import PortfolioHolding, StockBaseline

logger = logging.getLogger(__name__)


@contextmanager
def _unproxy_context():
    """Temporarily remove HTTP proxy env vars for China-domestic endpoints."""
    saved = {}
    for key in ("HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy"):
        if key in os.environ:
            saved[key] = os.environ.pop(key)
    try:
        yield
    finally:
        os.environ.update(saved)


class MarketDataPlugin(DataSourcePlugin):
    """Fetch stock price data via akshare for LLM context."""

    name = "market_data"

    def __init__(self, lookback_days: int = 30):
        self._lookback_days = lookback_days

    async def fetch(
        self,
        holding: PortfolioHolding,
        baseline: StockBaseline,
        **kwargs,
    ) -> DataSourceResult:
        """Fetch price data synchronously via akshare (runs in thread pool)."""
        import asyncio
        loop = asyncio.get_event_loop()
        try:
            df = await loop.run_in_executor(
                None, self._fetch_sync, holding,
            )
        except Exception as e:
            logger.warning("MarketData fetch failed for %s: %s", holding.ticker, e)
            df = None

        if df is None or df.empty:
            return DataSourceResult(source_name=self.name)

        formatted = self._format_price_table(df, holding)
        return DataSourceResult(
            source_name=self.name,
            items=[],
            formatted_text=formatted,
            item_count=len(df),
            new_item_count=len(df),
        )

    def _fetch_sync(self, holding: PortfolioHolding):
        """Synchronous price fetch — called via run_in_executor."""
        import akshare as ak
        import pandas as pd

        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=self._lookback_days)).strftime("%Y%m%d")

        ticker = holding.ticker
        market = holding.market

        try:
            if market == "china":
                # A-shares: strip suffix, use 6-digit code
                code = ticker.split(".")[0] if "." in ticker else ticker
                with _unproxy_context():
                    df = ak.stock_zh_a_hist(
                        symbol=code,
                        period="daily",
                        start_date=start_date,
                        end_date=end_date,
                        adjust="qfq",
                    )
                if df is None or df.empty:
                    return None
                df = df.rename(columns={
                    "日期": "date", "开盘": "open", "最高": "high",
                    "最低": "low", "收盘": "close", "成交量": "volume",
                    "涨跌幅": "change_pct",
                })
                df["date"] = pd.to_datetime(df["date"])
                return df[["date", "open", "high", "low", "close", "volume", "change_pct"]].sort_values("date").reset_index(drop=True)

            elif market == "us":
                df = ak.stock_us_daily(symbol=ticker, adjust="qfq")
                if df is None or df.empty:
                    return None
                df["date"] = pd.to_datetime(df["date"])
                sd = pd.to_datetime(start_date)
                df = df[df["date"] >= sd]
                for col in ["open", "high", "low", "close", "volume"]:
                    if col not in df.columns:
                        df[col] = 0
                # Calculate change_pct
                df = df.sort_values("date").reset_index(drop=True)
                df["change_pct"] = df["close"].pct_change() * 100
                return df[["date", "open", "high", "low", "close", "volume", "change_pct"]]

            elif market == "hk":
                hk_ticker = ticker.split(".")[0] if ".HK" in ticker.upper() else ticker
                df = ak.stock_hk_daily(symbol=hk_ticker, adjust="qfq")
                if df is None or df.empty:
                    return None
                df["date"] = pd.to_datetime(df["date"])
                sd = pd.to_datetime(start_date)
                df = df[df["date"] >= sd]
                df = df.sort_values("date").reset_index(drop=True)
                for col in ["open", "high", "low", "close", "volume"]:
                    if col not in df.columns:
                        df[col] = 0
                df["change_pct"] = df["close"].pct_change() * 100
                return df[["date", "open", "high", "low", "close", "volume", "change_pct"]]

            else:
                # KR/JP — try US ADR or return None
                logger.debug("MarketData: no direct support for market=%s ticker=%s", market, ticker)
                return None

        except Exception as e:
            logger.warning("MarketData akshare error for %s (%s): %s", ticker, market, e)
            return None

    def _format_price_table(self, df, holding: PortfolioHolding) -> str:
        """Format a DataFrame as a text table for LLM consumption."""
        if df is None or df.empty:
            return ""

        lines = [f"【股价数据 — {holding.name_cn} ({holding.ticker}) {holding.market_label}】"]

        # Show last 10 trading days
        recent = df.tail(10)
        lines.append("近10个交易日:")
        lines.append(f"{'日期':<12} {'开盘':>8} {'最高':>8} {'最低':>8} {'收盘':>8} {'涨跌幅':>8} {'成交量':>10}")

        for _, row in recent.iterrows():
            date_str = row["date"].strftime("%Y-%m-%d") if hasattr(row["date"], "strftime") else str(row["date"])[:10]
            change = f"{row.get('change_pct', 0):+.2f}%" if row.get("change_pct") is not None else ""
            vol = f"{row.get('volume', 0):,.0f}" if row.get("volume") else ""
            lines.append(
                f"{date_str:<12} {row.get('open', 0):>8.2f} {row.get('high', 0):>8.2f} "
                f"{row.get('low', 0):>8.2f} {row.get('close', 0):>8.2f} {change:>8} {vol:>10}"
            )

        # Summary stats
        if len(df) >= 10:
            close_10 = df.tail(10)
            if len(close_10) >= 2:
                ret_10 = (close_10.iloc[-1]["close"] / close_10.iloc[0]["close"] - 1) * 100
                lines.append(f"\n近10日表现: {ret_10:+.1f}%")
        if len(df) >= 20:
            close_30 = df.tail(30) if len(df) >= 30 else df
            if len(close_30) >= 2:
                ret_30 = (close_30.iloc[-1]["close"] / close_30.iloc[0]["close"] - 1) * 100
                lines.append(f"近30日表现: {ret_30:+.1f}%")

        return "\n".join(lines)

    def format_for_llm(self, result: DataSourceResult) -> str:
        return result.formatted_text

    # ------------------------------------------------------------------
    # Historical event study (v3)
    # ------------------------------------------------------------------

    async def fetch_event_study(
        self,
        historical_events: list[dict],
    ) -> list[dict]:
        """For each historical event, fetch price data around that date and compute returns.

        Args:
            historical_events: List of {"date": "2025-06-15", "ticker": "INTC",
                                        "market": "us", "description": "..."}

        Returns list of:
            {"event_date", "description", "ticker", "market",
             "price_before", "price_after_1d", "return_1d", "return_3d", "return_5d",
             "source": "akshare"}
        """
        import asyncio
        import pandas as pd

        loop = asyncio.get_event_loop()
        results = []

        for event in historical_events[:8]:  # Cap at 8 events
            event_date = event.get("date", "")
            ticker = event.get("ticker", "")
            market = event.get("market", "")
            description = event.get("description", "")

            if not event_date or not ticker or not market:
                continue

            try:
                df = await loop.run_in_executor(
                    None, self._fetch_event_window, ticker, market, event_date,
                )
                if df is None or df.empty:
                    continue

                impact = self._compute_event_impact(df, event_date)
                if impact:
                    impact["event_date"] = event_date
                    impact["ticker"] = ticker
                    impact["market"] = market
                    impact["description"] = description
                    impact["source"] = "akshare"
                    results.append(impact)

            except Exception as e:
                logger.debug("Event study failed for %s on %s: %s", ticker, event_date, e)

        return results

    def _fetch_event_window(
        self, ticker: str, market: str, event_date_str: str,
    ):
        """Fetch price data around an event date (10 days before, 10 after)."""
        import akshare as ak
        import pandas as pd

        try:
            event_dt = datetime.strptime(event_date_str, "%Y-%m-%d")
        except ValueError:
            return None

        start = (event_dt - timedelta(days=20)).strftime("%Y%m%d")
        end = (event_dt + timedelta(days=15)).strftime("%Y%m%d")

        try:
            if market == "china":
                code = ticker.split(".")[0] if "." in ticker else ticker
                with _unproxy_context():
                    df = ak.stock_zh_a_hist(
                        symbol=code, period="daily",
                        start_date=start, end_date=end, adjust="qfq",
                    )
                if df is None or df.empty:
                    return None
                import pandas as pd
                df = df.rename(columns={
                    "日期": "date", "收盘": "close", "涨跌幅": "change_pct",
                })
                df["date"] = pd.to_datetime(df["date"])
                return df[["date", "close"]].sort_values("date").reset_index(drop=True)

            elif market == "us":
                df = ak.stock_us_daily(symbol=ticker, adjust="qfq")
                if df is None or df.empty:
                    return None
                import pandas as pd
                df["date"] = pd.to_datetime(df["date"])
                sd = pd.to_datetime(start)
                ed = pd.to_datetime(end)
                df = df[(df["date"] >= sd) & (df["date"] <= ed)]
                return df[["date", "close"]].sort_values("date").reset_index(drop=True)

            elif market == "hk":
                hk_ticker = ticker.split(".")[0] if ".HK" in ticker.upper() else ticker
                df = ak.stock_hk_daily(symbol=hk_ticker, adjust="qfq")
                if df is None or df.empty:
                    return None
                import pandas as pd
                df["date"] = pd.to_datetime(df["date"])
                sd = pd.to_datetime(start)
                ed = pd.to_datetime(end)
                df = df[(df["date"] >= sd) & (df["date"] <= ed)]
                return df[["date", "close"]].sort_values("date").reset_index(drop=True)

        except Exception as e:
            logger.debug("Event window fetch error %s/%s: %s", ticker, market, e)
            return None

    def _compute_event_impact(
        self, df, event_date_str: str,
    ) -> dict | None:
        """Compute price returns around an event date."""
        import pandas as pd

        event_dt = pd.to_datetime(event_date_str)

        # Find the trading day at or after the event date (T+0)
        after_event = df[df["date"] >= event_dt]
        if after_event.empty:
            return None

        t0_idx = after_event.index[0]

        # Find the trading day before the event (T-1)
        before_event = df[df["date"] < event_dt]
        if before_event.empty:
            return None
        t_minus1_idx = before_event.index[-1]

        price_before = float(df.loc[t_minus1_idx, "close"])

        # Compute returns at T+1, T+3, T+5
        result = {"price_before": price_before}

        for offset, label in [(1, "1d"), (3, "3d"), (5, "5d")]:
            target_idx = t0_idx + offset
            if target_idx in df.index:
                price_after = float(df.loc[target_idx, "close"])
                ret = (price_after / price_before - 1) * 100
                result[f"return_{label}"] = round(ret, 2)
                if label == "1d":
                    result["price_after_1d"] = price_after
            else:
                result[f"return_{label}"] = None

        return result

    @staticmethod
    def format_event_study_table(
        precedents: list[dict], stock_name: str = "",
    ) -> str:
        """Format event study results as a text table for LLM and alert card."""
        if not precedents:
            return ""

        lines = [f"【历史先例价格验证 — {stock_name}】"]
        lines.append(f"{'日期':<12} | {'事件':<35} | {'1日收益':>8} | {'3日收益':>8} | {'5日收益':>8}")
        lines.append("-" * 85)

        valid_1d = []
        valid_3d = []
        valid_5d = []

        for p in precedents:
            date = p.get("event_date", "?")
            desc = p.get("description", "?")[:33]
            r1 = p.get("return_1d")
            r3 = p.get("return_3d")
            r5 = p.get("return_5d")

            r1_str = f"{r1:+.1f}%" if r1 is not None else "N/A"
            r3_str = f"{r3:+.1f}%" if r3 is not None else "N/A"
            r5_str = f"{r5:+.1f}%" if r5 is not None else "N/A"

            lines.append(f"{date:<12} | {desc:<35} | {r1_str:>8} | {r3_str:>8} | {r5_str:>8}")

            if r1 is not None:
                valid_1d.append(r1)
            if r3 is not None:
                valid_3d.append(r3)
            if r5 is not None:
                valid_5d.append(r5)

        # Average row
        if valid_1d:
            avg_1d = f"{sum(valid_1d)/len(valid_1d):+.1f}%"
            avg_3d = f"{sum(valid_3d)/len(valid_3d):+.1f}%" if valid_3d else "N/A"
            avg_5d = f"{sum(valid_5d)/len(valid_5d):+.1f}%" if valid_5d else "N/A"
            lines.append("-" * 85)
            lines.append(f"{'平均':<12} | {'—':<35} | {avg_1d:>8} | {avg_3d:>8} | {avg_5d:>8}")

        return "\n".join(lines)
