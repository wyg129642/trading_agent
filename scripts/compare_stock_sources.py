"""Compare yfinance / tushare / akshare for portfolio quote data.

Measures fetch latency and extracted fields (prev_close, latest_price,
market_cap, pe_ttm) across a representative sample of portfolio tickers.
"""
from __future__ import annotations

import os
import sys
import time
import traceback
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

SAMPLE = [
    # (ticker, market_label, chinese_name)
    ("GLW", "美股", "康宁"),
    ("MU", "美股", "美光科技"),
    ("TSM", "美股", "台积电"),
    ("300394", "创业板", "天孚通信"),
    ("688347", "科创板", "华虹公司"),
    ("600584", "主板", "长电科技"),
    ("06869", "港股", "长飞光纤光缆"),
    ("005930", "韩股", "三星电子"),
    ("285A", "日股", "铠侠"),
]


@dataclass
class QuoteResult:
    source: str
    ticker: str
    prev_close: float | None = None
    latest_price: float | None = None
    change_pct: float | None = None
    market_cap: float | None = None  # in local currency
    pe_ttm: float | None = None
    currency: str = ""
    latency_ms: float = 0.0
    error: str = ""
    raw: dict[str, Any] = field(default_factory=dict)


PROXY_VARS = (
    "HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy",
    "ALL_PROXY", "all_proxy",
)


@contextmanager
def unproxy():
    """akshare needs no-proxy for domestic Chinese endpoints."""
    saved = {k: os.environ.pop(k, None) for k in PROXY_VARS}
    try:
        yield
    finally:
        for k, v in saved.items():
            if v is not None:
                os.environ[k] = v


def _yf_ticker(ticker: str, market: str) -> str:
    """Convert portfolio ticker → yfinance symbol."""
    if market == "美股":
        return ticker
    if market in ("创业板", "科创板"):
        return f"{ticker}.SZ" if ticker.startswith(("0", "3")) else f"{ticker}.SS"
    if market == "主板":
        return f"{ticker}.SS" if ticker.startswith("6") else f"{ticker}.SZ"
    if market == "港股":
        return f"{ticker.lstrip('0').zfill(4)}.HK"
    if market == "韩股":
        return f"{ticker}.KS"
    if market == "日股":
        return f"{ticker}.T"
    return ticker


def test_yfinance(ticker: str, market: str) -> QuoteResult:
    r = QuoteResult(source="yfinance", ticker=ticker)
    sym = _yf_ticker(ticker, market)
    t0 = time.time()
    try:
        import yfinance as yf
        t = yf.Ticker(sym)
        info = t.info or {}
        r.latest_price = info.get("regularMarketPrice") or info.get("currentPrice")
        r.prev_close = info.get("regularMarketPreviousClose") or info.get("previousClose")
        r.market_cap = info.get("marketCap")
        r.pe_ttm = info.get("trailingPE")
        r.currency = info.get("currency", "")
        if r.latest_price and r.prev_close:
            r.change_pct = (r.latest_price - r.prev_close) / r.prev_close * 100
        r.raw = {"yf_symbol": sym}
    except Exception as e:
        r.error = f"{type(e).__name__}: {e}"
    r.latency_ms = (time.time() - t0) * 1000
    return r


def test_akshare(ticker: str, market: str) -> QuoteResult:
    r = QuoteResult(source="akshare", ticker=ticker)
    t0 = time.time()
    try:
        import akshare as ak
        if market == "美股":
            with unproxy():
                spot = ak.stock_us_spot_em()
            hit = spot[spot["代码"].str.endswith(f".{ticker}") | (spot["代码"] == ticker)]
            if hit.empty:
                r.error = "not found in stock_us_spot_em"
            else:
                row = hit.iloc[0]
                r.latest_price = float(row["最新价"]) if row["最新价"] else None
                r.prev_close = float(row["昨收价"]) if "昨收价" in row and row["昨收价"] else None
                r.change_pct = float(row["涨跌幅"]) if row["涨跌幅"] else None
                r.market_cap = float(row["总市值"]) if row["总市值"] else None
                r.pe_ttm = float(row["市盈率"]) if "市盈率" in row and row["市盈率"] else None
                r.currency = "USD"
        elif market in ("创业板", "科创板", "主板"):
            with unproxy():
                spot = ak.stock_zh_a_spot_em()
            hit = spot[spot["代码"] == ticker]
            if hit.empty:
                r.error = "not found in stock_zh_a_spot_em"
            else:
                row = hit.iloc[0]
                r.latest_price = float(row["最新价"]) if row["最新价"] else None
                r.prev_close = float(row["昨收"]) if "昨收" in row else None
                r.change_pct = float(row["涨跌幅"]) if row["涨跌幅"] else None
                r.market_cap = float(row["总市值"]) if row["总市值"] else None
                r.pe_ttm = float(row["市盈率-动态"]) if "市盈率-动态" in row and row["市盈率-动态"] else None
                r.currency = "CNY"
        elif market == "港股":
            with unproxy():
                spot = ak.stock_hk_spot_em()
            code = ticker.lstrip("0").zfill(5)
            hit = spot[spot["代码"] == code]
            if hit.empty:
                r.error = f"not found in stock_hk_spot_em ({code})"
            else:
                row = hit.iloc[0]
                r.latest_price = float(row["最新价"]) if row["最新价"] else None
                r.prev_close = float(row["昨收"]) if "昨收" in row and row["昨收"] else None
                r.change_pct = float(row["涨跌幅"]) if row["涨跌幅"] else None
                r.market_cap = float(row["总市值"]) if "总市值" in row and row["总市值"] else None
                r.currency = "HKD"
        elif market in ("韩股", "日股"):
            r.error = f"akshare has no spot endpoint for {market}"
        r.raw = {"market": market}
    except Exception as e:
        r.error = f"{type(e).__name__}: {e}"
    r.latency_ms = (time.time() - t0) * 1000
    return r


def test_tushare(ticker: str, market: str) -> QuoteResult:
    r = QuoteResult(source="tushare", ticker=ticker)
    t0 = time.time()
    try:
        import tushare as ts
        token = os.getenv("TUSHARE_TOKEN", "").strip()
        if market in ("创业板", "科创板", "主板"):
            if not token:
                r.error = "TUSHARE_TOKEN not set (pro API requires token)"
            else:
                pro = ts.pro_api(token)
                ts_code = f"{ticker}.SH" if ticker.startswith("6") else f"{ticker}.SZ"
                df = pro.daily(ts_code=ts_code, limit=2)
                if df.empty:
                    r.error = "no daily rows"
                else:
                    r.latest_price = float(df.iloc[0]["close"])
                    if len(df) > 1:
                        r.prev_close = float(df.iloc[1]["close"])
                        r.change_pct = (r.latest_price - r.prev_close) / r.prev_close * 100
                    basic = pro.daily_basic(ts_code=ts_code, limit=1)
                    if not basic.empty:
                        b = basic.iloc[0]
                        r.pe_ttm = float(b["pe_ttm"]) if b.get("pe_ttm") else None
                        r.market_cap = float(b["total_mv"]) * 10000 if b.get("total_mv") else None
                    r.currency = "CNY"
        elif market == "港股":
            if not token:
                r.error = "TUSHARE_TOKEN not set"
            else:
                pro = ts.pro_api(token)
                ts_code = f"{ticker.lstrip('0').zfill(5)}.HK"
                df = pro.hk_daily(ts_code=ts_code, limit=2)
                if df.empty:
                    r.error = "no hk_daily rows"
                else:
                    r.latest_price = float(df.iloc[0]["close"])
                    if len(df) > 1:
                        r.prev_close = float(df.iloc[1]["close"])
                        r.change_pct = (r.latest_price - r.prev_close) / r.prev_close * 100
                    r.currency = "HKD"
        elif market == "美股":
            r.error = "tushare US requires paid tier"
        else:
            r.error = f"tushare has no {market} endpoint"
    except Exception as e:
        r.error = f"{type(e).__name__}: {e}"
    r.latency_ms = (time.time() - t0) * 1000
    return r


def fmt(v, fmt_str="{:.2f}"):
    if v is None:
        return "—"
    try:
        return fmt_str.format(v)
    except Exception:
        return str(v)


def fmt_mcap(v):
    if v is None:
        return "—"
    if v > 1e12:
        return f"{v/1e12:.2f}T"
    if v > 1e9:
        return f"{v/1e9:.2f}B"
    if v > 1e6:
        return f"{v/1e6:.2f}M"
    return f"{v:.0f}"


def main():
    print(f"{'='*110}")
    print(f"{'ticker':<10} {'mkt':<6} {'src':<10} {'prev_close':<12} {'latest':<12} {'chg%':<8} {'mcap':<10} {'PE':<10} {'ms':<7} err")
    print(f"{'='*110}")
    for ticker, market, name in SAMPLE:
        for fn in (test_yfinance, test_akshare, test_tushare):
            try:
                r = fn(ticker, market)
            except Exception as e:
                traceback.print_exc()
                continue
            err = r.error[:28] if r.error else ""
            print(
                f"{ticker:<10} {market:<6} {r.source:<10} "
                f"{fmt(r.prev_close):<12} {fmt(r.latest_price):<12} "
                f"{fmt(r.change_pct):<8} {fmt_mcap(r.market_cap):<10} "
                f"{fmt(r.pe_ttm):<10} {r.latency_ms:<7.0f} {err}"
            )
        print("-" * 110)


if __name__ == "__main__":
    main()
