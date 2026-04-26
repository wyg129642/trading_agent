"""ClickHouse A-share quote provider.

Queries `t_realtime_kline_1m` (latest minute bar → current price) joined with
`t_adj_daily_data` (previous close + market cap + Chinese name). All prices
returned are unadjusted CNY.
"""
from __future__ import annotations

import logging
import os
import threading
from contextlib import contextmanager
from typing import Any, Optional

import clickhouse_connect
from clickhouse_connect.driver.client import Client

logger = logging.getLogger(__name__)


@contextmanager
def _unproxy():
    """ClickHouse HTTP is direct LAN — any outbound proxy will 502."""
    saved = {
        k: os.environ.pop(k, None)
        for k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy",
                  "ALL_PROXY", "all_proxy")
    }
    try:
        yield
    finally:
        for k, v in saved.items():
            if v is not None:
                os.environ[k] = v


# Cached persistent client. The handshake against 192.168.31.137:38123 is
# 3-35s (ClickHouse server-side, not TCP — raw curl connects in 0.5ms), so
# opening a new client per call blows past any reasonable query timeout.
# Once the client is warm, repeat queries are ~20ms.
# clickhouse-connect's Client uses urllib3.PoolManager internally which is
# thread-safe, so concurrent .query() calls from different executor threads
# share the pool safely. We only lock around create/invalidate.
_client_lock = threading.Lock()
_client: Optional[Client] = None
_client_key: tuple = ()


def _client_signature(settings) -> tuple:
    return (
        settings.market_ch_host, settings.market_ch_port,
        settings.market_ch_user, settings.market_ch_db,
    )


def _build_client(settings) -> Client:
    with _unproxy():
        return clickhouse_connect.get_client(
            host=settings.market_ch_host,
            port=settings.market_ch_port,
            username=settings.market_ch_user,
            password=settings.market_ch_password,
            database=settings.market_ch_db,
            connect_timeout=45,
            send_receive_timeout=60,
        )


def _invalidate_client() -> None:
    """Drop the cached client so the next query rebuilds. Safe to call under error."""
    global _client
    with _client_lock:
        try:
            if _client is not None:
                _client.close()
        except Exception:
            pass
        _client = None


def _to_ch_symbol(ticker: str, market_label: str) -> str:
    """Portfolio ticker → ClickHouse symbol (XXXXXX.XSHE/.XSHG).

    Convention in db_market: Shenzhen-listed tickers use .XSHE (0xxxxx, 3xxxxx),
    Shanghai-listed use .XSHG (6xxxxx).
    """
    t = ticker.strip()
    if market_label == "创业板":   # 300xxx → Shenzhen
        return f"{t}.XSHE"
    if market_label == "科创板":   # 688xxx → Shanghai
        return f"{t}.XSHG"
    if market_label == "主板":
        return f"{t}.XSHG" if t.startswith("6") else f"{t}.XSHE"
    return f"{t}.XSHG" if t.startswith("6") else f"{t}.XSHE"


def _get_client(settings) -> Client:
    """Return the cached client, lazily building it on first use or after invalidate."""
    global _client, _client_key
    key = _client_signature(settings)
    with _client_lock:
        if _client is not None and _client_key == key:
            return _client
        # Settings changed, or client was invalidated — rebuild.
        try:
            if _client is not None:
                _client.close()
        except Exception:
            pass
        _client = _build_client(settings)
        _client_key = key
        return _client


def fetch_ashare_quotes_sync(tickers: list[tuple[str, str]], settings) -> dict[str, dict[str, Any]]:
    """Batch-fetch all A-share quotes in a single ClickHouse query.

    Input: list of (ticker, market_label) tuples for 创业板/科创板/主板.
    Output: dict keyed by original ticker with price/prev_close/market_cap/name/change_pct.
    """
    if not tickers:
        return {}
    ticker_to_sym = {t: _to_ch_symbol(t, m) for t, m in tickers}
    sym_to_ticker = {v: k for k, v in ticker_to_sym.items()}
    symbols = list(ticker_to_sym.values())

    # Realtime market cap derivation:
    #   prev_mcap (= total_shares × prev_close_unadj) is in t_adj_daily_data.market_value
    #   close_price in that table is BACK-ADJUSTED, so prev_close_unadj = close_price / accum_adj_factor
    #   realtime_mcap = prev_mcap × latest_price / prev_close_unadj
    # This matches yfinance's marketCap and tracks intraday moves.
    query = """
    WITH latest AS (
        SELECT symbol,
               argMax(close, time_key) AS latest_price,
               max(time_key) AS latest_t
        FROM t_realtime_kline_1m
        WHERE symbol IN %(syms)s
        GROUP BY symbol
    ),
    prev_rt AS (
        SELECT symbol,
               argMax(close, time_key) AS prev_close_rt
        FROM t_realtime_kline_1m
        WHERE symbol IN %(syms)s
          AND toDate(time_key) < (
              SELECT max(toDate(time_key))
              FROM t_realtime_kline_1m
              WHERE symbol IN %(syms)s
          )
        GROUP BY symbol
    ),
    daily AS (
        SELECT symbol,
               argMax(market_value, trade_date) AS prev_mcap,
               argMax(neg_market_value, trade_date) AS prev_float_mcap,
               argMax(close_price / nullIf(accum_adj_factor, 0), trade_date) AS prev_close_daily,
               argMax(name, trade_date) AS name_cn
        FROM t_adj_daily_data
        WHERE symbol IN %(syms)s
        GROUP BY symbol
    )
    SELECT l.symbol,
           d.name_cn,
           l.latest_price,
           coalesce(p.prev_close_rt, d.prev_close_daily) AS prev_close,
           -- realtime market cap: scale prev_mcap by today's price move
           if(d.prev_close_daily > 0 AND l.latest_price > 0,
              d.prev_mcap * l.latest_price / d.prev_close_daily,
              d.prev_mcap) AS market_cap,
           if(d.prev_close_daily > 0 AND l.latest_price > 0,
              d.prev_float_mcap * l.latest_price / d.prev_close_daily,
              d.prev_float_mcap) AS float_market_cap,
           l.latest_t
    FROM latest l
    LEFT JOIN prev_rt p ON l.symbol = p.symbol
    LEFT JOIN daily d ON l.symbol = d.symbol
    """
    try:
        with _unproxy():
            client = _get_client(settings)
        result = client.query(query, parameters={"syms": symbols})
    except Exception as e:
        # A stale / poisoned connection survives in the pool; drop it so the
        # next call rebuilds instead of compounding the failure.
        _invalidate_client()
        logger.exception("ClickHouse A-share query failed: %s", e)
        return {}

    out: dict[str, dict[str, Any]] = {}
    for row in result.result_rows:
        sym, name_cn, latest, prev, mcap, float_mcap, latest_t = row
        ticker = sym_to_ticker.get(sym)
        if ticker is None:
            continue
        change_pct = None
        if latest and prev and prev != 0:
            change_pct = (latest - prev) / prev * 100
        out[ticker] = {
            "ch_symbol": sym,
            "name_cn": name_cn or "",
            "latest_price": float(latest) if latest else None,
            "prev_close": float(prev) if prev else None,
            "change_pct": change_pct,
            "market_cap": float(mcap) if mcap else None,        # 总市值 (realtime)
            "float_market_cap": float(float_mcap) if float_mcap else None,  # 流通市值 (realtime)
            "currency": "CNY",
            "latest_t": latest_t,
        }
    return out
