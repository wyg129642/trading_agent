"""Stock quote service — source-routed realtime quotes.

Primary routing (Futu OpenAPI via local FutuOpenD):
  - 美股 / 港股 / 创业板 / 科创板 / 主板 → Futu get_market_snapshot
    returns price + prev_close + market_cap + PE (TTM) in a single call.

Fallbacks (used when Futu is down or the symbol isn't configured for Futu):
  - 美股          → Alpaca (IEX free feed) + yfinance for mcap/PE
  - 创业板/科创板/主板 → ClickHouse db_market.t_realtime_kline_1m + yfinance for PE
  - 港股/韩股/日股/澳股 → yfinance only (15-min delayed for HK)
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import asdict, dataclass
from typing import Any

import redis.asyncio as aioredis

from backend.app.services.quote_providers.alpaca_provider import fetch_us_quotes
from backend.app.services.quote_providers.clickhouse_provider import fetch_ashare_quotes_sync
from backend.app.services.quote_providers import futu_provider

logger = logging.getLogger(__name__)

QUOTE_TTL_SECONDS = 90           # 90s — wider than frontend's 60s auto-refresh so cache usually hits
QUOTE_ERROR_TTL_SECONDS = 30     # 30s for errors
FAST_INFO_TIMEOUT = 6.0
PE_TIMEOUT = 2.5
MAX_YF_CONCURRENCY = 10
_YF_SEM: asyncio.Semaphore | None = None

A_SHARE_MARKETS = ("创业板", "科创板", "主板")
FUTU_HK_US_MARKETS = ("美股", "港股")
FUTU_ALL_MARKETS = FUTU_HK_US_MARKETS + A_SHARE_MARKETS


def _yf_sem() -> asyncio.Semaphore:
    global _YF_SEM
    if _YF_SEM is None:
        _YF_SEM = asyncio.Semaphore(MAX_YF_CONCURRENCY)
    return _YF_SEM


@dataclass
class StockQuote:
    ticker: str
    yf_symbol: str
    prev_close: float | None = None
    latest_price: float | None = None
    change_pct: float | None = None
    market_cap: float | None = None
    pe_ttm: float | None = None
    currency: str = ""
    source: str = ""          # which provider filled price ("alpaca"/"clickhouse"/"yfinance")
    fetched_at: float = 0.0
    error: str = ""


def _to_yf_symbol(ticker: str, market_label: str) -> str:
    t = ticker.strip()
    if market_label == "美股":
        return t
    if market_label in ("创业板", "科创板"):
        return f"{t}.SZ" if t.startswith(("0", "3")) else f"{t}.SS"
    if market_label == "主板":
        return f"{t}.SS" if t.startswith("6") else f"{t}.SZ"
    if market_label == "港股":
        return f"{t.lstrip('0').zfill(4)}.HK"
    if market_label == "韩股":
        return f"{t}.KS"
    if market_label == "日股":
        return f"{t}.T"
    if market_label == "澳股":
        return f"{t}.AX"
    return t


def _cache_key(ticker: str, market_label: str) -> str:
    return f"stock_quote:{market_label}:{ticker}"


# ─── yfinance helpers ───────────────────────────────────────────────────────


def _yf_fast_info_sync(yf_symbol: str) -> dict[str, Any]:
    import yfinance as yf
    fi = yf.Ticker(yf_symbol).fast_info
    return {
        "latest_price": fi.get("lastPrice"),
        "prev_close": fi.get("previousClose"),
        "market_cap": fi.get("marketCap"),
        "currency": fi.get("currency") or "",
    }


def _yf_pe_sync(yf_symbol: str) -> float | None:
    import yfinance as yf
    info = yf.Ticker(yf_symbol).info or {}
    return info.get("trailingPE")


def _yf_mcap_pe_sync(yf_symbol: str) -> tuple[float | None, float | None]:
    """Single heavy call — both market_cap and PE from info."""
    import yfinance as yf
    info = yf.Ticker(yf_symbol).info or {}
    return info.get("marketCap"), info.get("trailingPE")


async def _yf_fast_info(yf_symbol: str) -> dict[str, Any]:
    loop = asyncio.get_running_loop()
    async with _yf_sem():
        return await asyncio.wait_for(
            loop.run_in_executor(None, _yf_fast_info_sync, yf_symbol),
            timeout=FAST_INFO_TIMEOUT,
        )


async def _yf_pe(yf_symbol: str) -> float | None:
    loop = asyncio.get_running_loop()
    async with _yf_sem():
        try:
            return await asyncio.wait_for(
                loop.run_in_executor(None, _yf_pe_sync, yf_symbol),
                timeout=PE_TIMEOUT,
            )
        except Exception as e:
            logger.debug("PE fetch skipped for %s: %s", yf_symbol, e)
            return None


async def _yf_mcap_pe(yf_symbol: str) -> tuple[float | None, float | None]:
    loop = asyncio.get_running_loop()
    async with _yf_sem():
        try:
            return await asyncio.wait_for(
                loop.run_in_executor(None, _yf_mcap_pe_sync, yf_symbol),
                timeout=PE_TIMEOUT,
            )
        except Exception as e:
            logger.debug("mcap/PE fetch skipped for %s: %s", yf_symbol, e)
            return (None, None)


async def _build_yf_only_quote(ticker: str, market_label: str) -> StockQuote:
    """HK/KR/JP/AU path — everything from yfinance."""
    yf_symbol = _to_yf_symbol(ticker, market_label)
    quote = StockQuote(
        ticker=ticker, yf_symbol=yf_symbol, source="yfinance", fetched_at=time.time()
    )
    try:
        data = await _yf_fast_info(yf_symbol)
        quote.latest_price = data.get("latest_price")
        quote.prev_close = data.get("prev_close")
        quote.market_cap = data.get("market_cap")
        quote.currency = data.get("currency", "")
        if quote.latest_price and quote.prev_close:
            quote.change_pct = (quote.latest_price - quote.prev_close) / quote.prev_close * 100
    except Exception as e:
        quote.error = f"{type(e).__name__}: {e}"
        return quote
    quote.pe_ttm = await _yf_pe(yf_symbol)
    return quote


# ─── Cache helpers ──────────────────────────────────────────────────────────


async def _read_cache(
    pairs: list[tuple[str, str]], redis: aioredis.Redis | None
) -> tuple[dict[str, StockQuote], list[tuple[str, str]]]:
    """Split pairs into (cached, misses)."""
    if redis is None:
        return {}, list(pairs)
    keys = [_cache_key(t, m) for t, m in pairs]
    try:
        values = await redis.mget(keys)
    except Exception as e:
        logger.warning("Redis mget failed: %s", e)
        return {}, list(pairs)
    cached: dict[str, StockQuote] = {}
    misses: list[tuple[str, str]] = []
    for (ticker, market), v in zip(pairs, values):
        if v:
            try:
                cached[ticker] = StockQuote(**json.loads(v))
                continue
            except Exception:
                pass
        misses.append((ticker, market))
    return cached, misses


async def _write_cache(quote: StockQuote, market: str, redis: aioredis.Redis | None):
    if redis is None:
        return
    ttl = QUOTE_ERROR_TTL_SECONDS if quote.error else QUOTE_TTL_SECONDS
    try:
        await redis.setex(_cache_key(quote.ticker, market), ttl, json.dumps(asdict(quote)))
    except Exception as e:
        logger.warning("Redis setex failed for %s: %s", quote.ticker, e)


# ─── Batch fetchers per source ──────────────────────────────────────────────


async def _fetch_us_batch(tickers: list[str], settings) -> dict[str, StockQuote]:
    """Alpaca for price + prev_close, yfinance for mcap/PE concurrently."""
    quotes: dict[str, StockQuote] = {
        t: StockQuote(ticker=t, yf_symbol=t, source="alpaca", fetched_at=time.time())
        for t in tickers
    }

    # 1) Alpaca batch (single HTTP call)
    alpaca_data = await fetch_us_quotes(
        tickers,
        api_key=settings.alpaca_api_key,
        api_secret=settings.alpaca_api_secret,
        data_url=settings.alpaca_data_url,
    )
    if not alpaca_data and (settings.alpaca_api_key and settings.alpaca_api_secret):
        # Keys present but call failed — mark all as error
        for t in tickers:
            quotes[t].error = "alpaca fetch failed"
    for ticker, d in alpaca_data.items():
        q = quotes.get(ticker)
        if not q:
            continue
        q.latest_price = d.get("latest_price")
        q.prev_close = d.get("prev_close")
        q.change_pct = d.get("change_pct")
        q.currency = d.get("currency", "USD")

    # Fallback: if no Alpaca key configured, fall back to yfinance for everything
    if not settings.alpaca_api_key or not settings.alpaca_api_secret:
        logger.warning(
            "Alpaca keys not configured, US tickers falling back to yfinance"
        )
        tasks = [_build_yf_only_quote(t, "美股") for t in tickers]
        fallback_quotes = await asyncio.gather(*tasks, return_exceptions=True)
        out: dict[str, StockQuote] = {}
        for t, q in zip(tickers, fallback_quotes):
            if isinstance(q, StockQuote):
                out[t] = q
            else:
                out[t] = StockQuote(
                    ticker=t, yf_symbol=t, source="yfinance",
                    fetched_at=time.time(), error=str(q),
                )
        return out

    # 2) yfinance enrichment: mcap + PE (parallel)
    tickers_needing_enrich = [t for t, q in quotes.items() if q.latest_price and not q.error]
    enrich_tasks = [_yf_mcap_pe(t) for t in tickers_needing_enrich]
    if enrich_tasks:
        enriched = await asyncio.gather(*enrich_tasks, return_exceptions=True)
        for t, res in zip(tickers_needing_enrich, enriched):
            if isinstance(res, tuple):
                mcap, pe = res
                quotes[t].market_cap = mcap
                quotes[t].pe_ttm = pe
    return quotes


async def _fetch_ashare_batch(
    pairs: list[tuple[str, str]], settings
) -> dict[str, StockQuote]:
    """ClickHouse for price+mcap+prev, yfinance only for PE."""
    loop = asyncio.get_running_loop()
    out: dict[str, StockQuote] = {
        t: StockQuote(
            ticker=t, yf_symbol=_to_yf_symbol(t, m), source="clickhouse",
            fetched_at=time.time(),
        )
        for t, m in pairs
    }
    try:
        # clickhouse_provider now caches a persistent client, so the warm path
        # is ~20ms. The cold path still pays a server-side 3-35s handshake
        # (not TCP — raw curl is 0.5ms); budget 75s so the first warmer call
        # after restart always primes the cache instead of aborting empty.
        ch_data = await asyncio.wait_for(
            loop.run_in_executor(None, fetch_ashare_quotes_sync, pairs, settings),
            timeout=75.0,
        )
    except asyncio.TimeoutError:
        logger.warning("ClickHouse A-share query timed out")
        ch_data = {}
    except Exception as e:
        logger.exception("ClickHouse A-share query error: %s", e)
        ch_data = {}

    for ticker, d in ch_data.items():
        q = out.get(ticker)
        if not q:
            continue
        q.latest_price = d.get("latest_price")
        q.prev_close = d.get("prev_close")
        q.change_pct = d.get("change_pct")
        q.market_cap = d.get("market_cap")
        q.currency = d.get("currency", "CNY")

    # Mark failed lookups
    for ticker, q in out.items():
        if q.latest_price is None and not q.error:
            q.error = "not found in t_realtime_kline_1m"

    # PE enrichment via yfinance (best-effort)
    tickers_needing_pe = [t for t, q in out.items() if q.latest_price and not q.error]
    pe_tasks = [_yf_pe(out[t].yf_symbol) for t in tickers_needing_pe]
    if pe_tasks:
        pes = await asyncio.gather(*pe_tasks, return_exceptions=True)
        for t, pe in zip(tickers_needing_pe, pes):
            if not isinstance(pe, Exception):
                out[t].pe_ttm = pe
    return out


async def _fetch_futu_batch(
    pairs: list[tuple[str, str]], settings
) -> dict[str, StockQuote]:
    """Single Futu snapshot call covers HK / US / A-share — price, prev, mcap, PE."""
    loop = asyncio.get_running_loop()
    out: dict[str, StockQuote] = {}
    try:
        data = await asyncio.wait_for(
            loop.run_in_executor(
                None,
                futu_provider.fetch_quotes_sync,
                pairs,
                settings.futu_opend_host,
                settings.futu_opend_port,
            ),
            timeout=4.0,
        )
    except asyncio.TimeoutError:
        logger.warning("Futu snapshot query timed out")
        futu_provider._trip_circuit("asyncio timeout")
        return {}
    except Exception as e:
        logger.exception("Futu snapshot error: %s", e)
        futu_provider._trip_circuit(f"{type(e).__name__}: {e}")
        return {}

    for ticker, d in data.items():
        market = next((m for t, m in pairs if t == ticker), "")
        out[ticker] = StockQuote(
            ticker=ticker,
            yf_symbol=_to_yf_symbol(ticker, market),
            latest_price=d.get("latest_price"),
            prev_close=d.get("prev_close"),
            change_pct=d.get("change_pct"),
            market_cap=d.get("market_cap"),
            pe_ttm=d.get("pe_ttm"),
            currency=d.get("currency", ""),
            source="futu",
            fetched_at=time.time(),
        )
    return out


# ─── Public API ─────────────────────────────────────────────────────────────


async def get_quotes(
    pairs: list[tuple[str, str]],
    redis: aioredis.Redis | None = None,
    settings=None,
    use_cache: bool = True,
) -> dict[str, StockQuote]:
    """Fetch quotes for (ticker, market_label) pairs with Redis caching.

    Strategy: one Futu snapshot call serves HK/US/A-share primarily. Anything
    Futu didn't return (connection down, missing symbol, no permission) falls
    back to Alpaca/ClickHouse/yfinance per market.
    """
    if settings is None:
        from backend.app.config import get_settings
        settings = get_settings()

    # 1) Hit cache
    if use_cache:
        cached, misses = await _read_cache(pairs, redis)
    else:
        cached, misses = {}, list(pairs)

    # 2) Primary path — single Futu snapshot. A-shares only if permission is on;
    # Futu's batch is all-or-nothing so one forbidden ticker tanks the whole call.
    futu_allowed = FUTU_ALL_MARKETS if settings.futu_ashare_enabled else FUTU_HK_US_MARKETS
    futu_pairs = [(t, m) for t, m in misses if m in futu_allowed]
    futu_results: dict[str, StockQuote] = {}
    if futu_pairs and settings.futu_login_account:
        futu_results = await _fetch_futu_batch(futu_pairs, settings)

    # 3) Anything Futu didn't cover → fallback providers, split by market
    covered = {t for t, q in futu_results.items() if q.latest_price}
    remaining = [(t, m) for t, m in misses if t not in covered]

    us_misses = [t for t, m in remaining if m == "美股"]
    ashare_misses = [(t, m) for t, m in remaining if m in A_SHARE_MARKETS]
    other_misses = [(t, m) for t, m in remaining if m not in A_SHARE_MARKETS and m != "美股"]

    tasks: list[asyncio.Task] = []
    if us_misses:
        tasks.append(asyncio.create_task(_fetch_us_batch(us_misses, settings)))
    if ashare_misses:
        tasks.append(asyncio.create_task(_fetch_ashare_batch(ashare_misses, settings)))
    other_tasks: list[asyncio.Task] = []
    for t, m in other_misses:
        other_tasks.append(asyncio.create_task(_build_yf_only_quote(t, m)))

    batch_results = await asyncio.gather(*tasks, return_exceptions=True)
    other_results = await asyncio.gather(*other_tasks, return_exceptions=True)

    # 4) Merge — Futu wins, fallback fills the gaps
    fresh: dict[str, StockQuote] = dict(futu_results)
    for res in batch_results:
        if isinstance(res, dict):
            for t, q in res.items():
                if t not in fresh or not fresh[t].latest_price:
                    fresh[t] = q
    for (t, _), res in zip(other_misses, other_results):
        if isinstance(res, StockQuote):
            fresh[t] = res
        else:
            fresh[t] = StockQuote(
                ticker=t, yf_symbol=_to_yf_symbol(t, _), source="yfinance",
                fetched_at=time.time(), error=str(res),
            )

    # 5) Write cache
    market_map = {t: m for t, m in pairs}
    for ticker, q in fresh.items():
        await _write_cache(q, market_map.get(ticker, ""), redis)

    # 6) Combine cached + fresh
    return {**cached, **fresh}
