"""A-share 一致预期 (analyst consensus forecast) data from Wind MySQL.

Queries `wind.ASHARECONSENSUSROLLINGDATAHIS` (rolling FY1/FY2/FY3 forecasts:
net profit, EPS, PE, PB, ROE, revenue) and `wind.ASHARESTOCKRATINGCONSUSHIS`
(target price + buy/outperform/hold/underperform/sell rating counts).

Read-only, best-effort. Any MySQL failure returns an empty dict so the
dashboard keeps working. Results are cached in Redis for 30 minutes —
consensus updates at most a few times per day.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field, asdict
from typing import Any

import pymysql

logger = logging.getLogger(__name__)

CACHE_TTL_SECONDS = 1800  # 30 min
QUERY_TIMEOUT_SECONDS = 30.0  # Wind tables have no secondary indexes — full scan is slow
LOOKBACK_DAYS = 180  # how far back to search for latest consensus row per ticker
RATING_CYCLE_180D = "263003000"  # Wind code: 30d=263001000, 90d=263002000, 180d=263003000


# ─── Wind code mapping ──────────────────────────────────────────────────────


def to_windcode(ticker: str, market_label: str) -> str | None:
    """Map (ticker, market) → Wind S_INFO_WINDCODE. Returns None for non-A-share."""
    t = (ticker or "").strip()
    if not t or not t.isdigit():
        return None
    # A-share markets in portfolio_sources.yaml: 主板 / 创业板 / 科创板
    if market_label in ("主板", "创业板", "科创板"):
        if t.startswith(("6", "9")):  # Shanghai main-board + 科创板 688xxx + B-share 900
            return f"{t}.SH"
        if t.startswith(("0", "3")):  # Shenzhen main + SME 00x/002 + 创业板 300
            return f"{t}.SZ"
        if t.startswith(("4", "8")):  # Beijing 北交所
            return f"{t}.BJ"
    return None


# ─── Rating translation ─────────────────────────────────────────────────────


def rating_label(avg: float | None) -> str:
    """Wind scale: 1=buy, 5=sell. Buckets match Wind's own terminology."""
    if avg is None:
        return ""
    if avg < 1.5:
        return "买入"
    if avg < 2.5:
        return "增持"
    if avg < 3.5:
        return "中性"
    if avg < 4.5:
        return "减持"
    return "卖出"


# ─── Result dataclass ───────────────────────────────────────────────────────


@dataclass
class FyForecast:
    year: str | None = None        # benchmark fiscal year (YYYY)
    net_profit: float | None = None  # raw RMB
    eps: float | None = None
    pe: float | None = None
    pb: float | None = None
    roe: float | None = None       # %
    revenue: float | None = None   # raw RMB


@dataclass
class ConsensusData:
    ticker: str
    windcode: str
    as_of: str | None = None              # YYYYMMDD
    analyst_count: int | None = None
    target_price: float | None = None
    target_price_num_inst: int | None = None
    rating_avg: float | None = None
    rating_label: str = ""
    rating_num_buy: int = 0
    rating_num_outperform: int = 0
    rating_num_hold: int = 0
    rating_num_underperform: int = 0
    rating_num_sell: int = 0
    fy1: FyForecast = field(default_factory=FyForecast)
    fy2: FyForecast = field(default_factory=FyForecast)
    fy3: FyForecast = field(default_factory=FyForecast)
    yoy_net_profit: float | None = None   # YoY net profit growth %


# ─── MySQL query (sync; run in executor) ────────────────────────────────────


def _fetch_sync(windcodes: list[str], settings) -> dict[str, dict[str, Any]]:
    """Returns {windcode: {rolling, rating}} raw dicts. pymysql is sync-only.

    Wind's consensus tables have only a PRIMARY index on OBJECT_ID, so any
    WHERE clause triggers a full-table scan. We minimise the scan with a
    lookback date filter and pick the latest row per (windcode, rolling_type)
    in Python rather than via a second aggregation subquery.
    """
    if not windcodes:
        return {}
    from datetime import date, timedelta
    cutoff = (date.today() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y%m%d")

    conn = pymysql.connect(
        host=settings.consensus_mysql_host,
        port=settings.consensus_mysql_port,
        user=settings.consensus_mysql_user,
        password=settings.consensus_mysql_password,
        database=settings.consensus_mysql_db,
        connect_timeout=5,
        read_timeout=int(QUERY_TIMEOUT_SECONDS),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )
    try:
        with conn.cursor() as cur:
            placeholders = ",".join(["%s"] * len(windcodes))

            sql_rolling = f"""
                SELECT S_INFO_WINDCODE, ROLLING_TYPE, EST_DT, BENCHMARK_YR,
                       NET_PROFIT, EST_EPS, EST_PE, EST_PB, EST_ROE, EST_OPER_REVENUE
                FROM ASHARECONSENSUSROLLINGDATAHIS
                WHERE EST_DT >= %s
                  AND S_INFO_WINDCODE IN ({placeholders})
                  AND ROLLING_TYPE IN ('FY1','FY2','FY3','YOY')
            """
            cur.execute(sql_rolling, (cutoff, *windcodes))
            rolling_rows = cur.fetchall()

            sql_rating = f"""
                SELECT S_INFO_WINDCODE, RATING_DT, S_WRATING_AVG, S_WRATING_INSTNUM,
                       S_WRATING_NUMOFBUY, S_WRATING_NUMOFOUTPERFORM, S_WRATING_NUMOFHOLD,
                       S_WRATING_NUMOFUNDERPERFORM, S_WRATING_NUMOFSELL,
                       S_EST_PRICE, S_EST_PRICEINSTNUM
                FROM ASHARESTOCKRATINGCONSUSHIS
                WHERE RATING_DT >= %s
                  AND S_INFO_WINDCODE IN ({placeholders})
                  AND S_WRATING_CYCLE=%s
            """
            cur.execute(sql_rating, (cutoff, *windcodes, RATING_CYCLE_180D))
            rating_rows = cur.fetchall()
    finally:
        conn.close()

    # Latest row per (windcode, rolling_type) — rolling
    out: dict[str, dict[str, Any]] = {w: {"rolling": {}, "rating": None} for w in windcodes}
    for r in rolling_rows:
        wc = r["S_INFO_WINDCODE"]
        rt = r["ROLLING_TYPE"]
        if wc not in out:
            continue
        cur_row = out[wc]["rolling"].get(rt)
        if cur_row is None or (r["EST_DT"] or "") > (cur_row["EST_DT"] or ""):
            out[wc]["rolling"][rt] = r

    # Latest row per windcode — rating. If a day has duplicate rows, prefer
    # the one with the highest analyst count (it's the fuller sample).
    for r in rating_rows:
        wc = r["S_INFO_WINDCODE"]
        if wc not in out:
            continue
        cur_row = out[wc]["rating"]
        if cur_row is None:
            out[wc]["rating"] = r
            continue
        cur_dt = cur_row["RATING_DT"] or ""
        new_dt = r["RATING_DT"] or ""
        if new_dt > cur_dt:
            out[wc]["rating"] = r
        elif new_dt == cur_dt:
            cur_n = _to_float(cur_row.get("S_WRATING_INSTNUM")) or 0
            new_n = _to_float(r.get("S_WRATING_INSTNUM")) or 0
            if new_n > cur_n:
                out[wc]["rating"] = r
    return out


def _to_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f


def _build_fy(row: dict[str, Any] | None, year_offset: int = 0) -> FyForecast:
    """BENCHMARK_YR is the FY0 (last-actual) reporting year; each FY-forward row
    shifts the target year by year_offset. Wind stores the same BENCHMARK_YR
    on FY1/FY2/FY3 rows, so we compute the actual forecasted year here.
    """
    if not row:
        return FyForecast()
    base = row.get("BENCHMARK_YR") or ""
    year = None
    if len(base) >= 4 and base[:4].isdigit():
        year = str(int(base[:4]) + year_offset)
    return FyForecast(
        year=year,
        net_profit=_to_float(row.get("NET_PROFIT")),
        eps=_to_float(row.get("EST_EPS")),
        pe=_to_float(row.get("EST_PE")),
        pb=_to_float(row.get("EST_PB")),
        roe=_to_float(row.get("EST_ROE")),
        revenue=_to_float(row.get("EST_OPER_REVENUE")),
    )


def _assemble(ticker: str, windcode: str, raw: dict[str, Any]) -> ConsensusData | None:
    rolling = raw.get("rolling") or {}
    rating = raw.get("rating")
    if not rolling and not rating:
        return None

    fy1_row = rolling.get("FY1")
    fy2_row = rolling.get("FY2")
    fy3_row = rolling.get("FY3")
    yoy_row = rolling.get("YOY")

    # EST_DT for "as_of" — prefer FY1, fall back to any present
    as_of = None
    for row in (fy1_row, fy2_row, fy3_row, yoy_row):
        if row and row.get("EST_DT"):
            as_of = row["EST_DT"]
            break

    data = ConsensusData(
        ticker=ticker,
        windcode=windcode,
        as_of=as_of,
        fy1=_build_fy(fy1_row, year_offset=1),
        fy2=_build_fy(fy2_row, year_offset=2),
        fy3=_build_fy(fy3_row, year_offset=3),
        yoy_net_profit=_to_float(yoy_row.get("NET_PROFIT")) if yoy_row else None,
    )
    if rating:
        data.analyst_count = int(_to_float(rating.get("S_WRATING_INSTNUM")) or 0) or None
        data.target_price = _to_float(rating.get("S_EST_PRICE"))
        data.target_price_num_inst = int(_to_float(rating.get("S_EST_PRICEINSTNUM")) or 0) or None
        data.rating_avg = _to_float(rating.get("S_WRATING_AVG"))
        data.rating_label = rating_label(data.rating_avg)
        data.rating_num_buy = int(_to_float(rating.get("S_WRATING_NUMOFBUY")) or 0)
        data.rating_num_outperform = int(_to_float(rating.get("S_WRATING_NUMOFOUTPERFORM")) or 0)
        data.rating_num_hold = int(_to_float(rating.get("S_WRATING_NUMOFHOLD")) or 0)
        data.rating_num_underperform = int(_to_float(rating.get("S_WRATING_NUMOFUNDERPERFORM")) or 0)
        data.rating_num_sell = int(_to_float(rating.get("S_WRATING_NUMOFSELL")) or 0)
    return data


# ─── Public async API ───────────────────────────────────────────────────────


async def fetch_consensus(
    pairs: list[tuple[str, str]],
    *,
    settings,
    redis=None,
    use_cache: bool = True,
) -> dict[str, ConsensusData]:
    """Fetch consensus forecast for (ticker, market_label) pairs.

    Returns {ticker: ConsensusData}. Only A-share tickers yield data; others
    are silently skipped. Failures degrade to an empty dict (logged).
    """
    if not getattr(settings, "consensus_enabled", False):
        return {}

    # Build windcode → ticker map (A-share only)
    windcode_to_ticker: dict[str, str] = {}
    for ticker, market in pairs:
        wc = to_windcode(ticker, market)
        if wc:
            windcode_to_ticker[wc] = ticker
    if not windcode_to_ticker:
        return {}

    # Redis cache — single key batched (simpler than per-ticker and consistent as-of)
    cache_key = "consensus:wind:" + ",".join(sorted(windcode_to_ticker))
    if use_cache and redis is not None:
        try:
            cached = await redis.get(cache_key)
            if cached:
                payload = json.loads(cached)
                return {
                    t: ConsensusData(**{k: (FyForecast(**v) if k.startswith("fy") else v)
                                        for k, v in d.items()})
                    for t, d in payload.items()
                }
        except Exception as e:
            logger.debug("consensus cache read failed: %s", e)

    loop = asyncio.get_running_loop()
    try:
        raw = await asyncio.wait_for(
            loop.run_in_executor(None, _fetch_sync, list(windcode_to_ticker.keys()), settings),
            timeout=QUERY_TIMEOUT_SECONDS + 5,
        )
    except asyncio.TimeoutError:
        logger.warning("Wind consensus MySQL query timed out")
        return {}
    except Exception as e:
        logger.exception("Wind consensus MySQL error: %s", e)
        return {}

    out: dict[str, ConsensusData] = {}
    for wc, ticker in windcode_to_ticker.items():
        assembled = _assemble(ticker, wc, raw.get(wc, {}))
        if assembled is not None:
            out[ticker] = assembled

    # Write cache (best-effort)
    if use_cache and redis is not None and out:
        try:
            payload = {t: {**asdict(d), "fy1": asdict(d.fy1), "fy2": asdict(d.fy2), "fy3": asdict(d.fy3)}
                       for t, d in out.items()}
            # asdict already recurses into nested dataclasses — the above is redundant but harmless
            await redis.setex(cache_key, CACHE_TTL_SECONDS, json.dumps(payload, default=str))
        except Exception as e:
            logger.debug("consensus cache write failed: %s", e)

    return out
