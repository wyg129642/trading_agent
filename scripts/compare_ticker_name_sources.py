"""Compare yfinance vs akshare for **name → ticker mapping** coverage across
six markets (A股/美股/港股/韩股/日股/澳股).

Goal: figure out which source we should pull from to expand
``backend/app/services/ticker_data/aliases.json`` so the LLM can pass
Chinese / English company names to ``kb_search`` and have them resolve
to canonical CODE.MARKET.

Tushare is skipped because the token currently configured in env (or the
one supplied by the operator) doesn't authenticate against Tushare Pro —
it's a Google API key, not a Tushare hex token.
"""
from __future__ import annotations

import os
import sys
import time
import traceback
from contextlib import contextmanager
from dataclasses import dataclass, field, asdict
from typing import Any

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# (canonical_id, market_label, raw_code, expected_cn, expected_en) — sample
# representing 6 markets, mostly large caps so both sources should know them.
SAMPLE: list[tuple[str, str, str, str, str]] = [
    # A股 (主板/创业板/科创板)
    ("600519.SH", "A股", "600519", "贵州茅台", "Kweichow Moutai"),
    ("000001.SZ", "A股", "000001", "平安银行", "Ping An Bank"),
    ("300750.SZ", "A股", "300750", "宁德时代", "CATL"),
    ("688981.SH", "A股", "688981", "中芯国际", "SMIC"),
    # 港股
    ("00700.HK", "港股", "00700", "腾讯控股", "Tencent"),
    ("09988.HK", "港股", "09988", "阿里巴巴-W", "Alibaba"),
    ("03690.HK", "港股", "03690", "美团-W", "Meituan"),
    # 美股
    ("NVDA.US", "美股", "NVDA", "英伟达", "NVIDIA"),
    ("AAPL.US", "美股", "AAPL", "苹果", "Apple"),
    ("TSM.US", "美股", "TSM", "台积电", "Taiwan Semiconductor"),
    # 日股
    ("7203.JP", "日股", "7203", "丰田汽车", "Toyota Motor"),
    ("6758.JP", "日股", "6758", "索尼集团", "Sony Group"),
    # 韩股
    ("005930.KS", "韩股", "005930", "三星电子", "Samsung Electronics"),
    ("000660.KS", "韩股", "000660", "SK海力士", "SK Hynix"),
    # 澳股
    ("BHP.AU", "澳股", "BHP", "必和必拓", "BHP Group"),
    ("CBA.AU", "澳股", "CBA", "联邦银行", "Commonwealth Bank"),
]


PROXY_VARS = ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy",
              "ALL_PROXY", "all_proxy")


@contextmanager
def unproxy():
    """akshare hits domestic CN endpoints — proxy must be disabled."""
    saved = {k: os.environ.pop(k, None) for k in PROXY_VARS}
    try:
        yield
    finally:
        for k, v in saved.items():
            if v is not None:
                os.environ[k] = v


@dataclass
class NameResult:
    source: str
    canonical: str
    cn_name: str = ""
    en_name: str = ""
    raw_code: str = ""
    latency_ms: float = 0.0
    error: str = ""
    extra: dict[str, Any] = field(default_factory=dict)


def _yf_symbol(canonical: str, market: str, raw_code: str) -> str:
    """Convert canonical CODE.MARKET → yfinance symbol."""
    if market == "美股":
        return raw_code
    if market == "A股":
        # 6-prefix → SS (Shanghai), else SZ
        return f"{raw_code}.SS" if raw_code.startswith("6") else f"{raw_code}.SZ"
    if market == "港股":
        # yfinance uses 4-digit (drop leading zero from 5-digit)
        digits = raw_code.lstrip("0").zfill(4)
        return f"{digits}.HK"
    if market == "日股":
        return f"{raw_code}.T"
    if market == "韩股":
        return f"{raw_code}.KS"
    if market == "澳股":
        return f"{raw_code}.AX"
    return raw_code


def fetch_yfinance(canonical: str, market: str, raw_code: str) -> NameResult:
    r = NameResult(source="yfinance", canonical=canonical)
    sym = _yf_symbol(canonical, market, raw_code)
    r.extra["yf_symbol"] = sym
    t0 = time.time()
    try:
        import yfinance as yf
        info = yf.Ticker(sym).info or {}
        r.en_name = info.get("longName") or info.get("shortName") or ""
        # yfinance occasionally has a localized name field; rare for non-CN markets
        r.cn_name = info.get("displayName") if (
            info.get("displayName") and any("一" <= c <= "鿿" for c in info.get("displayName", ""))
        ) else ""
        if not r.en_name and not r.cn_name:
            r.error = "info missing both name fields"
        r.raw_code = sym
    except Exception as e:
        r.error = f"{type(e).__name__}: {str(e)[:80]}"
    r.latency_ms = (time.time() - t0) * 1000
    return r


# Cache the bulk akshare fetches so we hit each endpoint only once
_AK_CACHE: dict[str, Any] = {}


def _ak_get(market: str):
    import akshare as ak
    if market in _AK_CACHE:
        return _AK_CACHE[market]
    with unproxy():
        if market == "A股":
            df = ak.stock_zh_a_spot_em()
        elif market == "港股":
            df = ak.stock_hk_spot_em()
        elif market == "美股":
            df = ak.stock_us_spot_em()
        else:
            df = None
    _AK_CACHE[market] = df
    return df


def fetch_akshare(canonical: str, market: str, raw_code: str) -> NameResult:
    r = NameResult(source="akshare", canonical=canonical)
    t0 = time.time()
    try:
        df = _ak_get(market)
        if df is None:
            r.error = f"akshare has no bulk endpoint for {market}"
        elif market == "A股":
            hit = df[df["代码"] == raw_code]
            if hit.empty:
                r.error = "code not in stock_zh_a_spot_em"
            else:
                r.cn_name = str(hit.iloc[0]["名称"]).strip()
                r.raw_code = raw_code
        elif market == "港股":
            code5 = raw_code.lstrip("0").zfill(5)
            hit = df[df["代码"] == code5]
            if hit.empty:
                r.error = f"code {code5} not in stock_hk_spot_em"
            else:
                r.cn_name = str(hit.iloc[0]["名称"]).strip()
                # 港股 spot also has 英文名 in some snapshots
                if "英文名" in df.columns:
                    r.en_name = str(hit.iloc[0]["英文名"]).strip()
                r.raw_code = code5
        elif market == "美股":
            # akshare US spot 代码 like "105.NVDA" — match by suffix
            code_col = df["代码"].astype(str)
            hit = df[code_col.str.endswith(f".{raw_code}") | (code_col == raw_code)]
            if hit.empty:
                r.error = f"code {raw_code} not in stock_us_spot_em"
            else:
                r.cn_name = str(hit.iloc[0]["名称"]).strip()
                # akshare US also has 英文名 column
                if "英文名" in df.columns:
                    r.en_name = str(hit.iloc[0]["英文名"]).strip()
                r.raw_code = str(hit.iloc[0]["代码"])
    except Exception as e:
        r.error = f"{type(e).__name__}: {str(e)[:80]}"
    r.latency_ms = (time.time() - t0) * 1000
    return r


def truncate(s: str, n: int = 20) -> str:
    if len(s) <= n:
        return s
    return s[: n - 1] + "…"


def main():
    rows: list[NameResult] = []
    for canonical, market, raw_code, _, _ in SAMPLE:
        for fn in (fetch_yfinance, fetch_akshare):
            try:
                r = fn(canonical, market, raw_code)
            except Exception:
                traceback.print_exc()
                continue
            rows.append(r)

    # Per-row table
    print(f"\n{'='*128}")
    print(f"{'canonical':<12}{'mkt':<6}{'src':<10}{'cn_name':<22}{'en_name':<32}{'ms':>7}  err")
    print("=" * 128)
    for r in rows:
        cn = truncate(r.cn_name, 20) if r.cn_name else "—"
        en = truncate(r.en_name, 30) if r.en_name else "—"
        market = next((m for c, m, *_ in SAMPLE if c == r.canonical), "?")
        err = truncate(r.error, 36) if r.error else ""
        print(f"{r.canonical:<12}{market:<6}{r.source:<10}{cn:<22}{en:<32}{r.latency_ms:>6.0f}  {err}")

    # Per-source coverage matrix
    print(f"\n{'='*72}")
    print("Coverage matrix — per (market, source)")
    print("=" * 72)
    print(f"{'market':<8}{'src':<10}{'cn_hit':>8}{'en_hit':>8}{'errors':>8}{'avg_ms':>10}")
    by_bucket: dict[tuple[str, str], list[NameResult]] = {}
    for r in rows:
        market = next((m for c, m, *_ in SAMPLE if c == r.canonical), "?")
        by_bucket.setdefault((market, r.source), []).append(r)
    for (market, src), bucket in sorted(by_bucket.items()):
        cn_hit = sum(1 for r in bucket if r.cn_name)
        en_hit = sum(1 for r in bucket if r.en_name)
        errs = sum(1 for r in bucket if r.error)
        avg_ms = sum(r.latency_ms for r in bucket) / len(bucket)
        n = len(bucket)
        print(f"{market:<8}{src:<10}{f'{cn_hit}/{n}':>8}{f'{en_hit}/{n}':>8}{errs:>8}{avg_ms:>10.0f}")


if __name__ == "__main__":
    main()
