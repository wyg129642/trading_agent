"""3-way comparison of ticker→name sources for the LLM alias table.

Sources:
  1. yfinance (per-ticker, EN-name-only, slow but universal)
  2. Tushare Pro (bulk, CN+EN for A/HK; us_basic broken on free tier)
  3. Existing CSVs in /home/ygwang/trading_agent/data/{a,hk,us,jp,kr}_stock_list.csv
     (CN names; format varies per market)

Output: per-ticker hit table + per-(market, source) coverage matrix +
overall recommendation per market.
"""
from __future__ import annotations

import csv
import os
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

PROD_DATA_DIR = "/home/ygwang/trading_agent/data"
TUSHARE_TOKEN = "32edd62d8ec424bd141e2992ffd0725c51b246e205115188d1576229"

# (canonical_id, market_label, raw_code, expected_cn, expected_en)
SAMPLE: list[tuple[str, str, str, str, str]] = [
    ("600519.SH", "A股",  "600519", "贵州茅台", "Kweichow Moutai"),
    ("000001.SZ", "A股",  "000001", "平安银行", "Ping An Bank"),
    ("300750.SZ", "A股",  "300750", "宁德时代", "CATL"),
    ("688981.SH", "A股",  "688981", "中芯国际", "SMIC"),
    ("00700.HK",  "港股", "00700",  "腾讯控股", "Tencent"),
    ("09988.HK",  "港股", "09988",  "阿里巴巴-W", "Alibaba"),
    ("03690.HK",  "港股", "03690",  "美团-W", "Meituan"),
    ("NVDA.US",   "美股", "NVDA",   "英伟达", "NVIDIA"),
    ("AAPL.US",   "美股", "AAPL",   "苹果", "Apple"),
    ("TSM.US",    "美股", "TSM",    "台积电", "Taiwan Semiconductor"),
    ("7203.JP",   "日股", "7203",   "丰田汽车", "Toyota Motor"),
    ("6758.JP",   "日股", "6758",   "索尼集团", "Sony Group"),
    ("005930.KS", "韩股", "005930", "三星电子", "Samsung Electronics"),
    ("000660.KS", "韩股", "000660", "SK海力士", "SK Hynix"),
    ("BHP.AU",    "澳股", "BHP",    "必和必拓", "BHP Group"),
    ("CBA.AU",    "澳股", "CBA",    "联邦银行", "Commonwealth Bank"),
]

PROXY_VARS = ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy",
              "ALL_PROXY", "all_proxy")


@contextmanager
def unproxy():
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
    market: str
    cn_name: str = ""
    en_name: str = ""
    error: str = ""
    extra: dict[str, Any] = field(default_factory=dict)


# ────────────── Tushare bulk caches (one-shot fetch) ──────────────

_TS_CACHE: dict[str, Any] = {}


def _ts_load():
    """Bulk-fetch the three Tushare endpoints once."""
    if _TS_CACHE:
        return
    import tushare as ts
    ts.set_token(TUSHARE_TOKEN)
    pro = ts.pro_api()
    with unproxy():
        try:
            _TS_CACHE["A股"] = pro.stock_basic(
                exchange="", list_status="L",
                fields="ts_code,symbol,name,enname,exchange,market,list_date",
            )
        except Exception as e:
            _TS_CACHE["A股_err"] = str(e)
        try:
            _TS_CACHE["港股"] = pro.hk_basic(
                list_status="L",
                fields="ts_code,name,enname,fullname,list_date",
            )
        except Exception as e:
            _TS_CACHE["港股_err"] = str(e)
        try:
            _TS_CACHE["美股"] = pro.us_basic(
                list_status="L",
                fields="ts_code,name,enname,classify,list_date",
            )
        except Exception as e:
            _TS_CACHE["美股_err"] = str(e)


def fetch_tushare(canonical: str, market: str, raw_code: str) -> NameResult:
    r = NameResult(source="tushare", canonical=canonical, market=market)
    try:
        _ts_load()
        df = _TS_CACHE.get(market)
        if df is None:
            r.error = _TS_CACHE.get(f"{market}_err", f"no Tushare endpoint for {market}")
            return r
        if market == "A股":
            hit = df[df["ts_code"] == canonical]
        elif market == "港股":
            hit = df[df["ts_code"] == canonical]
        elif market == "美股":
            hit = df[df["ts_code"].fillna("").str.startswith(raw_code + ".", na=False)]
            if hit.empty:
                hit = df[df["ts_code"] == raw_code]
        else:
            r.error = f"Tushare doesn't cover {market}"
            return r
        if hit.empty:
            r.error = f"{raw_code} not in Tushare {market}"
            return r
        row = hit.iloc[0]
        r.cn_name = str(row.get("name") or "").strip() if row.get("name") else ""
        r.en_name = str(row.get("enname") or "").strip() if row.get("enname") else ""
    except Exception as e:
        r.error = f"{type(e).__name__}: {str(e)[:80]}"
    return r


# ────────────── Existing prod CSV caches ──────────────

_CSV_CACHE: dict[str, dict[str, dict[str, str]]] = {}


def _csv_load(market: str) -> dict[str, dict[str, str]]:
    """Load a prod CSV, key by code."""
    if market in _CSV_CACHE:
        return _CSV_CACHE[market]
    fname = {
        "A股": "a_stock_list.csv",
        "港股": "hk_stock_list.csv",
        "美股": "us_stock_list.csv",
        "日股": "jp_stock_list.csv",
        "韩股": "kr_stock_list.csv",
        "澳股": None,
    }.get(market)
    if not fname:
        _CSV_CACHE[market] = {}
        return {}
    path = os.path.join(PROD_DATA_DIR, fname)
    out: dict[str, dict[str, str]] = {}
    if not os.path.exists(path):
        _CSV_CACHE[market] = {}
        return out
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        # Normalize column names: A股 uses "代码","名称"; HK uses code,name,name_cn; US/JP/KR use code,name
        for row in reader:
            code = row.get("代码") or row.get("code") or ""
            code = code.strip()
            if not code:
                continue
            cn_name = (row.get("名称") or row.get("name_cn") or row.get("name") or "").strip()
            # HK CSV: name = traditional, name_cn = simplified — prefer name_cn
            if market == "港股" and row.get("name_cn"):
                cn_name = row["name_cn"].strip()
            out[code] = {"cn_name": cn_name, "raw_name": row.get("name", "").strip()}
    _CSV_CACHE[market] = out
    return out


def fetch_csv(canonical: str, market: str, raw_code: str) -> NameResult:
    r = NameResult(source="prod_csv", canonical=canonical, market=market)
    table = _csv_load(market)
    if not table:
        r.error = f"no CSV file for {market}"
        return r
    # A股: lookup by 6-digit code; HK: 5-digit; US: bare; JP/KR: 6-digit padded
    key = raw_code if market != "港股" else raw_code.lstrip("0").zfill(5)
    hit = table.get(key)
    if not hit:
        r.error = f"{key} not in {market} CSV"
        return r
    r.cn_name = hit["cn_name"]
    return r


# ────────────── yfinance per-ticker ──────────────

def _yf_symbol(canonical: str, market: str, raw_code: str) -> str:
    if market == "美股":
        return raw_code
    if market == "A股":
        return f"{raw_code}.SS" if raw_code.startswith("6") else f"{raw_code}.SZ"
    if market == "港股":
        return f"{raw_code.lstrip('0').zfill(4)}.HK"
    if market == "日股":
        return f"{raw_code}.T"
    if market == "韩股":
        return f"{raw_code}.KS"
    if market == "澳股":
        return f"{raw_code}.AX"
    return raw_code


def fetch_yfinance(canonical: str, market: str, raw_code: str) -> NameResult:
    r = NameResult(source="yfinance", canonical=canonical, market=market)
    sym = _yf_symbol(canonical, market, raw_code)
    r.extra["yf_symbol"] = sym
    try:
        import yfinance as yf
        info = yf.Ticker(sym).info or {}
        r.en_name = info.get("longName") or info.get("shortName") or ""
        if not r.en_name:
            r.error = "longName/shortName both empty"
    except Exception as e:
        r.error = f"{type(e).__name__}: {str(e)[:80]}"
    return r


# ────────────── Output ──────────────

def truncate(s: str, n: int = 22) -> str:
    return s[: n - 1] + "…" if len(s) > n else s


def main():
    rows: list[NameResult] = []
    for canonical, market, raw_code, _, _ in SAMPLE:
        for fn in (fetch_tushare, fetch_csv, fetch_yfinance):
            try:
                rows.append(fn(canonical, market, raw_code))
            except Exception as e:
                rows.append(NameResult(source=fn.__name__, canonical=canonical,
                                       market=market, error=f"crash: {e}"))

    print(f"\n{'='*120}")
    print(f"{'canonical':<12}{'mkt':<5}{'src':<10}{'cn_name':<22}{'en_name':<32} err")
    print("=" * 120)
    last_canonical = None
    for r in rows:
        if r.canonical != last_canonical and last_canonical is not None:
            print()
        last_canonical = r.canonical
        cn = truncate(r.cn_name, 20) if r.cn_name else "—"
        en = truncate(r.en_name, 30) if r.en_name else "—"
        err = truncate(r.error, 36) if r.error else ""
        print(f"{r.canonical:<12}{r.market:<5}{r.source:<10}{cn:<22}{en:<32} {err}")

    # Per-(market, source) coverage matrix
    print(f"\n{'='*82}")
    print("Coverage matrix")
    print("=" * 82)
    print(f"{'market':<6}{'src':<10}{'cn_hit':>10}{'en_hit':>10}{'errors':>10}")
    by_bucket: dict[tuple[str, str], list[NameResult]] = {}
    for r in rows:
        by_bucket.setdefault((r.market, r.source), []).append(r)
    for (market, src), bucket in sorted(by_bucket.items()):
        cn_hit = sum(1 for r in bucket if r.cn_name)
        en_hit = sum(1 for r in bucket if r.en_name)
        errs = sum(1 for r in bucket if r.error)
        n = len(bucket)
        print(f"{market:<6}{src:<10}{f'{cn_hit}/{n}':>10}{f'{en_hit}/{n}':>10}{errs:>10}")

    # Per-source bulk size
    print(f"\n{'='*60}")
    print("Bulk fetch size (per source × market)")
    print("=" * 60)
    print(f"{'market':<6}{'tushare':<14}{'prod_csv':<14}{'yfinance':<14}")
    for market in ("A股", "港股", "美股", "日股", "韩股", "澳股"):
        ts_n = "—"
        if market in _TS_CACHE and hasattr(_TS_CACHE[market], "__len__"):
            ts_n = str(len(_TS_CACHE[market]))
        elif f"{market}_err" in _TS_CACHE:
            ts_n = "FAIL"
        else:
            ts_n = "no endpt"
        csv_n = str(len(_csv_load(market))) if _csv_load(market) else "—"
        yf_n = "per-ticker"
        print(f"{market:<6}{ts_n:<14}{csv_n:<14}{yf_n:<14}")


if __name__ == "__main__":
    main()
