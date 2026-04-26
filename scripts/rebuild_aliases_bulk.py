"""Build the bulk ticker-alias table for kb_search input normalization.

Sources (decided 2026-04-25 after 3-way comparison in compare_ticker_name_3way.py):
  • A股: Tushare ``stock_basic`` (CN ``name`` + EN ``enname``) — 5,500+ rows.
  • 港股: Tushare ``hk_basic`` (CN ``name`` + EN ``enname`` + ``fullname``) — 2,700+ rows.
  • 美股: ``Tushare us_basic`` (paginated) ∩ prod CSV ``us_stock_list.csv``.
    The intersection (~6,000 rows) is the only set where each ticker has both
    CN (from prod CSV) and EN (from Tushare) names. Long-tail US tickers that
    only have one of the two are skipped per operator decision (2026-04-25).

Skipped:
  • 日股 / 韩股 — only have native-language names (日文 / 한글), and the LLM
    is unlikely to type those literally; alias-matching CJK-non-Chinese leads
    to false positives.
  • 澳股 — no source data.

Output: ``backend/app/services/ticker_data/aliases_bulk.json`` — read by
``ticker_normalizer._alias_table()`` alongside the curated ``aliases.json``
(curated wins on conflict).

Re-run when:
  • New IPOs since last build push popular names that don't resolve.
  • Tushare data quality complaint lands.
"""
from __future__ import annotations

import csv
import json
import os
import re
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_PATH = REPO_ROOT / "backend/app/services/ticker_data/aliases_bulk.json"
PROD_US_CSV = "/home/ygwang/trading_agent/data/us_stock_list.csv"
TUSHARE_TOKEN_ENV = "TUSHARE_TOKEN"


PROXY_VARS = ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy",
              "ALL_PROXY", "all_proxy")


@contextmanager
def unproxy():
    """Tushare API hits domestic CN endpoints — ensure no proxy."""
    saved = {k: os.environ.pop(k, None) for k in PROXY_VARS}
    try:
        yield
    finally:
        for k, v in saved.items():
            if v is not None:
                os.environ[k] = v


# Keys that look like raw codes get filtered — they'd collide with the
# code-classification path in ``_parse_bare`` and either become no-ops or
# (worse) get redirected to a different ticker that happens to share a
# code-shape. Examples: pure digits ("600519"), canonical form ("AAPL.US"),
# market.code style ("US.AAPL"), single-letter ("A"), etc.
_CANONICAL_RE = re.compile(r"^[A-Za-z0-9]+\.[A-Za-z]+$")
_REVERSE_RE = re.compile(r"^[A-Za-z]+\.[A-Za-z0-9]+$")


def _is_cjk(s: str) -> bool:
    return any("一" <= c <= "鿿" or "぀" <= c <= "ヿ" for c in s)


def _is_safe_alias_key(key: str) -> bool:
    if not key:
        return False
    # Latin-only keys: require ≥3 chars (1-2 letter latin tickers like
    # "A", "BP", "GE" should fall through to the bare-code path, not be
    # captured by some random alias).
    if not _is_cjk(key) and len(key) < 3:
        return False
    # CJK keys: 2 chars is a normal short company name (苹果, 茅台, 腾讯).
    if _is_cjk(key) and len(key) < 2:
        return False
    if key.isdigit():
        return False  # Pure digits → bare-code path handles it
    if _CANONICAL_RE.match(key) or _REVERSE_RE.match(key):
        return False  # Already canonical-shaped
    return True


# Common name-suffix patterns. Stripping them lets users type the brand alone:
#   "Tencent Holdings Ltd."          → "Tencent Holdings", "Tencent"
#   "Microsoft Corporation"          → "Microsoft"     (when present in source)
#   "苹果公司" / "微软公司"             → "苹果"   / "微软"
#   "Kweichow Moutai Co.,Ltd."       → "Kweichow Moutai"
# Single-word legal-suffix tail. Iterating this regex peels one trailing word
# at a time so that "Tencent Holdings Ltd." yields BOTH "Tencent Holdings" and
# "Tencent". Multi-word tails like "Co.,Ltd." come first so they win on greedy
# alternation when the comma-form is present.
_EN_SUFFIX_RE = re.compile(
    r"[,\s]+("
    r"Co\.?,?\s*Ltd\.?|Co\.?,?\s*Limited|"
    r"Holdings|Holding|Group|"
    r"Limited|Ltd\.?|Inc\.?|Corp\.?|Corporation|Company|"
    r"Plc|Pty\.?\s*Ltd\.?|S\.A\.|N\.V\.|AG|SE"
    r")\.?\s*$",
    re.IGNORECASE,
)
# CN suffix family — stripped iteratively. Common patterns:
#   "腾讯控股有限公司" → "腾讯控股" → "腾讯"
#   "阿里巴巴集团控股" → "阿里巴巴集团" → "阿里巴巴"
#   "阿里巴巴-W"      → "阿里巴巴"           (HK weighted voting class)
#   "比亚迪股份"      → "比亚迪"
_CN_SUFFIX_RE = re.compile(
    r"(股份有限公司|有限公司|集团股份|集团控股|股份|控股|公司|集团|"
    r"-W|-SW|-S|-WR)$"
)


def _en_stems(name: str) -> list[str]:
    """Return progressively shorter forms by peeling one legal-suffix word at
    a time. ``"Tencent Holdings Ltd."`` → ``["Tencent Holdings", "Tencent"]``."""
    out: list[str] = []
    cur = name.strip()
    for _ in range(5):
        new = _EN_SUFFIX_RE.sub("", cur).strip().rstrip(",")
        if new == cur or len(new) < 2:
            break
        cur = new
        out.append(cur)
    return out


def _cn_stems(name: str) -> list[str]:
    out: list[str] = []
    cur = name.strip()
    for _ in range(3):
        new = _CN_SUFFIX_RE.sub("", cur).strip()
        if new == cur or len(new) < 2:
            break
        cur = new
        out.append(cur)
    return out


def _put(table: dict[str, str], key: str, value: str, *, with_stems: bool = True) -> None:
    """Insert into the alias table, normalizing whitespace and skipping
    keys that would collide with ``_parse_bare`` fast paths.

    When ``with_stems`` is True, also insert the suffix-stripped variants
    (e.g. "苹果公司" → "苹果"; "Tencent Holdings Ltd." → "Tencent Holdings"
    → "Tencent") so that brand-short queries also resolve.
    """
    if not key or not value:
        return
    k = key.strip()
    v = value.strip().upper()
    if not _is_safe_alias_key(k):
        return
    # First-write-wins to keep insertion order semantics (A→HK→US).
    table.setdefault(k, v)
    if with_stems:
        for variant in _en_stems(k):
            if _is_safe_alias_key(variant):
                table.setdefault(variant, v)
        for variant in _cn_stems(k):
            if _is_safe_alias_key(variant):
                table.setdefault(variant, v)


def fetch_tushare_a(pro) -> Iterable[dict]:
    """A-share listed equities; ts_code already canonical (.SH/.SZ/.BJ)."""
    with unproxy():
        df = pro.stock_basic(
            exchange="", list_status="L",
            fields="ts_code,name,enname,exchange,market",
        )
    print(f"  Tushare stock_basic: {len(df)} rows", flush=True)
    for r in df.to_dict("records"):
        yield r


def fetch_tushare_hk(pro) -> Iterable[dict]:
    """HK listed equities; ts_code already 5-digit padded (e.g. 00700.HK)."""
    with unproxy():
        df = pro.hk_basic(
            list_status="L",
            fields="ts_code,name,enname,fullname",
        )
    print(f"  Tushare hk_basic: {len(df)} rows", flush=True)
    for r in df.to_dict("records"):
        yield r


def fetch_tushare_us(pro) -> Iterable[dict]:
    """US listed equities (paginated, free tier ≤ 6000/call)."""
    rows: list[dict] = []
    for offset in range(0, 60000, 6000):
        with unproxy():
            df = pro.us_basic(
                list_status="L", offset=offset, limit=6000,
                fields="ts_code,enname,classify",
            )
        if df.empty:
            break
        rows.extend(df.to_dict("records"))
        time.sleep(0.4)
    print(f"  Tushare us_basic: {len(rows)} rows (paginated)", flush=True)
    yield from rows


def load_prod_csv_us() -> dict[str, str]:
    """Load /home/ygwang/trading_agent/data/us_stock_list.csv → {code: cn_name}."""
    out: dict[str, str] = {}
    if not os.path.exists(PROD_US_CSV):
        print(f"  ⚠️  {PROD_US_CSV} not found — US CN names will be missing", flush=True)
        return out
    with open(PROD_US_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            code = (row.get("code") or "").strip().upper()
            name = (row.get("name") or "").strip()
            if code and name:
                out[code] = name
    print(f"  Prod CSV us_stock_list: {len(out)} rows", flush=True)
    return out


def main() -> int:
    token = os.environ.get(TUSHARE_TOKEN_ENV, "").strip()
    if not token or len(token) < 40:
        print(f"❌ Set {TUSHARE_TOKEN_ENV}=<64-hex tushare token> first", flush=True)
        return 1

    import tushare as ts
    ts.set_token(token)
    pro = ts.pro_api()

    table: dict[str, str] = {}
    counts = {"A股": 0, "港股": 0, "美股": 0}
    skipped = {"A股": 0, "港股": 0, "美股": 0}

    print("→ Fetching A股 (Tushare stock_basic)…", flush=True)
    for r in fetch_tushare_a(pro):
        canonical = (r.get("ts_code") or "").strip().upper()
        if not _CANONICAL_RE.match(canonical):
            skipped["A股"] += 1
            continue
        before = len(table)
        _put(table, r.get("name"), canonical)
        _put(table, r.get("enname"), canonical)
        counts["A股"] += len(table) - before

    print("→ Fetching 港股 (Tushare hk_basic)…", flush=True)
    for r in fetch_tushare_hk(pro):
        canonical = (r.get("ts_code") or "").strip().upper()
        if not _CANONICAL_RE.match(canonical):
            skipped["港股"] += 1
            continue
        before = len(table)
        _put(table, r.get("name"), canonical)
        _put(table, r.get("enname"), canonical)
        # Tushare 港股 fullname like "长江和记实业有限公司" — also useful
        _put(table, r.get("fullname"), canonical)
        counts["港股"] += len(table) - before

    print("→ Fetching 美股 (Tushare us_basic, paginated)…", flush=True)
    ts_us: dict[str, str] = {}  # bare ticker → enname
    for r in fetch_tushare_us(pro):
        bare = (r.get("ts_code") or "").strip().upper()
        # Skip rows with no ts_code (Tushare DUMMY rows) or non-EQ classify
        # (PINK/ETF noise).
        if not bare or "." in bare or not bare.replace("-", "").replace(".", "").isalnum():
            continue
        if r.get("classify") and r["classify"] != "EQ":
            continue
        en = (r.get("enname") or "").strip()
        if en:
            ts_us[bare] = en

    csv_us = load_prod_csv_us()
    intersection = sorted(set(ts_us) & set(csv_us))
    print(f"→ 美股 intersection (Tushare ∩ prod CSV, classify=EQ): {len(intersection)} rows", flush=True)

    for bare in intersection:
        canonical = f"{bare}.US"
        before = len(table)
        _put(table, csv_us[bare], canonical)
        _put(table, ts_us[bare], canonical)
        counts["美股"] += len(table) - before

    # Header / metadata
    meta = {
        "_meta": {
            "build_ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "total_aliases": len(table),
            "by_market": counts,
            "skipped": skipped,
            "sources": {
                "A股":  "Tushare stock_basic (list_status=L)",
                "港股": "Tushare hk_basic (list_status=L)",
                "美股": "Tushare us_basic ∩ /home/ygwang/trading_agent/data/us_stock_list.csv (classify=EQ)",
            },
        },
    }

    # Sort the rest for stable diffs
    sorted_table = dict(sorted(table.items()))
    payload = {**meta, **sorted_table}

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"✓ Wrote {OUTPUT_PATH}: {len(table):,} aliases", flush=True)
    for m, n in counts.items():
        print(f"    {m}: {n:,} aliases", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
