"""Resolve config/portfolio_sources.yaml to a set of canonical Mongo tickers.

The yaml uses pairs like ``stock_ticker: GLW`` + ``stock_market: 美股`` whose
``_canonical_tickers`` form (the field stamped on every Mongo doc by
``crawl/ticker_tag.py`` / ``backend/app/services/ticker_normalizer.py``) is
``GLW.US``. We mirror that mapping here so the runner can do an O(1) Mongo
``$in`` filter on ``_canonical_tickers``.

Refreshed every poll (``load_holdings()`` re-reads the file) so portfolio
edits surface within one runner cycle without restarting the process.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable

import yaml

logger = logging.getLogger(__name__)


PORTFOLIO_YAML = Path(__file__).resolve().parent.parent.parent / "config" / "portfolio_sources.yaml"


# stock_market label → canonical suffix. Matches what `ticker_normalizer`
# writes into `_canonical_tickers`.
_MARKET_SUFFIX = {
    "美股": "US",
    "港股": "HK",
    "主板": "SH",         # 600xxx
    "创业板": "SZ",        # 300xxx
    "科创板": "SH",        # 688xxx → SH (Shanghai STAR Market)
    "韩股": "KS",
    "日股": "T",
    "澳股": "AU",
}


def _canonicalize(stock_ticker: str, stock_market: str) -> str | None:
    """Return ``CODE.SUFFIX`` form, or None if we can't resolve the market.

    Special-cases:
      - HK tickers: zero-pad to 5 digits (00700.HK), matching `_canonical_tickers`
      - A-share: route by code prefix when stock_market is just "A股"-flavored
        (yaml uses 主板/创业板/科创板; 600/000/300/688 hint stays consistent).
    """
    code = (stock_ticker or "").strip().upper()
    market = (stock_market or "").strip()
    if not code:
        return None

    suffix = _MARKET_SUFFIX.get(market)
    if not suffix:
        return None

    if suffix == "HK":
        # zero-pad to 5 digits for HK (Mongo `_canonical_tickers` form is "00700.HK")
        digits = "".join(c for c in code if c.isdigit())
        if digits:
            code = digits.zfill(5)

    if suffix == "SH" and code.isdigit():
        # 科创板 (688xxx) and 主板 (600xxx) both map to .SH; keep numeric form
        code = code.zfill(6)
    elif suffix == "SZ" and code.isdigit():
        code = code.zfill(6)

    return f"{code}.{suffix}"


def load_holdings(yaml_path: Path = PORTFOLIO_YAML) -> set[str]:
    """Return the set of canonical tickers from portfolio_sources.yaml.

    Returns an empty set on parse error — caller should treat that as "do
    nothing this cycle" and try again.
    """
    if not yaml_path.exists():
        logger.warning("[holdings] yaml not found: %s", yaml_path)
        return set()
    try:
        with open(yaml_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except Exception as e:
        logger.warning("[holdings] yaml parse failed: %s", e)
        return set()

    tickers: set[str] = set()
    for s in data.get("sources", []):
        if not isinstance(s, dict):
            continue
        canon = _canonicalize(s.get("stock_ticker", ""), s.get("stock_market", ""))
        if canon:
            tickers.add(canon)
    return tickers


def describe_holdings(tickers: Iterable[str]) -> str:
    """One-line summary for log messages."""
    ts = sorted(tickers)
    n = len(ts)
    if n == 0:
        return "(empty)"
    head = ", ".join(ts[:5])
    if n > 5:
        head += f", ... ({n} total)"
    return head
