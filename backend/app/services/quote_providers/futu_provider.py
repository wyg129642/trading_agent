"""Futu OpenAPI quote provider — HK / A-share / US via a local FutuOpenD gateway.

FutuOpenD runs as a daemon on 127.0.0.1:11111 and proxies requests to Futu's
backend. One `get_market_snapshot` call returns price + prev_close + market_cap
+ PE (TTM) + currency for up to 400 symbols, so the whole portfolio fits in a
single call.

Symbol format:
  HK          → HK.00700       (5-digit zero-padded)
  US          → US.AAPL
  主板 (沪市)  → SH.600519     (600xxx / 688xxx)
  主板 (深市)  → SZ.000001     (000xxx)
  创业板       → SZ.300750
  科创板       → SH.688981

Permissions (牛牛号 / domestic account):
  港股 LV2 / 沪深 LV1 / 美股 LV3  — all free.
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any

logger = logging.getLogger(__name__)

# Lazy singleton — one TCP connection to OpenD reused across requests.
_CTX_LOCK = threading.Lock()
_CTX: Any = None

# Circuit breaker — after a failure we skip Futu for this many seconds instead
# of retrying every call. Prevents a stuck OpenD (e.g. waiting for a fresh SMS
# verify code on a new IP) from serializing the whole /portfolio/quotes path.
_CIRCUIT_OPEN_SECONDS = 120
_circuit_open_until: float = 0.0


def _get_ctx(host: str, port: int):
    """Return the shared OpenQuoteContext, creating it on first use."""
    global _CTX
    with _CTX_LOCK:
        if _CTX is None:
            from futu import OpenQuoteContext   # type: ignore[import-not-found]
            _CTX = OpenQuoteContext(host=host, port=port)
            logger.info("FutuOpenD context connected at %s:%s", host, port)
        return _CTX


def close_ctx() -> None:
    global _CTX
    with _CTX_LOCK:
        if _CTX is not None:
            try:
                _CTX.close()
            except Exception:
                pass
            _CTX = None


def _trip_circuit(reason: str) -> None:
    """Mark Futu unhealthy so subsequent callers short-circuit for a while."""
    global _circuit_open_until, _CTX
    _circuit_open_until = time.time() + _CIRCUIT_OPEN_SECONDS
    logger.warning(
        "Futu circuit tripped for %ss (%s) — reset connection",
        _CIRCUIT_OPEN_SECONDS, reason,
    )
    # Drop the (likely wedged) singleton — next call will reconnect.
    with _CTX_LOCK:
        if _CTX is not None:
            try:
                _CTX.close()
            except Exception:
                pass
            _CTX = None


def _circuit_open() -> bool:
    return time.time() < _circuit_open_until


def to_futu_symbol(ticker: str, market_label: str) -> str | None:
    """Portfolio (ticker, market_label) → Futu symbol, or None if unsupported."""
    t = ticker.strip()
    if market_label == "美股":
        return f"US.{t}"
    if market_label == "港股":
        return f"HK.{t.lstrip('0').zfill(5)}"
    if market_label == "创业板":          # 300xxx — 深市
        return f"SZ.{t}"
    if market_label == "科创板":          # 688xxx — 沪市
        return f"SH.{t}"
    if market_label == "主板":
        return f"SH.{t}" if t.startswith("6") else f"SZ.{t}"
    return None


def _currency_for(market_label: str) -> str:
    if market_label == "美股":
        return "USD"
    if market_label == "港股":
        return "HKD"
    return "CNY"


def fetch_quotes_sync(
    pairs: list[tuple[str, str]],
    host: str = "127.0.0.1",
    port: int = 11111,
) -> dict[str, dict[str, Any]]:
    """Batch-fetch snapshots for all (ticker, market_label) pairs in one call.

    Returns dict keyed by original ticker. Entries missing from the response
    (invalid code, no quote permission, etc.) are simply absent.
    """
    if not pairs:
        return {}

    sym_to_ticker: dict[str, tuple[str, str]] = {}
    for ticker, market in pairs:
        sym = to_futu_symbol(ticker, market)
        if sym is None:
            continue
        sym_to_ticker[sym] = (ticker, market)

    if not sym_to_ticker:
        return {}

    if _circuit_open():
        return {}

    try:
        ctx = _get_ctx(host, port)
        from futu import RET_OK   # type: ignore[import-not-found]
        # Cheap health probe first — if OpenD is stuck (needs SMS verify, disconnected,
        # not Ready), this fails fast and we trip the circuit instead of letting
        # get_market_snapshot hang the executor thread.
        ret_s, state = ctx.get_global_state()
        if ret_s != RET_OK:
            logger.warning("Futu get_global_state failed: %s", state)
            _trip_circuit(f"state_err: {str(state)[:80]}")
            return {}
        if (state.get("program_status_type") != "READY"
                or not state.get("qot_logined")):
            logger.warning("Futu not ready: %s", state.get("program_status_type"))
            _trip_circuit(f"not_ready: {state.get('program_status_type')}")
            return {}

        ret, data = ctx.get_market_snapshot(list(sym_to_ticker.keys()))
        if ret != RET_OK:
            msg = str(data)
            logger.warning("Futu get_market_snapshot failed: %s", msg)
            _trip_circuit(msg[:100])
            return {}
    except Exception as e:
        logger.exception("Futu snapshot error: %s", e)
        _trip_circuit(f"{type(e).__name__}: {e}")
        return {}

    out: dict[str, dict[str, Any]] = {}
    # `data` is a pandas DataFrame.
    for _, row in data.iterrows():
        code = row.get("code")
        mapping = sym_to_ticker.get(code)
        if not mapping:
            continue
        ticker, market = mapping
        last = _num(row.get("last_price"))
        prev = _num(row.get("prev_close_price"))
        change_pct = ((last - prev) / prev * 100) if (last and prev) else None
        out[ticker] = {
            "futu_symbol": code,
            "name": row.get("name") or "",
            "latest_price": last,
            "prev_close": prev,
            "change_pct": change_pct,
            "market_cap": _num(row.get("total_market_val")),
            "pe_ttm": _num(row.get("pe_ttm_ratio")) or _num(row.get("pe_ratio")),
            "currency": _currency_for(market),
        }
    return out


def _num(v: Any) -> float | None:
    """Coerce Futu value to float; treat 0/NaN/None as missing."""
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if f != f:          # NaN
        return None
    return f if f != 0 else None
