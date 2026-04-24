"""Chat tool wrapper around consensus_forecast.fetch_consensus().

Exposes Wind A-share consensus as a first-class LLM tool so step executors
and the research chat can reality-check their modeled EPS/revenue against
the market-wide analyst consensus.

The tool gracefully no-ops for non-A-share tickers (returns a clear
message rather than an error) so the LLM learns not to call it for US/HK.
"""
from __future__ import annotations

import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


async def execute_tool(
    name: str,
    arguments: dict[str, Any],
    citation_tracker: Any,
) -> str:
    """Execute the ``consensus_forecast_query`` tool.

    Returns a human-readable Chinese summary keyed to a citation index so
    the LLM can cite [N] in its response.
    """
    if name != "consensus_forecast_query":
        return f"Unknown consensus tool: {name}"

    ticker = str(arguments.get("ticker") or "").strip()
    market = str(arguments.get("market") or "主板").strip()
    if not ticker:
        return "consensus_forecast_query 需要 ticker 参数."

    # Normalize — accept "600519" or "600519.SH"
    plain = ticker.split(".")[0]

    try:
        from backend.app.services.consensus_forecast import (
            fetch_consensus,
            to_windcode,
        )
    except Exception as e:
        return f"consensus_forecast 模块不可用: {e}"

    wc = to_windcode(plain, market)
    if not wc:
        return (
            f"⚠️ {ticker}(market={market}) 不是 A 股或代码非法；一致预期仅对"
            " 主板/创业板/科创板/北交所 有数据。如需美股/港股分析师预期请使用 web_search。"
        )

    try:
        data = await fetch_consensus(plain, market)
    except Exception as e:
        logger.exception("consensus_forecast.fetch_consensus failed")
        return f"查询 Wind 一致预期失败: {e}"

    if not data or not getattr(data, "windcode", None):
        return f"Wind 没有 {ticker} ({wc}) 的一致预期数据 (可能分析师覆盖不足或基准日超出查询窗口)."

    # Register a citation
    source_entry = {
        "title": f"{ticker} Wind 一致预期",
        "url": "",
        "website": "Wind 金融终端 (ASHARECONSENSUS*)",
        "date": (data.as_of or "")[:10] if hasattr(data, "as_of") else "",
        "source_type": "consensus",
        "doc_type": "一致预期",
    }
    idx = None
    try:
        # CitationTracker public-ish API
        idx = citation_tracker._register(
            f"consensus:{wc}", source_entry,
        )
    except Exception:
        pass

    fy = lambda x: {
        "year": getattr(x, "year", None),
        "net_profit_rmb": getattr(x, "net_profit", None),
        "eps": getattr(x, "eps", None),
        "pe": getattr(x, "pe", None),
        "pb": getattr(x, "pb", None),
        "roe_pct": getattr(x, "roe", None),
        "revenue_rmb": getattr(x, "revenue", None),
    }
    summary = {
        "ticker": ticker,
        "windcode": data.windcode,
        "as_of": data.as_of,
        "analyst_count": data.analyst_count,
        "target_price": data.target_price,
        "rating_label": data.rating_label,
        "rating_avg": data.rating_avg,
        "yoy_net_profit_pct": data.yoy_net_profit,
        "fy1": fy(data.fy1),
        "fy2": fy(data.fy2),
        "fy3": fy(data.fy3),
        "citation_index": idx,
    }

    # Format a clean Chinese summary plus the JSON blob
    lines = [
        f"[{idx}] Wind 一致预期 — {ticker}（{data.windcode}，as_of={data.as_of or 'n/a'}）:",
        f"  · 覆盖分析师 {data.analyst_count} 人；综合评级 {data.rating_label or 'n/a'} "
        f"(avg={data.rating_avg})；目标价 {data.target_price}",
        f"  · FY1({data.fy1.year}): net={data.fy1.net_profit}, EPS={data.fy1.eps}, PE={data.fy1.pe}, rev={data.fy1.revenue}",
        f"  · FY2({data.fy2.year}): net={data.fy2.net_profit}, EPS={data.fy2.eps}, PE={data.fy2.pe}, rev={data.fy2.revenue}",
        f"  · FY3({data.fy3.year}): net={data.fy3.net_profit}, EPS={data.fy3.eps}, PE={data.fy3.pe}, rev={data.fy3.revenue}",
        f"  · 预期 YoY 净利润增速: {data.yoy_net_profit}%",
        "",
        "Raw:",
        json.dumps(summary, ensure_ascii=False, default=str),
    ]
    return "\n".join(lines)
