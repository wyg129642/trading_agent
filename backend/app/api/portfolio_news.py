"""Portfolio Breaking News API — serves proactive scanner alerts from ClickHouse."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, Query

from backend.app.config import get_settings, Settings
from backend.app.deps import get_current_boss_or_admin

logger = logging.getLogger(__name__)

router = APIRouter()


def _get_ch_client(settings: Settings):
    """Create a ClickHouse client from settings (cached per process via module-level)."""
    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        database=settings.clickhouse_db,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
    )


# Module-level client (lazy singleton)
_ch_client = None


def _client(settings: Settings):
    global _ch_client
    if _ch_client is None and settings.clickhouse_enabled:
        try:
            _ch_client = _get_ch_client(settings)
        except Exception as e:
            logger.error("ClickHouse connection failed: %s", e)
            return None
    return _ch_client


def _parse_json_safe(s: str) -> Any:
    """Parse a JSON string, returning [] on failure."""
    if not s:
        return []
    try:
        return json.loads(s)
    except (json.JSONDecodeError, TypeError):
        return []


@router.get("/breaking-news")
async def list_breaking_news(
    user=Depends(get_current_boss_or_admin),
    settings: Settings = Depends(get_settings),
    ticker: str | None = Query(None, description="Filter by ticker"),
    market: str | None = Query(None, description="Filter by market (us, china, hk, kr, jp)"),
    materiality: str | None = Query(None, description="Filter by materiality (material, critical)"),
    hours: int = Query(168, ge=1, le=8760, description="Lookback window in hours"),
    limit: int = Query(50, ge=1, le=200, description="Max results"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
):
    """List breaking news from the proactive portfolio scanner.

    Returns recent material/critical breaking news events with full analysis data.
    """
    client = _client(settings)
    if not client:
        return {"items": [], "total": 0, "error": "ClickHouse not available"}

    db = settings.clickhouse_db

    # Build WHERE clauses
    conditions = [f"scan_time >= now() - INTERVAL {hours} HOUR"]
    params: dict[str, Any] = {}

    if ticker:
        conditions.append("ticker = {ticker:String}")
        params["ticker"] = ticker
    if market:
        conditions.append("market = {market:String}")
        params["market"] = market
    if materiality:
        conditions.append("news_materiality = {materiality:String}")
        params["materiality"] = materiality

    where = " AND ".join(conditions)

    # Count query
    count_sql = f"SELECT count() FROM {db}.portfolio_breaking_news FINAL WHERE {where}"
    try:
        total = client.query(count_sql, parameters=params).result_rows[0][0]
    except Exception as e:
        logger.error("ClickHouse count query failed: %s", e)
        return {"items": [], "total": 0, "error": str(e)}

    # Data query
    data_sql = f"""
    SELECT
        id, ticker, name_cn, name_en, market, market_label, scan_time,
        news_materiality, news_summary, new_developments,
        novelty_status, earliest_report_time, deep_research_performed,
        research_iterations, key_findings, news_timeline, referenced_sources,
        historical_precedents,
        alert_confidence, alert_rationale, sentiment, impact_magnitude,
        impact_timeframe, surprise_factor, bull_case, bear_case,
        recommended_action, tokens_used, cost_cny, created_at
    FROM {db}.portfolio_breaking_news FINAL
    WHERE {where}
    ORDER BY scan_time DESC
    LIMIT {limit} OFFSET {offset}
    """

    try:
        result = client.query(data_sql, parameters=params)
    except Exception as e:
        logger.error("ClickHouse data query failed: %s", e)
        return {"items": [], "total": 0, "error": str(e)}

    items = []
    for row in result.result_rows:
        scan_time = row[6]
        earliest = row[11]

        items.append({
            "id": row[0],
            "ticker": row[1],
            "name_cn": row[2],
            "name_en": row[3],
            "market": row[4],
            "market_label": row[5],
            "scan_time": scan_time.isoformat() if isinstance(scan_time, datetime) else str(scan_time),
            "news_materiality": row[7],
            "news_summary": row[8],
            "new_developments": row[9] if isinstance(row[9], list) else [],
            "novelty_status": row[10],
            "earliest_report_time": earliest.isoformat() if isinstance(earliest, datetime) else None,
            "deep_research_performed": bool(row[12]),
            "research_iterations": row[13],
            "key_findings": row[14] if isinstance(row[14], list) else [],
            "news_timeline": _parse_json_safe(row[15]),
            "referenced_sources": _parse_json_safe(row[16]),
            "historical_precedents": _parse_json_safe(row[17]),
            "alert_confidence": round(float(row[18]), 3),
            "alert_rationale": row[19],
            "sentiment": row[20],
            "impact_magnitude": row[21],
            "impact_timeframe": row[22],
            "surprise_factor": round(float(row[23]), 3),
            "bull_case": row[24],
            "bear_case": row[25],
            "recommended_action": row[26],
            "tokens_used": row[27],
            "cost_cny": round(float(row[28]), 4),
            "created_at": row[29].isoformat() if isinstance(row[29], datetime) else str(row[29]),
        })

    return {"items": items, "total": total}


@router.get("/breaking-news/summary")
async def breaking_news_summary(
    user=Depends(get_current_boss_or_admin),
    settings: Settings = Depends(get_settings),
    hours: int = Query(168, ge=1, le=8760, description="Lookback window in hours"),
):
    """Get per-ticker summary of breaking news counts and latest materiality.

    Useful for the portfolio overview to show alert badges on each stock card.
    """
    client = _client(settings)
    if not client:
        return {"summary": {}}

    db = settings.clickhouse_db

    sql = f"""
    SELECT
        ticker,
        count() AS news_count,
        max(scan_time) AS latest_scan,
        argMax(news_materiality, scan_time) AS latest_materiality,
        argMax(sentiment, scan_time) AS latest_sentiment,
        argMax(news_summary, scan_time) AS latest_summary
    FROM {db}.portfolio_breaking_news FINAL
    WHERE scan_time >= now() - INTERVAL {hours} HOUR
    GROUP BY ticker
    ORDER BY latest_scan DESC
    """

    try:
        result = client.query(sql)
    except Exception as e:
        logger.error("ClickHouse summary query failed: %s", e)
        return {"summary": {}}

    summary: dict[str, dict] = {}
    for row in result.result_rows:
        latest_scan = row[2]
        summary[row[0]] = {
            "news_count": row[1],
            "latest_scan": latest_scan.isoformat() if isinstance(latest_scan, datetime) else str(latest_scan),
            "latest_materiality": row[3],
            "latest_sentiment": row[4],
            "latest_summary": row[5][:200] if row[5] else "",
        }

    return {"summary": summary}
