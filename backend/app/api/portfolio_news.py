"""Portfolio Breaking News API — serves proactive scanner alerts.

Primary data source: PostgreSQL (portfolio_scan_results + portfolio_scan_baselines).
The proactive scanner always writes scan results to PostgreSQL, making it the
reliable source of truth. ClickHouse is used for long-term analytics only.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.deps import get_current_boss_or_admin, get_portfolio_scan_db

logger = logging.getLogger(__name__)

router = APIRouter()

MARKET_LABELS = {
    "us": "美股",
    "china": "A股",
    "hk": "港股",
    "kr": "韩股",
    "jp": "日股",
}

# Neutral and empty sentiments are excluded from the dashboard.
_EXCLUDED_SENTIMENTS = ("neutral", "")


def _safe_json(val: Any, default=None):
    """Handle JSONB values from asyncpg (returned as Python dicts/lists)."""
    if val is None:
        return default if default is not None else []
    if isinstance(val, (list, dict)):
        return val
    if isinstance(val, str):
        import json
        try:
            return json.loads(val)
        except (json.JSONDecodeError, TypeError):
            return default if default is not None else []
    return default if default is not None else []


def _parse_iso(value: Any) -> datetime | None:
    """Parse a timestamp string or datetime into a UTC-aware datetime."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not isinstance(value, str):
        return None
    try:
        from dateutil import parser as dateutil_parser
        dt = dateutil_parser.parse(value, fuzzy=True)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except (ValueError, OverflowError, TypeError):
        return None


def _compute_event_age_hours(analysis: dict, snapshot: dict, timeline: list) -> float | None:
    """Derive event age in hours from the earliest known source timestamp.

    Prefers the freshness_gate's pre-computed value (full_analysis.event_age_hours);
    falls back to earliest_report_time / news_timeline for legacy rows written
    before the gate shipped.
    """
    cached = analysis.get("event_age_hours")
    if isinstance(cached, (int, float)):
        return max(0.0, float(cached))

    earliest_iso = (
        snapshot.get("earliest_report_time")
        or (timeline[0].get("time") if timeline and isinstance(timeline[0], dict) else None)
    )
    dt = _parse_iso(earliest_iso)
    if not dt:
        return None
    age = (datetime.now(timezone.utc) - dt).total_seconds() / 3600
    return max(0.0, age)


# An event is considered stale once it exceeds this many hours with no new
# corroborating source. Keep in sync with freshness_gate.DEFAULT_NOVELTY_HOURS.
STALE_EVENT_AGE_HOURS = 48


def _row_to_item(row: Any) -> dict:
    """Convert a database row to a breaking news item dict."""
    analysis = _safe_json(row.full_analysis, {})
    snapshot = _safe_json(row.snapshot_summary, {})
    market = row.market or ""

    # Extract earliest_report_time from snapshot_summary or news_timeline
    timeline = _safe_json(row.news_timeline, [])
    earliest_report_time = snapshot.get("earliest_report_time")
    if not earliest_report_time and timeline:
        first_time = timeline[0].get("time") if timeline else None
        if first_time:
            earliest_report_time = first_time

    scan_time = row.scan_time
    created_at = row.created_at

    event_age_hours = _compute_event_age_hours(analysis, snapshot, timeline)
    rejection_reason = analysis.get("rejection_reason")
    is_stale_event = (
        rejection_reason == "event_too_old"
        or (event_age_hours is not None and event_age_hours >= STALE_EVENT_AGE_HOURS)
    )

    # Sources from full_analysis (with clickable URLs)
    sources = analysis.get("sources", [])
    # Also merge referenced_sources if sources is empty
    if not sources:
        sources = _safe_json(row.referenced_sources, [])

    return {
        "id": row.id,
        "ticker": row.ticker,
        "name_cn": row.name_cn or row.ticker,
        "name_en": getattr(row, "name_en", "") or "",
        "market": market,
        "market_label": MARKET_LABELS.get(market, market),
        "scan_time": scan_time.isoformat() if isinstance(scan_time, datetime) else str(scan_time or ""),
        "news_materiality": row.delta_magnitude or "none",
        "news_summary": analysis.get("summary", "") or row.delta_description or "",
        "new_developments": _safe_json(row.new_developments, []),
        "novelty_status": snapshot.get("novelty_status", ""),
        "earliest_report_time": earliest_report_time,
        "event_age_hours": round(event_age_hours, 2) if event_age_hours is not None else None,
        "is_stale_event": is_stale_event,
        "rejection_reason": rejection_reason,
        "deep_research_performed": bool(row.deep_research_performed),
        "research_iterations": row.research_iterations or 0,
        "key_findings": analysis.get("key_findings", []) or _safe_json(row.key_findings, []),
        "news_timeline": timeline,
        "referenced_sources": _safe_json(row.referenced_sources, []),
        "sources": sources,
        "historical_precedents": snapshot.get("historical_precedents", []),
        "historical_evidence_summary": analysis.get("historical_evidence_summary", ""),
        "alert_confidence": round(float(row.alert_confidence or 0), 3),
        "alert_rationale": row.alert_rationale or "",
        "sentiment": analysis.get("sentiment", "neutral"),
        "impact_magnitude": analysis.get("impact_magnitude", "low"),
        "impact_timeframe": analysis.get("impact_timeframe", "short_term"),
        "surprise_factor": round(float(analysis.get("surprise_factor", 0.5)), 3),
        "bull_case": analysis.get("bull_case", ""),
        "bear_case": analysis.get("bear_case", ""),
        "recommended_action": analysis.get("recommended_action", ""),
        "should_alert": bool(row.should_alert),
        "tokens_used": row.tokens_used or 0,
        "cost_cny": round(float(row.cost_cny or 0), 4),
        "created_at": created_at.isoformat() if isinstance(created_at, datetime) else str(created_at or ""),
    }


# Sentiment filter: exclude neutral and NULL sentiment in SQL.
# full_analysis is JSONB; sentiment lives at full_analysis->>'sentiment'.
_SENTIMENT_FILTER = """
    AND r.full_analysis IS NOT NULL
    AND r.full_analysis->>'sentiment' IS NOT NULL
    AND r.full_analysis->>'sentiment' NOT IN ('neutral', '')
"""


@router.get("/breaking-news")
async def list_breaking_news(
    user=Depends(get_current_boss_or_admin),
    db: AsyncSession = Depends(get_portfolio_scan_db),
    ticker: str | None = Query(None, description="Filter by ticker"),
    market: str | None = Query(None, description="Filter by market (us, china, hk, kr, jp)"),
    materiality: str | None = Query(None, description="Filter by materiality (material, critical)"),
    hours: int = Query(168, ge=1, le=8760, description="Lookback window in hours"),
    limit: int = Query(50, ge=1, le=200, description="Max results"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    hide_stale: bool = Query(
        True,
        description=(
            "Suppress alerts that the freshness gate rejected as stale "
            "(rejection_reason=event_too_old). Defaults to True; pass "
            "false to see the historical raw scan output."
        ),
    ),
):
    """List breaking news from the proactive portfolio scanner.

    Returns material/critical breaking news events with non-neutral sentiment.
    Reads from PostgreSQL portfolio_scan_results (always available).
    """
    # Build WHERE clauses
    conditions = [
        "r.delta_detected = true",
        "r.delta_magnitude IN ('material', 'critical')",
        "r.scan_time >= NOW() - make_interval(hours => :hours)",
    ]
    params: dict[str, Any] = {"hours": hours, "limit": limit, "offset": offset}

    if ticker:
        conditions.append("r.ticker = :ticker")
        params["ticker"] = ticker
    if market:
        conditions.append("b.market = :market")
        params["market"] = market
    if materiality:
        conditions.append("r.delta_magnitude = :materiality")
        params["materiality"] = materiality

    if hide_stale:
        # The freshness gate (engine/proactive/freshness_gate.py) writes
        # rejection_reason="event_too_old" into full_analysis when it
        # suppresses an alert. Hide those rows from the default feed so
        # a week-old earnings story never shows up as "breaking news".
        conditions.append(
            "(r.full_analysis IS NULL "
            "OR r.full_analysis->>'rejection_reason' IS NULL "
            "OR r.full_analysis->>'rejection_reason' != 'event_too_old')"
        )

    where = " AND ".join(conditions) + _SENTIMENT_FILTER

    # Count query
    count_sql = text(f"""
        SELECT count(*)
        FROM portfolio_scan_results r
        JOIN portfolio_scan_baselines b ON r.ticker = b.ticker
        WHERE {where}
    """)

    try:
        count_result = await db.execute(count_sql, params)
        total = count_result.scalar() or 0
    except Exception as e:
        logger.error("Breaking news count query failed: %s", e)
        return {"items": [], "total": 0, "error": str(e)}

    if total == 0:
        return {"items": [], "total": 0}

    # Data query
    data_sql = text(f"""
        SELECT
            r.id, r.ticker, b.name_cn, b.name_en, b.market,
            r.scan_time, r.delta_magnitude, r.delta_description,
            r.new_developments, r.deep_research_performed,
            r.research_iterations, r.key_findings, r.news_timeline,
            r.referenced_sources, r.should_alert, r.alert_confidence,
            r.alert_rationale, r.full_analysis, r.snapshot_summary,
            r.tokens_used, r.cost_cny, r.created_at
        FROM portfolio_scan_results r
        JOIN portfolio_scan_baselines b ON r.ticker = b.ticker
        WHERE {where}
        ORDER BY r.scan_time DESC
        LIMIT :limit OFFSET :offset
    """)

    try:
        result = await db.execute(data_sql, params)
        rows = result.all()
    except Exception as e:
        logger.error("Breaking news data query failed: %s", e)
        return {"items": [], "total": 0, "error": str(e)}

    items = [_row_to_item(row) for row in rows]
    return {"items": items, "total": total}


@router.get("/breaking-news/summary")
async def breaking_news_summary(
    user=Depends(get_current_boss_or_admin),
    db: AsyncSession = Depends(get_portfolio_scan_db),
    hours: int = Query(168, ge=1, le=8760, description="Lookback window in hours"),
):
    """Per-ticker summary of breaking news counts and latest materiality.

    Used by the portfolio overview to show alert badges on each stock card.
    """
    sql = text(f"""
        WITH ranked AS (
            SELECT
                r.ticker,
                b.name_cn,
                r.scan_time,
                r.delta_magnitude,
                r.full_analysis,
                r.delta_description,
                ROW_NUMBER() OVER (PARTITION BY r.ticker ORDER BY r.scan_time DESC) AS rn,
                COUNT(*) OVER (PARTITION BY r.ticker) AS news_count
            FROM portfolio_scan_results r
            JOIN portfolio_scan_baselines b ON r.ticker = b.ticker
            WHERE r.delta_detected = true
              AND r.delta_magnitude IN ('material', 'critical')
              AND r.scan_time >= NOW() - make_interval(hours => :hours)
              AND (
                r.full_analysis IS NULL
                OR r.full_analysis->>'rejection_reason' IS NULL
                OR r.full_analysis->>'rejection_reason' != 'event_too_old'
              )
              {_SENTIMENT_FILTER}
        )
        SELECT ticker, name_cn, news_count, scan_time,
               delta_magnitude, full_analysis, delta_description
        FROM ranked
        WHERE rn = 1
        ORDER BY scan_time DESC
    """)

    try:
        result = await db.execute(sql, {"hours": hours})
        rows = result.all()
    except Exception as e:
        logger.error("Breaking news summary query failed: %s", e)
        return {"summary": {}}

    summary: dict[str, dict] = {}
    for row in rows:
        analysis = _safe_json(row.full_analysis, {})
        scan_time = row.scan_time
        summary[row.ticker] = {
            "news_count": row.news_count,
            "latest_scan": scan_time.isoformat() if isinstance(scan_time, datetime) else str(scan_time or ""),
            "latest_materiality": row.delta_magnitude or "none",
            "latest_sentiment": analysis.get("sentiment", "neutral"),
            "latest_summary": (row.delta_description or "")[:200],
        }

    return {"summary": summary}


@router.get("/scanner-status")
async def scanner_status(
    user=Depends(get_current_boss_or_admin),
    db: AsyncSession = Depends(get_portfolio_scan_db),
):
    """Scanner health: last scan times, active stock count, recent alert count.

    The "offline" threshold has to accommodate the *longest* configured
    scan interval. Engine settings.yaml uses scan_interval_weekend_min=240
    (4h on weekends) and scan_interval_closed_min=120 (2h weekday-closed)
    — so any threshold under ~5h fires false "可能离线" alarms during
    normal operation. The 5h default = 240min weekend + 60min cycle
    execution + grace. Override via SCANNER_OFFLINE_THRESHOLD_MIN env var.
    """
    threshold_min = int(os.environ.get("SCANNER_OFFLINE_THRESHOLD_MIN", "300"))
    interval_clause = f"INTERVAL '{threshold_min} minutes'"
    sql = text(f"""
        SELECT
            COUNT(*) AS total_stocks,
            COUNT(*) FILTER (WHERE last_scan_at >= NOW() - {interval_clause}) AS active_stocks,
            MAX(last_scan_at) AS last_scan_at,
            SUM(scan_count) AS total_scans,
            SUM(alert_count) AS total_alerts,
            COUNT(*) FILTER (WHERE last_alert_at >= NOW() - INTERVAL '24 hours') AS stocks_alerted_24h
        FROM portfolio_scan_baselines
    """)

    try:
        result = await db.execute(sql)
        row = result.one_or_none()
    except Exception as e:
        logger.error("Scanner status query failed: %s", e)
        return {"status": "error", "error": str(e)}

    if not row or row.total_stocks == 0:
        return {
            "status": "inactive",
            "total_stocks": 0,
            "active_stocks": 0,
            "last_scan_at": None,
            "total_scans": 0,
            "total_alerts": 0,
            "stocks_alerted_24h": 0,
            "threshold_min": threshold_min,
        }

    last_scan = row.last_scan_at
    # Liveness check: did MAX(last_scan_at) advance within the threshold?
    # Falling back from active_stocks > 0 to MAX comparison so a single
    # successful scan in the cycle keeps us "active" even when most
    # stocks lag (typical on weekends with the 4h cycle).
    is_active = bool(
        last_scan and last_scan >= datetime.now(last_scan.tzinfo) - timedelta(minutes=threshold_min)
    )

    return {
        "status": "active" if is_active else "stale",
        "total_stocks": row.total_stocks,
        "active_stocks": row.active_stocks,
        "last_scan_at": last_scan.isoformat() if isinstance(last_scan, datetime) else None,
        "total_scans": row.total_scans or 0,
        "total_alerts": row.total_alerts or 0,
        "stocks_alerted_24h": row.stocks_alerted_24h or 0,
        "threshold_min": threshold_min,
    }
