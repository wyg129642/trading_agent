"""Portfolio Breaking News API — serves proactive scanner alerts.

Primary data source: PostgreSQL (portfolio_scan_results + portfolio_scan_baselines).
The proactive scanner always writes scan results to PostgreSQL, making it the
reliable source of truth. ClickHouse is used for long-term analytics only.
"""

from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.deps import get_current_boss_or_admin, get_db, get_portfolio_scan_db

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


# ---------------------------------------------------------------------------
# Per-ticker fuzzy dedup
# ---------------------------------------------------------------------------
#
# The scanner's baseline.known_event_hashes dedup hashes the LLM's polished
# summary verbatim, so two scans of the same underlying news that happen to
# paraphrase differently ("4 月 22 日前" vs "2026-04-21 至 27 期间") slip
# through and produce duplicate cards. We additionally dedupe at API time:
# within a ticker we cluster by (a) overlapping referenced source URLs and
# (b) char-bigram Jaccard on the summary, then keep only the freshest row
# per cluster.

_DEDUP_JACCARD_STRONG = 0.45         # content alone is enough
_DEDUP_JACCARD_FLOOR = 0.20          # min for "same event" when paired with metadata
_DEDUP_URL_OVERLAP_MIN = 2           # ≥2 shared host+path → same event
_DEDUP_EARLIEST_GAP_HOURS = 36       # earliest_report_time bucket
_DEDUP_TIME_WINDOW_HOURS = 96        # outer bound on scan_time gap

# Sentiment "families" — LLM commonly drifts between e.g. "bearish" and
# "very_bearish" between scans of the same story; treat them equivalently
# for dedup so paraphrased rows merge.
_SENTIMENT_FAMILY = {
    "very_bearish": "bear",
    "bearish": "bear",
    "neutral": "neutral",
    "bullish": "bull",
    "very_bullish": "bull",
}


_CN_CHAR_RE = re.compile(r"[一-鿿]")
_EN_TOKEN_RE = re.compile(r"[a-z]{3,}")


def _summary_signature(text_in: str) -> set[str]:
    """Content fingerprint robust to LLM paraphrasing.

    Combines:
    - the set of Chinese characters (drops bigram fragility on word order)
    - English tokens of length ≥ 3 lowercased (catches "Lam", "AMAT", "KLA",
      "is-informed", "Reuters" etc.)

    Numbers/dates/punctuation are *not* in either bucket, so wording like
    "4月22日" vs "2026-04-21至27" doesn't affect the signature.
    """
    if not text_in:
        return set()
    text_lower = text_in.lower()
    cn = set(_CN_CHAR_RE.findall(text_in))
    en = set(_EN_TOKEN_RE.findall(text_lower))
    return cn | en


def _normalize_url(url: str) -> str:
    """Reduce a URL to host+path with trailing slash and fragment removed."""
    if not url:
        return ""
    url = url.strip().lower()
    url = re.sub(r"^https?://", "", url)
    url = re.sub(r"#.*$", "", url)
    url = re.sub(r"\?.*$", "", url)
    return url.rstrip("/")


def _item_url_set(item: dict) -> set[str]:
    """Collect normalized URLs from sources / referenced_sources / news_timeline."""
    urls: set[str] = set()
    for bucket_name in ("sources", "referenced_sources", "news_timeline"):
        bucket = item.get(bucket_name) or []
        if not isinstance(bucket, list):
            continue
        for entry in bucket:
            if isinstance(entry, dict):
                u = _normalize_url(str(entry.get("url") or ""))
                if u:
                    urls.add(u)
    return urls


def _jaccard(a: set, b: set) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    if inter == 0:
        return 0.0
    return inter / len(a | b)


def _earliest_dt(item: dict) -> datetime | None:
    """Extract the earliest known report time from the item, if any."""
    return _parse_iso(item.get("earliest_report_time"))


def _sentiment_family(item: dict) -> str:
    return _SENTIMENT_FAMILY.get((item.get("sentiment") or "").lower(), "")


def _is_duplicate_event(
    item: dict, prev_item: dict,
    sig: set[str], prev_sig: set[str],
    urls: set[str], prev_urls: set[str],
) -> bool:
    """Composite duplicate test for two scan rows of the same ticker.

    Two rows are the same underlying event when **any** of:
      (a) they share ≥ N referenced URLs (host+path) — strongest signal;
      (b) their content signatures (CJK char set ∪ EN tokens ≥ 3 chars)
          have Jaccard ≥ JACCARD_STRONG — content alone is conclusive;
      (c) earliest_report_time fall in the same ~1.5 day bucket AND they
          share the same sentiment family + materiality AND have at least
          a JACCARD_FLOOR worth of content overlap (avoids merging two
          unrelated bearish stories that happened to break the same day).

    The sentiment family relaxation matters because the LLM frequently
    drifts between "bearish" and "very_bearish" across scans of the same
    underlying story without the underlying news changing.
    """
    # (a) URL overlap — strongest signal
    if urls and prev_urls and len(urls & prev_urls) >= _DEDUP_URL_OVERLAP_MIN:
        return True

    sig_jaccard = _jaccard(sig, prev_sig)

    # (b) Strong content match alone
    if sig_jaccard >= _DEDUP_JACCARD_STRONG:
        return True

    # (c) Earliest-report bucket + sentiment family + magnitude + minimum overlap
    e1, e2 = _earliest_dt(item), _earliest_dt(prev_item)
    if e1 and e2:
        gap_h = abs((e1 - e2).total_seconds()) / 3600
        if (
            gap_h <= _DEDUP_EARLIEST_GAP_HOURS
            and _sentiment_family(item) == _sentiment_family(prev_item)
            and item.get("news_materiality") == prev_item.get("news_materiality")
            and sig_jaccard >= _DEDUP_JACCARD_FLOOR
        ):
            return True

    return False


def _dedupe_breaking_items(items: list[dict]) -> list[dict]:
    """Collapse duplicate scan rows for the same underlying event.

    Items must already be sorted by scan_time DESC (newest first). For each
    ticker we walk the list in order; a row is kept only if it is not a
    duplicate of any already-kept row for the same ticker (within the
    DEDUP_TIME_WINDOW). The newest row wins because we walk newest→oldest
    and only kept rows are compared against future candidates.
    """
    if not items:
        return items

    by_ticker: dict[str, list[tuple[dict, set[str], set[str]]]] = {}
    out: list[dict] = []

    for item in items:
        ticker = item.get("ticker") or ""
        urls = _item_url_set(item)
        sig = _summary_signature(item.get("news_summary") or "")
        scan_time = _parse_iso(item.get("scan_time"))

        kept = by_ticker.setdefault(ticker, [])
        is_dup = False
        for prev_item, prev_urls, prev_sig in kept:
            prev_time = _parse_iso(prev_item.get("scan_time"))
            if scan_time and prev_time:
                gap_h = abs((prev_time - scan_time).total_seconds()) / 3600
                if gap_h > _DEDUP_TIME_WINDOW_HOURS:
                    continue
            if _is_duplicate_event(item, prev_item, sig, prev_sig, urls, prev_urls):
                is_dup = True
                break

        if not is_dup:
            kept.append((item, urls, sig))
            out.append(item)

    return out


# ---------------------------------------------------------------------------
# Per-user dismissals (已阅)
# ---------------------------------------------------------------------------


async def _load_dismissed_signal_ids(
    user_db: AsyncSession, user_id: Any, hours: int
) -> set[str]:
    """Return signal_ids the current user has marked 已阅 within the window.

    Bounded by the same lookback used for the feed so a stale dismissal from
    months ago doesn't keep accumulating in the result set.
    """
    if user_id is None:
        return set()
    sql = text(
        """
        SELECT signal_id
        FROM portfolio_signal_dismissals
        WHERE user_id = :uid
          AND dismissed_at >= NOW() - make_interval(hours => :h)
        """
    )
    try:
        res = await user_db.execute(sql, {"uid": user_id, "h": max(hours, 168)})
        return {row[0] for row in res.all() if row[0]}
    except Exception as e:
        logger.warning("dismissals lookup failed: %s", e)
        return set()


@router.get("/breaking-news")
async def list_breaking_news(
    user=Depends(get_current_boss_or_admin),
    db: AsyncSession = Depends(get_portfolio_scan_db),
    user_db: AsyncSession = Depends(get_db),
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
    include_dismissed: bool = Query(
        False,
        description="Include items the current user has marked 已阅. Default False.",
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

    # Data query — fetch a wider slice than `limit` so the post-filtering
    # passes (dismissals + fuzzy dedup) don't truncate too eagerly.
    raw_limit = max(limit * 4, limit + 50)
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
        LIMIT :raw_limit OFFSET :offset
    """)

    try:
        result = await db.execute(data_sql, {**params, "raw_limit": raw_limit})
        rows = result.all()
    except Exception as e:
        logger.error("Breaking news data query failed: %s", e)
        return {"items": [], "total": 0, "error": str(e)}

    items = [_row_to_item(row) for row in rows]

    # Fuzzy-dedup first — picks the newest cluster representative — then
    # apply per-user dismissals. Doing it in this order means dismissing a
    # cluster's representative makes the whole cluster disappear (the
    # cluster's older rows aren't promoted to fill the gap).
    items = _dedupe_breaking_items(items)

    dismissed_ids: set[str] = set()
    if not include_dismissed:
        dismissed_ids = await _load_dismissed_signal_ids(user_db, user.id, hours)
        if dismissed_ids:
            items = [it for it in items if it["id"] not in dismissed_ids]

    visible_total = len(items)
    items = items[:limit]

    return {
        "items": items,
        "total": visible_total,
        "raw_total": total,
        "dismissed_count": len(dismissed_ids),
    }


# ---------------------------------------------------------------------------
# 已阅 (dismiss) endpoints
# ---------------------------------------------------------------------------


@router.post("/breaking-news/{signal_id}/dismiss")
async def dismiss_breaking_news(
    signal_id: str = Path(..., min_length=1, max_length=64, description="portfolio_scan_results.id"),
    user=Depends(get_current_boss_or_admin),
    user_db: AsyncSession = Depends(get_db),
    scan_db: AsyncSession = Depends(get_portfolio_scan_db),
):
    """Mark a breaking-news signal as 已阅 for the current user.

    Idempotent: a second call updates dismissed_at to now(). Per-user state,
    so two users dismissing/un-dismissing don't affect each other.
    """
    exists = await scan_db.execute(
        text("SELECT 1 FROM portfolio_scan_results WHERE id = :sid LIMIT 1"),
        {"sid": signal_id},
    )
    if exists.scalar() is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="signal_id not found")

    await user_db.execute(
        text(
            """
            INSERT INTO portfolio_signal_dismissals (user_id, signal_id, dismissed_at)
            VALUES (:uid, :sid, now())
            ON CONFLICT (user_id, signal_id) DO UPDATE
              SET dismissed_at = EXCLUDED.dismissed_at
            """
        ),
        {"uid": user.id, "sid": signal_id},
    )
    await user_db.commit()
    return {"ok": True, "signal_id": signal_id, "dismissed": True}


@router.delete("/breaking-news/{signal_id}/dismiss")
async def undismiss_breaking_news(
    signal_id: str = Path(..., min_length=1, max_length=64),
    user=Depends(get_current_boss_or_admin),
    user_db: AsyncSession = Depends(get_db),
):
    """Restore a previously-dismissed signal for the current user."""
    await user_db.execute(
        text(
            "DELETE FROM portfolio_signal_dismissals "
            "WHERE user_id = :uid AND signal_id = :sid"
        ),
        {"uid": user.id, "sid": signal_id},
    )
    await user_db.commit()
    return {"ok": True, "signal_id": signal_id, "dismissed": False}


@router.get("/breaking-news/summary")
async def breaking_news_summary(
    user=Depends(get_current_boss_or_admin),
    db: AsyncSession = Depends(get_portfolio_scan_db),
    user_db: AsyncSession = Depends(get_db),
    hours: int = Query(168, ge=1, le=8760, description="Lookback window in hours"),
):
    """Per-ticker summary of breaking news counts and latest materiality.

    Used by the portfolio overview to show alert badges on each stock card.
    The count reflects the user's view: dismissed and near-duplicate rows
    are subtracted so the badge tracks 1:1 with what /breaking-news returns.
    """
    sql = text(f"""
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
        WHERE r.delta_detected = true
          AND r.delta_magnitude IN ('material', 'critical')
          AND r.scan_time >= NOW() - make_interval(hours => :hours)
          AND (
            r.full_analysis IS NULL
            OR r.full_analysis->>'rejection_reason' IS NULL
            OR r.full_analysis->>'rejection_reason' != 'event_too_old'
          )
          {_SENTIMENT_FILTER}
        ORDER BY r.scan_time DESC
    """)

    try:
        result = await db.execute(sql, {"hours": hours})
        rows = result.all()
    except Exception as e:
        logger.error("Breaking news summary query failed: %s", e)
        return {"summary": {}}

    items = [_row_to_item(row) for row in rows]

    # Dedup first, then drop dismissed — same ordering as /breaking-news
    items = _dedupe_breaking_items(items)
    dismissed_ids = await _load_dismissed_signal_ids(user_db, user.id, hours)
    if dismissed_ids:
        items = [it for it in items if it["id"] not in dismissed_ids]

    summary: dict[str, dict] = {}
    for it in items:
        ticker = it["ticker"]
        if ticker in summary:
            summary[ticker]["news_count"] += 1
            continue
        summary[ticker] = {
            "news_count": 1,
            "latest_scan": it.get("scan_time", ""),
            "latest_materiality": it.get("news_materiality") or "none",
            "latest_sentiment": it.get("sentiment", "neutral"),
            "latest_summary": (it.get("news_summary") or "")[:200],
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
