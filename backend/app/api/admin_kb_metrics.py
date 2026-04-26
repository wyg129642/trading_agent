"""Admin observability dashboard for KB retrieval (kb_search / user_kb_search /
kb_fetch_document).

Endpoints (all require admin role):

* ``GET /api/admin/kb-metrics/summary``       — per-tool counts, p50/p95/p99
                                                 latency, empty-result rate.
* ``GET /api/admin/kb-metrics/timeseries``    — buckets for trend charts.
* ``GET /api/admin/kb-metrics/empty-queries`` — recent zero-hit queries (for
                                                 alias/recall improvement).
* ``GET /api/admin/kb-metrics/slow-queries``  — recent slowest queries (for
                                                 performance regressions).

All read from the ``kb_search_metrics`` table populated fire-and-forget by
``backend/app/services/kb_metrics.py``.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy import desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.deps import get_current_admin, get_db
from backend.app.models.kb_metrics import KbSearchMetric
from backend.app.models.user import User

router = APIRouter()
logger = logging.getLogger(__name__)


def _percentile(rows: list[int], p: float) -> int:
    """Inline percentile to avoid pulling numpy. Returns int for ms display."""
    if not rows:
        return 0
    s = sorted(rows)
    if p <= 0:
        return s[0]
    if p >= 1:
        return s[-1]
    k = (len(s) - 1) * p
    f, c = int(k), min(int(k) + 1, len(s) - 1)
    if f == c:
        return s[f]
    return int(s[f] + (s[c] - s[f]) * (k - f))


@router.get("/summary")
async def summary(
    days: int = Query(7, ge=1, le=90),
    db: AsyncSession = Depends(get_db),
    _admin: User = Depends(get_current_admin),
) -> dict[str, Any]:
    """Per-tool roll-up over the past ``days`` days."""
    since = datetime.now(timezone.utc) - timedelta(days=days)

    # Pull every (tool_name, total_ms, result_count, error) row in window.
    # 110k rows/year ≈ 2k/week — small enough to do percentile in Python.
    res = await db.execute(
        select(
            KbSearchMetric.tool_name,
            KbSearchMetric.total_ms,
            KbSearchMetric.result_count,
            KbSearchMetric.error,
        ).where(KbSearchMetric.ts >= since)
    )
    rows = res.all()

    by_tool: dict[str, dict[str, Any]] = {}
    for tool, total_ms, hits, err in rows:
        bucket = by_tool.setdefault(tool, {
            "tool_name": tool, "calls": 0, "empty": 0, "errors": 0,
            "result_counts": [], "total_ms": [],
        })
        bucket["calls"] += 1
        if hits == 0:
            bucket["empty"] += 1
        if err:
            bucket["errors"] += 1
        bucket["result_counts"].append(int(hits or 0))
        bucket["total_ms"].append(int(total_ms or 0))

    out = []
    for tool, b in by_tool.items():
        rc = b["result_counts"]
        tm = b["total_ms"]
        out.append({
            "tool_name": tool,
            "calls": b["calls"],
            "empty_result_rate": round(b["empty"] / b["calls"], 4) if b["calls"] else 0.0,
            "error_rate": round(b["errors"] / b["calls"], 4) if b["calls"] else 0.0,
            "avg_results": round(sum(rc) / len(rc), 2) if rc else 0.0,
            "p50_ms": _percentile(tm, 0.5),
            "p95_ms": _percentile(tm, 0.95),
            "p99_ms": _percentile(tm, 0.99),
        })
    out.sort(key=lambda r: -r["calls"])
    return {"window_days": days, "since": since.isoformat(), "tools": out}


@router.get("/timeseries")
async def timeseries(
    days: int = Query(14, ge=1, le=90),
    interval: str = Query("1h", pattern="^(15m|1h|6h|1d)$"),
    db: AsyncSession = Depends(get_db),
    _admin: User = Depends(get_current_admin),
) -> dict[str, Any]:
    """Bucketed call count + p95 latency for trend charts."""
    since = datetime.now(timezone.utc) - timedelta(days=days)
    bucket_seconds = {"15m": 900, "1h": 3600, "6h": 21600, "1d": 86400}[interval]
    bucket_expr = func.date_bin(
        f"{bucket_seconds} seconds", KbSearchMetric.ts, since,
    ).label("bucket")

    # Pull (bucket, tool_name, total_ms) and aggregate in Python so p95 is
    # easy without window functions per bucket.
    res = await db.execute(
        select(bucket_expr, KbSearchMetric.tool_name, KbSearchMetric.total_ms)
        .where(KbSearchMetric.ts >= since)
        .order_by("bucket")
    )
    rows = res.all()

    grouped: dict[tuple[Any, str], list[int]] = {}
    for bucket, tool, total_ms in rows:
        grouped.setdefault((bucket, tool), []).append(int(total_ms or 0))

    series: dict[str, list[dict]] = {}
    for (bucket, tool), latencies in grouped.items():
        series.setdefault(tool, []).append({
            "bucket": bucket.isoformat() if bucket else None,
            "calls": len(latencies),
            "p95_ms": _percentile(latencies, 0.95),
        })
    for tool in series:
        series[tool].sort(key=lambda r: r["bucket"] or "")
    return {"window_days": days, "interval": interval, "series": series}


@router.get("/empty-queries")
async def empty_queries(
    days: int = Query(7, ge=1, le=30),
    limit: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
    _admin: User = Depends(get_current_admin),
) -> dict[str, Any]:
    """Most recent KB calls that returned 0 hits (recall improvement input)."""
    since = datetime.now(timezone.utc) - timedelta(days=days)
    res = await db.execute(
        select(
            KbSearchMetric.ts, KbSearchMetric.trace_id, KbSearchMetric.tool_name,
            KbSearchMetric.query, KbSearchMetric.ticker_count,
            KbSearchMetric.has_date_filter, KbSearchMetric.total_ms,
        )
        .where(KbSearchMetric.ts >= since)
        .where(KbSearchMetric.result_count == 0)
        .where(KbSearchMetric.tool_name.in_(("kb_search", "user_kb_search")))
        .order_by(desc(KbSearchMetric.ts))
        .limit(limit)
    )
    rows = [
        {
            "ts": r[0].isoformat(), "trace_id": r[1], "tool_name": r[2],
            "query": r[3] or "", "ticker_count": r[4],
            "has_date_filter": bool(r[5]), "total_ms": r[6],
        }
        for r in res.all()
    ]
    return {"window_days": days, "rows": rows}


@router.get("/slow-queries")
async def slow_queries(
    days: int = Query(7, ge=1, le=30),
    limit: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
    _admin: User = Depends(get_current_admin),
) -> dict[str, Any]:
    """Slowest KB calls in the past window (perf regression input)."""
    since = datetime.now(timezone.utc) - timedelta(days=days)
    res = await db.execute(
        select(
            KbSearchMetric.ts, KbSearchMetric.trace_id, KbSearchMetric.tool_name,
            KbSearchMetric.query, KbSearchMetric.total_ms,
            KbSearchMetric.embed_ms, KbSearchMetric.milvus_ms, KbSearchMetric.mongo_ms,
            KbSearchMetric.result_count, KbSearchMetric.mode,
        )
        .where(KbSearchMetric.ts >= since)
        .order_by(desc(KbSearchMetric.total_ms))
        .limit(limit)
    )
    rows = [
        {
            "ts": r[0].isoformat(), "trace_id": r[1], "tool_name": r[2],
            "query": r[3] or "", "total_ms": r[4],
            "embed_ms": r[5], "milvus_ms": r[6], "mongo_ms": r[7],
            "result_count": r[8], "mode": r[9],
        }
        for r in res.all()
    ]
    return {"window_days": days, "rows": rows}
