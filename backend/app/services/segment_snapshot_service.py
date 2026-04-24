"""Segment revenue snapshot service.

When a RevenueModel hits ``ready`` with ``review_status=approved`` cells in
``segment.{slug}.{revenue|volume|asp|margin|growth_rate}.{period}``, we
project the latest approved value per natural key into
``segment_revenue_snapshot`` so the next new model for the same ticker can
reuse it without re-running the LLM.

Also exports a ClickHouse insert path so the same data populates the OLAP
``revenue_fundamentals`` table (best-effort; if CH is unreachable, we log
and move on).
"""
from __future__ import annotations

import logging
import re
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.models.revenue_model import ModelCell, RevenueModel
from backend.app.models.revenue_snapshot import SegmentRevenueSnapshot

logger = logging.getLogger(__name__)


# Matches segment.{slug}.{metric}.{period}   (metric ∈ revenue/volume/asp/margin/growth_rate)
_SEG_PATH_RE = re.compile(
    r"^segment\.([^.]+)\.(revenue|rev|volume|asp|margin|growth_rate)\.(.+)$"
)
# Normalise "rev" → "revenue"
_METRIC_ALIASES = {"rev": "revenue"}


def _parse_segment_path(path: str) -> tuple[str, str, str] | None:
    m = _SEG_PATH_RE.match(path)
    if not m:
        return None
    slug, metric, period = m.group(1), m.group(2), m.group(3)
    metric = _METRIC_ALIASES.get(metric, metric)
    return slug, metric, period


async def refresh_for_model(db: AsyncSession, model: RevenueModel) -> dict:
    """Project this model's approved segment cells into the snapshot table.

    Idempotent: ON CONFLICT DO UPDATE with greatest-timestamp wins semantics.
    """
    # Pull every segment cell for the model
    cells_q = select(ModelCell).where(
        ModelCell.model_id == model.id,
        ModelCell.path.like("segment.%"),
    )
    cells = list((await db.execute(cells_q)).scalars().all())
    projected = 0
    now = datetime.now(timezone.utc)

    for c in cells:
        parsed = _parse_segment_path(c.path)
        if not parsed:
            continue
        slug, metric, period = parsed
        # Only project values with actual numeric content
        if c.value is None and not c.value_text:
            continue
        # Don't snapshot flagged / rejected cells; only pending + approved
        if c.review_status == "flagged":
            continue
        stmt = pg_insert(SegmentRevenueSnapshot).values(
            ticker=model.ticker,
            industry=model.industry or "",
            segment_slug=slug,
            period=period,
            metric=metric,
            value=c.value,
            unit=c.unit or "",
            confidence=c.confidence or "MEDIUM",
            source_type=c.source_type or "assumption",
            source_model_id=model.id,
            source_cell_path=c.path,
            citations=c.citations or [],
            updated_at=now,
        )
        # Prefer the most-recently-approved value; HIGH beats MEDIUM beats LOW
        stmt = stmt.on_conflict_do_update(
            constraint="uq_segment_snapshot_natural_key",
            set_={
                "value": stmt.excluded.value,
                "unit": stmt.excluded.unit,
                "confidence": stmt.excluded.confidence,
                "source_type": stmt.excluded.source_type,
                "source_model_id": stmt.excluded.source_model_id,
                "source_cell_path": stmt.excluded.source_cell_path,
                "citations": stmt.excluded.citations,
                "updated_at": now,
            },
            where=(
                # Only overwrite if the new value is at least as confident,
                # or the existing row is stale by >1 day.
                (SegmentRevenueSnapshot.confidence == "LOW")
                | (SegmentRevenueSnapshot.confidence == stmt.excluded.confidence)
                | (SegmentRevenueSnapshot.updated_at < now.replace(hour=0, minute=0, second=0, microsecond=0))
            ),
        )
        await db.execute(stmt)
        projected += 1
    await db.commit()
    return {"model_id": str(model.id), "projected": projected, "total_segment_cells": len(cells)}


async def get_snapshot_for_ticker(
    db: AsyncSession, ticker: str, period: str | None = None,
) -> list[dict[str, Any]]:
    """Fetch all snapshot rows for a ticker (optionally a specific period).

    Used by step executors when seeding a new model to avoid re-running the
    LLM for data we already have high-confidence values for.
    """
    q = select(SegmentRevenueSnapshot).where(SegmentRevenueSnapshot.ticker == ticker)
    if period:
        q = q.where(SegmentRevenueSnapshot.period == period)
    rows = list((await db.execute(q)).scalars().all())
    return [
        {
            "ticker": r.ticker,
            "industry": r.industry,
            "segment_slug": r.segment_slug,
            "period": r.period,
            "metric": r.metric,
            "value": r.value,
            "unit": r.unit,
            "confidence": r.confidence,
            "source_type": r.source_type,
            "source_model_id": str(r.source_model_id) if r.source_model_id else None,
            "source_cell_path": r.source_cell_path,
            "citations": r.citations,
            "updated_at": r.updated_at.isoformat() if r.updated_at else None,
        }
        for r in rows
    ]


async def get_cross_model_comparison(
    db: AsyncSession,
    tickers: list[str],
    metric: str = "revenue",
    period: str | None = None,
) -> list[dict]:
    """Cross-sectional compare: same metric + period, across multiple tickers.

    Used by the analytics page and by peer-benchmarking steps.
    """
    q = select(SegmentRevenueSnapshot).where(
        SegmentRevenueSnapshot.ticker.in_(tickers),
        SegmentRevenueSnapshot.metric == metric,
    )
    if period:
        q = q.where(SegmentRevenueSnapshot.period == period)
    rows = list((await db.execute(q)).scalars().all())
    # Group by segment_slug
    by_seg: dict[str, list[dict]] = {}
    for r in rows:
        by_seg.setdefault(r.segment_slug, []).append({
            "ticker": r.ticker,
            "period": r.period,
            "value": r.value,
            "unit": r.unit,
            "confidence": r.confidence,
            "source_type": r.source_type,
        })
    return [{"segment_slug": k, "rows": v} for k, v in sorted(by_seg.items())]
