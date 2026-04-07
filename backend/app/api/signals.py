"""Signal export API for quantitative trading systems."""

from __future__ import annotations

import csv
import io
from datetime import date, datetime, timedelta, timezone

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.deps import get_db, get_current_user
from backend.app.models.user import User
from backend.app.models.leaderboard import SignalEvaluation

router = APIRouter(prefix="/signals", tags=["signals"])


@router.get("/export")
async def export_signals(
    ticker: str | None = Query(None, description="Filter by ticker"),
    source: str | None = Query(None, description="Filter by source name"),
    market: str | None = Query(None, description="Filter by market (china/us/hk)"),
    start: date | None = Query(None, description="Start date (default: 90 days ago)"),
    end: date | None = Query(None, description="End date (default: today)"),
    format: str = Query("json", description="Output format: json or csv"),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Export flat signal data for quant systems / pandas consumption."""
    if not start:
        start = (datetime.now(timezone.utc) - timedelta(days=90)).date()
    if not end:
        end = datetime.now(timezone.utc).date()

    filters = [
        SignalEvaluation.signal_time >= datetime.combine(start, datetime.min.time()).replace(tzinfo=timezone.utc),
        SignalEvaluation.signal_time <= datetime.combine(end, datetime.max.time()).replace(tzinfo=timezone.utc),
    ]
    if ticker:
        filters.append(SignalEvaluation.ticker == ticker)
    if source:
        filters.append(SignalEvaluation.source_name == source)
    if market:
        filters.append(SignalEvaluation.market == market)

    stmt = (
        select(SignalEvaluation)
        .where(and_(*filters))
        .order_by(SignalEvaluation.signal_time.desc())
        .limit(10000)
    )
    result = await db.execute(stmt)
    rows = result.scalars().all()

    data = []
    for r in rows:
        data.append({
            "signal_time": r.signal_time.isoformat() if r.signal_time else None,
            "ticker": r.ticker,
            "market": r.market,
            "source_name": r.source_name,
            "source_category": r.category,
            "predicted_sentiment": r.predicted_sentiment,
            "sentiment_score_short": r.sentiment_score_t1,
            "confidence_short": r.confidence_t1,
            "sentiment_score_medium": r.sentiment_score_t5,
            "confidence_medium": r.confidence_t5,
            "sentiment_score_long": r.sentiment_score_t20,
            "confidence_long": r.confidence_t20,
            "return_t0": r.return_t0,
            "return_t1": r.return_t1,
            "return_t5": r.return_t5,
            "return_t20": r.return_t20,
            "correct_t1": r.correct_t1,
            "correct_t5": r.correct_t5,
            "correct_t20": r.correct_t20,
        })

    if format == "csv":
        if not data:
            return StreamingResponse(io.StringIO(""), media_type="text/csv")
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
        output.seek(0)
        return StreamingResponse(
            output,
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename=signals_{start}_{end}.csv"},
        )

    return {"signals": data, "total": len(data), "start": str(start), "end": str(end)}
