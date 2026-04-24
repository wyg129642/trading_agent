"""Citation audit API — trigger audits, view history, fetch hallucination summary."""
from __future__ import annotations

import logging
import uuid
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.deps import get_current_boss_or_admin, get_current_user, get_db
from backend.app.models.user import User
from backend.app.services.citation_audit import (
    CitationAuditLog,
    audit_model,
    hallucination_summary,
)

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/models/{model_id}/audit")
async def run_audit(
    model_id: uuid.UUID,
    sample_rate: float = Query(0.05, ge=0.01, le=1.0),
    max_samples: int = Query(20, ge=1, le=200),
    user: User = Depends(get_current_user),
):
    """Trigger a citation audit on a model (any authenticated user)."""
    result = await audit_model(
        model_id=model_id,
        sample_rate=sample_rate,
        max_samples=max_samples,
    )
    return result


@router.get("/models/{model_id}/audit")
async def list_audits(
    model_id: uuid.UUID,
    limit: int = Query(100, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    q = (
        select(CitationAuditLog)
        .where(CitationAuditLog.model_id == model_id)
        .order_by(CitationAuditLog.created_at.desc())
        .limit(limit)
    )
    rows = list((await db.execute(q)).scalars().all())
    return [
        {
            "id": str(r.id),
            "model_id": str(r.model_id),
            "cell_id": str(r.cell_id) if r.cell_id else None,
            "cell_path": r.cell_path,
            "citation_title": r.citation_title,
            "citation_url": r.citation_url,
            "claimed_snippet": r.claimed_snippet,
            "verdict": r.verdict,
            "verdict_reason": r.verdict_reason,
            "details": r.details,
            "tokens_used": r.tokens_used,
            "latency_ms": r.latency_ms,
            "created_at": r.created_at.isoformat(),
        }
        for r in rows
    ]


@router.get("/summary")
async def get_summary(
    since_days: int = Query(7, ge=1, le=365),
    user: User = Depends(get_current_boss_or_admin),
):
    return await hallucination_summary(since_days=since_days)


@router.post("/review/run-now")
async def run_review_now(
    since_days: int = Query(7, ge=1, le=365),
    auto_pause: bool = Query(True),
    user: User = Depends(get_current_boss_or_admin),
):
    """Admin: kick off the weekly hallucination-guard review on demand.

    Useful after the pipeline changes land so operators don't have to wait
    until Monday 08:00 for the first cycle.
    """
    from backend.app.services.hallucination_guard import weekly_review_and_alert
    return await weekly_review_and_alert(since_days=since_days, auto_pause=auto_pause)


@router.post("/review/resume-model/{model_id}")
async def resume_model(
    model_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_boss_or_admin),
):
    """Admin: clear a guard-triggered pause once the underlying issue is fixed."""
    from backend.app.models.revenue_model import RevenueModel
    m = await db.get(RevenueModel, model_id)
    if not m:
        raise HTTPException(404)
    if not m.paused_by_guard:
        return {"status": "no-op", "model_id": str(model_id)}
    m.paused_by_guard = False
    m.paused_reason = None
    await db.commit()
    return {"status": "resumed", "model_id": str(model_id)}
