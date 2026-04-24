"""Expert call request management API."""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.deps import get_current_user, get_db
from backend.app.models.revenue_model_extras import ExpertCallRequest
from backend.app.models.user import User

logger = logging.getLogger(__name__)
router = APIRouter()


class ExpertCallCreate(BaseModel):
    model_id: str
    cell_path: str | None = None
    ticker: str
    topic: str
    questions: list[str]
    rationale: str = ""


class ExpertCallUpdate(BaseModel):
    status: str | None = None
    assigned_to: str | None = None
    interview_doc_id: str | None = None
    questions: list[str] | None = None


@router.get("")
async def list_requests(
    status: str | None = Query(None),
    ticker: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    q = select(ExpertCallRequest).order_by(ExpertCallRequest.created_at.desc())
    if status:
        q = q.where(ExpertCallRequest.status == status)
    if ticker:
        q = q.where(ExpertCallRequest.ticker == ticker)
    rows = list((await db.execute(q)).scalars().all())
    return [_to_dict(r) for r in rows]


@router.post("")
async def create_request(
    body: ExpertCallCreate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    row = ExpertCallRequest(
        model_id=uuid.UUID(body.model_id),
        cell_path=body.cell_path,
        ticker=body.ticker,
        topic=body.topic,
        questions=list(body.questions or []),
        rationale=body.rationale,
        requested_by=user.id,
        status="open",
    )
    db.add(row)
    await db.commit()
    await db.refresh(row)
    return _to_dict(row)


@router.patch("/{req_id}")
async def update_request(
    req_id: uuid.UUID,
    body: ExpertCallUpdate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    row = await db.get(ExpertCallRequest, req_id)
    if not row:
        raise HTTPException(404)
    if body.status is not None:
        if body.status not in ("open", "scheduled", "completed", "cancelled"):
            raise HTTPException(400, "invalid status")
        row.status = body.status
        if body.status in ("completed", "cancelled"):
            row.resolved_at = datetime.now(timezone.utc)
    if body.assigned_to is not None:
        row.assigned_to = uuid.UUID(body.assigned_to) if body.assigned_to else None
    if body.interview_doc_id is not None:
        row.interview_doc_id = body.interview_doc_id
    if body.questions is not None:
        row.questions = list(body.questions)
    await db.commit()
    return _to_dict(row)


@router.post("/{req_id}/mark-completed")
async def mark_completed(
    req_id: uuid.UUID,
    interview_doc_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Marks an expert-call request as completed and triggers a re-run of
    dependent cells in the source model so they can re-incorporate the
    new interview evidence.
    """
    row = await db.get(ExpertCallRequest, req_id)
    if not row:
        raise HTTPException(404)
    row.status = "completed"
    row.interview_doc_id = interview_doc_id
    row.resolved_at = datetime.now(timezone.utc)
    await db.commit()
    # Emit a feedback event so the consolidator sees this completion
    from backend.app.services import model_cell_store as _store
    await _store.emit_feedback(
        db, user_id=user.id, event_type="expert_call_completed",
        model_id=row.model_id,
        cell_path=row.cell_path,
        payload={
            "interview_doc_id": interview_doc_id,
            "topic": row.topic,
            "req_id": str(row.id),
        },
    )
    await db.commit()
    return _to_dict(row)


def _to_dict(r: ExpertCallRequest) -> dict[str, Any]:
    return {
        "id": str(r.id),
        "model_id": str(r.model_id),
        "cell_path": r.cell_path,
        "ticker": r.ticker,
        "topic": r.topic,
        "questions": r.questions,
        "rationale": r.rationale,
        "status": r.status,
        "requested_by": str(r.requested_by) if r.requested_by else None,
        "assigned_to": str(r.assigned_to) if r.assigned_to else None,
        "interview_doc_id": r.interview_doc_id,
        "created_at": r.created_at.isoformat(),
        "resolved_at": r.resolved_at.isoformat() if r.resolved_at else None,
    }
