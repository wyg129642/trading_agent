"""Team collaboration — comments, @mentions, model sharing."""
from __future__ import annotations

import logging
import uuid
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.deps import get_current_user, get_db
from backend.app.models.revenue_model import RevenueModel
from backend.app.models.revenue_model_extras import ModelCollaborator, ModelComment
from backend.app.models.user import User

logger = logging.getLogger(__name__)
router = APIRouter()


# ── Access helper ────────────────────────────────────────────

async def _ensure_access(
    db: AsyncSession, model_id: uuid.UUID, user: User, min_role: str = "viewer",
) -> RevenueModel:
    m = await db.get(RevenueModel, model_id)
    if not m:
        raise HTTPException(404, "model not found")
    if m.owner_user_id == user.id or user.role in ("admin", "boss"):
        return m
    q = select(ModelCollaborator).where(
        ModelCollaborator.model_id == model_id,
        ModelCollaborator.user_id == user.id,
    )
    row = (await db.execute(q)).scalar_one_or_none()
    if not row:
        raise HTTPException(403, "not a collaborator")
    rank = {"viewer": 0, "editor": 1, "admin": 2}
    if rank.get(row.role, 0) < rank.get(min_role, 0):
        raise HTTPException(403, f"requires role >= {min_role}")
    return m


# ── Comments ─────────────────────────────────────────────────

class CommentCreate(BaseModel):
    body: str
    cell_id: str | None = None
    mentions: list[str] = []


class CommentUpdate(BaseModel):
    body: str | None = None
    resolved: bool | None = None


@router.get("/{model_id}/comments")
async def list_comments(
    model_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    await _ensure_access(db, model_id, user)
    q = (
        select(ModelComment)
        .where(ModelComment.model_id == model_id)
        .order_by(ModelComment.created_at.desc())
    )
    rows = list((await db.execute(q)).scalars().all())
    return [
        {
            "id": str(r.id),
            "model_id": str(r.model_id),
            "cell_id": str(r.cell_id) if r.cell_id else None,
            "author_id": str(r.author_id) if r.author_id else None,
            "mentions": r.mentions,
            "body": r.body,
            "resolved": r.resolved,
            "created_at": r.created_at.isoformat(),
        }
        for r in rows
    ]


@router.post("/{model_id}/comments")
async def post_comment(
    model_id: uuid.UUID,
    body: CommentCreate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    await _ensure_access(db, model_id, user)
    row = ModelComment(
        model_id=model_id,
        cell_id=uuid.UUID(body.cell_id) if body.cell_id else None,
        author_id=user.id,
        mentions=list(body.mentions or []),
        body=body.body,
    )
    db.add(row)
    await db.commit()
    await db.refresh(row)
    return {"id": str(row.id), "created_at": row.created_at.isoformat()}


@router.patch("/{model_id}/comments/{comment_id}")
async def update_comment(
    model_id: uuid.UUID,
    comment_id: uuid.UUID,
    body: CommentUpdate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    await _ensure_access(db, model_id, user)
    row = await db.get(ModelComment, comment_id)
    if not row or row.model_id != model_id:
        raise HTTPException(404)
    if row.author_id != user.id and user.role not in ("admin", "boss"):
        raise HTTPException(403, "only author can edit")
    if body.body is not None:
        row.body = body.body
    if body.resolved is not None:
        row.resolved = body.resolved
    await db.commit()
    return {"ok": True, "resolved": row.resolved}


@router.delete("/{model_id}/comments/{comment_id}")
async def delete_comment(
    model_id: uuid.UUID,
    comment_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    await _ensure_access(db, model_id, user)
    row = await db.get(ModelComment, comment_id)
    if not row or row.model_id != model_id:
        raise HTTPException(404)
    if row.author_id != user.id and user.role not in ("admin", "boss"):
        raise HTTPException(403, "only author can delete")
    await db.delete(row)
    await db.commit()
    return {"ok": True}


# ── Collaborators ────────────────────────────────────────────

class CollaboratorAdd(BaseModel):
    user_id: str
    role: str = "viewer"


@router.get("/{model_id}/collaborators")
async def list_collaborators(
    model_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    await _ensure_access(db, model_id, user)
    q = select(ModelCollaborator).where(ModelCollaborator.model_id == model_id)
    rows = list((await db.execute(q)).scalars().all())
    return [
        {"id": str(r.id), "user_id": str(r.user_id), "role": r.role,
         "added_at": r.added_at.isoformat()}
        for r in rows
    ]


@router.post("/{model_id}/collaborators")
async def add_collaborator(
    model_id: uuid.UUID,
    body: CollaboratorAdd,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    m = await _ensure_access(db, model_id, user, min_role="admin")
    uid = uuid.UUID(body.user_id)
    target = await db.get(User, uid)
    if not target:
        raise HTTPException(404, "target user not found")
    # Idempotent upsert
    q = select(ModelCollaborator).where(
        ModelCollaborator.model_id == model_id,
        ModelCollaborator.user_id == uid,
    )
    existing = (await db.execute(q)).scalar_one_or_none()
    if existing:
        existing.role = body.role
        await db.commit()
        return {"id": str(existing.id), "role": existing.role, "action": "updated"}
    row = ModelCollaborator(model_id=model_id, user_id=uid, role=body.role)
    db.add(row)
    await db.commit()
    await db.refresh(row)
    return {"id": str(row.id), "role": row.role, "action": "added"}


@router.delete("/{model_id}/collaborators/{user_id}")
async def remove_collaborator(
    model_id: uuid.UUID,
    user_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    await _ensure_access(db, model_id, user, min_role="admin")
    q = select(ModelCollaborator).where(
        ModelCollaborator.model_id == model_id,
        ModelCollaborator.user_id == user_id,
    )
    row = (await db.execute(q)).scalar_one_or_none()
    if not row:
        raise HTTPException(404)
    await db.delete(row)
    await db.commit()
    return {"ok": True}
