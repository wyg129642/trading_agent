"""Chat feedback + per-user memory API.

Endpoints:
  POST   /feedback/{response_id}        Submit detailed feedback (rating + tags + text)
  GET    /feedback                      List the current user's feedback events
  GET    /memories                      List the current user's memories
  POST   /memories                      Manually add a memory
  PATCH  /memories/{id}                 Toggle is_active / is_pinned / edit content
  DELETE /memories/{id}                 Permanently delete one memory
  POST   /memories/reprocess/{fb_id}    Admin: re-enqueue a feedback event for reprocessing

All endpoints require auth. Memories and feedback are strictly user-scoped —
a user can never see or touch another user's rows.
"""
from __future__ import annotations

import logging
import re
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, func, desc, delete
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.deps import get_db, get_current_user
from backend.app.models.chat import ChatConversation, ChatMessage, ChatModelResponse
from backend.app.models.chat_memory import (
    ChatFeedbackEvent, UserChatMemory,
    MEMORY_TYPES, FEEDBACK_SENTIMENTS,
)
from backend.app.models.user import User
from backend.app.schemas.chat import (
    FeedbackSubmitRequest, FeedbackResponse,
    MemoryResponse, MemoryListResponse, MemoryUpdateRequest, MemoryCreateRequest,
)
from backend.app.services.chat_memory_extractor import _sentiment_from_signals

logger = logging.getLogger(__name__)
router = APIRouter()


_KEY_SAFE_RE = re.compile(r"[^a-z0-9_]+")


def _normalize_memory_key(raw: str) -> str:
    k = _KEY_SAFE_RE.sub("_", (raw or "").lower()).strip("_")
    k = re.sub(r"_+", "_", k)
    return k[:120]


def _memory_to_response(m: UserChatMemory) -> MemoryResponse:
    return MemoryResponse(
        id=str(m.id),
        memory_type=m.memory_type,
        memory_key=m.memory_key,
        content=m.content,
        evidence=list(m.evidence or []),
        confidence_score=float(m.confidence_score or 0),
        source_type=m.source_type,
        usage_count=int(m.usage_count or 0),
        is_active=bool(m.is_active),
        is_pinned=bool(m.is_pinned),
        last_used_at=m.last_used_at,
        created_at=m.created_at,
        updated_at=m.updated_at,
    )


def _feedback_to_response(f: ChatFeedbackEvent) -> FeedbackResponse:
    return FeedbackResponse(
        id=str(f.id),
        response_id=str(f.response_id),
        rating=f.rating,
        feedback_tags=list(f.feedback_tags or []),
        feedback_text=f.feedback_text or "",
        sentiment=f.sentiment,
        processed=bool(f.processed),
        created_at=f.created_at,
    )


async def _require_response_access(
    db: AsyncSession, response_id: str, user: User,
) -> ChatModelResponse:
    """Fetch a response and assert the user owns the underlying conversation.

    Factored so feedback endpoints can share auth + 404 logic.
    """
    resp = await db.scalar(
        select(ChatModelResponse).where(ChatModelResponse.id == response_id)
    )
    if not resp:
        raise HTTPException(404, "Response not found")
    msg = await db.scalar(select(ChatMessage).where(ChatMessage.id == resp.message_id))
    if not msg:
        raise HTTPException(404, "Message not found")
    conv = await db.scalar(
        select(ChatConversation).where(
            ChatConversation.id == msg.conversation_id,
            ChatConversation.user_id == user.id,
        )
    )
    if not conv:
        raise HTTPException(403, "Not your conversation")
    return resp


# ───────────────────────────────────────────────────────────────
# Feedback
# ───────────────────────────────────────────────────────────────

@router.post("/feedback/{response_id}", response_model=FeedbackResponse)
async def submit_feedback(
    response_id: str,
    body: FeedbackSubmitRequest,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Submit detailed qualitative feedback on one model response.

    Multiple feedback events per response are allowed (the user can revisit
    and elaborate). The current latest rating is denormalized onto
    chat_model_responses for fast model-ranking queries.
    """
    # Reject empty submissions — one of rating/tags/text must be populated.
    if body.rating is None and not body.feedback_tags and not (body.feedback_text or "").strip():
        raise HTTPException(400, "Feedback must include at least one of: rating, tags, or text")

    resp = await _require_response_access(db, response_id, user)

    # Clamp + sanitize tags: lower-case snake_case, de-dup, cap at 16
    tags = []
    seen = set()
    for t in (body.feedback_tags or []):
        if not isinstance(t, str):
            continue
        tt = _normalize_memory_key(t)
        if tt and tt not in seen:
            seen.add(tt)
            tags.append(tt)
            if len(tags) >= 16:
                break

    sentiment = _sentiment_from_signals(
        body.rating, body.feedback_text or "", tags,
    )
    if sentiment not in FEEDBACK_SENTIMENTS:
        sentiment = "neutral"

    fb = ChatFeedbackEvent(
        response_id=resp.id,
        user_id=user.id,
        rating=body.rating,
        feedback_text=(body.feedback_text or "").strip(),
        feedback_tags=tags,
        sentiment=sentiment,
        processed=False,
    )
    db.add(fb)

    # Mirror latest rating / comment onto the response row for legacy fast path
    if body.rating is not None:
        resp.rating = body.rating
    if body.feedback_text:
        # Keep the most recent comment readable in the admin UI
        resp.rating_comment = (body.feedback_text or "").strip()[:4000]

    await db.commit()
    await db.refresh(fb)
    return _feedback_to_response(fb)


@router.get("/feedback", response_model=list[FeedbackResponse])
async def list_my_feedback(
    limit: int = Query(50, ge=1, le=200),
    processed: bool | None = Query(None),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    stmt = (
        select(ChatFeedbackEvent)
        .where(ChatFeedbackEvent.user_id == user.id)
        .order_by(desc(ChatFeedbackEvent.created_at))
        .limit(limit)
    )
    if processed is not None:
        stmt = stmt.where(ChatFeedbackEvent.processed == processed)
    rows = (await db.execute(stmt)).scalars().all()
    return [_feedback_to_response(r) for r in rows]


# ───────────────────────────────────────────────────────────────
# Memories
# ───────────────────────────────────────────────────────────────

@router.get("/memories", response_model=MemoryListResponse)
async def list_my_memories(
    include_inactive: bool = Query(True),
    memory_type: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    stmt = select(UserChatMemory).where(UserChatMemory.user_id == user.id)
    if not include_inactive:
        stmt = stmt.where(UserChatMemory.is_active == True)  # noqa: E712
    if memory_type:
        if memory_type not in MEMORY_TYPES:
            raise HTTPException(400, f"Invalid memory_type; must be one of {MEMORY_TYPES}")
        stmt = stmt.where(UserChatMemory.memory_type == memory_type)
    stmt = stmt.order_by(
        UserChatMemory.is_pinned.desc(),
        UserChatMemory.is_active.desc(),
        UserChatMemory.confidence_score.desc(),
        UserChatMemory.updated_at.desc(),
    )
    rows = (await db.execute(stmt)).scalars().all()

    active_count = await db.scalar(
        select(func.count(UserChatMemory.id)).where(
            UserChatMemory.user_id == user.id,
            UserChatMemory.is_active == True,  # noqa: E712
        )
    ) or 0
    total_count = await db.scalar(
        select(func.count(UserChatMemory.id)).where(
            UserChatMemory.user_id == user.id,
        )
    ) or 0

    return MemoryListResponse(
        memories=[_memory_to_response(m) for m in rows],
        total=int(total_count),
        total_active=int(active_count),
    )


@router.post("/memories", response_model=MemoryResponse)
async def create_memory(
    body: MemoryCreateRequest,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    """Manually pin a memory (e.g. 'I always want forward-PE in industry tables').

    A manual memory is marked source_type='manual' and is_pinned defaults to
    the body flag. Manual memories never get overwritten by the extractor —
    the upsert path only touches memories it produced.
    """
    if body.memory_type not in MEMORY_TYPES:
        raise HTTPException(400, f"Invalid memory_type; must be one of {MEMORY_TYPES}")
    key = _normalize_memory_key(body.memory_key)
    if not key:
        raise HTTPException(400, "memory_key must contain alphanumerics")

    existing = await db.scalar(
        select(UserChatMemory).where(
            UserChatMemory.user_id == user.id,
            UserChatMemory.memory_key == key,
        )
    )
    if existing:
        raise HTTPException(409, "A memory with this key already exists")

    row = UserChatMemory(
        user_id=user.id,
        memory_type=body.memory_type,
        memory_key=key,
        content=body.content.strip(),
        evidence=[{"type": "manual", "note": "User-created"}],
        confidence_score=1.0,
        source_type="manual",
        is_active=True,
        is_pinned=bool(body.is_pinned),
    )
    db.add(row)
    await db.commit()
    await db.refresh(row)
    return _memory_to_response(row)


@router.patch("/memories/{memory_id}", response_model=MemoryResponse)
async def update_memory(
    memory_id: str,
    body: MemoryUpdateRequest,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    try:
        memory_uuid = uuid.UUID(memory_id)
    except ValueError:
        raise HTTPException(400, "Invalid memory id")

    row = await db.scalar(
        select(UserChatMemory).where(
            UserChatMemory.id == memory_uuid,
            UserChatMemory.user_id == user.id,
        )
    )
    if not row:
        raise HTTPException(404, "Memory not found")

    changed = False
    if body.is_active is not None and body.is_active != row.is_active:
        row.is_active = bool(body.is_active)
        changed = True
    if body.is_pinned is not None and body.is_pinned != row.is_pinned:
        row.is_pinned = bool(body.is_pinned)
        changed = True
    if body.content is not None:
        trimmed = body.content.strip()
        if not trimmed:
            raise HTTPException(400, "Content cannot be empty")
        if trimmed != row.content:
            row.content = trimmed
            changed = True
    if changed:
        row.updated_at = datetime.now(timezone.utc)
        await db.commit()
        await db.refresh(row)
    return _memory_to_response(row)


@router.delete("/memories/{memory_id}")
async def delete_memory(
    memory_id: str,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_current_user),
):
    try:
        memory_uuid = uuid.UUID(memory_id)
    except ValueError:
        raise HTTPException(400, "Invalid memory id")

    row = await db.scalar(
        select(UserChatMemory).where(
            UserChatMemory.id == memory_uuid,
            UserChatMemory.user_id == user.id,
        )
    )
    if not row:
        raise HTTPException(404, "Memory not found")
    await db.delete(row)
    await db.commit()
    return {"ok": True, "id": memory_id}
