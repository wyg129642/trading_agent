"""Chat memory storage + retrieval + system-prompt injection.

Two responsibilities:
1. Upsert memories produced by the extractor (dedup on (user_id, memory_key))
2. Fetch active memories for a user and format them into a system-prompt block
   that gets prepended to the chat system prompt.

Keep this module I/O-only — no LLM calls live here. All LLM work is in
`chat_memory_extractor.py`.
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Iterable

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.models.chat_memory import (
    ChatFeedbackEvent, UserChatMemory,
    MEMORY_TYPES, MEMORY_SOURCE_TYPES,
)
from backend.app.services.chat_memory_extractor import ExtractedMemory

logger = logging.getLogger(__name__)


# Max memories injected into a single chat turn. The LLM already carries the
# user's current message + history; memory should *bias* not *dominate* it.
MAX_MEMORIES_IN_PROMPT = 16

# Order memories are emitted within the prompt block (most impactful first).
_MEMORY_TYPE_DISPLAY_ORDER = [
    "correction",       # "don't do X" trumps everything
    "preference",
    "style",
    "profile",
    "topic_interest",
    "domain_knowledge",
]
_MEMORY_TYPE_LABELS = {
    "correction": "纠偏（避免重复错误）",
    "preference": "偏好（回答结构）",
    "style": "风格（语气与篇幅）",
    "profile": "画像",
    "topic_interest": "长期关注话题",
    "domain_knowledge": "领域知识 / 上下文",
}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ───────────────────────────────────────────────────────────────
# Upsert
# ───────────────────────────────────────────────────────────────

async def upsert_memories(
    db: AsyncSession,
    user_id: uuid.UUID,
    extracted: Iterable[ExtractedMemory],
    evidence: list[dict] | None = None,
    source_type: str = "feedback_derived",
) -> list[uuid.UUID]:
    """Insert-or-update a batch of extracted memories.

    Returns the list of memory ids that exist post-upsert (new + existing).
    On conflict (user_id, memory_key), we merge:
      * content ← newest non-empty
      * confidence ← max(old, new) (monotonic toward higher certainty)
      * evidence ← append (de-duped), capped to last 20 entries
      * memory_type ← newest (in case extractor reclassified)
      * is_active ← unchanged (user's disable wins forever)
    """
    if source_type not in MEMORY_SOURCE_TYPES:
        source_type = "feedback_derived"

    upserted: list[uuid.UUID] = []
    evidence = evidence or []

    for mem in extracted:
        if mem.memory_type not in MEMORY_TYPES:
            continue
        if not mem.memory_key or not mem.content:
            continue

        existing = await db.scalar(
            select(UserChatMemory).where(
                UserChatMemory.user_id == user_id,
                UserChatMemory.memory_key == mem.memory_key,
            )
        )
        if existing is None:
            row = UserChatMemory(
                user_id=user_id,
                memory_type=mem.memory_type,
                memory_key=mem.memory_key,
                content=mem.content,
                evidence=list(evidence),
                confidence_score=mem.confidence,
                source_type=source_type,
                is_active=True,
                is_pinned=False,
            )
            db.add(row)
            await db.flush()  # get the id
            upserted.append(row.id)
        else:
            # Merge evidence, cap at 20
            merged_evidence = list(existing.evidence or [])
            for e in evidence:
                if e not in merged_evidence:
                    merged_evidence.append(e)
            merged_evidence = merged_evidence[-20:]

            existing.content = mem.content
            existing.memory_type = mem.memory_type
            existing.confidence_score = max(
                float(existing.confidence_score or 0.0), float(mem.confidence),
            )
            existing.evidence = merged_evidence
            existing.updated_at = _utcnow()
            upserted.append(existing.id)

    return upserted


# ───────────────────────────────────────────────────────────────
# Retrieval
# ───────────────────────────────────────────────────────────────

async def fetch_active_memories(
    db: AsyncSession,
    user_id: uuid.UUID,
    limit: int = MAX_MEMORIES_IN_PROMPT,
) -> list[UserChatMemory]:
    """Fetch active memories for a user, ordered by impact.

    Priority: pinned > correction > higher-confidence > more-recently-updated.
    This is a deterministic cheap ranking — no embeddings needed for v1.
    """
    # Pinned always wins; correction type is next (trust-building move).
    # Then confidence × freshness.
    stmt = (
        select(UserChatMemory)
        .where(
            UserChatMemory.user_id == user_id,
            UserChatMemory.is_active == True,  # noqa: E712
        )
        .order_by(
            UserChatMemory.is_pinned.desc(),
            # "correction" is the only type we hoist above confidence
            (UserChatMemory.memory_type == "correction").desc(),
            UserChatMemory.confidence_score.desc(),
            UserChatMemory.updated_at.desc(),
        )
        .limit(limit)
    )
    rows = (await db.execute(stmt)).scalars().all()
    return list(rows)


async def mark_memories_used(
    db: AsyncSession, memory_ids: list[uuid.UUID],
) -> None:
    """Bump usage_count + last_used_at. Called after prompt injection.

    Best-effort: never raise. A bump failure must not break chat.
    """
    if not memory_ids:
        return
    try:
        await db.execute(
            update(UserChatMemory)
            .where(UserChatMemory.id.in_(memory_ids))
            .values(
                usage_count=UserChatMemory.usage_count + 1,
                last_used_at=_utcnow(),
            )
            .execution_options(synchronize_session=False)
        )
        await db.commit()
    except Exception:
        logger.exception("mark_memories_used failed — non-fatal")


# ───────────────────────────────────────────────────────────────
# System-prompt formatting
# ───────────────────────────────────────────────────────────────

MEMORY_PROMPT_HEADER = (
    "## 用户长期记忆（基于该用户历史反馈自动提炼，务必遵守）\n\n"
    "以下是你与**当前用户**的长期合作要点。这些都是从该用户过往的明确反馈中"
    "提炼出来的，你的回答应当**自然体现**这些偏好/画像/纠偏，不要机械地引用或"
    "明确提到记忆本身。若与用户当前问题直接冲突，以用户当前意图为准。\n\n"
)


def build_memory_prompt_block(memories: list[UserChatMemory]) -> str:
    """Render active memories into a human-readable block for the system prompt.

    Groups by type for readability and skips empty categories.
    Returns "" if there are no memories.
    """
    if not memories:
        return ""

    buckets: dict[str, list[UserChatMemory]] = {t: [] for t in MEMORY_TYPES}
    for m in memories:
        if m.memory_type in buckets:
            buckets[m.memory_type].append(m)

    lines: list[str] = [MEMORY_PROMPT_HEADER.rstrip()]
    for t in _MEMORY_TYPE_DISPLAY_ORDER:
        bucket = buckets.get(t) or []
        if not bucket:
            continue
        lines.append(f"\n### {_MEMORY_TYPE_LABELS.get(t, t)}")
        for m in bucket:
            pin = "📌 " if m.is_pinned else ""
            lines.append(f"- {pin}{m.content}")
    lines.append("")  # trailing newline
    return "\n".join(lines)


async def build_user_memory_prompt(
    db: AsyncSession,
    user_id: uuid.UUID,
    limit: int = MAX_MEMORIES_IN_PROMPT,
) -> tuple[str, list[uuid.UUID]]:
    """Convenience: fetch + render in one call.

    Returns (prompt_block_text, memory_ids_used). The memory_ids can be fed
    to `mark_memories_used` asynchronously after the chat request settles.
    """
    memories = await fetch_active_memories(db, user_id, limit=limit)
    return build_memory_prompt_block(memories), [m.id for m in memories]


# ───────────────────────────────────────────────────────────────
# Feedback event helpers
# ───────────────────────────────────────────────────────────────

async def list_unprocessed_feedback(
    db: AsyncSession, limit: int = 50,
) -> list[ChatFeedbackEvent]:
    """Oldest-first batch of events needing extraction."""
    stmt = (
        select(ChatFeedbackEvent)
        .where(ChatFeedbackEvent.processed == False)  # noqa: E712
        .order_by(ChatFeedbackEvent.created_at.asc())
        .limit(limit)
    )
    rows = (await db.execute(stmt)).scalars().all()
    return list(rows)


async def mark_feedback_processed(
    db: AsyncSession,
    event_id: uuid.UUID,
    memory_ids: list[uuid.UUID],
    sentiment: str,
    error: str | None = None,
) -> None:
    """Record that the daemon has consumed this feedback event.

    On extractor failure we still set processed=True but record the error so
    the daemon doesn't infinite-loop on a single broken event. Re-processing
    is manual (admin can UPDATE processed=false).
    """
    await db.execute(
        update(ChatFeedbackEvent)
        .where(ChatFeedbackEvent.id == event_id)
        .values(
            processed=True,
            processed_at=_utcnow(),
            memory_ids_created=[str(mid) for mid in memory_ids],
            sentiment=sentiment,
            process_error=error,
        )
    )
