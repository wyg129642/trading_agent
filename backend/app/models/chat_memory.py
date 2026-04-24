"""Chat feedback + per-user memory models.

Powers the AI assistant self-evolution loop: users submit detailed feedback on
model responses → a background daemon distills that feedback (plus the
underlying conversation) into structured user memories → those memories are
injected into future chat system prompts so the assistant adapts to each user.

Memory types (discrete, constrained by CHECK constraint in migration):
  preference       — how the user likes responses to be structured
                     ("prefers numerical tables when comparing companies")
  style            — tone / verbosity / language
                     ("prefers concise bullet-point answers")
  profile          — who the user is
                     ("is a fundamental analyst focused on A-share semis")
  topic_interest   — topics the user actively tracks
                     ("watches AI-capex supply chain weekly")
  domain_knowledge — factual context for the user's work
                     ("owns positions in NVDA, SMCI, 002384.SZ")
  correction       — patterns the user pushed back on and wants avoided
                     ("do not cite WSJ for Chinese macro — not reliable")
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    Boolean, CheckConstraint, DateTime, Float, ForeignKey, Index, Integer,
    String, Text, UniqueConstraint, text,
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from backend.app.core.database import Base


def utcnow():
    return datetime.now(timezone.utc)


# Allowed discrete values — keep in sync with CHECK constraints in migration
MEMORY_TYPES = (
    "preference",
    "style",
    "profile",
    "topic_interest",
    "domain_knowledge",
    "correction",
)
MEMORY_SOURCE_TYPES = ("feedback_derived", "conversation_derived", "manual")
FEEDBACK_SENTIMENTS = ("positive", "negative", "neutral", "mixed")


class ChatFeedbackEvent(Base):
    """A single user feedback submission on a model response.

    One response can have multiple events (e.g. user rates, then later adds a
    comment) — we treat them as an append-only event log. The most recent
    event's rating is mirrored back onto chat_model_responses.rating so the
    existing model-ranking query stays fast.
    """
    __tablename__ = "chat_feedback_events"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    response_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("chat_model_responses.id", ondelete="CASCADE"),
        nullable=False,
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )
    # Optional — a user may submit free-text feedback without touching the
    # star rating, and vice-versa.
    rating: Mapped[int | None] = mapped_column(Integer, nullable=True)
    feedback_text: Mapped[str] = mapped_column(Text, nullable=False, default="")
    # Multi-select qualitative tags. Examples:
    #   accuracy, clarity, depth, relevance, tone, citations, completeness,
    #   helpful, concise, too_long, outdated, off_topic, wrong, biased
    feedback_tags: Mapped[list] = mapped_column(
        JSONB, nullable=False, default=list, server_default=text("'[]'::jsonb"),
    )
    sentiment: Mapped[str] = mapped_column(
        String(20), nullable=False, default="neutral",
        server_default="neutral",
    )
    processed: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=text("false"),
    )
    processed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )
    process_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Distilled memory ids produced by the background extractor. List[str].
    memory_ids_created: Mapped[list] = mapped_column(
        JSONB, nullable=False, default=list, server_default=text("'[]'::jsonb"),
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow,
        server_default=text("now()"),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow,
        server_default=text("now()"), onupdate=utcnow,
    )

    __table_args__ = (
        CheckConstraint(
            "rating IS NULL OR (rating >= 1 AND rating <= 5)",
            name="ck_chat_feedback_rating_range",
        ),
        CheckConstraint(
            "sentiment IN ('positive','negative','neutral','mixed')",
            name="ck_chat_feedback_sentiment",
        ),
        Index("ix_chat_feedback_user", "user_id"),
        Index("ix_chat_feedback_response", "response_id"),
        Index("ix_chat_feedback_processed", "processed"),
        Index("ix_chat_feedback_created_at", "created_at"),
    )


class UserChatMemory(Base):
    """A distilled long-term memory about a specific user.

    memory_key is a stable identifier (e.g. "prefers_concise_answers") used
    for deduplication — when the extractor produces the same key, we upsert
    rather than insert a duplicate row. The extractor should pick short
    snake_case keys that describe the fact in 2-5 words.
    """
    __tablename__ = "user_chat_memories"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )
    memory_type: Mapped[str] = mapped_column(String(30), nullable=False)
    memory_key: Mapped[str] = mapped_column(String(120), nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    # [{"type":"feedback","id":"...","excerpt":"..."},
    #  {"type":"conversation","message_id":"...","excerpt":"..."}]
    evidence: Mapped[list] = mapped_column(
        JSONB, nullable=False, default=list, server_default=text("'[]'::jsonb"),
    )
    confidence_score: Mapped[float] = mapped_column(
        Float, nullable=False, default=0.6, server_default="0.6",
    )
    source_type: Mapped[str] = mapped_column(
        String(30), nullable=False, default="feedback_derived",
        server_default="feedback_derived",
    )
    usage_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0, server_default="0",
    )
    last_used_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )
    is_active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default=text("true"),
    )
    # Pinned memories never decay/get-archived; the user explicitly wants them.
    is_pinned: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False, server_default=text("false"),
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow,
        server_default=text("now()"),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow,
        server_default=text("now()"), onupdate=utcnow,
    )

    __table_args__ = (
        CheckConstraint(
            "memory_type IN ('preference','style','profile','topic_interest',"
            "'domain_knowledge','correction')",
            name="ck_user_memory_type",
        ),
        CheckConstraint(
            "source_type IN ('feedback_derived','conversation_derived','manual')",
            name="ck_user_memory_source",
        ),
        CheckConstraint(
            "confidence_score >= 0 AND confidence_score <= 1",
            name="ck_user_memory_confidence",
        ),
        UniqueConstraint("user_id", "memory_key", name="uq_user_chat_memory_key"),
        Index("ix_user_chat_memory_user_active", "user_id", "is_active"),
        Index("ix_user_chat_memory_user_type", "user_id", "memory_type"),
    )
