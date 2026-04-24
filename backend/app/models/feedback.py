"""Feedback + Lessons domain — captures user edits so the system can self-evolve."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    Boolean, DateTime, ForeignKey, Index, Integer, String, Text,
    text, CheckConstraint,
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from backend.app.core.database import Base


def utcnow():
    return datetime.now(timezone.utc)


class UserFeedbackEvent(Base):
    """Every signal from the user that matters for evolution.

    Types:
      cell_edit, cell_rating, source_type_change, recipe_prompt_edit,
      note_added, citation_added, sanity_override
    """
    __tablename__ = "user_feedback_events"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )
    event_type: Mapped[str] = mapped_column(String(40), nullable=False)
    model_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    cell_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    recipe_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    industry: Mapped[str | None] = mapped_column(String(80), nullable=True)
    cell_path: Mapped[str | None] = mapped_column(String(500), nullable=True)
    # Full before/after snapshot + context
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=text("now()"),
    )
    # Whether this feedback has been consumed by the weekly consolidator
    consumed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    __table_args__ = (
        Index("ix_feedback_user", "user_id"),
        Index("ix_feedback_industry", "industry"),
        Index("ix_feedback_event_type", "event_type"),
        Index("ix_feedback_consumed", "consumed"),
        Index("ix_feedback_created_at", "created_at"),
    )


class PendingLesson(Base):
    """Lesson distilled by the weekly consolidator awaiting admin/boss approval."""
    __tablename__ = "pending_lessons"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    industry: Mapped[str] = mapped_column(String(80), nullable=False)
    # Stable identifier like "L-2026-04-23-017"
    lesson_id: Mapped[str] = mapped_column(String(60), nullable=False, unique=True)
    title: Mapped[str] = mapped_column(String(200), nullable=False)
    body: Mapped[str] = mapped_column(Text, nullable=False)
    scenario: Mapped[str] = mapped_column(Text, nullable=False, default="")
    observation: Mapped[str] = mapped_column(Text, nullable=False, default="")
    rule: Mapped[str] = mapped_column(Text, nullable=False, default="")
    # List of user_feedback_event ids that triggered this lesson
    sources: Mapped[list[dict]] = mapped_column(JSONB, nullable=False, default=list)
    # pending / approved / rejected / archived
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")
    reviewed_by: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL"), nullable=True,
    )
    review_note: Mapped[str] = mapped_column(Text, nullable=False, default="")
    # Week start date (YYYY-MM-DD) this batch belongs to
    batch_week: Mapped[str] = mapped_column(String(20), nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=text("now()"),
    )
    reviewed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )

    __table_args__ = (
        Index("ix_pending_lessons_status", "status"),
        Index("ix_pending_lessons_industry", "industry"),
        Index("ix_pending_lessons_batch_week", "batch_week"),
        CheckConstraint(
            "status IN ('pending','approved','rejected','archived')",
            name="ck_pending_lessons_status",
        ),
    )
