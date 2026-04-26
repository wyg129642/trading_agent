"""Chat audit log — full-fidelity record of each AI-chat request.

A two-table design that mirrors the LangSmith / OpenTelemetry GenAI model:

- ``chat_audit_run``  — one row per user message (one chat request, possibly
  fanned out to N models). Holds rolled-up totals so the list view is fast
  without touching the events table.
- ``chat_audit_event`` — ordered timeline of significant events inside a run
  (LLM calls, tool calls, search queries, webpage reads, KB lookups, final
  responses). Each row carries a JSONB ``payload`` with the full input /
  output for that step so the detail UI can replay exactly what happened.

The ``payload`` JSONB is intentionally schema-flexible: new event types ship
without a migration, matching how Langfuse / Phoenix / Arize structure their
spans. Every event still carries enough indexed columns (``event_type``,
``tool_name``, ``run_id``, ``sequence``) to drive the common queries.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    Boolean, DateTime, ForeignKey, Index, Integer, Numeric, String, Text, text,
)
from sqlalchemy.dialects.postgresql import INET, JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from backend.app.core.database import Base


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class ChatAuditRun(Base):
    """One row per chat request (one user message → N model responses)."""

    __tablename__ = "chat_audit_run"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True,
        server_default=text("gen_random_uuid()"),
    )

    # Correlation
    trace_id: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    user_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    username: Mapped[str] = mapped_column(String(120), nullable=False, server_default="")
    conversation_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("chat_conversations.id", ondelete="SET NULL"),
        nullable=True,
    )
    message_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("chat_messages.id", ondelete="SET NULL"),
        nullable=True,
    )

    # Request shape
    user_content: Mapped[str] = mapped_column(Text, nullable=False, server_default="")
    models_requested: Mapped[list | None] = mapped_column(JSONB, nullable=True)
    mode: Mapped[str] = mapped_column(String(40), nullable=False, server_default="standard")
    web_search_mode: Mapped[str] = mapped_column(String(20), nullable=False, server_default="off")
    feature_flags: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    system_prompt_len: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    history_messages: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    tools_offered: Mapped[list | None] = mapped_column(JSONB, nullable=True)

    # Status + rollups (filled progressively; finalize_run() seals them)
    status: Mapped[str] = mapped_column(String(20), nullable=False, server_default="running")
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    rounds_used: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    tool_calls_total: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    tool_calls_by_name: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    urls_searched: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    urls_read: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    citations_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    total_tokens: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    total_cost_usd: Mapped[float | None] = mapped_column(Numeric(12, 6), nullable=True)
    total_latency_ms: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    final_content_len: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")

    # Optional client metadata
    client_ip: Mapped[str | None] = mapped_column(INET, nullable=True)
    user_agent: Mapped[str | None] = mapped_column(Text, nullable=True)

    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow,
        server_default=text("now()"),
    )
    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )

    events: Mapped[list[ChatAuditEvent]] = relationship(
        "ChatAuditEvent", back_populates="run",
        cascade="all, delete-orphan",
        order_by="ChatAuditEvent.sequence",
    )

    __table_args__ = (
        Index("ix_chat_audit_run_started", "started_at"),
        Index("ix_chat_audit_run_user_started", "user_id", "started_at"),
        Index("ix_chat_audit_run_conv", "conversation_id"),
        Index("ix_chat_audit_run_status", "status"),
    )


class ChatAuditEvent(Base):
    """One ordered event in a run timeline.

    ``sequence`` is monotonically increasing within a run and is the source of
    truth for ordering — wall-clock ``created_at`` interleaves across
    concurrent model fan-out tasks.
    """

    __tablename__ = "chat_audit_event"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("chat_audit_run.id", ondelete="CASCADE"),
        nullable=False,
    )
    trace_id: Mapped[str] = mapped_column(String(64), nullable=False)
    sequence: Mapped[int] = mapped_column(Integer, nullable=False)

    event_type: Mapped[str] = mapped_column(String(40), nullable=False)
    model_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    round_num: Mapped[int | None] = mapped_column(Integer, nullable=True)
    tool_name: Mapped[str | None] = mapped_column(String(60), nullable=True)
    latency_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    payload: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    payload_truncated: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default="false",
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=utcnow,
        server_default=text("now()"),
    )

    run: Mapped[ChatAuditRun] = relationship("ChatAuditRun", back_populates="events")

    __table_args__ = (
        Index("ix_chat_audit_event_run_seq", "run_id", "sequence"),
        Index("ix_chat_audit_event_trace", "trace_id"),
        Index("ix_chat_audit_event_type", "event_type"),
        Index("ix_chat_audit_event_tool", "tool_name"),
        Index("ix_chat_audit_event_created", "created_at"),
    )
