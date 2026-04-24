"""Database models for AI Chat feature."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    String, Text, Boolean, Integer, Float, DateTime, ForeignKey, Index,
    text,
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from backend.app.core.database import Base


def utcnow():
    return datetime.now(timezone.utc)


class ChatConversation(Base):
    """A chat conversation (session) belonging to a user."""
    __tablename__ = "chat_conversations"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False,
    )
    title: Mapped[str] = mapped_column(String(200), default="新对话")
    tags: Mapped[dict | None] = mapped_column(JSONB, default=list)
    is_pinned: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=text("now()"),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=text("now()"),
        onupdate=utcnow,
    )

    messages: Mapped[list[ChatMessage]] = relationship(
        "ChatMessage", back_populates="conversation",
        cascade="all, delete-orphan", order_by="ChatMessage.created_at",
    )

    __table_args__ = (
        Index("ix_chat_conv_user", "user_id"),
        Index("ix_chat_conv_updated", "updated_at"),
    )


class ChatMessage(Base):
    """A single message in a conversation (user or system prompt)."""
    __tablename__ = "chat_messages"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    conversation_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("chat_conversations.id", ondelete="CASCADE"),
        nullable=False,
    )
    role: Mapped[str] = mapped_column(String(20), nullable=False)  # user / system
    content: Mapped[str] = mapped_column(Text, default="")
    attachments: Mapped[dict | None] = mapped_column(JSONB, default=list)
    is_debate: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    # Set by the chat-memory background daemon after passive conversation
    # scan. NULL = not yet processed; non-NULL = skip on next tick.
    memory_processed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=text("now()"),
    )

    conversation: Mapped[ChatConversation] = relationship(
        "ChatConversation", back_populates="messages",
    )
    model_responses: Mapped[list[ChatModelResponse]] = relationship(
        "ChatModelResponse", back_populates="message",
        cascade="all, delete-orphan", order_by="ChatModelResponse.created_at",
    )

    __table_args__ = (
        Index("ix_chat_msg_conv", "conversation_id"),
        Index("ix_chat_msg_memory_processed", "memory_processed_at"),
    )


class ChatModelResponse(Base):
    """Response from a specific LLM model for a user message."""
    __tablename__ = "chat_model_responses"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    message_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("chat_messages.id", ondelete="CASCADE"),
        nullable=False,
    )
    model_id: Mapped[str] = mapped_column(String(100), nullable=False)
    model_name: Mapped[str] = mapped_column(String(100), nullable=False)
    content: Mapped[str] = mapped_column(Text, default="")
    tokens_used: Mapped[int | None] = mapped_column(Integer, nullable=True)
    latency_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    rating: Mapped[int | None] = mapped_column(Integer, nullable=True)
    rating_comment: Mapped[str | None] = mapped_column(Text, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    sources: Mapped[list | None] = mapped_column(JSONB, nullable=True)
    debate_round: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=text("now()"),
    )

    message: Mapped[ChatMessage] = relationship(
        "ChatMessage", back_populates="model_responses",
    )

    __table_args__ = (
        Index("ix_chat_resp_msg", "message_id"),
        Index("ix_chat_resp_model", "model_id"),
        Index("ix_chat_resp_rating", "rating"),
    )


class ChatPromptTemplate(Base):
    """Reusable prompt templates (system-wide or per-user)."""
    __tablename__ = "chat_prompt_templates"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    user_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=True,
    )
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    category: Mapped[str] = mapped_column(
        String(50), default="general",
    )  # general, fundamental, technical, news, macro, industry
    is_system: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    usage_count: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=text("now()"),
    )

    __table_args__ = (
        Index("ix_chat_tpl_user", "user_id"),
    )


class ChatTrackingTopic(Base):
    """User-defined tracking topic for periodic news monitoring."""
    __tablename__ = "chat_tracking_topics"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False,
    )
    topic: Mapped[str] = mapped_column(Text, nullable=False)
    keywords: Mapped[list] = mapped_column(JSONB, default=list, server_default="[]")
    related_tickers: Mapped[list] = mapped_column(JSONB, default=list, server_default="[]")
    related_sectors: Mapped[list] = mapped_column(JSONB, default=list, server_default="[]")
    notify_channels: Mapped[list] = mapped_column(
        JSONB, default=lambda: ["browser"], server_default='["browser"]',
    )
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, server_default="true")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=text("now()"),
    )
    last_checked_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )
    last_triggered_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )

    alerts: Mapped[list[ChatTrackingAlert]] = relationship(
        "ChatTrackingAlert", back_populates="topic",
        cascade="all, delete-orphan", order_by="ChatTrackingAlert.created_at.desc()",
    )

    __table_args__ = (
        Index("ix_tracking_user", "user_id"),
        Index("ix_tracking_active", "is_active"),
    )


class ChatTrackingAlert(Base):
    """Alert triggered by a tracking topic match."""
    __tablename__ = "chat_tracking_alerts"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    topic_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("chat_tracking_topics.id", ondelete="CASCADE"),
        nullable=False,
    )
    news_item_id: Mapped[str] = mapped_column(
        Text, ForeignKey("news_items.id", ondelete="CASCADE"), nullable=False,
    )
    match_score: Mapped[float] = mapped_column(Float, nullable=False)
    match_reason: Mapped[str] = mapped_column(Text, default="")
    is_read: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=text("now()"),
    )

    topic: Mapped[ChatTrackingTopic] = relationship(
        "ChatTrackingTopic", back_populates="alerts",
    )

    __table_args__ = (
        Index("ix_tracking_alert_topic", "topic_id"),
        Index("ix_tracking_alert_unread", "is_read"),
    )


class ChatRecommendedQuestion(Base):
    """Per-user LLM-generated quick-start question suggestions, refreshed daily."""
    __tablename__ = "chat_recommended_questions"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False, unique=True,
    )
    questions: Mapped[list] = mapped_column(JSONB, default=list, server_default="[]")
    source_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    generated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=text("now()"),
    )

    __table_args__ = (
        Index("ix_chat_rec_q_user", "user_id"),
        Index("ix_chat_rec_q_generated", "generated_at"),
    )
