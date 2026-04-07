from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import text, String, Text, Boolean, Integer, Float, DateTime, ForeignKey, Index, UniqueConstraint, PrimaryKeyConstraint
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from backend.app.core.database import Base


def utcnow():
    return datetime.now(timezone.utc)


class UserPreference(Base):
    __tablename__ = "user_preferences"

    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        primary_key=True,
    )
    feed_columns: Mapped[list] = mapped_column(
        JSONB,
        default=lambda: ["all", "watchlist", "critical"],
        server_default='["all", "watchlist", "critical"]',
    )
    digest_frequency: Mapped[str] = mapped_column(
        String(20), default="hourly", server_default="hourly",
    )
    alert_schedule: Mapped[dict] = mapped_column(
        JSONB,
        default=lambda: {"start": "09:00", "end": "18:00", "timezone": "Asia/Shanghai"},
        server_default='{"start": "09:00", "end": "18:00", "timezone": "Asia/Shanghai"}',
    )
    theme: Mapped[str] = mapped_column(
        String(20), default="light", server_default="light",
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=text("now()"),
    )

    # Relationships
    user: Mapped[User] = relationship("User", back_populates="preferences")

    def __repr__(self) -> str:
        return f"<UserPreference user_id={self.user_id}>"


class UserNewsRead(Base):
    __tablename__ = "user_news_read"

    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )
    news_item_id: Mapped[str] = mapped_column(Text, nullable=False)
    read_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=text("now()"),
    )

    __table_args__ = (
        PrimaryKeyConstraint("user_id", "news_item_id"),
    )

    def __repr__(self) -> str:
        return f"<UserNewsRead user={self.user_id} news={self.news_item_id}>"


class UserFavorite(Base):
    __tablename__ = "user_favorites"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )
    item_type: Mapped[str] = mapped_column(String(30), nullable=False)
    item_id: Mapped[str] = mapped_column(Text, nullable=False)
    note: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=text("now()"),
    )

    __table_args__ = (
        UniqueConstraint("user_id", "item_type", "item_id", name="uq_user_favorites"),
        Index("ix_user_favorites_user", "user_id"),
    )

    # Relationships
    user: Mapped[User] = relationship("User", back_populates="favorites")

    def __repr__(self) -> str:
        return f"<UserFavorite id={self.id} user={self.user_id} type={self.item_type} item={self.item_id}>"
