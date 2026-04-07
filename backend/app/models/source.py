from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import text, String, Text, Boolean, Integer, Float, DateTime, ForeignKey, Index, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from backend.app.core.database import Base


def utcnow():
    return datetime.now(timezone.utc)


class UserSource(Base):
    __tablename__ = "user_sources"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    url: Mapped[str] = mapped_column(String(2048), nullable=False)
    source_type: Mapped[str] = mapped_column(
        String(20), default="rss", server_default="rss",
    )
    priority: Mapped[str] = mapped_column(
        String(5), default="p1", server_default="p1",
    )
    category: Mapped[str] = mapped_column(
        String(50), default="", server_default="",
    )
    config: Mapped[dict] = mapped_column(
        JSONB, default=dict, server_default="{}",
    )
    is_active: Mapped[bool] = mapped_column(
        Boolean, default=True, server_default="true",
    )
    # Stock subscription fields
    stock_market: Mapped[str | None] = mapped_column(
        String(20), nullable=True,
    )  # e.g. "US", "A", "HK", "KR", "JP"
    stock_ticker: Mapped[str | None] = mapped_column(
        String(20), nullable=True,
    )  # e.g. "NVDA", "300394", "06869"
    stock_name: Mapped[str | None] = mapped_column(
        String(100), nullable=True,
    )  # Display name, e.g. "英伟达", "天孚通信"
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=text("now()"),
    )

    # Relationships
    user: Mapped[User] = relationship("User", back_populates="sources")

    def __repr__(self) -> str:
        return f"<UserSource {self.name}>"


class SourceHealth(Base):
    __tablename__ = "source_health"

    source_name: Mapped[str] = mapped_column(String(200), primary_key=True)
    last_success: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )
    last_failure: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )
    consecutive_failures: Mapped[int] = mapped_column(
        Integer, default=0, server_default="0",
    )
    total_items_fetched: Mapped[int] = mapped_column(
        Integer, default=0, server_default="0",
    )
    is_healthy: Mapped[bool] = mapped_column(
        Boolean, default=True, server_default="true",
    )

    def __repr__(self) -> str:
        return f"<SourceHealth {self.source_name} healthy={self.is_healthy}>"
