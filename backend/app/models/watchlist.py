from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import text, String, Text, Boolean, Integer, Float, DateTime, ForeignKey, Index, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from backend.app.core.database import Base


def utcnow():
    return datetime.now(timezone.utc)


class Watchlist(Base):
    __tablename__ = "watchlists"

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
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_default: Mapped[bool] = mapped_column(
        Boolean, default=False, server_default="false",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=text("now()"),
    )

    # Relationships
    user: Mapped[User] = relationship("User", back_populates="watchlists")
    items: Mapped[list[WatchlistItem]] = relationship(
        "WatchlistItem", back_populates="watchlist", cascade="all, delete-orphan",
    )

    def __repr__(self) -> str:
        return f"<Watchlist {self.name}>"


class WatchlistItem(Base):
    __tablename__ = "watchlist_items"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    watchlist_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("watchlists.id", ondelete="CASCADE"),
        nullable=False,
    )
    item_type: Mapped[str] = mapped_column(String(20), nullable=False)
    value: Mapped[str] = mapped_column(String(100), nullable=False)
    display_name: Mapped[str | None] = mapped_column(String(200), nullable=True)
    metadata_: Mapped[dict] = mapped_column(
        "metadata", JSONB, default=dict, server_default="{}",
    )
    added_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=text("now()"),
    )

    __table_args__ = (
        UniqueConstraint("watchlist_id", "item_type", "value", name="uq_watchlist_item"),
    )

    # Relationships
    watchlist: Mapped[Watchlist] = relationship(
        "Watchlist", back_populates="items",
    )

    def __repr__(self) -> str:
        return f"<WatchlistItem {self.item_type}:{self.value}>"
