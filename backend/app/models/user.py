from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import text, String, Text, Boolean, Integer, Float, DateTime, ForeignKey, Index, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from backend.app.core.database import Base


def utcnow():
    return datetime.now(timezone.utc)


class User(Base):
    __tablename__ = "users"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    username: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    display_name: Mapped[str | None] = mapped_column(String(100), nullable=True)
    role: Mapped[str] = mapped_column(String(20), default="trader", server_default="trader")
    language: Mapped[str] = mapped_column(String(5), default="zh", server_default="zh")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, server_default="true")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=text("now()"),
    )
    last_login_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )
    # LLM cost governance — monthly USD cap and per-run hard cap. Null = use defaults
    # from cost_estimation.DEFAULT_MONTHLY_BUDGET_USD. Admins can raise individually.
    llm_budget_usd_monthly: Mapped[float | None] = mapped_column(
        Float, nullable=True,
    )
    llm_run_cap_usd: Mapped[float | None] = mapped_column(
        Float, nullable=True,
    )

    # Relationships
    watchlists: Mapped[list[Watchlist]] = relationship(
        "Watchlist", back_populates="user", cascade="all, delete-orphan",
    )
    sources: Mapped[list[UserSource]] = relationship(
        "UserSource", back_populates="user", cascade="all, delete-orphan",
    )
    alert_rules: Mapped[list[AlertRule]] = relationship(
        "AlertRule", back_populates="user", cascade="all, delete-orphan",
    )
    preferences: Mapped[UserPreference | None] = relationship(
        "UserPreference", back_populates="user", uselist=False, cascade="all, delete-orphan",
    )
    favorites: Mapped[list[UserFavorite]] = relationship(
        "UserFavorite", back_populates="user", cascade="all, delete-orphan",
    )

    def __repr__(self) -> str:
        return f"<User {self.username}>"
