from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import String, Text, Boolean, Integer, Float, DateTime, ForeignKey, Index, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from backend.app.core.database import Base


def utcnow():
    return datetime.now(timezone.utc)


class TokenUsage(Base):
    __tablename__ = "token_usage"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True,
    )
    model: Mapped[str] = mapped_column(String(100), nullable=False)
    stage: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    prompt_tokens: Mapped[int] = mapped_column(Integer, nullable=False)
    completion_tokens: Mapped[int] = mapped_column(Integer, nullable=False)
    estimated_prompt: Mapped[int] = mapped_column(
        Integer, default=0, server_default="0",
    )
    source_name: Mapped[str] = mapped_column(
        String(200), default="", server_default="",
    )
    duration_ms: Mapped[int] = mapped_column(
        Integer, default=0, server_default="0",
    )
    cost_cny: Mapped[float] = mapped_column(
        Float, default=0.0, server_default="0.0",
    )

    def __repr__(self) -> str:
        return f"<TokenUsage {self.model} stage={self.stage}>"
