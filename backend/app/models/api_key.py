"""API Key model for Open API authentication."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import text, String, Boolean, Integer, DateTime, ForeignKey, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from backend.app.core.database import Base


def utcnow():
    return datetime.now(timezone.utc)


class ApiKey(Base):
    __tablename__ = "api_keys"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    # SHA-256 hash of the actual key (we never store plaintext)
    key_hash: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)
    # Human-readable name, e.g. "Alice's OpenClaw agent"
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    # Optional description
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Who created this key
    created_by: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL"), nullable=True,
    )
    # Rate limit: requests per minute (0 = unlimited)
    rate_limit: Mapped[int] = mapped_column(Integer, default=60, server_default="60")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, server_default="true")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=text("now()"),
    )
    last_used_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )

    def __repr__(self) -> str:
        return f"<ApiKey {self.name}>"
