from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import text, String, Text, Boolean, Integer, Float, DateTime, ForeignKey, Index, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from backend.app.core.database import Base


def utcnow():
    return datetime.now(timezone.utc)


class AlertRule(Base):
    __tablename__ = "alert_rules"

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
    rule_type: Mapped[str] = mapped_column(String(50), nullable=False)
    conditions: Mapped[dict] = mapped_column(JSONB, nullable=False)
    channels: Mapped[list] = mapped_column(
        JSONB, default=lambda: ["browser"], server_default='["browser"]',
    )
    is_active: Mapped[bool] = mapped_column(
        Boolean, default=True, server_default="true",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=text("now()"),
    )

    # Relationships
    user: Mapped[User] = relationship("User", back_populates="alert_rules")

    def __repr__(self) -> str:
        return f"<AlertRule {self.name} type={self.rule_type}>"
