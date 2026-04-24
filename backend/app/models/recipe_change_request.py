"""RecipeChangeRequest — fork → propose → review → merge workflow for canonical recipes.

When a researcher forks a canonical recipe and edits it, they can submit a
``RecipeChangeRequest`` (≈ pull request) back to the canonical. A reviewer
(boss/admin) compares the two graphs, leaves a review note, and either
approves (merge into canonical, bump version) or rejects.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    CheckConstraint, DateTime, ForeignKey, Index, String, Text, text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from backend.app.core.database import Base


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class RecipeChangeRequest(Base):
    """One proposed change from a forked recipe back to its canonical parent."""
    __tablename__ = "recipe_change_requests"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    # The canonical recipe being changed
    canonical_recipe_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("recipes.id", ondelete="CASCADE"),
        nullable=False,
    )
    # The fork (child) containing the proposed graph
    fork_recipe_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("recipes.id", ondelete="CASCADE"),
        nullable=False,
    )
    title: Mapped[str] = mapped_column(String(200), nullable=False, default="")
    description: Mapped[str] = mapped_column(Text, nullable=False, default="")
    requested_by: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    # open | approved | rejected | merged | withdrawn
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="open")
    # Diff computed at submission time so we don't have to recompute every read
    graph_diff: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    review_note: Mapped[str] = mapped_column(Text, nullable=False, default="")
    reviewed_by: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    reviewed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=text("now()"),
    )

    __table_args__ = (
        Index("ix_recipe_cr_canonical", "canonical_recipe_id"),
        Index("ix_recipe_cr_status", "status"),
        CheckConstraint(
            "status IN ('open','approved','rejected','merged','withdrawn')",
            name="ck_recipe_cr_status",
        ),
    )
