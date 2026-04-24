"""Knowledge-base folder tree model.

Folders organize the existing MongoDB-backed user_kb documents into a
VS Code-style workspace. Two scopes coexist:

* ``personal`` — user-owned, only the owner can read/write.
* ``public``   — shared across the organization; admin/boss can write,
                 everyone can read.

Three folder types:

* ``stock``    — bound to a single ticker (stock_ticker/market/name).
                 Picked via the header stock-suggest autocomplete.
* ``industry`` — a freely-named industry/sector folder.
* ``general``  — a freely-named catch-all folder.

Documents live in MongoDB; a folder_id field on each Mongo doc links to
``kb_folders.id``. Leaving folder_id NULL puts the doc in the "(unfiled)"
pseudo-folder at the scope root.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    CheckConstraint, DateTime, ForeignKey, Integer, String, text,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from backend.app.core.database import Base


def utcnow():
    return datetime.now(timezone.utc)


class KbFolder(Base):
    __tablename__ = "kb_folders"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    user_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=True,
    )
    scope: Mapped[str] = mapped_column(String(16), nullable=False)
    parent_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("kb_folders.id", ondelete="CASCADE"),
        nullable=True,
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    folder_type: Mapped[str] = mapped_column(String(16), nullable=False)
    stock_ticker: Mapped[str | None] = mapped_column(String(32), nullable=True)
    stock_market: Mapped[str | None] = mapped_column(String(8), nullable=True)
    stock_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    order_index: Mapped[int] = mapped_column(
        Integer, default=0, server_default="0",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=text("now()"),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=text("now()"),
    )

    __table_args__ = (
        CheckConstraint(
            "scope IN ('public','personal')", name="ck_kb_folders_scope",
        ),
        CheckConstraint(
            "folder_type IN ('stock','industry','general')",
            name="ck_kb_folders_type",
        ),
        CheckConstraint(
            "(scope = 'public' AND user_id IS NULL) "
            "OR (scope = 'personal' AND user_id IS NOT NULL)",
            name="ck_kb_folders_scope_user",
        ),
        CheckConstraint(
            "(folder_type = 'stock' AND stock_ticker IS NOT NULL) "
            "OR (folder_type IN ('industry','general') AND stock_ticker IS NULL)",
            name="ck_kb_folders_stock_ticker",
        ),
    )

    parent: Mapped["KbFolder | None"] = relationship(
        "KbFolder", remote_side="KbFolder.id", back_populates="children",
    )
    children: Mapped[list["KbFolder"]] = relationship(
        "KbFolder", back_populates="parent",
        cascade="all, delete-orphan",
        single_parent=True,
    )

    def __repr__(self) -> str:
        return f"<KbFolder {self.scope}:{self.name} ({self.folder_type})>"
