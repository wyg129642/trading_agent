"""Knowledge-base skill templates.

A *skill* is a reusable bundle of folders + files (markdown + workbook)
that a user can install into any target folder. Think Notion/Obsidian
templates scoped to this app's workspace.

Spec shape (stored in the ``spec`` JSONB column):

.. code-block:: json

    {
      "files": [
        {"path": "估值表.xlsx", "kind": "workbook", "template": "dcf_standard"},
        {"path": "研报模板.md", "kind": "markdown",
         "content": "# {{stock_name}} 研报\\n"}
      ],
      "folders": ["研报", "公司公告"],
      "variables": ["stock_name", "ticker", "today"]
    }

Interpolation keys are whitelisted server-side for safety; no HTML/JS eval.

Three scopes:

* ``system`` — shipped with the app, seeded at startup, not editable by users.
* ``public`` — org-wide, admin/boss can create/delete.
* ``personal`` — per-user; only the owner can edit/delete.

``slug`` is a stable external identifier used for idempotent system seeding.
Nullable for user-authored skills, always set for system ones.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    Boolean, CheckConstraint, DateTime, ForeignKey, Integer, String, Text, text,
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from backend.app.core.database import Base


def utcnow():
    return datetime.now(timezone.utc)


class KbSkillTemplate(Base):
    __tablename__ = "kb_skill_templates"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        server_default=text("gen_random_uuid()"),
    )
    owner_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=True,
    )
    scope: Mapped[str] = mapped_column(
        String(16), nullable=False, server_default="personal",
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str] = mapped_column(
        Text, nullable=False, server_default="",
    )
    icon: Mapped[str] = mapped_column(
        String(64), nullable=False, server_default="ThunderboltOutlined",
    )
    # CSV of folder types — e.g. "stock,general". Enforced at service layer.
    target_types: Mapped[str] = mapped_column(
        String(128), nullable=False, server_default="stock,industry,general",
    )
    slug: Mapped[str | None] = mapped_column(String(64), nullable=True)
    spec: Mapped[dict] = mapped_column(
        JSONB, nullable=False, default=dict,
        server_default=text("'{}'::jsonb"),
    )
    is_published: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("true"),
    )
    installs_count: Mapped[int] = mapped_column(
        Integer, nullable=False, server_default="0",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=text("now()"),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=text("now()"),
    )

    __table_args__ = (
        CheckConstraint(
            "scope IN ('system','public','personal')",
            name="ck_kb_skills_scope",
        ),
        CheckConstraint(
            "(scope = 'personal' AND owner_id IS NOT NULL) "
            "OR (scope IN ('system','public') AND owner_id IS NULL)",
            name="ck_kb_skills_scope_owner",
        ),
    )

    def __repr__(self) -> str:
        return f"<KbSkillTemplate {self.scope}:{self.name} slug={self.slug}>"
