"""Jiuqian (久谦) data models — Forum expert calls, research minutes, WeChat articles."""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import (
    String, Text, Boolean, Integer, Float, DateTime, Date, Index, BigInteger,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from backend.app.core.database import Base


# --------------------------------------------------------------------------- #
# Forum expert calls (高价值专家访谈, ~50条)
# --------------------------------------------------------------------------- #
class JiuqianForum(Base):
    __tablename__ = "jiuqian_forum"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    industry: Mapped[str | None] = mapped_column(String(100), nullable=True)
    related_targets: Mapped[str | None] = mapped_column(Text, nullable=True)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    author: Mapped[str | None] = mapped_column(String(200), nullable=True)
    expert_information: Mapped[str | None] = mapped_column(Text, nullable=True)
    topic: Mapped[str | None] = mapped_column(Text, nullable=True)
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    content: Mapped[str] = mapped_column(Text, default="", server_default="")
    insight: Mapped[str | None] = mapped_column(Text, nullable=True)
    create_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    meeting_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    operation_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # LLM enrichment
    enrichment: Mapped[dict] = mapped_column(JSONB, default=dict, server_default="{}")
    is_enriched: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    synced_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )

    __table_args__ = (
        Index("ix_jq_forum_meeting", "meeting_time"),
        Index("ix_jq_forum_enriched", "is_enriched"),
        Index("ix_jq_forum_industry", "industry"),
    )

    def __repr__(self) -> str:
        return f"<JiuqianForum {self.id} {self.title[:40]}>"


# --------------------------------------------------------------------------- #
# Research minutes (纪要, ~16000条)
# --------------------------------------------------------------------------- #
class JiuqianMinutes(Base):
    __tablename__ = "jiuqian_minutes"

    id: Mapped[str] = mapped_column(String(100), primary_key=True)
    platform: Mapped[str | None] = mapped_column(String(50), nullable=True)
    source: Mapped[str | None] = mapped_column(String(200), nullable=True)
    pub_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    content: Mapped[str] = mapped_column(Text, default="", server_default="")
    author: Mapped[str | None] = mapped_column(String(200), nullable=True)
    company: Mapped[list] = mapped_column(JSONB, default=list, server_default="[]")
    # LLM enrichment
    enrichment: Mapped[dict] = mapped_column(JSONB, default=dict, server_default="{}")
    is_enriched: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    synced_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )

    __table_args__ = (
        Index("ix_jq_min_pub", "pub_time"),
        Index("ix_jq_min_enriched", "is_enriched"),
        Index("ix_jq_min_source", "source"),
    )

    def __repr__(self) -> str:
        return f"<JiuqianMinutes {self.id} {self.title[:40]}>"


# --------------------------------------------------------------------------- #
# WeChat articles (公众号, ~25000条)
# --------------------------------------------------------------------------- #
class JiuqianWechat(Base):
    __tablename__ = "jiuqian_wechat"

    id: Mapped[str] = mapped_column(String(100), primary_key=True)
    platform: Mapped[str | None] = mapped_column(String(50), nullable=True)
    source: Mapped[str | None] = mapped_column(String(200), nullable=True)
    district: Mapped[str | None] = mapped_column(String(50), nullable=True)
    pub_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    content: Mapped[str] = mapped_column(Text, default="", server_default="")
    post_url: Mapped[str] = mapped_column(Text, default="", server_default="")
    # LLM enrichment
    enrichment: Mapped[dict] = mapped_column(JSONB, default=dict, server_default="{}")
    is_enriched: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    synced_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )

    __table_args__ = (
        Index("ix_jq_wx_pub", "pub_time"),
        Index("ix_jq_wx_enriched", "is_enriched"),
        Index("ix_jq_wx_source", "source"),
    )

    def __repr__(self) -> str:
        return f"<JiuqianWechat {self.id} {self.title[:40]}>"


# --------------------------------------------------------------------------- #
# Sync state — tracks last processed position per data source
# --------------------------------------------------------------------------- #
class JiuqianSyncState(Base):
    __tablename__ = "jiuqian_sync_state"

    source_name: Mapped[str] = mapped_column(String(50), primary_key=True)
    last_processed_ids: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    last_sync_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )
    total_synced: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
