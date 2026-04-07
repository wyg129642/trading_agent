"""AlphaPai data models — WeChat articles, roadshows (CN/US), analyst comments, sync state, digests."""
from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import (
    String, Text, Boolean, Integer, Float, DateTime, Date, Index,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from backend.app.core.database import Base


# --------------------------------------------------------------------------- #
# WeChat articles  (~4000-5000 / day)
# --------------------------------------------------------------------------- #
class AlphaPaiArticle(Base):
    __tablename__ = "alphapai_articles"

    arc_code: Mapped[str] = mapped_column(String(100), primary_key=True)
    arc_name: Mapped[str] = mapped_column(Text, nullable=False)
    author: Mapped[str | None] = mapped_column(String(200), nullable=True)
    publish_time: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )
    spider_time: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )
    text_count: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    read_duration: Mapped[str] = mapped_column(String(20), default="", server_default="")
    is_original: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    url: Mapped[str] = mapped_column(Text, default="", server_default="")
    content_html_path: Mapped[str] = mapped_column(Text, default="", server_default="")
    content_cached: Mapped[str] = mapped_column(Text, default="", server_default="")
    wxacc_code: Mapped[str | None] = mapped_column(String(100), nullable=True)
    research_type: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # LLM enrichment  (JSONB blob: summary, tags, tickers, sectors, sentiment, relevance_score)
    enrichment: Mapped[dict] = mapped_column(JSONB, default=dict, server_default="{}")
    is_enriched: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    synced_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )

    __table_args__ = (
        Index("ix_ap_art_publish", "publish_time"),
        Index("ix_ap_art_enriched", "is_enriched"),
    )

    def __repr__(self) -> str:
        return f"<AlphaPaiArticle {self.arc_code} {self.arc_name[:40]}>"


# --------------------------------------------------------------------------- #
# A-share roadshow transcripts  (~200-250 / day, MT + AI paired)
# --------------------------------------------------------------------------- #
class AlphaPaiRoadshowCN(Base):
    __tablename__ = "alphapai_roadshows_cn"

    trans_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    roadshow_id: Mapped[str] = mapped_column(String(100), nullable=False)
    show_title: Mapped[str] = mapped_column(Text, nullable=False)
    company: Mapped[str | None] = mapped_column(String(200), nullable=True)
    guest: Mapped[str | None] = mapped_column(Text, nullable=True)
    stime: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )
    word_count: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    est_reading_time: Mapped[str] = mapped_column(String(20), default="", server_default="")
    ind_json: Mapped[list] = mapped_column(JSONB, default=list, server_default="[]")
    trans_source: Mapped[str] = mapped_column(String(10), nullable=False)  # MT / AI
    content_path: Mapped[str] = mapped_column(Text, default="", server_default="")
    content_cached: Mapped[str] = mapped_column(Text, default="", server_default="")
    is_conference: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    is_investigation: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    is_executive: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    is_buyside: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    # LLM enrichment
    enrichment: Mapped[dict] = mapped_column(JSONB, default=dict, server_default="{}")
    is_enriched: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    synced_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )

    __table_args__ = (
        Index("ix_ap_rs_cn_stime", "stime"),
        Index("ix_ap_rs_cn_roadshow", "roadshow_id"),
        Index("ix_ap_rs_cn_company", "company"),
        Index("ix_ap_rs_cn_enriched", "is_enriched"),
    )

    def __repr__(self) -> str:
        return f"<RoadshowCN [{self.trans_source}] {self.show_title[:40]}>"


# --------------------------------------------------------------------------- #
# US roadshow transcripts  (~22-120 / day)
# --------------------------------------------------------------------------- #
class AlphaPaiRoadshowUS(Base):
    __tablename__ = "alphapai_roadshows_us"

    trans_id: Mapped[str] = mapped_column(String(100), primary_key=True)
    roadshow_id: Mapped[str] = mapped_column(String(100), nullable=False)
    show_title: Mapped[str] = mapped_column(Text, nullable=False)
    company: Mapped[str | None] = mapped_column(String(200), nullable=True)
    guest: Mapped[str | None] = mapped_column(Text, nullable=True)
    stime: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )
    word_count: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    est_reading_time: Mapped[str] = mapped_column(String(20), default="", server_default="")
    ind_json: Mapped[list] = mapped_column(JSONB, default=list, server_default="[]")
    trans_source: Mapped[str] = mapped_column(String(10), nullable=False)
    content_path: Mapped[str] = mapped_column(Text, default="", server_default="")
    content_cached: Mapped[str] = mapped_column(Text, default="", server_default="")
    rec_source: Mapped[str | None] = mapped_column(String(100), nullable=True)
    quarter_year: Mapped[str | None] = mapped_column(String(20), nullable=True)
    files_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    ai_auxiliary_json: Mapped[dict] = mapped_column(JSONB, default=dict, server_default="{}")
    # LLM enrichment
    enrichment: Mapped[dict] = mapped_column(JSONB, default=dict, server_default="{}")
    is_enriched: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    synced_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )

    __table_args__ = (
        Index("ix_ap_rs_us_stime", "stime"),
        Index("ix_ap_rs_us_roadshow", "roadshow_id"),
        Index("ix_ap_rs_us_enriched", "is_enriched"),
    )

    def __repr__(self) -> str:
        return f"<RoadshowUS [{self.trans_source}] {self.show_title[:40]}>"


# --------------------------------------------------------------------------- #
# Analyst comments  (~350-400 / day)
# --------------------------------------------------------------------------- #
class AlphaPaiComment(Base):
    __tablename__ = "alphapai_comments"

    cmnt_hcode: Mapped[str] = mapped_column(String(100), primary_key=True)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    content: Mapped[str] = mapped_column(Text, default="", server_default="")
    psn_name: Mapped[str | None] = mapped_column(String(200), nullable=True)
    team_cname: Mapped[str | None] = mapped_column(String(200), nullable=True)
    inst_cname: Mapped[str | None] = mapped_column(String(200), nullable=True)
    cmnt_date: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True,
    )
    is_new_fortune: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    src_type: Mapped[int | None] = mapped_column(Integer, nullable=True)
    group_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    # LLM enrichment
    enrichment: Mapped[dict] = mapped_column(JSONB, default=dict, server_default="{}")
    is_enriched: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    synced_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )

    __table_args__ = (
        Index("ix_ap_cmt_date", "cmnt_date"),
        Index("ix_ap_cmt_inst", "inst_cname"),
        Index("ix_ap_cmt_enriched", "is_enriched"),
    )

    def __repr__(self) -> str:
        return f"<AlphaPaiComment {self.cmnt_hcode} {self.title[:40]}>"


# --------------------------------------------------------------------------- #
# Sync watermark — one row per API
# --------------------------------------------------------------------------- #
class AlphaPaiSyncState(Base):
    __tablename__ = "alphapai_sync_state"

    api_name: Mapped[str] = mapped_column(String(100), primary_key=True)
    last_sync_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False,
    )
    last_sync_count: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    total_synced: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )


# --------------------------------------------------------------------------- #
# Daily digest  (LLM-generated morning brief)
# --------------------------------------------------------------------------- #
class AlphaPaiDigest(Base):
    __tablename__ = "alphapai_digests"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    digest_date: Mapped[datetime] = mapped_column(
        Date, nullable=False, unique=True,
    )
    content_markdown: Mapped[str] = mapped_column(Text, default="", server_default="")
    stats: Mapped[dict] = mapped_column(JSONB, default=dict, server_default="{}")
    generated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(),
    )
    model_used: Mapped[str] = mapped_column(String(100), default="", server_default="")
