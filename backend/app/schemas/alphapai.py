"""Pydantic schemas for AlphaPai data endpoints."""
from __future__ import annotations

from datetime import datetime, date
from typing import Any

from pydantic import BaseModel


# ------------------------------------------------------------------ #
# Shared
# ------------------------------------------------------------------ #
class PaginatedResponse(BaseModel):
    items: list[Any]
    total: int
    page: int
    page_size: int
    has_next: bool


# ------------------------------------------------------------------ #
# WeChat articles
# ------------------------------------------------------------------ #
class ArticleBrief(BaseModel):
    arc_code: str
    arc_name: str
    author: str | None = None
    publish_time: datetime | None = None
    text_count: int = 0
    url: str = ""
    enrichment: dict = {}
    is_enriched: bool = False
    model_config = {"from_attributes": True}


class ArticleDetail(ArticleBrief):
    content_cached: str = ""
    spider_time: datetime | None = None
    content_html_path: str = ""
    read_duration: str = ""
    is_original: int = 0
    wxacc_code: str | None = None
    research_type: int | None = None
    synced_at: datetime | None = None


# ------------------------------------------------------------------ #
# A-share roadshows
# ------------------------------------------------------------------ #
class RoadshowCNBrief(BaseModel):
    trans_id: str
    roadshow_id: str
    show_title: str
    company: str | None = None
    guest: str | None = None
    stime: datetime | None = None
    word_count: int = 0
    est_reading_time: str = ""
    ind_json: list = []
    trans_source: str = ""
    enrichment: dict = {}
    is_enriched: bool = False
    model_config = {"from_attributes": True}


class RoadshowCNDetail(RoadshowCNBrief):
    content_cached: str = ""
    content_path: str = ""
    is_conference: bool = False
    is_investigation: bool = False
    is_executive: bool = False
    is_buyside: bool = False


# ------------------------------------------------------------------ #
# US roadshows
# ------------------------------------------------------------------ #
class RoadshowUSBrief(BaseModel):
    trans_id: str
    roadshow_id: str
    show_title: str
    company: str | None = None
    stime: datetime | None = None
    word_count: int = 0
    trans_source: str = ""
    quarter_year: str | None = None
    ind_json: list = []
    ai_auxiliary_json: dict = {}
    enrichment: dict = {}
    is_enriched: bool = False
    model_config = {"from_attributes": True}


class RoadshowUSDetail(RoadshowUSBrief):
    content_cached: str = ""
    content_path: str = ""
    guest: str | None = None
    est_reading_time: str = ""
    rec_source: str | None = None
    files_type: str | None = None


# ------------------------------------------------------------------ #
# Analyst comments
# ------------------------------------------------------------------ #
class CommentBrief(BaseModel):
    cmnt_hcode: str
    title: str
    content: str = ""
    psn_name: str | None = None
    team_cname: str | None = None
    inst_cname: str | None = None
    cmnt_date: datetime | None = None
    is_new_fortune: bool = False
    enrichment: dict = {}
    is_enriched: bool = False
    model_config = {"from_attributes": True}


class CommentDetail(CommentBrief):
    src_type: int | None = None
    group_id: str | None = None


# ------------------------------------------------------------------ #
# Digest
# ------------------------------------------------------------------ #
class DigestResponse(BaseModel):
    id: int
    digest_date: date
    content_markdown: str = ""
    stats: dict = {}
    generated_at: datetime | None = None
    model_used: str = ""
    model_config = {"from_attributes": True}


# ------------------------------------------------------------------ #
# Unified feed item (multi-source)
# ------------------------------------------------------------------ #
class FeedItem(BaseModel):
    """A normalized item that can represent any of the 4 source types."""
    source_type: str  # wechat / roadshow_cn / roadshow_us / comment
    pk: str
    title: str
    time: datetime | None = None
    summary: str = ""
    relevance_score: float = 0.0
    tags: list[str] = []
    tickers: list[str] = []
    sectors: list[str] = []
    sentiment: str = ""
    url: str = ""  # original URL for wechat articles
    # source-specific metadata
    author: str | None = None
    company: str | None = None
    institution: str | None = None
    analyst: str | None = None
    is_new_fortune: bool = False
    trans_source: str | None = None


# ------------------------------------------------------------------ #
# Stats / sync status
# ------------------------------------------------------------------ #
class SyncStatusItem(BaseModel):
    api_name: str
    last_sync_time: datetime
    last_sync_count: int
    total_synced: int
    last_error: str | None = None
    model_config = {"from_attributes": True}


class StatsResponse(BaseModel):
    articles_total: int = 0
    articles_today: int = 0
    roadshows_cn_total: int = 0
    roadshows_cn_today: int = 0
    roadshows_us_total: int = 0
    roadshows_us_today: int = 0
    comments_total: int = 0
    comments_today: int = 0
    enriched_total: int = 0
    last_sync_at: datetime | None = None
    sync_status: list[SyncStatusItem] = []
