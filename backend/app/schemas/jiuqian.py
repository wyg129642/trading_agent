"""Pydantic schemas for Jiuqian data endpoints."""
from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel


# ------------------------------------------------------------------ #
# Forum expert calls
# ------------------------------------------------------------------ #
class ForumBrief(BaseModel):
    id: int
    industry: str | None = None
    related_targets: str | None = None
    title: str
    author: str | None = None
    expert_information: str | None = None
    summary: str | None = None
    meeting_time: datetime | None = None
    enrichment: dict = {}
    is_enriched: bool = False
    model_config = {"from_attributes": True}


class ForumDetail(ForumBrief):
    topic: str | None = None
    content: str = ""
    insight: str | None = None
    create_time: datetime | None = None
    operation_time: datetime | None = None
    synced_at: datetime | None = None


# ------------------------------------------------------------------ #
# Research minutes
# ------------------------------------------------------------------ #
class MinutesBrief(BaseModel):
    id: str
    platform: str | None = None
    source: str | None = None
    pub_time: datetime | None = None
    title: str
    summary: str | None = None
    author: str | None = None
    company: list = []
    enrichment: dict = {}
    is_enriched: bool = False
    model_config = {"from_attributes": True}


class MinutesDetail(MinutesBrief):
    content: str = ""
    synced_at: datetime | None = None


# ------------------------------------------------------------------ #
# WeChat articles
# ------------------------------------------------------------------ #
class WechatBrief(BaseModel):
    id: str
    platform: str | None = None
    source: str | None = None
    district: str | None = None
    pub_time: datetime | None = None
    title: str
    summary: str | None = None
    post_url: str = ""
    enrichment: dict = {}
    is_enriched: bool = False
    model_config = {"from_attributes": True}


class WechatDetail(WechatBrief):
    content: str = ""
    synced_at: datetime | None = None


# ------------------------------------------------------------------ #
# Stats
# ------------------------------------------------------------------ #
class JiuqianStatsResponse(BaseModel):
    forum_total: int = 0
    forum_recent: int = 0
    minutes_total: int = 0
    minutes_recent: int = 0
    wechat_total: int = 0
    wechat_recent: int = 0
    enriched_total: int = 0
    last_sync_at: datetime | None = None
