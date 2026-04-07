"""Pydantic schemas for AI Chat feature."""
from __future__ import annotations

from datetime import datetime
from pydantic import BaseModel, Field


# ── Models ──────────────────────────────────────────────────────

class ModelInfo(BaseModel):
    id: str
    name: str
    provider: str
    supports_vision: bool = False
    supports_thinking: bool = False
    description: str = ""


# ── Conversations ───────────────────────────────────────────────

class ConversationCreate(BaseModel):
    title: str = "新对话"
    tags: list[str] = []

class ConversationUpdate(BaseModel):
    title: str | None = None
    tags: list[str] | None = None
    is_pinned: bool | None = None

class ConversationResponse(BaseModel):
    id: str
    title: str
    tags: list[str]
    is_pinned: bool
    created_at: datetime
    updated_at: datetime
    message_count: int = 0
    last_message_preview: str = ""

class ConversationListResponse(BaseModel):
    conversations: list[ConversationResponse]
    total: int


# ── Messages ────────────────────────────────────────────────────

class ModelResponseData(BaseModel):
    id: str
    model_id: str
    model_name: str
    content: str
    tokens_used: int | None = None
    latency_ms: int | None = None
    rating: int | None = None
    rating_comment: str | None = None
    error: str | None = None
    debate_round: int | None = None
    created_at: datetime

class MessageResponse(BaseModel):
    id: str
    role: str
    content: str
    attachments: list[dict] = []
    is_debate: bool = False
    model_responses: list[ModelResponseData] = []
    created_at: datetime

class ConversationDetailResponse(BaseModel):
    id: str
    title: str
    tags: list[str]
    is_pinned: bool
    created_at: datetime
    updated_at: datetime
    messages: list[MessageResponse]


# ── Send message ────────────────────────────────────────────────

class SendMessageRequest(BaseModel):
    content: str = Field(..., min_length=1, max_length=50000)
    models: list[str] = Field(..., min_items=1, max_items=6)
    attachments: list[dict] = []
    system_prompt: str | None = None
    mode: str = "standard"  # standard | thinking | fast
    web_search: bool = False  # enable web search for real-time info

class SendMessageResponse(BaseModel):
    message_id: str
    model_responses: list[ModelResponseData]


# ── Rating ──────────────────────────────────────────────────────

class RateRequest(BaseModel):
    rating: int = Field(..., ge=1, le=5)
    comment: str | None = None

class RateResponse(BaseModel):
    id: str
    rating: int
    rating_comment: str | None


# ── Prompt templates ────────────────────────────────────────────

class TemplateCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    content: str = Field(..., min_length=1, max_length=10000)
    category: str = "general"

class TemplateUpdate(BaseModel):
    name: str | None = None
    content: str | None = None
    category: str | None = None

class TemplateResponse(BaseModel):
    id: str
    name: str
    content: str
    category: str
    is_system: bool
    usage_count: int
    created_at: datetime


# ── Model rankings ──────────────────────────────────────────────

class ModelRanking(BaseModel):
    model_id: str
    model_name: str
    avg_rating: float
    total_ratings: int
    total_uses: int

class ModelRankingResponse(BaseModel):
    rankings: list[ModelRanking]


# ── Chat statistics (admin) ─────────────────────────────────────

class ChatStatsResponse(BaseModel):
    total_conversations: int
    total_messages: int
    total_model_calls: int
    active_users: int
    top_models: list[dict]
    daily_usage: list[dict]


# ── File upload ─────────────────────────────────────────────────

class FileUploadResponse(BaseModel):
    filename: str
    file_type: str
    file_url: str
    file_path: str | None = None  # server-side path for native PDF/image upload to LLM


# ── Export ──────────────────────────────────────────────────────

class ExportResponse(BaseModel):
    markdown: str
    title: str


# ── Debate mode ────────────────────────────────────────────────

class DebateRequest(BaseModel):
    content: str = Field(..., min_length=1, max_length=50000)
    debate_models: list[str] = Field(..., min_length=2, max_length=3)
    attachments: list[dict] = []
    system_prompt: str | None = None


# ── Tracking topics ────────────────────────────────────────────

class TrackingTopicCreate(BaseModel):
    topic: str = Field(..., min_length=2, max_length=500)
    keywords: list[str] = []
    related_tickers: list[str] = []
    related_sectors: list[str] = []
    notify_channels: list[str] = ["browser"]
    auto_extract: bool = True

class TrackingTopicUpdate(BaseModel):
    is_active: bool | None = None
    notify_channels: list[str] | None = None
    keywords: list[str] | None = None
    related_tickers: list[str] | None = None
    related_sectors: list[str] | None = None

class TrackingTopicResponse(BaseModel):
    id: str
    topic: str
    keywords: list[str]
    related_tickers: list[str]
    related_sectors: list[str]
    notify_channels: list[str]
    is_active: bool
    created_at: datetime
    last_checked_at: datetime | None = None
    last_triggered_at: datetime | None = None
    unread_count: int = 0

class TrackingAlertResponse(BaseModel):
    id: str
    topic_id: str
    news_title: str = ""
    news_summary: str = ""
    match_score: float
    match_reason: str = ""
    is_read: bool
    created_at: datetime
