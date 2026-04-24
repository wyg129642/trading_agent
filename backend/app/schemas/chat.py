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
    sources: list[dict] | None = None
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
    web_search: str = "auto"  # "on" = always search, "off" = never, "auto" = LLM decides
    alphapai_enabled: bool = True  # enable Alpha派投研工具
    jinmen_enabled: bool = True  # enable 进门财经投研数据
    kb_enabled: bool = True  # enable 内部知识库 (7个来源: alphapai+jinmen+meritco+thirdbridge+funda+gangtise+acecamp)
    user_kb_enabled: bool = False  # enable 用户个人知识库 (用户上传的 PDF/文本/Markdown 等)
    # Files dragged from the personal knowledge base workspace into the chat
    # input. Each id points at a user_kb document the caller can read (own
    # personal or any public doc). Content is inlined as a "Reference
    # Documents" prefix on the user message before the LLM sees it.
    kb_document_ids: list[str] = []

class SendMessageResponse(BaseModel):
    message_id: str
    model_responses: list[ModelResponseData]


class RegenerateRequest(BaseModel):
    models: list[str] = Field(..., min_length=1)
    system_prompt: str | None = None
    mode: str = "standard"
    web_search: str = "auto"
    alphapai_enabled: bool = True
    jinmen_enabled: bool = True
    kb_enabled: bool = True
    user_kb_enabled: bool = False
    kb_document_ids: list[str] = []


class SavePartialRequest(BaseModel):
    partial_responses: dict[str, str]  # model_id -> partial content


# ── Rating ──────────────────────────────────────────────────────

class RateRequest(BaseModel):
    rating: int = Field(..., ge=1, le=5)
    comment: str | None = None

class RateResponse(BaseModel):
    id: str
    rating: int
    rating_comment: str | None


# ── Detailed feedback ───────────────────────────────────────────

class FeedbackSubmitRequest(BaseModel):
    """Detailed qualitative feedback on a single model response.

    At least one of `rating`, `feedback_tags`, or `feedback_text` must be
    populated — an empty submission is rejected at the endpoint.
    """
    rating: int | None = Field(default=None, ge=1, le=5)
    feedback_tags: list[str] = Field(default_factory=list, max_length=32)
    feedback_text: str = Field(default="", max_length=4000)


class FeedbackResponse(BaseModel):
    id: str
    response_id: str
    rating: int | None
    feedback_tags: list[str]
    feedback_text: str
    sentiment: str
    processed: bool
    created_at: datetime


# ── User chat memory ────────────────────────────────────────────

class MemoryResponse(BaseModel):
    id: str
    memory_type: str
    memory_key: str
    content: str
    evidence: list[dict]
    confidence_score: float
    source_type: str
    usage_count: int
    is_active: bool
    is_pinned: bool
    last_used_at: datetime | None = None
    created_at: datetime
    updated_at: datetime


class MemoryListResponse(BaseModel):
    memories: list[MemoryResponse]
    total: int
    total_active: int


class MemoryUpdateRequest(BaseModel):
    """Partial update on a memory. Only the fields the user touches are sent."""
    is_active: bool | None = None
    is_pinned: bool | None = None
    content: str | None = Field(default=None, max_length=600)


class MemoryCreateRequest(BaseModel):
    memory_type: str
    memory_key: str = Field(..., min_length=1, max_length=120)
    content: str = Field(..., min_length=1, max_length=600)
    is_pinned: bool = False


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
    debate_models: list[str] = Field(..., min_length=2, max_length=6)
    attachments: list[dict] = []
    system_prompt: str | None = None
    web_search: bool = False
    debate_format: str = "bull_bear"  # "bull_bear" | "multi_perspective" | "round_robin"
    num_rounds: int | None = None  # override auto-detection


class DebateSummary(BaseModel):
    conclusion: str = ""
    rating: str = ""
    confidence: int = 0
    time_horizon: str = ""
    key_bull_arguments: list[str] = []
    key_bear_arguments: list[str] = []
    consensus_points: list[str] = []
    unresolved_questions: list[str] = []
    action_items: list[str] = []
    key_metrics_to_watch: list[str] = []
    mentioned_tickers: list[str] = []


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
