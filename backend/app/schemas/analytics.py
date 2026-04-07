"""Pydantic schemas for analytics and reporting."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel


class TokenUsageStats(BaseModel):
    total_calls: int = 0
    total_prompt_tokens: int = 0
    total_completion_tokens: int = 0
    total_tokens: int = 0
    total_cost_cny: float = 0.0
    by_stage: dict[str, dict[str, Any]] = {}
    by_model: dict[str, dict[str, Any]] = {}
    daily_trend: list[dict[str, Any]] = []


class SourceHealthResponse(BaseModel):
    source_name: str
    last_success: datetime | None
    last_failure: datetime | None
    consecutive_failures: int
    total_items_fetched: int
    is_healthy: bool

    model_config = {"from_attributes": True}


class SourceHealthListResponse(BaseModel):
    sources: list[SourceHealthResponse]
    total_healthy: int
    total_unhealthy: int


class PipelineStats(BaseModel):
    total_processed: int = 0
    pass_rate_phase1: float = 0.0
    avg_processing_time_ms: float = 0.0
    queue_depth: int = 0


class TickerSentimentPoint(BaseModel):
    date: str
    sentiment_score: float
    news_count: int


class TickerAnalytics(BaseModel):
    ticker: str
    display_name: str
    total_mentions: int = 0
    sentiment_trend: list[TickerSentimentPoint] = []
    recent_news: list[dict[str, Any]] = []
