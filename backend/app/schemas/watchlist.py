"""Pydantic schemas for watchlist management."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class WatchlistCreate(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    description: str | None = None


class WatchlistUpdate(BaseModel):
    name: str | None = Field(None, min_length=1, max_length=100)
    description: str | None = None


class WatchlistItemCreate(BaseModel):
    item_type: str = Field(pattern="^(ticker|sector|keyword)$")
    value: str = Field(min_length=1, max_length=100)
    display_name: str | None = None
    metadata: dict[str, Any] = {}


class WatchlistItemResponse(BaseModel):
    id: str
    item_type: str
    value: str
    display_name: str | None
    metadata: dict[str, Any] = {}
    added_at: datetime

    model_config = {"from_attributes": True}


class WatchlistResponse(BaseModel):
    id: str
    name: str
    description: str | None
    is_default: bool
    created_at: datetime
    items: list[WatchlistItemResponse] = []
    item_count: int = 0

    model_config = {"from_attributes": True}


class WatchlistListResponse(BaseModel):
    watchlists: list[WatchlistResponse]
