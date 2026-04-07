"""Pydantic schemas for favorites / bookmarks."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


ALLOWED_ITEM_TYPES = Literal["news", "wechat", "roadshow_cn", "roadshow_us", "comment"]


class FavoriteCreate(BaseModel):
    item_type: ALLOWED_ITEM_TYPES
    item_id: str = Field(min_length=1)
    note: str | None = None


class FavoriteResponse(BaseModel):
    id: int
    item_type: str
    item_id: str
    note: str | None
    title: str | None = None
    created_at: datetime

    model_config = {"from_attributes": True}


class FavoriteListResponse(BaseModel):
    favorites: list[FavoriteResponse]
    total: int


class FavoriteCheckResponse(BaseModel):
    is_favorited: bool
    favorite_id: int | None = None


class FavoriteIdsResponse(BaseModel):
    item_ids: list[str]
