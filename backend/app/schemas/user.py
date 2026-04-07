"""Pydantic schemas for authentication and user management."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, EmailStr, Field


class UserRegister(BaseModel):
    username: str = Field(min_length=3, max_length=50)
    email: EmailStr
    password: str = Field(min_length=6, max_length=128)
    display_name: str | None = None
    language: str = "zh"


class UserLogin(BaseModel):
    username: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class TokenRefresh(BaseModel):
    refresh_token: str


class UserResponse(BaseModel):
    id: str
    username: str
    email: str
    display_name: str | None
    role: str
    language: str
    is_active: bool
    created_at: datetime
    last_login_at: datetime | None

    model_config = {"from_attributes": True}


class UserUpdate(BaseModel):
    display_name: str | None = None
    language: str | None = None
    email: EmailStr | None = None


class AdminUserUpdate(BaseModel):
    role: str | None = None
    is_active: bool | None = None
