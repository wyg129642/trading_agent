"""Auth API: register, login, refresh, profile."""
from __future__ import annotations
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from backend.app.config import Settings, get_settings
from backend.app.deps import get_db, get_current_user
from backend.app.schemas.user import (
    UserRegister, UserLogin, TokenResponse, TokenRefresh, UserResponse, UserUpdate,
)
from backend.app.services.auth_service import (
    create_user, authenticate_user, get_user_by_username, get_user_by_email,
)
from backend.app.core.security import (
    create_access_token, create_refresh_token, decode_refresh_token,
)
from backend.app.models.user import User

router = APIRouter()

@router.post("/register", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
async def register(body: UserRegister, db: AsyncSession = Depends(get_db)):
    # Check duplicates
    if await get_user_by_username(db, body.username):
        raise HTTPException(status_code=409, detail="Username already exists")
    if await get_user_by_email(db, body.email):
        raise HTTPException(status_code=409, detail="Email already exists")

    user = await create_user(db, body.username, body.email, body.password, body.display_name, body.language)
    return UserResponse(
        id=str(user.id), username=user.username, email=user.email,
        display_name=user.display_name, role=user.role, language=user.language,
        is_active=user.is_active, created_at=user.created_at, last_login_at=user.last_login_at,
    )

@router.post("/login", response_model=TokenResponse)
async def login(body: UserLogin, db: AsyncSession = Depends(get_db), settings: Settings = Depends(get_settings)):
    user = await authenticate_user(db, body.username, body.password)
    if user is None:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    return TokenResponse(
        access_token=create_access_token(str(user.id), settings),
        refresh_token=create_refresh_token(str(user.id), settings),
    )

@router.post("/refresh", response_model=TokenResponse)
async def refresh(body: TokenRefresh, settings: Settings = Depends(get_settings)):
    payload = decode_refresh_token(body.refresh_token, settings)
    if payload is None:
        raise HTTPException(status_code=401, detail="Invalid refresh token")

    return TokenResponse(
        access_token=create_access_token(payload["sub"], settings),
        refresh_token=create_refresh_token(payload["sub"], settings),
    )

@router.get("/me", response_model=UserResponse)
async def get_me(user: User = Depends(get_current_user)):
    return UserResponse(
        id=str(user.id), username=user.username, email=user.email,
        display_name=user.display_name, role=user.role, language=user.language,
        is_active=user.is_active, created_at=user.created_at, last_login_at=user.last_login_at,
    )

@router.put("/me", response_model=UserResponse)
async def update_me(body: UserUpdate, user: User = Depends(get_current_user), db: AsyncSession = Depends(get_db)):
    if body.display_name is not None:
        user.display_name = body.display_name
    if body.language is not None:
        user.language = body.language
    if body.email is not None:
        existing = await get_user_by_email(db, body.email)
        if existing and existing.id != user.id:
            raise HTTPException(status_code=409, detail="Email already in use")
        user.email = body.email
    await db.commit()
    await db.refresh(user)
    return UserResponse(
        id=str(user.id), username=user.username, email=user.email,
        display_name=user.display_name, role=user.role, language=user.language,
        is_active=user.is_active, created_at=user.created_at, last_login_at=user.last_login_at,
    )
