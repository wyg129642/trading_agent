"""Admin API: user management, system configuration."""

from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from backend.app.deps import get_current_admin, get_db
from backend.app.models.user import User
from backend.app.schemas.user import AdminUserUpdate, UserResponse

router = APIRouter()


@router.get("/users", response_model=list[UserResponse])
async def list_users(
    db: AsyncSession = Depends(get_db),
    admin: User = Depends(get_current_admin),
):
    """List all users (admin only)."""
    result = await db.execute(select(User).order_by(User.created_at))
    users = result.scalars().all()
    return [
        UserResponse(
            id=str(u.id), username=u.username, email=u.email,
            display_name=u.display_name, role=u.role, language=u.language,
            is_active=u.is_active, created_at=u.created_at, last_login_at=u.last_login_at,
        )
        for u in users
    ]


@router.put("/users/{user_id}", response_model=UserResponse)
async def update_user(
    user_id: str,
    body: AdminUserUpdate,
    db: AsyncSession = Depends(get_db),
    admin: User = Depends(get_current_admin),
):
    """Update a user's role or status (admin only)."""
    user = await db.scalar(select(User).where(User.id == uuid.UUID(user_id)))
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    if body.role is not None:
        if body.role not in ("admin", "boss", "trader", "viewer"):
            raise HTTPException(status_code=400, detail="Invalid role")
        user.role = body.role
    if body.is_active is not None:
        user.is_active = body.is_active

    await db.commit()
    await db.refresh(user)

    return UserResponse(
        id=str(user.id), username=user.username, email=user.email,
        display_name=user.display_name, role=user.role, language=user.language,
        is_active=user.is_active, created_at=user.created_at, last_login_at=user.last_login_at,
    )
