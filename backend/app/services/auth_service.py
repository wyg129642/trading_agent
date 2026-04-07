"""Authentication business logic."""
from __future__ import annotations
from datetime import datetime, timezone
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from backend.app.models.user import User
from backend.app.core.security import hash_password, verify_password

async def create_user(db: AsyncSession, username: str, email: str, password: str, display_name: str | None = None, language: str = "zh") -> User:
    """Create a new user account."""
    user = User(
        username=username,
        email=email,
        password_hash=hash_password(password),
        display_name=display_name or username,
        language=language,
    )
    db.add(user)
    await db.commit()
    await db.refresh(user)
    return user

async def authenticate_user(db: AsyncSession, username: str, password: str) -> User | None:
    """Validate credentials, return User or None."""
    stmt = select(User).where((User.username == username) | (User.email == username))
    user = await db.scalar(stmt)
    if user is None or not verify_password(password, user.password_hash):
        return None
    if not user.is_active:
        return None
    # Update last login
    user.last_login_at = datetime.now(timezone.utc)
    await db.commit()
    return user

async def get_user_by_username(db: AsyncSession, username: str) -> User | None:
    return await db.scalar(select(User).where(User.username == username))

async def get_user_by_email(db: AsyncSession, email: str) -> User | None:
    return await db.scalar(select(User).where(User.email == email))
