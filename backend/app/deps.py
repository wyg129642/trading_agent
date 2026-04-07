"""FastAPI dependency injection providers."""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import Depends, Header, HTTPException, Request, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from backend.app.config import Settings, get_settings
from backend.app.core.database import async_session_factory
from backend.app.core.security import decode_access_token

security_scheme = HTTPBearer()

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with async_session_factory() as session:
        try:
            yield session
        finally:
            await session.close()

async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security_scheme),
    db: AsyncSession = Depends(get_db),
    settings: Settings = Depends(get_settings),
):
    token = credentials.credentials
    payload = decode_access_token(token, settings)
    if payload is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired token")

    from backend.app.models.user import User
    from sqlalchemy import select
    user = await db.scalar(select(User).where(User.id == payload["sub"]))
    if user is None or not user.is_active:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found or inactive")
    return user

async def get_current_admin(user = Depends(get_current_user)):
    if user.role != "admin":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin access required")
    return user

async def get_current_boss_or_admin(user = Depends(get_current_user)):
    if user.role not in ("admin", "boss"):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Boss or admin access required")
    return user


# ── Open API key authentication ────────────────────────────────────────

async def verify_api_key(
    request: Request,
    x_api_key: str | None = Header(None),
    db: AsyncSession = Depends(get_db),
) -> "ApiKey":
    """Validate the API key from X-API-Key header.

    Also enforces a sliding-window rate limit using Redis.
    """
    if not x_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-API-Key header",
        )

    from sqlalchemy import select, update
    from backend.app.models.api_key import ApiKey

    key_hash = hashlib.sha256(x_api_key.encode()).hexdigest()
    api_key = await db.scalar(
        select(ApiKey).where(ApiKey.key_hash == key_hash, ApiKey.is_active.is_(True))
    )
    if api_key is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or inactive API key",
        )

    # Rate limiting via Redis (sliding window, best-effort)
    redis = getattr(request.app.state, "redis", None)
    if redis and api_key.rate_limit > 0:
        rk = f"open_api_rate:{api_key.id}"
        try:
            current = await redis.incr(rk)
            if current == 1:
                await redis.expire(rk, 60)
            if current > api_key.rate_limit:
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail=f"Rate limit exceeded ({api_key.rate_limit} req/min)",
                )
        except HTTPException:
            raise
        except Exception:
            pass  # Redis down → allow request

    # Update last_used_at (fire-and-forget, don't block the response)
    await db.execute(
        update(ApiKey).where(ApiKey.id == api_key.id).values(
            last_used_at=datetime.now(timezone.utc),
        )
    )
    await db.commit()

    return api_key
