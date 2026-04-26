"""Async SQLAlchemy database engine and session factory."""
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from backend.app.config import get_settings

class Base(DeclarativeBase):
    pass

settings = get_settings()

engine = create_async_engine(
    settings.database_url,
    echo=settings.app_debug,
    pool_size=20,
    max_overflow=10,
    pool_pre_ping=True,
)

async_session_factory = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


# Prod-DB engine for staging endpoints that opt into reading prod tables
# (e.g. portfolio_scan_results, populated only by the prod-only scanner).
# In a prod process database_url_prod == database_url, so the connection
# pool is logically the same — but we keep them separate so the staging
# process gets its own pool against the raw `trading_agent` DB.
_prod_engine = None
_prod_session_factory: async_sessionmaker[AsyncSession] | None = None


def get_prod_session_factory() -> async_sessionmaker[AsyncSession]:
    """Lazy-initialise an async session factory pinned to prod's Postgres DB."""
    global _prod_engine, _prod_session_factory
    if _prod_session_factory is None:
        _prod_engine = create_async_engine(
            settings.database_url_prod,
            echo=settings.app_debug,
            pool_size=5,
            max_overflow=5,
            pool_pre_ping=True,
        )
        _prod_session_factory = async_sessionmaker(
            _prod_engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
    return _prod_session_factory
