from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.config import get_settings


_engine = None
_Session: async_sessionmaker[AsyncSession] | None = None


def get_engine():
    global _engine
    if _engine is None:
        dsn = get_settings().postgres_dsn
        _engine = create_async_engine(dsn, pool_pre_ping=True, future=True)
    return _engine


def get_session_maker() -> async_sessionmaker[AsyncSession]:
    global _Session
    if _Session is None:
        _Session = async_sessionmaker(bind=get_engine(), expire_on_commit=False)
    return _Session


@asynccontextmanager
async def session_context() -> AsyncIterator[AsyncSession]:
    session_maker = get_session_maker()
    session = session_maker()
    try:
        yield session
    finally:
        await session.close()


async def get_session() -> AsyncIterator[AsyncSession]:
    """FastAPI dependency that yields an AsyncSession.

    This uses a plain async generator so FastAPI manages the context correctly.
    """
    session_maker = get_session_maker()
    async with session_maker() as session:  # type: ignore[call-arg]
        yield session


async def init_db() -> None:
    """Create tables if they don't exist (for MVP, no migrations)."""
    from app.db.models import Base  # local import to avoid circular

    engine = get_engine()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


