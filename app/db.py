from __future__ import annotations

import asyncpg

from app.config import get_settings

_pool: asyncpg.Pool | None = None


async def init_pool() -> asyncpg.Pool:
    global _pool
    if _pool is not None:
        return _pool
    settings = get_settings()
    # asyncpg wants postgresql:// not postgres://
    dsn = settings.database_url.replace("postgresql+asyncpg://", "postgresql://")
    _pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=settings.db_pool_min,
        max_size=settings.db_pool_max,
        command_timeout=30,
    )
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("Database pool is not initialized")
    return _pool
