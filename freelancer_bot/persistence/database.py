from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    create_async_engine,
)


class DatabaseConfigurationError(ValueError):
    pass


class Database:
    def __init__(
        self,
        database_url: str,
        *,
        pool_size: int = 5,
        max_overflow: int = 10,
    ) -> None:
        try:
            url = make_url(database_url)
        except (ArgumentError, ValueError):
            raise DatabaseConfigurationError("Invalid PostgreSQL database URL") from None
        if url.drivername != "postgresql+psycopg":
            raise DatabaseConfigurationError(
                "PostgreSQL database URL must use postgresql+psycopg://"
            )

        self._engine: AsyncEngine = create_async_engine(
            url,
            pool_pre_ping=True,
            pool_size=pool_size,
            max_overflow=max_overflow,
        )

    @property
    def engine(self) -> AsyncEngine:
        return self._engine

    @asynccontextmanager
    async def connect(self) -> AsyncIterator[AsyncConnection]:
        async with self._engine.connect() as connection:
            yield connection

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncConnection]:
        async with self._engine.begin() as connection:
            yield connection

    async def close(self) -> None:
        await self._engine.dispose()
