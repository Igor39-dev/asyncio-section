from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import datetime

from sqlalchemy import DateTime, Integer, String, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from src.config import Settings
from src.models import TradeRow

logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


class AsyncSpimexTrade(Base):
    __tablename__ = "async_spimex_trades"
    __table_args__ = (
        UniqueConstraint(
            "instrument_code",
            "instrument_name",
            "source_file",
            name="uq_async_spimex_trade_identity",
        ),
    )

    id:              Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    instrument_code: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    instrument_name: Mapped[str] = mapped_column(String(512), nullable=False, index=True)
    source_file:     Mapped[str] = mapped_column(String(256), nullable=False)
    bulletin_url:    Mapped[str | None] = mapped_column(String(1024), nullable=True)
    trade_date:      Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at:      Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at:      Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class AsyncDatabase:
    def __init__(self, settings: Settings) -> None:
        self.engine = create_async_engine(settings.postgres_dsn_asyncpg, pool_pre_ping=True)
        self.session_factory = async_sessionmaker(self.engine, class_=AsyncSession, expire_on_commit=False)

    async def create_schema(self) -> None:
        async with self.engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        logger.info("Проверка/создание схемы БД завершена")

    async def upsert_trade_rows(self, rows: Sequence[TradeRow], batch_size: int = 1000) -> int:
        unique_rows = self._deduplicate_rows(rows)
        if not unique_rows:
            logger.warning("Нет строк для загрузки в БД")
            return 0

        affected = 0
        async with self.session_factory() as session:
            for start in range(0, len(unique_rows), batch_size):
                batch = unique_rows[start : start + batch_size]
                payload = [
                    {
                        "instrument_code": row.instrument_code,
                        "instrument_name": row.instrument_name,
                        "source_file": row.source_file,
                        "bulletin_url": str(row.bulletin_url) if row.bulletin_url else None,
                        "trade_date": row.trade_date,
                    }
                    for row in batch
                ]
                query = insert(AsyncSpimexTrade).values(payload)
                query = query.on_conflict_do_update(
                    constraint="uq_async_spimex_trade_identity",
                    set_={
                        "bulletin_url": query.excluded.bulletin_url,
                        "trade_date": query.excluded.trade_date,
                        "updated_at": func.now(),
                    },
                )
                await session.execute(query)
                affected += len(batch)

            await session.commit()

        logger.info("Загрузка в БД завершена, обработано строк=%s", affected)
        return affected

    async def close(self) -> None:
        await self.engine.dispose()

    @staticmethod
    def _deduplicate_rows(rows: Sequence[TradeRow]) -> list[TradeRow]:
        unique: dict[tuple[str, str, str], TradeRow] = {}
        for row in rows:
            key = (
                row.instrument_code.strip(),
                row.instrument_name.strip(),
                row.source_file.strip(),
            )
            unique[key] = row
        return list(unique.values())
