from __future__ import annotations

import argparse
import asyncio
import shutil
import sys
from dataclasses import dataclass, replace
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter
from typing import Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sqlalchemy import select, text
from sqlalchemy import func as sa_func

from src.async_downloader import AsyncBulletinDownloader
from src.async_parser import AsyncBulletinParser
from src.config import Settings, load_settings
from src.database import AsyncDatabase, AsyncSpimexTrade, SyncDatabase, SyncSpimexTrade
from src.models import BulletinLink, TradeRow
from src.sync_downloader import SyncBulletinDownloader
from src.sync_parser import SyncBulletinParser


@dataclass(slots=True)
class StageMetrics:
    download_seconds: float
    parse_seconds: float
    db_seconds: float
    total_seconds: float
    inserted_rows: int
    db_rows: int
    file_count: int
    parsed_rows: int


def _extract_trade_date(file_name: str):
    import re
    from datetime import UTC, datetime

    match = re.search(r"(\d{14})", file_name)
    if not match:
        return None
    return datetime.strptime(match.group(1), "%Y%m%d%H%M%S").replace(tzinfo=UTC)


def _enrich_rows(rows: Sequence[TradeRow], files: Sequence[Path], links_by_name: dict[str, str]) -> list[TradeRow]:
    files_by_name = {file.name for file in files}
    enriched: list[TradeRow] = []
    for row in rows:
        source_file_name = Path(row.source_file).name
        if source_file_name not in files_by_name:
            continue
        enriched.append(
            TradeRow(
                instrument_code=row.instrument_code,
                instrument_name=row.instrument_name,
                source_file=source_file_name,
                bulletin_url=links_by_name.get(source_file_name),
                trade_date=_extract_trade_date(source_file_name),
            )
        )
    return enriched


def _prepare_clean_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def _collect_shared_links(settings: Settings, pages: int | None, limit: int | None) -> list[BulletinLink]:
    downloader = SyncBulletinDownloader(settings=settings)
    links = downloader.collect_bulletin_links(pages=pages)
    if limit is not None:
        return links[:limit]
    return links


def run_sync_benchmark(base_settings: Settings, links: list[BulletinLink], batch_size: int, data_dir: Path) -> StageMetrics:
    settings = replace(base_settings, data_dir=data_dir)
    downloader = SyncBulletinDownloader(settings=settings)
    parser = SyncBulletinParser()
    database = SyncDatabase(settings=settings)

    _prepare_clean_dir(settings.data_dir)
    database.create_schema()
    with database.engine.begin() as connection:
        connection.execute(text("TRUNCATE TABLE sync_spimex_trades RESTART IDENTITY"))

    started_total = perf_counter()

    started_stage = perf_counter()
    files = downloader.download_files(links=links, limit=None)
    download_seconds = perf_counter() - started_stage

    started_stage = perf_counter()
    parser.parse_files(files=files)
    rows = parser.extract_trade_rows(files=files)
    links_by_name = {Path(link.url.path).name: str(link.url) for link in links}
    enriched_rows = _enrich_rows(rows=rows, files=files, links_by_name=links_by_name)
    parse_seconds = perf_counter() - started_stage

    started_stage = perf_counter()
    inserted_rows = database.upsert_trade_rows(rows=enriched_rows, batch_size=batch_size)
    with database.session_factory() as session:
        db_rows = session.execute(select(sa_func.count()).select_from(SyncSpimexTrade)).scalar_one()
    db_seconds = perf_counter() - started_stage

    total_seconds = perf_counter() - started_total
    database.close()

    return StageMetrics(
        download_seconds=download_seconds,
        parse_seconds=parse_seconds,
        db_seconds=db_seconds,
        total_seconds=total_seconds,
        inserted_rows=inserted_rows,
        db_rows=db_rows,
        file_count=len(files),
        parsed_rows=len(enriched_rows),
    )


async def run_async_benchmark(
    base_settings: Settings,
    links: list[BulletinLink],
    batch_size: int,
    data_dir: Path,
) -> StageMetrics:
    settings = replace(base_settings, data_dir=data_dir)
    downloader = AsyncBulletinDownloader(settings=settings)
    parser = AsyncBulletinParser()
    database = AsyncDatabase(settings=settings)

    _prepare_clean_dir(settings.data_dir)
    await database.create_schema()
    async with database.engine.begin() as connection:
        await connection.execute(text("TRUNCATE TABLE async_spimex_trades RESTART IDENTITY"))

    started_total = perf_counter()

    started_stage = perf_counter()
    files = await downloader.download_files(links=links, limit=None)
    download_seconds = perf_counter() - started_stage

    started_stage = perf_counter()
    await parser.parse_files(files=files)
    rows = await parser.extract_trade_rows(files=files)
    links_by_name = {Path(link.url.path).name: str(link.url) for link in links}
    enriched_rows = _enrich_rows(rows=rows, files=files, links_by_name=links_by_name)
    parse_seconds = perf_counter() - started_stage

    started_stage = perf_counter()
    inserted_rows = await database.upsert_trade_rows(rows=enriched_rows, batch_size=batch_size)
    async with database.session_factory() as session:
        result = await session.execute(select(sa_func.count()).select_from(AsyncSpimexTrade))
        db_rows = result.scalar_one()
    db_seconds = perf_counter() - started_stage

    total_seconds = perf_counter() - started_total
    await database.close()

    return StageMetrics(
        download_seconds=download_seconds,
        parse_seconds=parse_seconds,
        db_seconds=db_seconds,
        total_seconds=total_seconds,
        inserted_rows=inserted_rows,
        db_rows=db_rows,
        file_count=len(files),
        parsed_rows=len(enriched_rows),
    )


def _fmt_seconds(value: float) -> str:
    return f"{value:.2f} сек"


def _fmt_speedup(sync_value: float, async_value: float) -> str:
    if async_value == 0:
        return "n/a"
    return f"{sync_value / async_value:.2f}x"


def _print_comparison_table(sync_metrics: StageMetrics, async_metrics: StageMetrics) -> None:
    headers: tuple[str, str, str, str] = ("Метрика", "Синхронный код", "Асинхронный код", "Ускорение")
    widths: tuple[int, int, int, int] = tuple(len(header) for header in headers)

    def _format_row(cells: tuple[str, str, str, str]) -> str:
        return "|" + "|".join(f" {cell.center(width)} " for cell, width in zip(cells, widths, strict=True)) + "|"

    separator = "|" + "|".join("-" * (width + 2) for width in widths) + "|"
    rows: list[tuple[str, str, str, str]] = [
        (
            "Скачивание файлов",
            _fmt_seconds(sync_metrics.download_seconds),
            _fmt_seconds(async_metrics.download_seconds),
            _fmt_speedup(sync_metrics.download_seconds, async_metrics.download_seconds),
        ),
        (
            "Парсинг данных",
            _fmt_seconds(sync_metrics.parse_seconds),
            _fmt_seconds(async_metrics.parse_seconds),
            _fmt_speedup(sync_metrics.parse_seconds, async_metrics.parse_seconds),
        ),
        (
            "Загрузка в БД",
            _fmt_seconds(sync_metrics.db_seconds),
            _fmt_seconds(async_metrics.db_seconds),
            _fmt_speedup(sync_metrics.db_seconds, async_metrics.db_seconds),
        ),
        (
            "Общее время",
            _fmt_seconds(sync_metrics.total_seconds),
            _fmt_seconds(async_metrics.total_seconds),
            _fmt_speedup(sync_metrics.total_seconds, async_metrics.total_seconds),
        ),
    ]

    print()
    print(_format_row(headers))
    print(separator)
    for row in rows:
        print(_format_row(row))
    print()
    print(
        "Проверка записей в БД: "
        f"sync inserted={sync_metrics.inserted_rows}, sync db_rows={sync_metrics.db_rows}; "
        f"async inserted={async_metrics.inserted_rows}, async db_rows={async_metrics.db_rows}"
    )
    print(
        "Проверка набора данных: "
        f"sync files={sync_metrics.file_count}, async files={async_metrics.file_count}; "
        f"sync parsed_rows={sync_metrics.parsed_rows}, async parsed_rows={async_metrics.parsed_rows}"
    )


def main() -> None:
    cli = argparse.ArgumentParser(description="Сравнение производительности sync/async пайплайнов")
    cli.add_argument("--pages", type=int, default=1, help="Количество страниц для сбора ссылок")
    cli.add_argument("--limit", type=int, default=10, help="Количество файлов для сравнения")
    cli.add_argument("--batch-size", type=int, default=1000, help="Размер batch для upsert")
    args = cli.parse_args()

    settings = load_settings()
    links = _collect_shared_links(settings=settings, pages=args.pages, limit=args.limit)
    if not links:
        raise RuntimeError("Не удалось получить ссылки для сравнения")

    with TemporaryDirectory(prefix="sync_bench_") as sync_dir, TemporaryDirectory(prefix="async_bench_") as async_dir:
        sync_metrics = run_sync_benchmark(
            base_settings=settings,
            links=links,
            batch_size=args.batch_size,
            data_dir=Path(sync_dir),
        )
        async_metrics = asyncio.run(
            run_async_benchmark(
                base_settings=settings,
                links=links,
                batch_size=args.batch_size,
                data_dir=Path(async_dir),
            )
        )

    _print_comparison_table(sync_metrics=sync_metrics, async_metrics=async_metrics)


if __name__ == "__main__":
    main()
