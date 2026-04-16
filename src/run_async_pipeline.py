from __future__ import annotations

import argparse
import asyncio
import logging
import re
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter

from src.async_downloader import AsyncBulletinDownloader
from src.async_parser import AsyncBulletinParser
from src.config import load_settings
from src.database import AsyncDatabase
from src.models import TradeRow


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def _extract_trade_date(file_name: str) -> datetime | None:
    match = re.search(r"(\d{14})", file_name)
    if not match:
        return None
    return datetime.strptime(match.group(1), "%Y%m%d%H%M%S").replace(tzinfo=UTC)


def _enrich_rows(rows: list[TradeRow], files: list[Path], links_by_name: dict[str, str]) -> list[TradeRow]:
    files_by_name = {file.name: file for file in files}
    enriched: list[TradeRow] = []
    for row in rows:
        source_file_name = Path(row.source_file).name
        bulletin_url = links_by_name.get(source_file_name)
        trade_date = _extract_trade_date(source_file_name)
        if source_file_name not in files_by_name:
            continue
        enriched.append(
            TradeRow(
                instrument_code=row.instrument_code,
                instrument_name=row.instrument_name,
                source_file=source_file_name,
                bulletin_url=bulletin_url,
                trade_date=trade_date,
            )
        )
    return enriched


async def main(pages: int | None, limit: int | None, batch_size: int) -> None:
    settings = load_settings()
    downloader = AsyncBulletinDownloader(settings=settings)
    parser = AsyncBulletinParser()
    database = AsyncDatabase(settings=settings)

    total_started_at = perf_counter()

    stage_started_at = perf_counter()
    links = await downloader.collect_bulletin_links(pages=pages)
    collect_elapsed = perf_counter() - stage_started_at
    logging.info("Этап collect_bulletin_links завершен за %.2f сек", collect_elapsed)

    stage_started_at = perf_counter()
    files = await downloader.download_files(links=links, limit=limit)
    download_elapsed = perf_counter() - stage_started_at
    logging.info("Этап download_files завершен за %.2f сек", download_elapsed)

    stage_started_at = perf_counter()
    parsed = await parser.parse_files(files=files)
    parse_elapsed = perf_counter() - stage_started_at
    logging.info("Этап parse_files завершен за %.2f сек", parse_elapsed)

    if not parsed:
        logging.warning("Нет успешно распарсенных файлов")
        return

    stage_started_at = perf_counter()
    rows = await parser.extract_trade_rows(files=files)
    links_by_name = {Path(link.url.path).name: str(link.url) for link in links}
    enriched_rows = _enrich_rows(rows=rows, files=files, links_by_name=links_by_name)
    normalize_elapsed = perf_counter() - stage_started_at
    logging.info("Этап extract_trade_rows завершен за %.2f сек", normalize_elapsed)

    stage_started_at = perf_counter()
    await database.create_schema()
    inserted = await database.upsert_trade_rows(rows=enriched_rows, batch_size=batch_size)
    await database.close()
    db_elapsed = perf_counter() - stage_started_at
    logging.info("Этап upsert_trade_rows завершен за %.2f сек", db_elapsed)

    total_elapsed = perf_counter() - total_started_at
    logging.info("Итог async pipeline: файлов=%s, строк=%s, upsert=%s", len(files), len(enriched_rows), inserted)
    logging.info("Общее время async pipeline: %.2f сек", total_elapsed)


if __name__ == "__main__":
    configure_logging()
    cli = argparse.ArgumentParser()
    cli.add_argument("--pages", type=int, default=None, help="Количество страниц, по умолчанию все")
    cli.add_argument("--limit", type=int, default=None, help="Ограничение числа файлов")
    cli.add_argument("--batch-size", type=int, default=1000, help="Размер batch для upsert")
    args = cli.parse_args()
    asyncio.run(main(pages=args.pages, limit=args.limit, batch_size=args.batch_size))
