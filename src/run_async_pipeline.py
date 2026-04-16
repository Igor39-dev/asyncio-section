from __future__ import annotations

import argparse
import asyncio
import logging

from src.async_downloader import AsyncBulletinDownloader
from src.async_parser import AsyncBulletinParser
from src.config import load_settings


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


async def main(pages: int, limit: int) -> None:
    settings = load_settings()
    downloader = AsyncBulletinDownloader(settings=settings)
    parser = AsyncBulletinParser()

    links = await downloader.collect_bulletin_links(pages=pages)
    files = await downloader.download_files(links=links, limit=limit)
    parsed = await parser.parse_files(files=files)

    if not parsed:
        logging.warning("Нет успешно распарсенных файлов")
        return

    logging.info("Примеры распарсенных данных:")
    for item in parsed:
        logging.info(
            "Файл=%s | тип=%s | записей=%s",
            item.source_file.name,
            item.file_type,
            item.record_count,
        )


if __name__ == "__main__":
    configure_logging()
    cli = argparse.ArgumentParser()
    cli.add_argument("--pages", type=int, default=1, help="Количество страниц для обхода")
    cli.add_argument("--limit", type=int, default=3, help="Сколько файлов скачать для теста")
    args = cli.parse_args()
    asyncio.run(main(pages=args.pages, limit=args.limit))
