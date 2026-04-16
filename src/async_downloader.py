from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin, urlparse

import aiohttp
from bs4 import BeautifulSoup

from src.config import Settings
from src.models import BulletinLink

logger = logging.getLogger(__name__)


class AsyncBulletinDownloader:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.semaphore = asyncio.Semaphore(settings.concurrency_limit)

    async def collect_bulletin_links(self, pages: int = 1) -> list[BulletinLink]:
        timeout = aiohttp.ClientTimeout(total=self.settings.request_timeout_seconds)
        connector = aiohttp.TCPConnector(limit=self.settings.concurrency_limit * 2)
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            tasks = [
                self._parse_links_from_page(session=session, page_number=page_number)
                for page_number in range(1, pages + 1)
            ]
            raw_links = await asyncio.gather(*tasks, return_exceptions=True)

        links: list[BulletinLink] = []
        for page_idx, links_or_exc in enumerate(raw_links, start=1):
            if isinstance(links_or_exc, Exception):
                logger.error("Ошибка разбора страницы %s: %s", page_idx, links_or_exc)
                continue
            links.extend(links_or_exc)

        unique = {(str(link.url), link.title): link for link in links}
        logger.info("Собрано %s уникальных ссылок на бюллетени", len(unique))
        return list(unique.values())

    async def download_files(self, links: Iterable[BulletinLink], limit: int | None = None) -> list[Path]:
        selected_links = list(links)
        if limit is not None:
            selected_links = selected_links[:limit]
        if not selected_links:
            logger.warning("Нет ссылок для скачивания")
            return []

        self.settings.data_dir.mkdir(parents=True, exist_ok=True)
        timeout = aiohttp.ClientTimeout(total=self.settings.request_timeout_seconds)
        connector = aiohttp.TCPConnector(limit=self.settings.concurrency_limit * 2)
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            tasks = [self._download_one(session, link, idx + 1, len(selected_links)) for idx, link in enumerate(selected_links)]
            results = await asyncio.gather(*tasks, return_exceptions=True)

        downloaded: list[Path] = []
        for result in results:
            if isinstance(result, Exception):
                logger.error("Ошибка скачивания файла: %s", result)
                continue
            downloaded.append(result)

        logger.info("Скачивание завершено: %s/%s", len(downloaded), len(selected_links))
        return downloaded

    async def _parse_links_from_page(self, session: aiohttp.ClientSession, page_number: int) -> list[BulletinLink]:
        page_url = self._build_page_url(page_number)
        html = await self._fetch_text(session=session, url=page_url)
        soup = BeautifulSoup(html, "html.parser")

        selectors = (
            "div.accordeon-inner__header a.link.pdf",
            "a.accordeon-inner__item-title.link.pdf",
            "a.link.pdf",
        )

        elements = []
        for selector in selectors:
            elements = soup.select(selector)
            if elements:
                break

        parsed_links: list[BulletinLink] = []
        for element in elements:
            href = element.get("href", "").strip()
            title = element.get_text(strip=True)
            if not href:
                continue
            absolute_url = urljoin(self.settings.base_url, href)
            try:
                parsed_links.append(
                    BulletinLink(title=title, url=absolute_url, source_page=page_url)
                )
            except Exception as exc:
                logger.warning("Пропущена некорректная ссылка %s: %s", absolute_url, exc)
        logger.info("Страница %s: найдено %s ссылок", page_number, len(parsed_links))
        return parsed_links

    async def _download_one(
        self,
        session: aiohttp.ClientSession,
        link: BulletinLink,
        index: int,
        total: int,
    ) -> Path:
        async with self.semaphore:
            file_name = self._resolve_filename(link.url.path)
            destination = self.settings.data_dir / file_name
            if destination.exists() and destination.stat().st_size > 0:
                logger.info("[%s/%s] Файл уже существует, пропуск: %s", index, total, destination.name)
                return destination

            for attempt in range(1, self.settings.max_retries + 1):
                try:
                    logger.info("[%s/%s] Скачивание %s", index, total, link.url)
                    async with session.get(str(link.url)) as response:
                        response.raise_for_status()
                        payload = await response.read()
                        destination.write_bytes(payload)
                        logger.info("[%s/%s] Сохранён %s (%s байт)", index, total, destination.name, len(payload))
                        return destination
                except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                    logger.warning(
                        "[%s/%s] Попытка %s/%s неуспешна: %s",
                        index,
                        total,
                        attempt,
                        self.settings.max_retries,
                        exc,
                    )
                    if attempt == self.settings.max_retries:
                        raise
                    await asyncio.sleep(attempt)

            raise RuntimeError(f"Не удалось скачать файл: {link.url}")

    async def _fetch_text(self, session: aiohttp.ClientSession, url: str) -> str:
        for attempt in range(1, self.settings.max_retries + 1):
            try:
                async with session.get(url) as response:
                    response.raise_for_status()
                    return await response.text()
            except (aiohttp.ClientError, asyncio.TimeoutError):
                if attempt == self.settings.max_retries:
                    raise
                await asyncio.sleep(attempt)
        raise RuntimeError(f"Не удалось получить страницу: {url}")

    def _build_page_url(self, page_number: int) -> str:
        base = urljoin(self.settings.base_url, self.settings.results_path)
        if page_number == 1:
            return base
        return f"{base}?PAGEN_1={page_number}"

    @staticmethod
    def _resolve_filename(path: str) -> str:
        parsed = Path(urlparse(path).path).name
        return parsed or "unknown_file.bin"
