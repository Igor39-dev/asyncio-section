from __future__ import annotations

import logging
import re
from pathlib import Path
from time import sleep
from typing import Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup

from src.config import Settings
from src.models import BulletinLink

logger = logging.getLogger(__name__)


class SyncBulletinDownloader:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def collect_bulletin_links(self, pages: int | None = None) -> list[BulletinLink]:
        if pages is None:
            pages = self._detect_total_pages()
            logger.info("Определено страниц пагинации: %s", pages)

        links: list[BulletinLink] = []
        for page_number in range(1, pages + 1):
            try:
                links.extend(self._parse_links_from_page(page_number=page_number))
            except Exception as exc:
                logger.error("Ошибка разбора страницы %s: %s", page_number, exc)

        unique_by_file_name: dict[str, BulletinLink] = {}
        for link in links:
            file_name = self._resolve_filename(link.url.path)
            unique_by_file_name[file_name] = link

        logger.info("Собрано %s уникальных ссылок на бюллетени", len(unique_by_file_name))
        return list(unique_by_file_name.values())

    def download_files(self, links: Iterable[BulletinLink], limit: int | None = None) -> list[Path]:
        selected_links = list(links)
        if limit is not None:
            selected_links = selected_links[:limit]
        if not selected_links:
            logger.warning("Нет ссылок для скачивания")
            return []

        self.settings.data_dir.mkdir(parents=True, exist_ok=True)
        downloaded: list[Path] = []
        total = len(selected_links)

        for index, link in enumerate(selected_links, start=1):
            try:
                downloaded.append(self._download_one(link=link, index=index, total=total))
            except Exception as exc:
                logger.error("Ошибка скачивания файла: %s", exc)

        logger.info("Скачивание завершено: %s/%s", len(downloaded), total)
        return downloaded

    def _parse_links_from_page(self, page_number: int) -> list[BulletinLink]:
        page_url = self._build_page_url(page_number)
        html = self._fetch_text(url=page_url)
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

    def _detect_total_pages(self) -> int:
        page_url = self._build_page_url(1)
        html = self._fetch_text(url=page_url)
        soup = BeautifulSoup(html, "html.parser")

        pagination_block = soup.select_one("div.bx-pagination-container")
        if not pagination_block:
            return 1

        max_page = 1
        for link in pagination_block.select("a[href]"):
            href = link.get("href", "")
            match = re.search(r"page=page-(\d+)", href)
            if match:
                max_page = max(max_page, int(match.group(1)))
        return max_page

    def _download_one(self, link: BulletinLink, index: int, total: int) -> Path:
        file_name = self._resolve_filename(link.url.path)
        destination = self.settings.data_dir / file_name
        if destination.exists() and destination.stat().st_size > 0:
            logger.info("[%s/%s] Файл уже существует, пропуск: %s", index, total, destination.name)
            return destination

        for attempt in range(1, self.settings.max_retries + 1):
            try:
                logger.info("[%s/%s] Скачивание %s", index, total, link.url)
                payload = self._fetch_bytes(url=str(link.url))
                destination.write_bytes(payload)
                logger.info("[%s/%s] Сохранён %s (%s байт)", index, total, destination.name, len(payload))
                return destination
            except (HTTPError, URLError, TimeoutError) as exc:
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
                sleep(attempt)

        raise RuntimeError(f"Не удалось скачать файл: {link.url}")

    def _fetch_text(self, url: str) -> str:
        for attempt in range(1, self.settings.max_retries + 1):
            try:
                request = Request(url, headers={"User-Agent": "Mozilla/5.0"})
                with urlopen(request, timeout=self.settings.request_timeout_seconds) as response:
                    return response.read().decode("utf-8")
            except (HTTPError, URLError, TimeoutError):
                if attempt == self.settings.max_retries:
                    raise
                sleep(attempt)
        raise RuntimeError(f"Не удалось получить страницу: {url}")

    def _fetch_bytes(self, url: str) -> bytes:
        request = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(request, timeout=self.settings.request_timeout_seconds) as response:
            return response.read()

    def _build_page_url(self, page_number: int) -> str:
        base = urljoin(self.settings.base_url, self.settings.results_path)
        if page_number == 1:
            return base
        return f"{base}?page=page-{page_number}"

    @staticmethod
    def _resolve_filename(path: str) -> str:
        parsed = Path(urlparse(path).path).name
        return parsed or "unknown_file.bin"
