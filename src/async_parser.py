from __future__ import annotations

import logging
from datetime import datetime, UTC
from pathlib import Path

import pandas as pd
import pdfplumber

from src.models import ParsedTradeRecord

logger = logging.getLogger(__name__)


class AsyncBulletinParser:
    async def parse_files(self, files: list[Path]) -> list[ParsedTradeRecord]:
        parsed_records: list[ParsedTradeRecord] = []
        for file_path in files:
            try:
                parsed = await self._parse_file(file_path)
                parsed_records.append(parsed)
                logger.info(
                    "Файл %s обработан: тип=%s, строк=%s",
                    file_path.name,
                    parsed.file_type,
                    parsed.record_count,
                )
            except Exception as exc:
                logger.error("Ошибка парсинга файла %s: %s", file_path, exc)
        logger.info("Обработано файлов: %s, успешно: %s", len(files), len(parsed_records))
        return parsed_records

    async def _parse_file(self, file_path: Path) -> ParsedTradeRecord:
        file_type = self._detect_file_type(file_path)
        if file_type == "pdf":
            sample_text, record_count = self._parse_pdf(file_path)
        elif file_type in {"xls", "xlsx"}:
            sample_text, record_count = self._parse_excel(file_path, file_type=file_type)
        else:
            raise ValueError(f"Неподдерживаемый формат файла: {file_path.suffix}")

        return ParsedTradeRecord(
            source_file=file_path,
            file_type=file_type,
            parsed_at=datetime.now(UTC),
            record_count=record_count,
            sample_text=sample_text,
        )

    @staticmethod
    def _detect_file_type(file_path: Path) -> str:
        suffix = file_path.suffix.lower().lstrip(".")
        if suffix not in {"pdf", "xls", "xlsx"}:
            raise ValueError(f"Неизвестный формат: {suffix}")
        return suffix

    @staticmethod
    def _parse_pdf(file_path: Path) -> tuple[str, int]:
        lines: list[str] = []
        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ""
                for line in text.splitlines():
                    normalized = line.strip()
                    if normalized:
                        lines.append(normalized)
        sample_text = "\n".join(lines[:10])
        return sample_text, len(lines)

    @staticmethod
    def _parse_excel(file_path: Path, file_type: str) -> tuple[str, int]:
        engine = "openpyxl" if file_type == "xlsx" else "xlrd"
        frame = pd.read_excel(file_path, engine=engine)
        frame = frame.dropna(how="all")
        sample_text = frame.head(5).to_string(index=False)
        return sample_text, len(frame.index)
