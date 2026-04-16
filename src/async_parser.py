from __future__ import annotations

import logging
import re
from datetime import datetime, UTC
from pathlib import Path

import pandas as pd
import pdfplumber

from src.models import ParsedTradeRecord, TradeRow

logger = logging.getLogger(__name__)


class AsyncBulletinParser:
    code_pattern = re.compile(r"\b[A-ZА-Я0-9][A-ZА-Я0-9\-./]{2,}\b")

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

    async def extract_trade_rows(self, files: list[Path]) -> list[TradeRow]:
        rows: list[TradeRow] = []
        for file_path in files:
            try:
                file_type = self._detect_file_type(file_path)
                if file_type == "pdf":
                    parsed_rows = self._extract_rows_from_pdf(file_path)
                else:
                    parsed_rows = self._extract_rows_from_excel(file_path, file_type=file_type)
                rows.extend(parsed_rows)
                logger.info("Файл %s: извлечено строк=%s", file_path.name, len(parsed_rows))
            except Exception as exc:
                logger.error("Ошибка извлечения строк из %s: %s", file_path.name, exc)
        logger.info("Итоговое число нормализованных строк: %s", len(rows))
        return rows

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

    def _extract_rows_from_excel(self, file_path: Path, file_type: str) -> list[TradeRow]:
        engine = "openpyxl" if file_type == "xlsx" else "xlrd"
        frame = pd.read_excel(file_path, engine=engine)
        frame = frame.rename(columns=lambda x: str(x).strip().lower())

        code_col = self._find_column(frame.columns, ("код инструмента", "код", "instrument code"))
        name_col = self._find_column(
            frame.columns,
            ("наименование инструмента", "инструмент", "товар", "instrument name", "name"),
        )
        if not code_col or not name_col:
            return []

        rows: list[TradeRow] = []
        for _, row in frame.iterrows():
            code = str(row.get(code_col, "")).strip()
            name = str(row.get(name_col, "")).strip()
            if not code or code.lower() == "nan":
                continue
            if not name or name.lower() == "nan":
                continue
            rows.append(
                TradeRow(
                    instrument_code=code,
                    instrument_name=name,
                    source_file=file_path.name,
                )
            )
        return rows

    def _extract_rows_from_pdf(self, file_path: Path) -> list[TradeRow]:
        rows: list[TradeRow] = []
        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ""
                for line in text.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    match = self.code_pattern.search(line)
                    if not match:
                        continue
                    code = match.group(0)
                    name = line.replace(code, "", 1).strip(" -\t")
                    if len(name) < 2:
                        continue
                    rows.append(
                        TradeRow(
                            instrument_code=code,
                            instrument_name=name[:512],
                            source_file=file_path.name,
                        )
                    )
        return rows

    @staticmethod
    def _find_column(columns: pd.Index, variants: tuple[str, ...]) -> str | None:
        normalized = {str(col).strip().lower(): str(col) for col in columns}
        for variant in variants:
            found = normalized.get(variant)
            if found:
                return found
        return None
