from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(slots=True)
class Settings:
    postgres_db: str
    postgres_user: str
    postgres_password: str
    postgres_host: str
    postgres_port: int
    base_url: str = "https://spimex.com"
    results_path: str = "/markets/oil_products/trades/results/"
    data_dir: Path = Path("data")
    concurrency_limit: int = 5
    request_timeout_seconds: int = 45
    max_retries: int = 3
    test_download_limit: int = 3

    @property
    def postgres_dsn_asyncpg(self) -> str:
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def postgres_dsn_psycopg(self) -> str:
        return (
            f"postgresql+psycopg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


def _get_env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def load_settings() -> Settings:
    load_dotenv()
    return Settings(
        postgres_db=_get_env("POSTGRES_DB"),
        postgres_user=_get_env("POSTGRES_USER"),
        postgres_password=_get_env("POSTGRES_PASSWORD"),
        postgres_host=_get_env("POSTGRES_HOST", "localhost"),
        postgres_port=int(_get_env("POSTGRES_PORT", "5432")),
    )
