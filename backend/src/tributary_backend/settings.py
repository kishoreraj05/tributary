from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configured through TRIBUTARY_* environment variables (or a .env file)."""

    model_config = SettingsConfigDict(env_prefix="TRIBUTARY_", env_file=".env", extra="ignore")

    database_url: str = "postgresql+psycopg://tributary:tributary@localhost:5432/tributary"
    data_dir: Path = Path(".tributary-data")  # run logs, uploaded samples, local secret key
    secret_key: str | None = None  # Fernet key; generated into data_dir when unset (dev only)

    # Shared Spark session used for previews / validation. spark_remote = Spark Connect URL.
    spark_master: str = "local[*]"
    spark_remote: str | None = None
    preview_extra_packages: list[str] = []
    preview_max_rows: int = 500

    cors_origins: list[str] = ["http://localhost:5173"]

    @property
    def logs_dir(self) -> Path:
        return self.data_dir / "runs"

    @property
    def samples_dir(self) -> Path:
        return self.data_dir / "samples"


@lru_cache
def get_settings() -> Settings:
    return Settings()
