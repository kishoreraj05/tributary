from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config

from .settings import get_settings


def alembic_config(database_url: str | None = None) -> Config:
    config = Config()
    config.set_main_option("script_location", str(Path(__file__).parent / "migrations"))
    config.set_main_option("sqlalchemy.url", database_url or get_settings().database_url)
    return config


def upgrade(database_url: str | None = None) -> None:
    command.upgrade(alembic_config(database_url), "head")
