from __future__ import annotations

from alembic import context
from sqlalchemy import engine_from_config, pool

from tributary_backend.db import Base
from tributary_backend import models  # noqa: F401 - registers tables
from tributary_backend.settings import get_settings

config = context.config
url = config.get_main_option("sqlalchemy.url") or get_settings().database_url


def run_migrations() -> None:
    connectable = engine_from_config({"sqlalchemy.url": url}, prefix="sqlalchemy.", poolclass=pool.NullPool)
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=Base.metadata)
        with context.begin_transaction():
            context.run_migrations()


run_migrations()
