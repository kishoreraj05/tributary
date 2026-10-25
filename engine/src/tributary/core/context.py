from __future__ import annotations

import dataclasses
import logging
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from ..registry import Registry


@dataclass
class RunContext:
    """Everything a plugin may need at runtime, injected by the executor."""

    spark: SparkSession
    registry: Registry
    pipeline_id: str
    env: str
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    version: str | None = None
    batch_id: int | None = None
    # Preview mode: bounded source reads, no metrics/quarantine side effects.
    preview: bool = False
    preview_limit: int = 20
    log: logging.Logger = field(default_factory=lambda: logging.getLogger("tributary"))

    def for_batch(self, batch_id: int) -> RunContext:
        return dataclasses.replace(self, batch_id=batch_id)
