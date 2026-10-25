from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING, Any

from .plugin import Plugin

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from .context import RunContext


class Sink(Plugin):
    """Writes a static DataFrame.

    Streaming pipelines call ``write`` once per micro-batch (via foreachBatch), with
    ``ctx.batch_id`` set, so every sink works in both modes. Sinks that can write
    idempotently should use ``ctx.batch_id`` to do so.
    """

    category = "sink"

    @abstractmethod
    def write(self, ctx: RunContext, df: DataFrame, opts: Any) -> None: ...

    def describe(self, opts: Any) -> str:
        return self.type_name
