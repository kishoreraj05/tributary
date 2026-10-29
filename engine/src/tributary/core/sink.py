from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING, Any

from .plugin import Plugin

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.types import StructType

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

    def validate_input(self, ctx: RunContext, schema: StructType, opts: Any) -> list[tuple[str, str]]:
        """Design-time checks of the options against the schema of the data the sink will
        receive, known from analysing the upstream nodes on empty frames (no data is read).

        Returns ``(option field, message)`` pairs, so the UI can show each message under
        the field it concerns; an empty field name reports on the node itself. Mirrors
        ``Transform.validate`` for the things only a sink knows: key columns, partition
        columns, conditions, and whatever the existing target can tell about compatibility.
        """
        return []

    def describe(self, opts: Any) -> str:
        return self.type_name
