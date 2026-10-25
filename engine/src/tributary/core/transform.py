from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING, Any

from .plugin import Plugin

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from .context import RunContext


class Transform(Plugin):
    category = "transform"

    @abstractmethod
    def apply(self, ctx: RunContext, inputs: dict[str, DataFrame], opts: Any) -> DataFrame:
        """``inputs`` maps each input alias (the ``as`` name) to its DataFrame."""

    def validate(self, opts: Any, input_aliases: list[str]) -> list[str]:
        """Static checks run at compile time, without Spark. Return error messages."""
        return []

    def complexity_hint(self, opts: Any) -> int:
        """Rough count of expensive operations (joins, aggregations) for resource sizing."""
        return 0
