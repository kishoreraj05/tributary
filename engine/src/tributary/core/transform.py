from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING, Any, ClassVar

from .plugin import Plugin

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from .context import RunContext


class Transform(Plugin):
    category = "transform"
    # False: no node of this type may be upstream of another node of this type (the spec is
    # rejected with a message naming both); they go on separate branches instead.
    chainable: ClassVar[bool] = True

    @classmethod
    def metadata(cls) -> dict[str, Any]:
        return {**super().metadata(), "chainable": cls.chainable}

    @abstractmethod
    def apply(self, ctx: RunContext, inputs: dict[str, DataFrame], opts: Any) -> DataFrame:
        """``inputs`` maps each input alias (the ``as`` name) to its DataFrame."""

    def validate(self, opts: Any, input_aliases: list[str]) -> list[str]:
        """Static checks run at compile time, without Spark. Return error messages."""
        return []

    def complexity_hint(self, opts: Any) -> int:
        """Rough count of expensive operations (joins, aggregations) for resource sizing."""
        return 0

    def is_passthrough(self, opts: Any) -> bool:
        """True when ``apply`` returns its (single) input unchanged and only observes it, like a
        verification node. The planner then counts the node's own read of the input plus its
        consumers' reads against the input, so the input is persisted rather than recomputed."""
        return False
