from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING, Any

from .plugin import Plugin
from .quality import RESULT_DDL, CheckResult

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from .context import RunContext


class MetricsSink(Plugin):
    """Destination for data-quality results (a database table, files, Prometheus, ...)."""

    category = "metrics_sink"

    @abstractmethod
    def emit(self, ctx: RunContext, results: list[CheckResult], opts: Any) -> None: ...


def results_frame(ctx: RunContext, results: list[CheckResult]) -> DataFrame:
    return ctx.spark.createDataFrame([r.as_row() for r in results], schema=RESULT_DDL)
