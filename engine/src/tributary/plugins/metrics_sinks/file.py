from __future__ import annotations

from typing import Literal

from pydantic import Field

from ...core import MetricsSink, PluginOptions
from ...core.metrics_sink import results_frame


class FileMetricsOptions(PluginOptions):
    path: str = Field(..., description="Directory to append results to")
    format: Literal["json", "parquet", "csv"] = "json"


class FileMetricsSink(MetricsSink):
    """Appends data-quality results as files (one row per check)."""

    type_name = "file"
    display_name = "Files"
    icon = "file"
    Options = FileMetricsOptions

    def emit(self, ctx, results, opts: FileMetricsOptions) -> None:
        writer = results_frame(ctx, results).coalesce(1).write.mode("append").format(opts.format)
        if opts.format == "csv":
            writer = writer.option("header", "true")
        writer.save(opts.path)
