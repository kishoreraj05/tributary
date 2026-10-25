from __future__ import annotations

from ...core import MetricsSink, PluginOptions


class ConsoleMetricsSink(MetricsSink):
    """Prints data-quality results to stdout."""

    type_name = "console"
    display_name = "Console"
    icon = "terminal"
    Options = PluginOptions

    def emit(self, ctx, results, opts) -> None:
        batch = f", batch {ctx.batch_id}" if ctx.batch_id is not None else ""
        lines = [f"Data quality ({ctx.pipeline_id}, env={ctx.env}{batch})"]
        for r in results:
            status = "PASS" if r.passed else "FAIL"
            detail = f"failed={r.failed_rows}/{r.total_rows}" if r.level == "row" else f"observed={r.observed_value}"
            lines.append(f"  [{status}] {r.quality_id}.{r.check_id} ({r.check_type}) on {r.node_id}: {detail}")
        print("\n".join(lines))
