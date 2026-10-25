from __future__ import annotations

import urllib.request
from collections import defaultdict
from urllib.parse import quote

from pydantic import Field

from ...core import MetricsSink, PluginOptions


class PrometheusOptions(PluginOptions):
    url: str = Field(..., description="Pushgateway base URL, e.g. http://localhost:9091")
    job: str = "tributary"
    timeout_seconds: float = 10


def _label(value: object) -> str:
    return str(value).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def render(results) -> str:
    metrics: dict[str, list[str]] = defaultdict(list)
    for r in results:
        labels = f'node="{_label(r.node_id)}",check="{_label(r.check_id)}",check_type="{_label(r.check_type)}"'
        metrics["tributary_dq_check_passed"].append(f"tributary_dq_check_passed{{{labels}}} {int(r.passed)}")
        metrics["tributary_dq_total_rows"].append(f"tributary_dq_total_rows{{{labels}}} {r.total_rows}")
        if r.failed_rows is not None:
            metrics["tributary_dq_failed_rows"].append(f"tributary_dq_failed_rows{{{labels}}} {r.failed_rows}")
        if r.observed_value is not None:
            metrics["tributary_dq_observed_value"].append(f"tributary_dq_observed_value{{{labels}}} {r.observed_value}")
    out = []
    for name, lines in metrics.items():
        out.append(f"# TYPE {name} gauge")
        out.extend(lines)
    return "\n".join(out) + "\n"


class PrometheusMetricsSink(MetricsSink):
    """Pushes data-quality gauges to a Prometheus Pushgateway."""

    type_name = "prometheus"
    display_name = "Prometheus Pushgateway"
    icon = "chart"
    Options = PrometheusOptions

    def emit(self, ctx, results, opts: PrometheusOptions) -> None:
        if not results:
            return
        # Grouping by quality block keeps one block's push from replacing another's.
        grouping = "/".join(
            f"{k}/{quote(str(v), safe='')}"
            for k, v in (("pipeline", ctx.pipeline_id), ("env", ctx.env), ("quality", results[0].quality_id))
        )
        url = f"{opts.url.rstrip('/')}/metrics/job/{quote(opts.job, safe='')}/{grouping}"
        request = urllib.request.Request(
            url, data=render(results).encode(), method="PUT", headers={"Content-Type": "text/plain; version=0.0.4"}
        )
        with urllib.request.urlopen(request, timeout=opts.timeout_seconds) as response:
            response.read()
