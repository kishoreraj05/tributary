from __future__ import annotations

from tributary.core import MetricsSink, PluginOptions

from .db import session_factory
from .models import QualityResult

_FIELDS = (
    "pipeline_id", "env", "node_id", "quality_id", "check_id", "check_type", "level", "action",
    "total_rows", "failed_rows", "observed_value", "passed", "batch_id", "ts",
)


class BackendMetricsSink(MetricsSink):
    """Records every check result in the backend database (the run's DQ history).

    Registered only inside the run worker and appended to every quality block, so it
    never appears in the component catalog users pick from.
    """

    type_name = "tributary_backend"
    Options = PluginOptions

    def emit(self, ctx, results, opts) -> None:
        with session_factory()() as session:
            session.add_all(
                QualityResult(run_id=ctx.run_id, **{f: getattr(r, f) for f in _FIELDS}) for r in results
            )
            session.commit()
