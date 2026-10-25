from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from pyspark.sql import functions as F
from pyspark.sql.types import TimestampNTZType, TimestampType

from ..core import RunContext
from ..errors import TributaryError
from ..planner import build_plan
from ..spec import CompiledPipeline
from .executor import PlanExecutor
from .quality import annotate_failures
from .util import read_sample, to_jsonable


@dataclass
class PreviewResult:
    node_id: str
    kind: str
    schema: dict[str, Any]
    rows: list[dict[str, Any]]
    quality: list[dict[str, Any]] = field(default_factory=list)
    target: str | None = None  # sinks: where the data would be written
    written: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def preview_node(
    compiled: CompiledPipeline,
    node_id: str,
    spark,
    *,
    limit: int = 20,
    sample_data: dict[str, str] | None = None,
    write_sinks: bool = False,
) -> PreviewResult:
    """Runs only what ``node_id`` depends on, on bounded source reads, and returns its rows.

    Quality gates attached to the node run too, so the result shows their pass/fail counts.
    For a quality block itself, the rows are the ones it checks, failing rows first, with a
    ``_dq_failed_checks`` column. Sinks are a dry run unless ``write_sinks`` is set.
    """
    plan = build_plan(compiled)
    if node_id not in plan.steps:
        raise TributaryError(f"unknown node '{node_id}' (nodes: {', '.join(plan.steps)})")
    step = plan.steps[node_id]
    ctx = RunContext(
        spark=spark,
        registry=compiled.registry,
        pipeline_id=compiled.pipeline_id,
        env=compiled.env,
        preview=True,
        preview_limit=limit,
    )
    overrides = {}
    for sid, path in (sample_data or {}).items():
        if sid not in plan.steps or plan.steps[sid].kind != "source":
            raise TributaryError(f"sample data given for '{sid}', which is not a source")
        overrides[sid] = read_sample(spark, path)

    targets = {node_id} | {s.id for s in plan.steps.values() if s.kind == "quality" and s.attached_to == node_id}
    with PlanExecutor(plan, ctx) as executor:
        result = executor.run(source_overrides=overrides, targets=targets, write_sinks=False)
        df = result.frames[node_id]
        if step.kind == "quality":
            df = annotate_failures(ctx, step, result.frames[step.inputs["input"]])
        rows = [to_jsonable(r.asDict(recursive=True)) for r in _for_display(df).limit(limit).collect()]
        target, written = None, False
        if step.kind == "sink":
            plugin = compiled.registry.create("sink", step.node.type)
            target = plugin.describe(step.node.options)
            if write_sinks:
                plugin.write(ctx, df.limit(limit), step.node.options)
                written = True
        return PreviewResult(
            node_id=node_id,
            kind=step.kind,
            schema=df.schema.jsonValue(),
            rows=rows,
            quality=[r.as_dict() for r in result.quality],
            target=target,
            written=written,
        )


def _for_display(df):
    """Timestamps rendered by Spark in the session time zone. Collected as Python datetimes
    they would be shifted into the driver machine's local zone instead."""
    columns = [
        F.col(f"`{f.name}`").cast("string").alias(f.name)
        if isinstance(f.dataType, (TimestampType, TimestampNTZType))
        else F.col(f"`{f.name}`")
        for f in df.schema.fields
    ]
    return df.select(*columns)
