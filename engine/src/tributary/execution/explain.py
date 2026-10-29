"""Spark's physical plan of every source and transform, as a run would build them.

Reads are real but lazy (nothing is collected), so the plans show what reaches each
source: the filters Spark pushed down, the columns it pruned and, for table formats that
report it, the partitions and files it skipped. Quality gates are passed through without
being evaluated; their row filtering happens at run time only.
"""

from __future__ import annotations

from typing import Any

from ..core import BatchReadable, RunContext
from ..errors import TributaryError
from ..planner import build_plan
from ..spec import CompiledPipeline
from .util import resolve_source_schema


def explain_pipeline(compiled: CompiledPipeline, spark) -> dict[str, str]:
    """{node id: formatted physical plan} for sources and transforms, in plan order."""
    plan = build_plan(compiled)
    ctx = RunContext(spark=spark, registry=compiled.registry, pipeline_id=compiled.pipeline_id, env=compiled.env)
    frames: dict[str, Any] = {}
    plans: dict[str, str] = {}
    for sid in plan.order:
        step = plan.steps[sid]
        if step.kind == "sink":
            continue
        if step.kind == "quality":
            frames[sid] = frames[step.inputs["input"]]
            continue
        node = step.node
        if step.kind == "source":
            plugin = ctx.registry.create("source", node.type)
            if not isinstance(plugin, BatchReadable):
                raise TributaryError(f"source '{sid}' ({node.type}) is streaming-only; explain covers batch reads")
            df = plugin.read(ctx, node.options, resolve_source_schema(ctx, node))
        else:
            plugin = ctx.registry.create("transform", node.type)
            df = plugin.apply(ctx, {alias: frames[up] for alias, up in step.inputs.items()}, node.options)
        frames[sid] = df
        plans[sid] = physical_plan(df)
    return plans


def physical_plan(df) -> str:
    """Spark's formatted physical plan (what ``df.explain("formatted")`` prints)."""
    jvm = df.sparkSession._jvm
    mode = jvm.org.apache.spark.sql.execution.ExplainMode.fromString("formatted")
    return df._jdf.queryExecution().explainString(mode)
