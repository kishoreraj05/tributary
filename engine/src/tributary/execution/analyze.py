"""Column-level validation without data: every step is analysed on empty frames."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..core import RowCheck, RunContext
from ..planner import build_plan
from ..spec import CompiledPipeline
from .util import resolve_source_schema


@dataclass
class NodeAnalysis:
    node_id: str
    ok: bool
    schema: dict[str, Any] | None = None
    error: str | None = None


def analyze_pipeline(compiled: CompiledPipeline, spark) -> dict[str, NodeAnalysis]:
    plan = build_plan(compiled)
    ctx = RunContext(
        spark=spark,
        registry=compiled.registry,
        pipeline_id=compiled.pipeline_id,
        env=compiled.env,
        preview=True,
        preview_limit=0,
    )
    frames: dict[str, Any] = {}
    out: dict[str, NodeAnalysis] = {}
    for sid in plan.order:
        step = plan.steps[sid]
        missing = [u for u in step.inputs.values() if u not in frames]
        if missing:
            out[sid] = NodeAnalysis(sid, False, error=f"skipped: upstream {', '.join(missing)} has errors")
            continue
        if step.kind == "sink":
            out[sid] = NodeAnalysis(sid, True, frames[step.inputs["input"]].schema.jsonValue())
            continue
        try:
            df = _analyze_step(ctx, step, frames)
            out[sid] = NodeAnalysis(sid, True, df.schema.jsonValue())
            frames[sid] = df
        except Exception as exc:  # noqa: BLE001 - every failure is reported per node
            out[sid] = NodeAnalysis(sid, False, error=_short(exc))
    return out


def _analyze_step(ctx: RunContext, step: Any, frames: dict[str, Any]):
    registry, node = ctx.registry, step.node
    if step.kind == "source":
        plugin = registry.create("source", node.type)
        resolved = resolve_source_schema(ctx, node)
        struct = plugin.output_schema(node.options, resolved)
        if struct is not None:
            return ctx.spark.createDataFrame([], struct)
        return plugin.read_preview(ctx, node.options, resolved, 0)
    if step.kind == "transform":
        plugin = registry.create("transform", node.type)
        df = plugin.apply(ctx, {alias: frames[sid] for alias, sid in step.inputs.items()}, node.options)
        df.schema  # forces analysis
        return df
    df = frames[step.inputs["input"]]
    row_exprs, metric_exprs = [], []
    for i, check in enumerate(node.checks):
        plugin = registry.create("check", check.type)
        if isinstance(plugin, RowCheck):
            row_exprs.append(plugin.failure_condition(check).alias(f"c{i}"))
        else:
            metric_exprs.append(plugin.metric(check).alias(f"c{i}"))
    if row_exprs:
        df.select(*row_exprs).schema
    if metric_exprs:
        df.agg(*metric_exprs).schema
    return df


def _short(exc: Exception) -> str:
    text = str(exc).strip() or type(exc).__name__
    return text.splitlines()[0][:500]
