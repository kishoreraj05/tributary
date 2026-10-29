"""Column-level validation without data: every step is analysed on empty frames."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..core import RowCheck, RunContext
from ..errors import short_error
from ..planner import build_plan
from ..spec import CompiledPipeline
from .util import resolve_source_schema


@dataclass
class NodeAnalysis:
    node_id: str
    ok: bool
    schema: dict[str, Any] | None = None
    error: str | None = None  # Spark could not analyse the node
    # What a sink found wrong with its options for the data it would get, by spec path
    # ("sinks[0].options.merge_keys"), so they show up under the field like compile issues.
    issues: list[dict[str, str]] = field(default_factory=list)

    @property
    def messages(self) -> list[str]:
        return ([self.error] if self.error else []) + [i["message"] for i in self.issues]


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
            schema = frames[step.inputs["input"]].schema
            try:
                issues = _sink_issues(ctx, compiled, step, schema)
            except Exception as exc:  # noqa: BLE001 - a failing check must not hide the schema
                out[sid] = NodeAnalysis(sid, False, schema.jsonValue(), error=short_error(exc, 500))
                continue
            out[sid] = NodeAnalysis(sid, not issues, schema.jsonValue(), issues=issues)
            continue
        try:
            df = _analyze_step(ctx.for_node(sid), step, frames)
            out[sid] = NodeAnalysis(sid, True, df.schema.jsonValue())
            frames[sid] = df
        except Exception as exc:  # noqa: BLE001 - every failure is reported per node
            out[sid] = NodeAnalysis(sid, False, error=short_error(exc, 500))
    ctx.run_cleanups()
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
        ctx.take_reports()  # a verification node's results on an empty frame mean nothing here
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


def _sink_issues(ctx: RunContext, compiled: CompiledPipeline, step: Any, schema: Any) -> list[dict[str, str]]:
    node = step.node
    index = next(i for i, s in enumerate(compiled.spec.sinks) if s is node)
    plugin = ctx.registry.create("sink", node.type)
    issues = []
    for option, message in plugin.validate_input(ctx, schema, node.options):
        path = f"sinks[{index}].options.{option}" if option else f"sinks[{index}]"
        issues.append({"path": path, "message": message})
    return issues
