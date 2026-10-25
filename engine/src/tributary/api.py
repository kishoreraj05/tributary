"""Programmatic entry points (the CLI and, later, the backend use these)."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .core import CheckResult, RunContext
from .execution import PlanExecutor, PreviewResult, analyze_pipeline, preview_node, run_streaming
from .execution.analyze import NodeAnalysis
from .planner import PhysicalPlan, build_plan
from .registry import Registry, default_registry
from .runtime import create_session, decide_resources
from .spec import CompiledPipeline
from .spec import compile_pipeline as _compile
from .spec.schema_export import export_schema as _export_schema

Source = str | Path | Mapping[str, Any] | CompiledPipeline


@dataclass
class RunSummary:
    run_id: str
    pipeline_id: str
    env: str
    sinks_written: list[str] = field(default_factory=list)
    quality: list[CheckResult] = field(default_factory=list)


def compile_pipeline(
    source: Source, env: str | None = None, *, registry: Registry | None = None, resolve_secrets: bool = False
) -> CompiledPipeline:
    return _compile(source, env, registry=registry, resolve_secrets=resolve_secrets)


def plan(source: Source, env: str | None = None, *, registry: Registry | None = None) -> PhysicalPlan:
    return build_plan(compile_pipeline(source, env, registry=registry))


def _session(compiled: CompiledPipeline, spark):
    return spark if spark is not None else create_session(compiled, decide_resources(compiled))


def run(source: Source, env: str | None = None, *, spark=None, registry: Registry | None = None,
        await_termination: bool = True, run_id: str | None = None, version: str | None = None):
    """Batch: runs to completion and returns a RunSummary. Streaming: returns the StreamingQuery."""
    compiled = compile_pipeline(source, env, registry=registry, resolve_secrets=True)
    physical = build_plan(compiled)
    spark = _session(compiled, spark)
    ctx = RunContext(spark=spark, registry=compiled.registry, pipeline_id=compiled.pipeline_id, env=compiled.env,
                     version=version)
    if run_id:
        ctx.run_id = run_id
    if compiled.mode == "streaming":
        return run_streaming(physical, ctx, await_termination=await_termination)
    with PlanExecutor(physical, ctx) as executor:
        result = executor.run()
    return RunSummary(ctx.run_id, compiled.pipeline_id, compiled.env, result.sinks_written, result.quality)


def preview(source: Source, node_id: str, env: str | None = None, *, spark=None, limit: int = 20,
            sample_data: dict[str, str] | None = None, write_sinks: bool = False,
            registry: Registry | None = None) -> PreviewResult:
    compiled = compile_pipeline(source, env, registry=registry, resolve_secrets=True)
    return preview_node(compiled, node_id, _session(compiled, spark), limit=limit, sample_data=sample_data,
                        write_sinks=write_sinks)


def analyze(source: Source, env: str | None = None, *, spark=None,
            registry: Registry | None = None) -> dict[str, NodeAnalysis]:
    compiled = compile_pipeline(source, env, registry=registry, resolve_secrets=True)
    return analyze_pipeline(compiled, _session(compiled, spark))


def export_schema(registry: Registry | None = None) -> dict[str, Any]:
    return _export_schema(registry)


def list_components(registry: Registry | None = None) -> dict[str, list[dict[str, Any]]]:
    return (registry or default_registry()).catalog()
