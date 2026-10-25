"""Executes a physical plan over static DataFrames.

Batch runs use it directly; streaming runs call it once per micro-batch with the
batch frame injected as the stream source; previews run it on a subset of steps.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

from pyspark import StorageLevel
from pyspark.sql import DataFrame

from ..core import CheckResult, RunContext
from ..errors import TributaryError
from ..planner import PhysicalPlan, Step
from .quality import run_quality
from .util import resolve_source_schema


@dataclass
class ExecutionResult:
    frames: dict[str, DataFrame]
    quality: list[CheckResult] = field(default_factory=list)
    sinks_written: list[str] = field(default_factory=list)


class PlanExecutor:
    """Use as a context manager: persisted frames are released on exit."""

    def __init__(self, plan: PhysicalPlan, ctx: RunContext):
        self.plan = plan
        self.ctx = ctx
        self._persisted: list[DataFrame] = []

    def __enter__(self) -> PlanExecutor:
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def close(self) -> None:
        for df in self._persisted:
            try:
                df.unpersist()
            except Exception:  # noqa: BLE001 - cleanup must not mask the real error
                self.ctx.log.warning("could not unpersist a cached frame", exc_info=True)
        self._persisted.clear()

    def run(
        self,
        *,
        source_overrides: dict[str, DataFrame] | None = None,
        targets: set[str] | None = None,
        write_sinks: bool = True,
    ) -> ExecutionResult:
        overrides = source_overrides or {}
        needed = self.plan.ancestors(targets) if targets else set(self.plan.order)
        result = ExecutionResult(frames={})
        sinks: list[Step] = []
        for sid in self.plan.order:
            if sid not in needed:
                continue
            step = self.plan.steps[sid]
            if step.kind == "sink":
                sinks.append(step)
            else:
                result.frames[sid] = self._evaluate(step, result, overrides)
        # Sinks run last: every gate (including 'fail' ones) has passed by now.
        if write_sinks:
            result.sinks_written = self._write_sinks(sinks, result.frames)
        for step in sinks:
            result.frames[step.id] = result.frames[step.inputs["input"]]
        return result

    def _evaluate(self, step: Step, result: ExecutionResult, overrides: dict[str, DataFrame]) -> DataFrame:
        ctx, node, frames = self.ctx, step.node, result.frames
        if step.kind == "quality":
            outcome = run_quality(ctx, step, frames[step.inputs["input"]])
            result.quality.extend(outcome.results)
            self._persisted.extend(outcome.persisted)
            return outcome.df
        if step.kind == "source":
            if step.id in overrides:
                return overrides[step.id]
            df = self._read_source(step)
        else:
            plugin = ctx.registry.create("transform", node.type)
            df = plugin.apply(ctx, {alias: frames[sid] for alias, sid in step.inputs.items()}, node.options)
        if step.cache:
            df = df.persist(getattr(StorageLevel, node.storage_level))
            self._persisted.append(df)
        return df

    def _read_source(self, step: Step) -> DataFrame:
        ctx, node = self.ctx, step.node
        plugin = ctx.registry.create("source", node.type)
        schema = resolve_source_schema(ctx, node)
        if ctx.preview:
            return plugin.read_preview(ctx, node.options, schema, ctx.preview_limit)
        if step.stream:
            raise TributaryError(f"'{step.id}' is a streaming source; run the pipeline with the streaming executor")
        return plugin.read(ctx, node.options, schema)

    def _write_sinks(self, sinks: list[Step], frames: dict[str, DataFrame]) -> list[str]:
        def write(step: Step) -> str:
            plugin = self.ctx.registry.create("sink", step.node.type)
            self.ctx.log.info("writing sink %s: %s", step.id, plugin.describe(step.node.options))
            plugin.write(self.ctx, frames[step.inputs["input"]], step.node.options)
            return step.id

        settings = self.plan.spec.execution
        if settings.parallel_sinks and len(sinks) > 1:
            with ThreadPoolExecutor(max_workers=min(len(sinks), settings.max_parallel_sinks)) as pool:
                futures = [pool.submit(write, s) for s in sinks]
                return [f.result() for f in futures]
        return [write(s) for s in sinks]
