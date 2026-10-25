from __future__ import annotations

import dataclasses

from pyspark import StorageLevel

from ..core import RunContext
from ..errors import SpecError, SpecIssue
from ..planner import PhysicalPlan
from .executor import PlanExecutor
from .util import resolve_source_schema


def run_streaming(plan: PhysicalPlan, ctx: RunContext, *, await_termination: bool = True):
    """One streaming query per pipeline: the source is read once per micro-batch, and the
    whole plan (transforms, quality gates, every sink) runs on that persisted batch."""
    step = plan.stream_source
    settings = plan.spec.pipeline.streaming
    if not settings.checkpoint_location:
        raise SpecError([SpecIssue("pipeline.streaming.checkpoint_location", "required to run a streaming pipeline")])

    plugin = ctx.registry.create("source", step.node.type)
    stream = plugin.read_stream(ctx, step.node.options, resolve_source_schema(ctx, step.node))

    def process(batch_df, batch_id: int) -> None:
        batch = batch_df.persist(StorageLevel.MEMORY_AND_DISK)
        try:
            # The batch belongs to a cloned session; run the plan on it so temp views and
            # static sources live in the same session as the batch.
            batch_ctx = dataclasses.replace(ctx.for_batch(batch_id), spark=batch_df.sparkSession)
            with PlanExecutor(plan, batch_ctx) as executor:
                executor.run(source_overrides={step.id: batch})
        finally:
            batch.unpersist()

    writer = (
        stream.writeStream.queryName(plan.spec.pipeline.id)
        .option("checkpointLocation", settings.checkpoint_location)
        .foreachBatch(process)
    )
    if settings.trigger == "available_now":
        writer = writer.trigger(availableNow=True)
    elif settings.trigger == "once":
        writer = writer.trigger(once=True)
    else:
        writer = writer.trigger(processingTime=settings.interval)
    query = writer.start()
    if await_termination:
        query.awaitTermination()
    return query
