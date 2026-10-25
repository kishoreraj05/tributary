"""Runs one quality gate: every check in a single aggregate pass, then the action."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import reduce
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..core import CheckResult, RowCheck, RunContext
from ..errors import QualityFailure

_FLAG = "__dq_fail_{}"
_METRIC = "__dq_metric_{}"
_TOTAL = "__dq_total"


@dataclass
class QualityOutcome:
    df: DataFrame
    results: list[CheckResult]
    persisted: list[DataFrame] = field(default_factory=list)


def _checks(ctx: RunContext, q: Any) -> tuple[list, list]:
    """(row checks, dataset checks) as (index, check id, plugin, options) entries."""
    row_checks, dataset_checks = [], []
    for i, check in enumerate(q.checks):
        plugin = ctx.registry.create("check", check.type)
        entry = (i, check.id or f"{check.type}_{i + 1}", plugin, check)
        (row_checks if isinstance(plugin, RowCheck) else dataset_checks).append(entry)
    return row_checks, dataset_checks


def annotate_failures(ctx: RunContext, step: Any, df: DataFrame) -> DataFrame:
    """The checked rows plus ``_dq_failed_checks`` (the row checks each row fails), failing
    rows first. Used to preview a quality block without applying its action."""
    row_checks, _ = _checks(ctx, step.node)
    failed = F.array(
        *[F.when(F.coalesce(plugin.failure_condition(opts), F.lit(False)), F.lit(cid)) for _, cid, plugin, opts in row_checks]
    )
    column = F.filter(failed, lambda x: x.isNotNull()) if row_checks else F.array().cast("array<string>")
    annotated = df.withColumn("_dq_failed_checks", column)
    return annotated.orderBy(F.size("_dq_failed_checks").desc())


def run_quality(ctx: RunContext, step: Any, df: DataFrame) -> QualityOutcome:
    q = step.node
    row_checks, dataset_checks = _checks(ctx, q)

    flags = {_FLAG.format(i): F.coalesce(plugin.failure_condition(opts), F.lit(False)) for i, _, plugin, opts in row_checks}
    flagged = df.withColumns(flags) if flags else df
    persisted = []
    if step.persist_flags and flags:
        flagged = flagged.persist(StorageLevel.MEMORY_AND_DISK)
        persisted.append(flagged)

    aggregates = [F.count(F.lit(1)).alias(_TOTAL)]
    aggregates += [F.sum(F.col(name).cast("long")).alias(name) for name in flags]
    aggregates += [plugin.metric(opts).alias(_METRIC.format(i)) for i, _, plugin, opts in dataset_checks]
    stats = flagged.agg(*aggregates).collect()[0]
    total = int(stats[_TOTAL] or 0)

    common = dict(
        run_id=ctx.run_id,
        pipeline_id=ctx.pipeline_id,
        version=ctx.version,
        env=ctx.env,
        node_id=step.attached_to,
        quality_id=q.id,
        action=q.action,
        batch_id=ctx.batch_id,
        ts=datetime.now(timezone.utc).isoformat(),
    )
    results = []
    for i, check_id, plugin, opts in row_checks:
        failed = int(stats[_FLAG.format(i)] or 0)
        results.append(
            CheckResult(
                **common,
                check_id=check_id,
                check_type=opts.type,
                level="row",
                total_rows=total,
                failed_rows=failed,
                observed_value=float(failed),
                passed=plugin.passed(failed, total, opts),
            )
        )
    for i, check_id, plugin, opts in dataset_checks:
        raw = stats[_METRIC.format(i)]
        observed = float(raw) if raw is not None else None
        results.append(
            CheckResult(
                **common,
                check_id=check_id,
                check_type=opts.type,
                level="dataset",
                total_rows=total,
                failed_rows=None,
                observed_value=observed,
                passed=plugin.passed(observed, total, opts),
            )
        )

    failing = [r.check_id for r in results if not r.passed]
    ctx.log.info("quality %s on %s: %d checks, %d failing %s", q.id, step.attached_to, len(results), len(failing), failing)
    if not ctx.preview:
        for target in q.results_sinks:
            ctx.registry.create("metrics_sink", target.type).emit(ctx, results, target.options)
    if q.action == "fail" and failing:
        raise QualityFailure(q.id, results)

    if q.action in ("drop", "quarantine") and flags:
        any_failed = reduce(lambda a, b: a | b, [F.col(name) for name in flags])
        if q.action == "quarantine" and not ctx.preview:
            failed_ids = F.array(*[F.when(F.col(_FLAG.format(i)), F.lit(cid)) for i, cid, _, _ in row_checks])
            bad = (
                flagged.filter(any_failed)
                .withColumn("_dq_failed_checks", F.filter(failed_ids, lambda x: x.isNotNull()))
                .drop(*flags)
            )
            target = q.quarantine_sink
            ctx.registry.create("sink", target.type).write(ctx, bad, target.options)
        return QualityOutcome(flagged.filter(~any_failed).drop(*flags), results, persisted)
    return QualityOutcome(df, results, persisted)
