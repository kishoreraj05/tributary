"""Contract checks every plugin should pass. Plugin authors run these in their own tests."""

from __future__ import annotations

import inspect
from typing import Any

from pyspark.sql import DataFrame

from ..core import (
    BatchReadable,
    Check,
    DatasetCheck,
    MetricsSink,
    PluginOptions,
    RowCheck,
    RunContext,
    SchemaResolver,
    SecretProvider,
    Sink,
    Source,
    Transform,
)
from ..registry import Registry, default_registry

_FAMILIES = {
    "source": Source,
    "transform": Transform,
    "sink": Sink,
    "check": Check,
    "metrics_sink": MetricsSink,
    "schema": SchemaResolver,
    "secret_provider": SecretProvider,
}


def static_contract_issues(cls: type) -> list[str]:
    """Checks that need no Spark: family, abstract methods, options model, metadata."""
    issues = []
    family = _FAMILIES.get(getattr(cls, "category", None))
    if family is None or not issubclass(cls, family):
        issues.append(f"category {getattr(cls, 'category', None)!r} does not match its base class")
    if inspect.isabstract(cls):
        issues.append(f"abstract methods not implemented: {', '.join(sorted(cls.__abstractmethods__))}")
    if not (isinstance(cls.Options, type) and issubclass(cls.Options, PluginOptions)):
        issues.append("Options must subclass PluginOptions")
    try:
        cls.metadata()
    except Exception as exc:  # noqa: BLE001
        issues.append(f"metadata()/JSON Schema failed: {exc}")
    if family is Source and not cls.capabilities():
        issues.append("a source must implement BatchReadable and/or StreamReadable")
    if family is Check and not issubclass(cls, (RowCheck, DatasetCheck)):
        issues.append("a check must extend RowCheck or DatasetCheck")
    return issues


def _ctx(spark, registry: Registry | None, limit: int) -> RunContext:
    return RunContext(spark=spark, registry=registry or default_registry(), pipeline_id="contract", env="test",
                      preview=True, preview_limit=limit)


def assert_source_contract(spark, source: Source, opts: Any, *, schema=None, limit: int = 3, registry=None) -> None:
    ctx = _ctx(spark, registry, limit)
    if isinstance(source, BatchReadable):
        assert isinstance(source.read(ctx, opts, schema), DataFrame), "read() must return a DataFrame"
    sample = source.read_preview(ctx, opts, schema, limit)
    assert isinstance(sample, DataFrame), "read_preview() must return a DataFrame"
    assert sample.count() <= limit, "read_preview() must honour the limit"


def assert_sink_contract(spark, sink: Sink, opts: Any, df: DataFrame, *, registry=None) -> None:
    ctx = _ctx(spark, registry, 0)
    assert isinstance(sink.describe(opts), str), "describe() must return a string"
    sink.write(ctx, df, opts)
