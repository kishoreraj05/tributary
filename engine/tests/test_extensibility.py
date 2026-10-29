"""Adding a plugin needs no engine change: it shows up in the spec, the schema and the runtime."""

from __future__ import annotations

import pytest
from pydantic import Field
from pyspark.sql import functions as F

from conftest import pipeline
from tributary import PluginError, compile_pipeline, export_schema, run
from tributary.core import PluginOptions, Transform
from tributary.registry import Registry


class UpperOptions(PluginOptions):
    columns: list[str] = Field(..., min_length=1)


class UpperTransform(Transform):
    """Upper-cases string columns."""

    type_name = "upper"
    Options = UpperOptions

    def apply(self, ctx, inputs, opts):
        [df] = inputs.values()
        return df.select(*[F.upper(c).alias(c) if c in opts.columns else F.col(c) for c in df.columns])


@pytest.fixture
def registry():
    reg = Registry()
    reg.register(UpperTransform)
    return reg


def test_custom_transform_is_validated_exported_and_run(spark, tmp_path, orders_csv, registry):
    doc = pipeline(tmp_path, orders_csv)
    doc["transforms"] = [{"id": "orders_clean", "type": "upper", "inputs": ["orders_src"], "options": {"columns": ["country"]}}]
    compiled = compile_pipeline(doc, registry=registry)
    assert compiled.spec.transforms[0].options.columns == ["country"]

    types = [p["type"] for p in export_schema(registry)["components"]["transform"]]
    assert types == ["deequ", "rest_api_enrichment", "sql", "upper"]

    run(compiled, spark=spark)
    assert {r.country for r in spark.read.parquet(str(tmp_path / "out" / "orders")).collect()} == {"IN", "US", "DE", "USA", "GB"}


def test_registry_rejects_incomplete_or_conflicting_plugins(registry):
    class Broken(Transform):
        type_name = "broken"

    with pytest.raises(PluginError, match="does not implement: apply"):
        registry.register(Broken)

    class Clash(UpperTransform):
        pass

    with pytest.raises(PluginError, match="already registered"):
        registry.register(Clash)


def test_schema_export_has_every_family():
    exported = export_schema()
    assert set(exported["pipeline"]["properties"]) >= {"pipeline", "sources", "transforms", "quality", "sinks"}
    kafka = next(p for p in exported["components"]["source"] if p["type"] == "kafka")
    assert kafka["capabilities"] == ["batch", "streaming"]
    assert "brokers" in kafka["options_schema"]["properties"]


def test_builtins_load_without_entry_point_metadata(monkeypatch):
    """An install whose metadata predates a plugin (editable install not re-run) still gets it."""
    import tributary.registry as registry_module

    monkeypatch.setattr(registry_module, "entry_points", lambda group: [])
    reg = Registry()
    assert {"iceberg", "hudi"} <= set(reg.plugins("sink")) and "kafka" in reg.plugins("source")


def test_builtin_table_matches_pyproject_entry_points():
    import tomllib
    from pathlib import Path

    from tributary.plugins import BUILTIN
    from tributary.registry import group_for

    project = tomllib.loads((Path(__file__).resolve().parents[1] / "pyproject.toml").read_text())["project"]
    declared = {category: project["entry-points"][group_for(category)] for category in BUILTIN}
    assert declared == BUILTIN


def test_a_source_records_its_progress_only_after_the_sinks_wrote(spark, tmp_path, orders_csv):
    """``commit_read`` is the engine's one promise to a source that tracks where it stopped:
    it comes after every sink has written, not for previews, and not when a sink fails."""
    from tributary import preview
    from tributary.core import BatchReadable, Source
    from tributary.errors import TributaryError

    calls: list[tuple[str, str]] = []

    class CountedOptions(PluginOptions):
        path: str

    class CountedSource(Source, BatchReadable):
        type_name = "counted"
        Options = CountedOptions

        def read(self, ctx, opts, schema):
            calls.append(("read", ctx.run_id))
            return ctx.spark.read.option("header", "true").csv(opts.path)

        def commit_read(self, ctx, opts):
            calls.append(("commit", ctx.run_id))

    registry = Registry()
    registry.register(CountedSource)
    doc = pipeline(tmp_path, orders_csv, quality=[])
    doc["sources"] = [{"id": "orders_src", "type": "counted", "options": {"path": str(orders_csv)}}]

    summary = run(doc, spark=spark, registry=registry)
    assert calls == [("read", summary.run_id), ("commit", summary.run_id)]

    calls.clear()
    preview(doc, "orders_clean", spark=spark, registry=registry, limit=2)
    assert [c for c, _ in calls] == ["read"]  # a preview never commits

    calls.clear()
    doc["sinks"][0]["options"]["path"] = "/proc/nope/cannot/write"
    with pytest.raises(Exception):  # noqa: B017 - the sink's own failure, whatever it is
        run(doc, spark=spark, registry=registry)
    assert [c for c, _ in calls] == ["read"]  # the failed run recorded nothing

    calls.clear()
    doc["sinks"][0]["options"]["path"] = str(tmp_path / "out2")
    doc["quality"] = [{"id": "gate", "attach_to": "orders_clean", "action": "fail",
                       "checks": [{"type": "not_null", "column": "order_id"}]}]
    with pytest.raises(TributaryError):
        run(doc, spark=spark, registry=registry)
    assert [c for c, _ in calls] == ["read"]  # a failing gate stops before the sinks, so before the commit
