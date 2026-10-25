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
    assert types == ["sql", "upper"]

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
