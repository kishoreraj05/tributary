"""The Iceberg sink: options, session configuration and real writes through a Hadoop catalog.

Tests that write need the Iceberg runtime jar (see ``iceberg_jar`` in conftest); the
others run without Spark.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError
from pyspark.sql.types import ArrayType, IntegerType, StringType, StructField, StructType

from conftest import pipeline
from tributary import TributaryError, compile_pipeline, preview, run
from tributary.core import RunContext
from tributary.plugins.sinks.iceberg import (
    IcebergSink,
    IcebergSinkOptions,
    configure_catalog,
    schema_ddl,
)
from tributary.registry import default_registry
from tributary.runtime import decide_resources, required_conf, required_packages, session_conf
from tributary.testing import assert_sink_contract


def iceberg_sink(warehouse, table="sales.orders", **options) -> dict:
    return {
        "id": "orders_out",
        "type": "iceberg",
        "input": "orders_clean",
        "options": {"catalog_type": "hadoop", "warehouse": str(warehouse), "table": table, **options},
    }


def iceberg_pipeline(tmp_path, orders_csv, **options) -> dict:
    return pipeline(tmp_path, orders_csv, sinks=[iceberg_sink(tmp_path / "warehouse", **options)])


def rows(spark, opts: IcebergSinkOptions) -> dict[int | None, tuple[float, str]]:
    return {r.order_id: (r.amount, r.country) for r in spark.table(opts.qualified_table()).collect()}


# --- no Spark needed ---------------------------------------------------------------------


def test_options_are_validated():
    ok = IcebergSinkOptions(warehouse="/w", table="sales.orders", partition_by=["country", " days(ts) ", "bucket(4, id)"])
    assert ok.partition_by == ["country", "days(ts)", "bucket(4, id)"]
    for bad, message in [
        ({"warehouse": "/w", "table": "orders"}, "pattern"),  # a namespace is required
        ({"table": "a.b"}, r"warehouse\n  Value error, required for a hadoop catalog"),
        ({"catalog_type": "rest", "table": "a.b"}, r"uri\n  Value error, required for a rest catalog"),
        ({"catalog_type": "glue", "table": "a.b"}, "required for a glue catalog"),
        ({"warehouse": "/w", "table": "a.b", "mode": "merge"}, r"merge_keys\n  Value error, required for mode 'merge'"),
        ({"warehouse": "/w", "table": "a.b", "merge_keys": ["id"]}, "only applies to mode 'merge'"),
        ({"warehouse": "/w", "table": "a.b", "partition_by": ["x) TBLPROPERTIES ("]}, "invalid partition field"),
        ({"warehouse": "/w", "table": "a.b", "catalog_name": "my-cat"}, "pattern"),
    ]:
        with pytest.raises(ValidationError, match=message):
            IcebergSinkOptions(**bad)


def test_each_setting_says_which_catalog_types_it_applies_to():
    """The UI shows a field only for the catalog types in 'in' and stars it for 'required_in'."""
    props = IcebergSinkOptions.model_json_schema()["properties"]
    assert props["warehouse"]["x-when"] == {"field": "catalog_type", "required_in": ["hadoop", "glue", "jdbc", "nessie"]}
    assert props["uri"]["x-when"] == {"field": "catalog_type", "in": ["hive", "rest", "jdbc", "nessie"], "required_in": ["hive", "rest", "jdbc", "nessie"]}
    assert props["token"]["x-when"] == {"field": "catalog_type", "in": ["rest"], "required_in": []}
    assert props["merge_keys"]["x-when"] == {"field": "mode", "in": ["merge"], "required_in": ["merge"]}
    # hive and rest catalogs can supply the warehouse themselves, but always need an endpoint
    IcebergSinkOptions(catalog_type="hive", uri="thrift://h:9083", table="a.b")
    IcebergSinkOptions(catalog_type="rest", uri="http://r", table="a.b")
    with pytest.raises(ValidationError, match=r"uri\n  Value error, required for a hive catalog"):
        IcebergSinkOptions(catalog_type="hive", table="a.b")


def test_missing_settings_are_reported_on_their_field(tmp_path, orders_csv):
    """The UI shows an issue under the field its path names, so the pipeline cannot be
    versioned (and so not run) until the catalog settings for the chosen type are there."""
    from tributary import SpecError

    doc = pipeline(tmp_path, orders_csv, sinks=[{"id": "out", "type": "iceberg", "input": "orders_clean",
                                                  "options": {"catalog_type": "rest", "table": "a.b", "mode": "merge"}}])
    with pytest.raises(SpecError) as exc:
        compile_pipeline(doc)
    assert {(i.path, i.message) for i in exc.value.issues} == {
        ("sinks[0].options.uri", "required for a rest catalog"),
        ("sinks[0].options.merge_keys", "required for mode 'merge'"),
    }


def test_catalog_name_is_derived_from_the_settings_unless_given():
    a = IcebergSinkOptions(warehouse="/w", table="a.b")
    same = IcebergSinkOptions(warehouse="/w", table="c.d")
    other = IcebergSinkOptions(warehouse="/elsewhere", table="a.b")
    assert a.catalog == same.catalog != other.catalog
    assert a.catalog.startswith("iceberg_")
    named = IcebergSinkOptions(warehouse="/w", table="a.b", catalog_name="lake")
    assert named.catalog == "lake" and named.qualified_table() == "`lake`.`a`.`b`"


def test_catalog_conf_covers_every_catalog_type():
    rest = IcebergSinkOptions(catalog_type="rest", uri="http://r:8181", token="t", catalog_name="r", table="a.b",
                              catalog_properties={"io-impl": "org.apache.iceberg.aws.s3.S3FileIO"})
    assert IcebergSink().required_conf(rest) == {
        "spark.sql.catalog.r": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.r.type": "rest",
        "spark.sql.catalog.r.uri": "http://r:8181",
        "spark.sql.catalog.r.token": "t",
        "spark.sql.catalog.r.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    }
    hive = IcebergSinkOptions(catalog_type="hive", uri="thrift://h:9083", warehouse="s3a://b/w", catalog_name="h", table="a.b")
    assert hive.catalog_conf()["spark.sql.catalog.h.type"] == "hive"
    # Iceberg refuses 'type' next to 'catalog-impl', so a custom implementation replaces it.
    custom = IcebergSinkOptions(warehouse="/w", table="a.b", catalog_name="c", catalog_properties={"catalog-impl": "x.Y"})
    assert "spark.sql.catalog.c.type" not in custom.catalog_conf()
    assert custom.catalog_conf()["spark.sql.catalog.c.catalog-impl"] == "x.Y"


def test_schema_ddl_keeps_nullability_and_nested_types():
    schema = StructType([
        StructField("id", IntegerType(), nullable=False),
        StructField("tags", ArrayType(StringType())),
        StructField("odd`name", StringType()),
    ])
    assert schema_ddl(schema) == "`id` int NOT NULL, `tags` array<string>, `odd``name` string"


def test_session_gets_package_and_catalog_but_no_extensions(tmp_path, orders_csv):
    doc = iceberg_pipeline(tmp_path, orders_csv, catalog_name="lake")
    compiled = compile_pipeline(doc)
    [package] = required_packages(compiled)
    assert package.startswith("org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:")
    conf = session_conf(compiled, decide_resources(compiled))
    assert conf["spark.jars.packages"] == package
    assert conf["spark.sql.catalog.lake"] == "org.apache.iceberg.spark.SparkCatalog"
    assert conf["spark.sql.catalog.lake.warehouse"] == str(tmp_path / "warehouse")
    # Iceberg's SQL extensions are opt-in: they would load Iceberg into every session state.
    assert "spark.sql.extensions" not in conf
    doc["runtime"] = {"dev": {"spark_conf": {"spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"}}}
    conf = session_conf(compile_pipeline(doc), decide_resources(compile_pipeline(doc)))
    assert conf["spark.sql.extensions"].endswith("IcebergSparkSessionExtensions")


def test_conflicting_catalog_definitions_are_rejected(tmp_path, orders_csv):
    doc = iceberg_pipeline(tmp_path, orders_csv, catalog_name="lake")
    doc["sinks"].append({**iceberg_sink(tmp_path / "other", "a.b", catalog_name="lake"), "id": "second"})
    with pytest.raises(TributaryError, match="spark.sql.catalog.lake.warehouse"):
        required_conf(compile_pipeline(doc))


def test_connection_supplies_the_catalog(tmp_path, orders_csv):
    doc = pipeline(tmp_path, orders_csv, sinks=[
        {"id": "orders_out", "type": "iceberg", "input": "orders_clean", "connection": "lake",
         "options": {"table": "sales.orders", "mode": "overwrite"}},
    ])
    doc["environments"]["dev"]["connections"] = {
        "lake": {"type": "iceberg", "catalog_type": "hadoop", "warehouse": str(tmp_path / "warehouse"), "catalog_name": "lake"},
    }
    opts = compile_pipeline(doc).spec.sinks[0].options
    assert (opts.catalog, opts.warehouse, opts.mode) == ("lake", str(tmp_path / "warehouse"), "overwrite")


# --- real writes (Hadoop catalog on local disk) --------------------------------------------


def test_sink_contract(iceberg_spark, tmp_path):
    opts = IcebergSinkOptions(warehouse=str(tmp_path / "wh"), table="contract.numbers")
    assert_sink_contract(iceberg_spark, IcebergSink(), opts, iceberg_spark.range(3))
    assert iceberg_spark.table(opts.qualified_table()).count() == 3


def test_append_creates_then_appends(iceberg_spark, tmp_path, orders_csv):
    doc = iceberg_pipeline(tmp_path, orders_csv, partition_by=["country", "bucket(4, order_id)"],
                           table_properties={"format-version": "2", "tributary.note": "it's orders"})
    compiled = compile_pipeline(doc)
    opts = compiled.spec.sinks[0].options
    table = opts.qualified_table()

    assert run(compiled, spark=iceberg_spark).sinks_written == ["orders_out"]
    assert rows(iceberg_spark, opts) == {1: (10.0, "IN"), 2: (-1.0, "US"), None: (5.0, "DE"), 4: (7.5, "USA"), 5: (3.0, "GB")}
    partitions = [r.col_name for r in iceberg_spark.sql(f"DESCRIBE TABLE {table}").collect() if r.col_name.startswith("Part ")]
    assert partitions == ["Part 0", "Part 1"]
    props = {r.key: r.value for r in iceberg_spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}
    assert props["format-version"] == "2" and props["tributary.note"] == "it's orders"

    summary = run(compiled, spark=iceberg_spark)
    assert iceberg_spark.table(table).count() == 10
    [latest] = iceberg_spark.sql(f"SELECT summary FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").collect()
    assert latest.summary["tributary.run"] == summary.run_id  # every commit says which run made it


def test_overwrite_and_overwrite_partitions(iceberg_spark, tmp_path, orders_csv):
    opts = compile_pipeline(iceberg_pipeline(tmp_path, orders_csv, mode="overwrite", partition_by=["country"])).spec.sinks[0].options
    run(iceberg_pipeline(tmp_path, orders_csv, mode="overwrite", partition_by=["country"]), spark=iceberg_spark)
    run(iceberg_pipeline(tmp_path, orders_csv, mode="overwrite", partition_by=["country"]), spark=iceberg_spark)
    assert iceberg_spark.table(opts.qualified_table()).count() == 5

    update = tmp_path / "update.csv"
    update.write_text("order_id,amount,country\n1,99.0,IN\n6,1.0,IN\n")
    run(pipeline(tmp_path, update, sinks=[iceberg_sink(tmp_path / "warehouse", mode="overwrite_partitions")]), spark=iceberg_spark)
    assert rows(iceberg_spark, opts) == {1: (99.0, "IN"), 6: (1.0, "IN"), 2: (-1.0, "US"), None: (5.0, "DE"), 4: (7.5, "USA"), 5: (3.0, "GB")}


def test_merge_upserts_on_keys(iceberg_spark, tmp_path, orders_csv):
    merge = {"mode": "merge", "merge_keys": ["order_id"]}
    opts = compile_pipeline(iceberg_pipeline(tmp_path, orders_csv, **merge)).spec.sinks[0].options
    run(iceberg_pipeline(tmp_path, orders_csv, **merge), spark=iceberg_spark)  # first write creates the table
    update = tmp_path / "update.csv"
    update.write_text("order_id,amount,country\n2,20.0,us\n6,60.0,fr\n")
    run(pipeline(tmp_path, update, sinks=[iceberg_sink(tmp_path / "warehouse", **merge)]), spark=iceberg_spark)
    assert rows(iceberg_spark, opts) == {1: (10.0, "IN"), 2: (20.0, "US"), None: (5.0, "DE"), 4: (7.5, "USA"), 5: (3.0, "GB"), 6: (60.0, "FR")}

    bad = pipeline(tmp_path, update, sinks=[iceberg_sink(tmp_path / "warehouse", mode="merge", merge_keys=["nope"])])
    with pytest.raises(TributaryError, match="merge_keys not in the data: nope"):
        run(bad, spark=iceberg_spark)


def test_deep_validation_checks_merge_keys_and_partition_columns_against_the_input(spark, tmp_path, orders_csv):
    """Deep validation (what the editor runs on every change) reports a column the sink
    names but its input does not have, on the sink node, without touching the catalog."""
    from tributary import analyze

    doc = iceberg_pipeline(tmp_path, orders_csv, mode="merge", merge_keys=["order_id", "customer_id"], partition_by=["days(order_ts)"])
    report = analyze(compile_pipeline(doc), spark=spark)
    assert report["orders_clean"].ok and not report["orders_out"].ok
    assert [(i["path"], i["message"]) for i in report["orders_out"].issues] == [
        ("sinks[0].options.merge_keys", "not in the data: customer_id (columns: order_id, amount, country)"),
        ("sinks[0].options.partition_by", "not in the data: order_ts (columns: order_id, amount, country)"),
    ]
    assert not (tmp_path / "warehouse").exists()
    good = analyze(compile_pipeline(iceberg_pipeline(tmp_path, orders_csv, mode="merge", merge_keys=["order_id"], partition_by=["bucket(4, order_id)"])), spark=spark)
    assert good["orders_out"].ok and good["orders_out"].issues == []


def test_missing_table_without_create(iceberg_spark, tmp_path, orders_csv):
    with pytest.raises(TributaryError, match="does not exist"):
        run(iceberg_pipeline(tmp_path, orders_csv, create_if_missing=False), spark=iceberg_spark)


def test_preview_is_a_dry_run_unless_asked_to_write(iceberg_spark, tmp_path, orders_csv):
    doc = iceberg_pipeline(tmp_path, orders_csv, catalog_name="preview_cat")
    result = preview(doc, "orders_out", spark=iceberg_spark, limit=2)
    assert result.target == f"iceberg hadoop -> preview_cat.sales.orders (append) at {tmp_path / 'warehouse'}"
    assert len(result.rows) == 2 and not result.written
    assert not (tmp_path / "warehouse").exists()  # nothing written, catalog not even registered

    written = preview(doc, "orders_out", spark=iceberg_spark, limit=2, write_sinks=True)
    assert written.written
    assert iceberg_spark.table("`preview_cat`.`sales`.`orders`").count() == 2


def test_streaming_appends_once_per_micro_batch_and_skips_replays(iceberg_spark, tmp_path):
    incoming = tmp_path / "in"
    incoming.mkdir()
    (incoming / "a.json").write_text('{"id": 1, "amount": 5}\n{"id": 2, "amount": 7}\n')
    sink = iceberg_sink(tmp_path / "warehouse", "events.raw")
    sink["input"] = "events"
    doc = {
        "pipeline": {"id": "stream_to_iceberg", "mode": "streaming",
                     "streaming": {"checkpoint_location": str(tmp_path / "chk"), "trigger": "available_now"}},
        "sources": [{"id": "events", "type": "file", "stream": True, "options": {"path": str(incoming), "format": "json"},
                     "schema": {"kind": "ddl", "ddl": "id INT, amount INT"}}],
        "sinks": [sink],
    }
    query = run(doc, spark=iceberg_spark)
    assert not query.isActive
    opts = compile_pipeline(doc).spec.sinks[0].options
    table = opts.qualified_table()
    # Micro-batches run in a cloned session; create_session() would have put the catalog on
    # the parent too. Here the parent is the plain test session, so register it to read back.
    configure_catalog(iceberg_spark, opts)
    assert {r.id for r in iceberg_spark.table(table).collect()} == {1, 2}
    [latest] = iceberg_spark.sql(f"SELECT summary FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").collect()
    assert (latest.summary["tributary.pipeline"], latest.summary["tributary.batch"]) == ("stream_to_iceberg", "0")

    # The same micro-batch arriving again (a restart after the commit but before the checkpoint) is skipped...
    replay = RunContext(spark=iceberg_spark, registry=default_registry(), pipeline_id="stream_to_iceberg", env="dev", batch_id=0)
    batch = iceberg_spark.createDataFrame([(1, 5), (2, 7)], "id INT, amount INT")
    IcebergSink().write(replay, batch, opts)
    assert iceberg_spark.table(table).count() == 2
    # ...a new batch, or the same batch id from another pipeline, is not.
    IcebergSink().write(replay.for_batch(1), batch, opts)
    assert iceberg_spark.table(table).count() == 4
    other = RunContext(spark=iceberg_spark, registry=default_registry(), pipeline_id="other_pipeline", env="dev", batch_id=1)
    IcebergSink().write(other, batch, opts)
    assert iceberg_spark.table(table).count() == 6
    # And the protection can be turned off.
    IcebergSink().write(other, batch, opts.model_copy(update={"skip_replayed_batches": False}))
    assert iceberg_spark.table(table).count() == 8


def test_shared_session_rejects_a_redefined_catalog(iceberg_spark, tmp_path):
    first = IcebergSinkOptions(warehouse=str(tmp_path / "one"), catalog_name="shared", table="a.b")
    configure_catalog(iceberg_spark, first)
    configure_catalog(iceberg_spark, first)  # same settings again: fine
    other = IcebergSinkOptions(warehouse=str(tmp_path / "two"), catalog_name="shared", table="a.b")
    with pytest.raises(TributaryError, match="already configured in this session with a different 'warehouse'"):
        configure_catalog(iceberg_spark, other)
