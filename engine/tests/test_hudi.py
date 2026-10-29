"""The Hudi sink: options, session configuration and real writes to tables on local disk.

Tests that write need the Hudi Spark bundle (see ``hudi_jar`` in conftest); the others run
without Spark.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from conftest import pipeline
from tributary import SpecError, TributaryError, analyze, compile_pipeline, preview, run
from tributary.core import RunContext
from tributary.plugins.sinks.hudi import (
    HUDI_AWS_PACKAGE,
    HUDI_VERSION,
    KRYO_SERIALIZER,
    HudiSink,
    HudiSinkOptions,
    check_serializer,
    latest_commit,
    latest_commit_metadata,
    table_exists,
    table_properties,
)
from tributary.registry import default_registry
from tributary.runtime import decide_resources, required_conf, required_packages, session_conf
from tributary.testing import assert_sink_contract, static_contract_issues

KEYED = {"record_key": ["order_id"], "precombine_field": "amount"}
# orders.csv has a row without order_id; Hudi refuses null record keys, so keyed pipelines
# drop it at a quality gate in front of the sink (the engine reports the Hudi error otherwise).
DROP_NULL_KEYS = [{"id": "keys_dq", "attach_to": "orders_clean", "action": "drop",
                   "checks": [{"type": "not_null", "column": "order_id"}]}]


def hudi_sink(base_path, table="sales.orders", **options) -> dict:
    return {
        "id": "orders_out",
        "type": "hudi",
        "input": "orders_clean",
        "options": {"base_path": str(base_path), "table": table, **KEYED, **options},
    }


def hudi_pipeline(tmp_path, orders_csv, **options) -> dict:
    return pipeline(tmp_path, orders_csv, quality=DROP_NULL_KEYS, sinks=[hudi_sink(tmp_path / "lake", **options)])


def rows(spark, opts: HudiSinkOptions) -> dict[int | None, tuple[float, str]]:
    return {r.order_id: (r.amount, r.country) for r in spark.read.format("hudi").load(opts.location).collect()}


# --- no Spark needed ---------------------------------------------------------------------


def test_static_contract():
    assert static_contract_issues(HudiSink) == []


def test_options_are_validated():
    ok = HudiSinkOptions(base_path="/lake/", table="sales.orders", record_key=[" order_id "], precombine_field=" ts ",
                         partition_by=["country", " day:timestamp "])
    assert (ok.record_key, ok.precombine_field, ok.partition_by) == (["order_id"], "ts", ["country", "day:timestamp"])
    assert ok.location == "/lake/sales/orders" and ok.database == "sales" and ok.table_name == "orders"
    flat = HudiSinkOptions(base_path="s3a://bucket", table="orders", operation="insert")
    assert flat.location == "s3a://bucket/orders" and flat.database is None
    for bad, message in [
        ({"table": "orders", "operation": "insert"}, r"base_path\n  Field required"),
        ({"base_path": "/l", "table": "a.b.c", "operation": "insert"}, "pattern"),
        ({"base_path": "/l", "table": "orders"}, r"record_key\n  Value error, required for operation 'upsert'"),
        ({"base_path": "/l", "table": "orders", "operation": "delete"}, "required for operation 'delete'"),
        ({"base_path": "/l", "table": "orders", "record_key": ["id"]}, r"precombine_field\n  Value error, required for operation 'upsert'"),
        ({"base_path": "/l", "table": "orders", "record_key": ["id;drop"], "precombine_field": "ts"}, "invalid column name"),
        ({"base_path": "/l", "table": "orders", "operation": "insert", "partition_by": ["country", "x y"]}, "invalid partition field"),
        ({"base_path": "/l", "table": "orders", "operation": "insert", "catalog_sync": "hms"}, r"catalog_uri\n  Value error, required for catalog_sync 'hms'"),
        ({"base_path": "/l", "table": "orders", "operation": "insert", "catalog_sync": "jdbc"}, "required for catalog_sync 'jdbc'"),
        ({"base_path": "/l", "table": "orders", "operation": "insert", "nope": 1}, "extra"),
    ]:
        with pytest.raises(ValidationError, match=message):
            HudiSinkOptions(**bad)
    # glue needs no endpoint, and the keyless operations need no record key
    HudiSinkOptions(base_path="/l", table="orders", operation="bulk_insert", catalog_sync="glue")


def test_each_setting_says_when_it_applies():
    """The UI shows a field only for the values in 'in' and stars it for 'required_in'."""
    props = HudiSinkOptions.model_json_schema()["properties"]
    assert props["record_key"]["x-when"] == {"field": "operation", "required_in": ["upsert", "delete"]}
    assert props["precombine_field"]["x-when"] == {"field": "operation", "required_in": ["upsert"]}
    assert props["catalog_uri"]["x-when"] == {"field": "catalog_sync", "in": ["hms", "jdbc"], "required_in": ["hms", "jdbc"]}
    assert props["catalog_password"]["x-when"] == {"field": "catalog_sync", "in": ["jdbc"], "required_in": []}
    # Always mandatory, so always starred: the storage root (unless a connection supplies it) and the table.
    assert HudiSinkOptions.model_json_schema()["required"] == ["base_path", "table"]
    assert "x-when" not in props["table"] and "x-when" not in props["base_path"]


def test_missing_settings_are_reported_on_their_field(tmp_path, orders_csv):
    """The UI shows an issue under the field its path names, so the pipeline cannot be
    versioned (and so not run) until the settings for the chosen operation are there."""
    doc = pipeline(tmp_path, orders_csv, sinks=[{"id": "out", "type": "hudi", "input": "orders_clean",
                                                  "options": {"table": "orders", "catalog_sync": "hms"}}])
    with pytest.raises(SpecError) as exc:
        compile_pipeline(doc)
    assert {(i.path, i.message) for i in exc.value.issues} == {
        ("sinks[0].options.base_path", "Field required"),
        ("sinks[0].options.catalog_uri", "required for catalog_sync 'hms'"),
        ("sinks[0].options.record_key", "required for operation 'upsert'"),
        ("sinks[0].options.precombine_field", "required for operation 'upsert'"),
    }


def test_hoodie_options_cover_every_field_and_sync_mode():
    plain = HudiSinkOptions(base_path="/l", table="orders", operation="insert")
    assert plain.hoodie_options() == {
        "hoodie.table.name": "orders",
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": "insert",
        "hoodie.datasource.write.hive_style_partitioning": "true",
    }
    hms = HudiSinkOptions(base_path="/l", table="sales.orders", table_type="merge_on_read", record_key=["order_id", "country"],
                          precombine_field="ts", partition_by=["country", "day:simple"], hive_style_partitioning=False,
                          catalog_sync="hms", catalog_uri="thrift://h:9083",
                          write_options={"hoodie.index.type": "BLOOM", "hoodie.datasource.hive_sync.table": "orders_v2"})
    assert hms.hoodie_options() == {
        "hoodie.table.name": "orders",
        "hoodie.database.name": "sales",
        "hoodie.datasource.write.table.type": "MERGE_ON_READ",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.hive_style_partitioning": "false",
        "hoodie.datasource.write.recordkey.field": "order_id,country",
        "hoodie.datasource.write.precombine.field": "ts",
        "hoodie.datasource.write.payload.class": "org.apache.hudi.common.model.DefaultHoodieRecordPayload",
        "hoodie.datasource.write.partitionpath.field": "country,day:simple",
        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.datasource.hive_sync.mode": "hms",
        "hoodie.datasource.hive_sync.metastore.uris": "thrift://h:9083",
        "hoodie.datasource.hive_sync.database": "sales",
        "hoodie.datasource.hive_sync.partition_fields": "country,day",
        "hoodie.index.type": "BLOOM",
        "hoodie.datasource.hive_sync.table": "orders_v2",  # write_options win over derived settings
    }
    jdbc = HudiSinkOptions(base_path="/l", table="orders", operation="insert", catalog_sync="jdbc",
                           catalog_uri="jdbc:hive2://h:10000", catalog_user="hive", catalog_password="pw").hoodie_options()
    assert {k: v for k, v in jdbc.items() if "hive_sync" in k} == {
        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.datasource.hive_sync.mode": "jdbc",
        "hoodie.datasource.hive_sync.jdbcurl": "jdbc:hive2://h:10000",
        "hoodie.datasource.hive_sync.username": "hive",
        "hoodie.datasource.hive_sync.password": "pw",
        "hoodie.datasource.hive_sync.database": "default",
        "hoodie.datasource.hive_sync.table": "orders",
    }
    glue = HudiSinkOptions(base_path="/l", table="orders", operation="insert", catalog_sync="glue").hoodie_options()
    assert glue["hoodie.datasource.meta.sync.enable"] == "true"
    assert glue["hoodie.meta.sync.client.tool.class"] == "org.apache.hudi.aws.sync.AwsGlueCatalogSyncTool"
    assert "hoodie.datasource.hive_sync.enable" not in glue


def test_key_scope_and_schema_evolution_settings():
    def hoodie(**options):
        return HudiSinkOptions(base_path="/l", table="orders", operation="insert", **options).hoodie_options()

    assert "hoodie.index.type" not in hoodie()
    assert hoodie(global_index=True)["hoodie.index.type"] == "GLOBAL_BLOOM"
    assert hoodie(global_index=True)["hoodie.bloom.index.update.partition.path"] == "true"
    evolution = {k: v for k, v in hoodie().items() if "schema" in k}
    assert evolution == {} and evolution == {k: v for k, v in hoodie(schema_evolution="strict").items() if "schema" in k}
    assert hoodie(schema_evolution="reconcile")["hoodie.datasource.write.reconcile.schema"] == "true"
    assert hoodie(schema_evolution="drop_columns")["hoodie.datasource.write.schema.allow.auto.evolution.column.drop"] == "true"
    props = HudiSinkOptions.model_json_schema()["properties"]
    assert props["schema_evolution"]["enum"] == ["add_columns", "reconcile", "drop_columns", "strict"]
    assert props["schema_evolution"]["default"] == "add_columns" and props["global_index"]["default"] is False
    with pytest.raises(ValidationError, match="schema_evolution"):
        HudiSinkOptions(base_path="/l", table="orders", operation="insert", schema_evolution="merge")


def test_columns_are_checked_against_the_input_schema():
    """record_key, precombine_field and partition_by must name columns of the node the sink
    reads; nested columns (a.b) and key-generator suffixes (day:simple) are understood."""
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    schema = StructType([
        StructField("order_id", IntegerType()), StructField("country", StringType()),
        StructField("meta", StructType([StructField("updated_at", IntegerType())])),
    ])
    ok = HudiSinkOptions(base_path="/l", table="t", record_key=["order_id"], precombine_field="meta.updated_at", partition_by=["country:simple"])
    assert HudiSink().validate_input(None, schema, ok) == []
    bad = HudiSinkOptions(base_path="/l", table="t", record_key=["order_id", "customer_id"], precombine_field="updated_at",
                          partition_by=["country", "meta.region"])
    assert HudiSink().validate_input(None, schema, bad) == [
        ("record_key", "not in the data: customer_id (columns: order_id, country, meta)"),
        ("precombine_field", "not in the data: updated_at (columns: order_id, country, meta)"),
        ("partition_by", "not in the data: meta.region (columns: order_id, country, meta)"),
    ]
    keyless = HudiSinkOptions(base_path="/l", table="t", operation="insert")
    assert HudiSink().validate_input(None, schema, keyless) == []


def test_describe_says_where_and_how(tmp_path):
    opts = HudiSinkOptions(base_path=str(tmp_path), table="sales.orders", table_type="merge_on_read", **KEYED)
    assert HudiSink().describe(opts) == f"hudi merge_on_read -> sales.orders (upsert) at {tmp_path}/sales/orders"
    synced = opts.model_copy(update={"catalog_sync": "hms", "catalog_uri": "thrift://h:9083"})
    assert HudiSink().describe(synced) == f"hudi merge_on_read -> sales.orders (upsert, synced to hms) at {tmp_path}/sales/orders"


def test_session_gets_the_bundle_and_kryo(tmp_path, orders_csv):
    """A pipeline with a Hudi sink needs the bundle on the classpath and Kryo serialization,
    which Hudi refuses to run without; Glue sync adds the AWS bundle."""
    compiled = compile_pipeline(hudi_pipeline(tmp_path, orders_csv))
    packages = required_packages(compiled)
    assert packages == [f"org.apache.hudi:hudi-spark3.5-bundle_2.12:{HUDI_VERSION}"]
    assert required_conf(compiled) == {"spark.serializer": KRYO_SERIALIZER}
    conf = session_conf(compiled, decide_resources(compiled))
    assert conf["spark.serializer"] == KRYO_SERIALIZER and conf["spark.jars.packages"] == packages[0]
    assert "spark.sql.extensions" not in conf  # nothing Hudi-specific on the session state

    glue = compile_pipeline(hudi_pipeline(tmp_path, orders_csv, catalog_sync="glue"))
    assert required_packages(glue) == [packages[0], HUDI_AWS_PACKAGE]

    # The user's runtime.spark_conf still wins.
    doc = hudi_pipeline(tmp_path, orders_csv)
    doc["runtime"] = {"dev": {"spark_conf": {"spark.serializer": "org.example.Other"}}}
    compiled = compile_pipeline(doc)
    assert session_conf(compiled, decide_resources(compiled))["spark.serializer"] == "org.example.Other"


def test_a_session_without_kryo_is_refused_up_front():
    class Conf:
        def __init__(self, value):
            self.value = value

        def get(self, key, default=None):
            return self.value if key == "spark.serializer" else default

    class Session:
        def __init__(self, value):
            self.conf = Conf(value)

    check_serializer(Session(KRYO_SERIALIZER))
    with pytest.raises(TributaryError, match="needs spark.serializer=org.apache.spark.serializer.KryoSerializer \\(got the default\\)"):
        check_serializer(Session(None))
    with pytest.raises(TributaryError, match="got org.apache.spark.serializer.JavaSerializer"):
        check_serializer(Session("org.apache.spark.serializer.JavaSerializer"))


def test_connection_supplies_the_storage_settings(tmp_path, orders_csv):
    doc = hudi_pipeline(tmp_path, orders_csv)
    doc["environments"]["dev"]["connections"] = {
        "lake": {"type": "hudi", "base_path": str(tmp_path / "lake"), "catalog_sync": "hms", "catalog_uri": "thrift://h:9083"}
    }
    doc["sinks"][0] = {"id": "orders_out", "type": "hudi", "input": "orders_clean", "connection": "lake",
                       "options": {"table": "sales.orders", "operation": "insert", "catalog_sync": "none"}}
    opts = compile_pipeline(doc).spec.sinks[0].options
    assert (opts.location, opts.catalog_sync, opts.operation) == (f"{tmp_path}/lake/sales/orders", "none", "insert")
    assert opts.catalog_uri == "thrift://h:9083"  # harmless leftover: the node turned sync off


# --- real writes (tables on local disk) ----------------------------------------------------


def test_sink_contract(hudi_spark, tmp_path):
    opts = HudiSinkOptions(base_path=str(tmp_path), table="contract.numbers", record_key=["id"], precombine_field="id")
    assert_sink_contract(hudi_spark, HudiSink(), opts, hudi_spark.range(3))
    assert hudi_spark.read.format("hudi").load(opts.location).count() == 3


def test_upsert_creates_then_updates(hudi_spark, tmp_path, orders_csv):
    doc = hudi_pipeline(tmp_path, orders_csv, partition_by=["country"])
    compiled = compile_pipeline(doc)
    opts = compiled.spec.sinks[0].options

    assert not table_exists(hudi_spark, opts.location)
    summary = run(compiled, spark=hudi_spark)
    assert summary.sinks_written == ["orders_out"]
    assert table_exists(hudi_spark, opts.location)
    assert rows(hudi_spark, opts) == {1: (10.0, "IN"), 2: (-1.0, "US"), 4: (7.5, "USA"), 5: (3.0, "GB")}
    assert sorted(p.name for p in (tmp_path / "lake" / "sales" / "orders").iterdir() if not p.name.startswith(".")) == [
        "country=GB", "country=IN", "country=US", "country=USA",
    ]
    assert latest_commit_metadata(hudi_spark, opts.location)["_tributary.run"] == summary.run_id  # every commit says which run made it

    # Same rows again: an upsert keeps one row per key...
    again = run(compiled, spark=hudi_spark)
    assert len(rows(hudi_spark, opts)) == 4
    assert latest_commit_metadata(hudi_spark, opts.location)["_tributary.run"] == again.run_id != summary.run_id
    # ...and changed rows replace the stored ones, new keys are added.
    update = tmp_path / "update.csv"
    update.write_text("order_id,amount,country\n2,20.0,us\n6,60.0,fr\n")
    run(pipeline(tmp_path, update, sinks=[hudi_sink(tmp_path / "lake", partition_by=["country"])]), spark=hudi_spark)
    assert rows(hudi_spark, opts) == {1: (10.0, "IN"), 2: (20.0, "US"), 4: (7.5, "USA"), 5: (3.0, "GB"), 6: (60.0, "FR")}


def test_precombine_picks_the_largest_value_per_key(hudi_spark, tmp_path):
    """Within a batch and against the stored row: an older row arriving later does not win."""
    opts = HudiSinkOptions(base_path=str(tmp_path), table="dedup", record_key=["id"], precombine_field="version")
    ctx = RunContext(spark=hudi_spark, registry=default_registry(), pipeline_id="p", env="dev")

    def stored():
        return [tuple(r) for r in hudi_spark.read.format("hudi").load(opts.location).select("id", "v", "version").collect()]

    HudiSink().write(ctx, hudi_spark.createDataFrame([(1, "old", 1), (1, "new", 2)], "id INT, v STRING, version INT"), opts)
    assert stored() == [(1, "new", 2)]
    HudiSink().write(ctx, hudi_spark.createDataFrame([(1, "stale", 0)], "id INT, v STRING, version INT"), opts)
    assert stored() == [(1, "new", 2)]
    HudiSink().write(ctx, hudi_spark.createDataFrame([(1, "newer", 3)], "id INT, v STRING, version INT"), opts)
    assert stored() == [(1, "newer", 3)]


def test_insert_appends_and_overwrites_replace(hudi_spark, tmp_path, orders_csv):
    insert = {"operation": "insert", "partition_by": ["country"]}
    opts = compile_pipeline(hudi_pipeline(tmp_path, orders_csv, **insert)).spec.sinks[0].options
    run(hudi_pipeline(tmp_path, orders_csv, **insert), spark=hudi_spark)
    run(hudi_pipeline(tmp_path, orders_csv, **insert), spark=hudi_spark)
    assert hudi_spark.read.format("hudi").load(opts.location).count() == 8  # insert never deduplicates

    update = tmp_path / "update.csv"
    update.write_text("order_id,amount,country\n1,99.0,IN\n6,1.0,IN\n")
    run(pipeline(tmp_path, update, sinks=[hudi_sink(tmp_path / "lake", operation="insert_overwrite", partition_by=["country"])]), spark=hudi_spark)
    stored = hudi_spark.read.format("hudi").load(opts.location)
    assert {r.country: r.n for r in stored.groupBy("country").count().withColumnRenamed("count", "n").collect()} == {
        "IN": 2, "US": 2, "USA": 2, "GB": 2,  # only IN was replaced (by the 2 new rows)
    }
    run(pipeline(tmp_path, update, sinks=[hudi_sink(tmp_path / "lake", operation="insert_overwrite_table", partition_by=["country"])]), spark=hudi_spark)
    assert rows(hudi_spark, opts) == {1: (99.0, "IN"), 6: (1.0, "IN")}


def test_delete_removes_the_given_keys(hudi_spark, tmp_path, orders_csv):
    opts = compile_pipeline(hudi_pipeline(tmp_path, orders_csv)).spec.sinks[0].options
    run(hudi_pipeline(tmp_path, orders_csv), spark=hudi_spark)
    gone = tmp_path / "gone.csv"
    gone.write_text("order_id,amount,country\n1,0,xx\n4,0,xx\n")
    run(pipeline(tmp_path, gone, sinks=[hudi_sink(tmp_path / "lake", operation="delete")]), spark=hudi_spark)
    assert set(rows(hudi_spark, opts)) == {2, 5}


def test_merge_on_read_and_keyless_bulk_insert(hudi_spark, tmp_path, orders_csv):
    mor = {"table_type": "merge_on_read", "partition_by": ["country"]}
    opts = compile_pipeline(hudi_pipeline(tmp_path, orders_csv, **mor)).spec.sinks[0].options
    run(hudi_pipeline(tmp_path, orders_csv, **mor), spark=hudi_spark)
    update = tmp_path / "update.csv"
    update.write_text("order_id,amount,country\n2,20.0,us\n")
    run(pipeline(tmp_path, update, sinks=[hudi_sink(tmp_path / "lake", **mor)]), spark=hudi_spark)
    assert rows(hudi_spark, opts)[2] == (20.0, "US")  # the snapshot read merges base and log files
    props = (tmp_path / "lake" / "sales" / "orders" / ".hoodie" / "hoodie.properties").read_text()
    assert "hoodie.table.type=MERGE_ON_READ" in props

    keyless = {"id": "orders_out", "type": "hudi", "input": "orders_clean",
               "options": {"base_path": str(tmp_path), "table": "bulk", "operation": "bulk_insert"}}
    run(pipeline(tmp_path, orders_csv, sinks=[keyless]), spark=hudi_spark)
    run(pipeline(tmp_path, orders_csv, sinks=[keyless]), spark=hudi_spark)
    bulk = hudi_spark.read.format("hudi").load(str(tmp_path / "bulk"))
    assert bulk.count() == 10 and bulk.select("_hoodie_record_key").distinct().count() == 10  # generated keys


def test_deep_validation_reports_bad_columns_on_the_sink_and_nothing_is_written(hudi_spark, tmp_path, orders_csv):
    """The editor's column check (and `tributary validate --deep`) flags a column the sink
    names but its input lacks, on the sink node; a run refuses before creating the table."""
    doc = hudi_pipeline(tmp_path, orders_csv, record_key=["order_id"], precombine_field="updated_at", partition_by=["region"])
    report = analyze(compile_pipeline(doc), spark=hudi_spark)
    assert report["orders_clean"].ok and not report["orders_out"].ok
    assert [(i["path"], i["message"]) for i in report["orders_out"].issues] == [
        ("sinks[0].options.precombine_field", "not in the data: updated_at (columns: order_id, amount, country)"),
        ("sinks[0].options.partition_by", "not in the data: region (columns: order_id, amount, country)"),
    ]
    with pytest.raises(TributaryError, match="hudi sink sales.orders: precombine_field not in the data: updated_at"):
        run(doc, spark=hudi_spark)
    assert not (tmp_path / "lake").exists()

    # A column dropped by the SQL upstream is caught the same way, as soon as the SQL changes.
    doc = hudi_pipeline(tmp_path, orders_csv, partition_by=["country"])
    doc["transforms"][0]["options"]["sql"] = "SELECT order_id, amount FROM orders"
    report = analyze(compile_pipeline(doc), spark=hudi_spark)
    assert report["orders_out"].messages == ["not in the data: country (columns: order_id, amount)"]
    assert analyze(compile_pipeline(hudi_pipeline(tmp_path, orders_csv, partition_by=["country"])), spark=hudi_spark)["orders_out"].ok


def test_missing_table_without_create(hudi_spark, tmp_path, orders_csv):
    with pytest.raises(TributaryError, match="does not exist"):
        run(hudi_pipeline(tmp_path, orders_csv, create_if_missing=False), spark=hudi_spark)
    assert not (tmp_path / "lake").exists()


def test_preview_is_a_dry_run_unless_asked_to_write(hudi_spark, tmp_path, orders_csv):
    doc = hudi_pipeline(tmp_path, orders_csv)
    result = preview(doc, "orders_out", spark=hudi_spark, limit=2)
    assert result.target == f"hudi copy_on_write -> sales.orders (upsert) at {tmp_path / 'lake'}/sales/orders"
    assert len(result.rows) == 2 and not result.written
    assert not (tmp_path / "lake").exists()

    written = preview(doc, "orders_out", spark=hudi_spark, limit=2, write_sinks=True)
    assert written.written
    assert hudi_spark.read.format("hudi").load(f"{tmp_path}/lake/sales/orders").count() == 2


def test_streaming_writes_once_per_micro_batch_and_skips_replays(hudi_spark, tmp_path):
    incoming = tmp_path / "in"
    incoming.mkdir()
    (incoming / "a.json").write_text('{"id": 1, "amount": 5}\n{"id": 2, "amount": 7}\n')
    sink = {"id": "events_out", "type": "hudi", "input": "events",
            "options": {"base_path": str(tmp_path), "table": "events", "operation": "insert"}}
    doc = {
        "pipeline": {"id": "stream_to_hudi", "mode": "streaming",
                     "streaming": {"checkpoint_location": str(tmp_path / "chk"), "trigger": "available_now"}},
        "sources": [{"id": "events", "type": "file", "stream": True, "options": {"path": str(incoming), "format": "json"},
                     "schema": {"kind": "ddl", "ddl": "id INT, amount INT"}}],
        "sinks": [sink],
    }
    query = run(doc, spark=hudi_spark)
    assert not query.isActive
    opts = compile_pipeline(doc).spec.sinks[0].options


    def stored():
        return hudi_spark.read.format("hudi").load(opts.location).count()

    assert stored() == 2
    metadata = latest_commit_metadata(hudi_spark, opts.location)
    assert (metadata["_tributary.pipeline"], metadata["_tributary.batch"]) == ("stream_to_hudi", "0")

    # The same micro-batch arriving again (a restart after the commit but before the checkpoint) is skipped...
    replay = RunContext(spark=hudi_spark, registry=default_registry(), pipeline_id="stream_to_hudi", env="dev", batch_id=0)
    batch = hudi_spark.createDataFrame([(1, 5), (2, 7)], "id INT, amount INT")
    HudiSink().write(replay, batch, opts)
    assert stored() == 2
    # ...a new batch, or the same batch id from another pipeline, is not.
    HudiSink().write(replay.for_batch(1), batch, opts)
    assert stored() == 4
    other = RunContext(spark=hudi_spark, registry=default_registry(), pipeline_id="other_pipeline", env="dev", batch_id=1)
    HudiSink().write(other, batch, opts)
    assert stored() == 6
    # And the protection can be turned off.
    HudiSink().write(other, batch, opts.model_copy(update={"skip_replayed_batches": False}))
    assert stored() == 8


def test_commit_metadata_of_a_table_without_commits(hudi_spark, tmp_path):
    assert latest_commit_metadata(hudi_spark, str(tmp_path / "nowhere")) is None
    assert table_exists(hudi_spark, str(tmp_path / "nowhere")) is False


# --- table types and write operations ------------------------------------------------------


def ctx_for(spark, pipeline_id="p", **fields) -> RunContext:
    return RunContext(spark=spark, registry=default_registry(), pipeline_id=pipeline_id, env="dev", **fields)


def frame(spark, *rows):
    return spark.createDataFrame(list(rows), "id INT, amount DOUBLE, country STRING, ts INT")


def stored_rows(spark, opts: HudiSinkOptions) -> set[tuple]:
    return {(r.id, r.amount, r.country, r.ts) for r in spark.read.format("hudi").load(opts.location).collect()}


@pytest.mark.parametrize("table_type", ["copy_on_write", "merge_on_read"])
def test_every_operation_on_each_table_type(hudi_spark, tmp_path, table_type):
    """Each operation does what it says on both table types, and lands on the timeline as
    the matching Hudi operation: copy-on-write commits, merge-on-read delta commits, and
    replace commits for the overwrites."""
    commit = "commit" if table_type == "copy_on_write" else "deltacommit"
    base = dict(base_path=str(tmp_path), table="ops", table_type=table_type, record_key=["id"], precombine_field="ts", partition_by=["country"])
    ctx = ctx_for(hudi_spark)

    def write(operation, *rows):
        opts = HudiSinkOptions(**base, operation=operation)
        HudiSink().write(ctx, frame(hudi_spark, *rows), opts)
        latest = latest_commit(hudi_spark, opts.location)
        return (latest.action, latest.operation), stored_rows(hudi_spark, opts)

    assert write("upsert", (1, 10.0, "IN", 1), (2, 20.0, "US", 1)) == ((commit, "UPSERT"), {(1, 10.0, "IN", 1), (2, 20.0, "US", 1)})
    assert write("upsert", (2, 25.0, "US", 2)) == ((commit, "UPSERT"), {(1, 10.0, "IN", 1), (2, 25.0, "US", 2)})
    assert write("insert", (3, 30.0, "IN", 1)) == ((commit, "INSERT"), {(1, 10.0, "IN", 1), (2, 25.0, "US", 2), (3, 30.0, "IN", 1)})
    assert write("bulk_insert", (4, 40.0, "DE", 1))[0] == (commit, "BULK_INSERT")
    assert write("insert_overwrite", (5, 50.0, "IN", 1)) == (("replacecommit", "INSERT_OVERWRITE"), {(2, 25.0, "US", 2), (4, 40.0, "DE", 1), (5, 50.0, "IN", 1)})
    assert write("delete", (4, 0.0, "DE", 9)) == ((commit, "DELETE"), {(2, 25.0, "US", 2), (5, 50.0, "IN", 1)})
    assert write("insert_overwrite_table", (6, 60.0, "FR", 1)) == (("replacecommit", "INSERT_OVERWRITE_TABLE"), {(6, 60.0, "FR", 1)})
    location = HudiSinkOptions(**base).location
    assert table_properties(hudi_spark, location)["hoodie.table.type"] == table_type.upper()
    if table_type == "merge_on_read":  # 7 delta commits: Hudi compacted inline (every 5), and latest_commit() looked past it
        assert "COMPACT" in {str(m.getOperationType()) for m in _all_commit_metadata(hudi_spark, location)}


def _all_commit_metadata(spark, location):
    jvm = spark._jvm
    client = (jvm.org.apache.hudi.common.table.HoodieTableMetaClient.builder()
              .setConf(jvm.org.apache.hudi.hadoop.fs.HadoopFSUtils.getStorageConf(spark._jsc.hadoopConfiguration()))
              .setBasePath(location).build())
    timeline = client.getActiveTimeline().getCommitsTimeline().filterCompletedInstants()
    return [jvm.org.apache.hudi.common.table.timeline.TimelineUtils.getCommitMetadata(i, timeline) for i in timeline.getInstants()]


def test_existing_table_keeps_its_type_key_and_partitioning(hudi_spark, tmp_path):
    """Hudi fixes these when the table is created and fails obscurely otherwise (a
    copy-on-write table written as merge-on-read dies in compaction); the sink says what
    differs before writing, and still accepts a keyless insert into a keyed table."""
    cow = HudiSinkOptions(base_path=str(tmp_path), table="fixed", record_key=["id"], precombine_field="ts", partition_by=["country"])
    ctx = ctx_for(hudi_spark)
    HudiSink().write(ctx, frame(hudi_spark, (1, 10.0, "IN", 1)), cow)
    HudiSink().write(ctx, frame(hudi_spark, (1, 11.0, "IN", 2)), cow)  # same settings again: fine

    with pytest.raises(TributaryError, match="table_type is merge_on_read but the table was created with copy_on_write"):
        HudiSink().write(ctx, frame(hudi_spark, (2, 1.0, "IN", 1)), cow.model_copy(update={"table_type": "merge_on_read"}))
    with pytest.raises(TributaryError, match="record_key is id, country but the table was created with id"):
        HudiSink().write(ctx, frame(hudi_spark, (2, 1.0, "IN", 1)), cow.model_copy(update={"record_key": ["id", "country"]}))
    with pytest.raises(TributaryError, match="partition_by is no partitioning but the table was created with country"):
        HudiSink().write(ctx, frame(hudi_spark, (2, 1.0, "IN", 1)), cow.model_copy(update={"partition_by": []}))
    with pytest.raises(TributaryError) as exc:  # every difference in one message
        HudiSink().write(ctx, frame(hudi_spark, (2, 1.0, "IN", 1)), cow.model_copy(update={"table_type": "merge_on_read", "partition_by": ["ts"]}))
    assert "table_type is merge_on_read" in str(exc.value) and "partition_by is ts" in str(exc.value)
    assert stored_rows(hudi_spark, cow) == {(1, 11.0, "IN", 2)}  # nothing was written

    HudiSink().write(ctx, frame(hudi_spark, (3, 30.0, "IN", 1)), cow.model_copy(update={"operation": "insert", "record_key": [], "precombine_field": None}))
    assert {r[0] for r in stored_rows(hudi_spark, cow)} == {1, 3}

    mor = cow.model_copy(update={"table": "fixed_mor", "table_type": "merge_on_read"})
    HudiSink().write(ctx, frame(hudi_spark, (1, 10.0, "IN", 1)), mor)
    with pytest.raises(TributaryError, match="table_type is copy_on_write but the table was created with merge_on_read"):
        HudiSink().write(ctx, frame(hudi_spark, (1, 10.0, "IN", 1)), mor.model_copy(update={"table_type": "copy_on_write"}))


# --- record keys and precombine ordering ---------------------------------------------------


def test_record_keys_are_unique_within_a_partition_or_across_the_table(hudi_spark, tmp_path):
    """A key is one row per partition by default: a row whose partition value changes is
    duplicated, unless global_index makes keys unique across the table, in which case it
    moves. A composite key treats each combination as its own row."""
    ctx = ctx_for(hudi_spark)
    local = HudiSinkOptions(base_path=str(tmp_path), table="local_keys", record_key=["id"], precombine_field="ts", partition_by=["country"])
    HudiSink().write(ctx, frame(hudi_spark, (1, 10.0, "IN", 1), (2, 20.0, "US", 1)), local)
    HudiSink().write(ctx, frame(hudi_spark, (2, 20.0, "FR", 2), (2, 21.0, "US", 2)), local)
    assert stored_rows(hudi_spark, local) == {(1, 10.0, "IN", 1), (2, 21.0, "US", 2), (2, 20.0, "FR", 2)}

    global_keys = local.model_copy(update={"table": "global_keys", "global_index": True})
    HudiSink().write(ctx, frame(hudi_spark, (1, 10.0, "IN", 1), (2, 20.0, "US", 1)), global_keys)
    HudiSink().write(ctx, frame(hudi_spark, (2, 20.0, "FR", 2)), global_keys)
    assert stored_rows(hudi_spark, global_keys) == {(1, 10.0, "IN", 1), (2, 20.0, "FR", 2)}
    assert not (tmp_path / "global_keys" / "country=US").glob("*.parquet") or \
        hudi_spark.read.format("hudi").load(global_keys.location).filter("country = 'US'").count() == 0

    composite = local.model_copy(update={"table": "composite", "record_key": ["id", "country"], "partition_by": []})
    HudiSink().write(ctx, frame(hudi_spark, (1, 10.0, "IN", 1), (1, 10.0, "US", 1)), composite)
    HudiSink().write(ctx, frame(hudi_spark, (1, 99.0, "IN", 2)), composite)
    assert stored_rows(hudi_spark, composite) == {(1, 99.0, "IN", 2), (1, 10.0, "US", 1)}
    assert table_properties(hudi_spark, composite.location)["hoodie.table.recordkey.fields"] == "id,country"


def test_duplicate_keys_in_one_batch_collapse_to_the_latest(hudi_spark, tmp_path):
    """Upsert keeps one row per key even when a batch repeats it; insert keeps every row."""
    ctx = ctx_for(hudi_spark)
    upsert = HudiSinkOptions(base_path=str(tmp_path), table="dups", record_key=["id"], precombine_field="ts")
    batch = frame(hudi_spark, (1, 1.0, "IN", 3), (1, 2.0, "IN", 1), (1, 3.0, "IN", 2), (2, 1.0, "US", 1))
    HudiSink().write(ctx, batch, upsert)
    assert stored_rows(hudi_spark, upsert) == {(1, 1.0, "IN", 3), (2, 1.0, "US", 1)}

    insert = upsert.model_copy(update={"table": "dups_insert", "operation": "insert"})
    HudiSink().write(ctx, batch, insert)
    assert hudi_spark.read.format("hudi").load(insert.location).count() == 4


def test_precombine_orders_by_timestamp_on_merge_on_read(hudi_spark, tmp_path):
    """The ordering column can be a timestamp, and the rule holds across the log files of a
    merge-on-read table: the newest event wins whatever order the batches arrive in."""
    opts = HudiSinkOptions(base_path=str(tmp_path), table="events", table_type="merge_on_read",
                           record_key=["id"], precombine_field="event_ts", partition_by=["country"])
    ctx = ctx_for(hudi_spark)

    def batch(*rows):
        return hudi_spark.createDataFrame(list(rows), "id INT, status STRING, country STRING, event_ts TIMESTAMP")

    def status():
        return {r.id: (r.status, str(r.event_ts)) for r in hudi_spark.read.format("hudi").load(opts.location).collect()}

    from datetime import datetime as dt

    HudiSink().write(ctx, batch((1, "created", "IN", dt(2026, 1, 1, 10)), (2, "created", "US", dt(2026, 1, 1, 10))), opts)
    HudiSink().write(ctx, batch((1, "shipped", "IN", dt(2026, 1, 3, 9))), opts)  # newest event first...
    HudiSink().write(ctx, batch((1, "paid", "IN", dt(2026, 1, 2, 12)), (2, "paid", "US", dt(2026, 1, 2, 12))), opts)  # ...then a late one
    assert status() == {1: ("shipped", "2026-01-03 09:00:00"), 2: ("paid", "2026-01-02 12:00:00")}
    assert latest_commit(hudi_spark, opts.location).action == "deltacommit"


# --- schema evolution ----------------------------------------------------------------------


def test_schema_evolution_modes(hudi_spark, tmp_path):
    ctx = ctx_for(hudi_spark)
    base = dict(base_path=str(tmp_path), table="evolving", record_key=["id"], precombine_field="ts")
    widened = hudi_spark.createDataFrame([(1, 11.0, "IN", 2)], "id INT, amount DOUBLE, country STRING, ts BIGINT")
    extra = hudi_spark.createDataFrame([(1, 12.0, "IN", 3, "vip")], "id INT, amount DOUBLE, country STRING, ts BIGINT, segment STRING")
    narrower = hudi_spark.createDataFrame([(1, "IN", 4)], "id INT, country STRING, ts BIGINT")

    def columns(opts):
        schema = hudi_spark.read.format("hudi").load(opts.location).schema
        return {f.name: f.dataType.simpleString() for f in schema.fields if not f.name.startswith("_hoodie_")}

    def row(opts):
        [r] = hudi_spark.read.format("hudi").load(opts.location).collect()
        return {k: v for k, v in r.asDict().items() if not k.startswith("_hoodie_")}

    # add_columns (default): a new column and a widened type are taken in, a missing column is refused.
    add = HudiSinkOptions(**base)
    HudiSink().write(ctx, frame(hudi_spark, (1, 10.0, "IN", 1)), add)
    HudiSink().write(ctx, widened, add)
    assert columns(add)["ts"] == "bigint"
    HudiSink().write(ctx, extra, add)
    assert columns(add) == {"id": "int", "amount": "double", "ts": "bigint", "segment": "string", "country": "string"}
    with pytest.raises(Exception, match="missing field"):
        HudiSink().write(ctx, narrower, add)

    # reconcile: the table's schema stays, missing columns are written as null.
    HudiSink().write(ctx, narrower, add.model_copy(update={"schema_evolution": "reconcile"}))
    assert row(add) == {"id": 1, "amount": None, "country": "IN", "ts": 4, "segment": None}

    # drop_columns: columns missing from the data leave the table.
    HudiSink().write(ctx, narrower.withColumn("ts", narrower.ts + 1), add.model_copy(update={"schema_evolution": "drop_columns"}))
    assert columns(add) == {"id": "int", "ts": "bigint", "country": "string"}

    # strict: any difference is refused before writing, naming it; the same schema is fine.
    strict = HudiSinkOptions(**{**base, "table": "strict"}, schema_evolution="strict")
    HudiSink().write(ctx, frame(hudi_spark, (1, 10.0, "IN", 1)), strict)
    HudiSink().write(ctx, frame(hudi_spark, (1, 10.5, "IN", 2)), strict)
    with pytest.raises(TributaryError, match=r"not in the table: segment.*type changed: ts \(int -> bigint\).*schema_evolution is 'strict'"):
        HudiSink().write(ctx, extra, strict)
    with pytest.raises(TributaryError, match="missing from the data: amount"):
        HudiSink().write(ctx, narrower, strict)
    assert row(strict) == {"id": 1, "amount": 10.5, "country": "IN", "ts": 2} and columns(strict)["ts"] == "int"


def test_schema_evolution_in_a_pipeline_run(hudi_spark, tmp_path, orders_csv):
    """Through the engine: a second run whose SQL adds a column evolves the table, and with
    strict it fails with the sink's message."""
    doc = hudi_pipeline(tmp_path, orders_csv)
    opts = compile_pipeline(doc).spec.sinks[0].options
    run(doc, spark=hudi_spark)
    doc["transforms"][0]["options"]["sql"] = "SELECT order_id, amount, upper(country) AS country, round(amount * 0.2, 2) AS tax FROM orders"
    run(doc, spark=hudi_spark)
    stored = hudi_spark.read.format("hudi").load(opts.location)
    assert {r.order_id: r.tax for r in stored.collect()} == {1: 2.0, 2: -0.2, 4: 1.5, 5: 0.6}

    doc["sinks"][0]["options"]["schema_evolution"] = "strict"
    doc["transforms"][0]["options"]["sql"] = "SELECT order_id, amount, upper(country) AS country FROM orders"
    with pytest.raises(TributaryError, match="missing from the data: tax"):
        run(doc, spark=hudi_spark)


# --- streaming -----------------------------------------------------------------------------


def stream_doc(tmp_path, sink_options: dict, pipeline_id="orders_stream") -> dict:
    return {
        "pipeline": {"id": pipeline_id, "mode": "streaming",
                     "streaming": {"checkpoint_location": str(tmp_path / "chk"), "trigger": "available_now"}},
        "sources": [{"id": "events", "type": "file", "stream": True,
                     "options": {"path": str(tmp_path / "in"), "format": "json", "max_files_per_trigger": 1},
                     "schema": {"kind": "ddl", "ddl": "id INT, amount DOUBLE, country STRING, ts INT"}}],
        "sinks": [{"id": "out", "type": "hudi", "input": "events",
                   "options": {"base_path": str(tmp_path), "table": "events", **sink_options}}],
    }


def test_streaming_upserts_micro_batch_by_micro_batch_and_resumes_from_the_checkpoint(hudi_spark, tmp_path):
    """Each file is a micro-batch (max_files_per_trigger: 1): the sink upserts every batch
    into a merge-on-read table, tags each delta commit with its batch id, and a later run on
    the same checkpoint only processes the new file."""
    incoming = tmp_path / "in"
    incoming.mkdir()
    (incoming / "a.json").write_text('{"id": 1, "amount": 5, "country": "IN", "ts": 1}\n{"id": 2, "amount": 7, "country": "US", "ts": 1}\n')
    (incoming / "b.json").write_text('{"id": 2, "amount": 8, "country": "US", "ts": 2}\n{"id": 3, "amount": 9, "country": "DE", "ts": 2}\n')
    doc = stream_doc(tmp_path, {"table_type": "merge_on_read", "record_key": ["id"], "precombine_field": "ts", "partition_by": ["country"]})
    opts = compile_pipeline(doc).spec.sinks[0].options

    assert not run(doc, spark=hudi_spark).isActive
    assert stored_rows(hudi_spark, opts) == {(1, 5.0, "IN", 1), (2, 8.0, "US", 2), (3, 9.0, "DE", 2)}
    commit = latest_commit(hudi_spark, opts.location)
    assert (commit.action, commit.operation, commit.metadata["_tributary.batch"], commit.metadata["_tributary.pipeline"]) == ("deltacommit", "UPSERT", "1", "orders_stream")

    # A stale event for 2 and a new row arrive; only this file is processed, as batch 2.
    (incoming / "c.json").write_text('{"id": 2, "amount": 1, "country": "US", "ts": 0}\n{"id": 4, "amount": 4, "country": "IN", "ts": 3}\n')
    assert not run(doc, spark=hudi_spark).isActive
    assert stored_rows(hudi_spark, opts) == {(1, 5.0, "IN", 1), (2, 8.0, "US", 2), (3, 9.0, "DE", 2), (4, 4.0, "IN", 3)}
    assert latest_commit(hudi_spark, opts.location).metadata["_tributary.batch"] == "2"

    # Replaying batch 2 (a restart between the commit and the checkpoint) is skipped on a
    # merge-on-read table too; the next batch goes through.
    replay = ctx_for(hudi_spark, pipeline_id="orders_stream", batch_id=2)
    HudiSink().write(replay, frame(hudi_spark, (5, 5.0, "IN", 1)), opts)
    assert {r[0] for r in stored_rows(hudi_spark, opts)} == {1, 2, 3, 4}
    HudiSink().write(replay.for_batch(3), frame(hudi_spark, (5, 5.0, "IN", 1)), opts)
    assert {r[0] for r in stored_rows(hudi_spark, opts)} == {1, 2, 3, 4, 5}


def test_streaming_through_a_quality_gate_into_hudi(hudi_spark, tmp_path):
    """The gate in front of the sink drops rows without a key (which Hudi would refuse) and
    quarantines are per micro-batch; the sink only sees what passes."""
    incoming = tmp_path / "in"
    incoming.mkdir()
    (incoming / "a.json").write_text('{"id": 1, "amount": 5, "country": "IN", "ts": 1}\n{"amount": 7, "country": "US", "ts": 1}\n')
    (incoming / "b.json").write_text('{"id": 3, "amount": -1, "country": "DE", "ts": 1}\n{"id": 4, "amount": 4, "country": "DE", "ts": 1}\n')
    doc = stream_doc(tmp_path, {"record_key": ["id"], "precombine_field": "ts"})
    doc["quality"] = [{"id": "dq", "before": "out", "action": "quarantine",
                       "checks": [{"type": "not_null", "column": "id"}, {"type": "range", "column": "amount", "min": 0}],
                       "quarantine_sink": {"type": "file", "options": {"path": str(tmp_path / "bad"), "format": "json"}}}]
    opts = compile_pipeline(doc).spec.sinks[0].options
    assert not run(doc, spark=hudi_spark).isActive
    assert stored_rows(hudi_spark, opts) == {(1, 5.0, "IN", 1), (4, 4.0, "DE", 1)}
    bad = hudi_spark.read.json(str(tmp_path / "bad")).collect()
    assert sorted((r.amount, r._dq_failed_checks) for r in bad) == [(-1.0, ["range_2"]), (7.0, ["not_null_1"])]


def test_streaming_preview_of_a_hudi_sink_is_a_dry_run(hudi_spark, tmp_path):
    incoming = tmp_path / "in"
    incoming.mkdir()
    (incoming / "a.json").write_text('{"id": 1, "amount": 5, "country": "IN", "ts": 1}\n')
    doc = stream_doc(tmp_path, {"record_key": ["id"], "precombine_field": "ts"})
    result = preview(doc, "out", spark=hudi_spark, limit=5)
    assert [r["id"] for r in result.rows] == [1] and not result.written
    assert result.target == f"hudi copy_on_write -> events (upsert) at {tmp_path / 'events'}"
    assert not (tmp_path / "events").exists()
