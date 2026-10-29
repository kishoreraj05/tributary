"""The Iceberg source: options, session configuration and real reads through a Hadoop
catalog, including time travel, incremental reads and streaming.

Tests that read need the Iceberg runtime jar (see ``iceberg_jar`` in conftest); the
others run without Spark. Tables are produced by the Iceberg sink, so the two are also
tested together: one connection, one catalog, both directions.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from conftest import pipeline
from tributary import SpecError, TributaryError, compile_pipeline, preview, run
from tributary.core import RunContext
from tributary.execution import analyze_pipeline
from tributary.plugins.iceberg import IcebergCatalogOptions
from tributary.plugins.sinks.iceberg import IcebergSink, IcebergSinkOptions
from tributary.plugins.sources.iceberg import IcebergSource, IcebergSourceOptions, to_millis
from tributary.registry import default_registry
from tributary.runtime import decide_resources, required_conf, required_packages, session_conf
from tributary.testing import assert_source_contract, static_contract_issues


def iceberg_source(warehouse, table="sales.orders", **options) -> dict:
    return {
        "id": "orders_src",
        "type": "iceberg",
        "options": {"catalog_type": "hadoop", "warehouse": str(warehouse), "table": table, **options},
    }


def iceberg_sink(warehouse, table="sales.orders", **options) -> dict:
    return {
        "id": "orders_out",
        "type": "iceberg",
        "input": "orders_clean",
        "options": {"catalog_type": "hadoop", "warehouse": str(warehouse), "table": table, **options},
    }


def read_pipeline(tmp_path, **source_options) -> dict:
    """Iceberg table -> SQL -> parquet file."""
    doc = pipeline(tmp_path, tmp_path / "unused.csv")
    doc["sources"] = [iceberg_source(tmp_path / "warehouse", **source_options)]
    doc["transforms"][0]["options"]["sql"] = "SELECT order_id, amount, country FROM orders ORDER BY order_id"
    return doc


def write_orders(spark, tmp_path, orders_csv, **sink_options) -> IcebergSinkOptions:
    """Runs the CSV -> Iceberg pipeline once and returns the sink's options (for the table name)."""
    doc = pipeline(tmp_path, orders_csv, sinks=[iceberg_sink(tmp_path / "warehouse", **sink_options)])
    compiled = compile_pipeline(doc)
    run(compiled, spark=spark)
    return compiled.spec.sinks[0].options


def snapshots(spark, opts: IcebergCatalogOptions) -> list:
    return spark.sql(f"SELECT snapshot_id, committed_at FROM {opts.qualified_table()}.snapshots ORDER BY committed_at").collect()


def orders(df) -> dict[int | None, tuple[float, str]]:
    return {r.order_id: (r.amount, r.country) for r in df.collect()}


# --- no Spark needed ---------------------------------------------------------------------


def test_static_contract():
    assert static_contract_issues(IcebergSource) == []
    assert IcebergSource.capabilities() == ["batch", "streaming"]


def test_conflicting_options_are_reported_on_the_field_that_introduces_them():
    ok = IcebergSourceOptions(warehouse="/w", table="a.b", branch="audit", start_snapshot_id=5, end_snapshot_id=9,
                              read_options={"split-size": 1024, "locality": False})
    assert ok.batch_read_options() == {
        "split-size": "1024", "locality": "false", "branch": "audit", "start-snapshot-id": "5", "end-snapshot-id": "9",
    }
    for bad, message in [
        ({"table": "orders"}, "pattern"),  # a namespace is required, like the sink
        ({"catalog_type": "rest", "table": "a.b"}, r"uri\n  Value error, required for a rest catalog"),
        ({"snapshot_id": 1, "as_of_timestamp": "2024-01-01"}, r"as_of_timestamp\n  Value error, set either snapshot_id or as_of_timestamp"),
        ({"as_of_timestamp": "yesterday"}, r"as_of_timestamp\n  Value error, not a timestamp"),
        ({"snapshot_id": 1, "branch": "b"}, r"branch\n  Value error, cannot be combined with snapshot_id"),
        ({"branch": "b", "tag": "t"}, r"tag\n  Value error, cannot be combined with branch"),
        ({"tag": "t", "start_snapshot_id": 3}, r"start_snapshot_id\n  Value error, an incremental read cannot be combined with tag"),
        ({"end_snapshot_id": 3}, r"end_snapshot_id\n  Value error, needs start_snapshot_id"),
        ({"stream_from_timestamp": "soon"}, r"stream_from_timestamp\n  Value error, not a timestamp"),
        ({"max_rows_per_trigger": 0}, "greater than or equal to 1"),
    ]:
        with pytest.raises(ValidationError, match=message):
            IcebergSourceOptions(warehouse="/w", **{"table": "a.b", **bad})


def test_timestamps_accept_iso_8601_or_epoch_millis():
    assert to_millis("2024-05-01T00:00:00Z") == 1714521600000
    assert to_millis("2024-05-01") == 1714521600000  # naive = UTC
    assert to_millis("2024-05-01T02:00:00+02:00") == 1714521600000
    assert to_millis("1714521600000") == 1714521600000
    opts = IcebergSourceOptions(warehouse="/w", table="a.b", as_of_timestamp="2024-05-01T00:00:00Z",
                                stream_from_timestamp="1714521600000")
    assert opts.batch_read_options() == {"as-of-timestamp": "1714521600000"}
    assert opts.stream_read_options() == {"stream-from-timestamp": "1714521600000"}


def test_streaming_options_map_to_the_reader_and_pinned_reads_are_refused():
    opts = IcebergSourceOptions(warehouse="/w", table="a.b", streaming_skip_delete_snapshots=True,
                                streaming_skip_overwrite_snapshots=True, max_files_per_trigger=2, max_rows_per_trigger=100)
    assert opts.stream_read_options() == {
        "streaming-skip-delete-snapshots": "true",
        "streaming-skip-overwrite-snapshots": "true",
        "streaming-max-files-per-micro-batch": "2",
        "streaming-max-rows-per-micro-batch": "100",
    }
    # Iceberg silently streams the whole table when given a snapshot, tag or branch: say so instead.
    ctx = RunContext(spark=None, registry=default_registry(), pipeline_id="p", env="dev")
    pinned = IcebergSourceOptions(warehouse="/w", table="a.b", tag="v1")
    with pytest.raises(TributaryError, match="tag only applies to batch reads"):
        IcebergSource().read_stream(ctx, pinned, None)
    pinned = IcebergSourceOptions(warehouse="/w", table="a.b", branch="b", start_snapshot_id=1)
    with pytest.raises(TributaryError, match="branch, start_snapshot_id only apply to batch reads"):
        IcebergSource().read_stream(ctx, pinned, None)


def test_source_and_sink_share_the_catalog_settings():
    """One connection of type 'iceberg' serves both, and the same settings name the same catalog."""
    source_props = IcebergSourceOptions.model_json_schema()["properties"]
    sink_props = IcebergSinkOptions.model_json_schema()["properties"]
    for field in ("catalog_type", "warehouse", "uri", "credential", "token", "catalog_name", "catalog_properties", "table"):
        assert source_props[field] == sink_props[field], field
    src = IcebergSourceOptions(catalog_type="rest", uri="http://r:8181", token="t", table="a.b")
    sink = IcebergSinkOptions(catalog_type="rest", uri="http://r:8181", token="t", table="c.d")
    assert src.catalog == sink.catalog and src.catalog.startswith("iceberg_")
    assert IcebergSource().required_conf(src) == IcebergSink().required_conf(sink)
    assert IcebergSource.accepts_connection("iceberg") and not IcebergSource.accepts_connection("kafka")


def test_connection_supplies_the_catalog_to_a_source(tmp_path):
    doc = read_pipeline(tmp_path)
    doc["sources"] = [{"id": "orders_src", "type": "iceberg", "connection": "lake", "options": {"table": "sales.orders"}}]
    doc["environments"]["dev"]["connections"] = {
        "lake": {"type": "iceberg", "catalog_type": "hadoop", "warehouse": str(tmp_path / "warehouse"), "catalog_name": "lake"},
    }
    opts = compile_pipeline(doc).spec.sources[0].options
    assert (opts.catalog, opts.warehouse, opts.table) == ("lake", str(tmp_path / "warehouse"), "sales.orders")
    doc["environments"]["dev"]["connections"]["lake"]["type"] = "kafka"
    with pytest.raises(SpecError, match="connection 'lake' is 'kafka', node type is 'iceberg'"):
        compile_pipeline(doc)


def test_issues_land_on_the_source_field(tmp_path):
    doc = read_pipeline(tmp_path, end_snapshot_id=3)
    with pytest.raises(SpecError) as exc:
        compile_pipeline(doc)
    assert {(i.path, i.message) for i in exc.value.issues} == {("sources[0].options.end_snapshot_id", "needs start_snapshot_id")}


def test_session_gets_the_package_and_one_catalog_for_source_and_sink(tmp_path):
    doc = read_pipeline(tmp_path, catalog_name="lake")
    doc["sinks"] = [iceberg_sink(tmp_path / "warehouse", "sales.orders_copy", catalog_name="lake")]
    compiled = compile_pipeline(doc)
    [package] = required_packages(compiled)
    assert package.startswith("org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:")
    conf = session_conf(compiled, decide_resources(compiled))
    assert conf["spark.jars.packages"] == package
    assert conf["spark.sql.catalog.lake"] == "org.apache.iceberg.spark.SparkCatalog"
    assert conf["spark.sql.catalog.lake.warehouse"] == str(tmp_path / "warehouse")
    assert "spark.sql.extensions" not in conf
    # The same catalog name for a different warehouse is a spec problem, whichever side defines it.
    doc["sinks"][0]["options"]["warehouse"] = str(tmp_path / "elsewhere")
    with pytest.raises(TributaryError, match="spark.sql.catalog.lake.warehouse"):
        required_conf(compile_pipeline(doc))


def test_input_size_is_known_for_a_local_hadoop_warehouse(tmp_path):
    source = IcebergSource()
    local = IcebergSourceOptions(warehouse=str(tmp_path / "wh"), table="sales.orders")
    assert source.estimate_input_bytes(local) is None  # no table yet
    data = tmp_path / "wh" / "sales" / "orders" / "data"
    data.mkdir(parents=True)
    (data / "part-0.parquet").write_bytes(b"x" * 100)
    (data / "country=IN").mkdir()
    (data / "country=IN" / "part-1.parquet").write_bytes(b"y" * 50)
    assert source.estimate_input_bytes(local) == 150
    remote = IcebergSourceOptions(warehouse="s3a://bucket/wh", table="sales.orders")
    assert source.estimate_input_bytes(remote) is None
    rest = IcebergSourceOptions(catalog_type="rest", uri="http://r", table="sales.orders")
    assert source.estimate_input_bytes(rest) is None


# --- real reads (Hadoop catalog on local disk) ---------------------------------------------


def test_source_contract(iceberg_spark, tmp_path, orders_csv):
    opts = write_orders(iceberg_spark, tmp_path, orders_csv)
    assert_source_contract(iceberg_spark, IcebergSource(), IcebergSourceOptions(**opts.model_dump(include={"warehouse", "table"})))


def test_reads_the_current_table_time_travel_and_increments(iceberg_spark, tmp_path, orders_csv):
    first = write_orders(iceberg_spark, tmp_path, orders_csv, partition_by=["country"])
    update = tmp_path / "update.csv"
    update.write_text("order_id,amount,country\n6,60.0,fr\n")
    write_orders(iceberg_spark, tmp_path, update)  # second snapshot: one more row
    [old, new] = snapshots(iceberg_spark, first)
    # Tags and branches need Iceberg's SQL extensions or its API; use the API, the session has no extensions.
    table = iceberg_spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
        iceberg_spark._jsparkSession, f"{first.catalog}.{first.table}"
    )
    table.manageSnapshots().createTag("v1", old.snapshot_id).createBranch("audit", old.snapshot_id).commit()

    ctx = RunContext(spark=iceberg_spark, registry=default_registry(), pipeline_id="p", env="dev")
    all_rows = {1: (10.0, "IN"), 2: (-1.0, "US"), None: (5.0, "DE"), 4: (7.5, "USA"), 5: (3.0, "GB"), 6: (60.0, "FR")}
    first_rows = {k: v for k, v in all_rows.items() if k != 6}

    def read(**options):
        opts = IcebergSourceOptions(warehouse=str(tmp_path / "warehouse"), table="sales.orders", **options)
        return orders(IcebergSource().read(ctx, opts, None))

    assert read() == all_rows
    assert read(snapshot_id=old.snapshot_id) == first_rows
    assert read(as_of_timestamp=old.committed_at.isoformat()) == first_rows  # committed_at is UTC (session zone)
    assert read(as_of_timestamp=str(to_millis(new.committed_at.isoformat()))) == all_rows
    assert read(tag="v1") == first_rows
    assert read(branch="audit") == first_rows
    assert read(branch="main") == all_rows
    assert read(start_snapshot_id=old.snapshot_id) == {6: (60.0, "FR")}
    assert read(start_snapshot_id=old.snapshot_id, end_snapshot_id=new.snapshot_id) == {6: (60.0, "FR")}
    assert read(start_snapshot_id=new.snapshot_id) == {}


def test_declared_schema_selects_and_casts_columns(iceberg_spark, tmp_path, orders_csv):
    write_orders(iceberg_spark, tmp_path, orders_csv)
    doc = read_pipeline(tmp_path)
    doc["sources"][0]["schema"] = {"kind": "ddl", "ddl": "order_id STRING, amount DECIMAL(10,2)"}
    doc["transforms"][0]["options"]["sql"] = "SELECT order_id, amount FROM orders"
    # Known without reading, so deep validation does not need the catalog...
    analysis = analyze_pipeline(compile_pipeline(doc), iceberg_spark)
    assert [(f["name"], f["type"]) for f in analysis["orders_src"].schema["fields"]] == [("order_id", "string"), ("amount", "decimal(10,2)")]
    # ...and the data arrives in that shape.
    result = preview(doc, "orders_clean", spark=iceberg_spark, limit=10)
    assert [f["name"] for f in result.schema["fields"]] == ["order_id", "amount"]
    assert {r["order_id"]: r["amount"] for r in result.rows} == {"1": "10.00", "2": "-1.00", None: "5.00", "4": "7.50", "5": "3.00"}


def test_missing_table_is_a_clear_error(iceberg_spark, tmp_path):
    doc = read_pipeline(tmp_path, table="sales.nothing_here", catalog_name="empty_cat")
    with pytest.raises(TributaryError, match=r"iceberg table empty_cat.sales.nothing_here does not exist in the hadoop catalog at"):
        preview(doc, "orders_src", spark=iceberg_spark)


def test_preview_and_deep_validation_read_the_table(iceberg_spark, tmp_path, orders_csv):
    write_orders(iceberg_spark, tmp_path, orders_csv)
    doc = read_pipeline(tmp_path)
    # Without a declared schema the table's own schema comes back, by reading nothing.
    analysis = analyze_pipeline(compile_pipeline(doc), iceberg_spark)
    assert all(a.ok for a in analysis.values()), analysis
    assert [f["name"] for f in analysis["orders_src"].schema["fields"]] == ["order_id", "amount", "country"]
    result = preview(doc, "orders_src", spark=iceberg_spark, limit=2)
    assert result.kind == "source" and len(result.rows) == 2
    result = preview(doc, "orders_clean", spark=iceberg_spark, limit=10)
    assert [r["order_id"] for r in result.rows] == [None, 1, 2, 4, 5]


def test_iceberg_to_iceberg_through_one_connection(iceberg_spark, tmp_path, orders_csv):
    write_orders(iceberg_spark, tmp_path, orders_csv)
    doc = read_pipeline(tmp_path)
    doc["environments"]["dev"]["connections"] = {
        "lake": {"type": "iceberg", "catalog_type": "hadoop", "warehouse": str(tmp_path / "warehouse"), "catalog_name": "lake"},
    }
    doc["sources"] = [{"id": "orders_src", "type": "iceberg", "connection": "lake", "options": {"table": "sales.orders"}}]
    doc["transforms"][0]["options"]["sql"] = "SELECT order_id, amount * 2 AS amount, country FROM orders WHERE order_id IS NOT NULL"
    doc["sinks"] = [{"id": "orders_out", "type": "iceberg", "input": "orders_clean", "connection": "lake",
                     "options": {"table": "sales.orders_doubled", "mode": "overwrite"}}]
    compiled = compile_pipeline(doc)
    assert run(compiled, spark=iceberg_spark).sinks_written == ["orders_out"]
    assert run(compiled, spark=iceberg_spark).sinks_written == ["orders_out"]  # overwrite: still one copy
    assert orders(iceberg_spark.table("`lake`.`sales`.`orders_doubled`")) == {1: (20.0, "IN"), 2: (-2.0, "US"), 4: (15.0, "USA"), 5: (6.0, "GB")}
    assert iceberg_spark.table("`lake`.`sales`.`orders`").count() == 5  # the source table is untouched


def stream_pipeline(tmp_path, out, chk, trigger: dict, **source_options) -> dict:
    return {
        "pipeline": {"id": "iceberg_stream", "mode": "streaming", "streaming": {"checkpoint_location": str(chk), **trigger}},
        "sources": [iceberg_source(tmp_path / "warehouse", **source_options)],
        "transforms": [{"id": "orders_clean", "type": "sql", "inputs": [{"node": "orders_src", "as": "orders"}],
                        "options": {"sql": "SELECT order_id, amount, country FROM orders"}}],
        "sinks": [{"id": "out", "type": "file", "input": "orders_clean", "options": {"path": str(out), "format": "json"}}],
    }


def test_streaming_follows_appends(iceberg_spark, tmp_path, orders_csv):
    first = write_orders(iceberg_spark, tmp_path, orders_csv)
    [old] = snapshots(iceberg_spark, first)
    out, chk = tmp_path / "stream_out", tmp_path / "chk"
    doc = stream_pipeline(tmp_path, out, chk, {"trigger": "available_now"}, stream_from_timestamp=old.committed_at.isoformat())
    query = run(doc, spark=iceberg_spark)
    assert not query.isActive
    assert orders(iceberg_spark.read.json(str(out))) == {1: (10.0, "IN"), 2: (-1.0, "US"), None: (5.0, "DE"), 4: (7.5, "USA"), 5: (3.0, "GB")}

    # Rows appended later arrive in the next run from the same checkpoint; nothing is replayed.
    update = tmp_path / "update.csv"
    update.write_text("order_id,amount,country\n6,60.0,fr\n7,70.0,it\n")
    write_orders(iceberg_spark, tmp_path, update)
    run(doc, spark=iceberg_spark)
    streamed = orders(iceberg_spark.read.json(str(out)))
    assert len(streamed) == 7 and streamed[6] == (60.0, "FR") and streamed[7] == (70.0, "IT")

    # An overwrite is not an append: the stream stops unless told to skip such snapshots.
    write_orders(iceberg_spark, tmp_path, update, mode="overwrite")
    with pytest.raises(Exception, match="overwrite"):
        run(doc, spark=iceberg_spark)
    doc["sources"][0]["options"]["streaming_skip_overwrite_snapshots"] = True
    run(doc, spark=iceberg_spark)
    assert len(orders(iceberg_spark.read.json(str(out)))) == 7


def test_rate_limits_throttle_micro_batches(iceberg_spark, tmp_path, orders_csv):
    """One data file per micro-batch with a processing_time trigger. With available_now the
    Iceberg runtime this project ships would stall the stream instead, so that is refused."""
    import time

    first = write_orders(iceberg_spark, tmp_path, orders_csv)
    update = tmp_path / "update.csv"
    update.write_text("order_id,amount,country\n6,60.0,fr\n")
    write_orders(iceberg_spark, tmp_path, update)
    write_orders(iceberg_spark, tmp_path, update)
    assert len(snapshots(iceberg_spark, first)) == 3  # three appends, one data file each
    out, chk = tmp_path / "stream_out", tmp_path / "chk"

    doc = stream_pipeline(tmp_path, out, chk, {"trigger": "available_now"}, max_files_per_trigger=1)
    with pytest.raises(TributaryError, match="max_files_per_trigger cannot be combined with the available_now trigger"):
        run(doc, spark=iceberg_spark)
    assert not chk.exists()  # refused before the query started

    doc = stream_pipeline(tmp_path, out, chk, {"trigger": "processing_time", "interval": "500 milliseconds"}, max_files_per_trigger=1)
    query = run(doc, spark=iceberg_spark, await_termination=False)
    try:
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline and len([p for p in query.recentProgress if p["numInputRows"] > 0]) < 3:
            time.sleep(0.5)
        batches = [p["numInputRows"] for p in query.recentProgress if p["numInputRows"] > 0]
    finally:
        query.stop()
    assert batches == [5, 1, 1]  # one file per micro-batch, in commit order
    assert iceberg_spark.read.json(str(out)).count() == 7
