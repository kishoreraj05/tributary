"""Scenario tests for the Iceberg source, one section per feature area:

1. snapshots and time travel (discovering snapshots, reading them, wrong references),
2. incremental and streaming ingestion (bookmarks, deletes, skipping history, Iceberg to Iceberg),
3. schema evolution (added/renamed/dropped columns, snapshot schemas, declared schemas, streams),
4. predicate pushdown and partition pruning (what reaches Iceberg's scan, files skipped).

All of them need the Iceberg runtime jar (see ``iceberg_jar`` in conftest) and run on a
Hadoop catalog on local disk. Tables are written by the Iceberg sink, as a user would.
``docs/iceberg-source.md`` walks through the same scenarios by hand.
"""

from __future__ import annotations

import re
import time

import pytest

from conftest import pipeline
from tributary import TributaryError, compile_pipeline, explain, preview, run
from tributary.core import RunContext
from tributary.execution import analyze_pipeline
from tributary.plugins.sinks.iceberg import IcebergSink, IcebergSinkOptions
from tributary.plugins.sources.iceberg import IcebergSource, IcebergSourceOptions
from tributary.registry import default_registry

pytestmark = pytest.mark.usefixtures("iceberg_spark")

WAREHOUSE = "warehouse"
TABLE = "sales.orders"


# --- helpers ------------------------------------------------------------------------------


def source(tmp_path, **options) -> dict:
    return {"id": "orders_src", "type": "iceberg",
            "options": {"catalog_type": "hadoop", "warehouse": str(tmp_path / WAREHOUSE), "table": TABLE, **options}}


def sink_options(tmp_path, **options) -> IcebergSinkOptions:
    return IcebergSinkOptions(warehouse=str(tmp_path / WAREHOUSE), table=TABLE, **options)


def source_options(tmp_path, **options) -> IcebergSourceOptions:
    return IcebergSourceOptions(warehouse=str(tmp_path / WAREHOUSE), table=TABLE, **options)


def ctx(spark) -> RunContext:
    return RunContext(spark=spark, registry=default_registry(), pipeline_id="scenario", env="dev")


def append(spark, tmp_path, rows: list[tuple], **sink_opts) -> int:
    """Writes rows (order_id, amount, country) with the Iceberg sink, as one file per partition
    (the scans below count files); returns the new snapshot id."""
    df = spark.createDataFrame(rows, "order_id INT, amount DOUBLE, country STRING").coalesce(1)
    IcebergSink().write(ctx(spark), df, sink_options(tmp_path, **sink_opts))
    return snapshots(spark, tmp_path)[-1].snapshot_id


def snapshots(spark, tmp_path) -> list:
    table = sink_options(tmp_path).qualified_table()
    return spark.sql(f"SELECT snapshot_id, committed_at, operation FROM {table}.snapshots ORDER BY committed_at").collect()


def read(spark, tmp_path, schema=None, **options) -> dict[int, tuple[float, str]]:
    df = IcebergSource().read(ctx(spark), source_options(tmp_path, **options), schema)
    return {r.order_id: (r.amount, r.country) for r in df.collect()}


def batch_pipeline(tmp_path, **source_options) -> dict:
    """Iceberg -> SQL (pass-through, ordered) -> parquet."""
    doc = pipeline(tmp_path, tmp_path / "unused.csv", sources=[source(tmp_path, **source_options)])
    doc["transforms"][0]["options"]["sql"] = "SELECT * FROM orders ORDER BY order_id"
    doc["sinks"][0]["options"] = {"path": str(tmp_path / "out"), "format": "parquet", "mode": "overwrite"}
    return doc


def stream_pipeline(tmp_path, out, chk, trigger="available_now", **source_options) -> dict:
    return {
        "pipeline": {"id": "iceberg_scenario_stream", "mode": "streaming",
                     "streaming": {"checkpoint_location": str(chk), "trigger": trigger}},
        "sources": [source(tmp_path, **source_options)],
        "transforms": [{"id": "orders_clean", "type": "sql", "inputs": [{"node": "orders_src", "as": "orders"}],
                        "options": {"sql": "SELECT * FROM orders"}}],
        "sinks": [{"id": "out", "type": "file", "input": "orders_clean", "options": {"path": str(out), "format": "json"}}],
    }


def run_output(spark, doc) -> dict[int, tuple[float, str]]:
    run(doc, spark=spark)
    path = doc["sinks"][0]["options"]["path"]
    fmt = doc["sinks"][0]["options"]["format"]
    df = spark.read.format(fmt).load(path)
    return {r.order_id: (r.amount, r.country) for r in df.collect()}


def scan(df) -> dict:
    """What Iceberg's scan saw and did for ``df``: the filters pushed into it, the columns it
    read and (after running) how many data files it read or skipped. These are the
    ``BatchScan`` node's description and metrics, the same that the Spark UI shows."""
    df.collect()
    node = next(n for n in _nodes(df._jdf.queryExecution().executedPlan()) if "BatchScan" in n.nodeName())
    text = node.toString()
    metrics = {}
    keys = node.metrics().keysIterator()
    while keys.hasNext():
        key = keys.next()
        metrics[key] = node.metrics().apply(key).value()
    filters = re.search(r"filters=(.*?), groupedBy=", text).group(1)
    columns = re.findall(r"(\w+)#\d+", text.split("]")[0])
    return {"filters": filters, "columns": columns, "files_read": metrics["resultDataFiles"],
            "files_skipped": metrics["skippedDataFiles"], "manifests_skipped": metrics["skippedDataManifests"]}


def _nodes(node):
    yield node
    for i in range(node.children().size()):
        yield from _nodes(node.children().apply(i))
    # AQE and reused exchanges hide the scan under a wrapped plan.
    for attr in ("inputPlan", "executedPlan", "child"):
        if hasattr(node, attr) and "AdaptiveSparkPlan" in node.nodeName():
            yield from _nodes(getattr(node, attr)())


ROWS_V1 = [(1, 10.0, "IN"), (2, 20.0, "US"), (3, 30.0, "DE")]
ROWS_V2 = [(4, 40.0, "IN"), (5, 50.0, "US")]
ROWS_V3 = [(6, 60.0, "FR")]
V1 = {1: (10.0, "IN"), 2: (20.0, "US"), 3: (30.0, "DE")}
V2 = {**V1, 4: (40.0, "IN"), 5: (50.0, "US")}
V3 = {**V2, 6: (60.0, "FR")}


@pytest.fixture
def three_snapshots(iceberg_spark, tmp_path) -> list[int]:
    """A table partitioned by country with three appends, one second apart (timestamps matter)."""
    ids = [append(iceberg_spark, tmp_path, ROWS_V1, partition_by=["country"])]
    for rows in (ROWS_V2, ROWS_V3):
        time.sleep(1.1)
        ids.append(append(iceberg_spark, tmp_path, rows))
    return ids


# =============================================================================================
# 1. Snapshots and time travel
# =============================================================================================


def test_snapshots_tags_and_branches_are_discoverable_from_a_source_node(iceberg_spark, tmp_path, three_snapshots):
    """metadata_table turns the node into a view of the table's metadata: the way to find the
    ids and times that time travel options take, from the UI's Test button or a preview."""
    doc = batch_pipeline(tmp_path, metadata_table="snapshots")
    doc["transforms"][0]["options"]["sql"] = "SELECT snapshot_id, operation, committed_at FROM orders ORDER BY committed_at"
    result = preview(doc, "orders_clean", spark=iceberg_spark, limit=10)
    # Snapshot ids are 64-bit: previews carry them as strings so the UI (JavaScript) shows every digit,
    # and the time travel options take them as strings too.
    assert [r["snapshot_id"] for r in result.rows] == [str(s) for s in three_snapshots]
    assert [r["operation"] for r in result.rows] == ["append"] * 3
    assert {f["name"] for f in result.schema["fields"]} == {"snapshot_id", "operation", "committed_at"}

    table = iceberg_spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
        iceberg_spark._jsparkSession, f"{sink_options(tmp_path).catalog}.{TABLE}"
    )
    table.manageSnapshots().createTag("v1", three_snapshots[0]).createBranch("audit", three_snapshots[1]).commit()
    refs = preview(batch_pipeline(tmp_path, metadata_table="refs"), "orders_src", spark=iceberg_spark, limit=10).rows
    assert {(r["name"], r["type"], str(r["snapshot_id"])) for r in refs} == {
        ("main", "BRANCH", str(three_snapshots[2])), ("audit", "BRANCH", str(three_snapshots[1])), ("v1", "TAG", str(three_snapshots[0])),
    }
    history = preview(batch_pipeline(tmp_path, metadata_table="history"), "orders_src", spark=iceberg_spark, limit=10).rows
    assert [str(h["snapshot_id"]) for h in history] == [str(s) for s in three_snapshots]
    # The ids shown can be pasted straight into snapshot_id.
    travelled = preview(batch_pipeline(tmp_path, snapshot_id=result.rows[0]["snapshot_id"]), "orders_src", spark=iceberg_spark, limit=10)
    assert {r["order_id"] for r in travelled.rows} == {1, 2, 3}
    partitions = preview(batch_pipeline(tmp_path, metadata_table="partitions"), "orders_src", spark=iceberg_spark).rows
    assert {(p["partition"]["country"], p["record_count"]) for p in partitions} == {("IN", 2), ("US", 2), ("DE", 1), ("FR", 1)}
    # Metadata tables are read as they are now: no time travel on top of them.
    with pytest.raises(Exception, match="cannot be combined with snapshot_id"):
        compile_pipeline(batch_pipeline(tmp_path, metadata_table="files", snapshot_id=three_snapshots[0]))


def test_every_time_travel_option_reads_the_table_as_it_was(iceberg_spark, tmp_path, three_snapshots):
    """Through a whole pipeline run: what the sink receives is the table at that point."""
    s1, s2, s3 = three_snapshots
    [t1, t2, t3] = [s.committed_at for s in snapshots(iceberg_spark, tmp_path)]
    assert run_output(iceberg_spark, batch_pipeline(tmp_path)) == V3
    assert run_output(iceberg_spark, batch_pipeline(tmp_path, snapshot_id=s1)) == V1
    assert run_output(iceberg_spark, batch_pipeline(tmp_path, snapshot_id=s2)) == V2
    # as_of_timestamp: the snapshot current at that time, given as ISO 8601 or epoch millis
    assert run_output(iceberg_spark, batch_pipeline(tmp_path, as_of_timestamp=t1.isoformat())) == V1
    assert run_output(iceberg_spark, batch_pipeline(tmp_path, as_of_timestamp=f"{t2.isoformat()}Z")) == V2
    between = int(t2.timestamp() * 1000) + 300  # after the second commit, before the third
    assert run_output(iceberg_spark, batch_pipeline(tmp_path, as_of_timestamp=str(between))) == V2
    assert run_output(iceberg_spark, batch_pipeline(tmp_path, as_of_timestamp=t3.isoformat())) == V3
    # tags and branches
    table = iceberg_spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
        iceberg_spark._jsparkSession, f"{sink_options(tmp_path).catalog}.{TABLE}"
    )
    table.manageSnapshots().createTag("v1", s1).createBranch("audit", s2).commit()
    assert run_output(iceberg_spark, batch_pipeline(tmp_path, tag="v1")) == V1
    assert run_output(iceberg_spark, batch_pipeline(tmp_path, branch="audit")) == V2
    assert run_output(iceberg_spark, batch_pipeline(tmp_path, branch="main")) == V3
    # The source says what it reads, so logs and previews show it.
    assert source_options(tmp_path, snapshot_id=s1).describe().endswith(f"{TABLE} (snapshot {s1}) at {tmp_path / WAREHOUSE}")
    assert "(tag v1)" in source_options(tmp_path, tag="v1").describe()
    assert "(branch audit)" in source_options(tmp_path, branch="audit").describe()


def test_time_travel_in_a_preview_and_in_deep_validation(iceberg_spark, tmp_path, three_snapshots):
    """What the UI's Test button and deep validation see for a time-travelling node."""
    s1 = three_snapshots[0]
    result = preview(batch_pipeline(tmp_path, snapshot_id=s1), "orders_src", spark=iceberg_spark, limit=10)
    assert {r["order_id"] for r in result.rows} == {1, 2, 3}
    analysis = analyze_pipeline(compile_pipeline(batch_pipeline(tmp_path, snapshot_id=s1)), iceberg_spark)
    assert all(a.ok for a in analysis.values())


def test_wrong_snapshot_tag_branch_or_time_is_a_clear_error(iceberg_spark, tmp_path, three_snapshots):
    """Iceberg's own messages for these are cryptic ("table ns.t.tag_x not found"); the source
    names the problem and lists what exists."""
    s1, s2, s3 = three_snapshots
    with pytest.raises(TributaryError, match=rf"snapshot_id 12345 is not a snapshot of \S+\.sales\.orders; the latest are {s3} \(.*\), {s2}"):
        read(iceberg_spark, tmp_path, snapshot_id=12345)
    with pytest.raises(TributaryError, match="start_snapshot_id 1 is not a snapshot"):
        read(iceberg_spark, tmp_path, start_snapshot_id=1)
    with pytest.raises(TributaryError, match="no snapshot at or before 2000-01-01T00:00:00Z; the oldest is from"):
        read(iceberg_spark, tmp_path, as_of_timestamp="2000-01-01T00:00:00Z")
    with pytest.raises(TributaryError, match=r"has no tag 'v9' \(tags: none\)"):
        read(iceberg_spark, tmp_path, tag="v9")
    with pytest.raises(TributaryError, match=r"has no branch 'dev' \(branches: main\)"):
        read(iceberg_spark, tmp_path, branch="dev")
    # ...and the same through a preview, which is where the UI reports them on the node.
    with pytest.raises(TributaryError, match="is not a snapshot of"):
        preview(batch_pipeline(tmp_path, snapshot_id=12345), "orders_src", spark=iceberg_spark)


# =============================================================================================
# 2. Incremental and streaming ingestion
# =============================================================================================


def test_incremental_reads_walk_the_snapshots_with_a_bookmark(iceberg_spark, tmp_path, three_snapshots):
    """The batch pattern: remember the last snapshot you processed and read what came after."""
    s1, s2, s3 = three_snapshots
    assert read(iceberg_spark, tmp_path, start_snapshot_id=s1) == {4: (40.0, "IN"), 5: (50.0, "US"), 6: (60.0, "FR")}
    assert read(iceberg_spark, tmp_path, start_snapshot_id=s1, end_snapshot_id=s2) == {4: (40.0, "IN"), 5: (50.0, "US")}
    assert read(iceberg_spark, tmp_path, start_snapshot_id=s2) == {6: (60.0, "FR")}
    assert read(iceberg_spark, tmp_path, start_snapshot_id=s3) == {}  # nothing new yet: no rows, no error
    assert read(iceberg_spark, tmp_path, start_snapshot_id=s1, end_snapshot_id=s1) == {}
    # The schema is the table's even when nothing is read, so downstream SQL still compiles.
    empty = IcebergSource().read(ctx(iceberg_spark), source_options(tmp_path, start_snapshot_id=s3), None)
    assert empty.columns == ["order_id", "amount", "country"]
    # Through a run, the way a scheduled job would use it.
    assert run_output(iceberg_spark, batch_pipeline(tmp_path, start_snapshot_id=s2)) == {6: (60.0, "FR")}
    s4 = append(iceberg_spark, tmp_path, [(7, 70.0, "IT")])
    assert run_output(iceberg_spark, batch_pipeline(tmp_path, start_snapshot_id=s3)) == {7: (70.0, "IT")}
    assert run_output(iceberg_spark, batch_pipeline(tmp_path, start_snapshot_id=s4)) == {}


def test_incremental_reads_only_see_appends(iceberg_spark, tmp_path, three_snapshots):
    """An overwrite or merge in between is not an append: its rows are not part of an increment
    (Iceberg's incremental scan covers append snapshots only)."""
    s1, s2, s3 = three_snapshots
    df = iceberg_spark.createDataFrame([(2, 99.0, "US"), (8, 80.0, "US")], "order_id INT, amount DOUBLE, country STRING")
    IcebergSink().write(ctx(iceberg_spark), df, sink_options(tmp_path, mode="merge", merge_keys=["order_id"]))
    append(iceberg_spark, tmp_path, [(9, 90.0, "ES")])
    assert [s.operation for s in snapshots(iceberg_spark, tmp_path)] == ["append", "append", "append", "overwrite", "append"]
    assert read(iceberg_spark, tmp_path, start_snapshot_id=s3) == {9: (90.0, "ES")}
    assert read(iceberg_spark, tmp_path)[2] == (99.0, "US") and 8 in read(iceberg_spark, tmp_path)


def test_stream_from_timestamp_skips_history_and_each_run_continues_from_its_checkpoint(iceberg_spark, tmp_path, three_snapshots):
    s1, s2, s3 = three_snapshots
    [t1, t2, t3] = [s.committed_at for s in snapshots(iceberg_spark, tmp_path)]
    out, chk = tmp_path / "stream_out", tmp_path / "chk"
    doc = stream_pipeline(tmp_path, out, chk, stream_from_timestamp=t2.isoformat())
    assert run_output(iceberg_spark, doc) == {4: (40.0, "IN"), 5: (50.0, "US"), 6: (60.0, "FR")}  # nothing from s1
    assert run_output(iceberg_spark, doc) == {4: (40.0, "IN"), 5: (50.0, "US"), 6: (60.0, "FR")}  # nothing new: nothing replayed
    append(iceberg_spark, tmp_path, [(7, 70.0, "IT")])
    assert run_output(iceberg_spark, doc)[7] == (70.0, "IT")
    # A fresh checkpoint without stream_from_timestamp starts with the whole table.
    fresh = stream_pipeline(tmp_path, tmp_path / "out2", tmp_path / "chk2")
    assert len(run_output(iceberg_spark, fresh)) == 7


def test_streams_stop_at_deletes_unless_told_to_skip_them(iceberg_spark, tmp_path):
    """A stream replays appends. A merge-on-read DELETE makes a 'delete' snapshot (a
    copy-on-write one makes an 'overwrite'); both stop the stream unless skipped."""
    mor = {"format-version": "2", "write.delete.mode": "merge-on-read"}
    append(iceberg_spark, tmp_path, ROWS_V1, table_properties=mor)
    out, chk = tmp_path / "stream_out", tmp_path / "chk"
    doc = stream_pipeline(tmp_path, out, chk)
    assert run_output(iceberg_spark, doc) == V1

    iceberg_spark.sql(f"DELETE FROM {sink_options(tmp_path).qualified_table()} WHERE order_id = 2")
    append(iceberg_spark, tmp_path, ROWS_V2)
    assert [s.operation for s in snapshots(iceberg_spark, tmp_path)] == ["append", "delete", "append"]
    with pytest.raises(Exception, match="Cannot process delete snapshot"):
        run(doc, spark=iceberg_spark)
    doc["sources"][0]["options"]["streaming_skip_delete_snapshots"] = True
    assert run_output(iceberg_spark, doc) == V2  # the deleted row stays in the output: streams are append-only
    # The same applies to a new stream whose history contains the delete.
    fresh = stream_pipeline(tmp_path, tmp_path / "out2", tmp_path / "chk2")
    with pytest.raises(Exception, match="Cannot process delete snapshot"):
        run(fresh, spark=iceberg_spark)
    fresh["sources"][0]["options"]["streaming_skip_delete_snapshots"] = True
    assert run_output(iceberg_spark, fresh) == V2


def test_streaming_iceberg_to_iceberg_is_exactly_once_across_restarts(iceberg_spark, tmp_path, three_snapshots):
    """Source and sink on one catalog: every micro-batch lands once, and a replayed
    micro-batch (restart between the commit and the checkpoint) is skipped by the sink."""
    chk = tmp_path / "chk"
    doc = stream_pipeline(tmp_path, tmp_path / "unused", chk)
    doc["sinks"] = [{"id": "copy", "type": "iceberg", "input": "orders_clean",
                     "options": {"catalog_type": "hadoop", "warehouse": str(tmp_path / WAREHOUSE), "table": "sales.orders_copy"}}]
    run(doc, spark=iceberg_spark)
    copy = IcebergSourceOptions(warehouse=str(tmp_path / WAREHOUSE), table="sales.orders_copy")

    def copied() -> set[int]:
        # Through the source: it refreshes the catalog's cached metadata, so it sees commits made
        # by the micro-batches' cloned session (a plain spark.table() here could lag by up to 30 s).
        return {r.order_id for r in IcebergSource().read(ctx(iceberg_spark), copy, None).collect()}

    assert copied() == {1, 2, 3, 4, 5, 6}
    append(iceberg_spark, tmp_path, [(7, 70.0, "IT")])
    run(doc, spark=iceberg_spark)
    assert copied() == {1, 2, 3, 4, 5, 6, 7}
    run(doc, spark=iceberg_spark)  # nothing new: no micro-batch, no snapshot
    assert copied() == {1, 2, 3, 4, 5, 6, 7}
    batches = IcebergSource().read(ctx(iceberg_spark), copy.model_copy(update={"metadata_table": "snapshots"}), None)
    tags = [(r.summary["tributary.pipeline"], r.summary["tributary.batch"]) for r in batches.orderBy("committed_at").collect()]
    assert tags == [("iceberg_scenario_stream", "0"), ("iceberg_scenario_stream", "1")]


# =============================================================================================
# 3. Schema evolution
# =============================================================================================


def test_added_renamed_and_dropped_columns_show_up_on_the_next_read(iceberg_spark, tmp_path, three_snapshots):
    table = sink_options(tmp_path).qualified_table()
    iceberg_spark.sql(f"ALTER TABLE {table} ADD COLUMNS (note STRING)")
    iceberg_spark.sql(f"INSERT INTO {table} VALUES (7, 70.0, 'IT', 'new')")
    df = IcebergSource().read(ctx(iceberg_spark), source_options(tmp_path), None)
    assert df.columns == ["order_id", "amount", "country", "note"]
    notes = {r.order_id: r.note for r in df.collect()}
    assert notes[7] == "new" and notes[1] is None  # old rows read as null for the new column

    iceberg_spark.sql(f"ALTER TABLE {table} RENAME COLUMN amount TO total")
    iceberg_spark.sql(f"ALTER TABLE {table} DROP COLUMN note")
    iceberg_spark.sql(f"ALTER TABLE {table} ALTER COLUMN order_id TYPE BIGINT")  # type promotion
    df = IcebergSource().read(ctx(iceberg_spark), source_options(tmp_path), None)
    assert df.columns == ["order_id", "total", "country"]
    assert dict(df.dtypes)["order_id"] == "bigint"
    assert {r.order_id: r.total for r in df.collect()}[1] == 10.0  # renamed in place, data intact

    # Deep validation reports the evolved schema, and SQL over the old name fails on that node.
    doc = batch_pipeline(tmp_path)
    doc["transforms"][0]["options"]["sql"] = "SELECT order_id, amount FROM orders"
    analysis = analyze_pipeline(compile_pipeline(doc), iceberg_spark)
    assert [f["name"] for f in analysis["orders_src"].schema["fields"]] == ["order_id", "total", "country"]
    assert not analysis["orders_clean"].ok and "amount" in analysis["orders_clean"].error


def test_time_travel_reads_use_the_schema_of_that_snapshot(iceberg_spark, tmp_path, three_snapshots):
    s1 = three_snapshots[0]
    table = sink_options(tmp_path).qualified_table()
    iceberg_spark.sql(f"ALTER TABLE {table} ADD COLUMNS (note STRING)")
    iceberg_spark.sql(f"ALTER TABLE {table} RENAME COLUMN amount TO total")
    now = IcebergSource().read(ctx(iceberg_spark), source_options(tmp_path), None)
    then = IcebergSource().read(ctx(iceberg_spark), source_options(tmp_path, snapshot_id=s1), None)
    assert now.columns == ["order_id", "total", "country", "note"]
    assert then.columns == ["order_id", "amount", "country"]
    # An increment, on the other hand, is read with the current schema.
    later = IcebergSource().read(ctx(iceberg_spark), source_options(tmp_path, start_snapshot_id=s1), None)
    assert later.columns == ["order_id", "total", "country", "note"]


def test_a_declared_schema_pins_the_contract(iceberg_spark, tmp_path, three_snapshots):
    """With a schema on the node, new table columns do not leak into the pipeline, types are
    cast, and a column the table lost is an error on the source node."""
    doc = batch_pipeline(tmp_path)
    doc["sources"][0]["schema"] = {"kind": "ddl", "ddl": "order_id BIGINT, amount DECIMAL(10,2), country STRING"}
    table = sink_options(tmp_path).qualified_table()
    iceberg_spark.sql(f"ALTER TABLE {table} ADD COLUMNS (note STRING)")
    result = preview(doc, "orders_src", spark=iceberg_spark, limit=10)
    assert [(f["name"], f["type"]) for f in result.schema["fields"]] == [("order_id", "long"), ("amount", "decimal(10,2)"), ("country", "string")]
    assert {r["order_id"]: r["amount"] for r in result.rows}[1] == "10.00"
    # Deep validation knows the declared schema without touching the catalog...
    analysis = analyze_pipeline(compile_pipeline(doc), iceberg_spark)
    assert [f["name"] for f in analysis["orders_src"].schema["fields"]] == ["order_id", "amount", "country"]
    # ...but a read reports a column that disappeared.
    iceberg_spark.sql(f"ALTER TABLE {table} DROP COLUMN amount")
    with pytest.raises(Exception, match="amount"):
        preview(doc, "orders_src", spark=iceberg_spark, limit=10)


def test_columns_added_through_the_sink_are_readable(iceberg_spark, tmp_path):
    """Schema evolution done by the pipeline itself: the sink merges new columns on write
    (write_options merge-schema, which Iceberg allows when the table accepts any schema)."""
    props = {"write.spark.accept-any-schema": "true"}
    append(iceberg_spark, tmp_path, ROWS_V1, table_properties=props)
    wider = iceberg_spark.createDataFrame([(4, 40.0, "IN", "vip")], "order_id INT, amount DOUBLE, country STRING, segment STRING")
    IcebergSink().write(ctx(iceberg_spark), wider, sink_options(tmp_path, write_options={"merge-schema": "true"}))
    df = IcebergSource().read(ctx(iceberg_spark), source_options(tmp_path), None)
    assert df.columns == ["order_id", "amount", "country", "segment"]
    assert {r.order_id: r.segment for r in df.collect()} == {1: None, 2: None, 3: None, 4: "vip"}


def test_a_stream_picks_up_new_columns_when_it_restarts(iceberg_spark, tmp_path, three_snapshots):
    """A running stream keeps the schema it started with; the next run (or restart) uses the
    table's current one, with null for rows written before the column existed."""
    out, chk = tmp_path / "stream_out", tmp_path / "chk"
    doc = stream_pipeline(tmp_path, out, chk)
    run(doc, spark=iceberg_spark)
    assert iceberg_spark.read.json(str(out)).columns == ["amount", "country", "order_id"]
    table = sink_options(tmp_path).qualified_table()
    iceberg_spark.sql(f"ALTER TABLE {table} ADD COLUMNS (note STRING)")
    iceberg_spark.sql(f"INSERT INTO {table} VALUES (7, 70.0, 'IT', 'new')")
    run(doc, spark=iceberg_spark)
    rows = {r.order_id: r.asDict().get("note") for r in iceberg_spark.read.json(str(out)).collect()}
    assert rows[7] == "new" and rows[1] is None


# =============================================================================================
# 4. Predicate pushdown and partition pruning
# =============================================================================================


@pytest.fixture
def partitioned_orders(iceberg_spark, tmp_path) -> int:
    """Partitioned by country: three appends, each adding one file per country present."""
    append(iceberg_spark, tmp_path, ROWS_V1, partition_by=["country"])  # IN, US, DE -> 3 files
    append(iceberg_spark, tmp_path, ROWS_V2)  # IN, US -> 2 files
    append(iceberg_spark, tmp_path, ROWS_V3)  # FR -> 1 file
    table = sink_options(tmp_path).qualified_table()
    files = iceberg_spark.sql(f"SELECT partition.country AS country FROM {table}.files").collect()
    assert sorted(f.country for f in files) == ["DE", "FR", "IN", "IN", "US", "US"]
    return 6


def test_source_filter_is_pushed_down_and_prunes_partitions(iceberg_spark, tmp_path, partitioned_orders):
    df = IcebergSource().read(ctx(iceberg_spark), source_options(tmp_path, filter="country = 'IN'"), None)
    info = scan(df)
    assert "country = 'IN'" in info["filters"]
    assert info["files_read"] == 2 and info["files_skipped"] + info["files_read"] <= partitioned_orders
    assert {r.order_id for r in df.collect()} == {1, 4}
    # A non-partition predicate is pushed too, and pruned by the files' min/max statistics.
    df = IcebergSource().read(ctx(iceberg_spark), source_options(tmp_path, filter="country = 'IN' AND amount > 15"), None)
    info = scan(df)
    assert "amount > 15.0" in info["filters"] and info["files_read"] == 1
    assert {r.order_id for r in df.collect()} == {4}
    # Pushdown survives time travel and increments.
    s1 = snapshots(iceberg_spark, tmp_path)[0].snapshot_id
    df = IcebergSource().read(ctx(iceberg_spark), source_options(tmp_path, filter="country = 'US'", start_snapshot_id=s1), None)
    assert "country = 'US'" in scan(df)["filters"] and {r.order_id for r in df.collect()} == {5}


def test_sql_predicates_reach_the_scan_in_a_run_but_not_in_a_bounded_preview(iceberg_spark, tmp_path, partitioned_orders):
    """A WHERE in a downstream SQL transform is pushed into Iceberg's scan when the pipeline
    runs (see `tributary plan --explain`). Previews read sources bounded by a LIMIT, which
    stops predicates at the source, so use the source's own `filter` to preview a subset."""
    doc = batch_pipeline(tmp_path)
    doc["transforms"][0]["options"]["sql"] = "SELECT order_id, amount FROM orders WHERE country = 'DE' AND amount >= 30"
    plans = explain(doc, spark=iceberg_spark)
    assert set(plans) == {"orders_src", "orders_clean"}
    assert "filters=, groupedBy=" in plans["orders_src"]
    assert re.search(r"filters=.*country = 'DE'.*amount >= 30\.0", plans["orders_clean"])
    assert "Output [3]: [order_id" in plans["orders_clean"]  # country is needed for the residual filter
    run(doc, spark=iceberg_spark)
    assert [r.order_id for r in iceberg_spark.read.parquet(str(tmp_path / "out")).collect()] == [3]

    preview_plan = preview(doc, "orders_clean", spark=iceberg_spark, limit=10, explain=True).plan
    assert "LocalLimit" in preview_plan and re.search(r"filters=, groupedBy=", preview_plan)
    doc["sources"][0]["options"]["filter"] = "country = 'DE'"
    preview_plan = preview(doc, "orders_clean", spark=iceberg_spark, limit=10, explain=True).plan
    assert re.search(r"filters=.*country = 'DE'", preview_plan)


def test_column_pruning(iceberg_spark, tmp_path, partitioned_orders):
    df = IcebergSource().read(ctx(iceberg_spark), source_options(tmp_path, columns=["order_id", "country"]), None)
    assert scan(df)["columns"] == ["order_id", "country"]
    assert df.columns == ["order_id", "country"]
    # ...also when the projection comes from SQL downstream
    doc = batch_pipeline(tmp_path)
    doc["transforms"][0]["options"]["sql"] = "SELECT country, count(*) AS n FROM orders GROUP BY country"
    assert "Output [1]: [country" in explain(doc, spark=iceberg_spark)["orders_clean"]
    # A filter may use a column the output leaves out.
    df = IcebergSource().read(ctx(iceberg_spark), source_options(tmp_path, columns=["order_id"], filter="amount > 45"), None)
    assert {r.order_id for r in df.collect()} == {5, 6} and df.columns == ["order_id"]


def test_file_statistics_prune_within_a_partition(iceberg_spark, tmp_path):
    """Without partitioning, Iceberg still skips files whose min/max bounds exclude the predicate."""
    for batch in ([(i, float(i), "X") for i in range(1, 6)], [(i, float(i), "X") for i in range(100, 106)],
                  [(i, float(i), "X") for i in range(1000, 1006)]):
        append(iceberg_spark, tmp_path, batch)
    df = IcebergSource().read(ctx(iceberg_spark), source_options(tmp_path, filter="order_id >= 1000"), None)
    info = scan(df)
    assert info["files_read"] == 1 and info["files_skipped"] == 2
    assert df.count() == 6
    df = IcebergSource().read(ctx(iceberg_spark), source_options(tmp_path, filter="order_id BETWEEN 3 AND 102"), None)
    assert scan(df)["files_read"] == 2
