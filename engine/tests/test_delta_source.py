"""The Delta source: options, what the session needs, batch reads (the current table, time
travel, a declared schema as a contract), the change data feed, previews and design-time
analysis, and streams (start positions, updates and deletes, a change feed kept current
in a merge table).

Tests that read need the Delta jars (see ``delta_jars`` in conftest); the others run
without Spark. The shared session has neither Delta's SQL extension nor its catalog set:
reads must not need them.
"""

from __future__ import annotations

import time

import pytest
from pydantic import ValidationError

from conftest import pipeline
from tributary import SpecError, TributaryError, analyze, compile_pipeline, preview, run
from tributary.core import RunContext
from tributary.plugins.sinks.delta import DELTA_CATALOG, SESSION_CATALOG_KEY, DeltaSink, DeltaSinkOptions
from tributary.plugins.sources.delta import DeltaSource, DeltaSourceOptions
from tributary.registry import default_registry
from tributary.runtime import decide_resources, required_conf, required_packages, session_conf
from tributary.testing import assert_source_contract, static_contract_issues

SCHEMA = "order_id INT, amount DOUBLE, country STRING"
CHANGE_COLUMNS = ["_change_type", "_commit_version", "_commit_timestamp"]


def ctx(spark) -> RunContext:
    return RunContext(spark=spark, registry=default_registry(), pipeline_id="delta_source_test", env="dev")


def write(spark, path, records, schema=SCHEMA, **options) -> None:
    DeltaSink().write(ctx(spark), spark.createDataFrame(records, schema), DeltaSinkOptions(path=str(path), **options))


def upsert(spark, path, records, schema=SCHEMA, **options) -> None:
    write(spark, path, records, schema, mode="merge", merge_keys=["order_id"], **options)


@pytest.fixture
def table(delta_spark, tmp_path):
    """Three commits: 0 creates (with a change data feed), 1 updates 2 and inserts 3, 2 deletes 1."""
    path = tmp_path / "orders"
    write(delta_spark, path, [(1, 10.0, "in"), (2, 20.0, "us")], write_options={"delta.enableChangeDataFeed": True})
    upsert(delta_spark, path, [(2, 25.0, "us"), (3, 30.0, "de")])
    upsert(delta_spark, path, [(1, 0.0, "in", "D")], SCHEMA + ", op STRING", delete_condition="s.op = 'D'")
    return path


def source(path, schema=None, **options) -> dict:
    node = {"id": "orders", "type": "delta", "options": {"path": str(path), **options}}
    if schema:
        node["schema"] = {"kind": "ddl", "ddl": schema}
    return node


def doc(tmp_path, src: dict, sql="SELECT * FROM orders", sink: dict | None = None, mode="batch", **streaming) -> dict:
    return {
        "pipeline": {"id": "from_delta", "mode": mode, "streaming": {"checkpoint_location": str(tmp_path / "chk"), "trigger": "available_now", **streaming}},
        "sources": [src],
        "transforms": [{"id": "clean", "type": "sql", "inputs": ["orders"], "options": {"sql": sql}}],
        "sinks": [sink or {"id": "out", "type": "file", "input": "clean", "options": {"path": str(tmp_path / "out"), "format": "parquet", "mode": "overwrite"}}],
    }


def parquet(spark, tmp_path) -> list[tuple]:
    return sorted(tuple(r) for r in spark.read.parquet(str(tmp_path / "out")).collect())


def rows(spark, path) -> dict[int, float]:
    return {r.order_id: r.amount for r in spark.read.format("delta").load(str(path)).collect()}


def rows_or_none(spark, path) -> dict[int, float] | None:
    from pyspark.errors import AnalysisException

    try:
        return rows(spark, path)
    except AnalysisException:
        return None  # the table does not exist yet


def wait_until(predicate, what: str, limit: float = 120) -> None:
    deadline = time.monotonic() + limit
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.5)
    raise AssertionError(f"timed out waiting for {what}")


def changes(records) -> list[tuple]:
    """(order_id, change type, version) of change feed rows, in commit order."""
    return sorted((r["order_id"], r["_change_type"], r["_commit_version"]) for r in records)


# --- no Spark needed ---------------------------------------------------------------------


def test_registered_with_its_contract_met():
    assert default_registry().get("source", "delta") is DeltaSource
    assert static_contract_issues(DeltaSource) == []
    assert DeltaSource.capabilities() == ["batch", "streaming"]
    assert DeltaSource.spark_packages == DeltaSink.spark_packages  # one jar, both directions
    assert DeltaSource.metadata()["display_name"] == "Delta Lake"


def test_options_are_validated():
    ok = DeltaSourceOptions(path="/t", starting_version=" Latest ")
    assert ok.starting_version == "latest" and ok.on_changes == "fail" and not ok.change_feed
    assert DeltaSourceOptions(path="/t", starting_version="12").starting_version == 12
    for bad, message in [
        ({}, "path"),
        ({"path": "/t", "version": 1, "timestamp": "2024-05-01"}, r"timestamp\n  Value error, set either version or timestamp"),
        ({"path": "/t", "version": -1}, "greater than or equal to 0"),
        ({"path": "/t", "change_feed": True, "version": 0}, r"version\n  Value error, does not apply to a change feed"),
        ({"path": "/t", "change_feed": True, "timestamp": "2024-05-01"}, r"timestamp\n  Value error, does not apply to a change feed"),
        ({"path": "/t", "starting_version": "v3"}, "must be a version number \\(0 or more\\) or 'latest'"),
        ({"path": "/t", "starting_version": -1}, "must be a version number"),
        ({"path": "/t", "starting_version": 1, "starting_timestamp": "2024-05-01"}, r"starting_timestamp\n  Value error, set either"),
        ({"path": "/t", "change_feed": True, "on_changes": "skip_commits"}, r"on_changes\n  Value error, does not apply to a change feed"),
        ({"path": "/t", "on_changes": "ignore"}, "on_changes"),
        ({"path": "/t", "max_files_per_trigger": 0}, "greater than or equal to 1"),
        ({"path": "/t", "format": "delta"}, "extra"),
    ]:
        with pytest.raises(ValidationError, match=message):
            DeltaSourceOptions(**bad)


def test_fields_say_when_they_apply():
    """Time travel and change handling only make sense outside a change feed read; the UI
    hides them (and drops their values) when change_feed is turned on."""
    props = DeltaSourceOptions.model_json_schema()["properties"]
    for field in ("version", "timestamp", "on_changes"):
        assert props[field]["x-when"] == {"field": "change_feed", "in": [False], "required_in": []}
    assert "x-when" not in props["starting_version"] and "x-when" not in props["starting_timestamp"]
    assert props["on_changes"]["enum"] == ["fail", "ignore_deletes", "ignore_changes", "skip_commits"]
    assert DeltaSourceOptions.model_json_schema()["required"] == ["path"]


def test_issues_are_reported_on_their_field(tmp_path):
    bad = source("/t", change_feed=True, version=2, on_changes="skip_commits", starting_version="first")
    with pytest.raises(SpecError) as exc:
        compile_pipeline(doc(tmp_path, bad))
    assert {(i.path, i.message) for i in exc.value.issues} == {
        ("sources[0].options.version", "does not apply to a change feed read, which starts at starting_version or starting_timestamp"),
        ("sources[0].options.starting_version", "must be a version number (0 or more) or 'latest'"),
        ("sources[0].options.on_changes", "does not apply to a change feed, which delivers updates and deletes as rows"),
    }


def test_session_gets_the_jar_and_the_catalog_but_no_extensions(tmp_path):
    """Reads need Delta's session catalog as much as writes do; a pipeline using both the
    source and the sink asks for the same thing once."""
    compiled = compile_pipeline(doc(tmp_path, source("/lake/orders")))
    assert required_packages(compiled) == [p.format(scala_version="2.12") for p in DeltaSink.spark_packages]
    assert required_conf(compiled) == {SESSION_CATALOG_KEY: DELTA_CATALOG}

    sink = {"id": "out", "type": "delta", "input": "clean", "options": {"path": "/lake/copy"}}
    both = compile_pipeline(doc(tmp_path, source("/lake/orders"), sink=sink))
    assert required_packages(both) == required_packages(compiled)  # listed once
    assert required_conf(both) == {SESSION_CATALOG_KEY: DELTA_CATALOG}
    assert "spark.sql.extensions" not in session_conf(both, decide_resources(both))


def test_output_schema_is_the_declared_one_plus_the_change_feed_columns():
    from pyspark.sql.types import DoubleType, IntegerType, StructField, StructType

    from tributary.core import ResolvedSchema

    assert DeltaSource().output_schema(DeltaSourceOptions(path="/t"), None) is None  # read from the table instead
    declared = ResolvedSchema(struct=StructType([StructField("order_id", IntegerType()), StructField("amount", DoubleType())]))
    assert DeltaSource().output_schema(DeltaSourceOptions(path="/t"), declared).fieldNames() == ["order_id", "amount"]
    fed = DeltaSource().output_schema(DeltaSourceOptions(path="/t", change_feed=True), declared)
    assert fed.fieldNames() == ["order_id", "amount", *CHANGE_COLUMNS]
    assert [f.dataType.simpleString() for f in fed.fields[2:]] == ["string", "bigint", "timestamp"]


def test_input_size_is_the_table_data_on_local_disk(tmp_path):
    table = tmp_path / "t"
    (table / "_delta_log").mkdir(parents=True)
    (table / "_delta_log" / "00000000000000000000.json").write_bytes(b"x" * 500)
    (table / "country=IN").mkdir()
    (table / "country=IN" / "part-0.snappy.parquet").write_bytes(b"x" * 1000)
    (table / "part-1.snappy.parquet").write_bytes(b"x" * 24)
    (table / "part-1.snappy.parquet.crc").write_bytes(b"x" * 8)
    size = DeltaSource().estimate_input_bytes
    assert size(DeltaSourceOptions(path=str(table))) == 1024
    assert size(DeltaSourceOptions(path=f"file://{table}")) == 1024
    assert size(DeltaSourceOptions(path=str(tmp_path))) is None  # not a table
    assert size(DeltaSourceOptions(path="s3a://bucket/lake/orders")) is None


# --- reads (local disk) ----------------------------------------------------------------------


def test_source_contract(table, delta_spark):
    assert_source_contract(delta_spark, DeltaSource(), DeltaSourceOptions(path=str(table)), limit=2)


def test_a_shared_session_gets_deltas_catalog_before_the_first_read(table, delta_spark):
    """The backend's preview session is built without it; the source sets it, as the sink does."""
    other = delta_spark.newSession()
    assert other.conf.get(SESSION_CATALOG_KEY, None) is None
    sample = DeltaSource().read_preview(ctx(other), DeltaSourceOptions(path=str(table)), None, 5)
    assert other.conf.get(SESSION_CATALOG_KEY) == DELTA_CATALOG and sample.count() == 2
    assert other.conf.get("spark.sql.extensions", None) is None


def test_reads_the_current_table_or_a_past_version(table, delta_spark, tmp_path):
    assert run(doc(tmp_path, source(table)), spark=delta_spark).sinks_written == ["out"]
    assert parquet(delta_spark, tmp_path) == [(2, 25.0, "us"), (3, 30.0, "de")]

    run(doc(tmp_path, source(table, version=0)), spark=delta_spark)
    assert parquet(delta_spark, tmp_path) == [(1, 10.0, "in"), (2, 20.0, "us")]
    run(doc(tmp_path, source(table, version=1)), spark=delta_spark)
    assert parquet(delta_spark, tmp_path) == [(1, 10.0, "in"), (2, 25.0, "us"), (3, 30.0, "de")]

    from delta.tables import DeltaTable

    history = DeltaTable.forPath(delta_spark, str(table)).history().selectExpr("version", "cast(timestamp as string) AS ts")
    first_commit = history.filter("version = 0").collect()[0].ts
    run(doc(tmp_path, source(table, timestamp=first_commit)), spark=delta_spark)
    assert parquet(delta_spark, tmp_path) == [(1, 10.0, "in"), (2, 20.0, "us")]

    with pytest.raises(Exception, match="Cannot time travel Delta table to version 9"):
        run(doc(tmp_path, source(table, version=9)), spark=delta_spark)
    with pytest.raises(Exception, match="(?i)not a Delta table|doesn't exist|does not exist"):
        run(doc(tmp_path, source(tmp_path / "nope")), spark=delta_spark)


def test_declared_schema_is_a_contract_over_the_table(table, delta_spark, tmp_path):
    """The columns it names, cast to its types, in its order; the table's other columns are dropped."""
    run(doc(tmp_path, source(table, schema="amount DECIMAL(10,2), order_id STRING")), spark=delta_spark)
    assert parquet(delta_spark, tmp_path) == [(25.0, "2"), (30.0, "3")]
    assert [f.simpleString() for f in delta_spark.read.parquet(str(tmp_path / "out")).schema.fields] == [
        "amount:decimal(10,2)", "order_id:string"]

    with pytest.raises(TributaryError, match="schema column\\(s\\) not in the Delta table at .*: region \\(table columns: order_id, amount, country\\)"):
        run(doc(tmp_path, source(table, schema="order_id INT, region STRING")), spark=delta_spark)


def test_analysis_reads_the_table_schema_unless_one_is_declared(table, delta_spark, tmp_path):
    """Deep validation (what the editor runs) gets the columns from the table's log; with a
    declared schema it needs no table at all, so a pipeline can be drawn before the table
    its producer will create exists."""
    report = analyze(compile_pipeline(doc(tmp_path, source(table), sql="SELECT order_id, country FROM orders")), spark=delta_spark)
    assert report["orders"].ok and [f["name"] for f in report["orders"].schema["fields"]] == ["order_id", "amount", "country"]
    assert report["clean"].ok and [f["name"] for f in report["clean"].schema["fields"]] == ["order_id", "country"]

    report = analyze(compile_pipeline(doc(tmp_path, source(table, change_feed=True))), spark=delta_spark)
    assert [f["name"] for f in report["orders"].schema["fields"]] == ["order_id", "amount", "country", *CHANGE_COLUMNS]

    missing = tmp_path / "not_yet"
    report = analyze(compile_pipeline(doc(tmp_path, source(missing))), spark=delta_spark)
    assert not report["orders"].ok and "not_yet" in report["orders"].error
    assert report["clean"].error.startswith("skipped: upstream orders")
    report = analyze(compile_pipeline(doc(tmp_path, source(missing, schema="order_id INT, amount DOUBLE"))), spark=delta_spark)
    assert report["orders"].ok and [f["name"] for f in report["orders"].schema["fields"]] == ["order_id", "amount"]
    assert report["clean"].ok


def test_preview_is_a_bounded_read(table, delta_spark, tmp_path):
    result = preview(doc(tmp_path, source(table), sql="SELECT * FROM orders ORDER BY order_id"), "clean", spark=delta_spark, limit=1)
    assert result.rows == [{"order_id": 2, "amount": 25.0, "country": "us"}]
    result = preview(doc(tmp_path, source(table, version=0)), "orders", spark=delta_spark, limit=5)
    assert sorted(r["order_id"] for r in result.rows) == [1, 2]


# --- the change data feed ----------------------------------------------------------------------


def test_change_feed_in_batch_reads_the_changes_since_a_version_or_time(table, delta_spark, tmp_path):
    run(doc(tmp_path, source(table, change_feed=True, starting_version=0)), spark=delta_spark)
    feed = [r.asDict() for r in delta_spark.read.parquet(str(tmp_path / "out")).collect()]
    assert changes(feed) == [
        (1, "delete", 2), (1, "insert", 0),
        (2, "insert", 0), (2, "update_postimage", 1), (2, "update_preimage", 1),
        (3, "insert", 1),
    ]
    assert set(feed[0]) == {"order_id", "amount", "country", *CHANGE_COLUMNS}

    run(doc(tmp_path, source(table, change_feed=True, starting_version=2)), spark=delta_spark)
    assert changes(r.asDict() for r in delta_spark.read.parquet(str(tmp_path / "out")).collect()) == [(1, "delete", 2)]
    # 'latest' in a batch read is the most recent commit.
    run(doc(tmp_path, source(table, change_feed=True, starting_version="latest")), spark=delta_spark)
    assert changes(r.asDict() for r in delta_spark.read.parquet(str(tmp_path / "out")).collect()) == [(1, "delete", 2)]

    from delta.tables import DeltaTable

    history = DeltaTable.forPath(delta_spark, str(table)).history().selectExpr("version", "cast(timestamp as string) AS ts")
    second = history.filter("version = 1").collect()[0].ts
    run(doc(tmp_path, source(table, change_feed=True, starting_timestamp=second)), spark=delta_spark)
    assert {r._commit_version for r in delta_spark.read.parquet(str(tmp_path / "out")).collect()} == {1, 2}

    # A declared schema keeps the change columns next to the columns it names.
    run(doc(tmp_path, source(table, change_feed=True, starting_version=2, schema="order_id STRING")), spark=delta_spark)
    assert [tuple(r) for r in delta_spark.read.parquet(str(tmp_path / "out")).drop("_commit_timestamp").collect()] == [("1", "delete", 2)]


def test_change_feed_in_batch_needs_a_start_but_a_preview_samples_the_last_commit(table, delta_spark, tmp_path):
    with pytest.raises(TributaryError, match="reading a change feed in batch needs starting_version or starting_timestamp"):
        run(doc(tmp_path, source(table, change_feed=True)), spark=delta_spark)
    sample = preview(doc(tmp_path, source(table, change_feed=True)), "orders", spark=delta_spark)
    assert changes(sample.rows) == [(1, "delete", 2)]
    assert [f["name"] for f in sample.schema["fields"]] == ["order_id", "amount", "country", *CHANGE_COLUMNS]


def test_change_feed_needs_the_table_to_record_it(delta_spark, tmp_path):
    path = tmp_path / "plain"
    write(delta_spark, path, [(1, 1.0, "in")])
    with pytest.raises(Exception, match="DELTA_MISSING_CHANGE_DATA"):
        run(doc(tmp_path, source(path, change_feed=True, starting_version=0)), spark=delta_spark)


# --- streams ------------------------------------------------------------------------------------


def test_stream_starts_from_the_table_and_follows_new_commits(table, delta_spark, tmp_path):
    """available_now: the first run takes the table as it is, the next one only what was
    committed since (the checkpoint remembers), a run with nothing new writes nothing."""
    sink = {"id": "log", "type": "delta", "input": "clean", "options": {"path": str(tmp_path / "log")}}
    stream = doc(tmp_path, source(table), sink=sink, mode="streaming")
    assert not run(stream, spark=delta_spark).isActive
    assert rows(delta_spark, tmp_path / "log") == {2: 25.0, 3: 30.0}

    write(delta_spark, table, [(4, 40.0, "fr")])
    run(stream, spark=delta_spark)
    assert rows(delta_spark, tmp_path / "log") == {2: 25.0, 3: 30.0, 4: 40.0}
    before = rows(delta_spark, tmp_path / "log")
    run(stream, spark=delta_spark)
    assert rows(delta_spark, tmp_path / "log") == before



def test_latest_starts_a_running_query_at_the_next_commit(table, delta_spark, tmp_path):
    """starting_version latest: nothing of the table as it is, only commits made after the
    query started. That is a setting for a query that keeps running: with available_now
    and nothing to process, no offset is checkpointed, and the next run resolves 'latest'
    again, past whatever was committed in between."""
    new = tmp_path / "new"
    sink = {"id": "new", "type": "delta", "input": "clean", "options": {"path": str(new)}}
    stream = doc(tmp_path, source(table, starting_version="latest"), sink=sink, mode="streaming", trigger="processing_time", interval="1 second")
    query = run(stream, spark=delta_spark, await_termination=False)
    try:
        wait_until(lambda: query.lastProgress is not None, "the first (empty) trigger")  # 'latest' is resolved now
        assert not new.exists()
        write(delta_spark, table, [(5, 50.0, "it")])
        wait_until(lambda: rows_or_none(delta_spark, new) == {5: 50.0}, "the new commit")
        upsert(delta_spark, table, [(6, 60.0, "es")])
        wait_until(lambda: rows_or_none(delta_spark, new) == {5: 50.0, 6: 60.0}, "the next commit")
    finally:
        query.stop()
    assert not query.isActive


def test_time_travel_is_for_batch_reads(table, delta_spark, tmp_path):
    with pytest.raises(TributaryError, match="version/timestamp \\(time travel\\) apply to batch reads"):
        run(doc(tmp_path, source(table, version=0), mode="streaming"), spark=delta_spark)


def test_updates_and_deletes_in_a_stream_follow_on_changes(table, delta_spark, tmp_path):
    """From version 0 the stream meets an update (version 1) and a delete (version 2). The
    targets are merge tables, fed one file per micro-batch (max_files_per_trigger) so a
    re-emitted file never meets its own earlier version in the same batch."""

    def stream(name, **options):
        sink = {"id": "out", "type": "delta", "input": "clean", "options": {"path": str(tmp_path / name), "mode": "merge", "merge_keys": ["order_id"]}}
        return doc(tmp_path, source(table, starting_version=0, max_files_per_trigger=1, **options), sink=sink, mode="streaming",
                   checkpoint_location=str(tmp_path / f"chk_{name}"))

    with pytest.raises(Exception, match="(?i)data update.*not supported|skipChangeCommits"):
        run(stream("fails"), spark=delta_spark)  # Delta's default: a stream expects appends only
    run(stream("appends", on_changes="skip_commits"), spark=delta_spark)
    assert rows(delta_spark, tmp_path / "appends") == {1: 10.0, 2: 20.0}  # only version 0 was an append
    run(stream("rewritten", on_changes="ignore_changes"), spark=delta_spark)
    # The files the update and the delete rewrote come through again, so the merge table gets
    # the updated row; the deleted one stays, which is the documented limitation of
    # ignore_changes (the change feed is the way to propagate deletes).
    assert rows(delta_spark, tmp_path / "rewritten") == {1: 10.0, 2: 25.0, 3: 30.0}


def test_change_feed_stream_keeps_a_merge_table_current(table, delta_spark, tmp_path):
    """Delta to Delta change data capture: the feed, reduced to the last change per key in
    each micro-batch, is merged with deletes applied. Commits 0-2 arrive in one batch, so
    the first write of the target already has to leave out a deleted key."""
    sql = """
        SELECT order_id, amount, country, _change_type FROM (
            SELECT *, row_number() OVER (PARTITION BY order_id ORDER BY _commit_version DESC) AS rn
            FROM orders WHERE _change_type <> 'update_preimage'
        ) WHERE rn = 1
    """
    latest = tmp_path / "latest"
    sink = {"id": "latest", "type": "delta", "input": "clean",
            "options": {"path": str(latest), "mode": "merge", "merge_keys": ["order_id"], "delete_condition": "s._change_type = 'delete'"}}
    stream = doc(tmp_path, source(table, change_feed=True, starting_version=0), sql=sql, sink=sink, mode="streaming")
    run(stream, spark=delta_spark)
    assert rows(delta_spark, latest) == {2: 25.0, 3: 30.0}

    write(delta_spark, table, [(4, 40.0, "fr")])
    upsert(delta_spark, table, [(3, 0.0, "de", "D"), (2, 26.0, "us", "U")], SCHEMA + ", op STRING", delete_condition="s.op = 'D'")
    run(stream, spark=delta_spark)
    assert rows(delta_spark, latest) == {2: 26.0, 4: 40.0}
    assert rows(delta_spark, latest) == {k: v for k, v in rows(delta_spark, table).items()}  # the source's current state
    run(stream, spark=delta_spark)  # nothing new
    assert rows(delta_spark, latest) == {2: 26.0, 4: 40.0}
