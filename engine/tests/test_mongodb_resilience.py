"""The MongoDB connectors under the conditions that matter in production:

1. Idempotency: at-least-once delivery (a micro-batch replayed after a crash, a write
   retried after a partial failure, the same batch run twice) must not duplicate documents.
2. Schema: new fields flow into the documents; type drift is caught while the pipeline is
   drawn (sink) and handled per the chosen policy when reading (source).
3. Resiliency: the server going away for a while is retried and recovered from, in batch
   and in a running stream, without duplicates and without killing the query.

Tests that touch a server use ``mongodb_spark``/``flaky_mongodb`` from conftest.
"""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from pathlib import Path

import pytest
from pydantic import ValidationError

from conftest import MONGODB_URI, FlakyProxy, pipeline
from tributary import TributaryError, analyze, preview, run
from tributary.core import RunContext
from tributary.plugins._mongodb import SINGLE_PARTITION, is_transient, type_family
from tributary.plugins.sinks.mongodb import MongoDbSink, MongoDbSinkOptions, compatibility_issues, with_deterministic_ids
from tributary.plugins.sources.mongodb import MongoDbSourceOptions
from tributary.registry import default_registry

DB = "tributary_tests"
LIMIT = 120  # seconds to wait for a running query


def sink_options(collection: str, uri: str = MONGODB_URI, **overrides) -> MongoDbSinkOptions:
    return MongoDbSinkOptions(connection_uri=uri, database=DB, collection=collection, **overrides)


def read(spark, collection: str, uri: str = MONGODB_URI):
    return (
        spark.read.format("mongodb")
        .options(**{"connection.uri": uri, "database": DB, "collection": collection, "partitioner": SINGLE_PARTITION})
        .load()
    )


def docs(spark, collection: str) -> list[dict]:
    return sorted((r.asDict(recursive=True) for r in read(spark, collection).collect()), key=lambda d: json.dumps(d, sort_keys=True, default=str))


def ctx(spark, batch_id=None, run_id="run-1") -> RunContext:
    return RunContext(spark=spark, registry=default_registry(), pipeline_id="resilience", env="dev", batch_id=batch_id, run_id=run_id)


def stream_doc(tmp_path: Path, collection: str, *, uri: str = MONGODB_URI, trigger: str = "available_now", **sink_opts) -> dict:
    return {
        "pipeline": {
            "id": "events_to_mongodb",
            "mode": "streaming",
            "streaming": {"checkpoint_location": str(tmp_path / "chk"), "trigger": trigger, "interval": "1 second"},
        },
        "sources": [{
            "id": "events", "type": "file", "stream": True,
            "options": {"path": str(tmp_path / "in"), "format": "json", "max_files_per_trigger": 1},
            "schema": {"kind": "ddl", "ddl": "id INT, amount INT"},
        }],
        "sinks": [{
            "id": "out", "type": "mongodb", "input": "events",
            "options": {"connection_uri": uri, "database": DB, "collection": collection, **sink_opts},
        }],
    }


_ORDER = iter(range(10_000))


def drop_file(tmp_path: Path, name: str, *records: dict) -> None:
    (tmp_path / "in").mkdir(exist_ok=True)
    path = tmp_path / "in" / name
    path.write_text("".join(json.dumps(r) + "\n" for r in records))
    stamp = time.time() - 60 + next(_ORDER)  # files are read in modification-time order
    os.utime(path, (stamp, stamp))


def crash_before_checkpoint_commit(tmp_path: Path, batch_id: int) -> None:
    """Structured Streaming writes offsets/N before a micro-batch and commits/N after it.
    Removing commits/N leaves the checkpoint as a crash in between would: the next start
    replays batch N with the same id."""
    commits = tmp_path / "chk" / "commits"
    assert (commits / str(batch_id)).exists()
    (commits / str(batch_id)).unlink()
    (commits / f".{batch_id}.crc").unlink(missing_ok=True)


def wait_until(predicate, what: str, limit: float = LIMIT) -> None:
    deadline = time.monotonic() + limit
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.5)
    raise AssertionError(f"timed out waiting for {what}")


@pytest.fixture
def collection() -> str:
    return f"r_{uuid.uuid4().hex[:12]}"


# =========================================================================================
# 1. Idempotency and duplicate handling
# =========================================================================================


def test_deterministic_ids_are_stable_per_batch_and_distinct_per_row(spark):
    df = spark.createDataFrame([(1, "a"), (2, "b"), (2, "b")], "id INT, v STRING")  # a genuine duplicate row
    first = {r._id for r in with_deterministic_ids(ctx(spark, batch_id=7), df).collect()}
    again = {r._id for r in with_deterministic_ids(ctx(spark, batch_id=7, run_id="other-run"), df).collect()}
    assert len(first) == 3 and first == again, "same micro-batch, same ids (whatever the run)"
    assert first.isdisjoint({r._id for r in with_deterministic_ids(ctx(spark, batch_id=8), df).collect()}), "another batch, other ids"
    # Batch mode: scoped by run, so a re-run appends (as documented) while retries within it are safe.
    batch = {r._id for r in with_deterministic_ids(ctx(spark), df).collect()}
    assert batch == {r._id for r in with_deterministic_ids(ctx(spark), df).collect()}
    assert batch.isdisjoint({r._id for r in with_deterministic_ids(ctx(spark, run_id="run-2"), df).collect()})
    # A column named _id is the user's identity and is kept as is.
    keyed = spark.createDataFrame([("k1", 1)], "_id STRING, v INT")
    assert with_deterministic_ids(ctx(spark), keyed).columns == ["_id", "v"]
    assert with_deterministic_ids(ctx(spark), df).columns == ["_id", "id", "v"]


def test_options_for_idempotency_and_retries():
    assert sink_options("c").deterministic_ids and sink_options("c").retries == 3
    props = MongoDbSinkOptions.model_json_schema()["properties"]
    assert props["deterministic_ids"]["x-when"] == {"field": "mode", "in": ["append"], "required_in": []}
    with pytest.raises(ValidationError, match="retries"):
        sink_options("c", retries=-1)


def test_replayed_micro_batch_does_not_duplicate_appended_documents(mongodb_spark, tmp_path, collection):
    """At-least-once from Structured Streaming: a crash after the write but before the
    checkpoint commit replays the micro-batch. The appended documents are rewritten, not
    copied; genuine duplicates within the batch stay distinct."""
    drop_file(tmp_path, "b0.json", {"id": 1, "amount": 5}, {"id": 2, "amount": 7}, {"id": 2, "amount": 7})
    doc = stream_doc(tmp_path, collection)
    run(doc, spark=mongodb_spark)
    assert read(mongodb_spark, collection).count() == 3

    crash_before_checkpoint_commit(tmp_path, 0)
    run(doc, spark=mongodb_spark)  # replays batch 0 with the same id
    assert sorted((d["id"], d["amount"]) for d in docs(mongodb_spark, collection)) == [(1, 5), (2, 7), (2, 7)]

    # New data still arrives as new documents, and a second replay is just as harmless.
    drop_file(tmp_path, "b1.json", {"id": 3, "amount": 1})
    run(doc, spark=mongodb_spark)
    crash_before_checkpoint_commit(tmp_path, 1)
    run(doc, spark=mongodb_spark)
    assert sorted((d["id"], d["amount"]) for d in docs(mongodb_spark, collection)) == [(1, 5), (2, 7), (2, 7), (3, 1)]


def test_replay_duplicates_only_when_deterministic_ids_are_turned_off(mongodb_spark, tmp_path, collection):
    """The documented trade-off: ObjectIds from MongoDB mean every replay adds copies."""
    drop_file(tmp_path, "b0.json", {"id": 1, "amount": 5}, {"id": 2, "amount": 7})
    doc = stream_doc(tmp_path, collection, deterministic_ids=False)
    run(doc, spark=mongodb_spark)
    crash_before_checkpoint_commit(tmp_path, 0)
    run(doc, spark=mongodb_spark)
    assert read(mongodb_spark, collection).count() == 4


def test_retried_partial_write_does_not_duplicate(mongodb_spark, collection):
    """A write that failed half-way (some documents landed) and is retried as a whole, as
    the sink's own retry and Spark's micro-batch retry both do: the result holds each row once."""
    df = mongodb_spark.createDataFrame([(i, f"v{i}") for i in range(6)], "id INT, v STRING")
    batch = ctx(mongodb_spark, batch_id=3)
    sink, opts = MongoDbSink(), sink_options(collection)
    sink.write(batch, df.limit(2), opts)  # the part that made it before the failure
    sink.write(batch, df, opts)  # the retry
    assert sorted(d["id"] for d in docs(mongodb_spark, collection)) == [0, 1, 2, 3, 4, 5]

    # Upserts on merge keys are naturally idempotent, for replays and for batch re-runs alike.
    keyed = sink_options(collection + "_k", mode="upsert", merge_keys=["id"])
    for _ in range(3):
        sink.write(ctx(mongodb_spark, run_id=uuid.uuid4().hex), df, keyed)
    assert read(mongodb_spark, collection + "_k").count() == 6


def test_same_batch_pipeline_run_twice_appends_but_upsert_dedupes(mongodb_spark, tmp_path, orders_csv, collection):
    """Batch runs are deliberate: re-running an append pipeline adds the rows again (ids are
    scoped by run), while upsert on merge_keys is how reprocessing stays free of duplicates."""
    def sink(mode, target, **o):
        return {"id": "out", "type": "mongodb", "input": "orders_clean", "options": {"connection_uri": MONGODB_URI, "database": DB, "collection": target, "mode": mode, **o}}

    run(pipeline(tmp_path, orders_csv, sinks=[sink("append", collection)]), spark=mongodb_spark)
    run(pipeline(tmp_path, orders_csv, sinks=[sink("append", collection)]), spark=mongodb_spark)
    assert read(mongodb_spark, collection).count() == 10
    for _ in range(3):
        run(pipeline(tmp_path, orders_csv, sinks=[sink("upsert", collection + "_u", merge_keys=["order_id"])]), spark=mongodb_spark)
    assert read(mongodb_spark, collection + "_u").count() == 5


# =========================================================================================
# 2. Schema evaluation and validation
# =========================================================================================


def test_type_families_follow_bson():
    from pyspark.sql.types import ArrayType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, StringType, StructType, TimestampType

    assert type_family(IntegerType()) == type_family(LongType()) == "integer"
    assert type_family(FloatType()) == type_family(DoubleType()) == "double"
    assert type_family(DecimalType(10, 2)) == type_family(DecimalType(38, 6)) == "decimal"
    assert type_family(TimestampType()) == type_family(DateType()) == "datetime"
    assert type_family(StructType([])) == "struct" and type_family(ArrayType(StringType())) == "array"
    assert type_family(StringType()) != type_family(IntegerType())


def test_compatibility_issues_report_drift_and_allow_new_fields(mongodb_spark):
    existing = mongodb_spark.createDataFrame([], "order_id INT, amount DOUBLE, country STRING, order_ts TIMESTAMP").schema
    same = mongodb_spark.createDataFrame([], "order_id BIGINT, amount FLOAT, country STRING, order_ts DATE, note STRING").schema
    assert compatibility_issues(same, existing, sink_options("c", mode="upsert", merge_keys=["order_id"])) == []
    drifted = mongodb_spark.createDataFrame([], "order_id STRING, amount STRING, country STRING").schema
    issues = compatibility_issues(drifted, existing, sink_options("c", mode="upsert", merge_keys=["order_id"]))
    assert issues == [
        ("merge_keys", "order_id is int in the collection but string in the data: upserts would not match the existing documents and would add copies; cast it in the SQL upstream"),
        ("", "amount is double in the collection (sampled) but string in the data; cast it in the SQL upstream, or the field ends up with mixed types"),
    ]


def test_design_time_catches_type_drift_against_the_collection(mongodb_spark, tmp_path, orders_csv, collection):
    """Drawn against a collection that already holds documents, a transform that changes a
    column's type is flagged on the sink before anything is written."""
    sink = {"id": "orders_out", "type": "mongodb", "input": "orders_clean", "options": {"connection_uri": MONGODB_URI, "database": DB, "collection": collection, "mode": "upsert", "merge_keys": ["order_id"]}}
    run(pipeline(tmp_path, orders_csv, sinks=[sink]), spark=mongodb_spark)  # order_id int, amount double, country string

    drifted = pipeline(tmp_path, orders_csv, sinks=[sink])
    drifted["transforms"][0]["options"]["sql"] = "SELECT CAST(order_id AS STRING) AS order_id, CAST(amount AS STRING) AS amount, country, 'new' AS segment FROM orders"
    analysis = analyze(drifted, spark=mongodb_spark)["orders_out"]
    assert not analysis.ok
    assert [i["path"] for i in analysis.issues] == ["sinks[0].options.merge_keys", "sinks[0]"]
    assert "order_id is int in the collection but string in the data" in analysis.issues[0]["message"]
    assert "amount is double in the collection (sampled) but string in the data" in analysis.issues[1]["message"]

    widened = pipeline(tmp_path, orders_csv, sinks=[sink])  # a new column only: fine, documents are schemaless
    widened["transforms"][0]["options"]["sql"] = "SELECT order_id, amount, upper(country) AS country, 'new' AS segment FROM orders"
    assert analyze(widened, spark=mongodb_spark)["orders_out"].ok
    # An empty or unreachable target gives nothing to compare with, and never blocks the analysis.
    sink["options"]["collection"] = collection + "_missing"
    assert analyze(drifted, spark=mongodb_spark)["orders_out"].issues == []
    sink["options"]["connection_uri"] = "mongodb://127.0.0.1:1"
    started = time.monotonic()
    assert analyze(drifted, spark=mongodb_spark)["orders_out"].issues == []
    assert time.monotonic() - started < 30, "design-time checks use short timeouts"


def test_new_fields_flow_into_documents_in_every_mode(mongodb_spark, collection):
    sink = MongoDbSink()
    v1 = mongodb_spark.createDataFrame([(1, "a"), (2, "b")], "id INT, v STRING")
    v2 = mongodb_spark.createDataFrame([(2, "B", "x"), (3, "c", "y")], "id INT, v STRING, extra STRING")
    sink.write(ctx(mongodb_spark, batch_id=0), v1, sink_options(collection))
    sink.write(ctx(mongodb_spark, batch_id=1), v2, sink_options(collection))
    got = {(d["id"], d["v"]): d.get("extra") for d in docs(mongodb_spark, collection)}
    assert got == {(1, "a"): None, (2, "b"): None, (2, "B"): "x", (3, "c"): "y"}

    keyed = sink_options(collection + "_u", mode="upsert", merge_keys=["id"], on_match="update")
    sink.write(ctx(mongodb_spark), v1, keyed)
    sink.write(ctx(mongodb_spark), v2, keyed)
    assert {d["id"]: (d["v"], d.get("extra")) for d in docs(mongodb_spark, collection + "_u")} == {1: ("a", None), 2: ("B", "x"), 3: ("c", "y")}
    # update keeps fields the new row lacks; replace swaps the whole document.
    narrower = mongodb_spark.createDataFrame([(2, "BB")], "id INT, v STRING")
    sink.write(ctx(mongodb_spark), narrower, keyed)
    assert {d["id"]: (d["v"], d.get("extra")) for d in docs(mongodb_spark, collection + "_u")}[2] == ("BB", "x")
    sink.write(ctx(mongodb_spark), narrower, keyed.model_copy(update={"on_match": "replace"}))
    assert {d["id"]: (d["v"], d.get("extra")) for d in docs(mongodb_spark, collection + "_u")}[2] == ("BB", None)


def test_source_policy_for_documents_that_do_not_fit_the_declared_schema(mongodb_spark, collection):
    """Type drift in the collection itself (a string where the schema says double)."""
    sink = MongoDbSink()
    sink.write(ctx(mongodb_spark), mongodb_spark.createDataFrame([(1, 10.0), (2, 5.5)], "id INT, amount DOUBLE"), sink_options(collection))
    sink.write(ctx(mongodb_spark), mongodb_spark.createDataFrame([(3, "oops")], "id INT, amount STRING"), sink_options(collection))

    def source(on_malformed: str, schema: bool = True) -> dict:
        node = {"id": "src", "type": "mongodb", "options": {
            "connection_uri": MONGODB_URI, "database": DB, "collection": collection, "on_malformed": on_malformed,
            "read_options": {"partitioner": SINGLE_PARTITION},
        }}
        if schema:
            node["schema"] = {"kind": "ddl", "ddl": "id INT, amount DOUBLE"}
        return {"pipeline": {"id": "drift", "mode": "batch"}, "sources": [node], "sinks": [{"id": "out", "type": "console", "input": "src"}]}

    rows = lambda doc: sorted((r["id"], r["amount"]) for r in preview(doc, "src", spark=mongodb_spark).rows)  # noqa: E731
    assert rows(source("null")) == [(1, 10.0), (2, 5.5), (3, None)]
    assert rows(source("drop")) == [(1, 10.0), (2, 5.5)]
    with pytest.raises(Exception, match="(?i)invalid for|malformed|DataException"):
        rows(source("fail"))
    # Without a schema the connector infers the field as string rather than lose documents.
    inferred = preview(source("null", schema=False), "src", spark=mongodb_spark)
    assert {f["name"]: f["type"] for f in inferred.schema["fields"]}["amount"] == "string"
    assert sorted(r["amount"] for r in inferred.rows) == ["10.0", "5.5", "oops"]
    with pytest.raises(ValidationError, match="on_malformed"):
        MongoDbSourceOptions(connection_uri=MONGODB_URI, database=DB, collection=collection, on_malformed="ignore")


def test_dummy_data_follows_the_same_malformed_policy(spark):
    records = [{"id": 1, "amount": 10.0}, {"id": 2, "amount": "oops"}]
    schema = {"kind": "ddl", "ddl": "id INT, amount DOUBLE"}

    def doc(on_malformed: str) -> dict:
        return {"pipeline": {"id": "drift", "mode": "batch"},
                "sources": [{"id": "src", "type": "mongodb", "schema": schema, "options": {"use_dummy_data": True, "dummy_data": records, "on_malformed": on_malformed}}],
                "sinks": [{"id": "out", "type": "console", "input": "src"}]}

    rows = lambda d: sorted((r["id"], r["amount"]) for r in preview(d, "src", spark=spark).rows)  # noqa: E731
    assert rows(doc("drop")) == [(1, 10.0)]
    assert rows(doc("null"))[0] == (1, 10.0) and rows(doc("null"))[1][1] is None
    with pytest.raises(Exception, match="(?i)malformed"):
        rows(doc("fail"))


# =========================================================================================
# 3. Connection resiliency and network flakiness
# =========================================================================================


def test_transient_errors_are_told_apart_from_permanent_ones():
    transient = [
        "Caused by: com.mongodb.MongoSocketOpenException: Exception opening socket",
        "com.mongodb.MongoTimeoutException: Timed out after 2000 ms while waiting for a server that matches",
        "com.mongodb.MongoSocketReadException: Prematurely reached end of stream",
        "com.mongodb.MongoNotPrimaryException: ...",
        "com.mongodb.MongoCommandException: Command failed with error 10107 (NotWritablePrimary)",
        "Command failed with error 91 (ShutdownInProgress)",
        "java.net.ConnectException: Connection refused",
    ]
    assert all(is_transient(Exception(text)) for text in transient)
    permanent = [
        "com.mongodb.spark.sql.connector.exceptions.DataException: Invalid field: 'amount'",
        "com.mongodb.MongoWriteException: E11000 duplicate key error collection",
        "com.mongodb.MongoCommandException: Command failed with error 13 (Unauthorized)",
        "Command failed with error 8000 (AtlasError)",
    ]
    assert not any(is_transient(Exception(text)) for text in permanent)
    # The cause chain counts too (PySpark wraps Py4J errors).
    wrapped = RuntimeError("job aborted")
    wrapped.__cause__ = Exception("com.mongodb.MongoSocketWriteException: Exception sending message")
    assert is_transient(wrapped)


def test_write_waits_for_a_server_that_comes_back(mongodb_spark, flaky_mongodb, collection, caplog):
    """The server is down when the write starts: the attempts fail fast (short server
    selection timeout), the sink backs off and retries, and the write succeeds once the
    server is reachable again, with each row exactly once."""
    df = mongodb_spark.createDataFrame([(i, i * 1.5) for i in range(10)], "id INT, amount DOUBLE")
    opts = sink_options(collection, uri=flaky_mongodb.uri, retries=5, retry_backoff_seconds=1)
    flaky_mongodb.cut()
    threading.Timer(6.0, flaky_mongodb.restore).start()
    started = time.monotonic()
    with caplog.at_level("WARNING", logger="tributary"):
        MongoDbSink().write(ctx(mongodb_spark, batch_id=0), df, opts)
    assert time.monotonic() - started >= 5, "the write could not have succeeded before the server was back"
    assert any("transient error" in r.message and "retry 1/5" in r.message for r in caplog.records)
    assert sorted(d["id"] for d in docs(mongodb_spark, collection)) == list(range(10))


def test_write_gives_up_after_the_retries_and_never_retries_permanent_errors(mongodb_spark, flaky_mongodb, collection, caplog):
    df = mongodb_spark.createDataFrame([(1, "a")], "id INT, v STRING")
    flaky_mongodb.cut()
    started = time.monotonic()
    with caplog.at_level("WARNING", logger="tributary"), pytest.raises(Exception) as exc:
        MongoDbSink().write(ctx(mongodb_spark), df, sink_options(collection, uri=flaky_mongodb.uri, retries=2, retry_backoff_seconds=0.5))
    assert is_transient(exc.value) and time.monotonic() - started < 60
    assert [r.message.split(";")[1].strip().split(" in ")[0] for r in caplog.records if "transient error" in r.message] == ["retry 1/2", "retry 2/2"]

    # Not idempotent (plain append without deterministic ids): no retry, the error surfaces at once.
    caplog.clear()
    with pytest.raises(Exception):
        MongoDbSink().write(ctx(mongodb_spark), df, sink_options(collection, uri=flaky_mongodb.uri, retries=3, retry_backoff_seconds=0.5, deterministic_ids=False))
    assert not any("transient error" in r.message for r in caplog.records)
    # retries: 0 is "fail fast", whatever the error.
    with pytest.raises(Exception):
        MongoDbSink().write(ctx(mongodb_spark), df, sink_options(collection, uri=flaky_mongodb.uri, retries=0))
    assert not any("transient error" in r.message for r in caplog.records)


def test_stream_survives_the_server_going_away_mid_run(mongodb_spark, flaky_mongodb, tmp_path, collection):
    """A running query keeps going through an outage: the micro-batch that hits it is
    retried by the sink until the server is back, nothing is lost or duplicated, and the
    query is still active afterwards."""
    drop_file(tmp_path, "b0.json", {"id": 1, "amount": 5})
    doc = stream_doc(tmp_path, collection, uri=flaky_mongodb.uri, trigger="processing_time", retries=10, retry_backoff_seconds=1)
    query = run(doc, spark=mongodb_spark, await_termination=False)
    try:
        wait_until(lambda: read(mongodb_spark, collection).count() == 1, "the first micro-batch")
        flaky_mongodb.cut()
        drop_file(tmp_path, "b1.json", {"id": 2, "amount": 7}, {"id": 3, "amount": 1})
        time.sleep(8)  # long enough for the micro-batch to hit the outage and retry
        assert query.isActive, query.exception()
        flaky_mongodb.restore()
        wait_until(lambda: read(mongodb_spark, collection).count() == 3, "the batch written after the outage")
        drop_file(tmp_path, "b2.json", {"id": 4, "amount": 2})
        wait_until(lambda: read(mongodb_spark, collection).count() == 4, "a batch after recovery")
        assert query.isActive
        assert sorted((d["id"], d["amount"]) for d in docs(mongodb_spark, collection)) == [(1, 5), (2, 7), (3, 1), (4, 2)]
        assert query.exception() is None
    finally:
        query.stop()
    assert not query.isActive
