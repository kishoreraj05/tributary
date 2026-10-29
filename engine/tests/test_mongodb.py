"""The MongoDB sink: options, session packages, design-time checks and real writes.

Tests that write need the connector jars and a MongoDB server (``mongodb_spark`` in
conftest: ``docker compose up -d mongodb`` or TRIBUTARY_TEST_MONGODB_URI); the others run
without Spark. The shared session has no MongoDB-specific setting: the sink must not need one.
"""

from __future__ import annotations

import uuid

import pytest
from pydantic import ValidationError

from conftest import MONGODB_URI, pipeline
from tributary import SpecError, TributaryError, analyze, compile_pipeline, run
from tributary.core import RunContext
from tributary.plugins.sinks.mongodb import CONNECTOR_VERSION, MongoDbSink, MongoDbSinkOptions
from tributary.registry import default_registry
from tributary.runtime import decide_resources, required_conf, required_packages, session_conf
from tributary.testing import assert_sink_contract, static_contract_issues

URI = "mongodb://localhost:27017"
SINGLE_PARTITION = "com.mongodb.spark.sql.connector.read.partitioner.SinglePartitionPartitioner"


def options(**overrides) -> MongoDbSinkOptions:
    return MongoDbSinkOptions(**{"connection_uri": URI, "database": "shop", "collection": "orders", **overrides})


def mongodb_sink(collection: str, **options) -> dict:
    return {
        "id": "orders_out",
        "type": "mongodb",
        "input": "orders_clean",
        "options": {"connection_uri": MONGODB_URI, "database": "tributary_tests", "collection": collection, **options},
    }


def mongodb_pipeline(tmp_path, orders_csv, collection: str, **options) -> dict:
    return pipeline(tmp_path, orders_csv, sinks=[mongodb_sink(collection, **options)])


def read(spark, collection: str):
    # One partition, so the read never asks the server for collection statistics.
    return (
        spark.read.format("mongodb")
        .option("connection.uri", MONGODB_URI)
        .option("database", "tributary_tests")
        .option("collection", collection)
        .option("partitioner", SINGLE_PARTITION)
        .load()
    )


def rows(spark, collection: str) -> dict:
    return {r.order_id: (r.amount, r.country) for r in read(spark, collection).collect()}


def ctx(spark, batch_id=None) -> RunContext:
    return RunContext(spark=spark, registry=default_registry(), pipeline_id="mongodb_test", env="dev", batch_id=batch_id)


@pytest.fixture
def collection() -> str:
    """A collection of its own per test, so tests never see each other's documents."""
    return f"t_{uuid.uuid4().hex[:12]}"


# --- no Spark needed ---------------------------------------------------------------------


def test_registered_with_its_contract_met():
    assert default_registry().get("sink", "mongodb") is MongoDbSink
    assert static_contract_issues(MongoDbSink) == []
    assert MongoDbSink.spark_packages == (f"org.mongodb.spark:mongo-spark-connector_{{scala_version}}:{CONNECTOR_VERSION}",)
    assert MongoDbSink.accepts_connection("mongodb") and not MongoDbSink.accepts_connection("jdbc")


def test_options_are_validated():
    ok = options(mode="upsert", merge_keys=[" order_id "], database=" shop ", collection=" orders ")
    assert (ok.merge_keys, ok.database, ok.collection) == (["order_id"], "shop", "orders")
    for bad, message in [
        ({"connection_uri": URI, "database": "shop"}, "collection"),
        ({"connection_uri": "localhost:27017", "database": "shop", "collection": "c"}, "must start with mongodb://"),
        ({"connection_uri": URI, "database": "my.db", "collection": "c"}, r"database\n  Value error"),
        ({"connection_uri": URI, "database": "shop", "collection": "system.users"}, "start with system."),
        ({"connection_uri": URI, "database": "shop", "collection": "a$b"}, r"collection\n  Value error"),
        ({"connection_uri": URI, "database": "shop", "collection": "c", "mode": "upsert"}, "required for mode 'upsert'"),
        ({"connection_uri": URI, "database": "shop", "collection": "c", "merge_keys": ["id"]}, "only applies to mode 'upsert'"),
        ({"connection_uri": URI, "database": "shop", "collection": "c", "mode": "merge"}, "mode"),
        ({"connection_uri": URI, "database": "shop", "collection": "c", "on_match": "keep"}, "on_match"),
        ({"connection_uri": URI, "database": "shop", "collection": "c", "max_batch_size": 0}, "max_batch_size"),
        ({"connection_uri": URI, "database": "shop", "collection": "c", "path": "/x"}, "extra"),
    ]:
        with pytest.raises(ValidationError, match=message):
            MongoDbSinkOptions(**bad)


def test_fields_say_which_mode_they_apply_to():
    """The UI shows merge_keys (starred) and on_match only for mode upsert."""
    props = MongoDbSinkOptions.model_json_schema()["properties"]
    assert props["merge_keys"]["x-when"] == {"field": "mode", "in": ["upsert"], "required_in": ["upsert"]}
    assert props["on_match"]["x-when"] == {"field": "mode", "in": ["upsert"], "required_in": []}
    assert props["mode"]["enum"] == ["append", "overwrite", "upsert"]


def test_issues_are_reported_on_their_field(tmp_path, orders_csv):
    doc = mongodb_pipeline(tmp_path, orders_csv, "c", mode="append", merge_keys=["order_id"], database="a/b")
    with pytest.raises(SpecError) as exc:
        compile_pipeline(doc)
    assert {(i.path, i.message.split(" or ")[0]) for i in exc.value.issues} == {
        ("sinks[0].options.merge_keys", "only applies to mode 'upsert'"),
        ("sinks[0].options.database", "must not be empty"),
    }
    doc = mongodb_pipeline(tmp_path, orders_csv, "c", mode="upsert")
    with pytest.raises(SpecError) as exc:
        compile_pipeline(doc)
    assert [(i.path, i.message) for i in exc.value.issues] == [("sinks[0].options.merge_keys", "required for mode 'upsert'")]


def test_credentials_go_into_the_uri_but_never_into_logs():
    plain = options()
    assert plain.effective_uri() == URI and plain.host() == URI

    with_user = options(connection_uri="mongodb://db1,db2:27018/?replicaSet=rs0", username="app@corp", password="p:a/s s")
    assert with_user.effective_uri() == "mongodb://app%40corp:p%3Aa%2Fs%20s@db1,db2:27018/?replicaSet=rs0"
    assert with_user.host() == "mongodb://db1,db2:27018"
    description = MongoDbSink().describe(with_user)
    assert "corp" not in description and "p%3A" not in description and "s s" not in description

    inline = options(connection_uri="mongodb://u:p@host/admin?tls=true", username="other", password="x")
    assert inline.effective_uri() == "mongodb://u:p@host/admin?tls=true"  # the URI's own credentials win
    assert inline.host() == "mongodb://host"
    srv = options(connection_uri="mongodb+srv://cluster0.example.net/", username="u", password="p")
    assert srv.effective_uri() == "mongodb+srv://u:p@cluster0.example.net/"


def test_writer_options_follow_the_mode():
    append = options(write_options={"writeConcern.w": "majority", "convertJson": True}).writer_options()
    assert append == {
        "connection.uri": URI, "database": "shop", "collection": "orders", "operationType": "insert",
        "maxBatchSize": "512", "ordered": "true", "ignoreNullValues": "false",
        "writeConcern.w": "majority", "convertJson": "true",
    }
    upsert = options(mode="upsert", merge_keys=["a", "b"], on_match="update", ignore_null_values=True, ordered=False).writer_options()
    assert upsert["operationType"] == "update" and upsert["idFieldList"] == "a,b" and upsert["upsertDocument"] == "true"
    assert upsert["ignoreNullValues"] == "true" and upsert["ordered"] == "false"
    # Free-form options cannot override what the mode promises.
    with pytest.raises(ValidationError, match=r"write_options\n  Value error, set through the sink's own fields: operationType \(use mode\), idFieldList \(use merge_keys\)"):
        options(write_options={"operationType": "replace", "idFieldList": "x"})


def test_describe():
    assert MongoDbSink().describe(options()) == "mongodb -> shop.orders (append) at mongodb://localhost:27017"
    assert MongoDbSink().describe(options(mode="upsert", merge_keys=["order_id"])) == (
        "mongodb -> shop.orders (upsert on order_id) at mongodb://localhost:27017"
    )


def test_session_gets_the_connector_but_no_settings(tmp_path, orders_csv):
    compiled = compile_pipeline(mongodb_pipeline(tmp_path, orders_csv, "orders"))
    assert required_packages(compiled) == [f"org.mongodb.spark:mongo-spark-connector_2.12:{CONNECTOR_VERSION}"]
    assert required_conf(compiled) == {}
    conf = session_conf(compiled, decide_resources(compiled))
    assert f"mongo-spark-connector_2.12:{CONNECTOR_VERSION}" in conf["spark.jars.packages"]
    assert "spark.sql.extensions" not in conf


# --- design time (Spark, no server) -------------------------------------------------------


def test_analysis_reports_keys_and_field_names_the_data_cannot_give(spark, tmp_path, orders_csv):
    doc = mongodb_pipeline(tmp_path, orders_csv, "orders", mode="upsert", merge_keys=["order_id", "_id"])
    analysis = analyze(doc, spark=spark)
    sink = analysis["orders_out"]
    assert not sink.ok
    assert sink.issues == [{
        "path": "sinks[0].options.merge_keys",
        "message": "not in the data: _id (add a column named _id upstream, or choose another key) "
        "(columns: order_id, amount, country)",
    }]

    doc = mongodb_pipeline(tmp_path, orders_csv, "orders")
    doc["transforms"][0]["options"]["sql"] = "SELECT order_id AS `order.id`, amount AS `$amount`, country FROM orders"
    sink = analyze(doc, spark=spark)["orders_out"]
    assert [i["path"] for i in sink.issues] == ["sinks[0]"]
    assert sink.issues[0]["message"] == (
        "column 'order.id' has a dot in its name; column '$amount' starts with $; rename them in the SQL upstream"
    )
    assert analyze(mongodb_pipeline(tmp_path, orders_csv, "orders", mode="upsert", merge_keys=["order_id"]), spark=spark)["orders_out"].ok


# --- real writes (a MongoDB server) --------------------------------------------------------


def test_sink_contract(mongodb_spark, collection):
    opts = MongoDbSinkOptions(connection_uri=MONGODB_URI, database="tributary_tests", collection=collection)
    assert_sink_contract(mongodb_spark, MongoDbSink(), opts, mongodb_spark.range(3))
    assert read(mongodb_spark, collection).count() == 3


def test_append_inserts_every_run(mongodb_spark, tmp_path, orders_csv, collection):
    doc = mongodb_pipeline(tmp_path, orders_csv, collection)
    assert run(doc, spark=mongodb_spark).sinks_written == ["orders_out"]
    assert rows(mongodb_spark, collection) == {1: (10.0, "IN"), 2: (-1.0, "US"), None: (5.0, "DE"), 4: (7.5, "USA"), 5: (3.0, "GB")}
    run(doc, spark=mongodb_spark)
    assert read(mongodb_spark, collection).count() == 10


def test_overwrite_replaces_the_collection(mongodb_spark, tmp_path, orders_csv, collection):
    doc = mongodb_pipeline(tmp_path, orders_csv, collection, mode="overwrite")
    run(doc, spark=mongodb_spark)
    run(doc, spark=mongodb_spark)
    assert read(mongodb_spark, collection).count() == 5
    update = tmp_path / "update.csv"
    update.write_text("order_id,amount,country\n1,99.0,IN\n")
    run(pipeline(tmp_path, update, sinks=[mongodb_sink(collection, mode="overwrite")]), spark=mongodb_spark)
    assert rows(mongodb_spark, collection) == {1: (99.0, "IN")}


def test_upsert_replaces_or_updates_on_keys(mongodb_spark, tmp_path, orders_csv, collection):
    upsert = {"mode": "upsert", "merge_keys": ["order_id"]}
    run(mongodb_pipeline(tmp_path, orders_csv, collection, **upsert), spark=mongodb_spark)
    update = tmp_path / "update.csv"
    update.write_text("order_id,amount,country\n2,20.0,us\n6,60.0,fr\n")
    run(pipeline(tmp_path, update, sinks=[mongodb_sink(collection, **upsert)]), spark=mongodb_spark)
    assert rows(mongodb_spark, collection) == {1: (10.0, "IN"), 2: (20.0, "US"), None: (5.0, "DE"), 4: (7.5, "USA"), 5: (3.0, "GB"), 6: (60.0, "FR")}
    run(pipeline(tmp_path, update, sinks=[mongodb_sink(collection, **upsert)]), spark=mongodb_spark)  # same rows again: no change
    assert read(mongodb_spark, collection).count() == 6

    # replace drops fields the incoming row does not have; update keeps them.
    sink = MongoDbSink()
    opts = MongoDbSinkOptions(connection_uri=MONGODB_URI, database="tributary_tests", collection=collection, **upsert)
    narrower = mongodb_spark.createDataFrame([(2, "REPLACED")], "order_id INT, country STRING")
    sink.write(ctx(mongodb_spark), narrower, opts)
    assert rows(mongodb_spark, collection)[2] == (None, "REPLACED")
    sink.write(ctx(mongodb_spark), mongodb_spark.createDataFrame([(2, 2.0)], "order_id INT, amount DOUBLE"), opts.model_copy(update={"on_match": "update"}))
    assert rows(mongodb_spark, collection)[2] == (2.0, "REPLACED")

    bad = pipeline(tmp_path, update, sinks=[mongodb_sink(collection, mode="upsert", merge_keys=["nope"])])
    with pytest.raises(TributaryError, match="merge_keys not in the data: nope"):
        run(bad, spark=mongodb_spark)


def test_an_id_column_becomes_the_document_id(mongodb_spark, tmp_path, orders_csv, collection):
    doc = mongodb_pipeline(tmp_path, orders_csv, collection, mode="upsert", merge_keys=["_id"])
    doc["transforms"][0]["options"]["sql"] = "SELECT order_id AS _id, amount, upper(country) AS country FROM orders WHERE order_id IS NOT NULL"
    run(doc, spark=mongodb_spark)
    run(doc, spark=mongodb_spark)
    got = {r._id: r.country for r in read(mongodb_spark, collection).collect()}
    assert got == {1: "IN", 2: "US", 4: "USA", 5: "GB"}


def test_nulls_can_be_left_out_and_nested_data_becomes_documents(mongodb_spark, collection):
    df = mongodb_spark.sql(
        "SELECT 1 AS id, named_struct('city', 'Pune', 'zip', CAST(NULL AS STRING)) AS address, array(1, 2) AS tags, "
        "CAST(NULL AS STRING) AS note, TIMESTAMP '2026-10-01 10:00:00' AS ts"
    )
    MongoDbSink().write(ctx(mongodb_spark), df, MongoDbSinkOptions(connection_uri=MONGODB_URI, database="tributary_tests", collection=collection, ignore_null_values=True))
    back = read(mongodb_spark, collection).collect()[0].asDict(recursive=True)
    assert back["address"] == {"city": "Pune"} and back["tags"] == [1, 2] and "note" not in back
    assert str(back["ts"]).startswith("2026-10-01 10:00:00")


def test_micro_batches_upsert_without_duplicates(mongodb_spark, tmp_path, collection):
    """Streaming calls write once per micro-batch: a replayed batch in upsert mode changes nothing."""
    import json

    (tmp_path / "in").mkdir()
    (tmp_path / "in" / "a.json").write_text("".join(json.dumps(r) + "\n" for r in [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}]))
    doc = {
        "pipeline": {"id": "events_to_mongodb", "mode": "streaming",
                     "streaming": {"checkpoint_location": str(tmp_path / "chk"), "trigger": "available_now"}},
        "sources": [{"id": "events", "type": "file", "stream": True,
                     "options": {"path": str(tmp_path / "in"), "format": "json"},
                     "schema": {"kind": "ddl", "ddl": "id INT, v STRING"}}],
        "sinks": [{"id": "out", "type": "mongodb", "input": "events",
                   "options": {"connection_uri": MONGODB_URI, "database": "tributary_tests", "collection": collection,
                               "mode": "upsert", "merge_keys": ["id"]}}],
    }
    run(doc, spark=mongodb_spark)
    (tmp_path / "in" / "b.json").write_text(json.dumps({"id": 2, "v": "B"}) + "\n" + json.dumps({"id": 3, "v": "c"}) + "\n")
    run(doc, spark=mongodb_spark)
    assert {r.id: r.v for r in read(mongodb_spark, collection).collect()} == {1: "a", 2: "B", 3: "c"}
