"""The MongoDB source: options, dummy data that stands in for a collection (no server
needed), and reads of a real collection (``mongodb_spark`` in conftest)."""

from __future__ import annotations

import json
import uuid

import pytest
from pydantic import ValidationError

from conftest import MONGODB_URI
from tributary import SpecError, TributaryError, analyze, compile_pipeline, preview, run
from tributary.core import RunContext
from tributary.plugins.sources.mongodb import MAX_DUMMY_RECORDS, MongoDbSource, MongoDbSourceOptions
from tributary.registry import default_registry
from tributary.runtime import required_packages
from tributary.testing import assert_source_contract, static_contract_issues

URI = "mongodb://localhost:27017"
DOCS = [
    {"_id": "a1", "order_id": 1, "amount": 25.5, "country": "in", "customer": {"id": 100, "tier": "gold"}, "tags": ["web"]},
    {"_id": "a2", "order_id": 2, "amount": 99.99, "country": "us", "customer": {"id": 101, "tier": "silver"}, "tags": []},
    {"_id": "a3", "order_id": 3, "amount": 7.0, "country": "in", "customer": None, "tags": ["app", "promo"]},
]


def real(**overrides) -> dict:
    return {"connection_uri": URI, "database": "shop", "collection": "orders", **overrides}


def dummy(records=DOCS, **overrides) -> dict:
    return {"use_dummy_data": True, "dummy_data": records, **overrides}


def source_pipeline(options: dict, *, schema: dict | None = None, sql: str | None = None, sink: dict | None = None) -> dict:
    source = {"id": "orders_mongo", "type": "mongodb", "options": options}
    if schema:
        source["schema"] = schema
    doc = {
        "version": 1,
        "pipeline": {"id": "mongo_source_test", "mode": "batch"},
        "sources": [source],
        "transforms": [],
        "sinks": [sink or {"id": "out", "type": "console", "input": "orders_mongo"}],
    }
    if sql:
        doc["transforms"] = [{"id": "shaped", "type": "sql", "inputs": ["orders_mongo"], "options": {"sql": sql}}]
        doc["sinks"][0]["input"] = "shaped"
    return doc


def ctx(spark) -> RunContext:
    return RunContext(spark=spark, registry=default_registry(), pipeline_id="mongo_source_test", env="dev")


# --- no Spark needed ---------------------------------------------------------------------


def test_registered_with_its_contract_met():
    assert default_registry().get("source", "mongodb") is MongoDbSource
    assert static_contract_issues(MongoDbSource) == []
    assert MongoDbSource.capabilities() == ["batch"]
    assert MongoDbSource.accepts_connection("mongodb")


def test_connection_fields_are_required_unless_dummy_data_is_used():
    ok = MongoDbSourceOptions(**real(database=" shop ", pipeline='[{"$match": {"status": "active"}}]'))
    assert ok.database == "shop" and ok.reader_options()["aggregation.pipeline"] == '[{"$match": {"status": "active"}}]'
    assert MongoDbSourceOptions(**dummy()).connection_uri is None
    for bad, message in [
        ({}, r"connection_uri\n  Value error, required unless use_dummy_data is on"),
        ({"connection_uri": URI}, r"database\n  Value error, required unless use_dummy_data is on"),
        ({"connection_uri": URI, "database": "shop"}, r"collection\n  Value error, required unless use_dummy_data is on"),
        (real(connection_uri="localhost"), "must start with mongodb://"),
        (real(database="a.b"), r"database\n  Value error"),
        (real(collection="system.x"), "start with system."),
        (real(pipeline='{"$match": {}}'), "must be a JSON array of stages"),
        (real(pipeline="[nope"), "not valid JSON"),
        (real(read_options={"collection": "other"}), "set through the source's own fields: collection"),
        (real(sample_size=0), "sample_size"),
        (real(dummy_data=DOCS), "only applies when use_dummy_data is on"),
        (real(path="/x"), "extra"),
    ]:
        with pytest.raises(ValidationError, match=message):
            MongoDbSourceOptions(**bad)


def test_dummy_data_takes_json_text_or_a_list_of_up_to_100_documents():
    assert MongoDbSourceOptions(**dummy(json.dumps(DOCS))).records == DOCS
    assert MongoDbSourceOptions(**dummy(DOCS)).records == DOCS
    assert MongoDbSourceOptions(**dummy([{"n": i} for i in range(MAX_DUMMY_RECORDS)])).records[-1] == {"n": 99}
    for records, message in [
        (None, "required when use_dummy_data is on"),
        ("", "required when use_dummy_data is on"),
        ("[]", "required when use_dummy_data is on"),
        ("{nope", "not valid JSON"),
        ('{"a": 1}', "must be a JSON array of documents"),
        ("[1, 2]", "every element must be a JSON object"),
        ([{"n": i} for i in range(MAX_DUMMY_RECORDS + 1)], "at most 100 documents"),
    ]:
        with pytest.raises(ValidationError, match=rf"dummy_data\n  Value error, {message}"):
            MongoDbSourceOptions(**dummy(records))


def test_form_shows_dummy_data_or_connection_fields_depending_on_the_flag():
    props = MongoDbSourceOptions.model_json_schema()["properties"]
    assert props["dummy_data"]["x-when"] == {"field": "use_dummy_data", "in": [True], "required_in": [True]}
    assert props["dummy_data"]["type"] == "string" and props["dummy_data"]["format"] == "json"
    for field in ("connection_uri", "database", "collection"):
        assert props[field]["x-when"] == {"field": "use_dummy_data", "in": [False], "required_in": [False]}
    for field in ("username", "password", "pipeline", "sample_size", "read_options"):
        assert props[field]["x-when"] == {"field": "use_dummy_data", "in": [False], "required_in": []}


def test_issues_are_reported_on_their_field():
    with pytest.raises(SpecError) as exc:
        compile_pipeline(source_pipeline(dummy("[1]")))
    assert [(i.path, i.message) for i in exc.value.issues] == [
        ("sources[0].options.dummy_data", "every element must be a JSON object (document); element 0 is not")
    ]


def test_dummy_data_needs_no_connector_and_describes_itself():
    source = MongoDbSource()
    assert source.required_packages(MongoDbSourceOptions(**dummy())) == []
    assert required_packages(compile_pipeline(source_pipeline(dummy()))) == []
    assert required_packages(compile_pipeline(source_pipeline(real()))) == [f"org.mongodb.spark:mongo-spark-connector_2.12:{MongoDbSource.spark_packages[0].split(':')[-1]}"]
    assert source.describe(MongoDbSourceOptions(**dummy())) == "mongodb (dummy data: 3 documents)"
    assert source.describe(MongoDbSourceOptions(**real(username="u", password="p"))) == "mongodb <- shop.orders at mongodb://localhost:27017"
    assert source.estimate_input_bytes(MongoDbSourceOptions(**dummy())) == len(json.dumps(DOCS))
    assert source.estimate_input_bytes(MongoDbSourceOptions(**real())) is None


# --- dummy data (Spark, no server) ---------------------------------------------------------


def test_dummy_documents_flow_downstream_like_a_collection(spark):
    assert_source_contract(spark, MongoDbSource(), MongoDbSourceOptions(**dummy()), limit=2)
    doc = source_pipeline(
        dummy(),
        sql="SELECT upper(country) AS country, customer.tier AS tier, round(sum(amount), 2) AS revenue, count(*) AS n "
        "FROM orders_mongo GROUP BY country, customer.tier",
    )
    rows = preview(doc, "shaped", spark=spark)
    assert {(r["country"], r["tier"]): (r["revenue"], r["n"]) for r in rows.rows} == {("IN", "gold"): (25.5, 1), ("IN", None): (7.0, 1), ("US", "silver"): (99.99, 1)}
    assert run(doc, spark=spark).sinks_written == ["out"]

    # Analysis infers the columns from the records, so downstream SQL is checked while drawing.
    analysis = analyze(source_pipeline(dummy(), sql="SELECT nope FROM orders_mongo"), spark=spark)
    assert analysis["orders_mongo"].ok and [f["name"] for f in analysis["orders_mongo"].schema["fields"]] == ["_id", "amount", "country", "customer", "order_id", "tags"]
    assert not analysis["shaped"].ok and "nope" in analysis["shaped"].error


def test_a_declared_schema_types_the_dummy_data(spark):
    records = [{"order_id": 1, "order_ts": "2026-10-01T10:00:00Z", "amount": 10}, {"order_id": 2, "order_ts": "2026-10-02T11:30:00Z", "amount": 7.5}]
    schema = {"kind": "ddl", "ddl": "order_id INT, order_ts TIMESTAMP, amount DECIMAL(10,2)"}
    rows = preview(source_pipeline(dummy(records), schema=schema), "orders_mongo", spark=spark)
    assert [f["name"] for f in rows.schema["fields"]] == ["order_id", "order_ts", "amount"]
    assert {f["name"]: f["type"] for f in rows.schema["fields"]} == {"order_id": "integer", "order_ts": "timestamp", "amount": "decimal(10,2)"}
    assert [r["amount"] for r in rows.rows] == ["10.00", "7.50"]
    # Without a schema, Spark's JSON inference decides.
    inferred = preview(source_pipeline(dummy(records)), "orders_mongo", spark=spark)
    assert {f["name"]: f["type"] for f in inferred.schema["fields"]} == {"order_id": "long", "order_ts": "string", "amount": "double"}


def test_unserialisable_dummy_records_fail_clearly(spark):
    opts = MongoDbSourceOptions(**dummy([{"x": 1}]))
    opts.dummy_data = [{"x": object()}]
    with pytest.raises(TributaryError, match="not JSON-serialisable"):
        MongoDbSource().read(ctx(spark), opts, None)


# --- a real collection (a MongoDB server) --------------------------------------------------


@pytest.fixture
def collection(mongodb_spark) -> str:
    """A collection of its own per test, filled through the sink."""
    from tributary.plugins.sinks.mongodb import MongoDbSink, MongoDbSinkOptions

    name = f"src_{uuid.uuid4().hex[:12]}"
    df = mongodb_spark.createDataFrame([(1, 25.5, "IN"), (2, 99.99, "US"), (3, 7.0, "IN")], "order_id INT, amount DOUBLE, country STRING")
    MongoDbSink().write(ctx(mongodb_spark), df, MongoDbSinkOptions(connection_uri=MONGODB_URI, database="tributary_tests", collection=name))
    return name


def test_reads_a_collection(mongodb_spark, collection):
    options = {
        "connection_uri": MONGODB_URI,
        "database": "tributary_tests",
        "collection": collection,
        # One partition: the read never asks the server for collection statistics.
        "read_options": {"partitioner": "com.mongodb.spark.sql.connector.read.partitioner.SinglePartitionPartitioner"},
    }
    schema = {"kind": "ddl", "ddl": "order_id INT, amount DOUBLE, country STRING"}
    assert_source_contract(mongodb_spark, MongoDbSource(), MongoDbSourceOptions(**options), limit=2)
    rows = preview(source_pipeline(options, schema=schema), "orders_mongo", spark=mongodb_spark)
    assert sorted((r["order_id"], r["amount"], r["country"]) for r in rows.rows) == [(1, 25.5, "IN"), (2, 99.99, "US"), (3, 7.0, "IN")]

    filtered = source_pipeline({**options, "pipeline": '[{"$match": {"country": "IN"}}]'}, schema=schema, sql="SELECT sum(amount) AS total FROM orders_mongo")
    assert preview(filtered, "shaped", spark=mongodb_spark).rows == [{"total": 32.5}]
    assert run(filtered, spark=mongodb_spark).sinks_written == ["out"]
