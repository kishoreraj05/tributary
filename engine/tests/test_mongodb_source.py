"""The MongoDB source: options, and reads of a real collection (``mongodb_spark`` in conftest)."""

from __future__ import annotations

import uuid

import pytest
from pydantic import ValidationError

from conftest import MONGODB_URI
from tributary import SpecError, compile_pipeline, preview, run
from tributary.core import RunContext
from tributary.plugins.sources.mongodb import MongoDbSource, MongoDbSourceOptions
from tributary.registry import default_registry
from tributary.runtime import required_packages
from tributary.testing import assert_source_contract, static_contract_issues

URI = "mongodb://localhost:27017"


def real(**overrides) -> dict:
    return {"connection_uri": URI, "database": "shop", "collection": "orders", **overrides}


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


def test_connection_fields_are_required():
    ok = MongoDbSourceOptions(**real(database=" shop ", pipeline='[{"$match": {"status": "active"}}]'))
    assert ok.database == "shop" and ok.reader_options()["aggregation.pipeline"] == '[{"$match": {"status": "active"}}]'
    assert {"connection_uri", "database", "collection"} <= set(MongoDbSourceOptions.model_json_schema()["required"])
    for bad, message in [
        ({}, r"connection_uri\n  Field required"),
        ({"connection_uri": URI}, r"database\n  Field required"),
        ({"connection_uri": URI, "database": "shop"}, r"collection\n  Field required"),
        (real(connection_uri="localhost"), "must start with mongodb://"),
        (real(database="a.b"), r"database\n  Value error"),
        (real(collection="system.x"), "start with system."),
        (real(pipeline='{"$match": {}}'), "must be a JSON array of stages"),
        (real(pipeline="[nope"), "not valid JSON"),
        (real(read_options={"collection": "other"}), "set through the source's own fields: collection"),
        (real(sample_size=0), "sample_size"),
        (real(use_dummy_data=True), "use_dummy_data\n  Extra inputs are not permitted"),  # the node's dummy settings replaced it
        (real(path="/x"), "extra"),
    ]:
        with pytest.raises(ValidationError, match=message):
            MongoDbSourceOptions(**bad)


def test_issues_are_reported_on_their_field():
    with pytest.raises(SpecError) as exc:
        compile_pipeline(source_pipeline(real(pipeline='{"$match": {}}')))
    assert [(i.path, i.message) for i in exc.value.issues] == [("sources[0].options.pipeline", "must be a JSON array of stages, e.g. [{\"$match\": {...}}]")]


def test_needs_the_connector_and_describes_itself():
    source = MongoDbSource()
    assert required_packages(compile_pipeline(source_pipeline(real()))) == [f"org.mongodb.spark:mongo-spark-connector_2.12:{MongoDbSource.spark_packages[0].split(':')[-1]}"]
    assert source.describe(MongoDbSourceOptions(**real(username="u", password="p"))) == "mongodb <- shop.orders at mongodb://localhost:27017"
    assert source.estimate_input_bytes(MongoDbSourceOptions(**real())) is None


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
