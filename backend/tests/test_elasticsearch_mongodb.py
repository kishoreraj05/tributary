"""Elasticsearch in, MongoDB out, end to end through the API: both servers are stored
connections (the MongoDB password encrypted), the editor's deep validation knows the
columns, previews are dry runs on the shared session, and a run writes from its own worker.

Needs an Elasticsearch at TRIBUTARY_TEST_ES_NODES and a MongoDB at TRIBUTARY_TEST_MONGO_URI
(the docker-compose ones by default); the tests that talk to them skip otherwise.
"""

from __future__ import annotations

import json
import os
import socket
import urllib.request
import uuid

import pytest
from conftest import create_pipeline, spec, wait_for

from tributary.plugins._mongodb import CONNECTOR_VERSION as MONGO_SPARK_VERSION
from tributary.plugins.sources.elasticsearch import ES_HADOOP_VERSION

from tributary_backend.services.spark import get_spark, preview_conf, preview_packages
from tributary_backend.settings import get_settings

ES_NODES = os.environ.get("TRIBUTARY_TEST_ES_NODES", "localhost:9200")
MONGO_URI = os.environ.get("TRIBUTARY_TEST_MONGO_URI", "mongodb://localhost:27017")
DATABASE = "tributary_backend_test"
ORDERS = [
    {"order_id": 1, "amount": 10.0, "country": "in"},
    {"order_id": 2, "amount": -1.0, "country": "us"},
    {"amount": 5.0, "country": "de"},
    {"order_id": 4, "amount": 7.5, "country": "gb"},
]


def _listening(address: str, default_port: int) -> bool:
    address = address.split("://", 1)[-1].split("/", 1)[0].split("@")[-1].split(",")[0]
    host, _, port = address.partition(":")
    try:
        with socket.create_connection((host, int(port or default_port)), timeout=1):
            return True
    except OSError:
        return False


def _es(method: str, path: str, body=None) -> dict:
    base = ES_NODES if "://" in ES_NODES else f"http://{ES_NODES}"
    data = None if body is None else (body if isinstance(body, bytes) else json.dumps(body).encode())
    content_type = "application/x-ndjson" if isinstance(body, bytes) else "application/json"
    request = urllib.request.Request(f"{base}{path}", data=data, method=method, headers={"Content-Type": content_type})
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


@pytest.fixture
def services():
    if not _listening(ES_NODES, 9200):
        pytest.skip(f"no Elasticsearch at {ES_NODES}")
    if not _listening(MONGO_URI, 27017):
        pytest.skip(f"no MongoDB at {MONGO_URI}")


@pytest.fixture
def orders_index(services):
    name = f"tributary_backend_{uuid.uuid4().hex[:8]}"
    _es("PUT", f"/{name}", {"mappings": {"properties": {"order_id": {"type": "integer"}, "amount": {"type": "double"}, "country": {"type": "keyword"}}}})
    lines = []
    for doc in ORDERS:
        lines += [json.dumps({"index": {"_index": name}}), json.dumps(doc)]
    assert not _es("POST", "/_bulk?refresh=true", ("\n".join(lines) + "\n").encode())["errors"]
    yield name
    _es("DELETE", f"/{name}")


def _mongo_reader(collection: str):
    return get_spark().read.format("mongodb").option("connection.uri", MONGO_URI).option("database", DATABASE).option("collection", collection)


@pytest.fixture
def collection(services):
    name = f"orders_{uuid.uuid4().hex[:8]}"
    yield name
    get_spark().createDataFrame([], "x INT").write.format("mongodb").mode("overwrite").option("connection.uri", MONGO_URI).option(
        "database", DATABASE
    ).option("collection", name).save()


def _es_to_mongo_spec(index: str, collection: str, tmp_path) -> dict:
    doc = spec(tmp_path / "unused.csv", tmp_path / "out")
    doc["sources"][0] = {"id": "orders_src", "type": "elasticsearch", "connection": "search", "options": {"index": index, "include_metadata": True}}
    doc["transforms"][0]["options"]["sql"] = "SELECT _metadata['_id'] AS doc_id, order_id, amount, upper(country) AS country FROM orders"
    doc["sinks"][0] = {
        "id": "orders_out", "type": "mongodb", "input": "orders_clean", "connection": "docs",
        "options": {"database": DATABASE, "collection": collection, "mode": "upsert", "merge_keys": ["doc_id"]},
    }
    return doc


def test_preview_session_has_both_connectors_without_their_spark_33_baggage():
    packages = preview_packages(get_settings())
    assert any(p.endswith(f"elasticsearch-spark-30_2.12:{ES_HADOOP_VERSION}") for p in packages)
    assert any(p.endswith(f"mongo-spark-connector_2.12:{MONGO_SPARK_VERSION}") for p in packages)
    assert "org.apache.spark:spark-yarn_2.12" in preview_conf()["spark.jars.excludes"]
    assert "spark.sql.extensions" not in preview_conf()


def test_catalog_lists_both_with_their_forms(client):
    components = client.get("/api/v1/components").json()["components"]
    source = next(p for p in components["source"] if p["type"] == "elasticsearch")
    assert source["capabilities"] == ["batch"] and source["icon"] == "search"
    props = source["options_schema"]["properties"]  # nodes/index are mandatory unless use_dummy_data is on
    assert "required" not in source["options_schema"]
    assert props["nodes"]["x-when"] == props["index"]["x-when"] == {"field": "use_dummy_data", "required_in": [False]}
    assert props["dummy_data"]["x-when"] == {"field": "use_dummy_data", "in": [True], "required_in": [True]}
    sink = next(p for p in components["sink"] if p["type"] == "mongodb")
    assert sink["options_schema"]["required"] == ["connection_uri", "database", "collection"]
    assert sink["options_schema"]["properties"]["merge_keys"]["x-when"] == {"field": "mode", "in": ["upsert"], "required_in": ["upsert"]}


def test_elasticsearch_to_mongodb_from_connections_preview_and_run(client, orders_index, collection, tmp_path):
    assert client.post("/api/v1/connections", json={
        "name": "search", "type": "elasticsearch", "environments": {"dev": {"nodes": ES_NODES}},
    }).status_code == 201
    created = client.post("/api/v1/connections", json={
        "name": "docs", "type": "mongodb", "environments": {"dev": {"connection_uri": MONGO_URI, "password": ""}},
    })
    assert created.status_code == 201, created.text
    assert created.json()["secret_fields"] == ["password"]  # stored encrypted, never returned
    doc = _es_to_mongo_spec(orders_index, collection, tmp_path)

    # Deep validation: the source's columns come from the index mapping, the sink's key is checked.
    validated = client.post("/api/v1/validate", json={"spec": doc, "env": "dev", "deep": True}).json()
    assert validated["valid"] is True, validated
    assert [f["name"] for f in validated["nodes"]["orders_src"]["schema"]["fields"]] == ["amount", "country", "order_id", "_metadata"]
    assert [f["name"] for f in validated["nodes"]["orders_clean"]["schema"]["fields"]] == ["doc_id", "order_id", "amount", "country"]
    wrong = json.loads(json.dumps(doc))
    wrong["sinks"][0]["options"]["merge_keys"] = ["sku"]
    issues = client.post("/api/v1/validate", json={"spec": wrong, "env": "dev", "deep": True}).json()["issues"]
    assert [(i["path"], i["node_id"]) for i in issues] == [("sinks[0].options.merge_keys", "orders_out")]

    # Preview the sink: the rows the 'drop' gate lets through, and nothing written.
    preview = client.post("/api/v1/preview", json={"spec": doc, "env": "dev", "node_id": "orders_out", "limit": 10})
    assert preview.status_code == 200, preview.text
    result = preview.json()
    assert sorted(r["order_id"] for r in result["rows"]) == [1, 4] and result["written"] is False
    assert result["target"] == f"mongodb -> {DATABASE}.{collection} (upsert on doc_id) at {MONGO_URI}"
    assert _mongo_reader(collection).load().count() == 0

    # Run twice from a worker: the upsert keeps the collection at the 2 good orders.
    pid = create_pipeline(client, doc)["id"]
    assert client.post(f"/api/v1/pipelines/{pid}/versions", json={}).status_code == 201
    for _ in range(2):
        run = client.post(f"/api/v1/pipelines/{pid}/runs", json={"env": "dev"}).json()
        done = wait_for(client, run["id"], ("succeeded", "failed"))
        assert done["status"] == "succeeded", done
        assert done["summary"]["sinks_written"] == ["orders_out"]
    rows = {r.order_id: (r.amount, r.country) for r in _mongo_reader(collection).load().collect()}
    assert rows == {1: (10.0, "IN"), 4: (7.5, "GB")}
    quality = client.get(f"/api/v1/runs/{run['id']}/quality").json()
    assert {q["check_id"]: q["failed_rows"] for q in quality} == {"not_null_1": 1, "range_2": 1}


def test_dummy_data_previews_and_validates_without_any_cluster(client, tmp_path):
    """The editor's flow with use_dummy_data on: no connection, no nodes, no index; the
    documents typed into the form (JSON text) are what the node emits."""
    doc = spec(tmp_path / "unused.csv", tmp_path / "out")
    doc["sources"][0] = {
        "id": "orders_src", "type": "elasticsearch",
        "options": {
            "use_dummy_data": True, "include_metadata": True,
            "dummy_data": json.dumps([{"_id": "a", "order_id": 1, "amount": 2.5, "country": "in"}, {"order_id": 2, "amount": -1, "country": "us"}, {"amount": 3, "country": "de"}]),
        },
        "schema": {"kind": "ddl", "ddl": "order_id INT, amount DOUBLE, country STRING"},
    }
    validated = client.post("/api/v1/validate", json={"spec": doc, "env": "dev", "deep": True}).json()
    assert validated["valid"] is True, validated
    assert [f["name"] for f in validated["nodes"]["orders_src"]["schema"]["fields"]] == ["order_id", "amount", "country", "_metadata"]

    preview = client.post("/api/v1/preview", json={"spec": doc, "env": "dev", "node_id": "orders_out", "limit": 10}).json()
    assert [(r["order_id"], r["country"]) for r in preview["rows"]] == [(1, "IN")]  # the 'drop' gate removed the other two

    # Problems land on the field: a malformed array, and the switch on without data.
    doc["sources"][0]["options"]["dummy_data"] = "[1, 2]"
    issues = client.post("/api/v1/validate", json={"spec": doc, "env": "dev"}).json()["issues"]
    assert [(i["path"], i["node_id"]) for i in issues] == [("sources[0].options.dummy_data", "orders_src")]
    del doc["sources"][0]["options"]["dummy_data"]
    issues = client.post("/api/v1/validate", json={"spec": doc, "env": "dev"}).json()["issues"]
    assert [(i["path"], i["message"]) for i in issues] == [("sources[0].options.dummy_data", "required when use_dummy_data is on")]
