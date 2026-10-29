"""An Elasticsearch sink end to end through the API: the catalog shows its form, the cluster
comes from a stored connection (password encrypted), deep validation reports on the
fields, previews are dry runs on the shared session, and a run writes from its own worker.

The write tests need a cluster at TRIBUTARY_TEST_ES_URL (docker-compose's, at
localhost:9200, by default) and skip without one.
"""

from __future__ import annotations

import json
import os
import urllib.request
import uuid

import pytest
from conftest import create_pipeline, spec, wait_for

from tributary.plugins.sinks.elasticsearch import ELASTICSEARCH_EXCLUDES, ELASTICSEARCH_VERSION, EXCLUDES_KEY

from tributary_backend.services.spark import preview_conf, preview_packages
from tributary_backend.settings import get_settings

ES_URL = os.environ.get("TRIBUTARY_TEST_ES_URL", "http://localhost:9200").rstrip("/")
NODES = ES_URL.split("://", 1)[-1]


def es(method: str, path: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(f"{ES_URL}{path}", data=data, method=method, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        return json.load(exc)


def docs(index: str) -> dict[str, dict]:
    es("POST", f"/{index}/_refresh")
    return {h["_id"]: h["_source"] for h in es("GET", f"/{index}/_search?size=1000").get("hits", {}).get("hits", [])}


@pytest.fixture
def index():
    try:
        assert "version" in es("GET", "/")
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Elasticsearch not reachable at {ES_URL}: {exc} (docker compose -f deploy/docker-compose.yml up -d elasticsearch)")
    name = f"tributary-backend-{uuid.uuid4().hex[:8]}"
    yield name
    es("DELETE", f"/{name}*")


def _es_spec(orders_csv, tmp_path, connection: str | None = "search", **options) -> dict:
    doc = spec(orders_csv, tmp_path / "out")
    sink = {"id": "orders_out", "type": "elasticsearch", "input": "orders_clean", "options": options}
    if connection:
        sink["connection"] = connection
    doc["sinks"][0] = sink
    return doc


def _store_connection(client, **extra) -> None:
    body = {
        "name": "search",
        "type": "elasticsearch",
        "environments": {"dev": {"nodes": NODES, "user": "elastic", "password": "not-checked-without-security", **extra}},
    }
    created = client.post("/api/v1/connections", json=body)
    assert created.status_code == 201, created.text
    assert created.json()["secret_fields"] == ["password"] and created.json()["environments"]["dev"]["password"] == "********"


def test_preview_session_has_the_connector_and_its_exclusions():
    assert f"org.elasticsearch:elasticsearch-spark-30_2.12:{ELASTICSEARCH_VERSION}" in preview_packages(get_settings())
    conf = preview_conf()
    assert "spark.sql.extensions" not in conf
    for exclusion in ELASTICSEARCH_EXCLUDES:
        assert exclusion in conf[EXCLUDES_KEY].split(",")


def test_catalog_lists_the_sink_with_its_form(client):
    sink = next(p for p in client.get("/api/v1/components").json()["components"]["sink"] if p["type"] == "elasticsearch")
    assert sink["display_name"] == "Elasticsearch" and sink["icon"] == "search"
    form = sink["options_schema"]
    assert form["required"] == ["nodes", "index"]
    assert form["properties"]["id_column"]["x-when"] == {"field": "mode", "required_in": ["upsert"]}
    assert form["properties"]["ssl_allow_self_signed"]["x-when"] == {"field": "ssl", "in": [True], "required_in": []}


def test_validate_reports_on_the_fields(client, orders_csv, tmp_path):
    body = client.post("/api/v1/validate", json={"spec": _es_spec(orders_csv, tmp_path, connection=None, index="Orders", mode="upsert")}).json()
    assert body["valid"] is False
    assert {(i["node_id"], i["path"], i["message"]) for i in body["issues"]} == {
        ("orders_out", "sinks[0].options.nodes", "Field required"),
        ("orders_out", "sinks[0].options.index", "index names must be lowercase"),
        ("orders_out", "sinks[0].options.id_column", "required for mode 'upsert'"),
    }
    # A connection of another type cannot feed the sink.
    client.post("/api/v1/connections", json={"name": "lake", "type": "delta", "environments": {"dev": {"path": "/lake"}}})
    body = client.post("/api/v1/validate", json={"spec": _es_spec(orders_csv, tmp_path, connection="lake", index="orders")}).json()
    assert [i["path"] for i in body["issues"]] == ["sinks[0].connection"]


def test_deep_validation_checks_columns_against_the_upstream_schema(client, orders_csv, tmp_path):
    _store_connection(client)
    doc = _es_spec(orders_csv, tmp_path, index="orders-{region}", id_column="id")
    body = client.post("/api/v1/validate", json={"spec": doc, "env": "dev", "deep": True}).json()
    assert body["valid"] is False
    assert {(i["path"], i["message"]) for i in body["issues"]} == {
        ("sinks[0].options.id_column", "not in the data: id (columns: order_id, amount, country)"),
        ("sinks[0].options.index", "refers to column(s) not in the data: region (columns: order_id, amount, country)"),
    }


def test_deep_validation_asks_the_cluster_about_the_target(client, orders_csv, tmp_path, index):
    """A conflicting mapping, a strict mapping, an alias without a write index: all shown
    under the sink in the editor before anything runs."""
    _store_connection(client)
    es("PUT", f"/{index}", {"mappings": {"dynamic": "strict", "properties": {"order_id": {"type": "long"}, "amount": {"type": "boolean"}}}})
    doc = _es_spec(orders_csv, tmp_path, index=index, id_column="order_id")
    body = client.post("/api/v1/validate", json={"spec": doc, "env": "dev", "deep": True}).json()
    assert body["valid"] is False
    assert [(i["node_id"], i["path"], i["message"]) for i in body["issues"]] == [
        ("orders_out", "sinks[0]", f"amount is double in the data but boolean in index {index}; cast it in the SQL upstream or "
                                   "reindex with another mapping (Elasticsearch never changes a field's type)"),
        ("orders_out", "sinks[0]", f"index {index} has no field(s) country and refuses new ones (dynamic: strict): add them to the "
                                   "mapping or drop them upstream"),
    ]
    es("PUT", f"/{index}-a", {"aliases": {f"{index}-alias": {}}})
    es("PUT", f"/{index}-b", {"aliases": {f"{index}-alias": {}}})
    doc = _es_spec(orders_csv, tmp_path, index=f"{index}-alias", id_column="order_id")
    body = client.post("/api/v1/validate", json={"spec": doc, "env": "dev", "deep": True}).json()
    assert [i["path"] for i in body["issues"]] == ["sinks[0].options.index"] and "none of them is its write index" in body["issues"][0]["message"]


def test_elasticsearch_sink_preview_and_run(client, orders_csv, tmp_path, index):
    _store_connection(client)
    doc = _es_spec(orders_csv, tmp_path, index=index, id_column="order_id")
    validated = client.post("/api/v1/validate", json={"spec": doc, "env": "dev", "deep": True}).json()
    assert validated["valid"] is True, validated

    # Preview: the rows the gate lets through come back, nothing is written.
    preview = client.post("/api/v1/preview", json={"spec": doc, "env": "dev", "node_id": "orders_out", "limit": 10})
    assert preview.status_code == 200, preview.text
    result = preview.json()
    assert [r["order_id"] for r in result["rows"]] == [1, 4] and result["written"] is False
    assert result["target"] == f"elasticsearch -> {index} at {NODES} (append)"
    assert es("GET", f"/{index}")["error"]["type"] == "index_not_found_exception"

    # Writing from the preview goes through the shared session, with the connection's secret revealed.
    written = client.post("/api/v1/preview", json={"spec": doc, "env": "dev", "node_id": "orders_out", "limit": 10, "write": True}).json()
    assert written["written"] is True
    assert docs(index) == {"1": {"order_id": 1, "amount": 10.0, "country": "IN"}, "4": {"order_id": 4, "amount": 7.5, "country": "GB"}}

    # Run twice from a worker: the documents are replaced, not duplicated.
    pid = create_pipeline(client, doc)["id"]
    assert client.post(f"/api/v1/pipelines/{pid}/versions", json={}).status_code == 201
    for _ in range(2):
        run = client.post(f"/api/v1/pipelines/{pid}/runs", json={"env": "dev"}).json()
        done = wait_for(client, run["id"], ("succeeded", "failed"))
        assert done["status"] == "succeeded", done
        assert done["summary"]["sinks_written"] == ["orders_out"]
    assert {k: v["country"] for k, v in docs(index).items()} == {"1": "IN", "4": "GB"}
    # The stored run snapshot carries the connection's settings but its password masked.
    from sqlalchemy import select

    from tributary_backend.db import session_factory
    from tributary_backend.models import Run

    with session_factory()() as session:
        snapshot = session.scalars(select(Run).where(Run.id == run["id"])).one().resolved_spec
    stored = snapshot["environments"]["dev"]["connections"]["search"]
    assert stored["nodes"] == NODES and stored["password"] == "********"
