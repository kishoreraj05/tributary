"""The deequ node through the API: previews show its results, runs keep them in the DQ history."""

from __future__ import annotations

import copy

from conftest import create_pipeline, spec, wait_for
from tributary.registry import Registry

from tributary_backend.quality_sink import BackendMetricsSink
from tributary_backend.services.specs import referenced_connections
from tributary_backend.worker import metrics_target_fields

DEEQU = {
    "id": "orders_dq",
    "type": "deequ",
    "inputs": ["orders_src"],
    "options": {
        "runner": "verification",
        "output": "data",
        "fail_on": "never",
        "constraints": [
            {"function": "is_complete", "columns": ["order_id"]},
            {"function": "has_size", "assertion": ">= 1"},
        ],
    },
}


def deequ_spec(orders_csv, out) -> dict:
    doc = spec(orders_csv, out, quality=[])
    doc["transforms"] = [copy.deepcopy(DEEQU), {**doc["transforms"][0], "inputs": [{"node": "orders_dq", "as": "orders"}]}]
    return doc


def test_preview_shows_deequ_results(client, orders_csv, tmp_path):
    doc = deequ_spec(orders_csv, tmp_path / "out")
    # Previews verify the bounded sample they read: the limit must cover the rows of interest.
    response = client.post("/api/v1/preview", json={"spec": doc, "node_id": "orders_clean", "limit": 10})
    assert response.status_code == 200, response.text
    result = response.json()
    assert len(result["rows"]) == 4
    results = {q["check_id"]: q for q in result["quality"]}
    assert set(results) == {"is_complete(order_id)", "has_size"}
    assert results["is_complete(order_id)"]["passed"] is False and results["has_size"]["passed"] is True

    # deep validation names the constraint with a bad column, or a column of the wrong type
    doc["transforms"][0]["options"]["constraints"][0]["columns"] = ["order_idd"]
    body = client.post("/api/v1/validate", json={"spec": doc, "deep": True}).json()
    assert body["valid"] is False and "order_idd" in body["nodes"]["orders_dq"]["error"]
    doc["transforms"][0]["options"]["constraints"] = [{"function": "has_mean", "columns": ["country"], "assertion": "> 0"}]
    body = client.post("/api/v1/validate", json={"spec": doc, "deep": True}).json()
    assert "column 'country' is string, but this function needs a numeric column" in body["nodes"]["orders_dq"]["error"]


def test_run_keeps_deequ_results_in_the_quality_history(client, orders_csv, tmp_path):
    out = tmp_path / "out"
    pid = create_pipeline(client, deequ_spec(orders_csv, out), "orders_deequ")["id"]
    assert client.post(f"/api/v1/pipelines/{pid}/versions", json={}).status_code == 201
    run = client.post(f"/api/v1/pipelines/{pid}/runs", json={"env": "dev"}).json()
    done = wait_for(client, run["id"], ("succeeded", "failed"))
    assert done["status"] == "succeeded", done
    assert done["summary"]["failed_checks"] == ["is_complete(order_id)"]
    assert (out / "orders" / "_SUCCESS").exists()

    quality = {q["check_id"]: q for q in client.get(f"/api/v1/runs/{run['id']}/quality").json()}
    assert set(quality) == {"is_complete(order_id)", "has_size"}
    complete = quality["is_complete(order_id)"]
    assert (complete["quality_id"], complete["node_id"], complete["check_type"], complete["failed_rows"], complete["total_rows"]) == (
        "orders_dq", "orders_src", "deequ", 1, 4,
    )


def test_worker_adds_its_sink_to_deequ_nodes_and_connections_are_found():
    registry = Registry()
    registry.register(BackendMetricsSink)
    assert metrics_target_fields(registry, DEEQU) == ["results_sinks"]
    assert metrics_target_fields(registry, {"id": "t", "type": "sql", "inputs": ["a"], "options": {"sql": "SELECT 1"}}) == []
    assert metrics_target_fields(registry, {"id": "t", "type": "nope"}) == []

    node = {**DEEQU, "options": {**DEEQU["options"], "results_sinks": [{"type": "postgres", "connection": "metrics_db"}]}}
    doc = {"sources": [{"id": "s", "type": "file", "connection": "lake"}], "transforms": [node], "sinks": []}
    assert referenced_connections(doc) == ["lake", "metrics_db"]
