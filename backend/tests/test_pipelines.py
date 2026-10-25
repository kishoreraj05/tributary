from __future__ import annotations

from conftest import create_pipeline, spec


def test_drafts_are_not_validated_but_versions_are(client, orders_csv, tmp_path):
    doc = spec(orders_csv, tmp_path / "out")
    doc["transforms"][0]["options"]["sql"] = "SELECT * FROM order"
    pipeline = create_pipeline(client, doc)
    assert pipeline["latest_version"] == 0

    rejected = client.post(f"/api/v1/pipelines/{pipeline['id']}/versions", json={})
    assert rejected.status_code == 422
    [issue] = rejected.json()["detail"]["issues"]
    assert issue["node_id"] == "orders_clean" and "did you mean 'orders'" in issue["message"]

    doc["transforms"][0]["options"]["sql"] = "SELECT * FROM orders"
    assert client.put(f"/api/v1/pipelines/{pipeline['id']}", json={"spec": doc}).status_code == 200
    v1 = client.post(f"/api/v1/pipelines/{pipeline['id']}/versions", json={"message": "first"})
    assert v1.status_code == 201 and v1.json()["version"] == 1
    assert client.get(f"/api/v1/pipelines/{pipeline['id']}").json()["latest_version"] == 1


def test_versions_are_immutable_and_restorable(client, orders_csv, tmp_path):
    doc = spec(orders_csv, tmp_path / "out")
    pid = create_pipeline(client, doc)["id"]
    client.post(f"/api/v1/pipelines/{pid}/versions", json={"message": "v1"})
    doc["sinks"][0]["options"]["mode"] = "append"
    client.put(f"/api/v1/pipelines/{pid}", json={"spec": doc, "ui_layout": {"orders_src": {"x": 10, "y": 20}}})
    client.post(f"/api/v1/pipelines/{pid}/versions", json={"message": "v2"})

    assert [v["version"] for v in client.get(f"/api/v1/pipelines/{pid}/versions").json()] == [1, 2]
    v1 = client.get(f"/api/v1/pipelines/{pid}/versions/1").json()
    assert v1["spec"]["sinks"][0]["options"]["mode"] == "overwrite" and v1["ui_layout"] == {}
    restored = client.post(f"/api/v1/pipelines/{pid}/versions/1/restore").json()
    assert restored["draft_spec"]["sinks"][0]["options"]["mode"] == "overwrite"


def test_export_import_round_trip(client, orders_csv, tmp_path):
    pid = create_pipeline(client, spec(orders_csv, tmp_path / "out"))["id"]
    exported = client.get(f"/api/v1/pipelines/{pid}/export")
    assert exported.status_code == 200 and "orders_clean" in exported.text
    imported = client.post("/api/v1/pipelines/import", json={"name": "copy", "content": exported.text})
    assert imported.status_code == 201
    assert imported.json()["draft_spec"] == client.get(f"/api/v1/pipelines/{pid}").json()["draft_spec"]


def test_name_conflict_and_delete(client, orders_csv, tmp_path):
    pid = create_pipeline(client, spec(orders_csv, tmp_path / "out"))["id"]
    assert client.post("/api/v1/pipelines", json={"name": "orders"}).status_code == 409
    assert client.delete(f"/api/v1/pipelines/{pid}").status_code == 204
    assert client.get(f"/api/v1/pipelines/{pid}").status_code == 404


def test_run_requires_a_version(client, orders_csv, tmp_path):
    pid = create_pipeline(client, spec(orders_csv, tmp_path / "out"))["id"]
    assert client.post(f"/api/v1/pipelines/{pid}/runs", json={"env": "dev"}).status_code == 409


def test_components_catalog(client):
    body = client.get("/api/v1/components").json()
    assert {"source", "transform", "sink", "check", "metrics_sink"} <= set(body["components"])
    assert "tributary_backend" not in [p["type"] for p in body["components"]["metrics_sink"]]
