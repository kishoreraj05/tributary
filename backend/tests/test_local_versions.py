"""Saving versions of pipelines that use dummy data in the local environment.

A version is validated for every environment, but dummy nodes stand in for systems that
may not be set up yet: their missing settings outside local come back as warnings, not a
refusal, so the pipeline can still be saved and run locally. Anything else still blocks.
"""

from __future__ import annotations

from conftest import create_pipeline, wait_for

RECORDS = '{"order_id": 1, "amount": 25.5}\n{"order_id": 2, "amount": 9.0}'


def dummy_spec() -> dict:
    """What the editor makes of a Delta source and a console sink, both dummy, with none
    of their real settings filled in (the pipeline that could not be versioned)."""
    return {
        "version": 1,
        "pipeline": {"id": "dummy_pipeline", "mode": "batch"},
        "environments": {"local": {"vars": {}}, "dev": {"vars": {}}},
        "sources": [{"id": "delta_source_1", "type": "delta", "options": {}, "dummy": {"enabled": True, "data": RECORDS}}],
        "transforms": [],
        "sinks": [{"id": "console_sink_1", "type": "console", "input": "delta_source_1", "options": {}, "dummy": {"enabled": True}}],
    }


def test_dummy_nodes_missing_real_settings_elsewhere_do_not_block_a_version(client):
    pipeline = create_pipeline(client, dummy_spec(), name="dummy")
    response = client.post(f"/api/v1/pipelines/{pipeline['id']}/versions", json={"message": "local only for now"})
    assert response.status_code == 201, response.text
    assert response.json()["version"] == 1
    assert response.json()["warnings"] == ["dev: sources[0].options.path: Field required"]


def test_other_problems_still_block_and_name_their_environment(client):
    doc = dummy_spec()
    doc["sinks"][0]["input"] = "nowhere"  # a real mistake, dummy or not
    pipeline = create_pipeline(client, doc, name="broken")
    response = client.post(f"/api/v1/pipelines/{pipeline['id']}/versions", json={"message": "x"})
    assert response.status_code == 422
    body = response.json()["detail"]
    assert body["env"] in ("local", "dev")  # the first environment checked (jsonb orders the keys)
    assert [i["path"] for i in body["issues"]] == ["sinks[0].input"]


def test_a_valid_version_has_no_warnings(client, orders_csv, tmp_path):
    from conftest import spec

    pipeline = create_pipeline(client, spec(orders_csv, tmp_path / "out"), name="plain")
    response = client.post(f"/api/v1/pipelines/{pipeline['id']}/versions", json={"message": "x"})
    assert response.status_code == 201 and response.json()["warnings"] == []


def test_a_dummy_node_needs_no_local_settings_on_its_stored_connection(client):
    """In local, a dummy node's connection is not looked up, so a stored connection that
    only has settings for dev does not get in the way of previews."""
    created = client.post(
        "/api/v1/connections",
        json={"name": "lake", "type": "delta", "environments": {"dev": {"path": "/lake/orders"}}},
    )
    assert created.status_code == 201, created.text
    doc = dummy_spec()
    doc["sources"][0]["connection"] = "lake"
    preview = client.post("/api/v1/preview", json={"spec": doc, "env": "local", "node_id": "console_sink_1"})
    assert preview.status_code == 200, preview.text
    assert sorted(r["order_id"] for r in preview.json()["rows"]) == [1, 2]
    # In dev the same node runs for real and takes its path from the connection.
    validated = client.post("/api/v1/validate", json={"spec": doc, "env": "dev"}).json()
    assert validated["valid"] is True, validated["issues"]


def test_the_saved_version_runs_locally_on_its_dummy_data(client):
    pipeline = create_pipeline(client, dummy_spec(), name="runs")
    assert client.post(f"/api/v1/pipelines/{pipeline['id']}/versions", json={"message": "v1"}).status_code == 201
    started = client.post(f"/api/v1/pipelines/{pipeline['id']}/runs", json={"env": "local", "version": 1})
    assert started.status_code in (200, 201, 202), started.text
    run = wait_for(client, started.json()["id"], ("succeeded", "failed"))
    assert run["status"] == "succeeded", run.get("error")
