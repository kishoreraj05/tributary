from __future__ import annotations

from conftest import spec


def test_validate_tags_issues_with_nodes(client, orders_csv, tmp_path):
    doc = spec(orders_csv, tmp_path / "out")
    doc["sinks"][0]["input"] = "nope"
    body = client.post("/api/v1/validate", json={"spec": doc}).json()
    assert body["valid"] is False
    assert body["issues"][0]["node_id"] == "orders_out"


def test_deep_validate_returns_schemas_and_column_errors(client, orders_csv, tmp_path):
    doc = spec(orders_csv, tmp_path / "out")
    body = client.post("/api/v1/validate", json={"spec": doc, "deep": True}).json()
    assert body["valid"] is True
    fields = [f["name"] for f in body["nodes"]["orders_clean"]["schema"]["fields"]]
    assert fields == ["order_id", "amount", "country"]

    doc["transforms"][0]["options"]["sql"] = "SELECT amout FROM orders"
    body = client.post("/api/v1/validate", json={"spec": doc, "deep": True}).json()
    assert body["valid"] is False and "amout" in body["nodes"]["orders_clean"]["error"]


def test_plan(client, orders_csv, tmp_path):
    body = client.post("/api/v1/plan", json={"spec": spec(orders_csv, tmp_path / "out")}).json()
    assert [s["id"] for s in body["steps"]] == ["orders_src", "orders_clean", "dq", "orders_out"]
    assert body["resources"]["profile"] == "S"


def test_preview_with_quality_and_samples(client, orders_csv, tmp_path):
    doc = spec(orders_csv, tmp_path / "out")
    body = client.post("/api/v1/preview", json={"spec": doc, "node_id": "orders_clean", "limit": 2})
    assert body.status_code == 200, body.text
    result = body.json()
    assert len(result["rows"]) == 2
    assert {q["check_id"] for q in result["quality"]} == {"not_null_1", "range_2"}

    sample = client.post("/api/v1/samples", files={"file": ("s.csv", b"order_id,amount,country\n9,1.5,fr\n")})
    assert sample.status_code == 201
    sample_id = sample.json()["sample_id"]
    result = client.post("/api/v1/preview", json={
        "spec": doc, "node_id": "orders_out", "sample_data": {"orders_src": sample_id},
    }).json()
    assert result["rows"] == [{"order_id": 9, "amount": 1.5, "country": "FR"}]
    assert result["written"] is False and not (tmp_path / "out").exists()


def test_preview_spark_error_is_a_400(client, orders_csv, tmp_path):
    doc = spec(orders_csv, tmp_path / "out")
    doc["transforms"][0]["options"]["sql"] = "SELECT amout FROM orders"
    response = client.post("/api/v1/preview", json={"spec": doc, "node_id": "orders_clean"})
    assert response.status_code == 400 and "amout" in response.json()["detail"]["message"]


def test_preview_uses_stored_connection(client, orders_csv, tmp_path):
    client.post("/api/v1/connections", json={
        "name": "lake", "type": "file", "secret_fields": ["path"],
        "environments": {"dev": {"path": str(orders_csv), "format": "csv"}},
    })
    doc = spec(orders_csv, tmp_path / "out")
    doc["sources"][0]["connection"] = "lake"
    doc["sources"][0]["options"] = {"read_options": {"header": True}}
    result = client.post("/api/v1/preview", json={"spec": doc, "env": "dev", "node_id": "orders_src"}).json()
    assert len(result["rows"]) == 4

    missing = client.post("/api/v1/validate", json={"spec": {**doc, "environments": {"prod": {}}}, "env": "prod"}).json()
    assert "no settings for environment 'prod'" in missing["issues"][0]["message"]


def test_preview_survives_a_dead_spark_jvm(client, orders_csv, tmp_path):
    """If the shared preview JVM dies, the next preview starts a fresh one instead of
    failing forever with 'An error occurred while calling ...'."""
    from pyspark import SparkContext

    from tributary_backend.services.spark import get_spark

    old = get_spark()
    proc = SparkContext._gateway.proc
    proc.kill()
    proc.wait()
    doc = spec(orders_csv, tmp_path / "out")
    body = client.post("/api/v1/preview", json={"spec": doc, "node_id": "orders_src", "limit": 2})
    assert body.status_code == 200, body.text
    assert len(body.json()["rows"]) == 2
    assert get_spark() is not old


def test_preview_is_not_blocked_by_an_unfinished_downstream_node(client, orders_csv, tmp_path):
    """file source -> deequ node with no parameters yet: testing the source still works; testing
    the deequ node reports its own problem."""
    doc = spec(orders_csv, tmp_path / "out", quality=[])
    doc["transforms"] = [{"id": "orders_dq", "type": "deequ", "inputs": ["orders_src"], "options": {}}]
    doc["sinks"] = [{"id": "out", "type": "console", "input": "orders_dq"}]
    assert client.post("/api/v1/validate", json={"spec": doc}).json()["valid"] is False  # the pipeline as a whole is not

    response = client.post("/api/v1/preview", json={"spec": doc, "node_id": "orders_src", "limit": 10})
    assert response.status_code == 200, response.text
    assert len(response.json()["rows"]) == 4

    response = client.post("/api/v1/preview", json={"spec": doc, "node_id": "orders_dq"})
    assert response.status_code == 422
    [issue] = response.json()["detail"]["issues"]
    assert issue["node_id"] == "orders_dq" and issue["path"] == "transforms[0].options.runner"
