"""The REST API source and enrichment transform through the API the editor uses: they are
in the catalog, previews and deep validation work on dummy data without any network (the
source's from the local environment, the enrichment's own), and a stored rest_api connection
(with its secret) feeds the enrichment transform."""

from __future__ import annotations

from conftest import spec


def rest_spec(orders_csv, out) -> dict:
    doc = spec(orders_csv, out, quality=[], environments={"local": {"vars": {"out": str(out)}}})
    doc["sources"].append({
        "id": "students", "type": "rest_api",
        "options": {"endpoint": {"url": "https://api.university.edu/v1/students"}},
        "dummy": {"enabled": True, "data": '{"roll_no": 1, "name": "Asha"}\n{"roll_no": 2, "name": "Bo"}'},
    })
    doc["transforms"] = [{
        "id": "grades", "type": "rest_api_enrichment", "connection": "university",
        "inputs": [{"node": "students", "as": "student_enrollment_df"}],
        "options": {
            "endpoint": {"url": "/students/{roll_no}/grades"},
            "dynamicInput": {"inputDataFrameRef": "student_enrollment_df", "parameterMapping": {"pathParams": {"roll_no": "roll_no"}}},
            "parsing": {"responseType": "JSONObject", "recordExtractionPath": "$.data.semesters"},
            "concurrency": {"maxConcurrentCalls": 2},
            "useDummyData": True,
            "dummyData": [{"semester": 1, "gpa": 3.5}],
        },
    }]
    doc["sinks"][0]["input"] = "grades"
    return doc


def test_catalog_lists_both_rest_plugins(client):
    catalog = client.get("/api/v1/components").json()["components"]
    source = next(p for p in catalog["source"] if p["type"] == "rest_api")
    assert source["capabilities"] == ["batch"] and source["icon"] == "globe"
    assert set(source["options_schema"]["properties"]) >= {"endpoint", "parsing", "pagination"}
    assert not {"useDummyData", "dummyData"} & set(source["options_schema"]["properties"])  # the node's dummy settings instead
    transform = next(p for p in catalog["transform"] if p["type"] == "rest_api_enrichment")
    assert transform["connection_types"] == ["rest_api_enrichment", "rest_api"]
    dynamic = transform["options_schema"]["$defs"]["DynamicInput"]["properties"]
    assert {"inputStrategy", "inputDataFrameRef", "parameterMapping", "bulkRequestSupport", "bulkBatchSize"} <= set(dynamic)
    assert transform["options_schema"]["properties"]["dummyData"]["format"] == "json"


def test_preview_and_deep_validation_run_on_dummy_data(client, orders_csv, tmp_path):
    client.post("/api/v1/connections", json={
        "name": "university", "type": "rest_api",
        "environments": {"local": {"baseUrl": "https://api.university.edu/v1", "authType": "bearer", "token": "s3cret"}},
    })
    doc = rest_spec(orders_csv, tmp_path / "out")

    result = client.post("/api/v1/validate", json={"spec": doc, "env": "local", "deep": True}).json()
    assert result["valid"] is True, result["issues"]
    assert [f["name"] for f in result["nodes"]["grades"]["schema"]["fields"]] == ["name", "roll_no", "gpa", "semester"]  # JSON inference sorts record fields

    response = client.post("/api/v1/preview", json={"spec": doc, "env": "local", "node_id": "grades"})
    assert response.status_code == 200, response.text
    rows = response.json()["rows"]
    assert sorted((r["roll_no"], r["semester"], r["gpa"]) for r in rows) == [(1, 1, 3.5), (2, 1, 3.5)]

    doc["transforms"][0]["options"]["useDummyData"] = False
    doc["transforms"][0]["options"]["dummyData"] = []
    result = client.post("/api/v1/validate", json={"spec": doc, "env": "local", "deep": True}).json()
    assert result["valid"] is False
    assert "set parsing.recordSchema or give dummyData" in result["nodes"]["grades"]["error"]

    doc["transforms"][0]["connection"] = "nowhere"
    result = client.post("/api/v1/validate", json={"spec": doc, "env": "local"}).json()
    assert result["issues"][0]["node_id"] == "grades" and "unknown connection 'nowhere'" in result["issues"][0]["message"]
