"""A Delta sink end to end through the API: the catalog shows its form, previews are dry
runs on the shared session (which it configures on first write), and a run writes from
its own worker."""

from __future__ import annotations

from conftest import create_pipeline, spec, wait_for

from tributary.plugins.sinks.delta import DELTA_CATALOG, DELTA_VERSION, SESSION_CATALOG_KEY, configure_session

from tributary_backend.services.spark import get_spark, preview_conf, preview_packages
from tributary_backend.settings import get_settings


def _delta_spec(orders_csv, tmp_path, **options) -> dict:
    doc = spec(orders_csv, tmp_path / "out")
    doc["sinks"][0] = {"id": "orders_out", "type": "delta", "input": "orders_clean",
                       "options": {"path": "${env.out}/orders", **options}}
    return doc


def _read(path):
    configure_session(get_spark())  # reading Delta needs its catalog too (a preview write would have set it)
    return get_spark().read.format("delta").load(str(path))


def test_preview_session_has_the_delta_jars_but_no_extensions():
    packages = preview_packages(get_settings())
    assert f"io.delta:delta-spark_2.12:{DELTA_VERSION}" in packages
    assert "spark.sql.extensions" not in preview_conf() and SESSION_CATALOG_KEY not in preview_conf()


def test_catalog_lists_the_sink_with_conditional_fields(client):
    delta = next(p for p in client.get("/api/v1/components").json()["components"]["sink"] if p["type"] == "delta")
    assert delta["display_name"] == "Delta Lake" and delta["icon"] == "table"
    form = delta["options_schema"]
    assert form["required"] == ["path"]
    assert form["properties"]["merge_keys"]["x-when"] == {"field": "mode", "in": ["merge"], "required_in": ["merge"]}


def test_validate_reports_missing_merge_keys_on_the_field(client, orders_csv, tmp_path):
    body = client.post("/api/v1/validate", json={"spec": _delta_spec(orders_csv, tmp_path, mode="merge")}).json()
    assert body["valid"] is False
    [issue] = body["issues"]
    assert (issue["node_id"], issue["path"], issue["message"]) == ("orders_out", "sinks[0].options.merge_keys", "required for mode 'merge'")


def test_delta_sink_preview_and_run(client, orders_csv, tmp_path):
    out = tmp_path / "out"
    doc = _delta_spec(orders_csv, tmp_path, mode="merge", merge_keys=["order_id"], partition_by=["country"])
    validated = client.post("/api/v1/validate", json={"spec": doc, "env": "dev", "deep": True}).json()
    assert validated["valid"] is True, validated

    # Preview: the rows the gate lets through come back, nothing is written.
    preview = client.post("/api/v1/preview", json={"spec": doc, "env": "dev", "node_id": "orders_out", "limit": 10})
    assert preview.status_code == 200, preview.text
    result = preview.json()
    assert [r["order_id"] for r in result["rows"]] == [1, 4] and result["written"] is False
    assert result["target"] == f"delta -> {out}/orders (merge)"
    assert not out.exists()

    # Writing from the preview proves the shared session can take Delta's catalog at runtime.
    written = client.post("/api/v1/preview", json={"spec": doc, "env": "dev", "node_id": "orders_out", "limit": 10, "write": True}).json()
    assert written["written"] is True
    assert get_spark().conf.get(SESSION_CATALOG_KEY) == DELTA_CATALOG  # set by the sink, before anything here read the table
    assert get_spark().conf.get("spark.sql.extensions", None) is None
    assert _read(out / "orders").count() == 2

    # Run twice: the merge keeps the table at the 2 rows that pass the 'drop' gate.
    pid = create_pipeline(client, doc)["id"]
    assert client.post(f"/api/v1/pipelines/{pid}/versions", json={}).status_code == 201
    for _ in range(2):
        run = client.post(f"/api/v1/pipelines/{pid}/runs", json={"env": "dev"}).json()
        done = wait_for(client, run["id"], ("succeeded", "failed"))
        assert done["status"] == "succeeded", done
        assert done["summary"]["sinks_written"] == ["orders_out"]
    assert (out / "orders" / "_delta_log").is_dir() and (out / "orders" / "country=IN").is_dir()
    assert {r.order_id: r.country for r in _read(out / "orders").collect()} == {1: "IN", 4: "GB"}


def test_streaming_run_merges_each_micro_batch_into_delta(client, tmp_path):
    """A streaming run in its own worker: files dropped while it runs arrive in the Delta
    table one commit per micro-batch, and cancelling stops the query cleanly."""
    import json
    import os
    import time

    from delta.tables import DeltaTable
    from pyspark.errors import AnalysisException

    incoming, table = tmp_path / "in", tmp_path / "latest"
    incoming.mkdir()

    def drop(name, *records, stamp):
        path = incoming / name
        path.write_text("".join(json.dumps(r) + "\n" for r in records))
        os.utime(path, (stamp, stamp))  # the source reads files in modification-time order

    def rows():
        try:
            return {r.id: r.amount for r in _read(table).collect()}
        except AnalysisException:
            return None  # not created yet

    def wait_for_rows(expected, timeout=150):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if rows() == expected:
                return
            time.sleep(1)
        raise AssertionError(f"table has {rows()}, expected {expected}")

    drop("b0.json", {"id": 1, "amount": 5}, {"id": 2, "amount": 7}, stamp=time.time() - 30)
    doc = {
        "pipeline": {"id": "events_to_delta", "mode": "streaming",
                     "streaming": {"checkpoint_location": str(tmp_path / "chk"), "interval": "1 second"}},
        "sources": [{"id": "events", "type": "file", "options": {"path": str(incoming), "format": "json", "max_files_per_trigger": 1},
                     "schema": {"kind": "ddl", "ddl": "id INT, amount INT"}}],
        "sinks": [{"id": "latest", "type": "delta", "input": "events",
                   "options": {"path": str(table), "mode": "merge", "merge_keys": ["id"]}}],
    }
    pid = create_pipeline(client, doc, "events_to_delta")["id"]
    assert client.post(f"/api/v1/pipelines/{pid}/versions", json={}).status_code == 201
    run = client.post(f"/api/v1/pipelines/{pid}/runs", json={"env": "default"}).json()
    wait_for(client, run["id"], ("running",))

    wait_for_rows({1: 5, 2: 7})
    drop("b1.json", {"id": 2, "amount": 8}, {"id": 3, "amount": 1}, stamp=time.time() - 20)
    wait_for_rows({1: 5, 2: 8, 3: 1})
    drop("b2.json", {"id": 3, "amount": 2}, stamp=time.time() - 10)
    wait_for_rows({1: 5, 2: 8, 3: 2})
    history = [(r.version, r.operation) for r in DeltaTable.forPath(get_spark(), str(table)).history().orderBy("version").collect()]
    assert history == [(0, "WRITE"), (1, "MERGE"), (2, "MERGE")]

    cancelled = client.post(f"/api/v1/runs/{run['id']}/cancel").json()
    assert cancelled["status"] == "cancelled"
    logs = client.get(f"/api/v1/runs/{run['id']}/logs").json()["lines"]
    assert any("writing sink latest: delta -> " in line for line in logs), logs[-20:]
    assert rows() == {1: 5, 2: 8, 3: 2}  # the cancel did not lose or duplicate anything


def test_deep_validate_reports_a_merge_key_the_upstream_sql_does_not_produce(client, orders_csv, tmp_path):
    """Found while drawing, from the upstream node's analysed schema, not at run time."""
    doc = _delta_spec(orders_csv, tmp_path, mode="merge", merge_keys=["order_idx"], update_condition="s.amount > t.amount")
    shallow = client.post("/api/v1/validate", json={"spec": doc, "env": "dev"}).json()
    assert shallow["valid"] is True  # nothing structurally wrong: only the data knows
    deep = client.post("/api/v1/validate", json={"spec": doc, "env": "dev", "deep": True}).json()
    assert deep["valid"] is False
    assert deep["issues"] == [{"path": "sinks[0].options.merge_keys", "node_id": "orders_out",
                               "message": "not in the data: order_idx (columns: order_id, amount, country)"}]
    assert deep["nodes"]["orders_out"]["ok"] is False and deep["nodes"]["orders_out"]["error"] is None
    assert [f["name"] for f in deep["nodes"]["orders_out"]["schema"]["fields"]] == ["order_id", "amount", "country"]

    doc["sinks"][0]["options"]["merge_keys"] = ["order_id"]
    deep = client.post("/api/v1/validate", json={"spec": doc, "env": "dev", "deep": True}).json()
    assert deep["valid"] is True and deep["issues"] == []
