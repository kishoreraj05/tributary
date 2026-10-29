"""A Delta source end to end through the API: the catalog shows its form, deep validation
takes the columns from the table (or from a declared schema, before the table exists),
previews read on the shared session, and a run reads from its own worker, whose session
is built from what the source asks for."""

from __future__ import annotations

from conftest import create_pipeline, spec, wait_for

from tributary import run
from tributary.plugins.sinks.delta import DELTA_CATALOG, SESSION_CATALOG_KEY

from tributary_backend.services.spark import get_spark

CHANGE_COLUMNS = ["_change_type", "_commit_version", "_commit_timestamp"]


def _make_table(orders_csv, tmp_path, **sink_options):
    """The orders table csv_to_delta-style: the conftest pipeline (a 'drop' gate keeps orders
    1 and 4) written by the Delta sink on the shared session."""
    table = tmp_path / "lake" / "orders"
    doc = spec(orders_csv, tmp_path / "out")
    doc["sinks"][0] = {"id": "orders_out", "type": "delta", "input": "orders_clean", "options": {"path": str(table), **sink_options}}
    run(doc, spark=get_spark())
    return table


def _from_delta(table, out, **options) -> dict:
    """Delta table -> SQL -> Parquet; no schema block: the table has one."""
    return {
        "pipeline": {"id": "from_delta", "mode": "batch"},
        "environments": {"dev": {"vars": {"out": str(out)}}},
        "sources": [{"id": "orders_src", "type": "delta", "options": {"path": str(table), **options}}],
        "transforms": [{"id": "by_country", "type": "sql", "inputs": [{"node": "orders_src", "as": "orders"}],
                        "options": {"sql": "SELECT country, count(*) AS orders, sum(amount) AS amount FROM orders GROUP BY country"}}],
        "sinks": [{"id": "out", "type": "file", "input": "by_country", "options": {"path": "${env.out}/by_country", "mode": "overwrite"}}],
    }


def test_catalog_lists_the_source_with_its_form(client):
    delta = next(p for p in client.get("/api/v1/components").json()["components"]["source"] if p["type"] == "delta")
    assert delta["display_name"] == "Delta Lake" and delta["icon"] == "table"
    assert delta["capabilities"] == ["batch", "streaming"]
    form = delta["options_schema"]
    assert form["required"] == ["path"]
    assert form["properties"]["version"]["x-when"] == {"field": "change_feed", "in": [False], "required_in": []}
    assert form["properties"]["on_changes"]["enum"] == ["fail", "ignore_deletes", "ignore_changes", "skip_commits"]


def test_validate_reports_conflicting_options_on_their_field(client, tmp_path):
    doc = _from_delta(tmp_path / "orders", tmp_path / "out", version=3, timestamp="2024-05-01")
    body = client.post("/api/v1/validate", json={"spec": doc, "env": "dev"}).json()
    assert body["valid"] is False
    [issue] = body["issues"]
    assert (issue["node_id"], issue["path"], issue["message"]) == (
        "orders_src", "sources[0].options.timestamp", "set either version or timestamp, not both")


def test_delta_source_deep_validate_preview_and_run(client, orders_csv, tmp_path):
    table = _make_table(orders_csv, tmp_path)
    out = tmp_path / "from_delta"
    doc = _from_delta(table, out)

    # Deep validation: the source's columns come from the table's log, the SQL node's from them.
    deep = client.post("/api/v1/validate", json={"spec": doc, "env": "dev", "deep": True}).json()
    assert deep["valid"] is True, deep
    assert [f["name"] for f in deep["nodes"]["orders_src"]["schema"]["fields"]] == ["order_id", "amount", "country"]
    assert [f["name"] for f in deep["nodes"]["by_country"]["schema"]["fields"]] == ["country", "orders", "amount"]

    # Preview: a bounded read on the shared session, time travel included.
    preview = client.post("/api/v1/preview", json={"spec": doc, "env": "dev", "node_id": "orders_src", "limit": 10})
    assert preview.status_code == 200, preview.text
    assert sorted(r["order_id"] for r in preview.json()["rows"]) == [1, 4]
    assert get_spark().conf.get(SESSION_CATALOG_KEY) == DELTA_CATALOG and get_spark().conf.get("spark.sql.extensions", None) is None
    preview = client.post("/api/v1/preview", json={"spec": doc, "env": "dev", "node_id": "by_country", "limit": 10}).json()
    assert {r["country"]: r["orders"] for r in preview["rows"]} == {"IN": 1, "GB": 1}

    # Run: its worker has no Delta sink to configure the session; the source asks for the jar and the catalog.
    pid = create_pipeline(client, doc, "from_delta")["id"]
    assert client.post(f"/api/v1/pipelines/{pid}/versions", json={}).status_code == 201
    started = client.post(f"/api/v1/pipelines/{pid}/runs", json={"env": "dev"}).json()
    done = wait_for(client, started["id"], ("succeeded", "failed"))
    assert done["status"] == "succeeded", done
    assert done["summary"]["sinks_written"] == ["out"]
    rows = {r.country: (r.orders, r.amount) for r in get_spark().read.parquet(str(out / "by_country")).collect()}
    assert rows == {"IN": (1, 10.0), "GB": (1, 7.5)}


def test_deep_validate_before_the_table_exists_needs_a_declared_schema(client, tmp_path):
    """A pipeline drawn ahead of its producer: without a schema the source node reports
    the missing table; with one, every node analyses fine."""
    doc = _from_delta(tmp_path / "not_yet", tmp_path / "out")
    deep = client.post("/api/v1/validate", json={"spec": doc, "env": "dev", "deep": True}).json()
    assert deep["valid"] is False
    assert "not_yet" in deep["nodes"]["orders_src"]["error"]
    assert deep["nodes"]["by_country"]["error"].startswith("skipped: upstream orders_src")

    doc["sources"][0]["schema"] = {"kind": "ddl", "ddl": "order_id INT, amount DOUBLE, country STRING"}
    deep = client.post("/api/v1/validate", json={"spec": doc, "env": "dev", "deep": True}).json()
    assert deep["valid"] is True, deep
    assert [f["name"] for f in deep["nodes"]["by_country"]["schema"]["fields"]] == ["country", "orders", "amount"]


def test_change_feed_preview_shows_the_changes(client, orders_csv, tmp_path):
    table = _make_table(orders_csv, tmp_path, write_options={"delta.enableChangeDataFeed": True})
    doc = _from_delta(table, tmp_path / "out", change_feed=True, starting_version=0)
    doc["transforms"][0]["options"]["sql"] = "SELECT order_id, _change_type, _commit_version FROM orders"

    deep = client.post("/api/v1/validate", json={"spec": doc, "env": "dev", "deep": True}).json()
    assert deep["valid"] is True, deep
    assert [f["name"] for f in deep["nodes"]["orders_src"]["schema"]["fields"]] == ["order_id", "amount", "country", *CHANGE_COLUMNS]
    preview = client.post("/api/v1/preview", json={"spec": doc, "env": "dev", "node_id": "by_country", "limit": 10}).json()
    assert sorted((r["order_id"], r["_change_type"], r["_commit_version"]) for r in preview["rows"]) == [(1, "insert", 0), (4, "insert", 0)]

    # Without a start (a stream would not need one), a preview samples the last commit's changes.
    doc["sources"][0]["options"].pop("starting_version")
    preview = client.post("/api/v1/preview", json={"spec": doc, "env": "dev", "node_id": "by_country", "limit": 10})
    assert preview.status_code == 200, preview.text
    assert sorted((r["order_id"], r["_change_type"], r["_commit_version"]) for r in preview.json()["rows"]) == [(1, "insert", 0), (4, "insert", 0)]


def test_dummy_data_lets_the_editor_test_a_pipeline_without_the_table(client, tmp_path):
    """In the local environment the node's dummy data stands in: the path is never contacted."""
    nowhere = tmp_path / "nowhere"
    doc = _from_delta(nowhere, tmp_path / "out")
    doc["environments"] = {"local": doc["environments"]["dev"]}
    doc["sources"][0]["dummy"] = {"enabled": True, "data": "\n".join([
        '{"order_id": 1, "amount": 10.0, "country": "IN"}', '{"order_id": 2, "amount": 5.0, "country": "GB"}', '{"order_id": 3, "amount": 2.5, "country": "IN"}'])}

    deep = client.post("/api/v1/validate", json={"spec": doc, "env": "local", "deep": True}).json()
    assert deep["valid"] is True, deep
    assert sorted(f["name"] for f in deep["nodes"]["orders_src"]["schema"]["fields"]) == ["amount", "country", "order_id"]
    preview = client.post("/api/v1/preview", json={"spec": doc, "env": "local", "node_id": "by_country", "limit": 10}).json()
    assert {r["country"]: (r["orders"], r["amount"]) for r in preview["rows"]} == {"IN": (2, 12.5), "GB": (1, 5.0)}
    assert not nowhere.exists()

    doc["sources"][0]["dummy"]["data"] = [{"order_id": i} for i in range(1001)]
    body = client.post("/api/v1/validate", json={"spec": doc, "env": "local"}).json()
    assert body["valid"] is False
    assert [(i["node_id"], i["path"], i["message"]) for i in body["issues"]] == [
        ("orders_src", "sources[0].dummy.data", "at most 1000 records (1001 given)")]
