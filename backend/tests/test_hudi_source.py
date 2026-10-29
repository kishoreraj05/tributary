"""A Hudi source end to end through the API: the storage root comes from the same stored
connection the Hudi sink uses, previews and deep validation read the table on the shared
session, and a run reads it from its own worker."""

from __future__ import annotations

from conftest import create_pipeline, wait_for

from tributary.core import RunContext
from tributary.plugins._hudi import commits
from tributary.plugins.sinks.hudi import HudiSink, HudiSinkOptions
from tributary.registry import default_registry

from tributary_backend.services.spark import get_spark


def _write(lake, table, rows) -> None:
    """One commit on a keyed table, through the shared session (which has Kryo for the sink)."""
    spark = get_spark()
    ctx = RunContext(spark=spark, registry=default_registry(), pipeline_id="seed", env="dev")
    df = spark.createDataFrame(rows, "order_id INT, amount DOUBLE, country STRING, version INT")
    HudiSink().write(ctx, df, HudiSinkOptions(base_path=str(lake), table=table, record_key=["order_id"], precombine_field="version"))


def _source_spec(out, **source_options) -> dict:
    return {
        "pipeline": {"id": "orders_from_hudi", "mode": "batch"},
        "environments": {"dev": {"vars": {"out": str(out)}}},
        "sources": [{"id": "orders_src", "type": "hudi", "connection": "lake", "options": {"table": "sales.orders", **source_options}}],
        "transforms": [{
            "id": "orders_clean", "type": "sql", "inputs": [{"node": "orders_src", "as": "orders"}],
            "options": {"sql": "SELECT order_id, amount, upper(country) AS country FROM orders"},
        }],
        "sinks": [{"id": "orders_out", "type": "file", "input": "orders_clean", "options": {"path": "${env.out}/orders", "mode": "overwrite"}}],
    }


def test_catalog_exposes_the_hudi_source_and_when_its_settings_apply(client):
    """The editor's palette and form are generated from this: the source is there with both
    capabilities, and the form knows which fields go with which query type."""
    [hudi] = [p for p in client.get("/api/v1/components").json()["components"]["source"] if p["type"] == "hudi"]
    assert hudi["display_name"] == "Apache Hudi" and hudi["icon"] == "table"
    assert hudi["capabilities"] == ["batch", "streaming"]
    props = hudi["options_schema"]["properties"]
    assert hudi["options_schema"]["required"] == ["base_path", "table"]
    assert props["query_type"]["enum"] == ["snapshot", "read_optimized", "incremental"] and props["query_type"]["default"] == "snapshot"
    assert props["begin_instant"]["x-when"] == {"field": "query_type", "in": ["incremental"], "required_in": ["incremental"]}
    assert props["as_of_instant"]["x-when"] == {"field": "query_type", "in": ["snapshot"], "required_in": []}
    assert props["incremental_format"]["enum"] == ["latest_state", "cdc"]
    assert props["include_metadata_columns"]["type"] == "boolean" and props["include_metadata_columns"]["default"] is False


def test_hudi_source_from_connection_validate_preview_and_run(client, tmp_path):
    lake = tmp_path / "lake"
    _write(lake, "sales.orders", [(1, 10.0, "in", 1), (2, 20.0, "us", 1)])
    _write(lake, "sales.orders", [(2, 25.0, "us", 2), (3, 30.0, "de", 1)])
    first, second = [c.timestamp for c in commits(get_spark(), f"{lake}/sales/orders")]
    created = client.post("/api/v1/connections", json={
        "name": "lake", "type": "hudi", "environments": {"dev": {"base_path": str(lake), "catalog_sync": "none"}},
    })
    assert created.status_code == 201, created.text
    doc = _source_spec(tmp_path / "out")

    # Deep validation reads the table's schema so the SQL editor can offer its columns.
    validated = client.post("/api/v1/validate", json={"spec": doc, "env": "dev", "deep": True}).json()
    assert validated["valid"] is True, validated
    assert sorted(f["name"] for f in validated["nodes"]["orders_src"]["schema"]["fields"]) == ["amount", "country", "order_id", "version"]

    # Preview: the table's rows, without Hudi's metadata columns.
    preview = client.post("/api/v1/preview", json={"spec": doc, "env": "dev", "node_id": "orders_src", "limit": 10})
    assert preview.status_code == 200, preview.text
    result = preview.json()
    assert sorted((r["order_id"], r["amount"]) for r in result["rows"]) == [(1, 10.0), (2, 25.0), (3, 30.0)]
    assert not any(k.startswith("_hoodie_") for k in result["rows"][0])

    # Only the changes since the first commit, with a commit column kept for the next run.
    incremental = _source_spec(tmp_path / "out", query_type="incremental", begin_instant=first, include_metadata_columns=True)
    preview = client.post("/api/v1/preview", json={"spec": incremental, "env": "dev", "node_id": "orders_src", "limit": 10}).json()
    assert sorted((r["order_id"], r["amount"], r["_hoodie_commit_time"]) for r in preview["rows"]) == [(2, 25.0, second), (3, 30.0, second)]

    # A run reads the table from its own worker process.
    pid = create_pipeline(client, doc)["id"]
    assert client.post(f"/api/v1/pipelines/{pid}/versions", json={}).status_code == 201
    run = client.post(f"/api/v1/pipelines/{pid}/runs", json={"env": "dev"}).json()
    done = wait_for(client, run["id"], ("succeeded", "failed"), timeout=600)
    assert done["status"] == "succeeded", done
    assert done["summary"]["sinks_written"] == ["orders_out"]
    written = get_spark().read.parquet(str(tmp_path / "out" / "orders"))
    assert sorted((r.order_id, r.amount, r.country) for r in written.collect()) == [(1, 10.0, "IN"), (2, 25.0, "US"), (3, 30.0, "DE")]


def test_problems_are_reported_on_the_source_node(client, tmp_path):
    """A missing begin_instant lands on its field; a table that is not there is reported by
    deep validation on the source node, and the nodes after it are skipped."""
    assert client.post("/api/v1/connections", json={"name": "lake", "type": "hudi", "environments": {"dev": {"base_path": str(tmp_path / "lake")}}}).status_code == 201
    doc = _source_spec(tmp_path / "out", query_type="incremental")
    validated = client.post("/api/v1/validate", json={"spec": doc, "env": "dev"}).json()
    assert validated["valid"] is False
    assert [(i["path"], i["node_id"]) for i in validated["issues"]] == [("sources[0].options.begin_instant", "orders_src")]

    doc = _source_spec(tmp_path / "out")
    validated = client.post("/api/v1/validate", json={"spec": doc, "env": "dev", "deep": True}).json()
    assert validated["issues"] == [] and validated["valid"] is False
    source = validated["nodes"]["orders_src"]
    assert source["ok"] is False and source["error"].startswith(f"hudi table sales.orders not found at {tmp_path}/lake/sales/orders")
    assert validated["nodes"]["orders_clean"]["error"] == "skipped: upstream orders_src has errors"

    preview = client.post("/api/v1/preview", json={"spec": doc, "env": "dev", "node_id": "orders_src", "limit": 10})
    assert preview.status_code == 400 and "not found at" in preview.json()["detail"]["message"]
