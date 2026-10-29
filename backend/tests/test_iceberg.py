"""An Iceberg sink end to end through the API: the catalog comes from a stored connection,
previews are dry runs on the shared session, and a run writes from its own worker."""

from __future__ import annotations

from conftest import create_pipeline, spec, wait_for

from tributary.plugins.sinks.iceberg import ICEBERG_VERSION

from tributary_backend.services.spark import get_spark, preview_conf, preview_packages
from tributary_backend.settings import get_settings


def _iceberg_spec(orders_csv, tmp_path, **options) -> dict:
    doc = spec(orders_csv, tmp_path / "out")
    doc["sinks"][0] = {
        "id": "orders_out", "type": "iceberg", "input": "orders_clean", "connection": "lake",
        "options": {"table": "sales.orders", **options},
    }
    return doc


def test_preview_session_has_the_iceberg_runtime_but_no_extensions():
    """The jar is there for Iceberg previews; nothing Iceberg-specific is wired into the
    shared session state, so pipelines that never touch Iceberg are not affected by it."""
    assert any(p.endswith(f":{ICEBERG_VERSION}") and "iceberg-spark-runtime" in p for p in preview_packages(get_settings()))
    assert "spark.sql.extensions" not in preview_conf()
    assert get_spark().conf.get("spark.sql.extensions", None) is None


def test_iceberg_sink_from_connection_preview_and_run(client, orders_csv, tmp_path):
    warehouse = tmp_path / "warehouse"
    created = client.post("/api/v1/connections", json={
        "name": "lake", "type": "iceberg",
        "environments": {"dev": {"catalog_type": "hadoop", "warehouse": str(warehouse), "catalog_name": "lake_test"}},
    })
    assert created.status_code == 201, created.text
    doc = _iceberg_spec(orders_csv, tmp_path, mode="merge", merge_keys=["order_id"], partition_by=["country"])

    validated = client.post("/api/v1/validate", json={"spec": doc, "env": "dev", "deep": True}).json()
    assert validated["valid"] is True, validated

    # Preview: the rows the gate lets through come back, nothing is written, the catalog is not even touched.
    preview = client.post("/api/v1/preview", json={"spec": doc, "env": "dev", "node_id": "orders_out", "limit": 10})
    assert preview.status_code == 200, preview.text
    result = preview.json()
    assert [r["order_id"] for r in result["rows"]] == [1, 4] and result["written"] is False
    assert result["target"] == f"iceberg hadoop -> lake_test.sales.orders (merge) at {warehouse}"
    assert not warehouse.exists()

    # Run twice: the merge keeps the table at the 2 rows that pass the 'drop' gate.
    pid = create_pipeline(client, doc)["id"]
    assert client.post(f"/api/v1/pipelines/{pid}/versions", json={}).status_code == 201
    for _ in range(2):
        run = client.post(f"/api/v1/pipelines/{pid}/runs", json={"env": "dev"}).json()
        done = wait_for(client, run["id"], ("succeeded", "failed"))
        assert done["status"] == "succeeded", done
        assert done["summary"]["sinks_written"] == ["orders_out"]
    assert (warehouse / "sales" / "orders" / "metadata").is_dir()

    # Read it back through the shared session (the catalog registers itself on first use).
    from tributary.plugins.sinks.iceberg import IcebergSinkOptions, configure_catalog

    opts = IcebergSinkOptions(warehouse=str(warehouse), catalog_name="lake_test", table="sales.orders")
    configure_catalog(get_spark(), opts)
    rows = {r.order_id: r.country for r in get_spark().table(opts.qualified_table()).collect()}
    assert rows == {1: "IN", 4: "GB"}


def test_iceberg_source_from_connection_preview_and_run(client, orders_csv, tmp_path):
    """The same 'iceberg' connection serves the source. Deep validation shows the table's
    columns, previews read through the shared session, and a run reads in its own worker."""
    from tributary.core import RunContext
    from tributary.plugins.sinks.iceberg import IcebergSink, IcebergSinkOptions
    from tributary.registry import default_registry

    warehouse = tmp_path / "warehouse"
    created = client.post("/api/v1/connections", json={
        "name": "lake", "type": "iceberg",
        "environments": {"dev": {"catalog_type": "hadoop", "warehouse": str(warehouse), "catalog_name": "lake_src_test"}},
    })
    assert created.status_code == 201, created.text
    # A table to read: made by the sink on the shared session.
    spark = get_spark()
    table = IcebergSinkOptions(warehouse=str(warehouse), catalog_name="lake_src_test", table="sales.orders")
    ctx = RunContext(spark=spark, registry=default_registry(), pipeline_id="seed", env="dev")
    IcebergSink().write(ctx, spark.createDataFrame([(1, 10.0, "in"), (2, 5.0, "de"), (3, -1.0, "us")], "order_id INT, amount DOUBLE, country STRING"), table)

    doc = spec(orders_csv, tmp_path / "out")
    doc["sources"] = [{"id": "orders_src", "type": "iceberg", "connection": "lake", "options": {"table": "sales.orders"}}]
    body = client.get("/api/v1/components").json()
    [source_meta] = [p for p in body["components"]["source"] if p["type"] == "iceberg"]
    assert source_meta["capabilities"] == ["batch", "streaming"]
    assert source_meta["options_schema"]["properties"]["warehouse"]["x-when"]["required_in"] == ["hadoop", "glue", "jdbc", "nessie"]

    validated = client.post("/api/v1/validate", json={"spec": doc, "env": "dev", "deep": True}).json()
    assert validated["valid"] is True, validated
    assert [f["name"] for f in validated["nodes"]["orders_src"]["schema"]["fields"]] == ["order_id", "amount", "country"]

    preview = client.post("/api/v1/preview", json={"spec": doc, "env": "dev", "node_id": "orders_src", "limit": 2})
    assert preview.status_code == 200, preview.text
    assert len(preview.json()["rows"]) == 2 and preview.json()["plan"] is None
    # With explain, the plan shows what reached Iceberg: here the source's own filter, pushed down.
    filtered = {**doc, "sources": [{**doc["sources"][0], "options": {"table": "sales.orders", "filter": "country = 'in'"}}]}
    preview = client.post("/api/v1/preview", json={"spec": filtered, "env": "dev", "node_id": "orders_src", "explain": True})
    assert preview.status_code == 200, preview.text
    assert [r["order_id"] for r in preview.json()["rows"]] == [1]
    assert "filters=" in preview.json()["plan"] and "country = 'in'" in preview.json()["plan"]
    # Snapshot ids for time travel come from the metadata tables, previewable like any node.
    meta = {**doc, "sources": [{**doc["sources"][0], "options": {"table": "sales.orders", "metadata_table": "snapshots"}}]}
    preview = client.post("/api/v1/preview", json={"spec": meta, "env": "dev", "node_id": "orders_src"})
    assert preview.status_code == 200, preview.text
    [snapshot] = preview.json()["rows"]
    # 64-bit ids come as strings of digits: JavaScript would round them past 2**53.
    assert snapshot["operation"] == "append" and snapshot["snapshot_id"].isdigit()
    travel = {**doc, "sources": [{**doc["sources"][0], "options": {"table": "sales.orders", "snapshot_id": snapshot["snapshot_id"]}}]}
    preview = client.post("/api/v1/preview", json={"spec": travel, "env": "dev", "node_id": "orders_src"})
    assert preview.status_code == 200 and len(preview.json()["rows"]) == 3
    preview = client.post("/api/v1/preview", json={"spec": doc, "env": "dev", "node_id": "orders_out", "limit": 10})
    rows = preview.json()["rows"]
    assert sorted(r["order_id"] for r in rows) == [1, 2]  # the 'drop' gate removed the negative amount
    assert {r["country"] for r in rows} == {"IN", "DE"} and preview.json()["written"] is False

    # A wrong table is reported on the node, not as a crash.
    wrong = {**doc, "sources": [{**doc["sources"][0], "options": {"table": "sales.nope"}}]}
    preview = client.post("/api/v1/preview", json={"spec": wrong, "env": "dev", "node_id": "orders_src"})
    assert preview.status_code == 400 and "sales.nope does not exist" in preview.json()["detail"]["message"]

    pid = create_pipeline(client, doc, name="iceberg_to_parquet")["id"]
    assert client.post(f"/api/v1/pipelines/{pid}/versions", json={}).status_code == 201
    run = client.post(f"/api/v1/pipelines/{pid}/runs", json={"env": "dev"}).json()
    done = wait_for(client, run["id"], ("succeeded", "failed"))
    assert done["status"] == "succeeded", done
    assert done["summary"]["sinks_written"] == ["orders_out"]
    assert {r.order_id: r.country for r in spark.read.parquet(str(tmp_path / "out" / "orders")).collect()} == {1: "IN", 2: "DE"}
