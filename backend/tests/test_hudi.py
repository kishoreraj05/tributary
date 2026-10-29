"""A Hudi sink end to end through the API: the storage root comes from a stored connection,
previews are dry runs on the shared session, and a run writes from its own worker."""

from __future__ import annotations

from conftest import create_pipeline, spec, wait_for

from tributary.plugins.sinks.hudi import HUDI_VERSION, KRYO_SERIALIZER

from tributary_backend.services.spark import get_spark, preview_conf, preview_packages
from tributary_backend.settings import get_settings


def _hudi_spec(orders_csv, tmp_path, **options) -> dict:
    doc = spec(orders_csv, tmp_path / "out")
    doc["sinks"][0] = {
        "id": "orders_out", "type": "hudi", "input": "orders_clean", "connection": "lake",
        "options": {"table": "sales.orders", **options},
    }
    return doc


def test_preview_session_has_the_hudi_bundle_and_kryo_but_no_extensions():
    """The bundle is there for Hudi previews, and Kryo serialization (which Hudi insists on)
    is the one setting it puts on the shared session: no SQL extension or catalog, so
    pipelines that never touch Hudi are not affected by it."""
    assert any(p.endswith(f":{HUDI_VERSION}") and "hudi-spark3.5-bundle" in p for p in preview_packages(get_settings()))
    assert preview_conf()["spark.serializer"] == KRYO_SERIALIZER
    assert "spark.sql.extensions" not in preview_conf()
    assert get_spark().conf.get("spark.serializer") == KRYO_SERIALIZER
    assert get_spark().conf.get("spark.sql.extensions", None) is None


def test_hudi_sink_from_connection_preview_and_run(client, orders_csv, tmp_path):
    lake = tmp_path / "lake"
    created = client.post("/api/v1/connections", json={
        "name": "lake", "type": "hudi", "environments": {"dev": {"base_path": str(lake)}},
    })
    assert created.status_code == 201, created.text
    doc = _hudi_spec(orders_csv, tmp_path, record_key=["order_id"], precombine_field="amount", partition_by=["country"])

    validated = client.post("/api/v1/validate", json={"spec": doc, "env": "dev", "deep": True}).json()
    assert validated["valid"] is True, validated

    # Preview: the rows the gate lets through come back, nothing is written.
    preview = client.post("/api/v1/preview", json={"spec": doc, "env": "dev", "node_id": "orders_out", "limit": 10})
    assert preview.status_code == 200, preview.text
    result = preview.json()
    assert [r["order_id"] for r in result["rows"]] == [1, 4] and result["written"] is False
    assert result["target"] == f"hudi copy_on_write -> sales.orders (upsert) at {lake}/sales/orders"
    assert not lake.exists()

    # Run twice: the upsert keeps the table at the 2 rows that pass the 'drop' gate.
    pid = create_pipeline(client, doc)["id"]
    assert client.post(f"/api/v1/pipelines/{pid}/versions", json={}).status_code == 201
    for _ in range(2):
        run = client.post(f"/api/v1/pipelines/{pid}/runs", json={"env": "dev"}).json()
        done = wait_for(client, run["id"], ("succeeded", "failed"), timeout=600)
        assert done["status"] == "succeeded", done
        assert done["summary"]["sinks_written"] == ["orders_out"]
    table = lake / "sales" / "orders"
    assert (table / ".hoodie" / "hoodie.properties").is_file()
    assert sorted(p.name for p in table.iterdir() if not p.name.startswith(".")) == ["country=GB", "country=IN"]

    # Read it back through the shared session.
    rows = {r.order_id: r.country for r in get_spark().read.format("hudi").load(str(table)).collect()}
    assert rows == {1: "IN", 4: "GB"}


def test_missing_settings_are_reported_per_field(client, orders_csv, tmp_path):
    """Without a connection (so no storage root) and with upsert's keys missing, every issue
    lands on its own field so the editor can show them in the form."""
    doc = _hudi_spec(orders_csv, tmp_path)
    del doc["sinks"][0]["connection"]
    validated = client.post("/api/v1/validate", json={"spec": doc, "env": "dev"}).json()
    assert validated["valid"] is False
    assert {i["path"] for i in validated["issues"]} == {
        "sinks[0].options.base_path", "sinks[0].options.record_key", "sinks[0].options.precombine_field",
    }


def test_catalog_exposes_the_schema_evolution_and_key_scope_options(client):
    """The editor's form is generated from this: the choices, their defaults and when they
    apply must be in the JSON Schema the API serves."""
    [hudi] = [p for p in client.get("/api/v1/components").json()["components"]["sink"] if p["type"] == "hudi"]
    props = hudi["options_schema"]["properties"]
    assert props["schema_evolution"]["enum"] == ["add_columns", "reconcile", "drop_columns", "strict"]
    assert props["schema_evolution"]["default"] == "add_columns"
    assert props["global_index"]["type"] == "boolean" and props["global_index"]["default"] is False
    assert props["table_type"]["enum"] == ["copy_on_write", "merge_on_read"]
    assert props["operation"]["enum"] == ["upsert", "insert", "bulk_insert", "insert_overwrite", "insert_overwrite_table", "delete"]
    assert props["record_key"]["x-when"] == {"field": "operation", "required_in": ["upsert", "delete"]}
    assert hudi["display_name"] == "Apache Hudi" and hudi["icon"] == "table"


def test_deep_validation_flags_columns_the_sink_names_but_its_input_lacks(client, orders_csv, tmp_path):
    """What the editor shows on the sink node after "Checking columns": the message names
    the option, the column and the columns the input does have."""
    assert client.post("/api/v1/connections", json={"name": "lake", "type": "hudi", "environments": {"dev": {"base_path": str(tmp_path / "lake")}}}).status_code == 201
    doc = _hudi_spec(orders_csv, tmp_path, record_key=["order_id"], precombine_field="updated_at", partition_by=["country"])
    validated = client.post("/api/v1/validate", json={"spec": doc, "env": "dev", "deep": True}).json()
    assert validated["valid"] is False
    assert validated["issues"] == [{"path": "sinks[0].options.precombine_field", "node_id": "orders_out",
                                    "message": "not in the data: updated_at (columns: order_id, amount, country)"}]
    sink = validated["nodes"]["orders_out"]
    assert sink["ok"] is False and sink["error"] is None
    assert validated["nodes"]["orders_clean"]["ok"] is True
