"""The Delta sink: options, session configuration and real writes to local tables.

Tests that write need the Delta jars (see ``delta_jars`` in conftest); the others run
without Spark. The shared session has no Delta SQL extension: the sink must not need it.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from conftest import ROW_CHECKS, pipeline
from tributary import SpecError, TributaryError, compile_pipeline, preview, run
from tributary.core import RunContext
from tributary.plugins.sinks.delta import (
    DELTA_CATALOG,
    DELTA_VERSION,
    SESSION_CATALOG_KEY,
    DeltaSink,
    DeltaSinkOptions,
    configure_session,
)
from tributary.registry import default_registry
from tributary.runtime import decide_resources, required_conf, required_packages, session_conf
from tributary.testing import assert_sink_contract, static_contract_issues


def delta_sink(path, **options) -> dict:
    return {"id": "orders_out", "type": "delta", "input": "orders_clean", "options": {"path": str(path), **options}}


def delta_pipeline(tmp_path, orders_csv, **options) -> dict:
    return pipeline(tmp_path, orders_csv, sinks=[delta_sink(tmp_path / "orders", **options)])


def read(spark, path):
    return spark.read.format("delta").load(str(path))


def rows(spark, path) -> dict[int | None, tuple[float, str]]:
    return {r.order_id: (r.amount, r.country) for r in read(spark, path).collect()}


def history(spark, path) -> list[str]:
    from delta.tables import DeltaTable

    return [r.operation for r in DeltaTable.forPath(spark, str(path)).history().orderBy("version").collect()]


def ctx(spark, batch_id=None, pipeline_id="delta_test") -> RunContext:
    return RunContext(spark=spark, registry=default_registry(), pipeline_id=pipeline_id, env="dev", batch_id=batch_id)


# --- no Spark needed ---------------------------------------------------------------------


def test_registered_with_its_contract_met():
    assert default_registry().get("sink", "delta") is DeltaSink
    assert static_contract_issues(DeltaSink) == []
    assert DeltaSink.spark_packages[0] == f"io.delta:delta-spark_{{scala_version}}:{DELTA_VERSION}"


def test_options_are_validated():
    ok = DeltaSinkOptions(path="/t", mode="merge", merge_keys=["id"], partition_by=[" country "])
    assert ok.partition_by == ["country"]
    for bad, message in [
        ({}, "path"),
        ({"path": "/t", "mode": "merge"}, r"merge_keys\n  Value error, required for mode 'merge'"),
        ({"path": "/t", "merge_keys": ["id"]}, "only applies to mode 'merge'"),
        ({"path": "/t", "delete_condition": "s.op = 'D'"}, r"delete_condition\n  Value error, only applies to mode 'merge'"),
        ({"path": "/t", "partition_by": ["days(ts)"]}, "invalid partition column"),
        ({"path": "/t", "mode": "upsert"}, "mode"),
        ({"path": "/t", "coalesce": 1}, "extra"),
    ]:
        with pytest.raises(ValidationError, match=message):
            DeltaSinkOptions(**bad)


def test_fields_say_which_mode_they_apply_to():
    """The UI shows merge_keys only for mode merge (starred), the conditions for their mode."""
    props = DeltaSinkOptions.model_json_schema()["properties"]
    assert props["merge_keys"]["x-when"] == {"field": "mode", "in": ["merge"], "required_in": ["merge"]}
    assert props["delete_condition"]["x-when"] == {"field": "mode", "in": ["merge"], "required_in": []}
    assert props["mode"]["enum"] == ["append", "overwrite", "overwrite_partitions", "merge"]


def test_issues_are_reported_on_their_field(tmp_path, orders_csv):
    doc = delta_pipeline(tmp_path, orders_csv, mode="merge", delete_condition="s.op = 'D'", partition_by=["days(ts)"])
    doc["sinks"][0]["options"]["mode"] = "append"
    with pytest.raises(SpecError) as exc:
        compile_pipeline(doc)
    assert {(i.path, i.message) for i in exc.value.issues} == {
        ("sinks[0].options.delete_condition", "only applies to mode 'merge'"),
        ("sinks[0].options.partition_by", "invalid partition column(s): days(ts); Delta partitions by plain columns"),
    }
    doc = delta_pipeline(tmp_path, orders_csv, mode="merge")
    with pytest.raises(SpecError) as exc:
        compile_pipeline(doc)
    assert [(i.path, i.message) for i in exc.value.issues] == [("sinks[0].options.merge_keys", "required for mode 'merge'")]


def test_describe():
    assert DeltaSink().describe(DeltaSinkOptions(path="/lake/orders", mode="merge", merge_keys=["id"])) == "delta -> /lake/orders (merge)"


def test_session_gets_the_jar_and_the_catalog_but_no_extensions(tmp_path, orders_csv):
    plain = compile_pipeline(pipeline(tmp_path, orders_csv))
    assert required_packages(plain) == [] and required_conf(plain) == {}

    compiled = compile_pipeline(delta_pipeline(tmp_path, orders_csv))
    assert required_packages(compiled) == [f"io.delta:delta-spark_2.12:{DELTA_VERSION}", f"io.delta:delta-storage:{DELTA_VERSION}"]
    assert required_conf(compiled) == {SESSION_CATALOG_KEY: DELTA_CATALOG}
    conf = session_conf(compiled, decide_resources(compiled))
    assert conf[SESSION_CATALOG_KEY] == DELTA_CATALOG
    assert "spark.sql.extensions" not in conf
    assert f"delta-spark_2.12:{DELTA_VERSION}" in conf["spark.jars.packages"]

    # The user can still add Delta's SQL extension, and their spark_conf wins elsewhere.
    doc = delta_pipeline(tmp_path, orders_csv)
    doc["runtime"] = {"dev": {"spark_conf": {"spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension"}}}
    compiled = compile_pipeline(doc)
    assert session_conf(compiled, decide_resources(compiled))["spark.sql.extensions"] == "io.delta.sql.DeltaSparkSessionExtension"


# --- real writes (local disk) --------------------------------------------------------------


def test_sink_contract(delta_spark, tmp_path):
    assert_sink_contract(delta_spark, DeltaSink(), DeltaSinkOptions(path=str(tmp_path / "numbers")), delta_spark.range(3))
    assert read(delta_spark, tmp_path / "numbers").count() == 3


def test_append_creates_then_appends(delta_spark, tmp_path, orders_csv):
    path = tmp_path / "orders"
    doc = delta_pipeline(tmp_path, orders_csv, partition_by=["country"], write_options={"delta.appendOnly": True, "userMetadata": "first"})
    assert run(doc, spark=delta_spark).sinks_written == ["orders_out"]
    assert rows(delta_spark, path) == {1: (10.0, "IN"), 2: (-1.0, "US"), None: (5.0, "DE"), 4: (7.5, "USA"), 5: (3.0, "GB")}
    assert (path / "_delta_log").is_dir() and (path / "country=IN").is_dir()

    run(doc, spark=delta_spark)
    assert read(delta_spark, path).count() == 10
    assert history(delta_spark, path) == ["WRITE", "WRITE"]
    # delta.* writer options became table properties: the table is append-only now.
    with pytest.raises(Exception, match="DELTA_CANNOT_MODIFY_APPEND_ONLY"):
        run(pipeline(tmp_path, orders_csv, sinks=[delta_sink(path, mode="overwrite")]), spark=delta_spark)


def test_overwrite_and_overwrite_partitions(delta_spark, tmp_path, orders_csv):
    path = tmp_path / "orders"
    overwrite = delta_pipeline(tmp_path, orders_csv, mode="overwrite", partition_by=["country"])
    run(overwrite, spark=delta_spark)
    run(overwrite, spark=delta_spark)
    assert read(delta_spark, path).count() == 5

    update = tmp_path / "update.csv"
    update.write_text("order_id,amount,country\n1,99.0,IN\n6,1.0,IN\n")
    run(pipeline(tmp_path, update, sinks=[delta_sink(path, mode="overwrite_partitions")]), spark=delta_spark)
    assert rows(delta_spark, path) == {1: (99.0, "IN"), 6: (1.0, "IN"), 2: (-1.0, "US"), None: (5.0, "DE"), 4: (7.5, "USA"), 5: (3.0, "GB")}
    assert history(delta_spark, path) == ["WRITE", "WRITE", "WRITE"]


def test_merge_upserts_on_keys(delta_spark, tmp_path, orders_csv):
    path = tmp_path / "orders"
    merge = {"mode": "merge", "merge_keys": ["order_id"]}
    run(delta_pipeline(tmp_path, orders_csv, **merge), spark=delta_spark)  # first write creates the table
    update = tmp_path / "update.csv"
    update.write_text("order_id,amount,country\n2,20.0,us\n6,60.0,fr\n")
    run(pipeline(tmp_path, update, sinks=[delta_sink(path, **merge)]), spark=delta_spark)
    assert rows(delta_spark, path) == {1: (10.0, "IN"), 2: (20.0, "US"), None: (5.0, "DE"), 4: (7.5, "USA"), 5: (3.0, "GB"), 6: (60.0, "FR")}
    assert history(delta_spark, path) == ["WRITE", "MERGE"]
    run(pipeline(tmp_path, update, sinks=[delta_sink(path, **merge)]), spark=delta_spark)  # same rows again: no change
    assert read(delta_spark, path).count() == 6

    bad = pipeline(tmp_path, update, sinks=[delta_sink(path, mode="merge", merge_keys=["nope"])])
    with pytest.raises(TributaryError, match="merge_keys not in the data: nope"):
        run(bad, spark=delta_spark)
    dupes = tmp_path / "dupes.csv"
    dupes.write_text("order_id,amount,country\n2,1.0,us\n2,2.0,us\n")
    with pytest.raises(Exception, match="(?i)multiple source rows"):
        run(pipeline(tmp_path, dupes, sinks=[delta_sink(path, **merge)]), spark=delta_spark)


def test_merge_with_delete_condition_applies_cdc_changes(delta_spark, tmp_path):
    path = tmp_path / "t"
    opts = DeltaSinkOptions(path=str(path), mode="merge", merge_keys=["id"], delete_condition="s.op = 'D'")
    schema = "id INT, v STRING, op STRING"
    DeltaSink().write(ctx(delta_spark), delta_spark.createDataFrame([(1, "a", "I"), (2, "b", "I"), (3, "c", "I")], schema), opts)
    changes = [(1, "A", "U"), (2, "b", "D"), (4, "d", "I"), (5, "e", "D")]  # 5 never existed: not inserted
    DeltaSink().write(ctx(delta_spark), delta_spark.createDataFrame(changes, schema), opts)
    assert {r.id: r.v for r in read(delta_spark, path).collect()} == {1: "A", 3: "c", 4: "d"}


def test_schema_evolution_on_append_and_merge(delta_spark, tmp_path):
    path = tmp_path / "t"
    sink = DeltaSink()
    sink.write(ctx(delta_spark), delta_spark.createDataFrame([(1, "a")], "id INT, v STRING"), DeltaSinkOptions(path=str(path)))
    wider = delta_spark.createDataFrame([(2, "b", 1.5)], "id INT, v STRING, extra DOUBLE")
    with pytest.raises(Exception, match="(?i)schema mismatch"):
        sink.write(ctx(delta_spark), wider, DeltaSinkOptions(path=str(path)))
    sink.write(ctx(delta_spark), wider, DeltaSinkOptions(path=str(path), schema_evolution=True))
    assert {r.id: r.extra for r in read(delta_spark, path).collect()} == {1: None, 2: 1.5}

    widest = delta_spark.createDataFrame([(2, "B", 2.5, True), (3, "c", None, False)], "id INT, v STRING, extra DOUBLE, flag BOOLEAN")
    merge = DeltaSinkOptions(path=str(path), mode="merge", merge_keys=["id"])
    sink.write(ctx(delta_spark), widest, merge)  # without evolution, a merge ignores columns the table lacks
    assert read(delta_spark, path).columns == ["id", "v", "extra"]
    sink.write(ctx(delta_spark), widest, merge.model_copy(update={"schema_evolution": True}))
    assert {r.id: (r.v, r.extra, r.flag) for r in read(delta_spark, path).collect()} == {
        1: ("a", None, None), 2: ("B", 2.5, True), 3: ("c", None, False)}


def test_quarantine_can_go_to_delta(delta_spark, tmp_path, orders_csv):
    out = tmp_path / "out"
    doc = delta_pipeline(tmp_path, orders_csv, mode="merge", merge_keys=["order_id"])
    doc["quality"] = [{"id": "dq", "attach_to": "orders_clean", "action": "quarantine", "checks": ROW_CHECKS,
                       "quarantine_sink": {"type": "delta", "options": {"path": str(out / "bad")}}}]
    run(doc, spark=delta_spark)
    assert set(rows(delta_spark, tmp_path / "orders")) == {1, 5}
    assert {r.order_id: r._dq_failed_checks for r in read(delta_spark, out / "bad").collect()} == {
        2: ["range_2"], None: ["not_null_1"], 4: ["expression_3"]}


def test_preview_is_a_dry_run_unless_asked_to_write(delta_spark, tmp_path, orders_csv):
    path = tmp_path / "orders"
    doc = delta_pipeline(tmp_path, orders_csv, mode="merge", merge_keys=["order_id"])
    result = preview(doc, "orders_out", spark=delta_spark, limit=2)
    assert result.target == f"delta -> {path} (merge)"
    assert len(result.rows) == 2 and not result.written
    assert not path.exists()

    written = preview(doc, "orders_out", spark=delta_spark, limit=2, write_sinks=True)
    assert written.written and read(delta_spark, path).count() == 2


def test_streaming_writes_once_per_micro_batch_and_skips_replays(delta_spark, tmp_path, caplog):
    incoming = tmp_path / "in"
    incoming.mkdir()
    (incoming / "a.json").write_text('{"id": 1, "amount": 5}\n{"id": 2, "amount": 7}\n')
    path = tmp_path / "events"
    sink = delta_sink(path)
    sink["input"] = "events"
    doc = {
        "pipeline": {"id": "stream_to_delta", "mode": "streaming",
                     "streaming": {"checkpoint_location": str(tmp_path / "chk"), "trigger": "available_now"}},
        "sources": [{"id": "events", "type": "file", "stream": True, "options": {"path": str(incoming), "format": "json"},
                     "schema": {"kind": "ddl", "ddl": "id INT, amount INT"}}],
        "sinks": [sink],
    }
    assert not run(doc, spark=delta_spark).isActive
    assert {r.id for r in read(delta_spark, path).collect()} == {1, 2}
    (incoming / "b.json").write_text('{"id": 3, "amount": 1}\n')
    run(doc, spark=delta_spark)  # picks up where the checkpoint left off
    assert read(delta_spark, path).count() == 3

    opts = DeltaSinkOptions(path=str(path))
    batch = delta_spark.createDataFrame([(1, 5), (2, 7)], "id INT, amount INT")
    # The same micro-batch arriving again (a restart after the commit but before the checkpoint) is skipped...
    with caplog.at_level("WARNING", logger="tributary"):
        DeltaSink().write(ctx(delta_spark, batch_id=0, pipeline_id="stream_to_delta"), batch, opts)
    assert read(delta_spark, path).count() == 3 and "already has micro-batch 0" in caplog.text
    # ...a new batch, or the same batch id from another pipeline, is not.
    DeltaSink().write(ctx(delta_spark, batch_id=2, pipeline_id="stream_to_delta"), batch, opts)
    assert read(delta_spark, path).count() == 5
    DeltaSink().write(ctx(delta_spark, batch_id=2, pipeline_id="other_pipeline"), batch, opts)
    assert read(delta_spark, path).count() == 7
    # And the protection can be turned off.
    DeltaSink().write(ctx(delta_spark, batch_id=2, pipeline_id="other_pipeline"), batch, opts.model_copy(update={"skip_replayed_batches": False}))
    assert read(delta_spark, path).count() == 9
    # Batch runs (no batch id) never deduplicate.
    DeltaSink().write(ctx(delta_spark), batch, opts)
    assert read(delta_spark, path).count() == 11


def test_shared_session_gets_deltas_catalog_unless_another_is_set(delta_spark):
    configure_session(delta_spark)
    assert delta_spark.conf.get(SESSION_CATALOG_KEY) == DELTA_CATALOG
    configure_session(delta_spark)  # idempotent
    assert delta_spark.conf.get("spark.sql.extensions", None) is None  # still no extension on the session

    other = delta_spark.newSession()
    other.conf.set(SESSION_CATALOG_KEY, "org.apache.iceberg.spark.SparkSessionCatalog")
    with pytest.raises(TributaryError, match="session catalog .* is 'org.apache.iceberg.spark.SparkSessionCatalog'"):
        configure_session(other)
