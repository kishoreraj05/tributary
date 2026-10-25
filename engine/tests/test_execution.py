from __future__ import annotations

import pytest

from conftest import ROW_CHECKS, pipeline
from tributary import QualityFailure, analyze, compile_pipeline, preview, run


def quality(action, **extra):
    return [{"id": "dq", "attach_to": "orders_clean", "action": action, "checks": ROW_CHECKS, **extra}]


def test_quarantine_splits_rows_and_emits_results(spark, tmp_path, orders_csv):
    out = tmp_path / "out"
    doc = pipeline(tmp_path, orders_csv, quality=quality(
        "quarantine",
        results_sinks=[{"type": "file", "options": {"path": str(out / "dq"), "format": "json"}}],
        quarantine_sink={"type": "file", "options": {"path": str(out / "bad"), "mode": "overwrite"}},
    ))
    summary = run(doc, spark=spark)
    assert summary.sinks_written == ["orders_out"]
    good = {r.order_id for r in spark.read.parquet(str(out / "orders")).collect()}
    assert good == {1, 5}
    bad = {r.order_id: r._dq_failed_checks for r in spark.read.parquet(str(out / "bad")).collect()}
    assert bad == {2: ["range_2"], None: ["not_null_1"], 4: ["expression_3"]}
    results = {r.check_id: r.failed_rows for r in spark.read.json(str(out / "dq")).collect()}
    assert results == {"not_null_1": 1, "range_2": 1, "expression_3": 1}


def test_drop_filters_without_quarantine(spark, tmp_path, orders_csv):
    run(pipeline(tmp_path, orders_csv, quality=quality("drop")), spark=spark)
    assert spark.read.parquet(str(tmp_path / "out" / "orders")).count() == 2


def test_observe_keeps_all_rows(spark, tmp_path, orders_csv):
    summary = run(pipeline(tmp_path, orders_csv, quality=quality("observe")), spark=spark)
    assert spark.read.parquet(str(tmp_path / "out" / "orders")).count() == 5
    assert [r.passed for r in summary.quality] == [False, False, False]


def test_fail_aborts_before_any_sink_writes(spark, tmp_path, orders_csv):
    with pytest.raises(QualityFailure, match="dq"):
        run(pipeline(tmp_path, orders_csv, quality=quality("fail")), spark=spark)
    assert not (tmp_path / "out" / "orders").exists()


def test_dataset_checks_and_thresholds(spark, tmp_path, orders_csv):
    checks = [
        {"type": "unique", "columns": ["country"]},
        {"type": "row_count", "min": 1, "max": 3},
        {"type": "range", "column": "amount", "min": 0, "max_failed_ratio": 0.25},
    ]
    doc = pipeline(tmp_path, orders_csv, quality=[{"id": "dq", "attach_to": "orders_src", "checks": checks}])
    results = {r.check_type: r for r in run(doc, spark=spark).quality}
    assert results["unique"].observed_value == 0 and results["unique"].passed
    assert results["row_count"].observed_value == 5 and not results["row_count"].passed
    assert results["range"].failed_rows == 1 and results["range"].passed  # 1/5 <= 25%


def test_parallel_sinks(spark, tmp_path, orders_csv):
    doc = pipeline(tmp_path, orders_csv, execution={"parallel_sinks": True})
    doc["sinks"].append({"id": "copy", "type": "file", "input": "orders_clean",
                         "options": {"path": str(tmp_path / "copy"), "mode": "overwrite"}})
    assert sorted(run(doc, spark=spark).sinks_written) == ["copy", "orders_out"]
    assert spark.read.parquet(str(tmp_path / "copy")).count() == 5


def test_preview_node_with_gate_results(spark, tmp_path, orders_csv):
    result = preview(pipeline(tmp_path, orders_csv, quality=quality("quarantine", quarantine_sink={"type": "console"})),
                     "orders_clean", spark=spark, limit=3)
    assert len(result.rows) == 3
    assert [f["name"] for f in result.schema["fields"]] == ["order_id", "amount", "country"]
    assert {q["check_id"] for q in result.quality} == {"not_null_1", "range_2", "expression_3"}
    assert not (tmp_path / "out").exists()  # previews have no side effects


def test_preview_sink_is_dry_run_and_sample_data_overrides_source(spark, tmp_path, orders_csv):
    sample = tmp_path / "sample.csv"
    sample.write_text("order_id,amount,country\n42,1.0,in\n")
    result = preview(pipeline(tmp_path, orders_csv), "orders_out", spark=spark, sample_data={"orders_src": str(sample)})
    assert result.rows == [{"order_id": 42, "amount": 1.0, "country": "IN"}]
    assert result.target.startswith("parquet -> ") and not result.written
    assert not (tmp_path / "out").exists()


def test_analyze_finds_bad_columns_without_data(spark, tmp_path, orders_csv):
    doc = pipeline(tmp_path, orders_csv)
    doc["transforms"][0]["options"]["sql"] = "SELECT order_id, amout FROM orders"
    doc["sources"][0]["options"]["path"] = str(tmp_path / "does_not_exist.csv")  # never read
    report = analyze(compile_pipeline(doc), spark=spark)
    assert report["orders_src"].ok
    assert not report["orders_clean"].ok and "amout" in report["orders_clean"].error
    assert "skipped" in report["orders_out"].error


def test_preview_renders_timestamps_in_session_time_zone(spark, tmp_path):
    source = tmp_path / "events.csv"
    source.write_text("id,ts\n1,2026-10-01 10:00:00\n")
    doc = {
        "pipeline": {"id": "ts_test"},
        "sources": [{"id": "src", "type": "file", "options": {"path": str(source), "format": "csv", "read_options": {"header": True}},
                     "schema": {"kind": "ddl", "ddl": "id INT, ts TIMESTAMP"}}],
        "sinks": [{"id": "out", "type": "console", "input": "src"}],
    }
    result = preview(doc, "src", spark=spark)
    assert result.rows == [{"id": 1, "ts": "2026-10-01 10:00:00"}]
    assert result.schema["fields"][1]["type"] == "timestamp"  # the schema keeps the real type
