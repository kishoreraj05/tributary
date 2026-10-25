from __future__ import annotations

from conftest import ROW_CHECKS, pipeline
from tributary import compile_pipeline
from tributary.planner import build_plan


def plan_for(doc):
    return build_plan(compile_pipeline(doc))


def two_sinks(doc):
    doc["sinks"].append({"id": "orders_console", "type": "console", "input": "orders_clean"})
    return doc


def test_fan_out_persists_once(tmp_path, orders_csv):
    plan = plan_for(two_sinks(pipeline(tmp_path, orders_csv)))
    clean = plan.steps["orders_clean"]
    assert clean.reads == 2 and clean.cache
    assert not plan.steps["orders_src"].cache
    assert plan.order[-2:] == ["orders_out", "orders_console"]  # sinks last


def test_single_consumer_is_not_cached(tmp_path, orders_csv):
    plan = plan_for(pipeline(tmp_path, orders_csv))
    assert plan.steps["orders_clean"].reads == 1
    assert not plan.steps["orders_clean"].cache


def test_cache_never_overrides(tmp_path, orders_csv):
    doc = two_sinks(pipeline(tmp_path, orders_csv))
    doc["transforms"][0]["cache"] = "never"
    assert not plan_for(doc).steps["orders_clean"].cache


def test_observe_gate_counts_as_extra_read(tmp_path, orders_csv):
    doc = pipeline(tmp_path, orders_csv, quality=[{"id": "q", "attach_to": "orders_clean", "checks": ROW_CHECKS}])
    plan = plan_for(doc)
    assert plan.steps["orders_out"].inputs == {"input": "q"}
    assert plan.steps["orders_clean"].reads == 2 and plan.steps["orders_clean"].cache


def test_quarantine_gate_persists_flagged_rows_not_upstream(tmp_path, orders_csv):
    doc = two_sinks(pipeline(tmp_path, orders_csv, quality=[{
        "id": "q", "attach_to": "orders_clean", "action": "quarantine", "checks": ROW_CHECKS,
        "quarantine_sink": {"type": "console"},
    }]))
    plan = plan_for(doc)
    assert not plan.steps["orders_clean"].cache
    gate = plan.steps["q"]
    assert gate.persist_flags and not gate.passthrough
    assert {plan.steps[s].inputs["input"] for s in ("orders_out", "orders_console")} == {"q"}


def test_before_gate_only_feeds_its_sink(tmp_path, orders_csv):
    doc = two_sinks(pipeline(tmp_path, orders_csv, quality=[
        {"id": "q", "before": "orders_out", "action": "drop", "checks": ROW_CHECKS},
    ]))
    plan = plan_for(doc)
    assert plan.steps["orders_out"].inputs == {"input": "q"}
    assert plan.steps["orders_console"].inputs == {"input": "orders_clean"}
    assert plan.steps["q"].attached_to == "orders_clean"


def test_explain_mentions_decisions(tmp_path, orders_csv):
    text = plan_for(two_sinks(pipeline(tmp_path, orders_csv))).explain()
    assert "persist MEMORY_AND_DISK (2 reads)" in text
