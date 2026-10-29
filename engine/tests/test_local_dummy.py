"""Dummy data in the local environment: a source or sink with ``dummy.enabled`` stands in for
the real system there (and only there), so a pipeline can be drawn, previewed and run
without any of its systems being reachable."""

from __future__ import annotations

import copy
import json

import pytest

from tributary import analyze, compile_pipeline, preview, run
from tributary.errors import SpecError
from tributary.registry import default_registry

ORDERS = [
    {"order_id": 1, "amount": 10.0, "country": "in"},
    {"order_id": 2, "amount": -1.0, "country": "us"},
    {"order_id": 3, "amount": 7.5, "country": "de", "meta": {"channel": "web"}},
]
JSONL = "\n".join(json.dumps(r) for r in ORDERS)


def pipeline(sink_dummy: bool = True, **source_dummy) -> dict:
    """MongoDB in, Elasticsearch out, with none of their options given: only dummy data
    makes this pipeline runnable, and only in the local environment."""
    return {
        "version": 1,
        "pipeline": {"id": "local_dummy"},
        "environments": {"local": {}, "prod": {}},
        "sources": [
            {"id": "orders", "type": "mongodb", "connection": "nowhere", "options": {}, "dummy": {"enabled": True, "data": JSONL, **source_dummy}}
        ],
        "transforms": [
            {
                "id": "orders_clean",
                "type": "sql",
                "inputs": [{"node": "orders"}],
                "options": {"sql": "SELECT order_id, amount, upper(country) AS country FROM orders WHERE amount > 0"},
            }
        ],
        "sinks": [{"id": "orders_out", "type": "elasticsearch", "input": "orders_clean", "options": {}, "dummy": {"enabled": sink_dummy}}],
    }


def test_the_dummy_plugins_are_not_in_the_catalog():
    catalog = default_registry().catalog()
    assert "dummy" not in {p["type"] for p in catalog["source"]}
    assert "dummy" not in {p["type"] for p in catalog["sink"]}


def test_local_environment_swaps_dummy_nodes_and_needs_none_of_their_options():
    compiled = compile_pipeline(pipeline(), env="local")
    source, sink = compiled.spec.sources[0], compiled.spec.sinks[0]
    assert (source.type, source.options.stands_for, len(source.options.records)) == ("dummy", "mongodb", 3)
    assert (sink.type, sink.options.stands_for) == ("dummy", "elasticsearch")
    assert source.connection is None  # the (undefined) connection is not looked up
    assert compiled.warnings == []


def test_other_environments_ignore_dummy_data_and_need_the_real_options():
    with pytest.raises(SpecError) as error:
        compile_pipeline(pipeline(), env="prod")
    paths = {i.path for i in error.value.issues}
    assert "sources[0].connection" in paths  # the real node wants its connection again
    doc = pipeline()
    doc["sources"][0] = {"id": "orders", "type": "file", "options": {"path": "/data/orders.json", "format": "json"}, "dummy": doc["sources"][0]["dummy"]}
    doc["sinks"][0] = {"id": "orders_out", "type": "console", "input": "orders_clean", "dummy": {"enabled": True}}
    compiled = compile_pipeline(doc, env="prod")
    assert [s.type for s in compiled.spec.sources] == ["file"] and [s.type for s in compiled.spec.sinks] == ["console"]
    assert compiled.warnings == [
        "sources[0] (orders): dummy data is only used in the 'local' environment",
        "sinks[0] (orders_out): dummy data is only used in the 'local' environment",
    ]


def test_a_disabled_dummy_keeps_its_data_but_runs_for_real():
    doc = pipeline(sink_dummy=False, enabled=False)
    with pytest.raises(SpecError) as error:
        compile_pipeline(doc, env="local")
    assert "sources[0].connection" in {i.path for i in error.value.issues}


@pytest.mark.parametrize(
    "data, message",
    [
        ('{"a": 1}\n{"a": ', "line 2 is not JSON: Expecting value"),
        ('{"a": 1}\n[1, 2]', "record 2 is not a JSON object"),
        ("\n".join('{"a": %d}' % i for i in range(1001)), "at most 1000 records (1001 given)"),
        ("", "upload or paste at least one record (JSON Lines)"),
    ],
)
def test_dummy_data_problems_are_reported_on_the_field(data, message):
    with pytest.raises(SpecError) as error:
        compile_pipeline(pipeline(data=data), env="local")
    assert [(i.path, i.message) for i in error.value.issues] == [("sources[0].dummy.data", message)]


def test_data_can_be_a_list_or_a_json_array_and_blank_lines_are_skipped():
    for data in (ORDERS, json.dumps(ORDERS), JSONL.replace("\n", "\n\n") + "\n"):
        assert len(compile_pipeline(pipeline(data=data), env="local").spec.sources[0].options.records) == 3
    big = "\n".join('{"a": %d}' % i for i in range(1000))
    assert len(compile_pipeline(pipeline(data=big), env="local").spec.sources[0].options.records) == 1000


def test_compiling_leaves_the_callers_document_alone():
    doc = pipeline()
    before = copy.deepcopy(doc)
    compile_pipeline(doc, env="local")
    assert doc == before


def test_preview_and_analysis_show_the_dummy_records(spark):
    doc = pipeline()
    source = preview(doc, "orders", env="local", spark=spark)
    assert sorted(r["order_id"] for r in source.rows) == [1, 2, 3]
    sink = preview(doc, "orders_out", env="local", spark=spark)
    assert sorted((r["order_id"], r["country"]) for r in sink.rows) == [(1, "IN"), (3, "DE")]
    assert sink.written is False and sink.target == "dummy elasticsearch sink: nothing is written (local environment)"
    report = analyze(compile_pipeline(doc, env="local"), spark=spark)
    assert all(a.ok for a in report.values()), {k: a.messages for k, a in report.items()}
    fields = {f["name"]: f["type"] for f in report["orders"].schema["fields"]}
    assert fields["order_id"] == "long" and fields["meta"]["type"] == "struct"


def test_a_declared_schema_types_the_dummy_records(spark):
    doc = pipeline()
    doc["sources"][0]["schema"] = {"kind": "ddl", "ddl": "order_id INT, amount DECIMAL(10,2), country STRING"}
    rows = preview(doc, "orders", env="local", spark=spark).rows
    assert set(rows[0]) == {"order_id", "amount", "country"}
    report = analyze(compile_pipeline(doc, env="local"), spark=spark)
    assert {f["name"]: f["type"] for f in report["orders"].schema["fields"]} == {"order_id": "integer", "amount": "decimal(10,2)", "country": "string"}


def test_a_run_writes_nothing_to_a_dummy_sink_and_real_sinks_still_get_the_dummy_rows(spark, tmp_path):
    doc = pipeline()
    doc["sinks"].append(
        {"id": "orders_file", "type": "file", "input": "orders_clean", "options": {"path": str(tmp_path / "out"), "format": "json"}}
    )
    run(doc, env="local", spark=spark)
    written = spark.read.json(str(tmp_path / "out"))
    assert sorted(r.order_id for r in written.collect()) == [1, 3]


def test_a_streaming_pipeline_reads_the_dummy_records_as_one_micro_batch(spark, tmp_path):
    doc = pipeline()
    doc["pipeline"] = {"id": "local_dummy_stream", "mode": "streaming", "streaming": {"checkpoint_location": str(tmp_path / "chk"), "trigger": "available_now"}}
    doc["sinks"] = [{"id": "orders_file", "type": "file", "input": "orders_clean", "options": {"path": str(tmp_path / "out"), "format": "json"}}]
    run(doc, env="local", spark=spark)
    assert sorted(r.order_id for r in spark.read.json(str(tmp_path / "out")).collect()) == [1, 3]


def test_use_dummies_checks_everything_but_the_dummy_nodes_in_any_environment():
    """What saving a version relies on: in prod the dummy nodes lack their real settings,
    but with use_dummies the rest of the pipeline is still checked."""
    with pytest.raises(SpecError):
        compile_pipeline(pipeline(), env="prod")
    compiled = compile_pipeline(pipeline(), env="prod", use_dummies=True)
    assert [s.type for s in compiled.spec.sources] == ["dummy"] and compiled.warnings == []
    broken = pipeline()
    broken["transforms"][0]["inputs"] = [{"node": "missing"}]
    with pytest.raises(SpecError) as error:
        compile_pipeline(broken, env="prod", use_dummies=True)
    assert error.value.issues and all(i.path.startswith("transforms[0]") for i in error.value.issues)
    # And use_dummies=False keeps a local pipeline's nodes real.
    with pytest.raises(SpecError):
        compile_pipeline(pipeline(), env="local", use_dummies=False)
