from __future__ import annotations

import pytest

from conftest import EXAMPLES, ROW_CHECKS, pipeline
from tributary import SpecError, compile_pipeline


def messages(exc: SpecError) -> str:
    return "\n".join(str(i) for i in exc.issues)


@pytest.mark.parametrize("name,env", [("csv_to_parquet.yaml", None), ("kafka_to_parquet.yaml", "dev"),
                                      ("kafka_to_parquet.yaml", "prod"), ("file_stream_to_parquet.yaml", None),
                                      ("csv_to_delta.yaml", None), ("file_stream_to_delta.yaml", None),
                                      ("rest_api_to_parquet.yaml", "dev"), ("rest_api_to_parquet.yaml", "local"),
                                      ("rest_api_enrichment.yaml", None),
                                      ("rest_api_local_test.yaml", None), ("rest_api_enrichment_local_test.yaml", None),
                                      ("mongodb_to_parquet.yaml", "local"), ("elasticsearch_dummy_data.yaml", "local"),
                                      ("delta_to_parquet.yaml", "local")])
def test_examples_compile(name, env):
    compiled = compile_pipeline(EXAMPLES / name, env)
    assert compiled.spec.sinks


def test_env_vars_and_typed_options(tmp_path, orders_csv):
    compiled = compile_pipeline(pipeline(tmp_path, orders_csv))
    sink = compiled.spec.sinks[0]
    assert sink.options.path == f"{tmp_path}/out/orders"
    assert type(sink.options).__name__ == "FileSinkOptions"


def test_multiple_envs_require_choice(tmp_path, orders_csv):
    doc = pipeline(tmp_path, orders_csv, environments={"dev": {}, "prod": {}})
    with pytest.raises(SpecError, match="several environments"):
        compile_pipeline(doc)
    with pytest.raises(SpecError, match="'qa' is not defined"):
        compile_pipeline(doc, "qa")


def test_undefined_variable(tmp_path, orders_csv):
    doc = pipeline(tmp_path, orders_csv)
    doc["sinks"][0]["options"]["path"] = "${env.nope}/x"
    with pytest.raises(SpecError, match="undefined variable 'nope'"):
        compile_pipeline(doc)


def test_unknown_type_and_unknown_option(tmp_path, orders_csv):
    doc = pipeline(tmp_path, orders_csv)
    doc["sinks"][0]["type"] = "fiel"
    doc["sources"][0]["options"]["paht"] = "x"
    with pytest.raises(SpecError) as err:
        compile_pipeline(doc)
    text = messages(err.value)
    assert "fiel" in text and "paht" in text


def test_reference_errors_are_all_reported(tmp_path, orders_csv):
    doc = pipeline(tmp_path, orders_csv)
    doc["transforms"][0]["inputs"] = [{"node": "missing"}]
    doc["sinks"][0]["input"] = "also_missing"
    doc["sinks"].append({"id": "orders_out", "type": "console", "input": "orders_clean"})
    with pytest.raises(SpecError) as err:
        compile_pipeline(doc)
    text = messages(err.value)
    assert "unknown node 'missing'" in text
    assert "unknown node 'also_missing'" in text
    assert "duplicate id 'orders_out'" in text


def test_cycle_detected(tmp_path, orders_csv):
    doc = pipeline(tmp_path, orders_csv)
    doc["transforms"] = [
        {"id": "a", "type": "sql", "inputs": ["b"], "options": {"sql": "SELECT * FROM b"}},
        {"id": "b", "type": "sql", "inputs": ["a"], "options": {"sql": "SELECT * FROM a"}},
    ]
    doc["sinks"][0]["input"] = "a"
    with pytest.raises(SpecError, match="cycle detected among: a, b"):
        compile_pipeline(doc)


def test_quality_rules(tmp_path, orders_csv):
    doc = pipeline(tmp_path, orders_csv, quality=[
        {"id": "q1", "attach_to": "orders_clean", "before": "orders_out", "checks": ROW_CHECKS},
        {"id": "q2", "attach_to": "orders_clean", "action": "quarantine", "checks": ROW_CHECKS},
        {"id": "q3", "attach_to": "orders_out", "checks": ROW_CHECKS},
    ])
    with pytest.raises(SpecError) as err:
        compile_pipeline(doc)
    text = messages(err.value)
    assert "exactly one of 'attach_to'" in text
    assert "needs a 'quarantine_sink'" in text
    assert "'orders_out' is a sink" in text


def test_connections_are_merged_and_type_checked(tmp_path, orders_csv):
    doc = pipeline(tmp_path, orders_csv)
    doc["environments"]["dev"]["connections"] = {
        "lake": {"type": "file", "format": "parquet"},
        "broker": {"type": "kafka", "brokers": "x:9092"},
    }
    doc["sinks"][0]["connection"] = "lake"
    del doc["sinks"][0]["options"]["format"]
    compiled = compile_pipeline(doc)
    assert compiled.spec.sinks[0].options.format == "parquet"

    doc["sinks"][0]["connection"] = "broker"
    with pytest.raises(SpecError, match="connection 'broker' is 'kafka'"):
        compile_pipeline(doc)


def test_secrets_resolved_only_when_asked(tmp_path, orders_csv, monkeypatch):
    doc = pipeline(tmp_path, orders_csv)
    doc["sinks"][0]["options"]["path"] = "${secret:lake/path}"
    assert compile_pipeline(doc).spec.sinks[0].options.path == "${secret:lake/path}"
    with pytest.raises(SpecError, match="TRIBUTARY_SECRET_LAKE_PATH"):
        compile_pipeline(doc, resolve_secrets=True)
    monkeypatch.setenv("TRIBUTARY_SECRET_LAKE_PATH", "/secret/place")
    assert compile_pipeline(doc, resolve_secrets=True).spec.sinks[0].options.path == "/secret/place"


def test_streaming_needs_one_stream_source(tmp_path, orders_csv):
    doc = pipeline(tmp_path, orders_csv)
    doc["pipeline"]["mode"] = "streaming"
    doc["sources"].append({**doc["sources"][0], "id": "other_src"})
    with pytest.raises(SpecError, match="mark the streaming one"):
        compile_pipeline(doc)
    doc["sources"][0]["stream"] = True
    assert compile_pipeline(doc).mode == "streaming"


def test_error_paths_omit_union_tags(tmp_path, orders_csv):
    doc = pipeline(tmp_path, orders_csv)
    doc["sources"][0]["options"]["paht"] = "x"
    doc["sources"][0]["schema"] = {"kind": "ddl"}
    with pytest.raises(SpecError) as err:
        compile_pipeline(doc)
    paths = {i.path for i in err.value.issues}
    assert {"sources[0].options.paht", "sources[0].schema.ddl"} <= paths


def test_short_error_surfaces_the_jvm_exception():
    from tributary import short_error

    py4j = Exception(
        "An error occurred while calling o38.read.\n"
        ": java.lang.UnsupportedClassVersionError: org/apache/iceberg/spark/ExtendedParser has been compiled "
        "by a more recent version of the Java Runtime (class file version 55.0)\n"
        "\tat java.lang.ClassLoader.defineClass1(Native Method)\n"
    )
    assert short_error(py4j).startswith("java.lang.UnsupportedClassVersionError: org/apache/iceberg/spark/ExtendedParser")
    nested = Exception("An error occurred while calling o1.sql.\n: java.lang.ExceptionInInitializerError\n\tat x\nCaused by: java.io.IOException: disk full\n\tat y")
    assert short_error(nested) == "java.lang.ExceptionInInitializerError (Caused by: java.io.IOException: disk full)"
    assert short_error(ValueError("plain\nsecond line")) == "plain"
    assert short_error(ValueError("")) == "ValueError"


def test_unconnected_nodes_and_bad_names_say_what_to_fix(tmp_path, orders_csv):
    """What the canvas sends for a node nobody is connected to, and for a bad id."""
    doc = pipeline(tmp_path, orders_csv)
    doc["sinks"][0]["input"] = ""
    doc["transforms"][0]["inputs"] = []
    doc["sources"][0]["id"] = "1st source"
    with pytest.raises(SpecError) as err:
        compile_pipeline(doc)
    assert {(i.path, i.message) for i in err.value.issues} == {
        ("sinks[0].input", "not connected: link a source or transform to this sink"),
        ("transforms[0].inputs", "not connected: link at least one node to this transform"),
        ("sources[0].id", "'1st source' is not a valid name: letters, digits and _ only, not starting with a digit"),
    }
