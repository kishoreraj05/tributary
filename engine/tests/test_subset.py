"""Previews compile only the tested node and its upstream: the rest of the canvas may be unfinished."""

from __future__ import annotations

import pytest

from conftest import pipeline
from tributary import SpecError, preview
from tributary.spec import restrict_to
from tributary.spec.subset import PREVIEW_SINK_ID

UNFINISHED_DEEQU = {"id": "orders_dq", "type": "deequ", "inputs": ["orders_clean"], "options": {}}  # no runner chosen yet


def ids(doc, section):
    return [n["id"] for n in doc.get(section) or []]


def test_restrict_keeps_the_node_its_upstream_and_attached_gates(tmp_path, orders_csv):
    doc = pipeline(tmp_path, orders_csv, quality=[
        {"id": "src_dq", "attach_to": "orders_src", "checks": [{"type": "not_null", "column": "order_id"}]},
        {"id": "clean_dq", "attach_to": "orders_clean", "checks": [{"type": "not_null", "column": "order_id"}]},
        {"id": "out_dq", "before": "orders_out", "checks": [{"type": "not_null", "column": "order_id"}]},
    ])
    doc["transforms"].append(UNFINISHED_DEEQU)
    doc["sinks"].append({"id": "dq_out", "type": "console", "input": "orders_dq"})

    source_only = restrict_to(doc, "orders_src")
    assert ids(source_only, "sources") == ["orders_src"]
    assert ids(source_only, "transforms") == [] and ids(source_only, "quality") == ["src_dq"]
    assert ids(source_only, "sinks") == [PREVIEW_SINK_ID] and source_only["sinks"][0]["input"] == "orders_src"

    clean = restrict_to(doc, "orders_clean")
    assert ids(clean, "transforms") == ["orders_clean"] and ids(clean, "quality") == ["src_dq", "clean_dq"]
    assert ids(clean, "sinks") == [PREVIEW_SINK_ID]

    sink = restrict_to(doc, "orders_out")
    assert ids(sink, "sinks") == ["orders_out"] and ids(sink, "quality") == ["src_dq", "clean_dq", "out_dq"]

    gate = restrict_to(doc, "out_dq")  # a 'before' gate brings its sink, no stand-in needed
    assert ids(gate, "sinks") == ["orders_out"] and ids(gate, "quality") == ["src_dq", "clean_dq", "out_dq"]
    gate = restrict_to(doc, "clean_dq")  # an 'attach_to' gate reads its node through the stand-in
    assert ids(gate, "sinks") == [PREVIEW_SINK_ID] and gate["sinks"][0]["input"] == "orders_clean"

    assert restrict_to(doc, "nope") == doc  # unknown ids: nothing to restrict, validation reports it
    assert ids(doc, "transforms") == ["orders_clean", "orders_dq"]  # the original is untouched


def test_preview_ignores_problems_downstream_but_not_upstream(spark, tmp_path, orders_csv):
    doc = pipeline(tmp_path, orders_csv)
    doc["transforms"].append(UNFINISHED_DEEQU)
    doc["sinks"].append({"id": "dq_out", "type": "console", "input": "orders_dq"})

    # The source and the SQL node test fine although the deequ node downstream has no checks yet.
    assert len(preview(doc, "orders_src", spark=spark, limit=3).rows) == 3
    assert len(preview(doc, "orders_clean", spark=spark, limit=2).rows) == 2
    # Sample data for a source the preview leaves out is simply not needed.
    assert len(preview(doc, "orders_src", spark=spark, limit=3, sample_data={"other": "/nope.csv"}).rows) == 3

    # The unfinished node itself, and anything downstream of it, still report its problem.
    for node in ("orders_dq", "dq_out"):
        with pytest.raises(SpecError) as exc:
            preview(doc, node, spark=spark)
        assert [(i.path, i.message) for i in exc.value.issues] == [("transforms[1].options.runner", "Field required")]

    # A problem upstream blocks: the SQL node cannot run on a source that is not configured.
    doc["sources"][0]["options"].pop("path")
    with pytest.raises(SpecError, match=r"sources\[0\].options.path"):
        preview(doc, "orders_clean", spark=spark)
