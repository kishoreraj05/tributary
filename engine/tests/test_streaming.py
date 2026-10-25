from __future__ import annotations

from tributary import run


def test_file_stream_runs_plan_per_micro_batch(spark, tmp_path):
    incoming = tmp_path / "in"
    incoming.mkdir()
    (incoming / "a.json").write_text('{"id": 1, "amount": 5}\n{"id": 2, "amount": -1}\n')
    lookup = tmp_path / "lookup.csv"
    lookup.write_text("id,label\n1,one\n2,two\n")
    out = tmp_path / "out"
    doc = {
        "pipeline": {
            "id": "stream_test",
            "mode": "streaming",
            "streaming": {"checkpoint_location": str(tmp_path / "chk"), "trigger": "available_now"},
        },
        "sources": [
            {"id": "events", "type": "file", "stream": True, "options": {"path": str(incoming), "format": "json"},
             "schema": {"kind": "ddl", "ddl": "id INT, amount INT"}},
            {"id": "labels", "type": "file",
             "options": {"path": str(lookup), "format": "csv", "read_options": {"header": True, "inferSchema": True}}},
        ],
        "transforms": [{
            "id": "joined", "type": "sql", "inputs": ["events", "labels"],
            "options": {"sql": "SELECT e.id, e.amount, l.label FROM events e JOIN labels l ON e.id = l.id"},
        }],
        "quality": [{
            "id": "dq", "attach_to": "joined", "action": "drop",
            "checks": [{"type": "range", "column": "amount", "min": 0}],
            "results_sinks": [{"type": "file", "options": {"path": str(out / "dq")}}],
        }],
        "sinks": [{"id": "sink", "type": "file", "input": "joined", "options": {"path": str(out / "data")}}],
    }
    query = run(doc, spark=spark)
    assert not query.isActive
    rows = spark.read.parquet(str(out / "data")).collect()
    assert [(r.id, r.label) for r in rows] == [(1, "one")]
    [result] = spark.read.json(str(out / "dq")).collect()
    assert result.batch_id == 0 and result.failed_rows == 1
