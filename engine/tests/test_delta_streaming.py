"""Streaming output into Delta: one commit per micro-batch, restarts that pick up where the
checkpoint left off, a crash between the write and the checkpoint commit, a continuously
running query, and a quality gate whose quarantine is a Delta table."""

from __future__ import annotations

import itertools
import json
import os
import time
from pathlib import Path

import pytest

from tributary import run

LIMIT = 120  # seconds to wait for a running query to write


def stream_doc(tmp_path: Path, *sinks: dict, trigger: str = "available_now", quality: list | None = None) -> dict:
    return {
        "pipeline": {
            "id": "events_to_delta",
            "mode": "streaming",
            "streaming": {"checkpoint_location": str(tmp_path / "chk"), "trigger": trigger, "interval": "1 second"},
        },
        "sources": [{
            "id": "events", "type": "file", "stream": True,
            "options": {"path": str(tmp_path / "in"), "format": "json", "max_files_per_trigger": 1},
            "schema": {"kind": "ddl", "ddl": "id INT, amount INT, op STRING"},
        }],
        "transforms": [{
            "id": "clean", "type": "sql", "inputs": ["events"],
            "options": {"sql": "SELECT id, amount, coalesce(op, 'I') AS op FROM events"},
        }],
        "quality": quality or [],
        "sinks": list(sinks),
    }


def delta(id: str, path: Path, **options) -> dict:
    return {"id": id, "type": "delta", "input": "clean", "options": {"path": str(path), **options}}


_ORDER = itertools.count()


def drop_file(tmp_path: Path, name: str, *records: dict) -> None:
    """The file source reads files in modification-time order; files written in the same
    millisecond would be read in any order, so each gets a later timestamp than the last."""
    (tmp_path / "in").mkdir(exist_ok=True)
    path = tmp_path / "in" / name
    path.write_text("".join(json.dumps(r) + "\n" for r in records))
    stamp = time.time() - 60 + next(_ORDER)
    os.utime(path, (stamp, stamp))


def crash_before_checkpoint_commit(tmp_path: Path, batch_id: int) -> None:
    """Structured Streaming writes offsets/N before a micro-batch and commits/N after it.
    Removing commits/N (and the checksum Hadoop keeps next to it) leaves the checkpoint as
    a crash in between would: the next start replays batch N with the same id."""
    commits = tmp_path / "chk" / "commits"
    assert (commits / str(batch_id)).exists()
    (commits / str(batch_id)).unlink()
    (commits / f".{batch_id}.crc").unlink(missing_ok=True)


def read(spark, path: Path):
    return spark.read.format("delta").load(str(path))


def rows_or_none(spark, path: Path) -> dict | None:
    """{id: amount}, or None while the table does not exist yet (or is being created)."""
    from pyspark.errors import AnalysisException

    try:
        return {r.id: r.amount for r in read(spark, path).collect()}
    except AnalysisException:
        return None


def commits(spark, path: Path) -> list[tuple[int, str]]:
    from delta.tables import DeltaTable

    return [(r.version, r.operation) for r in DeltaTable.forPath(spark, str(path)).history().orderBy("version").collect()]


def wait_until(predicate, what: str) -> None:
    deadline = time.monotonic() + LIMIT
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.5)
    raise AssertionError(f"timed out waiting for {what}")


def test_each_micro_batch_is_one_commit_in_append_and_merge_tables(delta_spark, tmp_path):
    """Two files -> two micro-batches -> two commits per table. The append table keeps
    every row; the merge table keeps the latest version of each key."""
    drop_file(tmp_path, "b0.json", {"id": 1, "amount": 5}, {"id": 2, "amount": 7})
    drop_file(tmp_path, "b1.json", {"id": 2, "amount": 8}, {"id": 3, "amount": 1})
    raw, latest = tmp_path / "raw", tmp_path / "latest"
    doc = stream_doc(tmp_path, delta("raw", raw), delta("latest", latest, mode="merge", merge_keys=["id"]))

    query = run(doc, spark=delta_spark)
    assert not query.isActive
    assert sorted((r.id, r.amount) for r in read(delta_spark, raw).collect()) == [(1, 5), (2, 7), (2, 8), (3, 1)]
    assert {r.id: r.amount for r in read(delta_spark, latest).collect()} == {1: 5, 2: 8, 3: 1}
    assert commits(delta_spark, raw) == [(0, "WRITE"), (1, "WRITE")]
    assert commits(delta_spark, latest) == [(0, "WRITE"), (1, "MERGE")]  # the first batch created the table

    # A restart with the same checkpoint only processes what is new.
    drop_file(tmp_path, "b2.json", {"id": 3, "amount": 2})
    run(doc, spark=delta_spark)
    assert read(delta_spark, raw).count() == 5
    assert {r.id: r.amount for r in read(delta_spark, latest).collect()} == {1: 5, 2: 8, 3: 2}
    assert commits(delta_spark, raw) == [(0, "WRITE"), (1, "WRITE"), (2, "WRITE")]
    # Nothing new: a run still starts and stops cleanly without touching the tables.
    run(doc, spark=delta_spark)
    assert commits(delta_spark, raw)[-1] == (2, "WRITE") and commits(delta_spark, latest)[-1] == (2, "MERGE")


def test_crash_after_the_write_but_before_the_checkpoint_commit_does_not_duplicate(delta_spark, tmp_path):
    """Structured Streaming writes offsets/N before a batch and commits/N after it. A crash
    in between replays batch N on restart, with the same batch id: the append sink skips it
    (Delta's txnAppId/txnVersion) and the merge sink is idempotent anyway."""
    drop_file(tmp_path, "b0.json", {"id": 1, "amount": 5})
    drop_file(tmp_path, "b1.json", {"id": 2, "amount": 7})
    raw, latest = tmp_path / "raw", tmp_path / "latest"
    doc = stream_doc(tmp_path, delta("raw", raw), delta("latest", latest, mode="merge", merge_keys=["id"]))
    run(doc, spark=delta_spark)
    assert read(delta_spark, raw).count() == 2

    # "Crash": the last micro-batch's work is committed to the tables but not to the checkpoint.
    crash_before_checkpoint_commit(tmp_path, 1)
    run(doc, spark=delta_spark)  # replays batch 1
    assert sorted((r.id, r.amount) for r in read(delta_spark, raw).collect()) == [(1, 5), (2, 7)]
    assert {r.id: r.amount for r in read(delta_spark, latest).collect()} == {1: 5, 2: 7}
    assert commits(delta_spark, raw) == [(0, "WRITE"), (1, "WRITE")]  # the replay added no commit
    assert (tmp_path / "chk" / "commits" / "1").exists()  # and the checkpoint caught up


def test_replay_protection_can_be_turned_off(delta_spark, tmp_path):
    """The opt-out exists for replaying a stream from a fresh checkpoint into an existing table;
    with it off, a replayed batch is written again."""
    drop_file(tmp_path, "b0.json", {"id": 1, "amount": 5})
    raw = tmp_path / "raw"
    doc = stream_doc(tmp_path, delta("raw", raw, skip_replayed_batches=False))
    run(doc, spark=delta_spark)
    crash_before_checkpoint_commit(tmp_path, 0)
    run(doc, spark=delta_spark)
    assert read(delta_spark, raw).count() == 2
    assert commits(delta_spark, raw) == [(0, "WRITE"), (1, "WRITE")]


def test_fresh_checkpoint_into_an_existing_table_is_the_documented_caveat(delta_spark, tmp_path):
    """Batch ids restart at 0 with a new checkpoint, so Delta sees versions it already has and
    skips them; the option description tells users to turn the protection off in that case."""
    drop_file(tmp_path, "b0.json", {"id": 1, "amount": 5})
    raw = tmp_path / "raw"
    run(stream_doc(tmp_path, delta("raw", raw)), spark=delta_spark)
    assert read(delta_spark, raw).count() == 1

    fresh = dict(stream_doc(tmp_path, delta("raw", raw)))
    fresh["pipeline"]["streaming"]["checkpoint_location"] = str(tmp_path / "chk2")
    run(fresh, spark=delta_spark)  # batch 0 again, same pipeline id: skipped
    assert read(delta_spark, raw).count() == 1
    fresh["pipeline"]["streaming"]["checkpoint_location"] = str(tmp_path / "chk3")
    fresh["sinks"][0]["options"]["skip_replayed_batches"] = False
    run(fresh, spark=delta_spark)
    assert read(delta_spark, raw).count() == 2


def test_continuous_query_writes_as_files_arrive(delta_spark, tmp_path):
    """A processing_time trigger keeps the query running; files dropped while it runs show up
    in the Delta table one commit per micro-batch, and the query stops cleanly."""
    drop_file(tmp_path, "b0.json", {"id": 1, "amount": 5})
    latest = tmp_path / "latest"
    doc = stream_doc(tmp_path, delta("latest", latest, mode="merge", merge_keys=["id"]), trigger="processing_time")
    query = run(doc, spark=delta_spark, await_termination=False)
    try:
        assert query.isActive
        wait_until(lambda: rows_or_none(delta_spark, latest) == {1: 5}, "the first batch")
        drop_file(tmp_path, "b1.json", {"id": 1, "amount": 6}, {"id": 2, "amount": 7})
        wait_until(lambda: rows_or_none(delta_spark, latest) == {1: 6, 2: 7}, "the second batch")
        drop_file(tmp_path, "b2.json", {"id": 2, "amount": 9})
        wait_until(lambda: rows_or_none(delta_spark, latest) == {1: 6, 2: 9}, "the third batch")
        assert commits(delta_spark, latest) == [(0, "WRITE"), (1, "MERGE"), (2, "MERGE")]
        progress = query.recentProgress
        assert progress and progress[-1]["batchId"] >= 2
    finally:
        query.stop()
    assert not query.isActive


def test_quality_gate_per_micro_batch_with_delta_quarantine(delta_spark, tmp_path):
    """Each micro-batch is checked; failing rows go to a Delta quarantine table with the checks
    they failed, good rows are merged, and the results carry the batch id."""
    drop_file(tmp_path, "b0.json", {"id": 1, "amount": 5}, {"id": 2, "amount": -1})
    drop_file(tmp_path, "b1.json", {"id": 3, "amount": 4}, {"id": None, "amount": 2})
    good, bad, results = tmp_path / "good", tmp_path / "bad", tmp_path / "dq"
    quality = [{
        "id": "dq", "attach_to": "clean", "action": "quarantine",
        "checks": [{"type": "not_null", "column": "id"}, {"type": "range", "column": "amount", "min": 0}],
        "results_sinks": [{"type": "file", "options": {"path": str(results), "format": "json"}}],
        "quarantine_sink": {"type": "delta", "options": {"path": str(bad)}},
    }]
    run(stream_doc(tmp_path, delta("good", good, mode="merge", merge_keys=["id"]), quality=quality), spark=delta_spark)

    assert {r.id: r.amount for r in read(delta_spark, good).collect()} == {1: 5, 3: 4}
    quarantined = {r.id: r._dq_failed_checks for r in read(delta_spark, bad).collect()}
    assert quarantined == {2: ["range_2"], None: ["not_null_1"]}
    assert commits(delta_spark, bad) == [(0, "WRITE"), (1, "WRITE")]  # one quarantine commit per batch
    by_batch = {}
    for r in delta_spark.read.json(str(results)).collect():
        by_batch.setdefault(r.batch_id, {})[r.check_id] = r.failed_rows
    assert by_batch == {0: {"not_null_1": 0, "range_2": 1}, 1: {"not_null_1": 1, "range_2": 0}}


def test_delete_condition_applies_change_events_in_order(delta_spark, tmp_path):
    """A change feed: inserts, updates and deletes arrive over several micro-batches and the
    merge table ends up at the current state."""
    drop_file(tmp_path, "b0.json", {"id": 1, "amount": 5, "op": "I"}, {"id": 2, "amount": 7, "op": "I"})
    drop_file(tmp_path, "b1.json", {"id": 1, "amount": 6, "op": "U"}, {"id": 2, "amount": 7, "op": "D"})
    drop_file(tmp_path, "b2.json", {"id": 2, "amount": 1, "op": "I"}, {"id": 9, "amount": 1, "op": "D"})
    state = tmp_path / "state"
    run(stream_doc(tmp_path, delta("state", state, mode="merge", merge_keys=["id"], delete_condition="s.op = 'D'")), spark=delta_spark)
    assert {r.id: r.amount for r in read(delta_spark, state).collect()} == {1: 6, 2: 1}
    assert commits(delta_spark, state) == [(0, "WRITE"), (1, "MERGE"), (2, "MERGE")]


@pytest.mark.parametrize("mode", ["overwrite", "overwrite_partitions"])
def test_overwrite_modes_in_a_stream_replace_per_micro_batch(delta_spark, tmp_path, mode):
    """overwrite keeps only the last micro-batch; overwrite_partitions keeps the partitions
    the last batch did not touch (a 'latest snapshot per key' table)."""
    drop_file(tmp_path, "b0.json", {"id": 1, "amount": 5}, {"id": 2, "amount": 7})
    drop_file(tmp_path, "b1.json", {"id": 2, "amount": 8})
    snap = tmp_path / "snap"
    run(stream_doc(tmp_path, delta("snap", snap, mode=mode, partition_by=["id"])), spark=delta_spark)
    rows = {r.id: r.amount for r in read(delta_spark, snap).collect()}
    assert rows == ({2: 8} if mode == "overwrite" else {1: 5, 2: 8})
    assert commits(delta_spark, snap) == [(0, "WRITE"), (1, "WRITE")]
