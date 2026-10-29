"""Delta write semantics, scenario by scenario: write modes and what overwrite replaces,
schema enforcement versus evolution, and the matched / unmatched / conditional branches
of a merge. Every option used here is a field of the sink's form in the UI."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from tributary import SpecError, compile_pipeline
from tributary.core import RunContext
from tributary.plugins.sinks.delta import DeltaSink, DeltaSinkOptions
from tributary.registry import default_registry

SINK = DeltaSink()


@pytest.fixture
def ctx(delta_spark) -> RunContext:
    return RunContext(spark=delta_spark, registry=default_registry(), pipeline_id="semantics", env="dev")


@pytest.fixture
def table(tmp_path):
    return tmp_path / "table"


def frame(spark, records, schema):
    return spark.createDataFrame(records, schema)


def read(spark, path):
    return spark.read.format("delta").load(str(path))


def rows(spark, path, *columns):
    return sorted(tuple(r[c] for c in columns) for r in read(spark, path).collect())


def write(ctx, records, schema, path, **options):
    SINK.write(ctx, frame(ctx.spark, records, schema), DeltaSinkOptions(path=str(path), **options))


# --- write modes -----------------------------------------------------------------------------

SCHEMA = "id INT, dt STRING, v INT"
INITIAL = [(1, "d1", 1), (2, "d1", 2), (3, "d2", 3)]
INCOMING = [(2, "d1", 20), (4, "d3", 40)]


def test_append_adds_rows_and_keeps_everything(ctx, table):
    write(ctx, INITIAL, SCHEMA, table, partition_by=["dt"])
    write(ctx, INCOMING, SCHEMA, table, mode="append")
    assert rows(ctx.spark, table, "id", "v") == [(1, 1), (2, 2), (2, 20), (3, 3), (4, 40)]


def test_overwrite_replaces_the_whole_table(ctx, table):
    write(ctx, INITIAL, SCHEMA, table, partition_by=["dt"])
    write(ctx, INCOMING, SCHEMA, table, mode="overwrite")
    assert rows(ctx.spark, table, "id", "v") == [(2, 20), (4, 40)]
    # Partitioning survives: d2 is gone because no row has it any more, not because the layout changed.
    assert {p.name for p in table.iterdir() if p.name.startswith("dt=")} >= {"dt=d1", "dt=d3"}


def test_overwrite_partitions_replaces_only_the_partitions_in_the_data(ctx, table):
    write(ctx, INITIAL, SCHEMA, table, partition_by=["dt"])
    write(ctx, INCOMING, SCHEMA, table, mode="overwrite_partitions")
    # d1 was replaced by the incoming d1 rows (so row 1 is gone), d2 untouched, d3 added.
    assert rows(ctx.spark, table, "id", "dt", "v") == [(2, "d1", 20), (3, "d2", 3), (4, "d3", 40)]


def test_overwrite_partitions_on_an_unpartitioned_table_replaces_everything(ctx, table):
    write(ctx, INITIAL, SCHEMA, table)
    write(ctx, INCOMING, SCHEMA, table, mode="overwrite_partitions")
    assert rows(ctx.spark, table, "id") == [(2,), (4,)]


def test_partition_by_only_counts_when_the_table_is_created(ctx, table):
    write(ctx, INITIAL, SCHEMA, table, partition_by=["dt"])
    write(ctx, INCOMING, SCHEMA, table, partition_by=["id"])  # ignored, no error
    assert rows(ctx.spark, table, "id") == [(1,), (2,), (2,), (3,), (4,)]
    assert not any(p.name.startswith("id=") for p in table.iterdir())


# --- schema enforcement and evolution --------------------------------------------------------


def test_enforcement_refuses_new_columns_unless_evolution_is_on(ctx, table):
    write(ctx, [(1, "a")], "id INT, v STRING", table)
    wider = [(2, "b", 1.5)]
    for mode in ("append", "overwrite"):
        with pytest.raises(Exception, match="(?i)schema mismatch"):
            write(ctx, wider, "id INT, v STRING, extra DOUBLE", table, mode=mode)
    assert read(ctx.spark, table).columns == ["id", "v"]  # nothing happened
    write(ctx, wider, "id INT, v STRING, extra DOUBLE", table, mode="append", schema_evolution=True)
    assert rows(ctx.spark, table, "id", "extra") == [(1, None), (2, 1.5)]
    # Evolution on overwrite keeps the old columns too (it is additive, not a schema replacement).
    write(ctx, [(3, "c", True)], "id INT, v STRING, flag BOOLEAN", table, mode="overwrite", schema_evolution=True)
    assert read(ctx.spark, table).columns == ["id", "v", "extra", "flag"]
    assert rows(ctx.spark, table, "id", "extra", "flag") == [(3, None, True)]


def test_missing_columns_are_allowed_and_filled_with_nulls(ctx, table):
    write(ctx, [(1, "a", 1.0)], "id INT, v STRING, extra DOUBLE", table)
    write(ctx, [(2, "b")], "id INT, v STRING", table)  # narrower data: fine without evolution
    assert rows(ctx.spark, table, "id", "extra") == [(1, 1.0), (2, None)]


def test_enforcement_refuses_incompatible_types_even_with_evolution(ctx, table):
    write(ctx, [(1, 10)], "id INT, v INT", table)
    for evolve in (False, True):
        with pytest.raises(Exception, match="(?i)schema mismatch|failed to merge|incompatible"):
            write(ctx, [(2, "not a number")], "id INT, v STRING", table, schema_evolution=evolve)
    assert rows(ctx.spark, table, "id", "v") == [(1, 10)]


def test_types_must_match_the_table_exactly(ctx, table):
    """Delta's enforcement is strict: even an int into a bigint column is refused, with or
    without evolution. The fix is a cast in the SQL transform feeding the sink."""
    write(ctx, [(1, 10)], "id INT, v BIGINT", table)
    for evolve in (False, True):
        with pytest.raises(Exception, match="DELTA_FAILED_TO_MERGE_FIELDS"):
            write(ctx, [(2, 20)], "id INT, v INT", table, schema_evolution=evolve)
    casted = frame(ctx.spark, [(2, 20)], "id INT, v INT").selectExpr("id", "cast(v AS bigint) AS v")
    SINK.write(ctx, casted, DeltaSinkOptions(path=str(table)))
    assert rows(ctx.spark, table, "id", "v") == [(1, 10), (2, 20)]
    assert read(ctx.spark, table).schema["v"].dataType.simpleString() == "bigint"


def test_overwrite_schema_option_replaces_the_schema(ctx, table):
    """write_options go straight to Delta's writer: overwriteSchema swaps the schema entirely."""
    write(ctx, [(1, "a", 1.0)], "id INT, v STRING, extra DOUBLE", table)
    write(ctx, [(2, 5)], "id INT, v INT", table, mode="overwrite", write_options={"overwriteSchema": True})
    assert read(ctx.spark, table).schema.simpleString() == "struct<id:int,v:int>"
    assert rows(ctx.spark, table, "id", "v") == [(2, 5)]


def test_merge_evolution_adds_columns_only_when_asked(ctx, table):
    merge = {"mode": "merge", "merge_keys": ["id"]}
    write(ctx, [(1, "a")], "id INT, v STRING", table, **merge)
    write(ctx, [(1, "A", True)], "id INT, v STRING, flag BOOLEAN", table, **merge)  # extra column ignored
    assert read(ctx.spark, table).columns == ["id", "v"] and rows(ctx.spark, table, "v") == [("A",)]
    write(ctx, [(1, "A", True)], "id INT, v STRING, flag BOOLEAN", table, schema_evolution=True, **merge)
    assert rows(ctx.spark, table, "id", "flag") == [(1, True)]


# --- upsert and merge --------------------------------------------------------------------------

EVENTS = "id INT, v STRING, updated_at INT, op STRING"


def test_matched_rows_are_updated_and_unmatched_rows_inserted(ctx, table):
    merge = {"mode": "merge", "merge_keys": ["id"]}
    write(ctx, [(1, "a", 1, "I"), (2, "b", 1, "I")], EVENTS, table, **merge)
    write(ctx, [(2, "B", 2, "U"), (3, "c", 2, "I")], EVENTS, table, **merge)
    assert rows(ctx.spark, table, "id", "v") == [(1, "a"), (2, "B"), (3, "c")]


def test_merge_on_several_keys(ctx, table):
    merge = {"mode": "merge", "merge_keys": ["k1", "k2"]}
    schema = "k1 INT, k2 STRING, v INT"
    write(ctx, [(1, "x", 1), (1, "y", 1)], schema, table, **merge)
    write(ctx, [(1, "y", 2), (2, "x", 3)], schema, table, **merge)
    assert rows(ctx.spark, table, "k1", "k2", "v") == [(1, "x", 1), (1, "y", 2), (2, "x", 3)]


def test_update_condition_keeps_rows_that_are_newer_in_the_table(ctx, table):
    merge = {"mode": "merge", "merge_keys": ["id"], "update_condition": "s.updated_at > t.updated_at"}
    write(ctx, [(1, "a", 5, "I"), (2, "b", 5, "I")], EVENTS, table, **merge)
    late = [(1, "stale", 3, "U"), (2, "fresh", 9, "U"), (3, "new", 1, "I")]
    write(ctx, late, EVENTS, table, **merge)
    assert rows(ctx.spark, table, "id", "v", "updated_at") == [(1, "a", 5), (2, "fresh", 9), (3, "new", 1)]


def test_insert_condition_filters_what_gets_inserted(ctx, table):
    merge = {"mode": "merge", "merge_keys": ["id"], "insert_condition": "s.op <> 'D'"}
    write(ctx, [(1, "a", 1, "I")], EVENTS, table, **merge)
    write(ctx, [(1, "A", 2, "D"), (2, "b", 2, "I"), (3, "c", 2, "D")], EVENTS, table, **merge)
    # 1 matched: updated (the insert condition does not apply to matches); 3 unmatched but a delete: not inserted.
    assert rows(ctx.spark, table, "id", "v") == [(1, "A"), (2, "b")]


def test_all_three_conditions_together(ctx, table):
    merge = {
        "mode": "merge", "merge_keys": ["id"],
        "delete_condition": "s.op = 'D'",
        "update_condition": "s.updated_at >= t.updated_at",
        "insert_condition": "s.v IS NOT NULL",
    }
    write(ctx, [(1, "a", 5, "I"), (2, "b", 5, "I"), (3, "c", 5, "I")], EVENTS, table, **merge)
    changes = [
        (1, "A", 6, "U"),      # matched, newer: updated
        (2, "stale", 4, "U"),  # matched, older: kept
        (3, "x", 9, "D"),      # matched, delete wins over update even though it is newer
        (4, "d", 1, "I"),      # unmatched: inserted
        (5, None, 1, "I"),     # unmatched but fails the insert condition: skipped
        (6, "f", 1, "D"),      # unmatched delete: never inserted
    ]
    write(ctx, changes, EVENTS, table, **merge)
    assert rows(ctx.spark, table, "id", "v", "updated_at") == [(1, "A", 6), (2, "b", 5), (4, "d", 1)]


def test_first_merge_write_inserts_only_what_a_merge_would(ctx, table):
    """A merge into a table that does not exist yet creates it, and every row is 'not
    matched': deletes and rows failing the insert condition stay out, as they would later.
    A change feed's first micro-batch (the Delta source) routinely carries deletes."""
    merge = {"mode": "merge", "merge_keys": ["id"], "delete_condition": "s.op = 'D'", "insert_condition": "s.v IS NOT NULL"}
    first = [(1, "a", 1, "I"), (2, "b", 2, "D"), (3, None, 1, "I"), (4, "d", 1, "U")]
    write(ctx, first, EVENTS, table, **merge)
    assert rows(ctx.spark, table, "id", "v") == [(1, "a"), (4, "d")]
    write(ctx, [(1, "A", 2, "U"), (4, "x", 2, "D"), (5, "e", 1, "I")], EVENTS, table, **merge)
    assert rows(ctx.spark, table, "id", "v") == [(1, "A"), (5, "e")]

    # Without conditions, the first write takes everything, as before.
    other = str(table) + "_plain"
    write(ctx, first, EVENTS, other, mode="merge", merge_keys=["id"])
    assert rows(ctx.spark, other, "id") == [(1,), (2,), (3,), (4,)]


def test_merge_conditions_are_validated_and_shown_only_for_merge(tmp_path):
    props = DeltaSinkOptions.model_json_schema()["properties"]
    for name in ("update_condition", "insert_condition", "delete_condition"):
        assert props[name]["x-when"] == {"field": "mode", "in": ["merge"], "required_in": []}
        with pytest.raises(ValidationError, match=rf"{name}\n  Value error, only applies to mode 'merge'"):
            DeltaSinkOptions(path="/t", **{name: "s.x = 1"})
    doc = {
        "pipeline": {"id": "p"},
        "sources": [{"id": "src", "type": "file", "options": {"path": str(tmp_path / "x.csv"), "format": "csv"}}],
        "sinks": [{"id": "out", "type": "delta", "input": "src",
                   "options": {"path": "/t", "mode": "overwrite", "update_condition": "s.a > t.a"}}],
    }
    with pytest.raises(SpecError) as exc:
        compile_pipeline(doc)
    assert [(i.path, i.message) for i in exc.value.issues] == [("sinks[0].options.update_condition", "only applies to mode 'merge'")]


def test_bad_condition_sql_fails_the_write_clearly(ctx, table):
    merge = {"mode": "merge", "merge_keys": ["id"], "update_condition": "s.nope > 1"}
    write(ctx, [(1, "a", 1, "I")], EVENTS, table, **merge)
    with pytest.raises(Exception, match="nope"):
        write(ctx, [(1, "A", 2, "U")], EVENTS, table, **merge)
    assert rows(ctx.spark, table, "v") == [("a",)]
