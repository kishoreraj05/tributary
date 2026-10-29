"""Design-time validation of the Delta sink: what would fail at write time is reported while
the pipeline is drawn, against the schema the upstream nodes produce (analysed on empty
frames, like the SQL transform's column checks) and against the existing table if any."""

from __future__ import annotations

import pytest

from conftest import pipeline
from tributary import analyze, compile_pipeline
from tributary.plugins.sinks.delta import DeltaSinkOptions, compatibility_issues, condition_issues
from tributary.plugins.sinks.iceberg import IcebergSinkOptions

COLUMNS = ["order_id", "amount", "country"]  # what orders_clean produces in conftest's pipeline


def sink(path, **options):
    return {"id": "orders_out", "type": "delta", "input": "orders_clean", "options": {"path": str(path), **options}}


def analysis(spark, tmp_path, orders_csv, **options):
    doc = pipeline(tmp_path, orders_csv, sinks=[sink(tmp_path / "orders", **options)])
    return analyze(compile_pipeline(doc), spark=spark)["orders_out"]


def issues(node) -> list[tuple[str, str]]:
    return [(i["path"], i["message"]) for i in node.issues]


# --- against the upstream schema (no table yet) ----------------------------------------------


def test_valid_options_pass_and_keep_the_schema(spark, tmp_path, orders_csv):
    node = analysis(spark, tmp_path, orders_csv, mode="merge", merge_keys=["order_id"], partition_by=["country"],
                    update_condition="s.amount > t.amount", delete_condition="s.amount < 0")
    assert node.ok and node.issues == [] and node.error is None
    assert [f["name"] for f in node.schema["fields"]] == COLUMNS


def test_merge_key_not_produced_upstream_is_reported_on_the_field(spark, tmp_path, orders_csv):
    node = analysis(spark, tmp_path, orders_csv, mode="merge", merge_keys=["order_idx", "amount"])
    assert not node.ok
    assert issues(node) == [("sinks[0].options.merge_keys", "not in the data: order_idx (columns: order_id, amount, country)")]
    assert node.messages == ["not in the data: order_idx (columns: order_id, amount, country)"]


def test_partition_column_not_produced_upstream(spark, tmp_path, orders_csv):
    node = analysis(spark, tmp_path, orders_csv, partition_by=["region"])
    assert issues(node) == [("sinks[0].options.partition_by", "not in the data: region (columns: order_id, amount, country)")]


def test_conditions_are_checked_column_by_column(spark, tmp_path, orders_csv):
    node = analysis(spark, tmp_path, orders_csv, mode="merge", merge_keys=["order_id"],
                    update_condition="s.amout > t.amount", insert_condition="x.country = 'IN'",
                    delete_condition="s.amount < 0 AND")
    found = [(p.rsplit(".", 1)[1], m) for p, m in issues(node)]
    assert [f for f, _ in found] == ["update_condition", "insert_condition", "delete_condition"]
    assert found[0][1] == "s.amout is not a column of the data (columns: order_id, amount, country)"
    assert found[1][1] == "unknown alias 'x' in x.country: use s (incoming row) or t (table row)"
    assert found[2][1].startswith("cannot parse:")


def test_condition_references():
    data, table = ["id", "v"], ["id", "v", "old"]
    assert condition_issues("s.v > t.v AND t.old IS NULL", data, table) == []
    assert condition_issues("t.nope = 1", data, table) == ["t.nope is not a column of the table (columns: id, v, old)"]
    assert condition_issues("t.nope = 1", data, None) == []  # no table yet: nothing to check t. against
    assert condition_issues("v > 1", data, None) == []  # unqualified: Delta resolves it, the column exists
    assert condition_issues("nope > 1", data, table) == ["unknown column nope; prefix it with s. (incoming row) or t. (table row)"]


def test_the_sql_node_still_gets_its_own_errors_and_the_sink_is_skipped(spark, tmp_path, orders_csv):
    doc = pipeline(tmp_path, orders_csv, sinks=[sink(tmp_path / "orders", mode="merge", merge_keys=["order_id"])])
    doc["transforms"][0]["options"]["sql"] = "SELECT order_idd FROM orders"
    report = analyze(compile_pipeline(doc), spark=spark)
    assert not report["orders_clean"].ok and "order_idd" in report["orders_clean"].error
    assert report["orders_out"].error.startswith("skipped")


def test_iceberg_sink_checks_its_merge_keys_the_same_way(spark, tmp_path, orders_csv):
    doc = pipeline(tmp_path, orders_csv, sinks=[{
        "id": "orders_out", "type": "iceberg", "input": "orders_clean",
        "options": {"warehouse": str(tmp_path / "wh"), "table": "a.b", "mode": "merge", "merge_keys": ["order_idx"]},
    }])
    node = analyze(compile_pipeline(doc), spark=spark)["orders_out"]
    assert issues(node) == [("sinks[0].options.merge_keys", "not in the data: order_idx (columns: order_id, amount, country)")]


# --- against an existing table --------------------------------------------------------------


@pytest.fixture
def existing(delta_spark, tmp_path):
    """orders with amount as a string and an extra column, unlike what orders_clean produces."""
    path = tmp_path / "orders"
    delta_spark.createDataFrame([(1, "10.0", "IN", "x")], "order_id INT, amount STRING, country STRING, note STRING") \
        .write.format("delta").save(str(path))
    return path


def test_schema_enforcement_is_checked_before_writing(delta_spark, tmp_path, orders_csv, existing):
    node = analysis(delta_spark, tmp_path, orders_csv)  # orders_clean has amount DOUBLE
    assert issues(node) == [("sinks[0]", "amount is double in the data but string in the table; cast it in the SQL upstream "
                                         "(Delta never changes a column's type)")]


def test_new_columns_need_schema_evolution_unless_merging(delta_spark, tmp_path, orders_csv, existing):
    doc = pipeline(tmp_path, orders_csv, sinks=[sink(existing)])
    doc["transforms"][0]["options"]["sql"] = "SELECT order_id, cast(amount AS string) AS amount, country, 1 AS extra FROM orders"
    assert issues(analyze(compile_pipeline(doc), spark=delta_spark)["orders_out"]) == [
        ("sinks[0].options.schema_evolution", "the table has no column(s) extra; turn this on to add them, or drop them upstream")]
    doc["sinks"][0]["options"]["schema_evolution"] = True
    assert analyze(compile_pipeline(doc), spark=delta_spark)["orders_out"].ok
    doc["sinks"][0]["options"].update(mode="merge", merge_keys=["order_id"], schema_evolution=False)
    assert analyze(compile_pipeline(doc), spark=delta_spark)["orders_out"].ok  # a merge ignores extra columns


def test_merge_keys_and_t_references_are_checked_against_the_table(delta_spark, tmp_path, orders_csv, existing):
    doc = pipeline(tmp_path, orders_csv, sinks=[sink(existing, mode="merge", merge_keys=["country", "note"],
                                                      update_condition="s.amount > t.amount AND t.nope IS NULL")])
    doc["transforms"][0]["options"]["sql"] = "SELECT order_id, cast(amount AS string) AS amount, country, note FROM orders"
    doc["sources"][0]["schema"]["ddl"] = "order_id INT, amount DOUBLE, country STRING, note STRING"
    node = analyze(compile_pipeline(doc), spark=delta_spark)["orders_out"]
    assert issues(node) == [
        ("sinks[0].options.update_condition", "t.nope is not a column of the table (columns: order_id, amount, country, note)"),
    ]
    doc["sinks"][0]["options"]["merge_keys"] = ["order_id", "region"]
    doc["sinks"][0]["options"]["update_condition"] = "s.amount > t.amount"
    node = analyze(compile_pipeline(doc), spark=delta_spark)["orders_out"]
    assert [p for p, _ in issues(node)] == ["sinks[0].options.merge_keys"]  # reported once, against the data


def test_compatibility_rules():
    from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

    table = StructType([StructField("id", IntegerType()), StructField("v", StringType())])
    same = StructType([StructField("id", IntegerType()), StructField("v", StringType())])
    assert compatibility_issues(same, table, DeltaSinkOptions(path="/t")) == []
    narrower = StructType([StructField("id", IntegerType())])
    assert compatibility_issues(narrower, table, DeltaSinkOptions(path="/t")) == []  # missing columns are nulls
    typed = StructType([StructField("id", IntegerType()), StructField("v", DoubleType())])
    [(option, message)] = compatibility_issues(typed, table, DeltaSinkOptions(path="/t", schema_evolution=True))
    assert option == "" and message.startswith("v is double in the data but string in the table")
    keyed = DeltaSinkOptions(path="/t", mode="merge", merge_keys=["k"])
    with_k = StructType([StructField("id", IntegerType()), StructField("v", StringType()), StructField("k", IntegerType())])
    assert compatibility_issues(with_k, table, keyed) == [("merge_keys", "not in the table at /t: k (columns: id, v)")]
    assert compatibility_issues(same, table, keyed) == []  # a key the data lacks is reported against the data instead


def test_iceberg_options_unaffected():
    assert IcebergSinkOptions(warehouse="/w", table="a.b").merge_keys == []
