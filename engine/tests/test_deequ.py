"""The deequ node: one runner at a time, its functions applied to columns, with type checks.

Tests that run deequ need its jar (see ``deequ_jar`` in conftest); the others need no Spark.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from conftest import pipeline
from tributary import QualityFailure, SpecError, TributaryError, analyze, compile_pipeline, export_schema, preview, run
from tributary.core import RunContext
from tributary.planner import build_plan
from tributary.plugins.transforms.deequ import (
    ANALYZERS,
    CONSTRAINTS,
    DEEQU_PACKAGE,
    RULES,
    Analyzer,
    Assertion,
    Constraint,
    DeequOptions,
    DeequTransform,
    Rule,
)
from tributary.registry import default_registry
from tributary.runtime import required_packages
from tributary.testing import static_contract_issues

CONSTRAINTS_SPEC = [
    {"function": "is_complete", "columns": ["order_id", "country"]},
    {"function": "is_non_negative", "columns": ["amount"]},
    {"function": "has_pattern", "columns": ["country"], "pattern": "^[A-Z]{2}$", "assertion": ">= 0.7"},
    {"function": "has_size", "assertion": ">= 3"},
    {"function": "has_mean", "columns": ["amount"], "assertion": "between 0 and 100"},
    {"function": "has_min", "columns": ["amount"], "assertion": ">= 0", "level": "warning"},
]


def deequ_node(runner="verification", entries=None, **options) -> dict:
    key = {"analysis": "analyzers", "verification": "constraints", "suggestion": "rules"}[runner]
    default = {"verification": CONSTRAINTS_SPEC, "analysis": [{"function": "size"}], "suggestion": [{"function": "default_rules"}]}
    return {
        "id": "orders_dq",
        "type": "deequ",
        "inputs": ["orders_src"],
        "options": {"runner": runner, key: entries if entries is not None else default[runner], **options},
    }


def deequ_pipeline(tmp_path, orders_csv, runner="verification", entries=None, **options) -> dict:
    """orders_src -> orders_dq (deequ) -> orders_clean (sql) -> orders_out."""
    sql = {"id": "orders_clean", "type": "sql", "inputs": [{"node": "orders_dq", "as": "orders"}],
           "options": {"sql": "SELECT * FROM orders"}}
    return pipeline(tmp_path, orders_csv, transforms=[deequ_node(runner, entries, **options), sql])


# --- no Spark needed ---------------------------------------------------------------------


def test_plugin_contract_and_catalog():
    assert static_contract_issues(DeequTransform) == []
    meta = next(p for p in export_schema()["components"]["transform"] if p["type"] == "deequ")
    assert meta["display_name"] == "Deequ" and meta["icon"] == "checklist" and meta["chainable"] is False
    props = meta["options_schema"]["properties"]
    assert props["runner"]["enum"] == ["analysis", "verification", "suggestion"] and "runner" in meta["options_schema"]["required"]
    assert props["results_sinks"]["x-targets"] == "metrics_sink"


def test_the_runner_decides_which_fields_apply():
    """The UI shows one function list per runner (x-when on 'runner'); the model enforces it."""
    props = DeequOptions.model_json_schema()["properties"]
    assert props["analyzers"]["x-when"] == {"field": "runner", "in": ["analysis"], "required_in": ["analysis"]}
    assert props["constraints"]["x-when"] == {"field": "runner", "in": ["verification"], "required_in": ["verification"]}
    assert props["rules"]["x-when"] == {"field": "runner", "in": ["suggestion"], "required_in": ["suggestion"]}
    for key in ("output", "fail_on", "results_sinks"):
        assert props[key]["x-when"]["in"] == ["verification"]

    for bad, path, message in [
        ({"runner": "analysis"}, "analyzers", "the analysis runner needs at least one entry"),
        ({"runner": "analysis", "analyzers": [{"function": "size"}], "constraints": [{"function": "has_size", "assertion": "> 0"}]},
         "constraints", "only the verification runner takes constraints"),
        ({"runner": "analysis", "analyzers": [{"function": "size"}], "output": "data"}, "output", "only applies to the verification runner"),
        ({"runner": "suggestion", "rules": [{"function": "retain_type"}], "fail_on": "never"}, "fail_on", "only applies to the verification runner"),
    ]:
        with pytest.raises(ValidationError) as exc:
            DeequOptions(**bad)
        [error] = exc.value.errors()
        assert error["loc"] == (path,) and message in error["msg"], (bad, error)


def test_function_fields_depend_on_the_function():
    for model, bad, path, message in [
        (Analyzer, {"function": "mean"}, "columns", "required for mean"),
        (Analyzer, {"function": "size", "columns": ["a"]}, "columns", "size takes no columns"),
        (Analyzer, {"function": "correlation", "columns": ["a"]}, "columns", "correlation compares exactly two columns, got 1"),
        (Analyzer, {"function": "approx_quantile", "columns": ["a"]}, "quantile", "required for approx_quantile"),
        (Analyzer, {"function": "mean", "columns": ["a"], "pattern": "x"}, "pattern", "does not apply to mean"),
        (Constraint, {"function": "has_mean", "columns": ["a"]}, "assertion", "required for has_mean"),
        (Constraint, {"function": "is_complete", "columns": ["a"], "assertion": ">= 1"}, "assertion", "does not apply to is_complete"),
        (Constraint, {"function": "is_in_range", "columns": ["a"], "lower_bound": 1}, "upper_bound", "required for is_in_range"),
        (Constraint, {"function": "satisfies"}, "expression", "required for satisfies"),
        (Constraint, {"function": "has_size", "assertion": "at least 5"}, "assertion", "invalid assertion 'at least 5'"),
        (Constraint, {"function": "is_less_than", "columns": ["a", "b", "c"]}, "columns", "compares exactly two columns, got 3"),
    ]:
        with pytest.raises(ValidationError) as exc:
            model(**bad)
        [error] = exc.value.errors()
        assert error["loc"] == (path,) and message in error["msg"], (bad, error)
    assert Rule(function="has_min").columns == []  # rules profile every column unless restricted
    # the x-when hints follow the same tables
    columns = Analyzer.model_json_schema()["properties"]["columns"]["x-when"]
    assert "size" not in columns["in"] and "mean" in columns["required_in"]
    assertion = Constraint.model_json_schema()["properties"]["assertion"]["x-when"]
    assert set(assertion["required_in"]) == {n for n, f in CONSTRAINTS.items() if f.assertion == "required"}
    assert Analyzer.model_json_schema()["properties"]["function"]["enum"] == list(ANALYZERS)
    assert Rule.model_json_schema()["properties"]["function"]["enum"] == list(RULES)


def test_functions_apply_to_each_listed_column():
    c = Constraint(function="is_complete", columns=["a", "b"], name="filled")
    assert c.applications() == [("a",), ("b",)] and [c.label(x) for x in c.applications()] == ["filled[a]", "filled[b]"]
    assert Constraint(function="is_primary_key", columns=["a", "b"]).applications() == [("a", "b")]
    assert Analyzer(function="correlation", columns=["a", "b"]).label(("a", "b")) == "correlation(a, b)"
    assert Analyzer(function="compliance", expression="x > 0").label(()) == "compliance(x > 0)"
    assert Constraint(function="has_size", assertion="5").label(()) == "has_size"
    assert Constraint(function="satisfies", expression="x > 0").assertion_ == Assertion("==", 1.0)
    assert Constraint(function="is_complete", columns=["a"]).assertion_ is None


def test_assertions_follow_deequ_semantics():
    assert Assertion.parse(">= 0.95").holds(0.95) and not Assertion.parse("> 0.95").holds(0.95)
    assert Assertion.parse("between 1 and 10").holds(10) and not Assertion.parse("between 1 and 10").holds(10.5)
    assert Assertion.parse("== 1").holds(1.0000000000001) and Assertion.parse("!= 0").holds(1e-3)
    assert Assertion.parse(" <  3e2 ") == Assertion("<", 300.0) and not Assertion.parse("== 1").holds(float("nan"))


def test_spec_validation_reports_inputs_expressions_and_chaining(tmp_path, orders_csv):
    doc = deequ_pipeline(tmp_path, orders_csv, entries=[
        {"function": "satisfies", "expression": "amount >"},
        {"function": "has_size", "assertion": "> 0", "where": "country = "},
    ])
    doc["sources"].append({**doc["sources"][0], "id": "second"})
    doc["transforms"][0]["inputs"] = ["orders_src", "second"]
    with pytest.raises(SpecError) as exc:
        compile_pipeline(doc)
    messages = [i.message for i in exc.value.issues if i.path == "transforms[0].options"]
    assert any("exactly one input, got 2" in m for m in messages)
    assert any("satisfies(amount >): expression syntax error" in m for m in messages)
    assert any("has_size: where syntax error" in m for m in messages)

    # a deequ node cannot be downstream of another one, even through other nodes
    doc = deequ_pipeline(tmp_path, orders_csv)
    doc["transforms"].append({"id": "profile", "type": "deequ", "inputs": ["orders_clean"],
                              "options": {"runner": "analysis", "analyzers": [{"function": "size"}]}})
    with pytest.raises(SpecError) as exc:
        compile_pipeline(doc)
    [issue] = exc.value.issues
    assert issue.path == "transforms[2].inputs" and "cannot be downstream of another deequ node ('orders_dq')" in issue.message
    doc["transforms"][2]["inputs"] = ["orders_src"]  # its own branch from the source is fine
    compile_pipeline(doc)


def test_results_sinks_are_typed_against_the_registry(tmp_path, orders_csv):
    doc = deequ_pipeline(tmp_path, orders_csv, results_sinks=[{"type": "nope"}])
    with pytest.raises(SpecError, match=r"transforms\[0\].options.results_sinks\[0\]"):
        compile_pipeline(doc)
    doc = deequ_pipeline(tmp_path, orders_csv, results_sinks=[{"type": "postgres", "connection": "metrics"}])
    doc["environments"]["dev"]["connections"] = {"metrics": {"type": "postgres", "url": "jdbc:postgresql://db/m", "table": "dq"}}
    compiled = compile_pipeline(doc)
    [target] = compiled.spec.transforms[0].options.results_sinks
    assert target.type == "postgres" and target.options.url == "jdbc:postgresql://db/m"
    packages = required_packages(compiled)
    assert DEEQU_PACKAGE in packages and any(p.startswith("org.postgresql:postgresql") for p in packages)


def test_plan_treats_a_data_gate_as_pass_through(tmp_path, orders_csv):
    plan = build_plan(compile_pipeline(deequ_pipeline(tmp_path, orders_csv, output="data")))
    node, source = plan.steps["orders_dq"], plan.steps["orders_src"]
    assert node.passthrough and not node.cache
    assert source.reads == 2 and source.cache  # read by deequ's aggregation and by the SQL node
    plan = build_plan(compile_pipeline(deequ_pipeline(tmp_path, orders_csv)))  # check_results: a real output
    assert not plan.steps["orders_dq"].passthrough and plan.steps["orders_src"].reads == 1


# --- with deequ on the session ------------------------------------------------------------

ORDERS_DDL = "order_id INT, amount DOUBLE, country STRING, email STRING, url STRING, other DOUBLE, tags ARRAY<STRING>"
ORDERS_ROWS = [
    (1, 10.0, "IN", "a@b.com", "http://x.io", 2.0, ["t"]),
    (2, 1.0, "US", "c@d.org", "https://y.com/z", 3.0, []),
    (3, 5.0, "DE", "e@f.net", "http://q.org", 4.0, None),
]


@pytest.fixture
def orders(deequ_spark):
    return deequ_spark.createDataFrame(ORDERS_ROWS, ORDERS_DDL)


def ctx_for(spark) -> RunContext:
    return RunContext(spark=spark, registry=default_registry(), pipeline_id="p", env="test", node_id="dq")


def test_verification_outputs_check_results(deequ_spark, tmp_path, orders_csv):
    summary = run(deequ_pipeline(tmp_path, orders_csv, fail_on="never"), spark=deequ_spark)
    rows = {r.constraint: r for r in deequ_spark.read.parquet(str(tmp_path / "out" / "orders")).collect()}
    assert set(rows) == {
        "is_complete(order_id)", "is_complete(country)", "is_non_negative(amount)", "has_pattern(country)", "has_size",
        "has_mean(amount)", "has_min(amount)",
    }
    assert rows["is_complete(order_id)"].asDict() == {
        "check": "orders_dq", "check_level": "error", "check_status": "error", "constraint": "is_complete(order_id)",
        "constraint_status": "failure", "constraint_message": "Value: 0.8 does not meet the constraint requirement!",
        "metric_name": "Completeness", "metric_value": 0.8,
    }
    assert rows["has_min(amount)"].check == "orders_dq (warning)" and rows["has_min(amount)"].constraint_status == "failure"
    assert rows["has_pattern(country)"].constraint_status == "success"  # 4 of 5 match, 0.8 >= 0.7

    results = {r.check_id: r for r in summary.quality}
    assert set(results) == set(rows)
    assert all(r.quality_id == "orders_dq" and r.node_id == "orders_src" and r.check_type == "deequ" for r in results.values())
    complete = results["is_complete(order_id)"]
    assert (complete.level, complete.total_rows, complete.failed_rows, complete.observed_value, complete.passed) == ("row", 5, 1, 0.8, False)
    size = results["has_size"]
    assert (size.level, size.failed_rows, size.observed_value, size.passed) == ("dataset", None, 5.0, True)
    assert all(r.action == "observe" for r in results.values())  # fail_on: never


def test_verification_gate_passes_data_on_and_fails_the_run(deequ_spark, tmp_path, orders_csv):
    with pytest.raises(QualityFailure, match="quality 'orders_dq' failed: is_complete\\(order_id\\)") as exc:
        run(deequ_pipeline(tmp_path, orders_csv, output="data"), spark=deequ_spark)
    assert not (tmp_path / "out" / "orders").exists()
    actions = {r.check_id: r.action for r in exc.value.results}
    assert actions["is_complete(order_id)"] == "fail" and actions["has_min(amount)"] == "observe"

    # only the warning-level constraint fails: fail_on 'error' lets the data through, 'warning' does not
    ok = [{"function": "has_size", "assertion": "== 5"}, {"function": "has_min", "columns": ["amount"], "assertion": ">= 0", "level": "warning"}]
    summary = run(deequ_pipeline(tmp_path, orders_csv, entries=ok, output="data"), spark=deequ_spark)
    assert [r.passed for r in summary.quality] == [True, False]
    assert deequ_spark.read.parquet(str(tmp_path / "out" / "orders")).count() == 5  # unchanged
    with pytest.raises(QualityFailure, match="has_min"):
        run(deequ_pipeline(tmp_path, orders_csv, entries=ok, output="data", fail_on="warning"), spark=deequ_spark)


def test_analysis_outputs_metrics(deequ_spark, tmp_path, orders_csv):
    analyzers = [
        {"function": "size"}, {"function": "completeness", "columns": ["order_id", "amount"]},
        {"function": "mean", "columns": ["amount"]}, {"function": "histogram", "columns": ["country"]},
        {"function": "compliance", "expression": "amount > 0", "name": "positive"},
    ]
    run(deequ_pipeline(tmp_path, orders_csv, runner="analysis", entries=analyzers), spark=deequ_spark)
    out = deequ_spark.read.parquet(str(tmp_path / "out" / "orders"))
    assert out.columns == ["entity", "instance", "name", "value"]
    metrics = {(r.entity, r.instance, r.name): r.value for r in out.collect()}
    assert metrics[("Dataset", "*", "Size")] == 5.0
    assert metrics[("Column", "order_id", "Completeness")] == 0.8 and metrics[("Column", "amount", "Completeness")] == 1.0
    assert metrics[("Column", "amount", "Mean")] == pytest.approx(4.9)
    assert metrics[("Column", "positive", "Compliance")] == 0.8
    assert metrics[("Column", "country", "Histogram.abs.IN")] == 1.0  # histograms are flattened, one row per bin


def test_suggestion_outputs_suggested_constraints(deequ_spark, tmp_path, orders_csv):
    rules = [{"function": "default_rules"}, {"function": "has_max", "columns": ["amount"]}]
    run(deequ_pipeline(tmp_path, orders_csv, runner="suggestion", entries=rules), spark=deequ_spark)
    out = deequ_spark.read.parquet(str(tmp_path / "out" / "orders"))
    assert out.columns == ["column_name", "constraint_name", "current_value", "description", "suggesting_rule", "rule_description", "code_for_constraint"]
    rows = out.collect()
    by_rule = {(r.column_name, r.suggesting_rule): r for r in rows}
    assert by_rule[("amount", "HasMax")].code_for_constraint == '.hasMax("amount", _ == 10.0)'
    assert by_rule[("amount", "CompleteIfCompleteRule")].code_for_constraint == '.isComplete("amount")'
    assert ("order_id", "CompleteIfCompleteRule") not in by_rule  # order_id has a null: not suggested as complete
    assert len(rows) == len({(r.column_name, r.code_for_constraint) for r in rows})  # no duplicates


def test_every_function_of_every_runner_runs(orders, deequ_spark):
    node, ctx = DeequTransform(), ctx_for(deequ_spark)
    analyzers = {
        "size": {}, "column_count": {}, "completeness": {"columns": ["order_id"]}, "uniqueness": {"columns": ["order_id"]},
        "distinctness": {"columns": ["country"]}, "unique_value_ratio": {"columns": ["country"]}, "count_distinct": {"columns": ["country"]},
        "approx_count_distinct": {"columns": ["country"]}, "entropy": {"columns": ["country"]}, "histogram": {"columns": ["country"]},
        "data_type": {"columns": ["amount"]}, "mean": {"columns": ["amount", "other"]}, "minimum": {"columns": ["amount"]},
        "maximum": {"columns": ["amount"]}, "sum": {"columns": ["amount"]}, "standard_deviation": {"columns": ["amount"]},
        "approx_quantile": {"columns": ["amount"], "quantile": 0.5}, "min_length": {"columns": ["country"]},
        "max_length": {"columns": ["email"]}, "pattern_match": {"columns": ["country"], "pattern": "^[A-Z]{2}$"},
        "correlation": {"columns": ["amount", "other"]}, "mutual_information": {"columns": ["country", "email"]},
        "compliance": {"expression": "amount > 0", "where": "country <> 'XX'"},
    }
    assert set(analyzers) == set(ANALYZERS)
    opts = DeequOptions(runner="analysis", analyzers=[{"function": f, **s} for f, s in analyzers.items()])
    metrics = {(r.instance, r.name): r.value for r in node.apply(ctx, {"orders": orders}, opts).collect()}
    assert metrics[("amount", "Mean")] == pytest.approx(16 / 3) and metrics[("other", "Mean")] == 3.0  # applied to each column
    assert metrics[("*", "ColumnCount")] == 7.0 and metrics[("amount,other", "Correlation")] == pytest.approx(-0.554, abs=1e-3)

    constraints = {
        "is_complete": {"columns": ["order_id"]}, "are_complete": {"columns": ["order_id", "amount"]},
        "has_completeness": {"columns": ["order_id"], "assertion": ">= 0.5"}, "is_unique": {"columns": ["order_id", "country"]},
        "is_primary_key": {"columns": ["order_id", "country"]}, "has_uniqueness": {"columns": ["order_id"], "assertion": "== 1"},
        "has_distinctness": {"columns": ["country"], "assertion": ">= 0.5"}, "has_unique_value_ratio": {"columns": ["country"], "assertion": ">= 0.5"},
        "satisfies": {"expression": "amount > 0"}, "is_contained_in": {"columns": ["country"], "values": ["IN", "US", "DE"]},
        "is_in_range": {"columns": ["amount"], "lower_bound": 0, "upper_bound": 10},
        "has_pattern": {"columns": ["country"], "pattern": "^[A-Z]{2}$"}, "is_non_negative": {"columns": ["amount"]},
        "is_positive": {"columns": ["amount"]},
        "is_less_than": {"columns": ["amount", "other"], "assertion": ">= 0.3"},
        "is_less_than_or_equal_to": {"columns": ["amount", "other"], "assertion": ">= 0.3"},
        "is_greater_than": {"columns": ["amount", "other"], "assertion": ">= 0.6"},
        "is_greater_than_or_equal_to": {"columns": ["amount", "other"], "assertion": ">= 0.6"},
        "has_data_type": {"columns": ["amount"], "data_type": "Numeric"}, "contains_email": {"columns": ["email"]},
        "contains_url": {"columns": ["url"]}, "contains_credit_card_number": {"columns": ["country"], "assertion": "== 0"},
        "contains_social_security_number": {"columns": ["country"], "assertion": "== 0"},
        "has_size": {"assertion": "== 3"}, "has_min": {"columns": ["amount"], "assertion": "== 1"},
        "has_max": {"columns": ["amount"], "assertion": "== 10", "where": "country <> 'XX'"},
        "has_mean": {"columns": ["amount"], "assertion": "between 5 and 6"}, "has_sum": {"columns": ["amount"], "assertion": "== 16"},
        "has_standard_deviation": {"columns": ["amount"], "assertion": "> 0"}, "has_min_length": {"columns": ["country"], "assertion": "== 2"},
        "has_max_length": {"columns": ["email"], "assertion": "<= 10"}, "has_approx_count_distinct": {"columns": ["country"], "assertion": "== 3"},
        "has_approx_quantile": {"columns": ["amount"], "quantile": 0.5, "assertion": "between 1 and 10"},
        "has_entropy": {"columns": ["country"], "assertion": "> 0"},
        "has_correlation": {"columns": ["amount", "other"], "assertion": "between -1 and 1"},
    }
    assert set(constraints) == set(CONSTRAINTS)
    opts = DeequOptions(runner="verification", constraints=[{"function": f, **s} for f, s in constraints.items()], fail_on="never")
    node.apply(ctx, {"orders": orders}, opts)
    results = {r.check_id: r for r in ctx.take_reports()}
    assert [k for k, r in results.items() if not r.passed] == []
    assert {"is_unique(order_id)", "is_unique(country)"} <= set(results)  # applied to each column
    assert results["is_less_than(amount, other)"].failed_rows == 2  # 10 > 2, 1 < 3, 5 > 4
    assert results["has_data_type(amount)"].observed_value == 1.0
    assert results["has_max(amount)"].level == "dataset"  # a 'where' filter: failed rows are not known

    opts = DeequOptions(runner="suggestion", rules=[{"function": f} for f in RULES])
    rows = node.apply(ctx, {"orders": orders}, opts).collect()
    assert {r.suggesting_rule for r in rows} >= {"CompleteIfCompleteRule", "NonNegativeNumbersRule", "HasMin", "HasMaxLength"}
    assert "tags" not in {r.column_name for r in rows}  # nested columns are not profiled


def test_column_types_are_checked_before_anything_runs(orders):
    for runner, entry, message in [
        ("analysis", {"function": "mean", "columns": ["country"]},
         "mean(country): column 'country' is string, but this function needs a numeric column"),
        ("analysis", {"function": "histogram", "columns": ["tags"]},
         "histogram(tags): column 'tags' is array<string>; deequ works on simple columns"),
        ("verification", {"function": "has_pattern", "columns": ["amount"], "pattern": "x"},
         "has_pattern(amount): column 'amount' is double, but this function needs a string column"),
        ("verification", {"function": "is_less_than", "columns": ["amount", "country"]},
         "cannot compare 'amount' (double) with 'country' (string): both columns must be numeric, both strings, or both dates/timestamps"),
        ("verification", {"function": "is_in_range", "columns": ["order_id", "email"], "lower_bound": 0, "upper_bound": 1},
         "is_in_range(email): column 'email' is string, but this function needs a numeric column"),
        ("verification", {"function": "has_mean", "columns": ["amoun"], "assertion": "> 0"},
         "has_mean(amoun): unknown column 'amoun' (columns: order_id, amount, country, email, url, other, tags)"),
        ("suggestion", {"function": "has_min", "columns": ["tags"]}, "has_min(tags): column 'tags' is array<string>"),
    ]:
        key = {"analysis": "analyzers", "verification": "constraints", "suggestion": "rules"}[runner]
        with pytest.raises(TributaryError, match=message.replace("(", "\\(").replace(")", "\\)")):
            DeequTransform.check_columns(orders, DeequOptions(runner=runner, **{key: [entry]}))


def test_analyze_reports_type_and_column_problems_per_node(deequ_spark, tmp_path, orders_csv):
    doc = deequ_pipeline(tmp_path, orders_csv, runner="analysis", entries=[{"function": "mean", "columns": ["country"]}])
    report = analyze(compile_pipeline(doc), spark=deequ_spark)
    assert not report["orders_dq"].ok and "mean(country): column 'country' is string" in report["orders_dq"].error
    assert report["orders_clean"].error.startswith("skipped")
    doc["transforms"][0]["options"]["analyzers"] = [{"function": "mean", "columns": ["amount"]}]
    report = analyze(compile_pipeline(doc), spark=deequ_spark)
    assert report["orders_dq"].ok and [f["name"] for f in report["orders_dq"].schema["fields"]] == ["entity", "instance", "name", "value"]


def test_preview_shows_results_instead_of_failing(deequ_spark, tmp_path, orders_csv):
    doc = deequ_pipeline(tmp_path, orders_csv, output="data",
                         results_sinks=[{"type": "file", "options": {"path": str(tmp_path / "dq")}}])
    result = preview(doc, "orders_clean", spark=deequ_spark, limit=3)  # downstream of the failing gate
    assert len(result.rows) == 3 and [f["name"] for f in result.schema["fields"]] == ["order_id", "amount", "country"]
    assert {q["check_id"] for q in result.quality} >= {"is_complete(order_id)", "has_min(amount)"}
    assert not (tmp_path / "dq").exists()  # previews have no side effects


def test_results_sinks_receive_one_row_per_constraint(deequ_spark, tmp_path, orders_csv):
    doc = deequ_pipeline(tmp_path, orders_csv, fail_on="never",
                         results_sinks=[{"type": "file", "options": {"path": str(tmp_path / "dq"), "format": "json"}}])
    summary = run(doc, spark=deequ_spark)
    rows = deequ_spark.read.json(str(tmp_path / "dq")).collect()
    assert {r.check_id for r in rows} == {r.check_id for r in summary.quality}
    assert all(r.quality_id == "orders_dq" and r.run_id == summary.run_id for r in rows)


def test_needs_a_jvm_session(orders):
    class Connectish:  # what a Spark Connect session looks like from here: no gateway
        pass

    ctx = RunContext(spark=Connectish(), registry=default_registry(), pipeline_id="p", env="test")
    opts = DeequOptions(runner="analysis", analyzers=[{"function": "size"}])
    with pytest.raises(TributaryError, match="Spark Connect"):
        DeequTransform().apply(ctx, {"t": orders}, opts)


def test_streaming_verifies_every_micro_batch(deequ_spark, tmp_path):
    incoming = tmp_path / "in"
    incoming.mkdir()
    (incoming / "a.json").write_text('{"id": 1, "amount": 5}\n{"id": 2, "amount": -1}\n')
    out = tmp_path / "out"
    doc = {
        "pipeline": {"id": "stream_dq", "mode": "streaming",
                     "streaming": {"checkpoint_location": str(tmp_path / "chk"), "trigger": "available_now"}},
        "sources": [{"id": "events", "type": "file", "options": {"path": str(incoming), "format": "json"},
                     "schema": {"kind": "ddl", "ddl": "id INT, amount INT"}}],
        "transforms": [{"id": "dq", "type": "deequ", "inputs": ["events"], "options": {
            "runner": "verification", "output": "data", "fail_on": "never",
            "constraints": [{"function": "is_positive", "columns": ["amount"]}],
            "results_sinks": [{"type": "file", "options": {"path": str(out / "dq")}}],
        }}],
        "sinks": [{"id": "sink", "type": "file", "input": "dq", "options": {"path": str(out / "data")}}],
    }
    query = run(doc, spark=deequ_spark)
    assert not query.isActive
    assert deequ_spark.read.parquet(str(out / "data")).count() == 2
    [result] = deequ_spark.read.json(str(out / "dq")).collect()
    assert (result.batch_id, result.check_id, result.failed_rows, result.passed) == (0, "is_positive(amount)", 1, False)
