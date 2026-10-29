"""Every deequ function, one at a time: each runs on its own and produces its runner's schema.

The three runners have three output schemas. Whatever function an entry names, the node's
output must have exactly the runner's columns and types, so a sink fed by a deequ node can
rely on them. These tests need the deequ jar (see ``deequ_jar`` in conftest).
"""

from __future__ import annotations

import pytest
from pyspark.sql.types import StructType

from tributary.core import RunContext
from tributary.plugins.transforms.deequ import (
    ANALYZERS,
    CHECK_RESULTS_DDL,
    CONSTRAINTS,
    METRICS_DDL,
    OUTPUT_DDL,
    RULES,
    SUGGESTIONS_DDL,
    DeequOptions,
    DeequTransform,
)
from tributary.registry import default_registry

# --- the data: a column of every kind a function may need --------------------------------------

DDL = (
    "id INT, amount DOUBLE, other DOUBLE, country STRING, code STRING, note STRING, contact STRING, "
    "city STRING, flag STRING, email STRING, url STRING"
)


def rows():
    out = []
    for i in range(100):
        country = ["IN", "US", "DE"][i % 3] if i < 98 else ["FR", "GB"][i - 98]  # two rare values
        out.append((
            i, float(i % 7) + 0.5, float(i), country, str(i % 4), "x" if i % 5 else None,
            "a@b.com" if i % 2 else "https://e.org", "NY" if i % 3 else "London", str(i % 2 == 0),
            f"user{i}@example.com", f"https://example.com/{i}",
        ))
    return out


@pytest.fixture(scope="module")
def data(deequ_spark):
    return deequ_spark.createDataFrame(rows(), DDL)


@pytest.fixture(scope="module")
def schemas(deequ_spark) -> dict[str, StructType]:
    return {runner: deequ_spark.createDataFrame([], ddl).schema for runner, ddl in OUTPUT_DDL.items()}


def run_one(spark, df, runner: str, entry: dict):
    key = {"analysis": "analyzers", "verification": "constraints", "suggestion": "rules"}[runner]
    ctx = RunContext(spark=spark, registry=default_registry(), pipeline_id="p", env="test", node_id="dq")
    extra = {"fail_on": "never"} if runner == "verification" else {}  # record outcomes, do not fail the run
    opts = DeequOptions(runner=runner, **{key: [entry]}, **extra)
    return DeequTransform().apply(ctx, {"t": df}, opts), ctx


# --- analysis: every analyzer -> entity, instance, name, value -----------------------------------

ANALYZER_CASES = {
    "size": ({}, [("Dataset", "*", "Size")]),
    "column_count": ({}, [("Dataset", "*", "ColumnCount")]),
    "completeness": ({"columns": ["note", "id"]}, [("Column", "note", "Completeness"), ("Column", "id", "Completeness")]),
    "uniqueness": ({"columns": ["id"]}, [("Column", "id", "Uniqueness")]),
    "distinctness": ({"columns": ["country"]}, [("Column", "country", "Distinctness")]),
    "unique_value_ratio": ({"columns": ["country"]}, [("Column", "country", "UniqueValueRatio")]),
    "count_distinct": ({"columns": ["country"]}, [("Column", "country", "CountDistinct")]),
    "approx_count_distinct": ({"columns": ["country"]}, [("Column", "country", "ApproxCountDistinct")]),
    "entropy": ({"columns": ["country"]}, [("Column", "country", "Entropy")]),
    "histogram": ({"columns": ["flag"]}, [("Column", "flag", "Histogram.bins"), ("Column", "flag", "Histogram.abs.True"), ("Column", "flag", "Histogram.ratio.False")]),
    "data_type": ({"columns": ["code"]}, [("Column", "code", "Histogram.ratio.Integral")]),
    "mean": ({"columns": ["amount"]}, [("Column", "amount", "Mean")]),
    "minimum": ({"columns": ["amount"]}, [("Column", "amount", "Minimum")]),
    "maximum": ({"columns": ["amount"]}, [("Column", "amount", "Maximum")]),
    "sum": ({"columns": ["amount"]}, [("Column", "amount", "Sum")]),
    "standard_deviation": ({"columns": ["amount"]}, [("Column", "amount", "StandardDeviation")]),
    "approx_quantile": ({"columns": ["amount"], "quantile": 0.5}, [("Column", "amount", "ApproxQuantile-0.5")]),
    "min_length": ({"columns": ["country"]}, [("Column", "country", "MinLength")]),
    "max_length": ({"columns": ["email"]}, [("Column", "email", "MaxLength")]),
    "pattern_match": ({"columns": ["country"], "pattern": "^[A-Z]{2}$"}, [("Column", "country", "PatternMatch")]),
    "correlation": ({"columns": ["amount", "other"]}, [("Multicolumn", "amount,other", "Correlation")]),
    "mutual_information": ({"columns": ["country", "city"]}, [("Multicolumn", "country,city", "MutualInformation")]),
    "compliance": ({"expression": "amount > 0", "name": "positive"}, [("Column", "positive", "Compliance")]),
}


def test_every_analyzer_has_a_case():
    assert set(ANALYZER_CASES) == set(ANALYZERS)


@pytest.mark.parametrize("function", list(ANALYZERS))
def test_analyzer_returns_the_metrics_schema(deequ_spark, data, schemas, function):
    entry, expected = ANALYZER_CASES[function]
    out, _ = run_one(deequ_spark, data, "analysis", {"function": function, **entry})
    assert out.schema == schemas["analysis"], f"{function}: {out.schema.simpleString()} != {METRICS_DDL}"
    metrics = {(r.entity, r.instance, r.name): r.value for r in out.collect()}
    assert metrics, f"{function} computed nothing"
    for key in expected:
        assert key in metrics and isinstance(metrics[key], float), (function, key, metrics)


# --- verification: every constraint -> check, check_level, ..., metric_value -----------------------

CONSTRAINT_CASES = {
    "is_complete": ({"columns": ["id", "note"]}, {"is_complete(id)": True, "is_complete(note)": False}),
    "are_complete": ({"columns": ["id", "amount"]}, {"are_complete(id, amount)": True}),
    "has_completeness": ({"columns": ["note"], "assertion": ">= 0.7"}, {"has_completeness(note)": True}),
    "is_unique": ({"columns": ["id", "country"]}, {"is_unique(id)": True, "is_unique(country)": False}),
    "is_primary_key": ({"columns": ["id", "country"]}, {"is_primary_key(id, country)": True}),
    "has_uniqueness": ({"columns": ["id"], "assertion": "== 1"}, {"has_uniqueness(id)": True}),
    "has_distinctness": ({"columns": ["country"], "assertion": "<= 0.1"}, {"has_distinctness(country)": True}),
    "has_unique_value_ratio": ({"columns": ["country"], "assertion": "between 0 and 1"}, {"has_unique_value_ratio(country)": True}),
    "satisfies": ({"expression": "amount > 0"}, {"satisfies(amount > 0)": True}),
    "is_contained_in": ({"columns": ["country"], "values": ["IN", "US", "DE"], "assertion": ">= 0.9"}, {"is_contained_in(country)": True}),
    "is_in_range": ({"columns": ["amount"], "lower_bound": 0, "upper_bound": 7}, {"is_in_range(amount)": True}),
    "has_pattern": ({"columns": ["country"], "pattern": "^[A-Z]{2}$"}, {"has_pattern(country)": True}),
    "is_non_negative": ({"columns": ["amount"]}, {"is_non_negative(amount)": True}),
    "is_positive": ({"columns": ["other"]}, {"is_positive(other)": False}),  # other starts at 0
    "is_less_than": ({"columns": ["amount", "other"], "assertion": ">= 0.9"}, {"is_less_than(amount, other)": True}),
    "is_less_than_or_equal_to": ({"columns": ["amount", "other"], "assertion": ">= 0.9"}, {"is_less_than_or_equal_to(amount, other)": True}),
    "is_greater_than": ({"columns": ["other", "amount"], "assertion": ">= 0.9"}, {"is_greater_than(other, amount)": True}),
    "is_greater_than_or_equal_to": ({"columns": ["other", "amount"], "assertion": ">= 0.9"}, {"is_greater_than_or_equal_to(other, amount)": True}),
    "has_data_type": ({"columns": ["code"], "data_type": "Integral"}, {"has_data_type(code)": True}),
    "contains_email": ({"columns": ["email"]}, {"contains_email(email)": True}),
    "contains_url": ({"columns": ["url"]}, {"contains_url(url)": True}),
    "contains_credit_card_number": ({"columns": ["country"], "assertion": "== 0"}, {"contains_credit_card_number(country)": True}),
    "contains_social_security_number": ({"columns": ["country"], "assertion": "== 0"}, {"contains_social_security_number(country)": True}),
    "has_size": ({"assertion": "== 100"}, {"has_size": True}),
    "has_min": ({"columns": ["amount"], "assertion": "== 0.5"}, {"has_min(amount)": True}),
    "has_max": ({"columns": ["amount"], "assertion": "== 6.5"}, {"has_max(amount)": True}),
    "has_mean": ({"columns": ["other"], "assertion": "== 49.5"}, {"has_mean(other)": True}),
    "has_sum": ({"columns": ["other"], "assertion": "== 4950"}, {"has_sum(other)": True}),
    "has_standard_deviation": ({"columns": ["other"], "assertion": "between 28 and 29"}, {"has_standard_deviation(other)": True}),
    "has_min_length": ({"columns": ["country"], "assertion": "== 2"}, {"has_min_length(country)": True}),
    "has_max_length": ({"columns": ["city"], "assertion": "== 6"}, {"has_max_length(city)": True}),
    "has_approx_count_distinct": ({"columns": ["country"], "assertion": "== 5"}, {"has_approx_count_distinct(country)": True}),
    "has_approx_quantile": ({"columns": ["other"], "quantile": 0.5, "assertion": "between 45 and 55"}, {"has_approx_quantile(other)": True}),
    "has_entropy": ({"columns": ["country"], "assertion": "> 1"}, {"has_entropy(country)": True}),
    "has_correlation": ({"columns": ["amount", "other"], "assertion": "between -1 and 1"}, {"has_correlation(amount, other)": True}),
}


def test_every_constraint_has_a_case():
    assert set(CONSTRAINT_CASES) == set(CONSTRAINTS)


@pytest.mark.parametrize("function", list(CONSTRAINTS))
def test_constraint_returns_the_check_results_schema(deequ_spark, data, schemas, function):
    entry, expected = CONSTRAINT_CASES[function]
    out, ctx = run_one(deequ_spark, data, "verification", {"function": function, **entry})
    assert out.schema == schemas["verification"], f"{function}: {out.schema.simpleString()} != {CHECK_RESULTS_DDL}"
    results = {r.constraint: r for r in out.collect()}
    assert {k: r.constraint_status == "success" for k, r in results.items()} == expected
    for r in results.values():
        assert (r.check, r.check_level) == ("dq", "error") and r.metric_name and isinstance(r.metric_value, float)
        assert (r.constraint_message is None) == (r.constraint_status == "success")
    # the same outcomes were reported for the run summary, previews and results sinks
    assert {r.check_id: r.passed for r in ctx.reports} == expected


# --- suggestion: every rule -> column_name, constraint_name, ..., code_for_constraint ---------------

RULE_CASES = {
    # rule -> (entry, a column it suggests something for, the rule class of every row)
    "default_rules": ({}, "id", None),
    "extended_rules": ({}, "amount", None),
    "complete_if_complete": ({}, "id", "CompleteIfCompleteRule"),
    "retain_completeness": ({}, "note", "RetainCompletenessRule"),
    "retain_type": ({}, "code", "RetainTypeRule"),
    "categorical_range": ({}, "city", "CategoricalRangeRule"),
    "fractional_categorical_range": ({}, "country", "FractionalCategoricalRangeRule"),
    "non_negative_numbers": ({"columns": ["amount", "id"]}, "amount", "NonNegativeNumbersRule"),
    "unique_if_approximately_unique": ({}, "id", "UniqueIfApproximatelyUniqueRule"),
    "has_min": ({"columns": ["amount"]}, "amount", "HasMin"),
    "has_max": ({"columns": ["amount"]}, "amount", "HasMax"),
    "has_mean": ({"columns": ["amount"]}, "amount", "HasMean"),
    "has_standard_deviation": ({"columns": ["amount"]}, "amount", "HasStandardDeviation"),
    "has_min_length": ({"columns": ["country", "city"]}, "city", "HasMinLength"),
    "has_max_length": ({"columns": ["country", "city"]}, "country", "HasMaxLength"),
}


def test_every_rule_has_a_case():
    assert set(RULE_CASES) == set(RULES)


@pytest.mark.parametrize("function", list(RULES))
def test_rule_returns_the_suggestions_schema(deequ_spark, data, schemas, function):
    entry, column, rule_class = RULE_CASES[function]
    out, _ = run_one(deequ_spark, data, "suggestion", {"function": function, **entry})
    assert out.schema == schemas["suggestion"], f"{function}: {out.schema.simpleString()} != {SUGGESTIONS_DDL}"
    suggestions = out.collect()
    assert column in {r.column_name for r in suggestions}, f"{function} suggested nothing for {column}"
    if rule_class:
        assert {r.suggesting_rule for r in suggestions} == {rule_class}
    if entry.get("columns"):
        assert {r.column_name for r in suggestions} <= set(entry["columns"])  # columns restrict the rule
    for r in suggestions:
        assert r.constraint_name and r.current_value and r.description and r.rule_description
        assert r.code_for_constraint.startswith(".")  # deequ's Check method call, e.g. .isComplete("id")


# --- the schema holds with no data too (schema analysis) --------------------------------------------


@pytest.mark.parametrize("runner", list(OUTPUT_DDL))
def test_schema_analysis_returns_the_same_schema(deequ_spark, data, schemas, runner):
    key = {"analysis": "analyzers", "verification": "constraints", "suggestion": "rules"}[runner]
    entry = {"analysis": {"function": "size"}, "verification": {"function": "has_size", "assertion": "> 0"},
             "suggestion": {"function": "default_rules"}}[runner]
    ctx = RunContext(spark=deequ_spark, registry=default_registry(), pipeline_id="p", env="test", preview=True, preview_limit=0)
    out = DeequTransform().apply(ctx, {"t": data}, DeequOptions(runner=runner, **{key: [entry]}))
    assert out.schema == schemas[runner] and out.count() == 0
