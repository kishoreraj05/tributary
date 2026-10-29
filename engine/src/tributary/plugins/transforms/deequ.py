"""Amazon Deequ (https://github.com/awslabs/deequ) as a node in the transform layer.

Deequ has three runners, and the node is one of them at a time (``runner``):

* ``analysis`` runs an ``AnalysisRunner``: analyzers compute metrics, one row each
  (``entity, instance, name, value``).
* ``verification`` runs a ``VerificationSuite``: constraints are checked and the node
  outputs one row per constraint (``check, check_level, check_status, constraint,
  constraint_status, constraint_message, metric_name, metric_value``), or, with
  ``output: data``, hands its input on unchanged and acts as a gate that fails the run
  (``fail_on``) and reports its results like a quality block.
* ``suggestion`` runs a ``ConstraintSuggestionRunner``: rules profile the columns and
  suggest constraints, one row each (``column_name, constraint_name, current_value,
  description, suggesting_rule, rule_description, code_for_constraint``).

Each runner has its own list of functions (analyzers, constraints, rules). An entry names
the function and the columns it applies to: most functions are applied to every listed
column separately; pair functions (``correlation``, ``is_less_than`` ...) take exactly two;
composite ones (``is_primary_key``, ``are_complete``) take them together; dataset-level ones
(``size``, ``has_size``, ``satisfies``) take none. Functions have type requirements, the
same ones deequ enforces (``isNumeric``, ``isString``, ``isNotNested``): they are checked
against the input schema before anything runs, naming the column and its type.

Deequ is a Scala library; the node drives it through Spark's JVM gateway (what PyDeequ does
as well) and needs no Python package beyond PySpark. Assertions (``>= 0.95``) are Python
objects the JVM calls back into, which is why the node needs a classic Spark session rather
than Spark Connect. The jar comes from Maven Central through ``spark.jars.packages``;
``TRIBUTARY_DEEQU_VERSION`` selects another release (it must match the Spark minor version).
"""

from __future__ import annotations

import math
import os
import re
import threading
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, ClassVar, Literal

import sqlglot
from pydantic import BaseModel, Field, ValidationInfo, field_validator, model_validator
from pyspark.sql import DataFrame, functions as F
from pyspark.sql import types as T

from ...core import CheckResult, PluginOptions, RunContext, Target, Transform, targets
from ...errors import QualityFailure, TributaryError, short_error
from .._util import when

DEEQU_VERSION = os.environ.get("TRIBUTARY_DEEQU_VERSION", "2.0.7-spark-3.5")
DEEQU_PACKAGE = f"com.amazon.deequ:deequ:{DEEQU_VERSION}"

Runner = Literal["analysis", "verification", "suggestion"]
RUNNERS: tuple[str, ...] = ("analysis", "verification", "suggestion")
CheckLevel = Literal["error", "warning"]
FailOn = Literal["error", "warning", "never"]
VerificationOutput = Literal["check_results", "data"]
DataType = Literal["Null", "Fractional", "Integral", "Boolean", "String", "Numeric"]

METRICS_DDL = "entity STRING, instance STRING, name STRING, value DOUBLE"
CHECK_RESULTS_DDL = (
    "check STRING, check_level STRING, check_status STRING, constraint STRING, constraint_status STRING, "
    "constraint_message STRING, metric_name STRING, metric_value DOUBLE"
)
SUGGESTIONS_DDL = (
    "column_name STRING, constraint_name STRING, current_value STRING, description STRING, "
    "suggesting_rule STRING, rule_description STRING, code_for_constraint STRING"
)
OUTPUT_DDL = {"analysis": METRICS_DDL, "verification": CHECK_RESULTS_DDL, "suggestion": SUGGESTIONS_DDL}


# --- the function catalogues ----------------------------------------------------------------
# One table per runner drives the options models (which fields a function takes, which are
# mandatory), the "x-when" hints the UI uses to show only the relevant fields, the column
# type checks and the JVM calls.

Arity = Literal["none", "each", "all", "pair"]
# Column type requirements, as deequ's Preconditions name them (plus what comparisons need).
# Every column function needs a simple (not nested) column; "any" means nothing more.
TypeRule = Literal["any", "numeric", "string", "comparable"]


@dataclass(frozen=True)
class Fn:
    arity: str = "each"  # none: no columns; each: one application per column; all: together; pair: exactly two
    types: str = "any"  # what the columns must be
    fields: tuple[str, ...] = ()  # extra option fields the function takes
    required: tuple[str, ...] = ()  # those that are mandatory
    assertion: str = "-"  # verification: "none" (deequ has none), "optional" (defaults to == 1), "required"
    fraction: bool = False  # verification: the metric is the fraction of rows satisfying a predicate
    deequ: str = ""  # the deequ name, for the docs and the JVM calls
    optional_columns: bool = False  # suggestion rules: columns only restrict what is profiled


def _fn(arity="each", types="any", fields="", required=None, assertion="-", fraction=False, deequ="") -> Fn:
    f = tuple(fields.split()) if fields else ()
    return Fn(arity, types, f, f if required is None else tuple(required.split()), assertion, fraction, deequ)


def _rule(deequ: str) -> Fn:
    return Fn("all", "any", (), (), "-", False, deequ, optional_columns=True)


ANALYZERS: dict[str, Fn] = {
    "size": _fn("none", deequ="Size"),
    "column_count": _fn("none", deequ="ColumnCount"),
    "completeness": _fn(deequ="Completeness"),
    "uniqueness": _fn(deequ="Uniqueness"),
    "distinctness": _fn(deequ="Distinctness"),
    "unique_value_ratio": _fn(deequ="UniqueValueRatio"),
    "count_distinct": _fn(deequ="CountDistinct"),
    "approx_count_distinct": _fn(deequ="ApproxCountDistinct"),
    "entropy": _fn(deequ="Entropy"),
    "histogram": _fn(deequ="Histogram"),
    "data_type": _fn(deequ="DataType"),
    "mean": _fn(types="numeric", deequ="Mean"),
    "minimum": _fn(types="numeric", deequ="Minimum"),
    "maximum": _fn(types="numeric", deequ="Maximum"),
    "sum": _fn(types="numeric", deequ="Sum"),
    "standard_deviation": _fn(types="numeric", deequ="StandardDeviation"),
    "approx_quantile": _fn(types="numeric", fields="quantile", deequ="ApproxQuantile"),
    "min_length": _fn(types="string", deequ="MinLength"),
    "max_length": _fn(types="string", deequ="MaxLength"),
    "pattern_match": _fn(types="string", fields="pattern", deequ="PatternMatch"),
    "correlation": _fn("pair", "numeric", deequ="Correlation"),
    "mutual_information": _fn("pair", deequ="MutualInformation"),
    "compliance": _fn("none", fields="expression", deequ="Compliance"),
}

CONSTRAINTS: dict[str, Fn] = {
    # completeness
    "is_complete": _fn(assertion="none", fraction=True, deequ="isComplete"),
    "are_complete": _fn("all", assertion="none", fraction=True, deequ="areComplete"),
    "has_completeness": _fn(assertion="required", fraction=True, deequ="hasCompleteness"),
    # uniqueness
    "is_unique": _fn(assertion="none", fraction=True, deequ="isUnique"),
    "is_primary_key": _fn("all", assertion="none", fraction=True, deequ="isPrimaryKey"),
    "has_uniqueness": _fn(assertion="required", fraction=True, deequ="hasUniqueness"),
    "has_distinctness": _fn(assertion="required", fraction=True, deequ="hasDistinctness"),
    "has_unique_value_ratio": _fn(assertion="required", deequ="hasUniqueValueRatio"),
    # values
    "satisfies": _fn("none", fields="expression", assertion="optional", fraction=True, deequ="satisfies"),
    "is_contained_in": _fn(fields="values", assertion="optional", fraction=True, deequ="isContainedIn"),
    "is_in_range": _fn(
        types="numeric", fields="lower_bound upper_bound include_lower include_upper",
        required="lower_bound upper_bound", assertion="none", fraction=True, deequ="isContainedIn",
    ),
    "has_pattern": _fn(types="string", fields="pattern", assertion="optional", fraction=True, deequ="hasPattern"),
    "is_non_negative": _fn(types="numeric", assertion="optional", fraction=True, deequ="isNonNegative"),
    "is_positive": _fn(types="numeric", assertion="optional", fraction=True, deequ="isPositive"),
    "is_less_than": _fn("pair", "comparable", assertion="optional", fraction=True, deequ="isLessThan"),
    "is_less_than_or_equal_to": _fn("pair", "comparable", assertion="optional", fraction=True, deequ="isLessThanOrEqualTo"),
    "is_greater_than": _fn("pair", "comparable", assertion="optional", fraction=True, deequ="isGreaterThan"),
    "is_greater_than_or_equal_to": _fn("pair", "comparable", assertion="optional", fraction=True, deequ="isGreaterThanOrEqualTo"),
    "has_data_type": _fn(fields="data_type", assertion="optional", fraction=True, deequ="hasDataType"),
    "contains_email": _fn(types="string", assertion="optional", fraction=True, deequ="containsEmail"),
    "contains_url": _fn(types="string", assertion="optional", fraction=True, deequ="containsURL"),
    "contains_credit_card_number": _fn(types="string", assertion="optional", fraction=True, deequ="containsCreditCardNumber"),
    "contains_social_security_number": _fn(types="string", assertion="optional", fraction=True, deequ="containsSocialSecurityNumber"),
    # statistics
    "has_size": _fn("none", assertion="required", deequ="hasSize"),
    "has_min": _fn(types="numeric", assertion="required", deequ="hasMin"),
    "has_max": _fn(types="numeric", assertion="required", deequ="hasMax"),
    "has_mean": _fn(types="numeric", assertion="required", deequ="hasMean"),
    "has_sum": _fn(types="numeric", assertion="required", deequ="hasSum"),
    "has_standard_deviation": _fn(types="numeric", assertion="required", deequ="hasStandardDeviation"),
    "has_min_length": _fn(types="string", assertion="required", deequ="hasMinLength"),
    "has_max_length": _fn(types="string", assertion="required", deequ="hasMaxLength"),
    "has_approx_count_distinct": _fn(assertion="required", deequ="hasApproxCountDistinct"),
    "has_approx_quantile": _fn(types="numeric", fields="quantile", assertion="required", deequ="hasApproxQuantile"),
    "has_entropy": _fn(assertion="required", deequ="hasEntropy"),
    "has_correlation": _fn("pair", "numeric", assertion="required", deequ="hasCorrelation"),
}

# Suggestion rules profile the columns they are given (all columns when none are listed).
RULES: dict[str, Fn] = {
    "default_rules": _rule("Rules.DEFAULT"),
    "extended_rules": _rule("Rules.EXTENDED"),
    "complete_if_complete": _rule("CompleteIfCompleteRule"),
    "retain_completeness": _rule("RetainCompletenessRule"),
    "retain_type": _rule("RetainTypeRule"),
    "categorical_range": _rule("CategoricalRangeRule"),
    "fractional_categorical_range": _rule("FractionalCategoricalRangeRule"),
    "non_negative_numbers": _rule("NonNegativeNumbersRule"),
    "unique_if_approximately_unique": _rule("UniqueIfApproximatelyUniqueRule"),
    "has_min": _rule("HasMin"),
    "has_max": _rule("HasMax"),
    "has_mean": _rule("HasMean"),
    "has_standard_deviation": _rule("HasStandardDeviation"),
    "has_min_length": _rule("HasMinLength"),
    "has_max_length": _rule("HasMaxLength"),
}

CATALOGUES: dict[str, dict[str, Fn]] = {"analysis": ANALYZERS, "verification": CONSTRAINTS, "suggestion": RULES}


def _takes(field: str, fn: Fn) -> bool:
    if field == "assertion":
        return fn.assertion not in ("-", "none")
    if field == "columns":
        return fn.arity != "none"
    return field in fn.fields


def _needs(field: str, fn: Fn) -> bool:
    if field == "assertion":
        return fn.assertion == "required"
    if field == "columns":
        return fn.arity != "none" and not fn.optional_columns
    return field in fn.required


def _when(catalogue: dict[str, Fn], field: str) -> dict:
    """x-when hint: the functions the field applies to / is mandatory for."""
    applies = tuple(name for name, fn in catalogue.items() if _takes(field, fn))
    required = tuple(name for name, fn in catalogue.items() if _needs(field, fn))
    return when("function", applies, required)


# --- assertions ------------------------------------------------------------------------------

_NUMBER = r"[-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?"
_COMPARISON = re.compile(rf"^\s*(==|!=|>=|<=|>|<)?\s*({_NUMBER})\s*$")
_BETWEEN = re.compile(rf"^\s*between\s+({_NUMBER})\s+and\s+({_NUMBER})\s*$", re.IGNORECASE)
ASSERTION_SYNTAX = "'>= 0.95', '< 10', '== 1', '!= 0' or 'between 1 and 100' (inclusive)"


@dataclass(frozen=True)
class Assertion:
    """What deequ's ``Double => Boolean`` lambda is in a spec: a comparison of the metric."""

    op: str
    value: float
    upper: float | None = None

    @classmethod
    def parse(cls, text: str) -> Assertion:
        match = _BETWEEN.match(text)
        if match:
            lo, hi = float(match.group(1)), float(match.group(2))
            if lo > hi:
                raise ValueError(f"invalid assertion '{text.strip()}': the lower bound is above the upper bound")
            return cls("between", lo, hi)
        match = _COMPARISON.match(text)
        if not match:
            raise ValueError(f"invalid assertion '{text.strip()}'; use {ASSERTION_SYNTAX}")
        return cls(match.group(1) or "==", float(match.group(2)))

    def holds(self, x: float) -> bool:
        if math.isnan(x):
            return False
        if self.op == "between":
            return self.value <= x <= self.upper  # type: ignore[operator]
        if self.op in ("==", "!="):
            equal = math.isclose(x, self.value, rel_tol=1e-9, abs_tol=1e-12)
            return equal if self.op == "==" else not equal
        return {">=": x >= self.value, "<=": x <= self.value, ">": x > self.value, "<": x < self.value}[self.op]

    def __str__(self) -> str:
        if self.op == "between":
            return f"between {self.value:g} and {self.upper:g}"
        return f"{self.op} {self.value:g}"


IS_ONE = Assertion("==", 1.0)


# --- options ------------------------------------------------------------------------------------


class Entry(PluginOptions):
    """A function applied to columns. Subclasses pin ``function`` to a runner's catalogue."""

    catalogue: ClassVar[dict[str, Fn]] = {}
    conditional: ClassVar[tuple[str, ...]] = ("columns",)

    function: str
    columns: list[str] = Field(default_factory=list, validate_default=True)

    @field_validator("columns", "expression", "pattern", "values", "lower_bound", "upper_bound", "include_lower",
                     "include_upper", "data_type", "quantile", "assertion", check_fields=False)
    @classmethod
    def _applies_to_function(cls, value: Any, info: ValidationInfo) -> Any:
        fn = cls.catalogue.get(info.data.get("function"))  # missing when 'function' is invalid: reported there
        if fn is None or info.field_name is None:
            return value
        name, function = info.field_name, info.data["function"]
        empty = value is None or value == [] or value == ""
        if _needs(name, fn) and empty:
            raise ValueError(f"required for {function}")
        if not _takes(name, fn) and not empty and value != cls.model_fields[name].default:
            raise ValueError(f"{function} takes no {name}" if name == "columns" else f"does not apply to {function}")
        if name == "columns" and fn.arity == "pair" and len(value) != 2:
            raise ValueError(f"{function} compares exactly two columns, got {len(value)}")
        if name == "assertion" and not empty:
            Assertion.parse(value)
        return value

    @property
    def fn(self) -> Fn:
        return self.catalogue[self.function]

    def applications(self) -> list[tuple[str, ...]]:
        """The column tuples the function is applied to: one per column, or one for all."""
        if self.fn.arity == "each":
            return [(c,) for c in self.columns]
        return [tuple(self.columns)]

    def label(self, columns: tuple[str, ...]) -> str:
        name = getattr(self, "name", None)
        if name:
            return f"{name}[{columns[0]}]" if self.fn.arity == "each" and len(self.columns) > 1 else name
        inner = ", ".join(columns) if columns else getattr(self, "expression", None) or ""
        return f"{self.function}({inner})" if inner else self.function


class Named(Entry):
    name: str | None = Field(None, description="Name in the results; defaults to function(column)")
    where: str | None = Field(None, description="SQL filter; the function runs on matching rows only")


class Analyzer(Named):
    catalogue = ANALYZERS

    function: Literal[tuple(ANALYZERS)] = Field(..., description="The deequ analyzer")  # type: ignore[valid-type]
    columns: list[str] = Field(
        default_factory=list, validate_default=True,
        description="Applied to each column; correlation and mutual_information take exactly two",
        json_schema_extra=_when(ANALYZERS, "columns"),
    )
    expression: str | None = Field(
        None, validate_default=True, description="SQL boolean expression; the metric is the fraction of rows satisfying it",
        json_schema_extra=_when(ANALYZERS, "expression"),
    )
    pattern: str | None = Field(
        None, validate_default=True, description="Java regular expression", json_schema_extra=_when(ANALYZERS, "pattern")
    )
    quantile: float | None = Field(None, ge=0.0, le=1.0, validate_default=True, json_schema_extra=_when(ANALYZERS, "quantile"))


class Constraint(Named):
    catalogue = CONSTRAINTS

    function: Literal[tuple(CONSTRAINTS)] = Field(..., description="The deequ constraint (Check method)")  # type: ignore[valid-type]
    columns: list[str] = Field(
        default_factory=list, validate_default=True,
        description="Applied to each column; is_primary_key and are_complete take them together, "
        "comparisons and has_correlation take exactly two",
        json_schema_extra=_when(CONSTRAINTS, "columns"),
    )
    level: CheckLevel = Field("error", description="A failing 'warning' constraint only fails the run with fail_on: warning")
    assertion: str | None = Field(
        None, validate_default=True,
        description=f"Condition on the metric, e.g. {ASSERTION_SYNTAX}. Predicate constraints "
        "(is_*, satisfies, has_pattern, contains_*) default to '== 1': every row must satisfy them",
        json_schema_extra=_when(CONSTRAINTS, "assertion"),
    )
    expression: str | None = Field(
        None, validate_default=True, description="SQL boolean expression every row must satisfy",
        json_schema_extra=_when(CONSTRAINTS, "expression"),
    )
    pattern: str | None = Field(
        None, validate_default=True, description="Java regular expression the values must match",
        json_schema_extra=_when(CONSTRAINTS, "pattern"),
    )
    values: list[str] = Field(
        default_factory=list, validate_default=True, description="The allowed values", json_schema_extra=_when(CONSTRAINTS, "values")
    )
    lower_bound: float | None = Field(None, validate_default=True, json_schema_extra=_when(CONSTRAINTS, "lower_bound"))
    upper_bound: float | None = Field(None, validate_default=True, json_schema_extra=_when(CONSTRAINTS, "upper_bound"))
    include_lower: bool = Field(True, validate_default=True, json_schema_extra=_when(CONSTRAINTS, "include_lower"))
    include_upper: bool = Field(True, validate_default=True, json_schema_extra=_when(CONSTRAINTS, "include_upper"))
    data_type: DataType | None = Field(
        None, validate_default=True, description="The type every value must have", json_schema_extra=_when(CONSTRAINTS, "data_type")
    )
    quantile: float | None = Field(None, ge=0.0, le=1.0, validate_default=True, json_schema_extra=_when(CONSTRAINTS, "quantile"))
    hint: str | None = Field(None, description="Added to the failure message")

    @property
    def assertion_(self) -> Assertion | None:
        if self.assertion:
            return Assertion.parse(self.assertion)
        return None if self.fn.assertion == "none" else IS_ONE


class Rule(Entry):
    catalogue = RULES

    function: Literal[tuple(RULES)] = Field(..., description="The suggestion rule (default_rules: deequ's default set)")  # type: ignore[valid-type]
    columns: list[str] = Field(default_factory=list, description="Columns the rule profiles; all columns when empty")


_RUNNER_FIELDS = {"analysis": "analyzers", "verification": "constraints", "suggestion": "rules"}


class DeequOptions(PluginOptions):
    runner: Runner = Field(
        ...,
        description="analysis: compute metrics (AnalysisRunner). verification: check constraints "
        "(VerificationSuite). suggestion: profile the columns and suggest constraints (ConstraintSuggestionRunner). "
        "Each produces its own output columns",
    )
    analyzers: list[Analyzer] = Field(
        default_factory=list, validate_default=True, description="Metrics to compute",
        json_schema_extra=when("runner", ("analysis",), ("analysis",)),
    )
    constraints: list[Constraint] = Field(
        default_factory=list, validate_default=True, description="Constraints to check",
        json_schema_extra=when("runner", ("verification",), ("verification",)),
    )
    rules: list[Rule] = Field(
        default_factory=list, validate_default=True, description="Rules that suggest constraints",
        json_schema_extra=when("runner", ("suggestion",), ("suggestion",)),
    )
    output: VerificationOutput = Field(
        "check_results", validate_default=True,
        description="check_results: one row per constraint. data: the input, unchanged (the node is a gate)",
        json_schema_extra=when("runner", ("verification",)),
    )
    fail_on: FailOn = Field(
        "error", validate_default=True,
        description="When to fail the run: a failing error-level constraint (default), any failing constraint, "
        "or never (results are recorded only). Previews never fail; they show the results",
        json_schema_extra=when("runner", ("verification",)),
    )
    results_sinks: list[Target] = Field(
        default_factory=list, validate_default=True,
        description="Where to send the results, one row per constraint (besides the run history)",
        json_schema_extra={**when("runner", ("verification",)), **targets("metrics_sink")},
    )

    @field_validator("analyzers", "constraints", "rules", "output", "fail_on", "results_sinks")
    @classmethod
    def _applies_to_runner(cls, value: Any, info: ValidationInfo) -> Any:
        runner = info.data.get("runner")
        if runner is None or info.field_name is None:
            return value
        name = info.field_name
        if name in _RUNNER_FIELDS.values():
            if _RUNNER_FIELDS[runner] == name:
                if not value:
                    raise ValueError(f"the {runner} runner needs at least one entry")
            elif value:
                raise ValueError(f"only the {next(r for r, f in _RUNNER_FIELDS.items() if f == name)} runner takes {name}")
        elif runner != "verification" and value != cls.model_fields[name].default and value != []:
            raise ValueError("only applies to the verification runner")
        return value

    @property
    def entries(self) -> list[Entry]:
        return list(getattr(self, _RUNNER_FIELDS[self.runner]))


# --- column types -----------------------------------------------------------------------------

_NUMERIC = (T.ByteType, T.ShortType, T.IntegerType, T.LongType, T.FloatType, T.DoubleType, T.DecimalType)
_STRING = (T.StringType, T.VarcharType, T.CharType)
_TEMPORAL = (T.DateType, T.TimestampType, T.TimestampNTZType)
_NESTED = (T.ArrayType, T.MapType, T.StructType)
_FAMILY_HINT = {
    "numeric": "a numeric column (tinyint, smallint, int, bigint, float, double or decimal)",
    "string": "a string column",
}


def type_family(data_type: T.DataType) -> str:
    if isinstance(data_type, _NUMERIC):
        return "numeric"
    if isinstance(data_type, _STRING):
        return "string"
    if isinstance(data_type, _TEMPORAL):
        return "temporal"
    if isinstance(data_type, T.BooleanType):
        return "boolean"
    if isinstance(data_type, _NESTED):
        return "nested"
    return "other"


def type_issue(fn: Fn, columns: tuple[str, ...], schema: T.StructType) -> str | None:
    """Why the function cannot be applied to these columns, or None. Same rules as deequ's
    preconditions (isNumeric, isString, isNotNested), spelled out with the column's type."""
    fields = {f.name.lower(): f for f in schema.fields}
    typed = [(c, fields[c.lower()].dataType) for c in columns if c.lower() in fields]
    for column, data_type in typed:
        if type_family(data_type) == "nested":
            return f"column '{column}' is {data_type.simpleString()}; deequ works on simple columns, not arrays, maps or structs"
    if fn.types in ("numeric", "string"):
        for column, data_type in typed:
            if type_family(data_type) != fn.types:
                return f"column '{column}' is {data_type.simpleString()}, but this function needs {_FAMILY_HINT[fn.types]}"
    elif fn.types == "comparable" and len(typed) == 2:
        (a, ta), (b, tb) = typed
        families = {type_family(ta), type_family(tb)}
        if not families <= {"numeric", "string", "temporal"} or len(families) != 1:
            return (
                f"cannot compare '{a}' ({ta.simpleString()}) with '{b}' ({tb.simpleString()}): both columns must be "
                "numeric, both strings, or both dates/timestamps"
            )
    return None


# --- the JVM side -------------------------------------------------------------------------------

_CALLBACK_LOCK = threading.Lock()


class _Function1:
    """A ``scala.Function1[Double, Boolean]`` the JVM calls back into (Py4J callback server)."""

    def __init__(self, assertion: Assertion):
        self.assertion = assertion

    def apply(self, value: Any) -> bool:
        return self.assertion.holds(float(value))

    def toString(self) -> str:  # noqa: N802 - Java names
        return str(self.assertion)

    def hashCode(self) -> int:  # noqa: N802
        return hash(self.assertion) & 0x7FFFFFFF

    def equals(self, other: Any) -> bool:
        return False

    class Java:
        implements = ["scala.Function1"]


@dataclass
class Metric:
    entity: str
    instance: str
    name: str
    value: float | None
    error: str | None = None


@dataclass
class ConstraintOutcome:
    level: str
    check_status: str
    entry: Constraint
    label: str
    status: str  # Success | Failure
    message: str | None
    metric: Metric | None

    @property
    def passed(self) -> bool:
        return self.status == "Success"


@dataclass
class Verification:
    status: str  # Success | Warning | Error
    size: float | None
    constraints: list[ConstraintOutcome]

    def fails(self, fail_on: str) -> bool:
        return (fail_on == "error" and self.status == "Error") or (fail_on == "warning" and self.status != "Success")


class _Jvm:
    """Deequ's Scala API over Py4J, bound to one session."""

    def __init__(self, spark):
        jvm = getattr(spark, "_jvm", None)
        if jvm is None or getattr(spark, "sparkContext", None) is None:
            raise TributaryError(
                "the deequ node drives deequ through the Spark JVM gateway and needs a classic Spark "
                "session; it does not run over Spark Connect"
            )
        self.spark, self.jvm = spark, jvm
        gateway = spark.sparkContext._gateway
        from pyspark.java_gateway import ensure_callback_server_started

        with _CALLBACK_LOCK:
            ensure_callback_server_started(gateway)
        self.gateway = gateway
        try:
            self.deequ = jvm.com.amazon.deequ
            self.none = getattr(getattr(jvm.scala, "None$"), "MODULE$")
            self.nil = getattr(getattr(jvm.scala.collection.immutable, "List$"), "MODULE$").empty()
            self.deequ.checks.CheckLevel.Error()  # fails fast when the jar is not on the session
        except Exception as exc:  # noqa: BLE001
            raise TributaryError(
                f"deequ is not available in this Spark session ({short_error(exc, 200)}); "
                f"the package {DEEQU_PACKAGE} must be on the classpath"
            ) from None

    # -- Scala values ---------------------------------------------------------------------------------

    def option(self, value: Any):
        return self.none if value is None else self.jvm.scala.Some(value)

    def seq(self, items: list[str] | tuple[str, ...]):
        out = self.nil
        for item in reversed(items):
            out = getattr(out, "$colon$colon")(item)
        return out

    def string_array(self, items: list[str]):
        array = self.gateway.new_array(self.jvm.java.lang.String, len(items))
        for i, item in enumerate(items):
            array[i] = item
        return array

    def regex(self, pattern: str):
        return self.jvm.scala.util.matching.Regex(pattern, self.nil)

    def function(self, assertion: Assertion):
        return self.deequ.checks.Check.IsOne() if assertion == IS_ONE else _Function1(assertion)

    @staticmethod
    def _iterate(collection):
        iterator = collection.iterator()
        while iterator.hasNext():
            yield iterator.next()

    def _metric(self, jmetric) -> Metric:
        value, error = None, None
        try_ = jmetric.value()
        if try_.isSuccess():
            try:
                value = float(try_.get())
            except (TypeError, ValueError):  # not a DoubleMetric (histograms are flattened elsewhere)
                value = None
        else:
            error = try_.failed().get().getMessage()
        return Metric(jmetric.entity().toString(), jmetric.instance(), jmetric.name(), value, error)

    def _flatten(self, jmetric) -> list[Metric]:
        """Every metric as double metrics: a histogram becomes one row per bin, like deequ's
        successMetricsAsDataFrame."""
        return [self._metric(m) for m in self._iterate(jmetric.flatten())]

    # -- analysis ------------------------------------------------------------------------------------

    def analyzer(self, e: Analyzer, columns: tuple[str, ...]):
        a, none, where = self.deequ.analyzers, self.none, self.option(e.where)
        f, col = e.function, columns[0] if columns else None
        if f == "size":
            return a.Size(where)
        if f == "column_count":
            return a.ColumnCount()
        if f == "completeness":
            return a.Completeness(col, where, none)
        if f == "uniqueness":
            return a.Uniqueness(self.seq(columns), where, none)
        if f == "distinctness":
            return a.Distinctness(self.seq(columns), where)
        if f == "unique_value_ratio":
            return a.UniqueValueRatio(self.seq(columns), where, none)
        if f == "count_distinct":
            return a.CountDistinct(self.seq(columns))
        if f == "approx_count_distinct":
            return a.ApproxCountDistinct(col, where)
        if f == "entropy":
            return a.Entropy(col, where)
        if f == "histogram":
            hist = a.Histogram
            return hist(col, none, getattr(hist, "apply$default$3")(), where, getattr(hist, "apply$default$5")(),
                        getattr(hist, "apply$default$6")())
        if f == "data_type":
            return a.DataType(col, where)
        if f in ("mean", "sum", "standard_deviation"):
            return getattr(a, e.fn.deequ)(col, where)
        if f in ("minimum", "maximum", "min_length", "max_length"):
            return getattr(a, e.fn.deequ)(col, where, none)
        if f == "approx_quantile":
            return a.ApproxQuantile(col, float(e.quantile), 0.01, where)
        if f == "pattern_match":
            return a.PatternMatch(col, self.regex(e.pattern), where, none)
        if f == "correlation":
            return a.Correlation(columns[0], columns[1], where)
        if f == "mutual_information":
            return a.MutualInformation(self.seq(columns), where)
        if f == "compliance":
            return a.Compliance(e.label(columns), e.expression, where, self.nil, none)
        raise TributaryError(f"unsupported deequ analyzer '{f}'")  # pragma: no cover

    def analyze(self, df: DataFrame, entries: list[Analyzer]) -> list[Metric]:
        builder = self.deequ.analyzers.runners.AnalysisRunner.onData(df._jdf)
        for e in entries:
            for columns in e.applications():
                builder = builder.addAnalyzer(self.analyzer(e, columns))
        context = builder.run()
        metrics: list[Metric] = []
        for pair in self._iterate(context.metricMap()):
            metrics.extend(self._flatten(pair._2()))
        return metrics

    # -- verification ---------------------------------------------------------------------------------

    def add_constraint(self, check, c: Constraint, columns: tuple[str, ...]):
        none, hint = self.none, self.option(c.hint)
        fn = self.function(c.assertion_) if c.assertion_ is not None else None
        f, col, label = c.function, columns[0] if columns else None, c.label(columns)
        if f == "is_complete":
            out = check.isComplete(col, hint, none)
        elif f == "are_complete":
            out = check.areComplete(self.seq(columns), hint)
        elif f == "has_completeness":
            out = check.hasCompleteness(col, fn, hint, none)
        elif f == "is_unique":
            out = check.isUnique(col, hint, none)
        elif f == "is_primary_key":
            out = check.isPrimaryKey(columns[0], hint, none, self.seq(columns[1:]))
        elif f == "has_uniqueness":
            out = check.hasUniqueness(self.seq(columns), fn, hint, none)
        elif f == "has_distinctness":
            out = check.hasDistinctness(self.seq(columns), fn, hint)
        elif f == "has_unique_value_ratio":
            out = check.hasUniqueValueRatio(self.seq(columns), fn, hint, none)
        elif f == "satisfies":
            out = check.satisfies(c.expression, label, fn, hint, self.nil, none)
        elif f == "is_contained_in":
            out = check.isContainedIn(col, self.string_array(c.values), fn, hint, none)
        elif f == "is_in_range":
            out = check.isContainedIn(col, float(c.lower_bound), float(c.upper_bound), c.include_lower, c.include_upper, hint, none)
        elif f == "has_pattern":
            out = check.hasPattern(col, self.regex(c.pattern), fn, none, hint, none)
        elif f in ("is_non_negative", "is_positive", "contains_email", "contains_url", "contains_credit_card_number",
                   "contains_social_security_number"):
            out = getattr(check, c.fn.deequ)(col, fn, hint)
        elif f in ("is_less_than", "is_less_than_or_equal_to", "is_greater_than", "is_greater_than_or_equal_to", "has_correlation"):
            out = getattr(check, c.fn.deequ)(columns[0], columns[1], fn, hint)
        elif f == "has_data_type":
            out = check.hasDataType(col, getattr(self.deequ.constraints.ConstrainableDataTypes, c.data_type)(), fn, hint)
        elif f == "has_size":
            out = check.hasSize(fn, hint)
        elif f in ("has_min", "has_max", "has_min_length", "has_max_length"):
            out = getattr(check, c.fn.deequ)(col, fn, hint, none)
        elif f in ("has_mean", "has_sum", "has_standard_deviation", "has_approx_count_distinct", "has_entropy"):
            out = getattr(check, c.fn.deequ)(col, fn, hint)
        elif f == "has_approx_quantile":
            out = check.hasApproxQuantile(col, float(c.quantile), fn, hint)
        else:  # pragma: no cover - the options model only allows the functions above
            raise TributaryError(f"unsupported deequ constraint '{f}'")
        return out.where(c.where) if c.where else out

    def verify(self, df: DataFrame, entries: list[Constraint], node_id: str) -> Verification:
        """One deequ Check per level, in the order the constraints are written."""
        levels = self.deequ.checks.CheckLevel
        by_level: dict[str, list[tuple[Constraint, tuple[str, ...]]]] = defaultdict(list)
        for c in entries:
            for columns in c.applications():
                by_level[c.level].append((c, columns))
        builder = self.deequ.VerificationSuite().onData(df._jdf)
        names = {}
        for level, items in by_level.items():
            names[level] = node_id if level == "error" else f"{node_id} ({level})"
            check = self.deequ.checks.Check(levels.Error() if level == "error" else levels.Warning(), names[level], self.nil)
            for c, columns in items:
                check = self.add_constraint(check, c, columns)
            builder = builder.addCheck(check)
        builder = builder.addRequiredAnalyzer(self.deequ.analyzers.Size(self.none))  # total rows, always
        result = builder.run()

        size = None
        for pair in self._iterate(result.metrics()):
            m = self._metric(pair._2())
            if m.name == "Size" and m.instance == "*":
                size = m.value
        outcomes: list[ConstraintOutcome] = []
        by_name = {name: level for level, name in names.items()}
        for pair in self._iterate(result.checkResults()):
            jcheck, jresult = pair._1(), pair._2()
            level = by_name[jcheck.description()]
            status = jresult.status().toString()
            for (c, columns), jc in zip(by_level[level], self._iterate(jresult.constraintResults())):
                metric = self._metric(jc.metric().get()) if jc.metric().isDefined() else None
                if metric is not None and c.function == "has_data_type" and metric.value is None:
                    metric.value = self._data_type_ratio(jc.metric().get(), c.data_type)
                outcomes.append(ConstraintOutcome(
                    level=level,
                    check_status=status,
                    entry=c,
                    label=c.label(columns),
                    status=jc.status().toString(),
                    message=jc.message().get() if jc.message().isDefined() else None,
                    metric=metric,
                ))
        return Verification(result.status().toString(), size, outcomes)

    @staticmethod
    def _data_type_ratio(jmetric, data_type: str | None) -> float | None:
        """hasDataType asserts on a histogram of inferred types: the fraction deequ checks."""
        keys = {"Null": ["Unknown"], "Numeric": ["Fractional", "Integral"]}.get(data_type or "", [data_type])
        try_ = jmetric.value()
        if not try_.isSuccess():
            return None
        values = try_.get().values()
        ratio = 0.0
        for key in keys:
            entry = values.get(key)
            if entry.isDefined():
                ratio += float(entry.get().ratio())
        return ratio

    # -- suggestion -------------------------------------------------------------------------------------

    def rules(self, entry: Rule):
        s = self.deequ.suggestions
        if entry.function == "default_rules":
            return list(self._iterate(s.Rules.DEFAULT()))
        if entry.function == "extended_rules":
            return list(self._iterate(s.Rules.EXTENDED()))
        cls = getattr(s.rules, entry.fn.deequ)
        if entry.function == "categorical_range":
            return [cls(getattr(cls, "apply$default$1")())]
        if entry.function == "fractional_categorical_range":
            return [cls(getattr(cls, "apply$default$1")(), getattr(cls, "apply$default$2")())]
        return [cls()]

    def suggest(self, df: DataFrame, entries: list[Rule]) -> list[tuple]:
        """Rules restricted to the same columns run together; deequ restricts columns per run.
        Without columns a rule profiles every simple column (the profiler rejects nested ones)."""
        simple = tuple(f.name for f in df.schema.fields if type_family(f.dataType) != "nested")
        groups: dict[tuple[str, ...], list[Rule]] = defaultdict(list)
        for e in entries:
            groups[tuple(e.columns) or simple].append(e)
        rows: list[tuple] = []
        for columns, group in groups.items():
            builder = self.deequ.suggestions.ConstraintSuggestionRunner().onData(df._jdf)
            for e in group:
                for rule in self.rules(e):
                    builder = builder.addConstraintRule(rule)
            builder = builder.restrictToColumns(self.seq(columns))
            result = builder.run()
            for pair in self._iterate(result.constraintSuggestions()):
                for s in self._iterate(pair._2()):
                    rule = s.suggestingRule()
                    row = (
                        s.columnName(), s.constraint().toString(), s.currentValue(), s.description(),
                        rule.getClass().getSimpleName(), rule.ruleDescription(), s.codeForConstraint(),
                    )
                    if row not in rows:  # default_rules and a rule it contains suggest the same thing
                        rows.append(row)
        return rows


# --- the node -------------------------------------------------------------------------------------


class DeequTransform(Transform):
    """Amazon Deequ on the node's input: compute metrics, check constraints (and gate the data), or suggest constraints."""

    type_name = "deequ"
    display_name = "Deequ"
    description = (
        "Data quality with Amazon Deequ. Pick a runner: analysis computes metrics, verification checks "
        "constraints (and can pass the data on, failing the run when they fail), suggestion profiles the "
        "columns and proposes constraints. Each function is applied to the columns you list."
    )
    icon = "checklist"
    Options = DeequOptions
    spark_packages = (DEEQU_PACKAGE,)
    chainable = False  # verify, analyze or suggest from the node to check; not from another deequ node

    def validate(self, opts: DeequOptions, input_aliases: list[str]) -> list[str]:
        issues = []
        if len(input_aliases) != 1:
            issues.append(f"a deequ node reads exactly one input, got {len(input_aliases)}")
        for e in opts.entries:
            for field_name in ("expression", "where"):
                text = getattr(e, field_name, None)
                if text:
                    try:
                        sqlglot.parse_one(text, read="spark")
                    except sqlglot.errors.ParseError as exc:
                        issues.append(f"{e.label(tuple(e.columns))}: {field_name} syntax error: {str(exc).splitlines()[0]}")
        return issues

    def complexity_hint(self, opts: DeequOptions) -> int:
        return 1  # one aggregation pass (uniqueness and suggestions add shuffles)

    def is_passthrough(self, opts: DeequOptions) -> bool:
        return opts.runner == "verification" and opts.output == "data"

    def apply(self, ctx: RunContext, inputs: dict[str, DataFrame], opts: DeequOptions) -> DataFrame:
        if len(inputs) != 1:
            raise TributaryError(f"a deequ node reads exactly one input, got {len(inputs)}")
        [(alias, df)] = inputs.items()
        self.check_columns(df, opts)
        passthrough = self.is_passthrough(opts)
        if ctx.preview and ctx.preview_limit <= 0:
            # Schema analysis, no rows: only the shape of the output matters.
            return df if passthrough else ctx.spark.createDataFrame([], OUTPUT_DDL[opts.runner])

        jvm = _Jvm(ctx.spark)
        if opts.runner == "analysis":
            metrics = jvm.analyze(df, opts.analyzers)
            failed = [m for m in metrics if m.value is None]
            for m in failed:
                ctx.log.warning("deequ %s: %s(%s) could not be computed: %s", ctx.node_id, m.name, m.instance, m.error)
            ctx.log.info("deequ %s on %s: %d metrics computed, %d failed", ctx.node_id, alias, len(metrics) - len(failed), len(failed))
            rows = [(m.entity, m.instance, m.name, m.value) for m in metrics if m.value is not None]
            return ctx.spark.createDataFrame(rows, METRICS_DDL)

        if opts.runner == "suggestion":
            rows = jvm.suggest(df, opts.rules)
            ctx.log.info("deequ %s on %s: %d constraints suggested", ctx.node_id, alias, len(rows))
            return ctx.spark.createDataFrame(rows, SUGGESTIONS_DDL)

        verification = jvm.verify(df, opts.constraints, ctx.node_id or self.type_name)
        results = self._results(ctx, alias, opts, verification)
        failing = [r.check_id for r in results if not r.passed]
        ctx.log.info(
            "deequ %s on %s: %s, %d constraints, %d failing %s",
            ctx.node_id, alias, verification.status.lower(), len(results), len(failing), failing,
        )
        for o in verification.constraints:
            if not o.passed:
                ctx.log.warning("deequ %s: %s failed: %s", ctx.node_id, o.label, o.message)
        ctx.report(results)
        if not ctx.preview:
            for target in opts.results_sinks:
                plugin = ctx.registry.create("metrics_sink", target.type)
                plugin.emit(ctx, results, _target_options(plugin, target))
            if verification.fails(opts.fail_on):
                raise QualityFailure(ctx.node_id or self.type_name, results)
        if passthrough:
            return df
        rows = [
            (node_check_name(ctx, o), o.level, o.check_status.lower(), o.label, o.status.lower(), o.message,
             o.metric.name if o.metric else None, o.metric.value if o.metric else None)
            for o in verification.constraints
        ]
        return ctx.spark.createDataFrame(rows, CHECK_RESULTS_DDL)

    @staticmethod
    def check_columns(df: DataFrame, opts: DeequOptions) -> None:
        """Unknown columns, wrong column types and bad expressions fail here, before any job
        runs, with the function and the column named; deequ itself would only report a failed
        metric (or compute something meaningless)."""
        known = {c.lower() for c in df.columns}
        for e in opts.entries:
            for columns in e.applications():
                where = e.label(columns)
                for column in columns:
                    if column.lower() not in known:
                        raise TributaryError(f"{where}: unknown column '{column}' (columns: {', '.join(df.columns)})")
                issue = type_issue(e.fn, columns, df.schema)
                if issue:
                    raise TributaryError(f"{where}: {issue}")
            for field_name in ("expression", "where"):
                text = getattr(e, field_name, None)
                if text:
                    try:
                        df.filter(F.expr(text)).schema  # analysis only
                    except Exception as exc:  # noqa: BLE001 - Spark's analysis error, with the function named
                        raise TributaryError(f"{e.label(tuple(e.columns))}: invalid {field_name}: {short_error(exc, 300)}") from None

    @staticmethod
    def _results(ctx: RunContext, alias: str, opts: DeequOptions, verification: Verification) -> list[CheckResult]:
        total = int(verification.size or 0)
        ts = datetime.now(timezone.utc).isoformat()
        results = []
        for o in verification.constraints:
            value = o.metric.value if o.metric else None
            # A predicate over rows: the metric is the fraction of rows passing, so the failing
            # rows are known (unless a 'where' filter narrowed the rows the metric covers).
            row_level = o.entry.fn.fraction and not o.entry.where and value is not None
            enforced = opts.fail_on == "warning" or (opts.fail_on == "error" and o.level == "error")
            results.append(
                CheckResult(
                    run_id=ctx.run_id,
                    pipeline_id=ctx.pipeline_id,
                    version=ctx.version,
                    env=ctx.env,
                    node_id=alias,
                    quality_id=ctx.node_id or "deequ",
                    check_id=o.label,
                    check_type="deequ",
                    level="row" if row_level else "dataset",
                    action="fail" if enforced else "observe",
                    total_rows=total,
                    failed_rows=int(round((1.0 - min(max(value, 0.0), 1.0)) * total)) if row_level else None,
                    observed_value=value,
                    passed=o.passed,
                    batch_id=ctx.batch_id,
                    ts=ts,
                )
            )
        return results


def node_check_name(ctx: RunContext, outcome: ConstraintOutcome) -> str:
    node = ctx.node_id or "deequ"
    return node if outcome.level == "error" else f"{node} ({outcome.level})"


def _target_options(plugin: Any, target: Any) -> Any:
    """Targets are typed once the spec model is built; plain ``Target`` entries (options built
    by hand) still carry a dict."""
    options = target.options
    return options if isinstance(options, BaseModel) else plugin.Options.model_validate(options)
