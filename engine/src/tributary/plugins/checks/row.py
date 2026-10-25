from __future__ import annotations

from functools import reduce
from typing import Any

from pydantic import Field, model_validator
from pyspark.sql import functions as F

from ...core import RowCheck, RowCheckOptions


class NotNullOptions(RowCheckOptions):
    column: str | None = None
    columns: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _some_column(self) -> NotNullOptions:
        if not self.column and not self.columns:
            raise ValueError("set 'column' or 'columns'")
        return self


class NotNullCheck(RowCheck):
    """Fails rows where any of the columns is null."""

    type_name = "not_null"
    Options = NotNullOptions

    def failure_condition(self, opts: NotNullOptions):
        cols = ([opts.column] if opts.column else []) + opts.columns
        return reduce(lambda a, b: a | b, [F.col(c).isNull() for c in cols])


class RangeOptions(RowCheckOptions):
    column: str
    min: float | int | str | None = None
    max: float | int | str | None = None
    inclusive: bool = True

    @model_validator(mode="after")
    def _some_bound(self) -> RangeOptions:
        if self.min is None and self.max is None:
            raise ValueError("set 'min' and/or 'max'")
        return self


class RangeCheck(RowCheck):
    """Fails rows whose value is outside [min, max]. Nulls pass (use not_null for those)."""

    type_name = "range"
    Options = RangeOptions

    def failure_condition(self, opts: RangeOptions):
        col = F.col(opts.column)
        conds = []
        if opts.min is not None:
            conds.append(col < F.lit(opts.min) if opts.inclusive else col <= F.lit(opts.min))
        if opts.max is not None:
            conds.append(col > F.lit(opts.max) if opts.inclusive else col >= F.lit(opts.max))
        return reduce(lambda a, b: a | b, conds)


class RegexOptions(RowCheckOptions):
    column: str
    pattern: str = Field(..., description="Java regex the value must match")


class RegexCheck(RowCheck):
    """Fails rows whose value does not match the pattern."""

    type_name = "regex"
    Options = RegexOptions

    def failure_condition(self, opts: RegexOptions):
        return ~F.col(opts.column).rlike(opts.pattern)


class AcceptedValuesOptions(RowCheckOptions):
    column: str
    values: list[Any] = Field(..., min_length=1)


class AcceptedValuesCheck(RowCheck):
    """Fails rows whose value is not one of the accepted values."""

    type_name = "accepted_values"
    Options = AcceptedValuesOptions

    def failure_condition(self, opts: AcceptedValuesOptions):
        return ~F.col(opts.column).isin(opts.values)


class ExpressionOptions(RowCheckOptions):
    expr: str = Field(..., description="SQL boolean expression every row must satisfy")


class ExpressionCheck(RowCheck):
    """Fails rows where the SQL expression is false."""

    type_name = "expression"
    Options = ExpressionOptions

    def failure_condition(self, opts: ExpressionOptions):
        return ~F.expr(opts.expr)
