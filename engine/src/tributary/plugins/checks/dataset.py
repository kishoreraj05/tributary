from __future__ import annotations

from pydantic import Field
from pyspark.sql import functions as F

from ...core import DatasetCheck, PluginOptions


class UniqueOptions(PluginOptions):
    columns: list[str] = Field(..., min_length=1)
    max_duplicates: int = Field(0, ge=0)


class UniqueCheck(DatasetCheck):
    """Number of duplicate rows over the key columns (observed value = duplicates)."""

    type_name = "unique"
    Options = UniqueOptions

    def metric(self, opts: UniqueOptions):
        return F.count(F.lit(1)) - F.countDistinct(F.struct(*opts.columns))

    def passed(self, observed, total_rows, opts: UniqueOptions) -> bool:
        return (observed or 0) <= opts.max_duplicates


class RowCountOptions(PluginOptions):
    min: int | None = Field(1, ge=0)
    max: int | None = Field(None, ge=0)


class RowCountCheck(DatasetCheck):
    """Row count within [min, max]."""

    type_name = "row_count"
    Options = RowCountOptions

    def metric(self, opts: RowCountOptions):
        return F.count(F.lit(1))

    def passed(self, observed, total_rows, opts: RowCountOptions) -> bool:
        count = observed or 0
        return (opts.min is None or count >= opts.min) and (opts.max is None or count <= opts.max)
