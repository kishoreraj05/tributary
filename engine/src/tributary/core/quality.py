from __future__ import annotations

from abc import abstractmethod
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any, ClassVar

from pydantic import Field

from .plugin import Plugin, PluginOptions

if TYPE_CHECKING:
    from pyspark.sql import Column

# Shape of one data-quality result row, shared by every metrics sink.
RESULT_COLUMNS: tuple[tuple[str, str], ...] = (
    ("run_id", "string"),
    ("pipeline_id", "string"),
    ("version", "string"),
    ("env", "string"),
    ("node_id", "string"),
    ("quality_id", "string"),
    ("check_id", "string"),
    ("check_type", "string"),
    ("level", "string"),
    ("action", "string"),
    ("total_rows", "bigint"),
    ("failed_rows", "bigint"),
    ("observed_value", "double"),
    ("passed", "boolean"),
    ("batch_id", "bigint"),
    ("ts", "string"),
)
RESULT_DDL = ", ".join(f"{name} {kind}" for name, kind in RESULT_COLUMNS)


@dataclass
class CheckResult:
    run_id: str
    pipeline_id: str
    version: str | None
    env: str
    node_id: str
    quality_id: str
    check_id: str
    check_type: str
    level: str
    action: str
    total_rows: int
    failed_rows: int | None
    observed_value: float | None
    passed: bool
    batch_id: int | None
    ts: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)

    def as_row(self) -> tuple:
        return tuple(getattr(self, name) for name, _ in RESULT_COLUMNS)


class RowCheckOptions(PluginOptions):
    max_failed_ratio: float = Field(
        0.0, ge=0.0, le=1.0, description="Fraction of rows allowed to fail before the check is marked failed"
    )


class Check(Plugin):
    category = "check"
    level: ClassVar[str] = "row"

    @classmethod
    def capabilities(cls) -> list[str]:
        return [cls.level]


class RowCheck(Check):
    """Evaluated per row. Failing rows can be dropped or quarantined."""

    level = "row"
    Options = RowCheckOptions

    @abstractmethod
    def failure_condition(self, opts: Any) -> Column:
        """Boolean column, true when the row violates the check. Null counts as passing."""

    def passed(self, failed_rows: int, total_rows: int, opts: Any) -> bool:
        if total_rows == 0:
            return True
        return failed_rows / total_rows <= getattr(opts, "max_failed_ratio", 0.0)


class DatasetCheck(Check):
    """Evaluated on the whole dataset (or micro-batch) via one aggregate expression."""

    level = "dataset"

    @abstractmethod
    def metric(self, opts: Any) -> Column:
        """An aggregate expression; it runs in the same single pass as all other checks."""

    @abstractmethod
    def passed(self, observed: float | None, total_rows: int, opts: Any) -> bool: ...
