from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterable

if TYPE_CHECKING:
    from .core.quality import CheckResult


class TributaryError(Exception):
    """Base class for all engine errors."""


@dataclass(frozen=True)
class SpecIssue:
    path: str
    message: str

    def __str__(self) -> str:
        return f"{self.path}: {self.message}" if self.path else self.message


class SpecError(TributaryError):
    """The pipeline spec is invalid. Carries every issue found, not just the first."""

    def __init__(self, issues: Iterable[SpecIssue]):
        self.issues = list(issues)
        super().__init__("invalid pipeline spec:\n" + "\n".join(f"  - {i}" for i in self.issues))


class PluginError(TributaryError):
    """A plugin is missing, misconfigured or violates its contract."""


class SecretNotFound(TributaryError):
    pass


class QualityFailure(TributaryError):
    """A quality block with action 'fail' had failing checks."""

    def __init__(self, quality_id: str, results: list[CheckResult]):
        self.quality_id = quality_id
        self.results = results
        failed = ", ".join(r.check_id for r in results if not r.passed)
        super().__init__(f"quality '{quality_id}' failed: {failed}")
