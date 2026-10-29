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


_PY4J_HEADER = "An error occurred while calling"


def short_error(exc: BaseException, limit: int = 1000) -> str:
    """One line a user can act on.

    Errors raised inside the JVM arrive as Py4J errors whose first line only says which
    JVM method failed ("An error occurred while calling o38.read."); the exception itself
    is on the next line, and the root cause, when there is one, in the last "Caused by:".
    """
    text = str(exc).strip() or type(exc).__name__
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if not lines:
        return type(exc).__name__
    first = lines[0]
    if first.startswith(_PY4J_HEADER) and len(lines) > 1:
        first = lines[1].lstrip(":").strip()
        causes = [line for line in lines[2:] if line.startswith("Caused by:")]
        if causes:
            first = f"{first} ({causes[-1]})"
    return first[:limit]
