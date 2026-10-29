from __future__ import annotations

import dataclasses
import logging
import uuid
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from ..registry import Registry
    from .quality import CheckResult


@dataclass
class RunContext:
    """Everything a plugin may need at runtime, injected by the executor."""

    spark: SparkSession
    registry: Registry
    pipeline_id: str
    env: str
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    version: str | None = None
    batch_id: int | None = None
    # Streaming runs: the trigger the query runs with (processing_time | available_now | once),
    # set before the stream source is read, so a source can adapt to (or refuse) it.
    trigger: str | None = None
    # Preview mode: bounded source reads, no metrics/quarantine side effects.
    preview: bool = False
    preview_limit: int = 20
    log: logging.Logger = field(default_factory=lambda: logging.getLogger("tributary"))
    # Callbacks to run when the run, micro-batch or preview ends (see ``defer``). Shared by
    # the copies ``for_batch`` makes, so the executor of a micro-batch releases what its
    # plugins set aside.
    cleanups: list[Callable[[], None]] = field(default_factory=list, repr=False)
    # The id of the node being evaluated (set by the executor for every step).
    node_id: str | None = None
    # Data-quality results handed over by steps that are not quality blocks (see ``report``).
    reports: list[CheckResult] = field(default_factory=list, repr=False)

    def for_batch(self, batch_id: int) -> RunContext:
        return dataclasses.replace(self, batch_id=batch_id, reports=[])

    def for_node(self, node_id: str) -> RunContext:
        """The context a step runs with: same run, same ``reports`` list, its own node id."""
        return dataclasses.replace(self, node_id=node_id)

    def defer(self, cleanup: Callable[[], None]) -> None:
        """Registers a cleanup (unpersist a cached RDD, close a client) that the executor
        runs once every sink has been written, or the preview collected."""
        self.cleanups.append(cleanup)

    def run_cleanups(self) -> None:
        while self.cleanups:
            cleanup = self.cleanups.pop()
            try:
                cleanup()
            except Exception:  # noqa: BLE001 - cleanup must not mask the real error
                self.log.warning("a deferred cleanup failed", exc_info=True)

    def report(self, results: Iterable[CheckResult]) -> None:
        """Surfaces check results evaluated by a plugin that is not a quality block (a deequ
        node, for instance). The executor collects them into the run summary and previews;
        sending them to metrics sinks stays the plugin's job."""
        self.reports.extend(results)

    def take_reports(self) -> list[CheckResult]:
        taken, self.reports[:] = list(self.reports), []
        return taken
