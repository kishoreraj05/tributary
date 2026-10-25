"""Run worker: ``python -m tributary_backend.worker RUN_ID``. Started by services.runner."""

from __future__ import annotations

import logging
import os
import sys
import traceback
from datetime import datetime, timezone

from sqlalchemy import select, update

from tributary import api
from tributary.errors import QualityFailure
from tributary.registry import Registry
from tributary.runtime import create_session, decide_resources

from .db import session_factory
from .models import Connection, PipelineVersion, Run
from .quality_sink import BackendMetricsSink
from .services.specs import materialize


def _update(run_id: str, *, expect: tuple[str, ...], **values) -> bool:
    with session_factory()() as session:
        result = session.execute(update(Run).where(Run.id == run_id, Run.status.in_(expect)).values(**values))
        session.commit()
        return result.rowcount == 1


def main(run_id: str) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s", stream=sys.stdout)
    logging.getLogger("py4j").setLevel(logging.WARNING)
    log = logging.getLogger("tributary.worker")

    with session_factory()() as session:
        run = session.get(Run, run_id)
        if run is None or run.status != "queued":
            log.error("run %s not found or not queued", run_id)
            return 1
        version = session.scalars(
            select(PipelineVersion).where(
                PipelineVersion.pipeline_id == run.pipeline_id, PipelineVersion.version == run.version
            )
        ).one()
        doc, env = materialize(version.spec, run.env, session.scalars(select(Connection)).all(), reveal=True)
        version_no = run.version

    for block in doc.get("quality") or []:
        block.setdefault("results_sinks", []).append({"type": BackendMetricsSink.type_name})
    registry = Registry()
    registry.register(BackendMetricsSink)

    now = datetime.now(timezone.utc)
    if not _update(run_id, expect=("queued",), status="running", started_at=now, pid=os.getpid()):
        log.warning("run %s was cancelled before it started", run_id)
        return 0
    log.info("run %s: pipeline version %s, env %s", run_id, version_no, env)
    try:
        compiled = api.compile_pipeline(doc, env, registry=registry, resolve_secrets=True)
        spark = create_session(compiled, decide_resources(compiled))
        spark.sparkContext.setLogLevel("WARN")
        result = api.run(compiled, spark=spark, run_id=run_id, version=str(version_no))
        if isinstance(result, api.RunSummary):
            summary = {
                "sinks_written": result.sinks_written,
                "checks": len(result.quality),
                "failed_checks": [r.check_id for r in result.quality if not r.passed],
            }
        else:
            summary = {"streaming": True, "query_id": str(result.id)}
        _update(run_id, expect=("running",), status="succeeded", finished_at=datetime.now(timezone.utc), summary=summary)
        log.info("run %s succeeded: %s", run_id, summary)
        return 0
    except QualityFailure as exc:
        log.error("%s", exc)
        failed = [r.check_id for r in exc.results if not r.passed]
        _update(run_id, expect=("running",), status="failed", finished_at=datetime.now(timezone.utc),
                error=str(exc), summary={"failed_checks": failed})
        return 2
    except Exception as exc:  # noqa: BLE001 - every failure must be recorded
        traceback.print_exc()
        message = (str(exc).strip() or type(exc).__name__).splitlines()[0][:1000]
        _update(run_id, expect=("running",), status="failed", finished_at=datetime.now(timezone.utc), error=message)
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
