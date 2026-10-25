from __future__ import annotations

import asyncio

from fastapi import APIRouter, Depends, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.concurrency import run_in_threadpool
from sqlalchemy import select
from sqlalchemy.orm import Session
from tributary import compile_pipeline
from tributary.errors import SpecError

from ..db import get_session, session_factory
from ..models import PipelineVersion, QualityResult, Run
from ..schemas import QualityOut, RunCreate, RunOut
from ..services import runner
from ..services.specs import materialize, spec_error_body
from .deps import connections, pipeline_or_404

router = APIRouter(tags=["runs"])


def _run_or_404(session: Session, run_id: str) -> Run:
    run = session.get(Run, run_id)
    if run is None:
        raise HTTPException(404, f"run '{run_id}' not found")
    return runner.refresh(session, run)


@router.post("/pipelines/{pipeline_id}/runs", response_model=RunOut, status_code=202)
def start_run(pipeline_id: str, body: RunCreate, session: Session = Depends(get_session)):
    pipeline = pipeline_or_404(session, pipeline_id)
    number = body.version or pipeline.latest_version
    if number == 0:
        raise HTTPException(409, "pipeline has no versions yet; save a version before running")
    version = session.scalars(
        select(PipelineVersion).where(PipelineVersion.pipeline_id == pipeline_id, PipelineVersion.version == number)
    ).first()
    if version is None:
        raise HTTPException(404, f"version {number} not found")
    try:
        # Masked secrets: the snapshot is safe to store; the worker decrypts its own copy.
        doc, env = materialize(version.spec, body.env, connections(session), reveal=False)
        compiled = compile_pipeline(doc, env)
    except SpecError as exc:
        raise HTTPException(422, spec_error_body(exc, version.spec)) from None
    run = Run(pipeline_id=pipeline_id, version=number, env=env, mode=compiled.mode, resolved_spec=doc, summary={})
    session.add(run)
    session.commit()
    return runner.launch(session, run)


@router.get("/runs", response_model=list[RunOut])
def list_runs(
    pipeline_id: str | None = None,
    status: str | None = None,
    limit: int = Query(50, ge=1, le=500),
    session: Session = Depends(get_session),
):
    query = select(Run).order_by(Run.created_at.desc()).limit(limit)
    if pipeline_id:
        query = query.where(Run.pipeline_id == pipeline_id)
    if status:
        query = query.where(Run.status == status)
    return [runner.refresh(session, r) for r in session.scalars(query).all()]


@router.get("/runs/{run_id}", response_model=RunOut)
def get_run(run_id: str, session: Session = Depends(get_session)):
    return _run_or_404(session, run_id)


@router.post("/runs/{run_id}/cancel", response_model=RunOut)
def cancel_run(run_id: str, session: Session = Depends(get_session)):
    return runner.cancel(session, _run_or_404(session, run_id))


@router.get("/runs/{run_id}/logs")
def run_logs(run_id: str, tail: int = Query(200, ge=1, le=5000), session: Session = Depends(get_session)):
    run = _run_or_404(session, run_id)
    return {"run_id": run.id, "status": run.status, "lines": runner.tail(run.log_path, tail)}


@router.get("/runs/{run_id}/quality", response_model=list[QualityOut])
def run_quality(run_id: str, session: Session = Depends(get_session)):
    _run_or_404(session, run_id)
    return session.scalars(
        select(QualityResult).where(QualityResult.run_id == run_id).order_by(QualityResult.batch_id, QualityResult.id)
    ).all()


@router.get("/quality", response_model=list[QualityOut])
def quality_history(
    pipeline_id: str = Query(..., description="Backend pipeline id"),
    check_id: str | None = None,
    passed: bool | None = None,
    limit: int = Query(200, ge=1, le=5000),
    session: Session = Depends(get_session),
):
    """DQ results across runs, newest first (for trend charts)."""
    query = (
        select(QualityResult)
        .join(Run, Run.id == QualityResult.run_id)
        .where(Run.pipeline_id == pipeline_id)
        .order_by(QualityResult.id.desc())
        .limit(limit)
    )
    if check_id:
        query = query.where(QualityResult.check_id == check_id)
    if passed is not None:
        query = query.where(QualityResult.passed == passed)
    return session.scalars(query).all()


def _snapshot(run_id: str) -> dict | None:
    with session_factory()() as session:
        run = session.get(Run, run_id)
        if run is None:
            return None
        return RunOut.model_validate(runner.refresh(session, run)).model_dump(mode="json") | {"log_path": run.log_path}


@router.websocket("/runs/{run_id}/events")
async def run_events(websocket: WebSocket, run_id: str, poll_seconds: float = 1.0):
    """Streams ``{"type": "status", "run": ...}`` on every status change and
    ``{"type": "log", "lines": [...]}`` as the log grows; closes once the run is finished."""
    await websocket.accept()
    offset, last_status = 0, None
    try:
        while True:
            snap = await run_in_threadpool(_snapshot, run_id)
            if snap is None:
                await websocket.send_json({"type": "error", "message": f"run '{run_id}' not found"})
                break
            log_path = snap.pop("log_path")
            if snap["status"] != last_status:
                last_status = snap["status"]
                await websocket.send_json({"type": "status", "run": snap})
            lines, offset = await run_in_threadpool(runner.read_from, log_path, offset)
            if lines:
                await websocket.send_json({"type": "log", "lines": lines})
            elif last_status in runner.TERMINAL:
                break
            await asyncio.sleep(poll_seconds)
        await websocket.close()
    except WebSocketDisconnect:
        pass
