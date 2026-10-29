from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from fastapi.responses import PlainTextResponse
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from tributary import compile_pipeline
from tributary.errors import SpecError
from tributary.spec import dump_document, parse_text

from ..db import get_session
from ..models import Pipeline, PipelineVersion, Run
from ..schemas import (
    ImportRequest,
    PipelineCreate,
    PipelineOut,
    PipelineSummary,
    PipelineUpdate,
    VersionCreate,
    VersionOut,
    VersionSummary,
)
from ..services.runner import ACTIVE
from ..services.specs import materialize, spec_error_body
from .deps import connections, pipeline_or_404

router = APIRouter(prefix="/pipelines", tags=["pipelines"])


def _commit_or_409(session: Session, name: str) -> None:
    try:
        session.commit()
    except IntegrityError:
        session.rollback()
        raise HTTPException(409, f"a pipeline named '{name}' already exists") from None


@router.get("", response_model=list[PipelineSummary])
def list_pipelines(session: Session = Depends(get_session)):
    return session.scalars(select(Pipeline).order_by(Pipeline.updated_at.desc())).all()


@router.post("", response_model=PipelineOut, status_code=201)
def create_pipeline(body: PipelineCreate, session: Session = Depends(get_session)):
    pipeline = Pipeline(name=body.name, description=body.description, draft_spec=body.spec, draft_layout=body.ui_layout)
    session.add(pipeline)
    _commit_or_409(session, body.name)
    return pipeline


@router.post("/import", response_model=PipelineOut, status_code=201)
def import_pipeline(body: ImportRequest, session: Session = Depends(get_session)):
    try:
        spec = parse_text(body.content, body.format)
    except SpecError as exc:
        raise HTTPException(422, spec_error_body(exc, {})) from None
    except Exception as exc:  # noqa: BLE001 - YAML/JSON syntax errors
        raise HTTPException(422, f"could not parse {body.format}: {exc}") from None
    return create_pipeline(PipelineCreate(name=body.name, description=body.description, spec=spec), session)


@router.get("/{pipeline_id}", response_model=PipelineOut)
def get_pipeline(pipeline_id: str, session: Session = Depends(get_session)):
    return pipeline_or_404(session, pipeline_id)


@router.put("/{pipeline_id}", response_model=PipelineOut)
def update_pipeline(pipeline_id: str, body: PipelineUpdate, session: Session = Depends(get_session)):
    """Saves the draft. Drafts are not validated: the canvas saves half-drawn pipelines."""
    pipeline = pipeline_or_404(session, pipeline_id)
    if body.name is not None:
        pipeline.name = body.name
    if body.description is not None:
        pipeline.description = body.description
    if body.spec is not None:
        pipeline.draft_spec = body.spec
    if body.ui_layout is not None:
        pipeline.draft_layout = body.ui_layout
    _commit_or_409(session, pipeline.name)
    return pipeline


@router.delete("/{pipeline_id}", status_code=204)
def delete_pipeline(pipeline_id: str, session: Session = Depends(get_session)):
    pipeline = pipeline_or_404(session, pipeline_id)
    active = session.scalars(select(Run.id).where(Run.pipeline_id == pipeline_id, Run.status.in_(ACTIVE))).first()
    if active:
        raise HTTPException(409, f"pipeline has an active run ({active}); cancel it first")
    session.delete(pipeline)
    session.commit()
    return Response(status_code=204)


@router.post("/{pipeline_id}/versions", response_model=VersionOut, status_code=201)
def create_version(pipeline_id: str, body: VersionCreate, session: Session = Depends(get_session)):
    """Freezes the current draft as the next immutable version, after validating it for
    every environment it defines (only valid pipelines can be versioned and run).

    Nodes with dummy data stand in for systems that may not be set up yet, so in the
    environments where they run for real (all but local) their missing settings do not
    block the version: they come back as warnings, and a run there fails until they are
    filled in. Everything else must be valid everywhere."""
    pipeline = pipeline_or_404(session, pipeline_id, lock=True)
    spec = pipeline.draft_spec or {}
    envs = list((spec.get("environments") or {}).keys()) or [None]
    conns = connections(session)
    warnings: list[str] = []
    for env in envs:
        doc, env_name = None, env
        try:
            doc, env_name = materialize(spec, env, conns, reveal=False)
            compile_pipeline(doc, env_name)
        except SpecError as exc:
            try:
                # The same environment with the dummy nodes standing in: if that is valid,
                # the only problems were the dummy nodes' own settings.
                if doc is None:
                    raise
                compile_pipeline(doc, env_name, use_dummies=True)
            except SpecError as blocking:
                body_ = spec_error_body(blocking, spec)
                body_["env"] = env
                raise HTTPException(422, body_) from None
            warnings += [f"{env_name}: {i.path}: {i.message}" for i in exc.issues]
    pipeline.latest_version += 1
    version = PipelineVersion(
        pipeline_id=pipeline.id,
        version=pipeline.latest_version,
        spec=spec,
        ui_layout=pipeline.draft_layout or {},
        message=body.message,
    )
    session.add(version)
    session.commit()
    return VersionOut.model_validate(version).model_copy(update={"warnings": warnings})


@router.get("/{pipeline_id}/versions", response_model=list[VersionSummary])
def list_versions(pipeline_id: str, session: Session = Depends(get_session)):
    return pipeline_or_404(session, pipeline_id).versions


def _version(session: Session, pipeline_id: str, version: int) -> PipelineVersion:
    row = session.scalars(
        select(PipelineVersion).where(PipelineVersion.pipeline_id == pipeline_id, PipelineVersion.version == version)
    ).first()
    if row is None:
        raise HTTPException(404, f"version {version} not found")
    return row


@router.get("/{pipeline_id}/versions/{version}", response_model=VersionOut)
def get_version(pipeline_id: str, version: int, session: Session = Depends(get_session)):
    pipeline_or_404(session, pipeline_id)
    return _version(session, pipeline_id, version)


@router.post("/{pipeline_id}/versions/{version}/restore", response_model=PipelineOut)
def restore_version(pipeline_id: str, version: int, session: Session = Depends(get_session)):
    """Copies a version back into the draft."""
    pipeline = pipeline_or_404(session, pipeline_id)
    row = _version(session, pipeline_id, version)
    pipeline.draft_spec = row.spec
    pipeline.draft_layout = row.ui_layout
    session.commit()
    return pipeline


@router.get("/{pipeline_id}/export", response_class=PlainTextResponse)
def export_pipeline(
    pipeline_id: str,
    version: int | None = Query(None, description="Defaults to the draft"),
    format: str = Query("yaml", pattern="^(yaml|json)$"),
    session: Session = Depends(get_session),
):
    pipeline = pipeline_or_404(session, pipeline_id)
    spec = _version(session, pipeline_id, version).spec if version is not None else pipeline.draft_spec
    media = "application/json" if format == "json" else "application/yaml"
    return PlainTextResponse(dump_document(spec, format), media_type=media)
