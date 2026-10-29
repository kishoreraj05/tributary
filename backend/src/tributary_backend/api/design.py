"""Design-time operations on an unsaved spec: what the canvas calls while the user draws."""

from __future__ import annotations

import re
import shutil
import uuid
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from sqlalchemy.orm import Session
from tributary import compile_pipeline
from tributary.errors import SpecError, SpecIssue, TributaryError, short_error
from tributary.execution import analyze_pipeline, preview_node
from tributary.planner import build_plan
from tributary.runtime import decide_resources
from tributary.spec import restrict_to

from ..db import get_session
from ..schemas import PreviewRequest, SampleOut, SpecRequest, ValidateRequest
from ..services.spark import get_spark
from ..services.specs import issue_dict, materialize, spec_error_body
from ..settings import get_settings
from .deps import connections

router = APIRouter(tags=["design"])
_SAMPLE_ID = re.compile(r"^[0-9a-f]{32}\.(csv|json|jsonl|ndjson|parquet)$")


def _compile(session: Session, body: SpecRequest, *, reveal: bool):
    try:
        doc, env = materialize(body.spec, body.env, connections(session), reveal=reveal)
        return compile_pipeline(doc, env, resolve_secrets=reveal)
    except SpecError as exc:
        raise HTTPException(422, spec_error_body(exc, body.spec)) from None


def _spark_error(exc: Exception) -> HTTPException:
    return HTTPException(400, {"message": short_error(exc)})


@router.post("/validate")
def validate(body: ValidateRequest, session: Session = Depends(get_session)) -> dict[str, Any]:
    """Always 200: ``valid`` plus issues tagged with the node they belong to. With ``deep``
    every node is also analysed with Spark and returns its output schema (for autocomplete)."""
    try:
        doc, env = materialize(body.spec, body.env, connections(session), reveal=body.deep)
        compiled = compile_pipeline(doc, env, resolve_secrets=body.deep)
    except SpecError as exc:
        return {"valid": False, "issues": [issue_dict(i, body.spec) for i in exc.issues], "warnings": []}
    result: dict[str, Any] = {"valid": True, "env": compiled.env, "issues": [], "warnings": compiled.warnings}
    if body.deep:
        nodes = analyze_pipeline(compiled, get_spark())
        result["nodes"] = {k: vars(v) for k, v in nodes.items()}
        # What a sink found wrong with its options for the data it would get (a merge key
        # the upstream SQL does not produce, say) is reported like a compile issue: by spec
        # path, so the editor shows it under that field.
        for node in nodes.values():
            result["issues"] += [issue_dict(SpecIssue(i["path"], i["message"]), body.spec) for i in node.issues]
        result["valid"] = all(v.ok for v in nodes.values())
    return result


@router.post("/plan")
def plan(body: SpecRequest, session: Session = Depends(get_session)) -> dict[str, Any]:
    compiled = _compile(session, body, reveal=False)
    physical = build_plan(compiled)
    resources = decide_resources(compiled)
    steps = [
        {
            "id": s.id,
            "kind": s.kind,
            "type": s.type,
            "inputs": s.inputs,
            "attached_to": s.attached_to,
            "stream": s.stream,
            "reads": s.reads,
            "cache": s.cache,
            "persist_flags": s.persist_flags,
        }
        for s in (physical.steps[i] for i in physical.order)
    ]
    return {
        "explain": physical.explain(),
        "steps": steps,
        "resources": {
            "profile": resources.profile,
            "master": resources.master,
            "input_bytes": resources.input_bytes,
            "reasons": resources.reasons,
            "conf": resources.conf,
        },
        "warnings": compiled.warnings,
    }


@router.post("/preview")
def preview(body: PreviewRequest, session: Session = Depends(get_session)) -> dict[str, Any]:
    """Tests one node. Only the node and its upstream (plus the quality blocks attached to
    them) are compiled, so an unfinished node elsewhere on the canvas does not block it."""
    body = body.model_copy(update={"spec": restrict_to(body.spec, body.node_id)})
    compiled = _compile(session, body, reveal=True)
    kept = {s.id for s in compiled.spec.sources}
    samples = {source: str(_sample_path(sample_id)) for source, sample_id in body.sample_data.items() if source in kept}
    limit = min(body.limit, get_settings().preview_max_rows)
    try:
        result = preview_node(compiled, body.node_id, get_spark(), limit=limit, sample_data=samples, write_sinks=body.write,
                              explain=body.explain)
    except TributaryError as exc:
        raise HTTPException(400, {"message": str(exc)}) from None
    except Exception as exc:  # noqa: BLE001 - Spark analysis/runtime errors go back to the editor
        raise _spark_error(exc) from None
    return result.to_dict()


@router.post("/samples", response_model=SampleOut, status_code=201)
def upload_sample(file: UploadFile = File(...)) -> SampleOut:
    """Sample data (.csv, .json/.jsonl, .parquet) to stand in for a source in previews."""
    suffix = Path(file.filename or "").suffix.lower()
    if suffix not in (".csv", ".json", ".jsonl", ".ndjson", ".parquet"):
        raise HTTPException(422, "sample must be .csv, .json, .jsonl, .ndjson or .parquet")
    directory = get_settings().samples_dir
    directory.mkdir(parents=True, exist_ok=True)
    sample_id = f"{uuid.uuid4().hex}{suffix}"
    with open(directory / sample_id, "wb") as out:
        shutil.copyfileobj(file.file, out)
    return SampleOut(sample_id=sample_id, filename=file.filename or sample_id, size=(directory / sample_id).stat().st_size)


def _sample_path(sample_id: str) -> Path:
    path = get_settings().samples_dir / sample_id
    if not _SAMPLE_ID.match(sample_id) or not path.exists():
        raise HTTPException(404, f"sample '{sample_id}' not found")
    return path.resolve()
