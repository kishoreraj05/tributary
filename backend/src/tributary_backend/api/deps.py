from __future__ import annotations

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..models import Connection, Pipeline


def connections(session: Session) -> list[Connection]:
    return list(session.scalars(select(Connection)).all())


def pipeline_or_404(session: Session, pipeline_id: str, *, lock: bool = False) -> Pipeline:
    pipeline = session.get(Pipeline, pipeline_id, with_for_update=lock)
    if pipeline is None:
        raise HTTPException(404, f"pipeline '{pipeline_id}' not found")
    return pipeline
