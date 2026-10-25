from __future__ import annotations

from functools import lru_cache
from typing import Any

from fastapi import APIRouter
from tributary import export_schema

router = APIRouter(tags=["components"])


@lru_cache
def _schema() -> dict[str, Any]:
    return export_schema()


@router.get("/components")
def components() -> dict[str, Any]:
    """JSON Schema of the pipeline spec plus every plugin's metadata and options schema.

    The UI builds its palette and node forms from this."""
    return _schema()
