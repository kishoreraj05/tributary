from __future__ import annotations

from typing import Any

from ..registry import Registry, default_registry
from .builder import build_spec_model


def export_schema(registry: Registry | None = None) -> dict[str, Any]:
    """JSON Schema of the full pipeline spec plus the component catalog (for the UI)."""
    registry = registry or default_registry()
    return {
        "pipeline": build_spec_model(registry).model_json_schema(by_alias=True),
        "components": registry.catalog(),
    }
