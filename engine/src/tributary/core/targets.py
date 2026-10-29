"""Metrics sinks (or sinks) embedded in another plugin's options.

A quality block sends its results to ``results_sinks``; a plugin that evaluates checks of
its own (the deequ node) wants the same. It declares a field as ``list[Target]`` with
``json_schema_extra=targets("metrics_sink")`` and the engine does the rest: the spec model
validates every entry against the registered plugins of that category (with its options
model), connections referenced by an entry are merged into it, the Spark packages and
settings of the targets are loaded, and the UI renders a target picker for the field.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from .plugin import PluginOptions

TARGETS_KEY = "x-targets"


class Target(PluginOptions):
    """One embedded target as written in the spec: ``{type, connection?, options}``.

    Once the spec model is built, entries are instances of the per-plugin target models
    (``ConsoleMetricsTarget`` ...) whose ``options`` is the plugin's Options model."""

    type: str
    connection: str | None = None
    options: dict[str, Any] = Field(default_factory=dict)


def targets(category: str) -> dict[str, str]:
    """``json_schema_extra`` marking a ``list[Target]`` field as targets of ``category``."""
    return {TARGETS_KEY: category}


def target_fields(options_cls: type[BaseModel]) -> dict[str, str]:
    """Field name -> plugin category, for every target list an options model declares."""
    out: dict[str, str] = {}
    for name, info in options_cls.model_fields.items():
        extra = info.json_schema_extra
        if isinstance(extra, dict) and isinstance(extra.get(TARGETS_KEY), str):
            out[name] = extra[TARGETS_KEY]
    return out


def embedded_targets(options: BaseModel) -> list[tuple[str, Any]]:
    """(category, target) for every target in a validated options model."""
    return [(category, t) for name, category in target_fields(type(options)).items() for t in getattr(options, name)]
