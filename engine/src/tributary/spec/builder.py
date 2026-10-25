"""Builds the full pipeline spec model from whatever plugins are registered."""

from __future__ import annotations

from typing import Annotated, Any, Literal, Optional, Union

from pydantic import BaseModel, Field, ValidationError, create_model

from ..errors import PluginError
from ..registry import Registry
from .models import QualityBase, SinkNodeBase, SourceNodeBase, SpecBase, TargetBase, TransformNodeBase

_cache: dict[tuple[int, int], type[BaseModel]] = {}


def _camel(name: str) -> str:
    return "".join(part.capitalize() for part in name.split("_"))


def _options_field(opts_cls: type[BaseModel]) -> tuple[Any, Any]:
    try:
        opts_cls()
    except ValidationError:
        return (opts_cls, ...)
    return (opts_cls, Field(default_factory=opts_cls))


def _union(models: list[type[BaseModel]], discriminator: str, what: str) -> Any:
    if not models:
        raise PluginError(f"no {what} plugins are registered")
    if len(models) == 1:
        return models[0]
    return Annotated[Union[tuple(models)], Field(discriminator=discriminator)]


def build_spec_model(registry: Registry) -> type[BaseModel]:
    registry.ensure_loaded()
    key = (id(registry), registry.version)
    if key in _cache:
        return _cache[key]

    def variants(category: str, suffix: str, base: type[BaseModel] | None, tag: str = "type", **extra: Any):
        models = []
        for name, plugin in sorted(registry.plugins(category).items()):
            fields: dict[str, Any] = {tag: (Literal[name], ...), **extra}
            if base is None:
                # Flat variant: the plugin's options are the node itself (checks, schemas).
                models.append(create_model(f"{_camel(name)}{suffix}", __base__=plugin.Options, **fields))
            else:
                fields["options"] = _options_field(plugin.Options)
                models.append(create_model(f"{_camel(name)}{suffix}", __base__=base, **fields))
        return models

    schema_u = _union(variants("schema", "SchemaSpec", None, tag="kind"), "kind", "schema")
    check_u = _union(variants("check", "Check", None, id=(Optional[str], None)), "type", "check")
    source_u = _union(
        variants("source", "SourceNode", SourceNodeBase, schema_=(Optional[schema_u], Field(None, alias="schema"))),
        "type",
        "source",
    )
    transform_u = _union(variants("transform", "TransformNode", TransformNodeBase), "type", "transform")
    sink_u = _union(variants("sink", "SinkNode", SinkNodeBase), "type", "sink")
    sink_target_u = _union(variants("sink", "SinkTarget", TargetBase), "type", "sink")
    metrics_u = _union(variants("metrics_sink", "MetricsTarget", TargetBase), "type", "metrics sink")

    quality = create_model(
        "QualitySpec",
        __base__=QualityBase,
        checks=(list[check_u], Field(..., min_length=1)),
        results_sinks=(list[metrics_u], Field(default_factory=list)),
        quarantine_sink=(Optional[sink_target_u], None),
    )
    model = create_model(
        "PipelineSpec",
        __base__=SpecBase,
        sources=(list[source_u], Field(..., min_length=1)),
        transforms=(list[transform_u], Field(default_factory=list)),
        quality=(list[quality], Field(default_factory=list)),
        sinks=(list[sink_u], Field(..., min_length=1)),
    )
    _cache[key] = model
    return model
