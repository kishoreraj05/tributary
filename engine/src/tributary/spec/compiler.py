"""Raw document -> validated, environment-resolved pipeline."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from ..errors import PluginError, SpecError, SpecIssue
from ..registry import Registry, default_registry
from .builder import build_spec_model
from .loader import Interpolator, SecretResolver, load_document
from .models import EnvironmentSpec, RuntimeSpec
from .validation import validate_semantics


@dataclass
class CompiledPipeline:
    spec: Any
    env: str
    registry: Registry
    warnings: list[str] = field(default_factory=list)

    @property
    def pipeline_id(self) -> str:
        return self.spec.pipeline.id

    @property
    def mode(self) -> str:
        return self.spec.pipeline.mode

    @property
    def runtime(self) -> RuntimeSpec:
        return self.spec.runtime.get(self.env) or RuntimeSpec()


def compile_pipeline(
    source: str | Path | Mapping[str, Any] | CompiledPipeline,
    env: str | None = None,
    *,
    registry: Registry | None = None,
    resolve_secrets: bool = False,
) -> CompiledPipeline:
    if isinstance(source, CompiledPipeline):
        return source
    registry = registry or default_registry()
    doc = load_document(source)
    issues: list[SpecIssue] = []

    env_name, env_spec = _select_env(doc, env, issues)
    if issues:
        raise SpecError(issues)

    secrets = SecretResolver(registry) if resolve_secrets else None
    variables = Interpolator({}, secrets, issues).resolve(env_spec.vars, f"environments.{env_name}.vars")
    _merge_connections(doc, env_spec.connections, registry, issues)
    interpolator = Interpolator(variables, secrets, issues)
    # The environments section itself is kept verbatim; every other section is resolved.
    resolved = {k: (v if k == "environments" else interpolator.resolve(v, k)) for k, v in doc.items()}
    if issues:
        raise SpecError(issues)

    try:
        spec = build_spec_model(registry).model_validate(resolved)
    except ValidationError as exc:
        raise SpecError(_from_pydantic(exc, resolved)) from None

    semantic_issues, warnings = validate_semantics(spec, registry)
    if semantic_issues:
        raise SpecError(semantic_issues)
    if env_name not in spec.runtime and spec.runtime:
        warnings.append(f"no runtime settings for environment '{env_name}'; using defaults")
    return CompiledPipeline(spec=spec, env=env_name, registry=registry, warnings=warnings)


def _select_env(doc: dict[str, Any], env: str | None, issues: list[SpecIssue]) -> tuple[str, EnvironmentSpec]:
    raw = doc.get("environments") or {}
    if not isinstance(raw, dict):
        issues.append(SpecIssue("environments", "must be a mapping of environment name to settings"))
        return env or "default", EnvironmentSpec()
    if env is None:
        if len(raw) > 1:
            issues.append(SpecIssue("environments", f"several environments defined ({', '.join(raw)}); choose one"))
            return "default", EnvironmentSpec()
        env = next(iter(raw), "default")
    if raw and env not in raw:
        issues.append(SpecIssue("environments", f"environment '{env}' is not defined (defined: {', '.join(raw)})"))
        return env, EnvironmentSpec()
    try:
        return env, EnvironmentSpec.model_validate(raw.get(env) or {})
    except ValidationError as exc:
        issues.extend(SpecIssue(f"environments.{env}.{i.path}".rstrip("."), i.message) for i in _from_pydantic(exc))
        return env, EnvironmentSpec()


def _merge_connections(
    doc: dict[str, Any], connections: dict[str, dict[str, Any]], registry: Registry, issues: list[SpecIssue]
) -> None:
    """Copies a referenced connection's settings into the node's options (node options win)."""

    def merge(node: Any, category: str, path: str) -> None:
        if not isinstance(node, dict) or not node.get("connection"):
            return
        name = node["connection"]
        if name not in connections:
            defined = ", ".join(connections) or "none"
            issues.append(SpecIssue(f"{path}.connection", f"unknown connection '{name}' (defined: {defined})"))
            return
        settings = dict(connections[name])
        conn_type = settings.pop("type", None)
        node_type = node.get("type")
        if conn_type and isinstance(node_type, str):
            try:
                plugin = registry.get(category, node_type)
            except PluginError:
                plugin = None  # reported by schema validation
            if plugin is not None and not plugin.accepts_connection(conn_type):
                issues.append(
                    SpecIssue(f"{path}.connection", f"connection '{name}' is '{conn_type}', node type is '{node_type}'")
                )
                return
        options = node.get("options") or {}
        if isinstance(options, dict):
            node["options"] = {**settings, **options}

    for key, category in (("sources", "source"), ("sinks", "sink")):
        for i, node in enumerate(doc.get(key) or []):
            merge(node, category, f"{key}[{i}]")
    for i, block in enumerate(doc.get("quality") or []):
        if not isinstance(block, dict):
            continue
        for j, target in enumerate(block.get("results_sinks") or []):
            merge(target, "metrics_sink", f"quality[{i}].results_sinks[{j}]")
        merge(block.get("quarantine_sink"), "sink", f"quality[{i}].quarantine_sink")


def _from_pydantic(exc: ValidationError, doc: Any = None) -> list[SpecIssue]:
    """Pydantic puts the union tag in error locations (sources[0].file.options.path);
    walking the document lets us drop those so paths match what the user wrote."""
    issues = []
    for err in exc.errors():
        path, node, tag_skipped = "", doc, False
        for part in err["loc"]:
            if not tag_skipped and isinstance(node, dict) and isinstance(part, str) and part not in node and part in (
                node.get("type"), node.get("kind")
            ):
                tag_skipped = True  # at most one tag per level: a field may share the tag's name
                continue
            tag_skipped = False
            path += f"[{part}]" if isinstance(part, int) else (f".{part}" if path else str(part))
            if isinstance(node, (dict, list)):
                try:
                    node = node[part]
                except (KeyError, IndexError, TypeError):
                    node = None
        issues.append(SpecIssue(path, err["msg"]))
    return issues
