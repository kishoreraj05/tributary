"""Raw document -> validated, environment-resolved pipeline."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from ..core.targets import target_fields
from ..errors import PluginError, SpecError, SpecIssue
from ..plugins._dummy import LOCAL_ENV, parse_records
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
    use_dummies: bool | None = None,
) -> CompiledPipeline:
    """``use_dummies``: whether nodes with ``dummy.enabled`` become the dummy plugins. By
    default only in the local environment; True does it in any environment, which checks
    everything else about a pipeline whose dummy nodes have no real settings yet."""
    if isinstance(source, CompiledPipeline):
        return source
    registry = registry or default_registry()
    doc = load_document(source)
    issues: list[SpecIssue] = []

    env_name, env_spec = _select_env(doc, env, issues)
    if issues:
        raise SpecError(issues)

    warnings: list[str] = []
    _apply_dummies(doc, env_name, issues, warnings, env_name == LOCAL_ENV if use_dummies is None else use_dummies)
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

    semantic_issues, semantic_warnings = validate_semantics(spec, registry)
    warnings += semantic_warnings
    if semantic_issues:
        raise SpecError(semantic_issues)
    if env_name not in spec.runtime and spec.runtime:
        warnings.append(f"no runtime settings for environment '{env_name}'; using defaults")
    return CompiledPipeline(spec=spec, env=env_name, registry=registry, warnings=warnings)


def _apply_dummies(doc: dict[str, Any], env: str, issues: list[SpecIssue], warnings: list[str], swap: bool) -> None:
    """In the local environment, a source or sink with ``dummy.enabled`` becomes the dummy
    plugin of its category, so its own options (and connection) are neither needed nor
    checked: the source hands its uploaded records downstream, the sink writes nothing.
    Everywhere else the setting is ignored and the node runs for real."""
    for key, category in (("sources", "source"), ("sinks", "sink")):
        for i, node in enumerate(doc.get(key) or []):
            if not isinstance(node, dict) or not isinstance(node.get("dummy"), dict) or not node["dummy"].get("enabled"):
                continue
            if not swap:
                warnings.append(f"{key}[{i}] ({node.get('id')}): dummy data is only used in the '{LOCAL_ENV}' environment")
                continue
            options: dict[str, Any] = {"stands_for": node.get("type") or ""}
            if category == "source":
                # Bad data is reported on the field, and the node is still swapped: its real
                # options (and connection) are no more the problem than in a good dummy node.
                try:
                    options["records"] = parse_records(node["dummy"].get("data"))
                except ValueError as exc:
                    issues.append(SpecIssue(f"{key}[{i}].dummy.data", str(exc)))
                    options["records"] = []
                else:
                    if not options["records"] and not node.get("schema"):
                        issues.append(SpecIssue(f"{key}[{i}].dummy.data", "upload or paste at least one record (JSON Lines)"))
            node.update(type="dummy", options=options)
            node.pop("connection", None)


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

    def merge_embedded(node: Any, category: str, path: str) -> None:
        """Targets a plugin embeds in its options (a deequ node's results_sinks) take connections too."""
        if not isinstance(node, dict) or not isinstance(node.get("type"), str) or not isinstance(node.get("options"), dict):
            return
        try:
            plugin = registry.get(category, node["type"])
        except PluginError:
            return  # reported by schema validation
        for field_name, target_category in target_fields(plugin.Options).items():
            entries = node["options"].get(field_name)
            if isinstance(entries, list):
                for j, target in enumerate(entries):
                    merge(target, target_category, f"{path}.options.{field_name}[{j}]")

    for key, category in (("sources", "source"), ("transforms", "transform"), ("sinks", "sink")):
        for i, node in enumerate(doc.get(key) or []):
            merge(node, category, f"{key}[{i}]")
            merge_embedded(node, category, f"{key}[{i}]")
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
        issues.append(SpecIssue(path, _message(err)))
    return issues


_NAME_RULE = "letters, digits and _ only, not starting with a digit"


def _message(err: dict[str, Any]) -> str:
    """Pydantic's wording, made actionable. The canvas sends an empty ``input`` for a sink
    nobody is connected to and an empty ``inputs`` for such a transform, so those two say
    what to do rather than which regex or list length failed."""
    field = err["loc"][-1] if err["loc"] else ""
    value = err.get("input")
    if err["type"] == "string_pattern_mismatch":
        if field == "input" and value == "":
            return "not connected: link a source or transform to this sink"
        if value == "":
            return "must not be empty"
        return f"'{value}' is not a valid name: {_NAME_RULE}"
    if err["type"] == "too_short" and field == "inputs" and not value:
        return "not connected: link at least one node to this transform"
    # Pydantic prefixes custom validator messages with "Value error, ": not useful to users.
    return err["msg"].removeprefix("Value error, ")
