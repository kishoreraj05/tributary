"""Cross-node checks that a schema alone cannot express."""

from __future__ import annotations

from typing import Any

from ..core.quality import RowCheck
from ..core.source import BatchReadable, StreamReadable
from ..errors import SpecIssue
from ..registry import Registry


def stream_source_id(spec: Any) -> str | None:
    if spec.pipeline.mode != "streaming":
        return None
    marked = [s.id for s in spec.sources if s.stream]
    if len(marked) == 1:
        return marked[0]
    if not marked and len(spec.sources) == 1:
        return spec.sources[0].id
    return None


def validate_semantics(spec: Any, registry: Registry) -> tuple[list[SpecIssue], list[str]]:
    issues: list[SpecIssue] = []
    warnings: list[str] = []

    seen: dict[str, str] = {}
    for section in ("sources", "transforms", "sinks", "quality"):
        for i, node in enumerate(getattr(spec, section)):
            path = f"{section}[{i}].id"
            if node.id in seen:
                issues.append(SpecIssue(path, f"duplicate id '{node.id}' (also used at {seen[node.id]})"))
            else:
                seen[node.id] = path

    data_nodes = {n.id: n for n in [*spec.sources, *spec.transforms]}
    sinks = {s.id: s for s in spec.sinks}
    consumed: set[str] = set()

    def check_ref(ref: str, path: str) -> None:
        consumed.add(ref)
        if ref in data_nodes:
            return
        if ref in sinks:
            issues.append(SpecIssue(path, f"'{ref}' is a sink; only sources and transforms produce data"))
        else:
            issues.append(SpecIssue(path, f"unknown node '{ref}' (known: {', '.join(data_nodes) or 'none'})"))

    for i, t in enumerate(spec.transforms):
        aliases = [ref.view_name for ref in t.inputs]
        for j, ref in enumerate(t.inputs):
            check_ref(ref.node, f"transforms[{i}].inputs[{j}]")
        dupes = sorted({a for a in aliases if aliases.count(a) > 1})
        if dupes:
            issues.append(SpecIssue(f"transforms[{i}].inputs", f"input names must be unique: {', '.join(dupes)}"))
        plugin = registry.create("transform", t.type)
        issues.extend(SpecIssue(f"transforms[{i}].options", msg) for msg in plugin.validate(t.options, aliases))

    upstream_of = {t.id: {ref.node for ref in t.inputs} for t in spec.transforms}
    for i, t in enumerate(spec.transforms):
        if registry.get("transform", t.type).chainable:
            continue
        seen, stack = set(), list(upstream_of[t.id])
        while stack:
            node_id = stack.pop()
            if node_id in seen or node_id not in upstream_of:
                continue
            seen.add(node_id)
            if data_nodes[node_id].type == t.type:
                issues.append(SpecIssue(
                    f"transforms[{i}].inputs",
                    f"a {t.type} node cannot be downstream of another {t.type} node ('{node_id}'); "
                    "give each its own branch from the node to check",
                ))
                break
            stack.extend(upstream_of[node_id])

    for i, s in enumerate(spec.sinks):
        check_ref(s.input, f"sinks[{i}].input")

    for i, q in enumerate(spec.quality):
        path = f"quality[{i}]"
        if bool(q.attach_to) == bool(q.before):
            issues.append(SpecIssue(path, "set exactly one of 'attach_to' (a source/transform) or 'before' (a sink)"))
        elif q.attach_to:
            check_ref(q.attach_to, f"{path}.attach_to")
        elif q.before not in sinks:
            issues.append(SpecIssue(f"{path}.before", f"unknown sink '{q.before}'"))
        if q.action == "quarantine" and q.quarantine_sink is None:
            issues.append(SpecIssue(path, "action 'quarantine' needs a 'quarantine_sink'"))
        if q.quarantine_sink is not None and q.action != "quarantine":
            warnings.append(f"{path}: quarantine_sink is ignored unless action is 'quarantine'")
        has_row_checks = any(issubclass(registry.get("check", c.type), RowCheck) for c in q.checks)
        if q.action in ("drop", "quarantine") and not has_row_checks:
            warnings.append(f"{path}: action '{q.action}' only filters rows for row-level checks; none are defined")

    issues.extend(_cycle_issues(spec))

    mode = spec.pipeline.mode
    stream_id = stream_source_id(spec)
    if mode == "streaming" and stream_id is None:
        marked = [s.id for s in spec.sources if s.stream]
        msg = (
            f"only one source can be the stream (marked: {', '.join(marked)})"
            if marked
            else "several sources defined; mark the streaming one with 'stream: true'"
        )
        issues.append(SpecIssue("sources", msg))
    for i, s in enumerate(spec.sources):
        plugin = registry.get("source", s.type)
        if s.id == stream_id:
            if not issubclass(plugin, StreamReadable):
                issues.append(SpecIssue(f"sources[{i}]", f"source type '{s.type}' does not support streaming"))
        elif not issubclass(plugin, BatchReadable):
            issues.append(SpecIssue(f"sources[{i}]", f"source type '{s.type}' does not support batch reads"))
        if mode == "batch" and s.stream:
            warnings.append(f"sources[{i}]: 'stream: true' is ignored in batch mode")

    for node_id in data_nodes:
        if node_id not in consumed:
            warnings.append(f"node '{node_id}' is not used by any transform, sink or quality block")
    return issues, warnings


def _cycle_issues(spec: Any) -> list[SpecIssue]:
    deps = {t.id: {ref.node for ref in t.inputs} for t in spec.transforms}
    indegree = {n: len([d for d in ds if d in deps]) for n, ds in deps.items()}
    ready = [n for n, d in indegree.items() if d == 0]
    done = 0
    while ready:
        node = ready.pop()
        done += 1
        for other, ds in deps.items():
            if node in ds:
                indegree[other] -= 1
                if indegree[other] == 0:
                    ready.append(other)
    if done == len(deps):
        return []
    stuck = sorted(n for n, d in indegree.items() if d > 0)
    return [SpecIssue("transforms", f"cycle detected among: {', '.join(stuck)}")]
