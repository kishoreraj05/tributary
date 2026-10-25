"""Logical spec -> physical plan.

The planner is where the "smart" decisions live, so plugins stay simple:

* quality blocks become gate steps spliced between a node and its consumers
  (``attach_to``) or in front of a single sink (``before``); the user-facing spec keeps
  sinks pointing at business nodes;
* every step's read fan-out is counted and multi-read outputs are persisted;
* sinks are ordered last so a failing ``fail`` gate aborts before anything is written.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from ..core.quality import RowCheck
from ..spec.compiler import CompiledPipeline
from ..spec.validation import stream_source_id


@dataclass
class Step:
    id: str
    kind: str  # source | transform | quality | sink
    node: Any
    inputs: dict[str, str] = field(default_factory=dict)  # role/alias -> upstream step id
    attached_to: str | None = None  # quality: the business node being checked
    stream: bool = False  # the streaming source of a streaming pipeline
    reads: int = 0  # how many times this step's output is read
    cache: bool = False  # persist this step's output
    persist_flags: bool = False  # quality: persist the flagged frame it splits
    passthrough: bool = False  # quality: output is its input unchanged

    @property
    def type(self) -> str | None:
        return getattr(self.node, "type", None)


@dataclass
class PhysicalPlan:
    compiled: CompiledPipeline
    steps: dict[str, Step]
    order: list[str]

    @property
    def spec(self) -> Any:
        return self.compiled.spec

    @property
    def stream_source(self) -> Step | None:
        return next((s for s in self.steps.values() if s.stream), None)

    def ancestors(self, targets: set[str] | list[str]) -> set[str]:
        seen: set[str] = set()
        stack = list(targets)
        while stack:
            sid = stack.pop()
            if sid in seen:
                continue
            seen.add(sid)
            stack.extend(self.steps[sid].inputs.values())
        return seen

    def explain(self) -> str:
        spec = self.spec
        lines = [f"Pipeline '{spec.pipeline.id}' ({spec.pipeline.mode}, env={self.compiled.env})"]
        for n, sid in enumerate(self.order, 1):
            step = self.steps[sid]
            if step.kind == "quality":
                q = step.node
                head = f"quality    {sid} on {step.attached_to} ({q.action}, {len(q.checks)} checks)"
            else:
                head = f"{step.kind:<10} {sid} [{step.type}]"
            src = ", ".join(f"{k}={v}" if k not in ("input", v) else v for k, v in step.inputs.items())
            notes = []
            if step.stream:
                notes.append("stream; each micro-batch persisted once")
            if step.cache:
                notes.append(f"persist {step.node.storage_level} ({step.reads} reads)")
            if step.persist_flags:
                notes.append(f"flagged rows persisted ({step.reads + 1 + (step.node.action == 'quarantine')} reads)")
            line = f"  {n:>2}. {head}"
            if src:
                line += f" <- {src}"
            if notes:
                line += "   [" + "; ".join(notes) + "]"
            lines.append(line)
        return "\n".join(lines)


def build_plan(compiled: CompiledPipeline) -> PhysicalPlan:
    spec = compiled.spec
    registry = compiled.registry
    stream_id = stream_source_id(spec)
    steps: dict[str, Step] = {}

    after: dict[str, list[Any]] = defaultdict(list)
    before: dict[str, list[Any]] = defaultdict(list)
    for q in spec.quality:
        (after[q.attach_to] if q.attach_to else before[q.before]).append(q)

    def gate(q: Any, upstream: str, attached_to: str) -> str:
        has_row = any(issubclass(registry.get("check", c.type), RowCheck) for c in q.checks)
        steps[q.id] = Step(
            q.id,
            "quality",
            q,
            inputs={"input": upstream},
            attached_to=attached_to,
            passthrough=q.action in ("observe", "fail") or not has_row,
        )
        return q.id

    effective: dict[str, str] = {}  # business node -> step id its consumers read

    def add_data_step(step: Step) -> None:
        steps[step.id] = step
        last = step.id
        for q in after[step.id]:
            last = gate(q, last, step.id)
        effective[step.id] = last

    for s in spec.sources:
        add_data_step(Step(s.id, "source", s, stream=s.id == stream_id))
    # Transforms in dependency order so their inputs' effective outputs exist.
    for t in _topological(spec.transforms):
        add_data_step(Step(t.id, "transform", t, inputs={r.view_name: effective[r.node] for r in t.inputs}))
    for s in spec.sinks:
        last = effective[s.input]
        for q in before[s.id]:
            last = gate(q, last, s.input)
        steps[s.id] = Step(s.id, "sink", s, inputs={"input": last})

    _decide_caching(steps)
    order = [sid for sid in steps if steps[sid].kind != "sink"] + [sid for sid in steps if steps[sid].kind == "sink"]
    return PhysicalPlan(compiled, steps, order)


def _topological(transforms: list[Any]) -> list[Any]:
    by_id = {t.id: t for t in transforms}
    done: set[str] = set()
    ordered: list[Any] = []

    def visit(t: Any) -> None:
        if t.id in done:
            return
        done.add(t.id)
        for ref in t.inputs:
            if ref.node in by_id:
                visit(by_id[ref.node])
        ordered.append(t)

    for t in transforms:
        visit(t)
    return ordered


def _decide_caching(steps: dict[str, Step]) -> None:
    consumers: dict[str, list[Step]] = defaultdict(list)
    for step in steps.values():
        for upstream in step.inputs.values():
            consumers[upstream].append(step)

    memo: dict[str, int] = {}

    def reads(sid: str) -> int:
        if sid not in memo:
            total = 0
            for c in consumers[sid]:
                # A pass-through gate runs one aggregate over its input and hands the same
                # frame on, so its consumers read the upstream output too.
                total += 1 + reads(c.id) if c.kind == "quality" and c.passthrough else 1
            memo[sid] = total
        return memo[sid]

    for step in steps.values():
        step.reads = reads(step.id)
        if step.kind in ("source", "transform"):
            mode = step.node.cache
            # A streaming source's micro-batch is persisted by the streaming executor.
            step.cache = not step.stream and (mode == "always" or (mode == "auto" and step.reads >= 2))
        elif step.kind == "quality" and not step.passthrough:
            flagged_reads = 1 + (step.node.action == "quarantine") + step.reads
            step.persist_flags = flagged_reads >= 2
