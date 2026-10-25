/**
 * The canvas graph and its mapping to the engine's pipeline spec.
 *
 * Nodes carry their spec id and settings; data edges carry the input alias (the
 * table name used in SQL); "tap" edges attach a quality block to the node (or the
 * sink) it checks. The spec is always derived from the graph, never edited
 * separately, so what the user sees is exactly what the engine gets.
 */
import type { Edge, Node, XYPosition } from "@xyflow/react";

import type { Json } from "../api/types";

export type NodeKind = "source" | "transform" | "sink" | "quality";

export interface NodeData extends Record<string, unknown> {
  specId: string;
  kind: NodeKind;
  type: string; // plugin type; "quality" for quality blocks
  options: Json; // plugin options (sources/transforms/sinks)
  extra: Json; // everything else on the spec node (schema, connection, checks, ...)
}

export interface EdgeData extends Record<string, unknown> {
  alias?: string;
}

export type FlowNode = Node<NodeData>;
export type FlowEdge = Edge<EdgeData>;

/** Spec sections that are not nodes: pipeline meta, environments, runtime, execution. */
export interface PipelineMeta {
  version?: number;
  pipeline: Json;
  environments?: Json;
  runtime?: Json;
  execution?: Json;
}

export const TAP_HANDLE = "dq";
export const IN_HANDLE = "in";
export const OUT_HANDLE = "out";
export const TAP_SOURCE_HANDLE = "tap";
const SECTION: Record<NodeKind, string> = { source: "sources", transform: "transforms", sink: "sinks", quality: "quality" };

export function isTap(edge: FlowEdge): boolean {
  return edge.targetHandle === TAP_HANDLE;
}

/** Drops keys whose value is undefined, null, "" or an empty object/array. */
export function prune<T>(value: T): T {
  if (Array.isArray(value)) return value.map(prune) as T;
  if (value && typeof value === "object") {
    const out: Json = {};
    for (const [k, v] of Object.entries(value as Json)) {
      const p = prune(v);
      if (p === undefined || p === null || p === "") continue;
      if (typeof p === "object" && !Array.isArray(p) && Object.keys(p as Json).length === 0) continue;
      out[k] = p;
    }
    return out as T;
  }
  return value;
}

export function toSpec(nodes: FlowNode[], edges: FlowEdge[], meta: PipelineMeta): Json {
  const byId = new Map(nodes.map((n) => [n.id, n]));
  const incoming = (id: string) => edges.filter((e) => e.target === id && !isTap(e));
  const specId = (id: string) => byId.get(id)?.data.specId ?? "";

  const spec: Json = { version: meta.version ?? 1, pipeline: meta.pipeline };
  if (meta.environments && Object.keys(meta.environments).length) spec.environments = meta.environments;
  const sections: Record<string, Json[]> = { sources: [], transforms: [], quality: [], sinks: [] };

  for (const node of nodes) {
    const { kind, type, options, extra } = node.data;
    const base: Json = { id: node.data.specId };
    if (kind === "quality") {
      const tap = edges.find((e) => e.source === node.id && isTap(e));
      const target = tap ? byId.get(tap.target) : undefined;
      const placement = target ? { [target.data.kind === "sink" ? "before" : "attach_to"]: target.data.specId } : {};
      sections.quality.push(prune({ ...base, ...placement, ...extra }));
      continue;
    }
    const entry: Json = { ...base, type, ...extra, options };
    if (kind === "transform") {
      entry.inputs = incoming(node.id).map((e) =>
        e.data?.alias && e.data.alias !== specId(e.source) ? { node: specId(e.source), as: e.data.alias } : { node: specId(e.source) },
      );
    } else if (kind === "sink") {
      const [first] = incoming(node.id);
      entry.input = first ? specId(first.source) : "";
    }
    const pruned = prune(entry);
    if (kind === "sink") pruned.input = entry.input; // keep an empty input so validation reports it
    if (kind === "transform") pruned.inputs = entry.inputs;
    if (!("options" in pruned)) pruned.options = {};
    sections[SECTION[kind]].push(pruned);
  }
  for (const [key, list] of Object.entries(sections)) if (list.length || key !== "quality") spec[key] = list;
  if (meta.runtime && Object.keys(meta.runtime).length) spec.runtime = meta.runtime;
  if (meta.execution && Object.keys(meta.execution).length) spec.execution = meta.execution;
  return spec;
}

export function layoutOf(nodes: FlowNode[]): Record<string, XYPosition> {
  return Object.fromEntries(nodes.map((n) => [n.data.specId, { x: Math.round(n.position.x), y: Math.round(n.position.y) }]));
}

let counter = 0;
export const newId = () => `n${Date.now().toString(36)}${(counter++).toString(36)}`;

export function fromSpec(
  spec: Json,
  layout: Record<string, XYPosition> = {},
): { nodes: FlowNode[]; edges: FlowEdge[]; meta: PipelineMeta } {
  const asList = (key: string) => (Array.isArray(spec[key]) ? (spec[key] as Json[]) : []);
  const nodes: FlowNode[] = [];
  const edges: FlowEdge[] = [];
  const internal = new Map<string, string>(); // spec id -> node id

  const add = (kind: NodeKind, raw: Json) => {
    const { id, type, options, ...rest } = raw;
    const extra = { ...rest };
    for (const k of ["inputs", "input", "attach_to", "before"]) delete extra[k];
    const node: FlowNode = {
      id: newId(),
      type: "pipeline",
      position: { x: 0, y: 0 },
      data: {
        specId: String(id ?? ""),
        kind,
        type: kind === "quality" ? "quality" : String(type ?? ""),
        options: (options as Json) ?? {},
        extra,
      },
    };
    internal.set(node.data.specId, node.id);
    nodes.push(node);
    return node;
  };

  asList("sources").forEach((s) => add("source", s));
  asList("transforms").forEach((t) => add("transform", t));
  asList("sinks").forEach((s) => add("sink", s));
  const qualityNodes = asList("quality").map((q) => [add("quality", q), q] as const);

  for (const t of asList("transforms")) {
    const target = internal.get(String(t.id));
    for (const raw of (t.inputs as unknown[]) ?? []) {
      const ref = typeof raw === "string" ? { node: raw } : (raw as Json);
      const source = internal.get(String(ref.node));
      if (source && target) edges.push(dataEdge(source, target, ref.as as string | undefined));
    }
  }
  for (const s of asList("sinks")) {
    const source = internal.get(String(s.input));
    const target = internal.get(String(s.id));
    if (source && target) edges.push(dataEdge(source, target));
  }
  for (const [node, q] of qualityNodes) {
    const target = internal.get(String(q.attach_to ?? q.before ?? ""));
    if (target) edges.push(tapEdge(node.id, target));
  }

  const auto = autoLayout(nodes, edges);
  for (const n of nodes) n.position = layout[n.data.specId] ?? auto[n.id];

  const meta: PipelineMeta = { pipeline: (spec.pipeline as Json) ?? { id: "pipeline", mode: "batch" } };
  if (spec.version !== undefined) meta.version = spec.version as number;
  for (const k of ["environments", "runtime", "execution"] as const) if (spec[k]) meta[k] = spec[k] as Json;
  return { nodes, edges, meta };
}

export function dataEdge(source: string, target: string, alias?: string): FlowEdge {
  return { id: newId(), source, target, sourceHandle: OUT_HANDLE, targetHandle: IN_HANDLE, data: alias ? { alias } : {}, label: alias };
}

export function tapEdge(source: string, target: string): FlowEdge {
  return { id: newId(), source, target, sourceHandle: TAP_SOURCE_HANDLE, targetHandle: TAP_HANDLE, type: "tap", animated: true };
}

/** Columns by depth (sources left, sinks right); quality blocks sit above what they check. */
export function autoLayout(nodes: FlowNode[], edges: FlowEdge[]): Record<string, XYPosition> {
  const depth = new Map<string, number>();
  const parents = (id: string) => edges.filter((e) => e.target === id && !isTap(e)).map((e) => e.source);
  const visit = (id: string, seen: Set<string>): number => {
    if (depth.has(id)) return depth.get(id)!;
    if (seen.has(id)) return 0;
    seen.add(id);
    const d = Math.max(-1, ...parents(id).map((p) => visit(p, seen))) + 1;
    depth.set(id, d);
    return d;
  };
  const rows = new Map<number, number>();
  const out: Record<string, XYPosition> = {};
  for (const n of nodes.filter((n) => n.data.kind !== "quality")) {
    const d = visit(n.id, new Set());
    const row = rows.get(d) ?? 0;
    rows.set(d, row + 1);
    out[n.id] = { x: 40 + d * 260, y: 60 + row * 150 };
  }
  for (const n of nodes.filter((n) => n.data.kind === "quality")) {
    const tap = edges.find((e) => e.source === n.id && isTap(e));
    const anchor = tap ? out[tap.target] : undefined;
    out[n.id] = anchor ? { x: anchor.x + 40, y: anchor.y - 110 } : { x: 40, y: 0 };
  }
  return out;
}

/** Spec ids must be unique identifiers; derive one from a base name. */
export function uniqueSpecId(base: string, nodes: FlowNode[]): string {
  const clean = base.replace(/[^A-Za-z0-9_]/g, "_").replace(/^([^A-Za-z_])/, "_$1") || "node";
  const taken = new Set(nodes.map((n) => n.data.specId));
  for (let i = 1; ; i++) {
    const candidate = `${clean}_${i}`;
    if (!taken.has(candidate)) return candidate;
  }
}

export type ConnectError = string | null;

/** Rules the canvas enforces while the user draws edges. */
export function connectionError(
  nodes: FlowNode[],
  edges: FlowEdge[],
  sourceId: string,
  targetId: string,
  targetHandle?: string | null,
): ConnectError {
  const source = nodes.find((n) => n.id === sourceId);
  const target = nodes.find((n) => n.id === targetId);
  if (!source || !target || sourceId === targetId) return "invalid connection";
  if (targetHandle === TAP_HANDLE) {
    if (source.data.kind !== "quality") return "only quality blocks attach to the quality handle";
    if (target.data.kind === "quality") return "a quality block cannot check another quality block";
    if (edges.some((e) => e.source === sourceId && isTap(e))) return "a quality block checks exactly one node";
    return null;
  }
  if (source.data.kind === "quality") return "drag quality blocks onto the amber handle of the node to check";
  if (source.data.kind === "sink") return "sinks have no outputs";
  if (target.data.kind === "source") return "sources have no inputs";
  if (edges.some((e) => e.source === sourceId && e.target === targetId && !isTap(e))) return "already connected";
  if (target.data.kind === "sink" && edges.some((e) => e.target === targetId && !isTap(e))) return "a sink has one input";
  // Cycle: is target already upstream of source?
  const upstream = new Set<string>();
  const stack = [sourceId];
  while (stack.length) {
    const id = stack.pop()!;
    if (id === targetId) return "that would create a cycle";
    if (upstream.has(id)) continue;
    upstream.add(id);
    edges.filter((e) => e.target === id && !isTap(e)).forEach((e) => stack.push(e.source));
  }
  return null;
}

/** JSON with sorted keys, so equal specs compare equal regardless of key order. */
export function stableStringify(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(",")}]`;
  if (value && typeof value === "object") {
    const entries = Object.entries(value as Json).filter(([, v]) => v !== undefined).sort(([a], [b]) => a.localeCompare(b));
    return `{${entries.map(([k, v]) => `${JSON.stringify(k)}:${stableStringify(v)}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

export interface SpecDiff {
  added: string[];
  removed: string[];
  changed: string[];
  settings: boolean; // pipeline meta, environments, runtime or execution differ
}

/** What the draft changes relative to a saved version, node by node. Both sides go
 * through the canvas mapping first, so equivalent spellings (inputs: [a] vs
 * [{node: a}], key order, empty values) are not reported as changes. */
export function specDiff(current: Json, base: Json): SpecDiff {
  const normalize = (spec: Json) => {
    const { nodes, edges, meta } = fromSpec(spec);
    return toSpec(nodes, edges, meta);
  };
  const a = normalize(current);
  const b = normalize(base);
  const diff: SpecDiff = { added: [], removed: [], changed: [], settings: false };
  const byId = (spec: Json) => {
    const out = new Map<string, string>();
    for (const key of ["sources", "transforms", "quality", "sinks"]) {
      for (const node of (spec[key] as Json[]) ?? []) out.set(String(node.id), stableStringify(node));
    }
    return out;
  };
  const now = byId(a);
  const then = byId(b);
  for (const [id, value] of now) {
    if (!then.has(id)) diff.added.push(id);
    else if (then.get(id) !== value) diff.changed.push(id);
  }
  for (const id of then.keys()) if (!now.has(id)) diff.removed.push(id);
  diff.settings = ["version", "pipeline", "environments", "runtime", "execution"].some(
    (k) => stableStringify(a[k]) !== stableStringify(b[k]),
  );
  return diff;
}

export function isSameSpec(diff: SpecDiff): boolean {
  return !diff.added.length && !diff.removed.length && !diff.changed.length && !diff.settings;
}

export function describeDiff(diff: SpecDiff): string[] {
  const lines = [];
  if (diff.changed.length) lines.push(`changed: ${diff.changed.join(", ")}`);
  if (diff.added.length) lines.push(`added: ${diff.added.join(", ")}`);
  if (diff.removed.length) lines.push(`removed: ${diff.removed.join(", ")}`);
  if (diff.settings) lines.push("pipeline settings or environments");
  return lines;
}
