import { applyEdgeChanges, applyNodeChanges, type EdgeChange, type NodeChange, type XYPosition } from "@xyflow/react";
import { create } from "zustand";

import type { Issue, Json, NodeAnalysis, Pipeline, PreviewResult } from "../api/types";
import {
  dataEdge,
  fromSpec,
  isTap,
  newId,
  tapEdge,
  TAP_HANDLE,
  uniqueSpecId,
  type FlowEdge,
  type FlowNode,
  type NodeKind,
  type PipelineMeta,
} from "../model/graph";

export type DrawerTab = "preview" | "problems" | "yaml" | "plan" | "runs";
export type SaveState = "saved" | "dirty" | "saving" | "error";

interface PreviewState {
  specId: string;
  loading: boolean;
  result?: PreviewResult;
  error?: string;
}

interface EditorState {
  pipeline: Pipeline | null;
  nodes: FlowNode[];
  edges: FlowEdge[];
  meta: PipelineMeta;
  revision: number; // bumps on every spec/layout change (drives autosave + validation)
  selectedId: string | null;
  env: string | null;
  issues: Issue[];
  warnings: string[];
  analysis: Record<string, NodeAnalysis>;
  checking: "idle" | "shallow" | "deep";
  preview: PreviewState | null;
  samples: Record<string, { sample_id: string; filename: string }>;
  drawer: DrawerTab | null;
  saveState: SaveState;
  notice: string | null;
  lastRunId: string | null;

  load(pipeline: Pipeline): void;
  onNodesChange(changes: NodeChange<FlowNode>[]): void;
  onEdgesChange(changes: EdgeChange<FlowEdge>[]): void;
  connect(source: string, target: string, targetHandle?: string | null): void;
  addNode(kind: NodeKind, type: string, position: XYPosition): void;
  updateNode(id: string, patch: { options?: Json; extra?: Json }): void;
  renameNode(id: string, specId: string): void;
  setAlias(edgeId: string, alias: string): void;
  removeNode(id: string): void;
  setMeta(meta: PipelineMeta): void;
  applySpec(spec: Json): void;
  select(id: string | null): void;
  setEnv(env: string | null): void;
  setDrawer(tab: DrawerTab | null): void;
  set(partial: Partial<EditorState>): void;
}

const touched = (s: EditorState) => ({ revision: s.revision + 1, saveState: "dirty" as SaveState });

export const useEditor = create<EditorState>((set, get) => ({
  pipeline: null,
  nodes: [],
  edges: [],
  meta: { pipeline: {} },
  revision: 0,
  selectedId: null,
  env: null,
  issues: [],
  warnings: [],
  analysis: {},
  checking: "idle",
  preview: null,
  samples: {},
  drawer: null,
  saveState: "saved",
  notice: null,
  lastRunId: null,

  load(pipeline) {
    const { nodes, edges, meta } = fromSpec(pipeline.draft_spec ?? {}, pipeline.draft_layout ?? {});
    const envs = Object.keys((meta.environments as Json) ?? {});
    set({
      pipeline,
      nodes,
      edges,
      meta,
      revision: 0,
      selectedId: null,
      env: envs.includes("dev") ? "dev" : envs[0] ?? null,
      issues: [],
      analysis: {},
      preview: null,
      samples: {},
      saveState: "saved",
      lastRunId: null,
    });
  },

  onNodesChange(changes) {
    // Selection and measurement changes are not edits; moves and removals are.
    const edits = changes.some((c) => c.type === "remove" || (c.type === "position" && !c.dragging));
    set((s) => {
      const nodes = applyNodeChanges(changes, s.nodes);
      const ids = new Set(nodes.map((n) => n.id));
      const edges = s.edges.filter((e) => ids.has(e.source) && ids.has(e.target));
      const selectedId = s.selectedId && ids.has(s.selectedId) ? s.selectedId : null;
      return { nodes, edges, selectedId, ...(edits ? touched(s) : {}) };
    });
  },

  onEdgesChange(changes) {
    const edits = changes.some((c) => c.type === "remove");
    set((s) => ({ edges: applyEdgeChanges(changes, s.edges), ...(edits ? touched(s) : {}) }));
  },

  connect(source, target, targetHandle) {
    set((s) => {
      const edge = targetHandle === TAP_HANDLE ? tapEdge(source, target) : dataEdge(source, target);
      return { edges: [...s.edges, edge], ...touched(s) };
    });
  },

  addNode(kind, type, position) {
    set((s) => {
      const specId = uniqueSpecId(kind === "quality" ? "dq" : kind === "transform" ? type : `${type}_${kind}`, s.nodes);
      const extra: Json = kind === "quality" ? { action: "observe", checks: [] } : {};
      const node: FlowNode = { id: newId(), type: "pipeline", position, data: { specId, kind, type, options: {}, extra } };
      return { nodes: [...s.nodes, node], selectedId: node.id, ...touched(s) };
    });
  },

  updateNode(id, patch) {
    set((s) => ({
      nodes: s.nodes.map((n) =>
        n.id === id
          ? { ...n, data: { ...n.data, options: patch.options ?? n.data.options, extra: patch.extra ?? n.data.extra } }
          : n,
      ),
      ...touched(s),
    }));
  },

  renameNode(id, specId) {
    set((s) => {
      const node = s.nodes.find((n) => n.id === id);
      if (!node) return {};
      const old = node.data.specId;
      // Keep downstream SQL working: an input without an alias was visible under the old id.
      const edges = s.edges.map((e) =>
        e.source === id && !isTap(e) && !e.data?.alias ? { ...e, data: { alias: old }, label: old } : e,
      );
      const nodes = s.nodes.map((n) => (n.id === id ? { ...n, data: { ...n.data, specId } } : n));
      return { nodes, edges, ...touched(s) };
    });
  },

  setAlias(edgeId, alias) {
    set((s) => ({
      edges: s.edges.map((e) => (e.id === edgeId ? { ...e, data: alias ? { alias } : {}, label: alias || undefined } : e)),
      ...touched(s),
    }));
  },

  removeNode(id) {
    set((s) => ({
      nodes: s.nodes.filter((n) => n.id !== id),
      edges: s.edges.filter((e) => e.source !== id && e.target !== id),
      selectedId: s.selectedId === id ? null : s.selectedId,
      ...touched(s),
    }));
  },

  setMeta(meta) {
    set((s) => ({ meta, ...touched(s) }));
  },

  applySpec(spec) {
    // Keep positions of nodes that still exist (matched by spec id).
    const layout = Object.fromEntries(get().nodes.map((n) => [n.data.specId, n.position]));
    const { nodes, edges, meta } = fromSpec(spec, layout);
    set((s) => ({ nodes, edges, meta, selectedId: null, ...touched(s) }));
  },

  select(id) {
    set({ selectedId: id });
  },
  setEnv(env) {
    set({ env, analysis: {} });
  },
  setDrawer(tab) {
    set({ drawer: tab });
  },
  set(partial) {
    set(partial);
  },
}));

export function nodeIssues(state: Pick<EditorState, "issues" | "analysis">, specId: string): string[] {
  // Name the field for generic messages: "sql: Field required" rather than "Field required".
  const messages = state.issues
    .filter((i) => i.node_id === specId)
    .map((i) => {
      const field = i.path.replace(/^\w+\[\d+\]\.?/, "").replace(/^options\./, "");
      return field ? `${field}: ${i.message}` : i.message;
    });
  const analysis = state.analysis[specId];
  if (analysis && !analysis.ok && analysis.error && !analysis.error.startsWith("skipped")) messages.push(analysis.error);
  return messages;
}

/** Structural issues plus column-level analysis errors (what the user has to fix). */
export function problemCount(state: Pick<EditorState, "issues" | "analysis">): number {
  const analysisErrors = Object.values(state.analysis).filter((a) => !a.ok && a.error && !a.error.startsWith("skipped"));
  return state.issues.length + analysisErrors.length;
}
