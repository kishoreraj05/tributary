import type { Issue } from "../api/types";
import type { FlowNode, NodeKind } from "../model/graph";

const SECTION: Record<NodeKind, string> = { source: "sources", transform: "transforms", sink: "sinks", quality: "quality" };

/** "sources[2]" for the node: the index matches the order toSpec writes nodes in. */
export function nodePath(nodes: FlowNode[], node: FlowNode): string {
  const index = nodes.filter((n) => n.data.kind === node.data.kind).findIndex((n) => n.id === node.id);
  return `${SECTION[node.data.kind]}[${index}]`;
}

/** Issues under ``prefix``, grouped by the next path segment (a field name). */
export function fieldErrors(issues: Issue[], prefix: string): Record<string, string[]> {
  const out: Record<string, string[]> = {};
  for (const issue of issues) {
    if (!issue.path.startsWith(prefix + ".")) continue;
    const field = issue.path.slice(prefix.length + 1).split(/[.[]/)[0];
    (out[field] ??= []).push(issue.message);
  }
  return out;
}

export function exactErrors(issues: Issue[], path: string): string[] {
  return issues.filter((i) => i.path === path).map((i) => i.message);
}
