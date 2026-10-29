import { Handle, Position, type NodeProps } from "@xyflow/react";
import { CircleAlert } from "lucide-react";
import { useMemo } from "react";

import { plugin, useCatalog } from "../api/hooks";
import type { Category } from "../api/types";
import { IconChip, pluginIcon } from "../components/icons";
import { IN_HANDLE, OUT_HANDLE, TAP_HANDLE, TAP_SOURCE_HANDLE, type FlowNode } from "../model/graph";
import { nodeIssues, useEditor } from "../store/editor";

const LABEL = { source: "Source", transform: "Transform", sink: "Sink", quality: "Quality" };
const ACTION = { observe: "observe", fail: "fail on error", drop: "drop bad rows", quarantine: "quarantine" } as Record<string, string>;

export function PipelineNode({ data, selected }: NodeProps<FlowNode>) {
  const catalog = useCatalog();
  const issueList = useEditor((s) => s.issues);
  const analysis = useEditor((s) => s.analysis);
  // Derive outside the selector: a selector returning a fresh array re-renders forever.
  const issues = useMemo(() => nodeIssues({ issues: issueList, analysis }, data.specId), [issueList, analysis, data.specId]);

  const meta = data.kind === "quality" ? undefined : plugin(catalog, data.kind as Category, data.type);
  const checks = data.kind === "quality" ? ((data.extra.checks as unknown[]) ?? []).length : 0;
  const subtitle = data.kind === "quality" ? `${checks} check${checks === 1 ? "" : "s"}` : meta?.display_name ?? data.type;
  const tags: string[] = [];
  if (data.kind === "quality") tags.push(ACTION[String(data.extra.action ?? "observe")] ?? String(data.extra.action));
  if (data.extra.connection) tags.push(String(data.extra.connection));
  if (data.extra.stream) tags.push("stream");

  return (
    <div
      className={`node ${data.kind}${selected ? " selected" : ""}${issues.length ? " has-issues" : ""}`}
      style={{ ["--c" as string]: `var(--${data.kind})` }}
      title={issues.join("\n")}
    >
      {issues.length > 0 && (
        <span className="badge"><CircleAlert size={11} strokeWidth={2.6} />{issues.length}</span>
      )}
      <div className="row">
        <IconChip icon={pluginIcon(meta?.icon, data.kind, data.type)} color={`var(--${data.kind})`} />
        <div className="text">
          <div className="title">{data.specId}</div>
          <div className="subtitle">{subtitle}</div>
        </div>
      </div>
      <div className="tags">
        <span className="tag kind">{LABEL[data.kind]}</span>
        {tags.map((t) => <span key={t} className="tag">{t}</span>)}
      </div>
      {(data.kind === "transform" || data.kind === "sink") && <Handle id={IN_HANDLE} type="target" position={Position.Left} />}
      {(data.kind === "source" || data.kind === "transform") && <Handle id={OUT_HANDLE} type="source" position={Position.Right} />}
      {data.kind !== "quality" && (
        <Handle id={TAP_HANDLE} className="dq" type="target" position={Position.Top} title="Attach a quality block here" />
      )}
      {data.kind === "quality" && <Handle id={TAP_SOURCE_HANDLE} type="source" position={Position.Bottom} className="dq" />}
    </div>
  );
}
