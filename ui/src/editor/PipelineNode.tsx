import { Handle, Position, type NodeProps } from "@xyflow/react";
import { useMemo } from "react";

import { IN_HANDLE, OUT_HANDLE, TAP_HANDLE, TAP_SOURCE_HANDLE, type FlowNode } from "../model/graph";
import { nodeIssues, useEditor } from "../store/editor";

const LABEL = { source: "Source", transform: "Transform", sink: "Sink", quality: "Quality" };

export function PipelineNode({ data, selected }: NodeProps<FlowNode>) {
  const issueList = useEditor((s) => s.issues);
  const analysis = useEditor((s) => s.analysis);
  // Derive outside the selector: a selector returning a fresh array re-renders forever.
  const issues = useMemo(() => nodeIssues({ issues: issueList, analysis }, data.specId), [issueList, analysis, data.specId]);
  const checks = data.kind === "quality" ? ((data.extra.checks as unknown[]) ?? []).length : 0;
  const subtitle =
    data.kind === "quality" ? `${String(data.extra.action ?? "observe")} · ${checks} check${checks === 1 ? "" : "s"}` : data.type;

  return (
    <div
      className={`node ${data.kind}${selected ? " selected" : ""}${issues.length ? " has-issues" : ""}`}
      style={{ ["--kind-color" as string]: `var(--${data.kind})` }}
      title={issues.join("\n")}
    >
      {issues.length > 0 && <span className="badge">{issues.length}</span>}
      <div className="kind">{LABEL[data.kind]}</div>
      <div className="title">{data.specId}</div>
      <div className="muted">{subtitle}</div>
      {(data.kind === "transform" || data.kind === "sink") && <Handle id={IN_HANDLE} type="target" position={Position.Left} />}
      {(data.kind === "source" || data.kind === "transform") && <Handle id={OUT_HANDLE} type="source" position={Position.Right} />}
      {data.kind !== "quality" && (
        <Handle id={TAP_HANDLE} className="dq" type="target" position={Position.Top} title="Attach a quality block" />
      )}
      {data.kind === "quality" && <Handle id={TAP_SOURCE_HANDLE} type="source" position={Position.Bottom} className="dq" />}
    </div>
  );
}
