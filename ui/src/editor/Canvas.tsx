import {
  Background,
  Controls,
  MiniMap,
  ReactFlow,
  ReactFlowProvider,
  useReactFlow,
  type Connection,
  type DefaultEdgeOptions,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { useCallback, type DragEvent } from "react";

import { connectionError, type NodeKind } from "../model/graph";
import { useEditor } from "../store/editor";
import { PipelineNode } from "./PipelineNode";

const nodeTypes = { pipeline: PipelineNode };
const edgeDefaults: DefaultEdgeOptions = { style: { strokeWidth: 1.6 } };
export const DRAG_MIME = "application/x-tributary-node";

function Flow() {
  const { nodes, edges, onNodesChange, onEdgesChange, connect, addNode, select, set } = useEditor();
  const { screenToFlowPosition } = useReactFlow();

  const onConnect = useCallback(
    (c: Connection) => {
      const { nodes, edges } = useEditor.getState();
      const error = connectionError(nodes, edges, c.source, c.target, c.targetHandle);
      if (error) set({ notice: error });
      else connect(c.source, c.target, c.targetHandle);
    },
    [connect, set],
  );

  const onDrop = (event: DragEvent) => {
    event.preventDefault();
    const raw = event.dataTransfer.getData(DRAG_MIME);
    if (!raw) return;
    const { kind, type } = JSON.parse(raw) as { kind: NodeKind; type: string };
    addNode(kind, type, screenToFlowPosition({ x: event.clientX - 75, y: event.clientY - 25 }));
  };

  const styled = edges.map((e) =>
    e.type === "tap" ? { ...e, type: "default", style: { stroke: "var(--quality)", strokeDasharray: "5 4" } } : e,
  );

  return (
    <ReactFlow
      nodes={nodes}
      edges={styled}
      nodeTypes={nodeTypes}
      defaultEdgeOptions={edgeDefaults}
      onNodesChange={onNodesChange}
      onEdgesChange={onEdgesChange}
      onConnect={onConnect}
      onNodeClick={(_, node) => select(node.id)}
      onPaneClick={() => select(null)}
      onDragOver={(e) => {
        e.preventDefault();
        e.dataTransfer.dropEffect = "move";
      }}
      onDrop={onDrop}
      deleteKeyCode={["Backspace", "Delete"]}
      fitView
      fitViewOptions={{ maxZoom: 1.1 }}
    >
      <Background gap={20} />
      <Controls showInteractive={false} />
      <MiniMap pannable zoomable style={{ height: 90, width: 130 }} />
    </ReactFlow>
  );
}

export function Canvas() {
  return (
    <div className="canvas">
      <ReactFlowProvider>
        <Flow />
      </ReactFlowProvider>
    </div>
  );
}
