import {
  Background,
  BackgroundVariant,
  ControlButton,
  Controls,
  MiniMap,
  ReactFlow,
  ReactFlowProvider,
  useReactFlow,
  type Connection,
  type DefaultEdgeOptions,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { LayoutGrid, MousePointerClick } from "lucide-react";
import { useCallback, type DragEvent } from "react";

import { connectionError, type NodeKind } from "../model/graph";
import { useEditor } from "../store/editor";
import { useTheme } from "../theme";
import { PipelineNode } from "./PipelineNode";

const nodeTypes = { pipeline: PipelineNode };
const edgeDefaults: DefaultEdgeOptions = {};
export const DRAG_MIME = "application/x-tributary-node";

function Flow() {
  const { nodes, edges, onNodesChange, onEdgesChange, connect, addNode, select, set, arrange } = useEditor();
  const { screenToFlowPosition, fitView } = useReactFlow();
  const [, theme] = useTheme();
  const tidy = () => {
    arrange();
    requestAnimationFrame(() => fitView({ padding: 0.15, duration: 300 }));
  };

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
    e.type === "tap"
      ? { ...e, type: "default", style: { stroke: "var(--quality)", strokeDasharray: "5 4", strokeWidth: 1.8 } }
      : { ...e, labelBgPadding: [6, 3] as [number, number], labelBgBorderRadius: 6 },
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
      colorMode={theme}
      minZoom={0.2}
      fitViewOptions={{ maxZoom: 1.1, padding: 0.15 }}
    >
      <Background variant={BackgroundVariant.Dots} gap={18} size={1.4} color="var(--canvas-dot)" />
      <Controls showInteractive={false} position="bottom-left" fitViewOptions={{ padding: 0.15 }}>
        <ControlButton onClick={tidy} title="Tidy layout" aria-label="Tidy layout">
          <LayoutGrid size={14} />
        </ControlButton>
      </Controls>
      <MiniMap
        pannable
        zoomable
        style={{ height: 96, width: 140 }}
        maskColor="color-mix(in srgb, var(--canvas) 70%, transparent)"
        nodeColor={(n) => `var(--${(n.data as { kind: string }).kind})`}
        nodeBorderRadius={6}
      />
      {nodes.length === 0 && (
        <div className="canvas-empty">
          <div>
            <MousePointerClick size={26} />
            <b>Start with a source</b>
            <span className="small">Drag a component from the left, or click it to add.<br />Connect nodes by dragging between their handles.</span>
          </div>
        </div>
      )}
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
