import type { Catalog } from "../api/types";
import type { NodeKind } from "../model/graph";
import { useEditor } from "../store/editor";
import { DRAG_MIME } from "./Canvas";

const GROUPS: { kind: NodeKind; label: string }[] = [
  { kind: "source", label: "Sources" },
  { kind: "transform", label: "Transforms" },
  { kind: "quality", label: "Quality" },
  { kind: "sink", label: "Sinks" },
];

export function Palette({ catalog }: { catalog: Catalog }) {
  const addNode = useEditor((s) => s.addNode);
  const mode = useEditor((s) => String(s.meta.pipeline.mode ?? "batch"));

  return (
    <aside className="palette">
      <div className="muted small" style={{ marginBottom: 8 }}>Drag onto the canvas, or click to add.</div>
      {GROUPS.map(({ kind, label }) => {
        const items =
          kind === "quality"
            ? [{ type: "quality", display_name: "Quality checks", description: "Checks attached to a node or sink", capabilities: [] as string[] }]
            : catalog.components[kind];
        return (
          <div key={kind}>
            <h3>{label}</h3>
            {items.map((p) => {
              const unsupported = kind === "source" && mode === "streaming" && !p.capabilities.includes("streaming");
              return (
                <div
                  key={p.type}
                  className="palette-item"
                  style={{ ["--kind-color" as string]: `var(--${kind})`, opacity: unsupported ? 0.55 : 1 }}
                  draggable
                  title={p.description + (unsupported ? " (batch only)" : "")}
                  onDragStart={(e) => {
                    e.dataTransfer.setData(DRAG_MIME, JSON.stringify({ kind, type: p.type }));
                    e.dataTransfer.effectAllowed = "move";
                  }}
                  onClick={() => addNode(kind, p.type, { x: 120 + Math.random() * 80, y: 80 + Math.random() * 80 })}
                >
                  {p.display_name}
                </div>
              );
            })}
          </div>
        );
      })}
    </aside>
  );
}
