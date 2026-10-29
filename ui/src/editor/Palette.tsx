import { ChevronDown, MousePointerClick, PanelLeftClose, Search } from "lucide-react";
import { useEffect, useLayoutEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";

import type { Catalog, PluginMeta } from "../api/types";
import { KIND_ICONS, pluginIcon } from "../components/icons";
import type { NodeKind } from "../model/graph";
import { useEditor } from "../store/editor";
import { DRAG_MIME } from "./Canvas";
import { PALETTE, ResizeHandle } from "./layout";

const GROUPS: { kind: NodeKind; label: string }[] = [
  { kind: "source", label: "Sources" },
  { kind: "transform", label: "Transforms" },
  { kind: "quality", label: "Quality" },
  { kind: "sink", label: "Sinks" },
];

const KIND_HINT: Record<NodeKind, string> = {
  source: "Reads data into the pipeline",
  transform: "Reshapes data from its inputs",
  quality: "Checks the data of the node it is attached to",
  sink: "Writes data out of the pipeline",
};

type Item = Pick<PluginMeta, "type" | "display_name" | "description" | "capabilities" | "icon" | "options_schema">;

const QUALITY_ITEM: Item = {
  type: "quality",
  display_name: "Quality checks",
  description: "Validate rows before they move on: observe, fail the run, drop or quarantine failing rows.",
  capabilities: [],
  icon: "",
  options_schema: {},
};

/** "Files (local / S3 / GCS / ADLS)" -> "Files": tiles show the short name, the card the full one. */
const shortName = (name: string) => name.replace(/\s*\(.*\)\s*$/, "") || name;

// ---------------------------------------------------------------------------

interface Hover {
  item: Item;
  kind: NodeKind;
  rect: DOMRect;
  unsupported: boolean;
}

export function Palette({ catalog, width, onResize, onCollapse }: { catalog: Catalog; width: number; onResize(w: number): void; onCollapse(): void }) {
  const addNode = useEditor((s) => s.addNode);
  const mode = useEditor((s) => String(s.meta.pipeline.mode ?? "batch"));
  const [query, setQuery] = useState("");
  const [collapsed, setCollapsed] = useState<Set<NodeKind>>(new Set());
  const [hover, setHover] = useState<Hover | null>(null);
  const timer = useRef<number | undefined>(undefined);
  const q = query.trim().toLowerCase();

  const showSoon = (next: Hover) => {
    window.clearTimeout(timer.current);
    timer.current = window.setTimeout(() => setHover(next), hover ? 0 : 280);
  };
  const hide = () => {
    window.clearTimeout(timer.current);
    setHover(null);
  };
  useEffect(() => () => window.clearTimeout(timer.current), []);

  const add = (kind: NodeKind, type: string) => {
    hide();
    addNode(kind, type, { x: 120 + Math.random() * 80, y: 80 + Math.random() * 80 });
  };
  const toggle = (kind: NodeKind) =>
    setCollapsed((c) => {
      const next = new Set(c);
      if (next.has(kind)) next.delete(kind);
      else next.add(kind);
      return next;
    });

  return (
    <aside className="palette" onScroll={hide}>
      <div className="palette-top">
        <div className="search">
          <Search size={14} />
          <input value={query} placeholder="Search components" onChange={(e) => setQuery(e.target.value)} />
        </div>
        <button className="ghost icon sm" onClick={onCollapse} title="Collapse components" aria-label="Collapse components panel">
          <PanelLeftClose size={15} />
        </button>
      </div>
      {GROUPS.map(({ kind, label }) => {
        const all: Item[] = kind === "quality" ? [QUALITY_ITEM] : catalog.components[kind];
        const items = all.filter((p) => !q || `${p.display_name} ${p.type} ${p.description}`.toLowerCase().includes(q));
        if (!items.length) return null;
        const KindIcon = KIND_ICONS[kind];
        const open = !collapsed.has(kind) || Boolean(q);
        return (
          <section key={kind} className="palette-group" style={{ ["--c" as string]: `var(--${kind})` }}>
            <button className="group-head" aria-expanded={open} onClick={() => toggle(kind)}>
              <KindIcon size={13} />
              {label}
              <span className="count">{items.length}</span>
              <span className="spacer" />
              <ChevronDown size={14} className={`chevron${open ? "" : " closed"}`} />
            </button>
            {open && (
              <div className="tile-grid">
                {items.map((p) => {
                  const unsupported = kind === "source" && mode === "streaming" && !p.capabilities.includes("streaming");
                  const Icon = pluginIcon(p.icon, kind, p.type);
                  return (
                    <div
                      key={p.type}
                      className={`tile${unsupported ? " disabled" : ""}`}
                      role="button"
                      tabIndex={0}
                      aria-label={`Add ${p.display_name}`}
                      draggable
                      onDragStart={(e) => {
                        hide();
                        e.dataTransfer.setData(DRAG_MIME, JSON.stringify({ kind, type: p.type }));
                        e.dataTransfer.effectAllowed = "move";
                      }}
                      onClick={() => add(kind, p.type)}
                      onKeyDown={(e) => (e.key === "Enter" || e.key === " ") && (e.preventDefault(), add(kind, p.type))}
                      onMouseEnter={(e) => showSoon({ item: p, kind, rect: e.currentTarget.getBoundingClientRect(), unsupported })}
                      onMouseLeave={hide}
                      onFocus={(e) => setHover({ item: p, kind, rect: e.currentTarget.getBoundingClientRect(), unsupported })}
                      onBlur={hide}
                    >
                      <span className="tile-icon">
                        <Icon size={24} strokeWidth={1.8} />
                        <span className="tile-badge"><KindIcon size={10} strokeWidth={2.4} /></span>
                      </span>
                      <span className="tile-label">{shortName(p.display_name)}</span>
                    </div>
                  );
                })}
              </div>
            )}
          </section>
        );
      })}
      {hover && <DetailsCard hover={hover} catalog={catalog} />}
      <ResizeHandle panel={PALETTE} size={width} edge="right" label="Resize components panel" onResize={(w) => { hide(); onResize(w); }} />
    </aside>
  );
}

function DetailsCard({ hover, catalog }: { hover: Hover; catalog: Catalog }) {
  const ref = useRef<HTMLDivElement>(null);
  const [top, setTop] = useState(hover.rect.top);
  const { item, kind, rect, unsupported } = hover;
  // Keep the card on screen: align with the tile, but never past the bottom edge.
  useLayoutEffect(() => {
    const height = ref.current?.offsetHeight ?? 0;
    setTop(Math.max(8, Math.min(rect.top - 6, window.innerHeight - height - 8)));
  }, [rect]);

  const Icon = pluginIcon(item.icon, kind, item.type);
  const required = item.options_schema.required ?? [];
  const caps = item.capabilities.filter((c) => c === "batch" || c === "streaming");
  const checks = kind === "quality" ? catalog.components.check.map((c) => c.type) : [];

  return createPortal(
    <div ref={ref} className="details-card" role="tooltip" style={{ top, left: rect.right + 12, ["--c" as string]: `var(--${kind})` }}>
      <div className="row" style={{ gap: 12, alignItems: "flex-start" }}>
        <span className="tile-icon lg"><Icon size={22} strokeWidth={1.8} /></span>
        <div style={{ minWidth: 0 }}>
          <div className="details-title">{item.display_name}</div>
          <div className="details-sub">
            <span className="mono">{item.type}</span> · {kind}
          </div>
        </div>
      </div>
      <p>{item.description}</p>
      <div className="details-meta">
        <span className="pill kind-pill">{KIND_HINT[kind]}</span>
        {caps.map((c) => <span key={c} className="pill info">{c}</span>)}
        {kind === "sink" && <span className="pill info">batch & streaming</span>}
      </div>
      {required.length > 0 && (
        <div className="details-row"><span className="muted">Required</span> <span className="mono">{required.join(", ")}</span></div>
      )}
      {checks.length > 0 && (
        <div className="details-row"><span className="muted">Checks</span> <span className="mono">{checks.join(", ")}</span></div>
      )}
      {unsupported && <div className="details-row error-text">Batch only: this pipeline is in streaming mode.</div>}
      <div className="details-foot"><MousePointerClick size={13} />Drag onto the canvas, or click to add</div>
    </div>,
    document.body,
  );
}
