import { useRef, useState, type KeyboardEvent, type PointerEvent } from "react";

// ---------------------------------------------------------------------------
// The editor's panels (components palette, details side panel, bottom drawer) can be
// resized by dragging their inner edge and collapsed; both are remembered per browser.

export interface PanelSize {
  key: string;
  initial: number;
  min: number;
  /** A fixed maximum, or one computed from the window (the drawer never covers the toolbar). */
  max: number | (() => number);
}

export const PALETTE: PanelSize = { key: "tributary.paletteWidth", initial: 248, min: 176, max: 520 };
export const SIDE: PanelSize = { key: "tributary.sideWidth", initial: 400, min: 300, max: 760 };
export const DRAWER: PanelSize = {
  key: "tributary.drawerHeight",
  initial: 340,
  min: 140,
  max: () => Math.max(200, window.innerHeight - 220),
};

const limit = (p: PanelSize) => (typeof p.max === "function" ? p.max() : p.max);
const clamp = (p: PanelSize, v: number) => Math.min(limit(p), Math.max(p.min, Math.round(v)));

function read(key: string): string | null {
  try {
    return localStorage.getItem(key);
  } catch {
    return null;
  }
}

function write(key: string, value: string) {
  try {
    localStorage.setItem(key, value);
  } catch {
    /* not persisted */
  }
}

export function usePanelSize(panel: PanelSize): [number, (v: number) => void] {
  const [size, setSize] = useState(() => {
    const saved = Number(read(panel.key));
    return saved ? clamp(panel, saved) : panel.initial;
  });
  const update = (v: number) => {
    const next = clamp(panel, v);
    setSize(next);
    write(panel.key, String(next));
  };
  return [size, update];
}

export function useCollapsed(key: string): [boolean, (collapsed: boolean) => void] {
  const [collapsed, setCollapsed] = useState(() => read(key) === "1");
  const update = (next: boolean) => {
    setCollapsed(next);
    write(key, next ? "1" : "0");
  };
  return [collapsed, update];
}

/**
 * A draggable edge. ``edge`` is the side of the panel the handle sits on: dragging it away
 * from the panel grows it (right edge: drag right; left edge: drag left; top edge: drag up).
 */
export function ResizeHandle({
  panel,
  size,
  onResize,
  edge,
  label,
}: {
  panel: PanelSize;
  size: number;
  onResize(v: number): void;
  edge: "right" | "left" | "top";
  label: string;
}) {
  const start = useRef<{ pos: number; size: number } | null>(null);
  const vertical = edge !== "top";
  const sign = edge === "right" ? 1 : -1;
  const position = (e: PointerEvent) => (vertical ? e.clientX : e.clientY);
  const onPointerDown = (e: PointerEvent) => {
    start.current = { pos: position(e), size };
    e.currentTarget.setPointerCapture(e.pointerId);
    document.body.classList.add(vertical ? "resizing" : "resizing-rows");
  };
  const onPointerMove = (e: PointerEvent) => {
    if (start.current) onResize(start.current.size + sign * (position(e) - start.current.pos));
  };
  const stop = () => {
    start.current = null;
    document.body.classList.remove("resizing", "resizing-rows");
  };
  const onKeyDown = (e: KeyboardEvent) => {
    const step = { ArrowLeft: -16, ArrowRight: 16, ArrowUp: -16, ArrowDown: 16 }[e.key];
    if (step === undefined) return;
    e.preventDefault();
    onResize(size + (vertical ? sign : -1) * step);
  };
  return (
    <div
      className={`resize-handle ${edge}`}
      role="separator"
      aria-orientation={vertical ? "vertical" : "horizontal"}
      aria-label={label}
      aria-valuenow={size}
      aria-valuemin={panel.min}
      aria-valuemax={limit(panel)}
      tabIndex={0}
      title="Drag to resize · double-click to reset"
      onPointerDown={onPointerDown}
      onPointerMove={onPointerMove}
      onPointerUp={stop}
      onPointerCancel={stop}
      onDoubleClick={() => onResize(panel.initial)}
      onKeyDown={onKeyDown}
    />
  );
}
