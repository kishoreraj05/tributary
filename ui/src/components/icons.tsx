import {
  Activity,
  BarChart3,
  Box,
  Code,
  Database,
  FileText,
  FolderOpen,
  HardDriveDownload,
  HardDriveUpload,
  Plug,
  Radio,
  ShieldCheck,
  Shuffle,
  Terminal,
  type LucideIcon,
} from "lucide-react";

import type { NodeKind } from "../model/graph";

/** Plugins declare a generic icon word (see Plugin.icon in the engine). */
const PLUGIN_ICONS: Record<string, LucideIcon> = {
  file: FileText,
  stream: Radio,
  code: Code,
  terminal: Terminal,
  database: Database,
  chart: BarChart3,
  box: Box,
};

export const KIND_ICONS: Record<NodeKind, LucideIcon> = {
  source: HardDriveDownload,
  transform: Shuffle,
  quality: ShieldCheck,
  sink: HardDriveUpload,
};

export function pluginIcon(icon: string | undefined, kind?: NodeKind): LucideIcon {
  if (kind === "quality") return ShieldCheck;
  return (icon && PLUGIN_ICONS[icon]) || (kind ? KIND_ICONS[kind] : Box);
}

const CONNECTION_ICONS: Record<string, LucideIcon> = {
  kafka: Radio,
  file: FolderOpen,
  postgres: Database,
  jdbc: Database,
  prometheus: Activity,
};

export function connectionIcon(type: string): LucideIcon {
  return CONNECTION_ICONS[type] ?? Plug;
}

export function IconChip({ icon: Icon, color, size = 16, large = false }: { icon: LucideIcon; color: string; size?: number; large?: boolean }) {
  return (
    <span className={`icon-chip${large ? " lg" : ""}`} style={{ ["--c" as string]: color }}>
      <Icon size={size} strokeWidth={2} />
    </span>
  );
}

/** "5m ago", "3h ago", "2d ago" — then a date. */
export function timeAgo(iso: string): string {
  const seconds = (Date.now() - new Date(iso).getTime()) / 1000;
  if (seconds < 60) return "just now";
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  if (seconds < 7 * 86400) return `${Math.floor(seconds / 86400)}d ago`;
  return new Date(iso).toLocaleDateString();
}
