import { ApiError, api } from "../api/client";
import type { Issue } from "../api/types";
import { describeDiff, isSameSpec, layoutOf, specDiff, toSpec, type SpecDiff } from "../model/graph";
import { useEditor } from "../store/editor";

/** Fetches the latest version's spec so the editor can show what the draft changes. */
export async function loadLatestVersionSpec(): Promise<void> {
  const { pipeline } = useEditor.getState();
  if (!pipeline?.latest_version) return useEditor.getState().set({ versionSpec: null });
  const version = await api.version(pipeline.id, pipeline.latest_version);
  if (useEditor.getState().pipeline?.id === pipeline.id) useEditor.getState().set({ versionSpec: version.spec });
}

export interface DraftStatus {
  versioned: boolean;
  upToDate: boolean;
  diff: SpecDiff | null;
  lines: string[];
}

export function draftStatus(state = useEditor.getState()): DraftStatus {
  const { pipeline, versionSpec, nodes, edges, meta } = state;
  if (!pipeline?.latest_version || !versionSpec) {
    return { versioned: Boolean(pipeline?.latest_version), upToDate: false, diff: null, lines: [] };
  }
  const diff = specDiff(toSpec(nodes, edges, meta), versionSpec);
  return { versioned: true, upToDate: isSameSpec(diff), diff, lines: describeDiff(diff) };
}

export class VersionError extends Error {
  constructor(public lines: string[]) {
    super(lines.join("; "));
  }
}

/** Saves the draft, freezes it as the next version and returns the version number. */
export async function saveVersion(message: string): Promise<number> {
  const { pipeline, nodes, edges, meta, set } = useEditor.getState();
  if (!pipeline) throw new VersionError(["no pipeline loaded"]);
  const spec = toSpec(nodes, edges, meta);
  try {
    await api.saveDraft(pipeline.id, spec, layoutOf(nodes));
    const version = await api.createVersion(pipeline.id, message);
    set({
      pipeline: { ...pipeline, latest_version: version.version },
      versionSpec: spec,
      saveState: "saved",
      notice: `Saved version ${version.version}`,
    });
    return version.version;
  } catch (e) {
    const detail = e instanceof ApiError ? (e.detail as { issues?: Issue[] }) : null;
    if (detail?.issues) {
      set({ issues: detail.issues });
      throw new VersionError(detail.issues.map((i) => `${i.node_id ?? i.path}: ${i.message}`));
    }
    throw new VersionError([String((e as Error).message)]);
  }
}
