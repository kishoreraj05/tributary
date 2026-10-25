import {
  ChevronRight,
  CircleAlert,
  CircleCheck,
  Cloud,
  CloudOff,
  GitCommitHorizontal,
  Loader2,
  Network,
  Play,
  Save,
  TriangleAlert,
  X,
} from "lucide-react";
import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { Link, useParams } from "react-router-dom";

import { api, ApiError } from "../api/client";
import { useCatalog } from "../api/hooks";
import { IconChip } from "../components/icons";
import type { VersionSummary } from "../api/types";
import { layoutOf, toSpec } from "../model/graph";
import { problemCount, useEditor } from "../store/editor";
import { Canvas } from "./Canvas";
import { Drawer } from "./Drawer";
import { NodePanel } from "./panels/NodePanel";
import { PipelinePanel } from "./panels/PipelinePanel";
import { Palette, usePaletteWidth } from "./Palette";
import { draftStatus, loadLatestVersionSpec, saveVersion, VersionError, type DraftStatus } from "./versioning";

export function EditorPage() {
  const { id = "" } = useParams();
  const catalog = useCatalog();
  const { pipeline, nodes, selectedId, load, notice, set } = useEditor();
  const [loadError, setLoadError] = useState<string | null>(null);
  const [paletteWidth, setPaletteWidth] = usePaletteWidth();

  useEffect(() => {
    api
      .pipeline(id)
      .then((p) => {
        load(p);
        return loadLatestVersionSpec();
      })
      .catch((e) => setLoadError(String(e.message ?? e)));
  }, [id, load]);
  useAutosave();
  useLiveValidation();
  useEffect(() => {
    if (!notice) return;
    const timer = setTimeout(() => set({ notice: null }), 3500);
    return () => clearTimeout(timer);
  }, [notice, set]);

  if (loadError) return <div className="page"><div className="callout danger"><CircleAlert size={16} />{loadError}</div></div>;
  if (!pipeline || pipeline.id !== id || !catalog)
    return <div className="page row muted"><Loader2 size={16} className="spin" /> Loading pipeline…</div>;
  const selected = nodes.find((n) => n.id === selectedId);

  return (
    <div className="editor">
      <Toolbar />
      <div className="workspace" style={{ ["--palette-w" as string]: `${paletteWidth}px` }}>
        <Palette catalog={catalog} width={paletteWidth} onResize={setPaletteWidth} />
        <div className="center">
          <Canvas />
          <Drawer />
        </div>
        <aside className="side">{selected ? <NodePanel key={selected.id} node={selected} /> : <PipelinePanel />}</aside>
      </div>
      {notice && <div className="toast"><CircleCheck size={15} />{notice}</div>}
    </div>
  );
}

const SAVE_STATE = {
  saved: { icon: Cloud, label: "Saved" },
  dirty: { icon: Cloud, label: "Unsaved" },
  saving: { icon: Loader2, label: "Saving…" },
  error: { icon: CloudOff, label: "Save failed" },
};

function Toolbar() {
  const state = useEditor();
  const { pipeline, env, setEnv, meta, saveState, checking, issues, analysis, set, setDrawer, nodes, edges, versionSpec } = state;
  const problems = problemCount({ issues, analysis });
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const status = useMemo(() => draftStatus(state), [pipeline, nodes, edges, meta, versionSpec]);
  const [dialog, setDialog] = useState<"version" | "run" | null>(null);
  const envs = Object.keys((meta.environments as object) ?? {});
  if (!pipeline) return null;
  const save = SAVE_STATE[saveState];
  return (
    <div className="toolbar">
      <div className="crumbs">
        <Link to="/">Pipelines</Link>
        <ChevronRight size={14} className="muted" />
        <span className="name">{pipeline.name}</span>
      </div>
      <VersionPill status={status} version={pipeline.latest_version} onClick={() => setDialog("version")} />
      <span className={`status${saveState === "error" ? " bad" : ""}`} title="Drafts save automatically">
        <save.icon size={14} className={saveState === "saving" ? "spin" : ""} />{save.label}
      </span>
      <span className="spacer" />
      <ValidationStatus checking={checking} problems={problems} onClick={() => setDrawer("problems")} />
      <span className="sep" />
      <label className="env-select" style={{ margin: 0 }}>
        Env
        <select value={env ?? ""} onChange={(e) => setEnv(e.target.value || null)}>
          {envs.length === 0 && <option value="">default</option>}
          {envs.map((e) => <option key={e} value={e}>{e}</option>)}
        </select>
      </label>
      <button className="ghost" onClick={() => setDrawer("plan")} title="Execution plan and resources"><Network size={15} />Plan</button>
      <button onClick={() => setDialog("version")}><Save size={15} />Save version</button>
      <button className="primary" onClick={() => setDialog("run")}><Play size={14} fill="currentColor" />Run</button>
      {dialog === "version" && <VersionDialog status={status} onClose={() => setDialog(null)} />}
      {dialog === "run" && (
        <RunDialog
          status={status}
          onClose={() => setDialog(null)}
          onStarted={(runId) => { set({ lastRunId: runId }); setDrawer("runs"); setDialog(null); }}
        />
      )}
    </div>
  );
}

function ValidationStatus({ checking, problems, onClick }: { checking: string; problems: number; onClick(): void }) {
  if (checking !== "idle")
    return <span className="status"><Loader2 size={14} className="spin" />{checking === "deep" ? "Checking columns…" : "Validating…"}</span>;
  if (problems)
    return (
      <button className="pill bad" onClick={onClick} style={{ border: "none", height: 26 }}>
        <CircleAlert size={13} />{problems} problem{problems === 1 ? "" : "s"}
      </button>
    );
  return <span className="status ok"><CircleCheck size={14} />Valid</span>;
}

function VersionPill({ status, version, onClick }: { status: DraftStatus; version: number; onClick(): void }) {
  if (!version) {
    return <button className="pill warn" title="Runs need a saved version" onClick={onClick}><span className="dot" />Not versioned</button>;
  }
  if (!status.diff) return <span className="pill"><GitCommitHorizontal size={12} />v{version}</span>;
  if (status.upToDate) return <span className="pill ok" title="The draft matches the latest version"><GitCommitHorizontal size={12} />v{version}</span>;
  return (
    <button className="pill warn" title={`Not in v${version}:\n${status.lines.join("\n")}`} onClick={onClick}>
      <span className="dot" />v{version} · unsaved changes
    </button>
  );
}

function ChangeList({ status, version }: { status: DraftStatus; version: number }) {
  if (!status.lines.length) return null;
  return (
    <div className="callout warn">
      <TriangleAlert size={16} />
      <div>
        <b>Changes not in v{version}</b>
        <ul>{status.lines.map((l) => <li key={l}>{l}</li>)}</ul>
      </div>
    </div>
  );
}

function Modal({ title, icon, children, footer, onClose }: { title: string; icon?: ReactNode; children: ReactNode; footer?: ReactNode; onClose(): void }) {
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);
  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal" role="dialog" aria-label={title} onClick={(e) => e.stopPropagation()}>
        <div className="modal-head">
          {icon}
          <h2>{title}</h2>
          <span className="spacer" />
          <button className="ghost icon sm" onClick={onClose} aria-label="Close"><X size={15} /></button>
        </div>
        {children}
        {footer && <div className="modal-foot">{footer}</div>}
      </div>
    </div>
  );
}

function VersionDialog({ status, onClose }: { status: DraftStatus; onClose(): void }) {
  const pipeline = useEditor((s) => s.pipeline);
  const [message, setMessage] = useState("");
  const [busy, setBusy] = useState(false);
  const [errors, setErrors] = useState<string[]>([]);
  const save = async () => {
    setBusy(true);
    try {
      await saveVersion(message);
      onClose();
    } catch (e) {
      setErrors(e instanceof VersionError ? e.lines : [String(e)]);
    } finally {
      setBusy(false);
    }
  };
  return (
    <Modal
      title="Save version"
      icon={<IconChip icon={Save} color="var(--accent)" />}
      onClose={onClose}
      footer={
        <>
          <button onClick={onClose}>Cancel</button>
          <button className="primary" disabled={busy} onClick={save}>
            {busy ? <Loader2 size={14} className="spin" /> : <Save size={14} />}Save v{(pipeline?.latest_version ?? 0) + 1}
          </button>
        </>
      }
    >
      <p className="muted small" style={{ marginTop: 0 }}>Versions are validated for every environment and can't be changed afterwards. Runs always use a version.</p>
      {status.upToDate && <p className="small">The draft is identical to v{pipeline?.latest_version}; saving creates a copy.</p>}
      <ChangeList status={status} version={pipeline?.latest_version ?? 0} />
      <div className="field">
        <label>Message</label>
        <input autoFocus value={message} onChange={(e) => setMessage(e.target.value)} onKeyDown={(e) => e.key === "Enter" && save()} placeholder="What changed?" />
      </div>
      <Errors errors={errors} />
    </Modal>
  );
}

function RunDialog({ status, onClose, onStarted }: { status: DraftStatus; onClose(): void; onStarted(runId: string): void }) {
  const { pipeline, env, meta } = useEditor();
  const latest = pipeline?.latest_version ?? 0;
  const [versions, setVersions] = useState<VersionSummary[]>([]);
  const [version, setVersion] = useState<number>(latest);
  const [runEnv, setRunEnv] = useState(env ?? "default");
  const [message, setMessage] = useState("");
  const [busy, setBusy] = useState(false);
  const [errors, setErrors] = useState<string[]>([]);
  const envs = Object.keys((meta.environments as object) ?? {});
  useEffect(() => {
    if (pipeline && latest) api.versions(pipeline.id).then(setVersions);
  }, [pipeline, latest]);

  // The draft only matters when running the latest version (or when nothing is versioned).
  const draftMissing = !latest || (version === latest && !status.upToDate && status.diff !== null);

  const start = async (saveFirst: boolean) => {
    setBusy(true);
    setErrors([]);
    try {
      const number = saveFirst ? await saveVersion(message || "saved before run") : version;
      const run = await api.startRun(pipeline!.id, runEnv, number);
      onStarted(run.id);
    } catch (e) {
      setErrors(e instanceof VersionError ? e.lines : [e instanceof ApiError ? e.message : String(e)]);
    } finally {
      setBusy(false);
    }
  };

  return (
    <Modal
      title="Run pipeline"
      icon={<IconChip icon={Play} color="var(--ok)" />}
      onClose={onClose}
      footer={
        <>
          <button onClick={onClose}>Cancel</button>
          {draftMissing && latest > 0 && <button disabled={busy} onClick={() => start(false)}>Run v{latest} as saved</button>}
          <button className="primary" disabled={busy} onClick={() => start(draftMissing)}>
            {busy ? <Loader2 size={14} className="spin" /> : <Play size={13} fill="currentColor" />}
            {draftMissing ? `Save as v${latest + 1} & run` : "Start run"}
          </button>
        </>
      }
    >
      {latest > 0 && (
        <div className="field">
          <label>Version</label>
          <select value={version} onChange={(e) => setVersion(Number(e.target.value))}>
            {[...versions].reverse().map((v) => <option key={v.version} value={v.version}>v{v.version}{v.message ? ` — ${v.message}` : ""}</option>)}
          </select>
        </div>
      )}
      <div className="field">
        <label>Environment</label>
        <select value={runEnv} onChange={(e) => setRunEnv(e.target.value)}>
          {(envs.length ? envs : ["default"]).map((e) => <option key={e} value={e}>{e}</option>)}
        </select>
        <div className="desc">Connections and variables of this environment are used.</div>
      </div>
      {draftMissing ? (
        <>
          {latest ? (
            <>
              <ChangeList status={status} version={latest} />
              <p className="small muted" style={{ marginTop: -4 }}>Runs use saved versions, so these changes would <b>not</b> run with v{latest}.</p>
            </>
          ) : (
            <div className="callout warn"><TriangleAlert size={16} /><div>This pipeline has no saved version yet. The draft will be saved as v1, then run.</div></div>
          )}
          <div className="field">
            <label>Version message</label>
            <input value={message} onChange={(e) => setMessage(e.target.value)} placeholder="What changed?" />
          </div>
        </>
      ) : (
        <p className="row small muted" style={{ margin: 0 }}>
          <CircleCheck size={14} className="status ok" />
          {version === latest ? `The draft matches v${latest}.` : `Runs v${version} exactly as it was saved.`}
        </p>
      )}
      <Errors errors={errors} />
    </Modal>
  );
}

function Errors({ errors }: { errors: string[] }) {
  if (!errors.length) return null;
  return (
    <div className="callout danger" style={{ marginTop: 12, marginBottom: 0 }}>
      <CircleAlert size={16} />
      <div>{errors.map((e) => <div key={e}>{e}</div>)}</div>
    </div>
  );
}

/** Drafts are saved shortly after every edit (they are allowed to be invalid). */
function useAutosave() {
  const revision = useEditor((s) => s.revision);
  useEffect(() => {
    if (revision === 0) return;
    const timer = setTimeout(async () => {
      const { pipeline, nodes, edges, meta, set } = useEditor.getState();
      if (!pipeline) return;
      set({ saveState: "saving" });
      try {
        await api.saveDraft(pipeline.id, toSpec(nodes, edges, meta), layoutOf(nodes));
        if (useEditor.getState().revision === revision) set({ saveState: "saved" });
      } catch {
        set({ saveState: "error" });
      }
    }, 800);
    return () => clearTimeout(timer);
  }, [revision]);
}

/** Fast structural validation on every change; column-level analysis once that passes. */
function useLiveValidation() {
  const { nodes, edges, meta, env, pipeline } = useEditor();
  const spec = useMemo(() => JSON.stringify(toSpec(nodes, edges, meta)), [nodes, edges, meta]);
  const requestId = useRef(0);
  useEffect(() => {
    if (!pipeline) return;
    const id = ++requestId.current;
    const controller = new AbortController();
    const { set } = useEditor.getState();
    const shallow = setTimeout(async () => {
      set({ checking: "shallow" });
      try {
        const result = await api.validate({ spec: JSON.parse(spec), env }, controller.signal);
        if (id !== requestId.current) return;
        set({ issues: result.issues, warnings: result.warnings, checking: result.valid ? "deep" : "idle" });
        if (!result.valid) return;
        const deep = await api.validate({ spec: JSON.parse(spec), env, deep: true }, controller.signal);
        if (id !== requestId.current) return;
        set({ analysis: deep.nodes ?? {}, issues: deep.issues, checking: "idle" });
      } catch (e) {
        if ((e as Error).name !== "AbortError" && id === requestId.current) set({ checking: "idle" });
      }
    }, 500);
    return () => {
      clearTimeout(shallow);
      controller.abort();
    };
  }, [spec, env, pipeline]);
}
