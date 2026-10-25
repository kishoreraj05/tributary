import { useEffect, useMemo, useRef, useState } from "react";
import { Link, useParams } from "react-router-dom";

import { api, ApiError } from "../api/client";
import { useCatalog } from "../api/hooks";
import type { Issue, VersionSummary } from "../api/types";
import { layoutOf, toSpec } from "../model/graph";
import { problemCount, useEditor } from "../store/editor";
import { Canvas } from "./Canvas";
import { Drawer } from "./Drawer";
import { NodePanel } from "./panels/NodePanel";
import { PipelinePanel } from "./panels/PipelinePanel";
import { Palette } from "./Palette";

export function EditorPage() {
  const { id = "" } = useParams();
  const catalog = useCatalog();
  const { pipeline, nodes, selectedId, load, notice, set } = useEditor();
  const [loadError, setLoadError] = useState<string | null>(null);

  useEffect(() => {
    api.pipeline(id).then(load).catch((e) => setLoadError(String(e.message ?? e)));
  }, [id, load]);
  useAutosave();
  useLiveValidation();
  useEffect(() => {
    if (!notice) return;
    const timer = setTimeout(() => set({ notice: null }), 3500);
    return () => clearTimeout(timer);
  }, [notice, set]);

  if (loadError) return <div className="page error-text">{loadError}</div>;
  if (!pipeline || pipeline.id !== id || !catalog) return <div className="page muted">Loading…</div>;
  const selected = nodes.find((n) => n.id === selectedId);

  return (
    <div className="editor">
      <Toolbar />
      <div className="workspace">
        <Palette catalog={catalog} />
        <div className="center">
          <Canvas />
          <Drawer />
        </div>
        <aside className="side">{selected ? <NodePanel key={selected.id} node={selected} /> : <PipelinePanel />}</aside>
      </div>
      {notice && (
        <div className="card small" style={{ position: "fixed", bottom: 16, left: "50%", transform: "translateX(-50%)", zIndex: 40 }}>
          {notice}
        </div>
      )}
    </div>
  );
}

const SAVE_LABEL = { saved: "Saved", dirty: "Unsaved changes", saving: "Saving…", error: "Save failed" };

function Toolbar() {
  const { pipeline, env, setEnv, meta, saveState, checking, issues, analysis, set, setDrawer } = useEditor();
  const problems = problemCount({ issues, analysis });
  const [dialog, setDialog] = useState<"version" | "run" | null>(null);
  const envs = Object.keys((meta.environments as object) ?? {});
  if (!pipeline) return null;
  return (
    <div className="toolbar">
      <Link to="/" className="small">Pipelines</Link>
      <b>{pipeline.name}</b>
      <span className="pill">v{pipeline.latest_version || "–"}</span>
      <span className={`small ${saveState === "error" ? "error-text" : "muted"}`}>{SAVE_LABEL[saveState]}</span>
      <span className="spacer" />
      <span className="small muted">
        {checking === "shallow" ? "Validating…" : checking === "deep" ? "Checking columns…" : problems ? `${problems} problem${problems === 1 ? "" : "s"}` : "Valid"}
      </span>
      <label className="row small" style={{ margin: 0 }}>
        env
        <select value={env ?? ""} onChange={(e) => setEnv(e.target.value || null)}>
          {envs.length === 0 && <option value="">default</option>}
          {envs.map((e) => <option key={e} value={e}>{e}</option>)}
        </select>
      </label>
      <button onClick={() => setDrawer("plan")}>Plan</button>
      <button onClick={() => setDialog("version")}>Save version</button>
      <button className="primary" disabled={!pipeline.latest_version} title={pipeline.latest_version ? "" : "Save a version first"} onClick={() => setDialog("run")}>
        Run
      </button>
      {dialog === "version" && <VersionDialog onClose={() => setDialog(null)} />}
      {dialog === "run" && (
        <RunDialog
          onClose={() => setDialog(null)}
          onStarted={(runId) => { set({ lastRunId: runId }); setDrawer("runs"); setDialog(null); }}
        />
      )}
    </div>
  );
}

function Modal({ title, children, onClose }: { title: string; children: React.ReactNode; onClose(): void }) {
  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <h2>{title}</h2>
        {children}
      </div>
    </div>
  );
}

function VersionDialog({ onClose }: { onClose(): void }) {
  const { pipeline, nodes, edges, meta, set } = useEditor();
  const [message, setMessage] = useState("");
  const [busy, setBusy] = useState(false);
  const [errors, setErrors] = useState<string[]>([]);
  const save = async () => {
    if (!pipeline) return;
    setBusy(true);
    try {
      await api.saveDraft(pipeline.id, toSpec(nodes, edges, meta), layoutOf(nodes));
      const version = await api.createVersion(pipeline.id, message);
      set({ pipeline: { ...pipeline, latest_version: version.version }, saveState: "saved", notice: `Saved version ${version.version}` });
      onClose();
    } catch (e) {
      const detail = e instanceof ApiError ? (e.detail as { issues?: Issue[]; env?: string }) : null;
      setErrors(detail?.issues?.map((i) => `${i.node_id ?? i.path}: ${i.message}`) ?? [String((e as Error).message)]);
      if (detail?.issues) set({ issues: detail.issues });
    } finally {
      setBusy(false);
    }
  };
  return (
    <Modal title="Save version" onClose={onClose}>
      <p className="muted small">Versions are validated for every environment and can't be changed afterwards. Runs use versions.</p>
      <div className="field">
        <label>Message</label>
        <input autoFocus value={message} onChange={(e) => setMessage(e.target.value)} placeholder="What changed?" />
      </div>
      {errors.map((e) => <div key={e} className="error-text small">{e}</div>)}
      <div className="row" style={{ marginTop: 12 }}>
        <span className="spacer" />
        <button onClick={onClose}>Cancel</button>
        <button className="primary" disabled={busy} onClick={save}>Save version</button>
      </div>
    </Modal>
  );
}

function RunDialog({ onClose, onStarted }: { onClose(): void; onStarted(runId: string): void }) {
  const { pipeline, env, meta } = useEditor();
  const [versions, setVersions] = useState<VersionSummary[]>([]);
  const [version, setVersion] = useState<number | undefined>(pipeline?.latest_version);
  const [runEnv, setRunEnv] = useState(env ?? "default");
  const [error, setError] = useState<string | null>(null);
  const envs = Object.keys((meta.environments as object) ?? {});
  useEffect(() => {
    if (pipeline) api.versions(pipeline.id).then(setVersions);
  }, [pipeline]);
  const start = async () => {
    try {
      const run = await api.startRun(pipeline!.id, runEnv, version);
      onStarted(run.id);
    } catch (e) {
      setError(e instanceof ApiError ? e.message : String(e));
    }
  };
  return (
    <Modal title="Run pipeline" onClose={onClose}>
      <div className="field">
        <label>Version</label>
        <select value={version} onChange={(e) => setVersion(Number(e.target.value))}>
          {[...versions].reverse().map((v) => <option key={v.version} value={v.version}>v{v.version}{v.message ? ` — ${v.message}` : ""}</option>)}
        </select>
      </div>
      <div className="field">
        <label>Environment</label>
        <select value={runEnv} onChange={(e) => setRunEnv(e.target.value)}>
          {(envs.length ? envs : ["default"]).map((e) => <option key={e} value={e}>{e}</option>)}
        </select>
      </div>
      <p className="muted small">The run uses the saved version, not unsaved canvas changes.</p>
      {error && <div className="error-text small">{error}</div>}
      <div className="row" style={{ marginTop: 12 }}>
        <span className="spacer" />
        <button onClick={onClose}>Cancel</button>
        <button className="primary" onClick={start}>Start run</button>
      </div>
    </Modal>
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
