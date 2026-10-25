import { useEffect, useMemo, useRef, useState } from "react";
import { Link, useParams } from "react-router-dom";

import { api, ApiError } from "../api/client";
import { useCatalog } from "../api/hooks";
import type { VersionSummary } from "../api/types";
import { layoutOf, toSpec } from "../model/graph";
import { problemCount, useEditor } from "../store/editor";
import { Canvas } from "./Canvas";
import { Drawer } from "./Drawer";
import { NodePanel } from "./panels/NodePanel";
import { PipelinePanel } from "./panels/PipelinePanel";
import { Palette } from "./Palette";
import { draftStatus, loadLatestVersionSpec, saveVersion, VersionError, type DraftStatus } from "./versioning";

export function EditorPage() {
  const { id = "" } = useParams();
  const catalog = useCatalog();
  const { pipeline, nodes, selectedId, load, notice, set } = useEditor();
  const [loadError, setLoadError] = useState<string | null>(null);

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
  const state = useEditor();
  const { pipeline, env, setEnv, meta, saveState, checking, issues, analysis, set, setDrawer, nodes, edges, versionSpec } = state;
  const problems = problemCount({ issues, analysis });
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const status = useMemo(() => draftStatus(state), [pipeline, nodes, edges, meta, versionSpec]);
  const [dialog, setDialog] = useState<"version" | "run" | null>(null);
  const envs = Object.keys((meta.environments as object) ?? {});
  if (!pipeline) return null;
  return (
    <div className="toolbar">
      <Link to="/" className="small">Pipelines</Link>
      <b>{pipeline.name}</b>
      <VersionPill status={status} version={pipeline.latest_version} onClick={() => setDialog("version")} />
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
      <button className="primary" onClick={() => setDialog("run")}>
        Run
      </button>
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

function VersionPill({ status, version, onClick }: { status: DraftStatus; version: number; onClick(): void }) {
  if (!version) {
    return <button className="pill warn" style={{ border: "none" }} title="Runs need a saved version" onClick={onClick}>not versioned yet</button>;
  }
  if (!status.diff) return <span className="pill">v{version}</span>;
  if (status.upToDate) return <span className="pill ok" title="The draft matches the latest version">v{version}</span>;
  return (
    <button className="pill warn" style={{ border: "none" }} title={`Not in v${version}:\n${status.lines.join("\n")}`} onClick={onClick}>
      v{version} · draft has unsaved changes
    </button>
  );
}

function ChangeList({ status, version }: { status: DraftStatus; version: number }) {
  if (!status.lines.length) return null;
  return (
    <div className="card small" style={{ borderColor: "var(--warn)", background: "var(--warn-soft)", margin: "8px 0" }}>
      <b>Changes not in v{version}</b>
      <ul style={{ margin: "4px 0 0", paddingLeft: 18 }}>{status.lines.map((l) => <li key={l}>{l}</li>)}</ul>
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
    <Modal title="Save version" onClose={onClose}>
      <p className="muted small">Versions are validated for every environment and can't be changed afterwards. Runs use versions.</p>
      {status.upToDate && <p className="small">The draft is identical to v{pipeline?.latest_version}; saving creates a copy.</p>}
      <ChangeList status={status} version={pipeline?.latest_version ?? 0} />
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
    <Modal title="Run pipeline" onClose={onClose}>
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
      </div>
      {draftMissing ? (
        <>
          {latest ? (
            <>
              <p className="small" style={{ marginBottom: 0 }}>Runs use saved versions. These draft changes would <b>not</b> run with v{latest}:</p>
              <ChangeList status={status} version={latest} />
            </>
          ) : (
            <p className="small">Runs use saved versions and this pipeline has none yet. Save the draft as v1 to run it.</p>
          )}
          <div className="field">
            <label>Version message</label>
            <input value={message} onChange={(e) => setMessage(e.target.value)} placeholder="What changed?" />
          </div>
        </>
      ) : (
        <p className="muted small">
          {version === latest ? `The draft matches v${latest}.` : `Runs v${version} exactly as it was saved.`}
        </p>
      )}
      {errors.map((e) => <div key={e} className="error-text small">{e}</div>)}
      <div className="row" style={{ marginTop: 12 }}>
        <span className="spacer" />
        <button onClick={onClose}>Cancel</button>
        {draftMissing && latest > 0 && <button disabled={busy} onClick={() => start(false)}>Run v{latest} as saved</button>}
        {draftMissing ? (
          <button className="primary" disabled={busy} onClick={() => start(true)}>Save as v{latest + 1} &amp; run</button>
        ) : (
          <button className="primary" disabled={busy} onClick={() => start(false)}>Start run</button>
        )}
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
