import Editor from "@monaco-editor/react";
import { useEffect, useMemo, useState } from "react";
import YAML from "yaml";

import { api, ApiError, subscribeRun } from "../api/client";
import type { PlanResult, QualityResult, Run } from "../api/types";
import { monacoTheme } from "../components/monaco";
import { toSpec } from "../model/graph";
import { problemCount, useEditor, type DrawerTab } from "../store/editor";

const TABS: { id: DrawerTab; label: string }[] = [
  { id: "preview", label: "Preview" },
  { id: "problems", label: "Problems" },
  { id: "yaml", label: "YAML" },
  { id: "plan", label: "Plan" },
  { id: "runs", label: "Runs" },
];

export function Drawer() {
  const { drawer, setDrawer, issues, analysis } = useEditor();
  const problems = problemCount({ issues, analysis });
  return (
    <div className={`drawer${drawer ? " open" : ""}`}>
      <div className="tabs">
        {TABS.map((t) => (
          <button key={t.id} className={drawer === t.id ? "active" : ""} onClick={() => setDrawer(drawer === t.id ? null : t.id)}>
            {t.label}
            {t.id === "problems" && problems > 0 && <span className="pill bad" style={{ marginLeft: 6 }}>{problems}</span>}
          </button>
        ))}
        <span className="spacer" />
        {drawer && <button className="link small" onClick={() => setDrawer(null)}>Hide</button>}
      </div>
      {drawer && (
        <div className="drawer-body">
          {drawer === "preview" && <PreviewView />}
          {drawer === "problems" && <ProblemsView />}
          {drawer === "yaml" && <YamlView />}
          {drawer === "plan" && <PlanView />}
          {drawer === "runs" && <RunsView />}
        </div>
      )}
    </div>
  );
}

function Cell({ value }: { value: unknown }) {
  if (value === null || value === undefined) return <td className="null">null</td>;
  return <td>{typeof value === "object" ? JSON.stringify(value) : String(value)}</td>;
}

export function QualityTable({ results }: { results: QualityResult[] }) {
  if (!results.length) return null;
  return (
    <table className="grid" style={{ marginTop: 10 }}>
      <thead>
        <tr><th>Check</th><th>On</th><th>Result</th><th>Failed / total</th><th>Observed</th>{results.some((r) => r.batch_id !== null) && <th>Batch</th>}</tr>
      </thead>
      <tbody>
        {results.map((r, i) => (
          <tr key={i}>
            <td>{r.quality_id}.{r.check_id} <span className="muted">({r.check_type})</span></td>
            <td>{r.node_id}</td>
            <td><span className={`pill ${r.passed ? "ok" : "bad"}`}>{r.passed ? "pass" : "fail"}</span></td>
            <td>{r.level === "row" ? `${r.failed_rows} / ${r.total_rows}` : "—"}</td>
            <td>{r.observed_value ?? "—"}</td>
            {r.batch_id !== null && <td>{r.batch_id}</td>}
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function PreviewView() {
  const preview = useEditor((s) => s.preview);
  if (!preview) return <div className="muted">Select a node and press ▶ Test to see sample output.</div>;
  if (preview.loading) return <div className="muted">Running {preview.specId} on sample data… (the first preview starts Spark and can take a little while)</div>;
  if (preview.error) return <div className="error-text">{preview.specId}: {preview.error}</div>;
  const result = preview.result!;
  const fields = result.schema.fields;
  return (
    <div>
      <div className="row small" style={{ marginBottom: 6 }}>
        <b>{result.node_id}</b>
        <span className="muted">{result.rows.length} rows</span>
        {result.target && <span className="pill info">{result.written ? "written to" : "dry run →"} {result.target}</span>}
      </div>
      <div style={{ overflow: "auto", maxHeight: 220 }}>
        <table className="grid">
          <thead>
            <tr>{fields.map((f) => <th key={f.name}>{f.name} <span className="muted">{typeof f.type === "string" ? f.type : "struct"}</span></th>)}</tr>
          </thead>
          <tbody>
            {result.rows.map((row, i) => (
              <tr key={i}>{fields.map((f) => <Cell key={f.name} value={row[f.name]} />)}</tr>
            ))}
          </tbody>
        </table>
      </div>
      <QualityTable results={result.quality} />
    </div>
  );
}

function ProblemsView() {
  const { issues, warnings, analysis, nodes, select, checking } = useEditor();
  const select_ = (specId: string | null) => {
    const node = nodes.find((n) => n.data.specId === specId);
    if (node) select(node.id);
  };
  const columnErrors = Object.values(analysis).filter((a) => !a.ok && !a.error?.startsWith("skipped"));
  if (!issues.length && !columnErrors.length && !warnings.length)
    return <div className="muted">{checking !== "idle" ? "Checking…" : "No problems found."}</div>;
  return (
    <table className="grid list">
      <tbody>
        {issues.map((i, k) => (
          <tr key={`i${k}`} onClick={() => select_(i.node_id)}>
            <td><span className="pill bad">error</span></td><td>{i.node_id ?? "pipeline"}</td><td className="mono">{i.path}</td><td style={{ whiteSpace: "normal" }}>{i.message}</td>
          </tr>
        ))}
        {columnErrors.map((a) => (
          <tr key={`a${a.node_id}`} onClick={() => select_(a.node_id)}>
            <td><span className="pill bad">columns</span></td><td>{a.node_id}</td><td /><td style={{ whiteSpace: "normal" }}>{a.error}</td>
          </tr>
        ))}
        {warnings.map((w) => (
          <tr key={w}><td><span className="pill warn">warning</span></td><td colSpan={3} style={{ whiteSpace: "normal" }}>{w}</td></tr>
        ))}
      </tbody>
    </table>
  );
}

function YamlView() {
  const { nodes, edges, meta, applySpec } = useEditor();
  const current = useMemo(() => YAML.stringify(toSpec(nodes, edges, meta)), [nodes, edges, meta]);
  const [text, setText] = useState(current);
  const [error, setError] = useState<string | null>(null);
  const [dirty, setDirty] = useState(false);
  useEffect(() => {
    if (!dirty) setText(current);
  }, [current, dirty]);

  const apply = () => {
    try {
      const parsed = YAML.parse(text);
      if (!parsed || typeof parsed !== "object") throw new Error("the document must be a mapping");
      applySpec(parsed);
      setDirty(false);
      setError(null);
    } catch (e) {
      setError(String((e as Error).message));
    }
  };
  return (
    <div style={{ display: "grid", gridTemplateRows: "auto 1fr", height: "100%" }}>
      <div className="row small" style={{ marginBottom: 6 }}>
        <span className="muted">The spec the engine receives. Edit and apply to update the canvas.</span>
        <span className="spacer" />
        {error && <span className="error-text">{error}</span>}
        <button disabled={!dirty} onClick={() => { setDirty(false); setText(current); setError(null); }}>Discard</button>
        <button className="primary" disabled={!dirty} onClick={apply}>Apply</button>
      </div>
      <Editor
        height="220px"
        language="yaml"
        theme={monacoTheme()}
        value={text}
        onChange={(v) => { setText(v ?? ""); setDirty(true); }}
        options={{ minimap: { enabled: false }, fontSize: 12, scrollBeyondLastLine: false, automaticLayout: true }}
      />
    </div>
  );
}

function PlanView() {
  const { nodes, edges, meta, env } = useEditor();
  const [plan, setPlan] = useState<PlanResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    api.plan({ spec: toSpec(nodes, edges, meta), env })
      .then((p) => { setPlan(p); setError(null); })
      .catch((e) => setError(e instanceof ApiError ? e.message : String(e)));
  }, [nodes, edges, meta, env]);
  if (error) return <div className="error-text">Plan unavailable until the pipeline is valid: {error}</div>;
  if (!plan) return <div className="muted">Planning…</div>;
  return (
    <div className="row" style={{ alignItems: "flex-start", gap: 24 }}>
      <pre className="log">{plan.explain}</pre>
      <div className="small">
        <b>Resources: {plan.resources.profile}</b>
        <ul>{plan.resources.reasons.map((r) => <li key={r}>{r}</li>)}</ul>
      </div>
    </div>
  );
}

const STATUS_CLASS: Record<string, string> = { succeeded: "ok", failed: "bad", cancelled: "warn", running: "info", queued: "" };

export function StatusPill({ status }: { status: string }) {
  return <span className={`pill ${STATUS_CLASS[status] ?? ""}`}>{status}</span>;
}

function RunsView() {
  const pipeline = useEditor((s) => s.pipeline);
  const lastRunId = useEditor((s) => s.lastRunId);
  const [runs, setRuns] = useState<Run[]>([]);
  const [selected, setSelected] = useState<string | null>(lastRunId);
  useEffect(() => {
    if (lastRunId) setSelected(lastRunId);
  }, [lastRunId]);
  useEffect(() => {
    if (!pipeline) return;
    const load = () => api.runs(pipeline.id).then((list) => { setRuns(list); setSelected((s) => s ?? list[0]?.id ?? null); });
    load();
    const timer = setInterval(load, 4000);
    return () => clearInterval(timer);
  }, [pipeline, lastRunId]);
  if (!runs.length) return <div className="muted">No runs yet. Save a version, then press Run.</div>;
  return (
    <div className="row" style={{ alignItems: "flex-start", gap: 16, height: "100%" }}>
      <table className="grid list" style={{ width: 300, flex: "none" }}>
        <tbody>
          {runs.map((r) => (
            <tr key={r.id} onClick={() => setSelected(r.id)} style={{ fontWeight: r.id === selected ? 600 : 400 }}>
              <td><StatusPill status={r.status} /></td>
              <td>v{r.version} · {r.env}</td>
              <td className="muted">{new Date(r.created_at).toLocaleTimeString()}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {selected && <RunDetail key={selected} runId={selected} />}
    </div>
  );
}

export function RunDetail({ runId }: { runId: string }) {
  const [run, setRun] = useState<Run | null>(null);
  const [lines, setLines] = useState<string[]>([]);
  const [quality, setQuality] = useState<QualityResult[]>([]);
  useEffect(() => {
    setLines([]);
    return subscribeRun(runId, (event) => {
      if (event.type === "status") setRun(event.run);
      if (event.type === "log") setLines((l) => [...l, ...event.lines].slice(-2000));
    });
  }, [runId]);
  useEffect(() => {
    const load = () => api.runQuality(runId).then(setQuality);
    load();
    if (run && !["running", "queued"].includes(run.status)) return;
    const timer = setInterval(load, 5000);
    return () => clearInterval(timer);
  }, [runId, run?.status]);
  if (!run) return <div className="muted">Connecting…</div>;
  return (
    <div style={{ flex: 1, minWidth: 0 }}>
      <div className="row small">
        <StatusPill status={run.status} />
        <span className="mono">{run.id}</span>
        <span className="muted">v{run.version} · {run.env} · {run.mode}</span>
        <span className="spacer" />
        {["queued", "running"].includes(run.status) && (
          <button className="danger" onClick={() => api.cancelRun(run.id).then(setRun)}>Cancel</button>
        )}
      </div>
      {run.error && <div className="error-text small" style={{ marginTop: 6 }}>{run.error}</div>}
      {Object.keys(run.summary ?? {}).length > 0 && <div className="mono muted" style={{ marginTop: 6, overflowWrap: "anywhere" }}>{JSON.stringify(run.summary)}</div>}
      <QualityTable results={quality} />
      <pre className="log" style={{ marginTop: 10, maxHeight: 160, overflow: "auto" }}>{lines.join("\n")}</pre>
    </div>
  );
}
